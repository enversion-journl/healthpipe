# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pure IFDB contribution mapping, owned by the Patient domain."""

from collections import Counter
from datetime import datetime
from typing import Any

import dagster as dg
import oracledb
from DSA.FHIR.Patient.field_ownership import USE_OFFICIAL
from DSA.FHIR.Patient.patient_identity import order_rows, row_cpr
from DSA.FHIR.utils.fhir_utils import fhir_date
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    parse_clob_date,
    parse_clob_datetime_safe,
)
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.cpr_status import is_active, is_deceased, normalize_cpr_status
from utils.fhir.fhir_extensions import (
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DISTRICT,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_SOCIAL_DISTRICT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_YDERNUMMER,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

# Column name mapping from YAML aliases to Oracle column names
# This handles the mismatch between DSI extraction (UPPERCASE Oracle) and DSA transformation (lowercase aliases)
PATIENT_COLUMN_MAPPING: dict[str, str] = {
    "patient_id": "PATIENT_ID",
    "identifier_cpr": "CPR",
    "name_given": "FORNAVNE",
    "name_family": "EFTERNAVN",
    "address_street": "VEJ",
    "address_postal": "POSTNR",
    "address_city": "STEDNAVN",
    "address_municipality": "KOMMUNEKODE",
    "gender_code": "KOEN_KODE",
    "gender_text": "KOEN_KODETEKST",
    "birth_date": "FOEDSELSDATO",
    "doctor_name": "LAEGENAVN",
    "doctor_number": "LAEGENUMMER",
    "person_status_code": "PERSON_STATUSKODE",
    "person_status_text": "PERSON_STATUSKODETEKST",
    "address_protection": "HAR_ADRESSEBESKYTTELSE",
    "discretion_flag": "OENSKER_DISKRETION",
    "remarks": "BEMAERKNINGER",
    "district_code": "PERSON_DISTRIKTKODE",
    "district_text": "PERSON_DISTRIKTKODETEKST",
    "social_district_code": "PERSON_SOCIALDISTRIKTKODE",
    "social_district_text": "PERSON_SOCIALDISTRIKTKODETEKST",
    "last_updated": "OPDATERET_TID",
    "is_deleted": "SLETTET",
    "modtidspunkt": "MODTIDSPUNKT",
}


def transform_fhir_patient_ifdb(
    context: dg.AssetExecutionContext,
    patient_rows: list[dict[str, Any]],
) -> list[Patient]:
    """
    Transform IFDB patient rows to FHIR Patient resources using YAML mappings.

    Args:
        context: Dagster asset execution context
        patient_rows: List of dictionaries from IFDB.PATIENT_OPLYSNINGER_COLUMNA table

    Returns:
        List of FHIR Patient resources with complete demographic information including:
        - Identifiers (CPR, patient_id)
        - Name (given, family)
        - Gender
        - Birth date
        - Address (street, postal, city, municipality/district)
        - Active/deceased flags (from the CPR person status code, see utils.fhir.cpr_status)
        - General practitioner reference
        - Danish-specific extensions (district codes, address protection)
    """
    patients: list[Patient] = []
    # Codes outside the CPR list count as no information; they are counted so drift shows.
    unknown_status_codes: Counter[str] = Counter()

    def get_column_value(row: dict[str, Any], col_name: str) -> Any:
        """Get column value from row, handling both aliased and Oracle column names.

        The DSI extraction layer returns UPPERCASE Oracle column names,
        while test fixtures may use lowercase aliased names.
        This function tries multiple variants to ensure compatibility.

        Args:
            row: Dictionary containing patient data
            col_name: The aliased column name (e.g., "doctor_name")

        Returns:
            The column value or None if not found
        """
        # Try aliased name first (as used in test fixtures)
        value = row.get(col_name)
        if value is not None:
            return value

        # Try uppercase version of alias
        value = row.get(col_name.upper())
        if value is not None:
            return value

        # Try mapped Oracle column name (from PATIENT_COLUMN_MAPPING)
        oracle_name = PATIENT_COLUMN_MAPPING.get(col_name)
        if oracle_name:
            value = row.get(oracle_name)
            if value is not None:
                return value

        return None

    def map_gender(code: str | None, text: str | None) -> str:
        """Map gender code/text to FHIR gender value."""
        if code is not None:
            normalized = str(code).strip().lower()
            if normalized in {"1", "m", "male"}:
                return "male"
            if normalized in {"2", "k", "f", "female"}:
                return "female"
        if text:
            t = text.strip().lower()
            if "mand" in t or t == "male":
                return "male"
            if "kvinde" in t or t == "female":
                return "female"
        return "unknown"

    def build_general_practitioner_reference(
        doctor_name: str | None,
        doctor_number: str | None,
    ) -> Reference | None:
        """Build a reference to the patient's general practitioner.

        Uses Danish GP registration number (ydernummer) as identifier.
        """
        if not doctor_number and not doctor_name:
            return None

        # Create identifier reference using GP number (ydernummer)
        if doctor_number:
            return Reference(
                type="Practitioner",
                identifier=Identifier(
                    system=SYSTEM_YDERNUMMER,
                    value=str(doctor_number),
                ),
                display=doctor_name if doctor_name else None,
            )
        # If only name is available, create display-only reference
        return Reference(
            type="Practitioner",
            display=doctor_name,
        )

    for row in patient_rows:
        # Extract core identifiers
        patient_id = get_column_value(row, "patient_id")
        cpr = get_column_value(row, "identifier_cpr")

        # Build identifiers
        identifiers = []
        if cpr:
            identifiers.append(Identifier(system=SYSTEM_CPR, value=str(cpr)))
        if patient_id:
            identifiers.append(Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=str(patient_id)))

        # Build name - only create if we have at least one name component
        given_names_str = get_column_value(row, "name_given")
        family_name = get_column_value(row, "name_family")
        given_names = (
            [n for n in (given_names_str or "").split(" ") if n] if given_names_str else None
        )

        # Only create HumanName if at least one field has a value.
        name = None
        if family_name or given_names:
            name = HumanName(
                use=USE_OFFICIAL,
                family=family_name if family_name else None,
                given=given_names if given_names else None,
            )

        # Build gender
        gender_code = get_column_value(row, "gender_code")
        gender_text = get_column_value(row, "gender_text")
        gender = map_gender(gender_code, gender_text)

        # Get address protection flag. DFM archive rows store source flags as
        # NVARCHAR ("0"/"1"), where bool("0") is True -- use robust flag parsing.
        address_protection = get_column_value(row, "address_protection")
        has_address_protection = is_true_flag(address_protection)

        # Address is always populated when IFDB has one. The contribution only
        # carries the protection flag; the Patient builder masks name and address.
        address = None
        street = get_column_value(row, "address_street")
        postal = get_column_value(row, "address_postal")
        city = get_column_value(row, "address_city")
        municipality = get_column_value(row, "address_municipality")

        if any([street, postal, city, municipality]):
            # IFDB has no address history. '1900-01-01' = current address, start unknown.
            address = Address(
                line=[v for v in [street] if v] if street else None,
                postalCode=str(postal) if postal else None,
                city=city if city else None,
                district=str(municipality) if municipality else None,
                period=Period(start="1900-01-01"),
            )

        # Build birth date
        birth_date = get_column_value(row, "birth_date")

        # Convert birth_date to FHIR format, handling CLOB date formats
        fhir_birth_date = None
        if birth_date:
            try:
                # If it's still a LOB (shouldn't happen after our fix, but be safe)
                if isinstance(birth_date, oracledb.LOB):
                    birth_date = birth_date.read()

                # Try parsing as CLOB format first
                parsed_dt = parse_clob_date(birth_date)
                if parsed_dt:
                    fhir_birth_date = fhir_date(parsed_dt)
                elif isinstance(birth_date, datetime):
                    fhir_birth_date = fhir_date(birth_date)
                else:
                    # String from MSSQL archive — may be "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
                    date_only = str(birth_date).strip().split(" ")[0].split("T")[0]
                    fhir_birth_date = fhir_date(date_only)
            except Exception:
                context.log.warning("Patient birth-date parsing failed; value omitted")

        # The status code carries EPJ's prefix (PAS_ks.ur.CPRStatus.90); utils.fhir.cpr_status interprets it.
        person_status_code = get_column_value(row, "person_status_code")
        status_text = str(person_status_code).strip() if person_status_code is not None else ""
        if status_text and normalize_cpr_status(status_text) is None:
            unknown_status_codes[status_text[:40]] += 1
        deceased = is_deceased(person_status_code)
        active = is_active(person_status_code)

        # Build general practitioner reference
        doctor_name = get_column_value(row, "doctor_name")
        doctor_number = get_column_value(row, "doctor_number")
        gp_reference = build_general_practitioner_reference(doctor_name, doctor_number)

        # Build extensions
        extensions = []

        # MODTIDSPUNKT extension (source receipt timestamp, per-row from IFDB)
        raw_modtidspunkt = get_column_value(row, "modtidspunkt")
        modtidspunkt_fhir = parse_clob_datetime_safe(raw_modtidspunkt)
        if modtidspunkt_fhir:
            extensions.append(
                Extension(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt_fhir)
            )

        # Address protection extension
        if has_address_protection:
            extensions.append(Extension(url=EXTENSION_ADDRESS_PROTECTION, valueBoolean=True))

        # District extension (person district)
        district_code = get_column_value(row, "district_code")
        district_text = get_column_value(row, "district_text")
        if district_code or district_text:
            district_ext = Extension(url=EXTENSION_DISTRICT)
            if district_code:
                district_ext.extension = district_ext.extension or []
                district_ext.extension.append(Extension(url="code", valueString=str(district_code)))
            if district_text:
                district_ext.extension = district_ext.extension or []
                district_ext.extension.append(Extension(url="text", valueString=str(district_text)))
            extensions.append(district_ext)

        # Social district extension
        social_district_code = get_column_value(row, "social_district_code")
        social_district_text = get_column_value(row, "social_district_text")
        if social_district_code or social_district_text:
            social_ext = Extension(url=EXTENSION_SOCIAL_DISTRICT)
            if social_district_code:
                social_ext.extension = social_ext.extension or []
                social_ext.extension.append(
                    Extension(url="code", valueString=str(social_district_code))
                )
            if social_district_text:
                social_ext.extension = social_ext.extension or []
                social_ext.extension.append(
                    Extension(url="text", valueString=str(social_district_text))
                )
            extensions.append(social_ext)

        # Source deletion extension (flag + IFDB column origin); always emit for consistent filtering
        is_deleted = get_column_value(row, "is_deleted")
        deleted_value = int(is_true_flag(is_deleted))
        extensions.append(build_is_deleted_extension(deleted_value))

        # Create Patient resource
        patient = Patient(
            identifier=identifiers if identifiers else None,
            name=[name] if name else None,
            gender=gender,
            birthDate=fhir_birth_date,
            address=[address] if address else None,
            active=active,
            deceasedBoolean=deceased if deceased else None,
            generalPractitioner=[gp_reference] if gp_reference else None,
            extension=extensions if extensions else None,
        )

        patients.append(patient)

    if unknown_status_codes:
        context.log.warning(
            "%d patients carry a status code outside CPR's list; treated as no information. "
            "Most common: %s",
            sum(unknown_status_codes.values()),
            unknown_status_codes.most_common(5),
        )
    context.log.info(f"Transformed {len(patients)} patients to FHIR resources")
    return patients


def _dedup_rows_by_cpr(rows: list[dict]) -> list[dict]:
    """Keep the primary row per CPR, chosen by ``order_rows``; rows without a CPR are all kept."""
    by_cpr: dict[str, list[dict]] = {}
    no_cpr: list[dict] = []
    for row in rows:
        cpr = row_cpr(row)
        (by_cpr.setdefault(cpr, []) if cpr else no_cpr).append(row)
    return [order_rows(group)[0] for group in by_cpr.values()] + no_cpr


def _transform_patient_rows_to_fhir(
    patient_rows: list[dict],
    context: dg.AssetExecutionContext,
) -> list[Patient]:
    """
    Transform IFDB patient rows into FHIR Patient resources.

    Pre-deduplicates rows by CPR (preferring non-deleted, then most recently
    updated) *before* the FHIR transform to avoid wasting transform cycles.

    Args:
        patient_rows: List of patient row dictionaries from IFDB extraction
        context: Dagster execution context

    Returns:
        List of unique Patient resources
    """
    deduped_rows = _dedup_rows_by_cpr(patient_rows)
    if len(deduped_rows) < len(patient_rows):
        context.log.info(
            f"CPR dedup: {len(patient_rows)} rows → {len(deduped_rows)} "
            f"({len(patient_rows) - len(deduped_rows)} duplicate CPR rows removed)"
        )

    patients = transform_fhir_patient_ifdb(
        context=context,
        patient_rows=deduped_rows,
    )

    # Remove duplicates based on IFDB patient identifier (belt-and-suspenders)
    seen_ids: set[str] = set()
    unique_patients: list[Patient] = []
    for patient in patients:
        patient_id = None
        if patient.identifier:
            for identifier in patient.identifier:
                if identifier.system == SYSTEM_IFDB_PATIENT_ID:
                    patient_id = identifier.value
                    break
        if patient_id and patient_id not in seen_ids:
            seen_ids.add(patient_id)
            unique_patients.append(patient)
        elif not patient_id:
            unique_patients.append(patient)

    return unique_patients
