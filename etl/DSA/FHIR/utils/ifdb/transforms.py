# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB transform functions for converting Oracle IFDB data to FHIR resources.

Keep this module to IFDB-wide source normalization and shared IFDB transforms.
Helpers that only make sense inside one asset pipeline should stay with that
asset or move to a narrowly named module such as ``xml_aktivitetsresultat.py``.
"""

from datetime import datetime
from typing import Any

import dagster as dg
import oracledb
from DSA.FHIR.utils.fhir_utils import fhir_date, fhir_datetime
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    CPR_STATUS_DECEASED,
    CPR_STATUS_INACTIVE,
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


def normalize_oracle_id(value: Any) -> str | None:
    """Stringify an Oracle identifier without a stray ``.0`` suffix.

    Oracle NUMBER columns come back as Python ``float`` via ``oracledb``;
    plain ``str(1.0)`` yields ``"1.0"`` which breaks identifier-based
    FHIR reference resolution downstream.  Integer-valued floats are
    normalised to their ``int`` form, and string values with a trailing
    ``.0`` over a digit-only body are stripped likewise.
    """
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    s = str(value)
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    return s


def is_true_flag(value: Any) -> bool:
    """Interpret IFDB source flag values robustly.

    Handles bool, int, float, and string representations including float-strings
    like "1.0" that arise when Oracle NUMBER(1) values are stored as NVARCHAR
    in the DWH archive.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int | float):
        return int(value) == 1
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in {"1", "true", "t", "yes", "y"}:
            return True
        try:
            return int(float(stripped)) == 1
        except (ValueError, OverflowError):
            return False
    return False


def source_deleted_to_int(value: Any) -> int:
    """Convert IFDB deleted/source removal flags to integer 0/1."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int | float):
        return 1 if int(value) != 0 else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "t", "yes", "y"} else 0
    return 0


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


def parse_clob_date(date_str: str) -> datetime | None:
    """
    Parse a CLOB date string to a datetime object (date-only, time set to midnight).

    Handles CLOB date formats:
    - "12DEC1955:00:00:00" (birth date format - extracts date part only)

    Args:
        date_str: Date string in CLOB format or standard format

    Returns:
        datetime object if parsing succeeds, None if not CLOB format

    Raises:
        ValueError: If CLOB format is detected but parsing fails
    """
    if not date_str:
        return None

    # Convert to string if not already
    date_str = str(date_str).strip()

    # Check if it matches CLOB format pattern (contains : and month names)
    if ":" in date_str and any(
        month in date_str.upper()
        for month in [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ]
    ):
        # CLOB format detected - must parse successfully or raise error
        try:
            # Extract date part before the colon (DDMONYYYY)
            date_part = date_str.split(":")[0]
            # Parse using format DDMONYYYY
            return datetime.strptime(date_part, "%d%b%Y")
        except (ValueError, IndexError) as e:
            # Format was detected as CLOB but parsing failed - raise error
            raise ValueError(f"Failed to parse CLOB date format '{date_str}': {e}") from e

    # Not CLOB format - return None (caller should try standard parsing)
    return None


def parse_clob_datetime(date_str: str) -> datetime | None:
    """
    Parse a CLOB datetime string to a datetime object (includes time components).

    Handles CLOB datetime formats:
    - "16MAY2025:11:39:05" (DDMONYYYY:HH24:MI:SS)
    - "20MAY2025:17:00:05.000000" (DDMONYYYY:HH24:MI:SS.FF6)

    Args:
        date_str: Datetime string in CLOB format or standard format

    Returns:
        datetime object if parsing succeeds, None if not CLOB format

    Raises:
        ValueError: If CLOB format is detected but parsing fails
    """
    if not date_str:
        return None

    # Convert to string if not already
    date_str = str(date_str).strip()

    # Check if it matches CLOB format pattern (contains : and month names)
    if ":" in date_str and any(
        month in date_str.upper()
        for month in [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ]
    ):
        # CLOB format detected - must parse successfully or raise error
        try:
            # Try format with microseconds first (DDMONYYYY:HH24:MI:SS.FF6)
            if "." in date_str:
                return datetime.strptime(date_str, "%d%b%Y:%H:%M:%S.%f")
            else:
                # Format without microseconds (DDMONYYYY:HH24:MI:SS)
                return datetime.strptime(date_str, "%d%b%Y:%H:%M:%S")
        except ValueError as e:
            # Format was detected as CLOB but parsing failed - raise error
            raise ValueError(f"Failed to parse CLOB datetime format '{date_str}': {e}") from e

    # Not CLOB format - return None (caller should try standard parsing)
    return None


def parse_clob_datetime_safe(value: Any) -> str | None:
    """Safely parse Oracle/CLOB/ISO datetime values to a FHIR datetime string."""
    if not value:
        return None
    try:
        if isinstance(value, str):
            parsed = parse_clob_datetime(value)
            if parsed:
                return fhir_datetime(parsed)
            try:
                return fhir_datetime(datetime.fromisoformat(value))
            except (ValueError, TypeError):
                return None
        return fhir_datetime(value)
    except Exception:
        return None


def parse_xml_compact_datetime_safe(value: Any) -> str | None:
    """Parse a RIS_YDELSE.XML compact datetime to a FHIR datetime string.

    The XML carries wall-clock timestamps as digit strings ``YYYYMMDDHHMM``
    (e.g. ``202605111423``), optionally with seconds (``YYYYMMDDHHMMSS``) or
    date-only (``YYYYMMDD``) — a format ``parse_clob_datetime`` does not handle.
    Falls back to the CLOB/ISO parser for any non-compact form.
    """
    if not value:
        return None
    s = str(value).strip()
    if s.isdigit() and len(s) in (8, 12, 14):
        for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
            try:
                return fhir_datetime(datetime.strptime(s, fmt))
            except ValueError:
                continue
        dg.get_dagster_logger().warning(
            f"Compact XML datetime {s!r} matched no %Y%m%d[%H%M[%S]] format; "
            "falling back to CLOB parser"
        )
    return parse_clob_datetime_safe(value)


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
        - Deceased status (based on CPR person status code)
        - General practitioner reference
        - Danish-specific extensions (district codes, address protection)
    """
    patients: list[Patient] = []

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

    def is_deceased(status_code: str | None) -> bool:
        """Check if person status code indicates deceased."""
        if status_code is None:
            return False
        return str(status_code).strip() in CPR_STATUS_DECEASED

    def is_active(status_code: str | None) -> bool:
        """Check if person status code indicates active (not annulled/cancelled)."""
        if status_code is None:
            return True  # Default to active if no status
        code = str(status_code).strip()
        # Inactive if annulled (80) or deceased (90)
        return code not in CPR_STATUS_INACTIVE and code not in CPR_STATUS_DECEASED

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

        # Only create HumanName if at least one field has a value
        name = None
        if family_name or given_names:
            name = HumanName(
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

        # Address is always populated when IFDB has one. Protection is the
        # EXTENSION_ADDRESS_PROTECTION flag, not omission of address[].
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
            except Exception as e:
                context.log.warning(
                    f"Failed to parse birth_date '{birth_date}': {e}. "
                    "Skipping birth_date for this patient."
                )

        # Check person status for deceased and active flags
        person_status_code = get_column_value(row, "person_status_code")
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

    context.log.info(f"Transformed {len(patients)} patients to FHIR resources")
    return patients


def to_fhir_instant(value: datetime | str | None) -> str | None:
    """Coerce a datetime or ISO-8601 string to FHIR ``instant`` format.

    FHIR ``instant`` requires full date+time with timezone (``YYYY-MM-DDThh:mm:ss+zz:zz``).
    This helper ensures timezone is present and that the output always includes time
    components -- unlike ``fhir_datetime`` which allows date-only values.
    """
    if value is None:
        return None
    from datetime import UTC

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.isoformat()
    except ValueError:
        if "T" not in s:
            s = f"{s}T00:00:00"
        if "+" not in s and "Z" not in s:
            s = f"{s}+00:00"
        return s


def build_performer_dict(row_dict: dict[str, Any]) -> list[dict]:
    """Build Observation/Procedure performer list with Practitioner and/or Organization refs.

    Shared across Observation and Procedure Aktivitetsresultat builders.
    """
    from utils.fhir.identifier_systems import SYSTEM_IFDB_STAFF, SYSTEM_SOR

    performers: list[dict] = []

    sor_code = row_dict.get("UDFOERT_ADMENHED_SORCODE")
    if sor_code and str(sor_code).strip():
        org_ref: dict[str, Any] = {"type": "Organization"}
        org_name = row_dict.get("UDFOERT_ADMENHED_NAVN")
        if org_name:
            org_ref["display"] = str(org_name)
        org_ref["identifier"] = {
            "system": SYSTEM_SOR,
            "value": str(sor_code).strip(),
        }
        performers.append(org_ref)

    pers_id = row_dict.get("UDFOERT_PERSID")
    pers_id_norm = normalize_oracle_id(pers_id)
    pers_navn = row_dict.get("UDFOERT_PERSNAVN")
    if pers_id_norm or pers_navn:
        pract_ref: dict[str, Any] = {"type": "Practitioner"}
        if pers_navn:
            pract_ref["display"] = str(pers_navn)
        if pers_id_norm:
            pract_ref["identifier"] = {
                "system": SYSTEM_IFDB_STAFF,
                "value": pers_id_norm,
            }
        performers.append(pract_ref)

    return performers


__all__ = [
    "build_performer_dict",
    "is_true_flag",
    "normalize_oracle_id",
    "parse_clob_datetime",
    "parse_clob_datetime_safe",
    "source_deleted_to_int",
    "to_fhir_instant",
    "transform_fhir_patient_ifdb",
]
