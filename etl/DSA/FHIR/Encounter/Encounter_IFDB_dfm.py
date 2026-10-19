# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Encounter asset using DFM (Data Flow Management).

Single-asset pattern: fetches from arc.* in batches, transforms to FHIR, and
posts to the FHIR server — all within one asset to keep memory bounded.

This module also owns the shared Encounter transform/post helpers (used by the
dependency resolver), which lived in Encounter_IFDB.py before the partitioned
assets were removed. Both the DFM asset and the dependency resolver fetch
location rows via ``_fetch_location_data_for_batch`` against arc.ifdb_location.

No Dagster partitions — checkpoint tracking replaces partition boundaries.
"""

import os
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import SCHEMA_TO_TARGET_TYPE, DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id, parse_clob_datetime
from DSA.FHIR.utils.ifdb.ward_unit_bed import (
    archive_datetime_to_dt,
    archive_datetime_to_fhir,
    bed_physical_type,
    build_absence_extension,
    child_affected_encounter_count_sql,
    child_affected_encounter_ids_sql,
    child_backfill_remaining_sql,
    child_backfill_slice_sql,
)
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig, ResolveValuesFn
from DSA.FHIR.utils.status_mapping import (
    map_encounter_location_status,
    map_encounter_status,
)
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.encounter import Encounter, EncounterLocation
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from pydantic import Field
from utils.fhir.fhir_extensions import EXTENSION_BED_SPACE_WARD_UNIT_CODE
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_BED,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# Loft paa antal kontakter, den natlige delta maa traekke ind VIA sengeophold og
# fravaer -- ikke paa Encounter-deltaet selv. Det er child-vandmaerket, der
# maales mod, saa en tilbagerulning af Encounter-checkpointet aendrer ikke tallet.
# Rutinemaessig child-churn er de aktuelt indlagte patienter. Maalt paa dev som
# kontakter med sengeophold/fravaer startet eller afsluttet pr. kalenderdag
# (kildens egne datoer -- arkivets dw_timestamp_update baerer kun bulk-dagen):
# seneste 365 dage median 1.272, p99 1.453, maks 1.480 kontakter pr. dag.
# Efterslaebet efter et frisk arkiv-load er hele historikken (ca. 915.000
# kontakter paa dev). Loftet skiller de to med ~35x luft til det maalte maks.
# Laeses fra env som FHIR-jobbenes _max_concurrent, saa en planlagt bulk kan
# haeve det uden en kodeaendring.
CHILD_TOUCHED_ENCOUNTERS_CEILING = int(
    os.environ.get("FHIR_ENCOUNTER_CHILD_TOUCHED_CEILING", "50000")
)

# Efterslaebet paa Encounter-berigelsen er en ENGANGSOPERATION pr. miljoe, men
# den koeres i skiver. Fremskridtet staar i meta.DFMState som "sidste
# behandlede OBJECT_ID" under dette navn, og saettes til DONE-sentinelen, naar
# der ikke er mere tilbage. Derefter er koerslen en no-op, uanset hvor mange
# gange den startes -- det er det, der goer den sikker at lade staa i en
# runbook pr. miljoe.
CHILD_BACKFILL_SUBJECT = "Encounter_ChildBackfill"
CHILD_BACKFILL_DONE = "DONE"
CHILD_BACKFILL_SLICE_DEFAULT = 25_000

# Child-affected-deltaet har sit EGET vandmaerke og maaler ikke paa
# Encounter-checkpointet. Uden det laaser de tre mekanismer hinanden: efter
# deploy matcher child-unionen hele historikken, saa vaernet fejler den natlige
# koersel; fordi koerslen fejler, rykker Encounter-checkpointet ikke; og
# efterslaebet roerer hverken child-arkivernes tidsstempler eller checkpointet.
# Vaernet ville derfor beregne det samme tal i morgen -- for evigt.
#
# Med et selvstaendigt vandmaerke er ansvaret delt: efterslaebet staempler det,
# naar det er faerdigt ("child-raekker til hertil er haandteret"), og derefter
# ser deltaet kun aegte nye child-aendringer. Det er praecis den "fremadrettede
# berigelse", driftsnoten hele tiden har lovet.
CHILD_DELTA_WATERMARK_SUBJECT = "Encounter_ChildDelta"

# Nedre graense, naar "alt" skal taelles. Arkivets dw_timestamp_update er altid
# nyere, saa den fungerer som epoke uden at skulle kende arkivets aelste raekke.
_CHILD_EPOCH = "1900-01-01 00:00:00"

_V3_ACT_CODE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ActCode"


class EncounterDfmConfig(RunModeConfig):
    """Encounter run config, extended with the child-affected backlog switch."""

    child_affected_backfill: bool = Field(
        default=False,
        description=(
            "Koer naeste skive af det historiske efterslaeb paa "
            "Encounter-berigelsen i stedet for et delta. Engangsoperation pr. "
            "miljoe: fremskridtet staemples i meta.DFMState, saa koerslen kan "
            "gentages til den melder faerdig og derefter er en no-op. "
            "Checkpointet for fhir_server.Encounter roeres ikke. Lad staa "
            "False i planlagte koersler."
        ),
    )
    child_backfill_slice: int = Field(
        default=CHILD_BACKFILL_SLICE_DEFAULT,
        description=(
            "Antal kontakter pr. efterslaebs-skive. Mindre skiver giver "
            "hyppigere staempling og dermed mindre tabt arbejde ved en afbrudt "
            "koersel; stoerre skiver mindre overhead."
        ),
    )
    child_backfill_max_slices: int = Field(
        default=0,
        description=(
            "Antal skiver pr. start. 0 (standard) betyder: koer til "
            "efterslaebet er faerdigt. Saet et tal for at afgraense koerslen, "
            "fx til at maale varigheden af én skive foerst. Markoeren "
            "staemples efter hver skive, saa en afbrudt koersel taber hoejst "
            "den skive, der var i gang -- uanset loftet."
        ),
    )
    child_backfill_restart: bool = Field(
        default=False,
        description=(
            "Nulstil efterslaebets markoer og begynd forfra. Kun til brug hvis "
            "efterslaebet skal koeres igen, fx efter en aendring i det, "
            "berigelsen udsender."
        ),
    )


# =============================================================================
# Reference Resolution Configuration
# =============================================================================

# Define which references Encounter has and how to resolve them
ENCOUNTER_REFERENCE_CONFIGS = [
    # Patient reference (REQUIRED) - field is "subject" for Encounter
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    # EpisodeOfCare reference (OPTIONAL)
    ReferenceFieldConfig(
        field_name="episodeOfCare",
        target_resource_type="EpisodeOfCare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        is_required=False,
        description="Episode of care reference from IFDB EPISODE_OF_CARE_ID",
    ),
    # Location reference (OPTIONAL) - handles EncounterLocation.location structure
    ReferenceFieldConfig(
        field_name="location",
        target_resource_type="Location",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Location reference via IFDB LOCATION_SORCODE (SOR identifier)",
    ),
    # Bed Location reference (OPTIONAL) — same field, different identifier system
    ReferenceFieldConfig(
        field_name="location",
        target_resource_type="Location",
        identifier_system=SYSTEM_IFDB_BED,
        is_required=False,
        description="Bed Location reference via IFDB BED_ID",
    ),
    # Service provider Organization reference (OPTIONAL) - via SOR code
    ReferenceFieldConfig(
        field_name="serviceProvider",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Service provider organization from IFDB SERVICE_PROVIDER_SORCODE",
    ),
]


# ============================================================================
# Shared Helper Functions
# ============================================================================


def _transform_encounters_to_fhir(
    encounter_rows: list[dict[str, Any]],
    locations_by_encounter: dict[str, list[dict[str, Any]]],
    context: dg.AssetExecutionContext,
    beds_by_encounter: dict[str, list[dict[str, Any]]] | None = None,
    absences_by_encounter: dict[str, list[dict[str, Any]]] | None = None,
) -> list[Encounter]:
    """
    Transform encounter rows to FHIR Encounter resources.

    Creates Encounter.location entries with:
    - location: Reference using SYSTEM_SOR and LOCATION_SORCODE (stripped of .0 suffix)
    - status: Required field derived from encounter status and period
    - period: Time period at this location

    Bed-space rows become extra location entries with physicalType=bd.
    Absence rows become parent extensions (Mønster A + fragment URLs).
    """
    encounters: list[Encounter] = []

    for enc_row in encounter_rows:
        encounter_id = enc_row.get("OBJECT_ID")
        encounter_status_text = enc_row.get("STATUS_TEXT")
        raw_deleted = enc_row.get("DELETED")
        deleted_value = int(is_true_flag(raw_deleted))
        is_deleted_flag = deleted_value != 0

        # Create encounter identifier
        encounter_identifier = Identifier(
            system=SYSTEM_IFDB_OBJECT_ID,
            value=normalize_oracle_id(encounter_id),
        )

        # Create subject reference to patient
        subject = Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=normalize_oracle_id(enc_row.get("PATIENT_ID")),
            )
        )

        # Create episode of care reference if available
        episode_of_care_ref = None
        if enc_row.get("EPISODE_OF_CARE_ID"):
            episode_of_care_ref = Reference(
                identifier=Identifier(
                    system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                    value=normalize_oracle_id(enc_row["EPISODE_OF_CARE_ID"]),
                )
            )

        # Create service provider reference via SOR code (resolved later)
        service_provider_ref = None
        if enc_row.get("SERVICE_PROVIDER_SORCODE"):
            service_provider_ref = Reference(
                identifier=Identifier(
                    system=SYSTEM_SOR,
                    value=str(enc_row["SERVICE_PROVIDER_SORCODE"]),
                )
            )

        # Map status (IFDB soft-delete -> FHIR entered-in-error)
        status = map_encounter_status(encounter_status_text, is_deleted=is_deleted_flag)

        # Parse period_start
        period_start_fhir = None
        period_start_dt = None
        if enc_row.get("PERIOD_START"):
            try:
                parsed_dt = parse_clob_datetime(enc_row["PERIOD_START"])
                if parsed_dt:
                    period_start_fhir = fhir_datetime(parsed_dt)
                    period_start_dt = parsed_dt
                else:
                    period_start_fhir = fhir_datetime(enc_row["PERIOD_START"])
                    period_start_dt = enc_row["PERIOD_START"]
            except Exception as e:
                raise ValueError(
                    f"Failed to parse PERIOD_START '{enc_row['PERIOD_START']}': {e}"
                ) from e

        # Parse period_end
        period_end_fhir = None
        period_end_dt = None
        if enc_row.get("PERIOD_END"):
            try:
                parsed_dt = parse_clob_datetime(enc_row["PERIOD_END"])
                if parsed_dt:
                    period_end_fhir = fhir_datetime(parsed_dt)
                    period_end_dt = parsed_dt
                else:
                    period_end_fhir = fhir_datetime(enc_row["PERIOD_END"])
                    period_end_dt = enc_row["PERIOD_END"]
            except Exception as e:
                raise ValueError(
                    f"Failed to parse PERIOD_END '{enc_row['PERIOD_END']}': {e}"
                ) from e

        # Create period
        period = Period(start=period_start_fhir, end=period_end_fhir)

        # Create class coding
        class_text = enc_row.get("CLASS_TEXT") or ""
        class_coding = Coding(
            system=_V3_ACT_CODE_SYSTEM,
            code="AMB" if "ambulant" in class_text.lower() else "IMP",
            display=class_text or "Unknown",
        )

        # Create location data with status
        encounter_locations = []
        for loc_data in locations_by_encounter.get(str(encounter_id), []):
            raw_sorcode = loc_data.get("LOCATION_SORCODE")
            if not raw_sorcode:
                continue

            sor_value = normalize_oracle_id(raw_sorcode)

            # Parse location dates
            loc_period_start_fhir = None
            loc_period_start_dt = None
            if loc_data.get("PERIOD_START"):
                try:
                    parsed_dt = parse_clob_datetime(loc_data["PERIOD_START"])
                    if parsed_dt:
                        loc_period_start_fhir = fhir_datetime(parsed_dt)
                        loc_period_start_dt = parsed_dt
                    else:
                        loc_period_start_fhir = fhir_datetime(loc_data["PERIOD_START"])
                        loc_period_start_dt = loc_data["PERIOD_START"]
                except Exception as e:
                    context.log.warning(f"Failed to parse location PERIOD_START: {e}")

            loc_period_end_fhir = None
            loc_period_end_dt = None
            if loc_data.get("PERIOD_END"):
                try:
                    parsed_dt = parse_clob_datetime(loc_data["PERIOD_END"])
                    if parsed_dt:
                        loc_period_end_fhir = fhir_datetime(parsed_dt)
                        loc_period_end_dt = parsed_dt
                    else:
                        loc_period_end_fhir = fhir_datetime(loc_data["PERIOD_END"])
                        loc_period_end_dt = loc_data["PERIOD_END"]
                except Exception as e:
                    context.log.warning(f"Failed to parse location PERIOD_END: {e}")

            # Determine location status (Required field in FHIR)
            # Use location-specific period if available, otherwise use encounter period
            location_status = map_encounter_location_status(
                encounter_status_text=encounter_status_text,
                period_start=loc_period_start_dt if loc_period_start_dt else period_start_dt,
                period_end=loc_period_end_dt if loc_period_end_dt else period_end_dt,
            )

            encounter_locations.append(
                EncounterLocation(
                    location=Reference(
                        type="Location",
                        identifier=Identifier(
                            system=SYSTEM_SOR,
                            value=sor_value,
                        ),
                    ),
                    status=location_status,
                    period=Period(start=loc_period_start_fhir, end=loc_period_end_fhir),
                )
            )

        for bed_data in (beds_by_encounter or {}).get(str(encounter_id), []):
            bed_id = normalize_oracle_id(bed_data.get("BED_ID"))
            if not bed_id:
                continue

            bed_start_fhir = archive_datetime_to_fhir(bed_data.get("PERIOD_START_REGISTRATION"))
            bed_end_fhir = archive_datetime_to_fhir(bed_data.get("PERIOD_END_REGISTRATION"))
            bed_start_dt = archive_datetime_to_dt(bed_data.get("PERIOD_START_REGISTRATION"))
            bed_end_dt = archive_datetime_to_dt(bed_data.get("PERIOD_END_REGISTRATION"))

            location_status = map_encounter_location_status(
                encounter_status_text=encounter_status_text,
                period_start=bed_start_dt if bed_start_dt else period_start_dt,
                period_end=bed_end_dt if bed_end_dt else period_end_dt,
            )
            # Enhedskoden baeres paa selve opholdet: en seng kan flytte mellem
            # enheder over tid, og opholdsraekken har koden som den var paa
            # tidspunktet. Joines direkte til WardUnit.admin_unit_code.
            ward_code = (
                str(bed_data["LOCATION_CODE"]).strip() if bed_data.get("LOCATION_CODE") else None
            )
            encounter_locations.append(
                EncounterLocation(
                    location=Reference(
                        type="Location",
                        identifier=Identifier(system=SYSTEM_IFDB_BED, value=bed_id),
                    ),
                    status=location_status,
                    period=Period(start=bed_start_fhir, end=bed_end_fhir),
                    physicalType=bed_physical_type(),
                    extension=(
                        [Extension(url=EXTENSION_BED_SPACE_WARD_UNIT_CODE, valueString=ward_code)]
                        if ward_code
                        else None
                    ),
                )
            )

        extensions = [build_is_deleted_extension(deleted_value)]
        absence_rows = (absences_by_encounter or {}).get(str(encounter_id), [])
        for absence_row in absence_rows:
            absence_ext = build_absence_extension(absence_row)
            if absence_ext is not None:
                extensions.append(absence_ext)

        # Create the encounter
        enc = Encounter(
            identifier=[encounter_identifier],
            status=status,
            class_fhir=class_coding,
            subject=subject,
            episodeOfCare=[episode_of_care_ref] if episode_of_care_ref else None,
            serviceProvider=service_provider_ref,
            period=period,
            location=encounter_locations if encounter_locations else None,
            extension=extensions if extensions else None,
        )
        encounters.append(enc)

    return encounters


def _filter_unresolved_locations(
    encounters: list[Encounter],
    context: dg.AssetExecutionContext,
) -> None:
    """Filter out EncounterLocation entries with unresolved Location references."""
    for enc in encounters:
        if not enc.location:
            continue

        valid_locations = []
        for enc_loc in enc.location:
            # Check if this location reference was resolved (has id set)
            if enc_loc.location and getattr(enc_loc.location, "id", None):
                valid_locations.append(enc_loc)
            elif enc_loc.location and getattr(enc_loc.location, "identifier", None):
                # Still has identifier - means it wasn't resolved
                loc_id = (
                    str(enc_loc.location.identifier.value)
                    if enc_loc.location.identifier.value
                    else "unknown"
                )
                context.log.warning(f"Could not resolve Location with identifier {loc_id}")

        enc.location = valid_locations if valid_locations else None


def _post_encounters_to_fhir(
    context: dg.AssetExecutionContext,
    encounters: list[Encounter],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post encounters to FHIR database with reference resolution.

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    resolving subject references to soft-deleted PATIENT_IDs whose active CPR
    twin exists in FHIR (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    # Collect all patient and episode IDs that need to exist for dependency resolution
    patient_ids_needed: set[str] = set()
    eoc_ids_needed: set[str] = set()

    for enc in encounters:
        if enc.subject and getattr(enc.subject, "identifier", None):
            patient_ids_needed.add(str(enc.subject.identifier.value))
        if enc.episodeOfCare:
            first_ep = enc.episodeOfCare[0]
            if first_ep and getattr(first_ep, "identifier", None):
                eoc_ids_needed.add(str(first_ep.identifier.value))

    # Resolve missing dependencies if enabled
    if resolve_missing_dependencies and mssql:
        from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients
        from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_episodes_of_care

        # First resolve episodes of care (this will also resolve their patient dependencies)
        if eoc_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(eoc_ids_needed)} episodes of care exist")
            resolve_missing_episodes_of_care(
                eoc_ids=eoc_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
            )

        # Then resolve any remaining patient dependencies
        if patient_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
            ensure_patients(
                ifdb_patient_ids=patient_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
                need=NEED_REFERENCE,
            )

    encounters = prepare_resources_for_upsert(
        resources=encounters,
        fhir_database=fhir_database,
        resource_type="Encounter",
        preferred_id_systems=[SYSTEM_IFDB_OBJECT_ID],
        reference_configs=ENCOUNTER_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        force_reload=True,
        logger=logger,
    )

    # Filter out unresolved location references (they are optional)
    _filter_unresolved_locations(encounters, context)

    # Post resources using direct DB insertion
    return post_resources_direct(
        context=context,
        resources=encounters,
        fhir_database=fhir_database,
        resource_type="Encounter",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


def _fetch_child_rows_for_batch(
    encounter_ids: list[str],
    mssql: MSSQLResource,
    *,
    table: str,
    columns: list[str],
    extra_where: str = "",
) -> dict[str, list[dict[str, Any]]]:
    """Fetch child-archive rows for a batch of encounter OBJECT_IDs."""
    by_encounter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not encounter_ids:
        return by_encounter

    col_sql = ", ".join(f"[{col}]" for col in columns)
    extra = f" {extra_where}" if extra_where else ""
    chunk_size = 999
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for i in range(0, len(encounter_ids), chunk_size):
                chunk = encounter_ids[i : i + chunk_size]
                placeholders = ", ".join(f"'{eid.replace(chr(39), chr(39) * 2)}'" for eid in chunk)
                query = (
                    f"SELECT {col_sql} FROM {table} "
                    f"WHERE [dw_is_deleted] = 0 "
                    f"{extra}"
                    f"AND [OBJECT_ID] IN ({placeholders})"
                )
                cursor.execute(query)
                result_columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    row_dict = dict(zip(result_columns, row, strict=False))
                    by_encounter[str(row_dict["OBJECT_ID"])].append(row_dict)
        finally:
            cursor.close()
    return by_encounter


def _fetch_location_data_for_batch(
    encounter_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch department-stay rows from arc.ifdb_location for a batch of IDs."""
    return _fetch_child_rows_for_batch(
        encounter_ids,
        mssql,
        table="[arc].[ifdb_location]",
        columns=["OBJECT_ID", "LOCATION_SORCODE", "PERIOD_START", "PERIOD_END"],
        extra_where="AND [LOCATION_SORCODE] IS NOT NULL ",
    )


def _fetch_bed_space_data_for_batch(
    encounter_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch bed-space history rows for a batch of encounter IDs."""
    return _fetch_child_rows_for_batch(
        encounter_ids,
        mssql,
        table="[arc].[ifdb_encounter_bed_space_history]",
        columns=[
            "OBJECT_ID",
            "BED_ID",
            "LOCATION_CODE",
            "PERIOD_START_REGISTRATION",
            "PERIOD_END_REGISTRATION",
        ],
        extra_where="AND [BED_ID] IS NOT NULL ",
    )


def _fetch_absence_data_for_batch(
    encounter_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch absence rows for a batch of encounter IDs."""
    return _fetch_child_rows_for_batch(
        encounter_ids,
        mssql,
        table="[arc].[ifdb_absence]",
        columns=[
            "OBJECT_ID",
            "PERIOD_START",
            "PERIOD_END",
            "EXPECTED_RETURN",
            "TYPE_CODE",
            "TYPE_TEXT",
        ],
    )


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


def _child_affected_encounter_count(checkpoint: str, mssql: MSSQLResource) -> int:
    """Distinct encounters the child-affected delta would re-emit."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(child_affected_encounter_count_sql(checkpoint))
            row = cursor.fetchone()
        finally:
            cursor.close()
    return int(row[0]) if row and row[0] is not None else 0


def _guard_child_affected_delta(
    context: dg.AssetExecutionContext,
    checkpoint: str | None,
    config: EncounterDfmConfig,
    mssql: MSSQLResource,
    child_watermark: str | None = None,
    child_watermark_mode: str | None = None,
) -> None:
    """Afvis et child-delta, der i virkeligheden er efterslaebet.

    Unionen i ``_build_encounter_query`` traekker enhver kontakt ind, hvis
    sengeophold eller fravaer er aendret efter child-vandmaerket. Uden
    vandmaerke er det hele historikken; med vandmaerke er det normalt nogle
    tusinde om dagen. Over loftet er child-arkiverne flyttet langt mere end en
    nats churn -- typisk en genindlaesning -- og saa er det efterslaebets job,
    ikke deltaets. Fejl tidligt med tallet og vejen videre.

    Undtagelsen er en BEVIDST nulstilling til epoken: er vandmaerket sat til
    1900 med meta.usp_dfm_reset (last_mode = 'reset'), skal hele historikken
    med i det ordinaere delta, og et genindlaest arkiv kan ikke laegge mere
    til. En delvis nulstilling (fx et par dage tilbage) faar INGEN undtagelse:
    genindlaeses arkivet, foer den koeres, ville den ellers genposte hele
    historikken -- praecis det, loftet skal stoppe. Deltaet saetter
    vandmaerket med last_mode = delta bagefter, og saa gaelder loftet igen.
    """
    if config.mode != "delta" or not checkpoint:
        return
    if not child_watermark:
        # Intet vandmaerke: efterslaebet er ikke afviklet, saa hele historikken
        # staar som uhaandteret. Deltaet maa ikke tage den -- det er hele
        # grunden til, at efterslaebet er en selvstaendig operation.
        samlet = _child_affected_encounter_count(_CHILD_EPOCH, mssql)
        raise dg.Failure(
            description=(
                f"Child-deltaet har intet vandmaerke endnu: {samlet:,} kontakter "
                "staar som uhaandterede. Vandmaerket saettes af migrationen "
                "20260929_fhir_encounter_child_delta_reset, som full_infrastructure_setup "
                "koerer ved deploy -- er den koert, tager naeste ordinaere koersel "
                "hele historikken med. Alternativt: dfm_reset med "
                f"schema_name=fhir_server, table_name={CHILD_DELTA_WATERMARK_SUBJECT}, "
                f"new_value='{_CHILD_EPOCH}'. Runbooken staar i "
                "data_lineage/fhir/fhir_encounter_bed_space.yml."
            ),
        )
    affected = _child_affected_encounter_count(child_watermark, mssql)
    if child_watermark_mode == "reset" and _er_epoke(child_watermark):
        context.log.info(
            f"Child-vandmaerket er nulstillet til {child_watermark} med "
            f"meta.usp_dfm_reset (last_mode=reset): {affected:,} kontakter beroert "
            "via sengeophold/fravaer tages med i deltaet. Loftet gaelder ikke en "
            "bevidst nulstilling; det gaelder igen fra naeste koersel."
        )
        return
    if affected <= CHILD_TOUCHED_ENCOUNTERS_CEILING:
        context.log.info(
            f"{affected:,} kontakter beroert via sengeophold/fravaer siden "
            f"child-vandmaerket {child_watermark} -- tages med i deltaet"
        )
        return
    raise dg.Failure(
        description=(
            f"{affected:,} kontakter er beroert via sengeophold/fravaer siden "
            f"child-vandmaerket {child_watermark}; loftet er "
            f"{CHILD_TOUCHED_ENCOUNTERS_CEILING:,}. Det er ikke Encounter-deltaet, "
            "der er for stort -- det er child-arkiverne, der har flyttet sig langt "
            "mere end en nats churn, typisk fordi de er genindlaest (archive_merge "
            "staempler dw_timestamp_update = SYSDATETIME() ved INSERT). Er det "
            "tilfaeldet og skal historikken med, saa goer det bevidst: dfm_reset med "
            f"schema_name=fhir_server, table_name={CHILD_DELTA_WATERMARK_SUBJECT}, "
            f"new_value='{_CHILD_EPOCH}' -- en nulstilling passerer vaernet een gang. "
            "Er det en planlagt bulk, kan loftet ogsaa haeves med "
            "FHIR_ENCOUNTER_CHILD_TOUCHED_CEILING. "
            "Runbook: data_lineage/fhir/fhir_encounter_bed_space.yml."
        ),
    )


def _er_epoke(vaerdi: str | None) -> bool:
    """Er vandmaerket nulstillet helt tilbage til epoken (1900-01-01)?

    Taaler de former, usp_dfm_reset kan faa: med og uden klokkeslaet og
    broekdele af sekunder.
    """
    if not vaerdi:
        return False
    try:
        return datetime.fromisoformat(str(vaerdi).strip()[:19]) == datetime(1900, 1, 1)
    except ValueError:
        return False


def _skal_rykke_child_vandmaerke(
    config: EncounterDfmConfig, total_fetched: int, child_watermark_mode: str | None
) -> bool:
    """Maa child-vandmaerket rykke efter denne koersel?

    Kun efter en koersel, der har laest child-raekkerne: delta-mode laegger
    child-praedikatet paa, og full_load tager alt. En ids- eller period-koersel
    laeser dem ikke, saa den maa hverken flytte vandmaerket eller bruge en
    nulstilling op. Inden for de to: rykke, naar der er hentet noget -- og efter
    en nulstilling ogsaa ved nul hentede, ellers bliver last_mode staaende paa
    'reset', og undtagelsen i vaernet gaelder ogsaa naeste nat.
    """
    if not config.should_advance_checkpoint or config.mode not in ("delta", "full_load"):
        return False
    return total_fetched > 0 or child_watermark_mode == "reset"


def _child_watermark_mode(mssql: MSSQLResource) -> str | None:
    """last_mode paa child-vandmaerket. 'reset', naar det er sat med meta.usp_dfm_reset.

    DfmManager.get_checkpoint giver kun vaerdien; om den er en bevidst
    nulstilling eller et almindeligt fremrykket vandmaerke, staar i last_mode.
    """
    sql = (
        f"SELECT [last_mode] FROM {DfmManager.STATE_TABLE} "
        "WHERE [scope] = ? AND [target_type] = ? AND [schema_name] = ? AND [table_name] = ?"
    )
    noegle = (
        "staging",
        SCHEMA_TO_TARGET_TYPE["fhir_server"],
        "fhir_server",
        CHILD_DELTA_WATERMARK_SUBJECT,
    )
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, noegle)
            row = cursor.fetchone()
        finally:
            cursor.close()
    return str(row[0]) if row and row[0] is not None else None


def _scalar(mssql: MSSQLResource, sql: str) -> Any:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            row = cursor.fetchone()
            return row[0] if row else None
        finally:
            cursor.close()


def _child_backfill_marker(mssql: MSSQLResource) -> tuple[str, str | None]:
    """Efterslaebets tilstand i dette miljoe: (status, markoerens vaerdi).

    DfmManager.get_checkpoint kan ikke skelne "raekken findes ikke" fra
    "raekken findes med NULL", og forskellen er hele pointen her: raekken
    indsaettes af migrationen 20260911_fhir_encounter_child_backfill, saa dens
    tilstedevaerelse ER miljoeets svar paa "skal efterslaebet koeres her?".
    Findes den ikke, er migrationen ikke anvendt, og saa skal koerslen ikke
    gaette sig til et ja.

    Status er "ikke_armeret" | "afventer" | "undervejs" | "afviklet".
    """
    sql = (
        "SELECT [checkpoint_value] FROM [meta].[DFMState] "
        "WHERE [scope] = 'staging' AND [target_type] = 'fhir_resource' "
        "AND [schema_name] = 'fhir_server' AND [table_name] = ?"
    )
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, (CHILD_BACKFILL_SUBJECT,))
            row = cursor.fetchone()
        finally:
            cursor.close()
    if row is None:
        return "ikke_armeret", None
    value = row[0]
    if value is None or str(value).strip() == "":
        return "afventer", None
    if str(value) == CHILD_BACKFILL_DONE:
        return "afviklet", CHILD_BACKFILL_DONE
    return "undervejs", str(value)


def _child_backfill_slice_ids(
    mssql: MSSQLResource, cursor_value: str | None, slice_size: int
) -> list[str]:
    """OBJECT_ID'erne i naeste skive, i noeglerraekkefoelge."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(child_backfill_slice_sql(cursor_value, slice_size))
            return [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
        finally:
            cursor.close()


def _build_child_backfill_query(object_ids: list[str]) -> str:
    """Hent netop skivens kontakter fra arkivet, slettede inkluderet.

    include_deleted=True af samme grund som deltaet: en soft-slettet kontakt
    skal stadig tomb-stones, ikke udelades.
    """
    escaped = ", ".join("'" + oid.replace("'", "''") + "'" for oid in object_ids)
    return "SELECT * FROM [arc].[ifdb_encounter] " f"WHERE 1=1 AND [OBJECT_ID] IN ({escaped})"


def _build_encounter_query(
    checkpoint: str | None,
    config: RunModeConfig,
    child_watermark: str | None = None,
) -> str:
    """Query arc.ifdb_encounter, including soft-deleted rows.

    include_deleted=True so a soft-deleted encounter (DELETED=1 → archive
    dw_is_deleted=1) still reaches the transform and is tomb-stoned (isDeleted
    extension + status=entered-in-error). Filtering dw_is_deleted=0 here would
    leave deleted encounters active in Aidbox forever — the source delete would
    silently never propagate.

    In delta mode the query also unions OBJECT_IDs from bed-space history and
    absence whose archive timestamp moved — "berørte kontakter". Den union
    måler på ``child_watermark``, IKKE på Encounter-checkpointet: efterslæbet
    stempler vandmærket, når det er afviklet, og først derfra er child-deltaet
    fremadrettet. Er vandmærket ikke sat, matcher unionen hele historikken —
    og det er netop det, ``_guard_child_affected_delta`` afviser. The LOCATION
    child-delta hole is left as a separate task.
    """
    child_predicate = None
    if config.mode == "delta" and checkpoint and child_watermark:
        child_predicate = f"[OBJECT_ID] IN ({child_affected_encounter_ids_sql(child_watermark)})"
    return build_fhir_archive_query(
        "arc.ifdb_encounter",
        checkpoint,
        config,
        include_deleted=True,
        extra_or_predicate=child_predicate,
    )


def _run_child_backfill(
    *,
    context: dg.AssetExecutionContext,
    dfm: DfmManager,
    config: "EncounterDfmConfig",
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    patient_cpr_fallback: Any,
) -> dict[str, Any]:
    """Afvikl efterslaebet, skive for skive, til det er faerdigt.

    Én start skal kunne koere det hele: en runbook, der siger "start den 37
    gange", bliver ikke fulgt korrekt klokken to om natten.
    ``child_backfill_max_slices`` afgraenser koerslen, hvis man vil maale én
    skive foerst; 0 betyder "koer til faerdig".

    Markoeren staemples efter HVER skive, ikke til sidst. Et nedbrud midtvejs
    koster derfor hoejst den skive, der var i gang -- uanset om koerslen tog
    én skive eller alle.
    """
    if config.mode in ("ids", "period"):
        raise dg.Failure(
            description=(
                f"child_affected_backfill kan ikke kombineres med mode={config.mode!r}. "
                "Efterslaebet vaelger selv sine noegler ud fra markoeren i "
                "meta.DFMState, saa mode-filtrene bliver aldrig laest. Start "
                "koerslen med child_affected_backfill=True alene."
            )
        )

    if config.child_backfill_restart:
        # Eksplicit NULL. set_checkpoint(value=None) betyder "nu" (SYSDATETIME)
        # og efterlod et tidsstempel som markoer, saa gennemloebet begyndte
        # midt i noegleordningen (efter '2026-...') i stedet for forfra.
        dfm.clear_checkpoint("fhir_server", CHILD_BACKFILL_SUBJECT, mode="ids")
        context.log.warning("Efterslaebets markoer nulstillet -- begynder forfra")

    status, cursor_value = _child_backfill_marker(mssql)
    if status == "ikke_armeret":
        raise dg.Failure(
            description=(
                "Efterslaebet er ikke armeret i dette miljoe: der er ingen "
                f"{CHILD_BACKFILL_SUBJECT}-raekke i meta.DFMState. Den indsaettes af "
                "migrationen 20260911_fhir_encounter_child_backfill, som koeres af "
                "staging_migrations ved deploy. Anvend migrationen foerst -- saa er "
                "det migrationshistorikken, og ikke en manuel beslutning, der "
                "afgoer, om miljoeet skal have efterslaebet."
            )
        )
    if status == "afviklet":
        context.log.info("Efterslaebet er allerede afviklet i dette miljoe. Koerslen er en no-op.")
        return {"backfill_status": "allerede afviklet", "posted_count": 0, "fetched_count": 0}

    # Vandmaerket tages FOER foerste skive -- ikke efter sidste. Markoeren
    # vandrer i OBJECT_ID-raekkefoelge; bliver en child-raekke for en noegle,
    # markoeren allerede er passeret, aendret mens efterslaebet koerer (fx af
    # den natlige arkivkoersel), saa saa efterslaebet den ikke. Et vandmaerke
    # taget ved afslutningen (T1) ville lade naeste delta se kun > T1, og
    # aendringen var tabt. Taget ved starten (T0) matcher deltaet stadig kun
    # aegte nye aendringer -- arkivets bulk-load ligger foer T0 -- men uden hul.
    # Sidegevinst: deltaet er IKKE spaerret, mens efterslaebet koerer.
    #
    # "Hvis ikke sat": et forudgaaende full_load kan have sat det tidligere,
    # og det er fint (en delmaengde mere at se paa, aldrig en mindre). Og en
    # genoptaget koersel maa ikke flytte det frem -- saa var hullet tilbage.
    if not dfm.get_checkpoint("fhir_server", CHILD_DELTA_WATERMARK_SUBJECT):
        t0 = _scalar(mssql, "SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
        if t0:
            dfm.set_checkpoint(
                "fhir_server", CHILD_DELTA_WATERMARK_SUBJECT, value=str(t0), mode="ids"
            )
            context.log.info(
                f"Child-deltaets vandmaerke sat til {t0!r} ved efterslaebets start. "
                "Den natlige delta ser herfra kun aendringer efter dette tidspunkt "
                "og er ikke spaerret, mens efterslaebet koerer."
            )

    resterende_ved_start = int(_scalar(mssql, child_backfill_remaining_sql(cursor_value)) or 0)
    loft = config.child_backfill_max_slices
    context.log.info(
        f"Efterslaeb starter: {resterende_ved_start:,} kontakter tilbage, "
        f"skive={config.child_backfill_slice}, "
        f"loft={'ingen (koerer til faerdig)' if loft < 1 else loft}"
    )

    skiver = 0
    i_alt_posted = 0
    i_alt_hentet = 0
    sidste_cursor = cursor_value
    start = time.monotonic()

    while loft < 1 or skiver < loft:
        ids = _child_backfill_slice_ids(mssql, sidste_cursor, config.child_backfill_slice)
        if not ids:
            # Kun DONE her. Vandmaerket blev sat ved starten og er maaske
            # allerede rykket videre af natlige delta-koersler undervejs; det
            # maa ikke saettes tilbage eller frem nu.
            dfm.set_checkpoint(
                "fhir_server", CHILD_BACKFILL_SUBJECT, value=CHILD_BACKFILL_DONE, mode="ids"
            )
            minutter = (time.monotonic() - start) / 60
            context.log.info(
                f"Efterslaebet er faerdigt: {i_alt_posted:,} kontakter re-emitteret i "
                f"{skiver} skiver paa {minutter:.1f} min. Markoeren er staemplet "
                f"{CHILD_BACKFILL_DONE}; fremtidige koersler er no-ops."
            )
            return {
                "backfill_status": "faerdig",
                "backfill_skiver": skiver,
                "backfill_minutter": round(minutter, 1),
                "posted_count": i_alt_posted,
                "fetched_count": i_alt_hentet,
            }

        skive_start = time.monotonic()
        hentet, posted, _, _ = _process_encounter_query(
            query=_build_child_backfill_query(ids),
            context=context,
            mssql=mssql,
            fhir_database=fhir_database,
            patient_cpr_fallback=patient_cpr_fallback,
        )
        dfm.set_checkpoint("fhir_server", CHILD_BACKFILL_SUBJECT, value=ids[-1], mode="ids")

        skiver += 1
        i_alt_posted += posted
        i_alt_hentet += hentet
        sidste_cursor = ids[-1]
        resterende = max(resterende_ved_start - i_alt_hentet, 0)
        context.log.info(
            f"Skive {skiver}: {posted} posted paa "
            f"{time.monotonic() - skive_start:.0f}s, ca. {resterende:,} tilbage"
        )

    minutter = (time.monotonic() - start) / 60
    context.log.info(
        f"Loftet paa {loft} skiver er naaet: {i_alt_posted:,} kontakter re-emitteret paa "
        f"{minutter:.1f} min. Start igen for at fortsaette."
    )
    return {
        "backfill_status": "loft naaet",
        "backfill_skiver": skiver,
        "backfill_minutter": round(minutter, 1),
        "backfill_cursor_til": sidste_cursor or "(start)",
        "backfill_resterende": max(resterende_ved_start - i_alt_hentet, 0),
        "posted_count": i_alt_posted,
        "fetched_count": i_alt_hentet,
    }


def _process_encounter_query(
    *,
    query: str,
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    patient_cpr_fallback: Any,
) -> tuple[int, int, int, int]:
    """Koer én forespoergsel igennem transform og post, batch for batch.

    Udtrukket af assetet, fordi efterslaebet koerer den samme behandling én
    gang pr. skive. Returnerer (hentet, posted, batches, stoerste
    fravaersliste) -- taellere assetets metadata samler op paa tvaers af
    skiver.
    """
    total_fetched = 0
    total_posted = 0
    batch_num = 0
    max_absence_count = 0

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]

            while True:
                raw_rows = cursor.fetchmany(BATCH_SIZE)
                if not raw_rows:
                    break

                batch_num += 1
                batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                total_fetched += len(batch)

                context.log.info(
                    f"Batch {batch_num}: processing {len(batch)} encounters "
                    f"(total so far: {total_fetched})"
                )

                encounter_ids = [str(row["OBJECT_ID"]) for row in batch if row.get("OBJECT_ID")]
                location_data = _fetch_location_data_for_batch(encounter_ids, mssql)
                bed_data = _fetch_bed_space_data_for_batch(encounter_ids, mssql)
                absence_data = _fetch_absence_data_for_batch(encounter_ids, mssql)
                batch_max_absence = max((len(rows) for rows in absence_data.values()), default=0)
                if batch_max_absence > max_absence_count:
                    max_absence_count = batch_max_absence

                encounters = _transform_encounters_to_fhir(
                    batch,
                    location_data,
                    context,
                    beds_by_encounter=bed_data,
                    absences_by_encounter=absence_data,
                )

                if encounters:
                    _post_encounters_to_fhir(
                        context=context,
                        encounters=encounters,
                        fhir_database=fhir_database,
                        extracted_count=len(batch),
                        mssql=None,
                        resolve_missing_dependencies=False,
                        reference_fallback_fn=patient_cpr_fallback,
                    )
                    total_posted += len(encounters)
        finally:
            cursor.close()

    return total_fetched, total_posted, batch_num, max_absence_count


@dg.asset(
    name="fhir_server_encounter_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_encounter_ifdb",
        "arc_location_ifdb",
        "arc_encounter_bed_space_history_ifdb",
        "arc_absence_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_eoc_dfm",
        "fhir_server_location_dfm",
        "fhir_server_location_bed_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch encounters from arc.ifdb_encounter in batches, transform to FHIR, "
        "and post to FHIR server. Memory-bounded streaming — never loads full "
        "archive into memory. Checkpoint owner for fhir_server.Encounter."
    ),
)
def fhir_server_encounter_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: EncounterDfmConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Encounter resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Encounter",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        # ----------------------------------------------------------------
        # Efterslaebet: engangsoperation pr. miljoe, koert i skiver.
        # ----------------------------------------------------------------
        if config.child_affected_backfill:
            # ----------------------------------------------------------------
            # Efterslaebet: engangsoperation pr. miljoe, koert i skiver.
            # Standard er at koere til den er faerdig -- én start, ikke 37.
            # Markoeren staemples efter HVER skive, saa et nedbrud midtvejs
            # hoejst koster den skive, der var i gang.
            # ----------------------------------------------------------------
            resultat = _run_child_backfill(
                context=context,
                dfm=dfm,
                config=config,
                mssql=mssql_target_staging,
                fhir_database=fhir_database,
                patient_cpr_fallback=patient_cpr_fallback,
            )
            dfm.complete_dfm_run(
                run_ctx.log_id,
                checkpoint_after=None,
                rows_affected=int(resultat.get("posted_count", 0)),
            )
            return dg.MaterializeResult(metadata=resultat)

        child_watermark = dfm.get_checkpoint("fhir_server", CHILD_DELTA_WATERMARK_SUBJECT)
        child_watermark_mode = (
            _child_watermark_mode(mssql_target_staging) if child_watermark else None
        )
        _guard_child_affected_delta(
            context,
            run_ctx.checkpoint,
            config,
            mssql_target_staging,
            child_watermark=child_watermark,
            child_watermark_mode=child_watermark_mode,
        )
        query = _build_encounter_query(run_ctx.checkpoint, config, child_watermark=child_watermark)
        context.log.info(
            f"Querying arc.ifdb_encounter (mode={config.mode}, "
            f"child-vandmaerke={child_watermark or '(ikke sat)'})"
        )
        total_fetched, total_posted, batch_num, max_absence_count = _process_encounter_query(
            query=query,
            context=context,
            mssql=mssql_target_staging,
            fhir_database=fhir_database,
            patient_cpr_fallback=patient_cpr_fallback,
        )

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches; "
            f"max absence periods on one encounter in this run: {max_absence_count}"
        )

        new_checkpoint = advance_fhir_checkpoint(dfm, run_ctx, "Encounter", config, total_fetched)
        if _skal_rykke_child_vandmaerke(config, total_fetched, child_watermark_mode):
            # Child-vandmaerket foelger med: ellers ville naeste koersel matche
            # de samme child-raekker igen, og deltaet ville aldrig blive mindre.
            # Samme sikre punkt som checkpointet -- laesningens start, holdt
            # tilbage til en arkiv-MERGE, der stadig koerer -- ogsaa naar der
            # intet var at hente efter en nulstilling.
            vaerdi = new_checkpoint or dfm.safe_reader_checkpoint(
                getattr(run_ctx, "started_at", None)
            )
            if vaerdi:
                dfm.set_checkpoint(
                    "fhir_server",
                    CHILD_DELTA_WATERMARK_SUBJECT,
                    value=vaerdi,
                    mode=config.mode,
                )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "max_absence_count": max_absence_count,
                "mode": config.mode,
                "child_watermark_reset": child_watermark_mode == "reset",
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
