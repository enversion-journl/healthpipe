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

from collections import defaultdict
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id, parse_clob_datetime
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig, ResolveValuesFn
from DSA.FHIR.utils.status_mapping import (
    map_encounter_location_status,
    map_encounter_status,
)
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.encounter import Encounter, EncounterLocation
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

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
) -> list[Encounter]:
    """
    Transform encounter rows to FHIR Encounter resources.

    Creates Encounter.location entries with:
    - location: Reference using SYSTEM_SOR and LOCATION_SORCODE (stripped of .0 suffix)
    - status: Required field derived from encounter status and period
    - period: Time period at this location
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
            system="http://terminology.hl7.org/CodeSystem/v3-ActCode",
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

        extensions = [build_is_deleted_extension(deleted_value)]

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
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_episodes_of_care,
            resolve_missing_patients,
        )

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
            resolve_missing_patients(
                patient_ids=patient_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
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


def _fetch_location_data_for_batch(
    encounter_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch location data from arc.ifdb_location for a batch of encounter IDs."""
    locations_by_encounter: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not encounter_ids:
        return locations_by_encounter

    chunk_size = 999
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for i in range(0, len(encounter_ids), chunk_size):
                chunk = encounter_ids[i : i + chunk_size]
                placeholders = ", ".join(f"'{eid}'" for eid in chunk)
                query = (
                    "SELECT [OBJECT_ID], [LOCATION_SORCODE], "
                    "[PERIOD_START], [PERIOD_END] "
                    "FROM [arc].[ifdb_location] "
                    "WHERE [dw_is_deleted] = 0 "
                    "AND [LOCATION_SORCODE] IS NOT NULL "
                    f"AND [OBJECT_ID] IN ({placeholders})"
                )
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    row_dict = dict(zip(columns, row, strict=False))
                    locations_by_encounter[str(row_dict["OBJECT_ID"])].append(row_dict)
        finally:
            cursor.close()

    return locations_by_encounter


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


def _build_encounter_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Query arc.ifdb_encounter, including soft-deleted rows.

    include_deleted=True so a soft-deleted encounter (DELETED=1 → archive
    dw_is_deleted=1) still reaches the transform and is tomb-stoned (isDeleted
    extension + status=entered-in-error). Filtering dw_is_deleted=0 here would
    leave deleted encounters active in Aidbox forever — the source delete would
    silently never propagate.
    """
    return build_fhir_archive_query("arc.ifdb_encounter", checkpoint, config, include_deleted=True)


@dg.asset(
    name="fhir_server_encounter_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_encounter_ifdb",
        "arc_location_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_eoc_dfm",
        "fhir_server_location_dfm",
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
    config: RunModeConfig,
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
        query = _build_encounter_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying arc.ifdb_encounter (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
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
                    location_data = _fetch_location_data_for_batch(
                        encounter_ids, mssql_target_staging
                    )

                    encounters = _transform_encounters_to_fhir(batch, location_data, context)

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

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted " f"in {batch_num} batches"
        )

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint and total_fetched > 0:
            with mssql_target_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row else None
                finally:
                    cursor.close()

            if new_checkpoint:
                dfm.set_checkpoint(
                    "fhir_server", "Encounter", value=new_checkpoint, mode=config.mode
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
                "mode": config.mode,
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
