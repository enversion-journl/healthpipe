# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR EpisodeOfCare asset using DFM (Data Flow Management).

Single-asset pattern: fetches from arc.ifdb_episode_of_care in batches,
transforms to FHIR, and posts to the FHIR server — all within one asset
to keep memory bounded.

This module also owns the shared EpisodeOfCare transform/post helpers (used by
the dependency resolver and the quality-check asset), which lived in
EpisodeOfCare_IFDB.py before the partitioned assets were removed.

Self-references (previousEpisodeOfCare) are resolved from the FHIR server
after the initial batch insert (2-pass). To make resolution robust regardless
of batch order, two safeguards are applied:

  A. The archive is streamed ordered by OBJECT_ID, so a referenced ("previous")
     episode — which has a lower OBJECT_ID — is normally processed in an
     earlier/same batch than the referencing one.
  B. Before posting a batch, any referenced previous-episode that is not yet in
     FHIR is fetched from the archive and posted first, so the in-batch 2-pass
     resolution always finds its target.

A companion quality asset (EpisodeOfCare_QualityCheck) reconciles any residual
gaps. No Oracle dependency is needed since the archive contains all records.

No Dagster partitions — checkpoint tracking replaces partition boundaries.
"""

import time
from datetime import UTC, datetime
from typing import Any, cast

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id, parse_clob_datetime
from DSA.FHIR.utils.reference_resolver import (
    ReferenceFieldConfig,
    ResolveValuesFn,
    batch_resolve_references,
)
from DSA.FHIR.utils.status_mapping import map_episode_of_care_status
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_DISCHARGE_DISPOSITION,
    EXTENSION_IFDB_HEALTHISSUE_CODE,
    EXTENSION_PREVIOUS_EOC_REF_TYPE,
    EXTENSION_PREVIOUS_EPISODE_OF_CARE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_HEALTHISSUE_CODE,
    SYSTEM_IFDB_HEALTHISSUE_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000
_PREV_FETCH_BATCH = 1000
_HEALTHISSUE_LOOKUP_BATCH = 1000

# =============================================================================
# Reference Resolution Configuration
# =============================================================================

# Top-level reference fields
EPISODE_OF_CARE_REFERENCE_CONFIGS = [
    # Patient reference (REQUIRED) - field is "patient" for EpisodeOfCare
    ReferenceFieldConfig(
        field_name="patient",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    # Managing Organization reference (OPTIONAL) - via SOR code
    ReferenceFieldConfig(
        field_name="managingOrganization",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Managing organization from IFDB MANAGING_ORGANIZATION_SORCODE",
    ),
]


# ============================================================================
# Shared Helper Functions
# ============================================================================


def _is_resolvable_eoc_id(raw: Any) -> bool:
    """True if an EpisodeOfCare id is a plain integer.

    IFDB OBJECT_IDs are numeric, so a resolvable previous-episode reference is
    always a digit string. Some IFDB ``PREVIOUS_EOC_ID`` values are hashed hex
    strings that point to no real episode and can never resolve; skipping them
    means the shared reference resolver is never asked to look them up (avoids
    wasted lookups + retries that would only be dropped afterwards).
    """
    normalized = normalize_oracle_id(raw)
    return normalized is not None and normalized.isdigit()


def _build_healthissue_code_map(
    rows: list[dict[str, Any]],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, tuple[str | None, str | None]]:
    """Look up HEALTHISSUE_CODE/_TEXT in arc.ifdb_healthissue for a batch of EOC rows.

    Collects the batch's distinct (normalized) HEALTHISSUE_IDs and fetches
    ``{OBJECT_ID: (HEALTHISSUE_CODE, HEALTHISSUE_TEXT)}`` from the lookup
    archive. Deleted rows are included so a soft-deleted health issue still
    classifies its episodes (the code stays the source of truth for the type).
    """
    hi_ids: set[str] = set()
    for row in rows:
        normalized = normalize_oracle_id(row.get("HEALTHISSUE_ID"))
        if normalized:
            hi_ids.add(normalized)
    if not hi_ids:
        return {}

    code_map: dict[str, tuple[str | None, str | None]] = {}
    hi_id_list = sorted(hi_ids)
    for i in range(0, len(hi_id_list), _HEALTHISSUE_LOOKUP_BATCH):
        sub = hi_id_list[i : i + _HEALTHISSUE_LOOKUP_BATCH]
        sql = build_fhir_archive_query(
            "arc.ifdb_healthissue",
            None,
            RunModeConfig(mode="ids", id_filter_column="OBJECT_ID", id_filter_values=sub),
            include_deleted=True,
            select_clause="SELECT [OBJECT_ID], [HEALTHISSUE_CODE], [HEALTHISSUE_TEXT]",
        )
        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                for object_id, code, text in cursor.fetchall():
                    key = normalize_oracle_id(object_id)
                    if key:
                        code_map[key] = (code, text)
            finally:
                cursor.close()

    missing = len(hi_ids) - len(code_map)
    if missing:
        context.log.warning(
            f"arc.ifdb_healthissue lookup: {missing} of {len(hi_ids)} HEALTHISSUE_IDs "
            f"not found — healthissue_code extension omitted for their episodes"
        )
    return code_map


def _transform_episodes_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    healthissue_code_map: dict[str, tuple[str | None, str | None]] | None = None,
) -> list[EpisodeOfCare]:
    """
    Transform episode of care rows to FHIR EpisodeOfCare resources.

    Aligned with fhir.ifdb.yml model specification:
    - Required: episode_id (OBJECT_ID), patient_id (PATIENT_ID), status (STATUS)
    - Core: period_start, period_end, managingOrganization (via MANAGING_ORGANIZATION_SORCODE)
    - Extension: previous_episode_id (PREVIOUS_EOC_ID), previous_eoc_ref_type_code,
      healthissue_id (HEALTHISSUE_ID), healthissue_code (valueCoding, looked up in
      arc.ifdb_healthissue via ``healthissue_code_map``), discharge_disposition_code
    """
    episodes: list[EpisodeOfCare] = []

    for row_dict in rows:
        object_id = row_dict.get("OBJECT_ID")
        patient_id = row_dict.get("PATIENT_ID")
        status = row_dict.get("STATUS")
        period_start = row_dict.get("PERIOD_START")
        period_end = row_dict.get("PERIOD_END")
        previous_eoc_id = row_dict.get("PREVIOUS_EOC_ID")
        previous_eoc_ref_type_code = row_dict.get("PREVIOUS_EOC_REF_TYPE_CODE")
        healthissue_id = row_dict.get("HEALTHISSUE_ID")
        managing_org_sorcode = row_dict.get("MANAGING_ORGANIZATION_SORCODE")
        discharge_disposition_code = row_dict.get("DISCHARGE_DISPOSITION_CODE")
        raw_deleted = row_dict.get("DELETED")
        deleted_value = int(is_true_flag(raw_deleted))
        is_deleted_flag = deleted_value != 0

        eoc_identifier = Identifier(
            system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            value=normalize_oracle_id(object_id),
        )

        patient_ref = Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_PATIENT_ID,
                value=normalize_oracle_id(patient_id),
            )
        )

        # Create managing organization reference via SOR code (resolved later)
        managing_org_ref = None
        if managing_org_sorcode:
            managing_org_ref = Reference(
                identifier=Identifier(
                    system=SYSTEM_SOR,
                    value=str(managing_org_sorcode),
                )
            )

        # Parse period_start with CLOB date parsing
        period_start_fhir = None
        period_start_dt = None
        if period_start:
            try:
                parsed_dt = parse_clob_datetime(period_start)
                if parsed_dt:
                    period_start_fhir = fhir_datetime(parsed_dt)
                    period_start_dt = parsed_dt
                else:
                    period_start_fhir = fhir_datetime(period_start)
                    period_start_dt = None
            except Exception as e:
                raise ValueError(f"Failed to parse period_start '{period_start}': {e}") from e

        # Parse period_end with CLOB date parsing
        period_end_fhir = None
        period_end_dt = None
        if period_end:
            try:
                parsed_dt = parse_clob_datetime(period_end)
                if parsed_dt:
                    period_end_fhir = fhir_datetime(parsed_dt)
                    period_end_dt = parsed_dt
                else:
                    period_end_fhir = fhir_datetime(period_end)
                    period_end_dt = None
            except Exception as e:
                raise ValueError(f"Failed to parse period_end '{period_end}': {e}") from e

        # Map status to FHIR EpisodeOfCareStatus (IFDB soft-delete -> entered-in-error)
        eoc_status = map_episode_of_care_status(
            status,
            period_start_dt,
            period_end_dt,
            is_deleted=is_deleted_flag,
        )

        # Create period
        period = Period(start=period_start_fhir, end=period_end_fhir)

        # Create extension for previous episode of care linkage if available.
        # Only emit the reference when the id is a plain integer; some IFDB
        # PREVIOUS_EOC_ID values are hashed hex strings that reference no real
        # episode and can never resolve — emitting them would waste resolver
        # lookups/retries and the extension would be dropped afterwards anyway.
        extensions = []
        if previous_eoc_id and _is_resolvable_eoc_id(previous_eoc_id):
            extensions.append(
                Extension(
                    url=EXTENSION_PREVIOUS_EPISODE_OF_CARE,
                    valueReference=Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            value=normalize_oracle_id(previous_eoc_id),
                        )
                    ),
                )
            )

        if previous_eoc_ref_type_code:
            extensions.append(
                Extension(
                    url=EXTENSION_PREVIOUS_EOC_REF_TYPE,
                    valueString=str(previous_eoc_ref_type_code),
                )
            )

        # Create healthissue_id extension without FHIR Reference
        if healthissue_id:
            extensions.append(
                Extension(
                    url=SYSTEM_IFDB_HEALTHISSUE_ID,
                    valueString=normalize_oracle_id(healthissue_id),
                )
            )

            # Forløbstype: HEALTHISSUE_CODE looked up in
            # arc.ifdb_healthissue. valueCoding so the system URI carries the
            # code-system provenance and display (HEALTHISSUE_TEXT) is kept.
            # Missing lookup → extension omitted (warned in map builder).
            if healthissue_code_map:
                hi_lookup = healthissue_code_map.get(cast(str, normalize_oracle_id(healthissue_id)))
                if hi_lookup and hi_lookup[0]:
                    hi_code, hi_text = hi_lookup
                    extensions.append(
                        Extension(
                            url=EXTENSION_IFDB_HEALTHISSUE_CODE,
                            valueCoding=Coding(
                                system=SYSTEM_IFDB_HEALTHISSUE_CODE,
                                code=hi_code,
                                display=hi_text,
                            ),
                        )
                    )

        if discharge_disposition_code:
            extensions.append(
                Extension(
                    url=EXTENSION_DISCHARGE_DISPOSITION,
                    valueString=str(discharge_disposition_code),
                )
            )

        extensions.append(build_is_deleted_extension(deleted_value))

        episode = EpisodeOfCare(
            identifier=[eoc_identifier],
            status=eoc_status,
            patient=patient_ref,
            managingOrganization=managing_org_ref,
            period=period,
            extension=extensions if extensions else None,
        )

        episodes.append(episode)

    return episodes


def _resolve_self_references_with_retry(
    episodes: list[EpisodeOfCare],
    fhir_database: FHIRDatabaseResource,
    mssql: MSSQLResource | None = None,
    context: dg.AssetExecutionContext | None = None,
    max_attempts: int = 3,
    initial_delay: float = 0.5,
) -> None:
    """
    Resolve previousEpisodeOfCare self-references with retry logic.

    Since these are self-references, the target resources were just inserted
    in the same batch, so we may need to retry if they're not yet visible.

    If mssql and context are provided, missing referenced EpisodeOfCare
    resources will be fetched from the archive and posted to FHIR before
    attempting resolution. This handles the case where referenced episodes are
    in future partitions (by modification date) but were created earlier.
    """
    logger = dg.get_dagster_logger()

    # Collect previous episode identifiers that need resolution
    prev_episode_identifiers: dict[str, str] = {}
    for eoc in episodes:
        if eoc.extension:
            for ext in eoc.extension:
                if (
                    ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
                    and ext.valueReference
                    and getattr(ext.valueReference, "identifier", None)
                ):
                    identifier_value = str(ext.valueReference.identifier.value)
                    prev_episode_identifiers[identifier_value] = ""

    if not prev_episode_identifiers:
        return

    logger.info(
        f"Resolving {len(prev_episode_identifiers)} unique previous EpisodeOfCare self-references"
    )

    # Resolve missing dependencies if resources provided
    # This handles cases where referenced episodes are in future partitions
    # (by modification date) but were created earlier
    if mssql and context:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_episodes_of_care,
        )

        logger.info(
            f"[DEPENDENCY] Ensuring {len(prev_episode_identifiers)} previous EpisodeOfCare references exist"
        )
        resolve_missing_episodes_of_care(
            eoc_ids=set(prev_episode_identifiers.keys()),
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

    # Try to resolve with retry logic
    resolved_ids: dict[str, str] = {}
    delay = initial_delay

    for attempt in range(1, max_attempts + 1):
        # Resolve identifiers that haven't been resolved yet
        unresolved = {k: "" for k in prev_episode_identifiers if k not in resolved_ids}
        if not unresolved:
            break

        batch_resolved = batch_resolve_references(
            identifiers=unresolved,
            resource_type="EpisodeOfCare",
            identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
            fhir_database=fhir_database,
        )
        resolved_ids.update(batch_resolved)

        still_unresolved = [k for k in prev_episode_identifiers if k not in resolved_ids]
        if not still_unresolved or attempt == max_attempts:
            break

        logger.debug(
            f"Attempt {attempt}: {len(still_unresolved)} still unresolved, retrying in {delay}s"
        )
        time.sleep(delay)
        delay *= 2

    # Apply resolved IDs to extension references
    missing_identifiers = set()
    for eoc in episodes:
        if not eoc.extension:
            continue

        valid_extensions = []
        for ext in eoc.extension:
            if (
                ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
                and ext.valueReference
                and getattr(ext.valueReference, "identifier", None)
            ):
                identifier_value = str(ext.valueReference.identifier.value)
                resolved_id = resolved_ids.get(identifier_value)
                if resolved_id:
                    # Apply resolved ID
                    ext.valueReference.id = resolved_id
                    ext.valueReference.type = "EpisodeOfCare"
                    ext.valueReference.identifier = None
                    valid_extensions.append(ext)
                else:
                    missing_identifiers.add(identifier_value)
            else:
                valid_extensions.append(ext)

        eoc.extension = valid_extensions if valid_extensions else None

    if missing_identifiers:
        logger.warning(
            f"Previous EpisodeOfCare not found for {len(missing_identifiers)} unique identifiers "
            f"- extensions removed. Example IDs: {list(missing_identifiers)[:10]}"
        )

    logger.info(
        f"Resolved {len(resolved_ids)}/{len(prev_episode_identifiers)} previous EpisodeOfCare references"
    )


def _post_episodes_to_fhir(
    context: dg.AssetExecutionContext,
    episodes: list[EpisodeOfCare],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post EpisodeOfCare resources to FHIR database using direct DB insertion.

    Args:
        context: Dagster execution context
        episodes: List of EpisodeOfCare resources to post
        fhir_database: FHIR database resource for direct DB operations
        extracted_count: Number of records extracted (for logging)
        mssql: Staging MSSQL resource for resolving missing patients from the archive
        resolve_missing_dependencies: Whether to resolve missing patient dependencies
        reference_fallback_fn: Optional patient-id → CPR fallback for resolving
            references to soft-deleted PATIENT_IDs whose active CPR twin exists
            in FHIR (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    if not episodes:
        partition_key = context.partition_key if context.has_partition_key else "N/A"
        logger.info(
            f"Summary[{partition_key}]: {extracted_count} ext -> 0 trans -> 0 posted (0 failed)"
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text("ifdb_oracle"),
                "partition_key": dg.MetadataValue.text(partition_key),
                "method": dg.MetadataValue.text("direct_database"),
            }
        )

    # Collect all patient IDs that need to exist
    patient_ids_needed: set[str] = set()
    for eoc in episodes:
        if eoc.patient and getattr(eoc.patient, "identifier", None):
            patient_ids_needed.add(str(eoc.patient.identifier.value))

    if resolve_missing_dependencies and mssql and patient_ids_needed:
        from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients

        logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
        ensure_patients(
            ifdb_patient_ids=patient_ids_needed,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need=NEED_REFERENCE,
        )

    logger.info(f"Using direct database insertion for {len(episodes)} EpisodeOfCare resources")

    episodes_before_resolve = len([ep for ep in episodes if ep.id is not None])
    episodes = prepare_resources_for_upsert(
        resources=episodes,
        fhir_database=fhir_database,
        resource_type="EpisodeOfCare",
        preferred_id_systems=[SYSTEM_IFDB_EPISODE_OF_CARE_ID],
        reference_configs=EPISODE_OF_CARE_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=logger,
    )
    episodes_after_resolve = len([ep for ep in episodes if ep.id is not None])
    resolved_count = episodes_after_resolve - episodes_before_resolve
    new_count = len(episodes) - episodes_after_resolve

    # For direct DB insertion with self-references, we need a 2-pass approach:
    # 1. First insert all records (generates FHIR IDs)
    # 2. Then resolve and update self-references

    # Pass 1: Insert all records
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=episodes,
        resource_type="EpisodeOfCare",
    )

    # Strict mode (Pass 1 only): a resource that could not be persisted must NOT
    # be silently left behind the checkpoint. Raising aborts before checkpoint
    # advancement so the window is reprocessed next run (idempotent upsert).
    # NOTE: this gates only on Pass-1 persistence failures. An unresolvable
    # previousEpisodeOfCare is NOT a failure here — the episode is posted without
    # the reference (extension dropped) and the quality-check asset
    # (fhir_quality_check_eoc_previous_references) reconciles it later. Likewise
    # Pass-2 reference-update failures below are tolerated.
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(episodes)} EpisodeOfCare "
                f"resources (pass 1) — checkpoint NOT advanced. "
                f"Failed IDs: {failed_ids[:10]}"
            )
        )

    # Pass 2: Resolve and update self-references (previousEpisodeOfCare)
    # Now that all records exist, we can resolve self-references
    _resolve_self_references_with_retry(
        episodes=episodes,
        fhir_database=fhir_database,
        mssql=mssql,
        context=context,
    )

    # Update records that have resolved self-references
    episodes_with_refs = [
        ep
        for ep in episodes
        if ep.extension
        and any(
            ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
            and ext.valueReference
            and getattr(ext.valueReference, "id", None)  # Has resolved ID
            for ext in ep.extension
        )
    ]

    if episodes_with_refs:
        logger.info(
            f"Updating {len(episodes_with_refs)} EpisodeOfCare records with resolved self-references"
        )
        ref_successes, ref_failures, ref_failed_ids = fhir_database.batch_upsert_resources(
            resources=episodes_with_refs,
            resource_type="EpisodeOfCare",
        )
        logger.info(f"Reference update: {ref_successes} succeeded, {ref_failures} failed")

    partition_key = context.partition_key if context.has_partition_key else "N/A"

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(successes),
            "failed_count": dg.MetadataValue.int(failures),
            "failed_ids": dg.MetadataValue.json(failed_ids),
            "resolved_count": dg.MetadataValue.int(resolved_count),
            "new_count": dg.MetadataValue.int(new_count),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text("ifdb_oracle"),
            "method": dg.MetadataValue.text("direct_database"),
            "decorated_at": dg.MetadataValue.text(datetime.now(UTC).isoformat()),
        }
    )


# ---------------------------------------------------------------------------
# Self-reference dependency helpers (safeguard B)
# ---------------------------------------------------------------------------


def _collect_previous_eoc_identifiers(episodes) -> set[str]:
    """Collect all previousEpisodeOfCare identifier values from a batch."""
    prev_ids: set[str] = set()
    for eoc in episodes:
        for ext in eoc.extension or []:
            if (
                ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
                and ext.valueReference
                and getattr(ext.valueReference, "identifier", None)
            ):
                prev_ids.add(str(ext.valueReference.identifier.value))
    return prev_ids


def _ensure_previous_episodes_from_archive(
    prev_ids: set[str],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> int:
    """Ensure referenced previous-episodes exist in FHIR.

    Looks up which of ``prev_ids`` are missing in FHIR, fetches those rows from
    ``arc.ifdb_episode_of_care`` (NOT Oracle), transforms and upserts them, so
    the in-batch 2-pass self-reference resolution can find its target.

    Returns the number of dependency episodes posted.
    """
    if not prev_ids:
        return 0

    existing = batch_resolve_references(
        identifiers={pid: "" for pid in prev_ids},
        resource_type="EpisodeOfCare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        fhir_database=fhir_database,
    )
    missing = [pid for pid in prev_ids if pid not in existing]
    if not missing:
        return 0

    context.log.info(
        f"[DEPENDENCY] Fetching {len(missing)} missing previous-EpisodeOfCare from archive"
    )

    posted = 0
    for i in range(0, len(missing), _PREV_FETCH_BATCH):
        sub = missing[i : i + _PREV_FETCH_BATCH]
        sql = build_fhir_archive_query(
            "arc.ifdb_episode_of_care",
            None,
            RunModeConfig(mode="ids", id_filter_column="OBJECT_ID", id_filter_values=sub),
            # Include deleted so a referenced previous-episode that is itself
            # soft-deleted is still posted (as a tomb-stone), letting the
            # self-reference resolve instead of being silently dropped.
            include_deleted=True,
        )
        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                cols = [d[0] for d in cursor.description]
                rows = [dict(zip(cols, r, strict=False)) for r in cursor.fetchall()]
            finally:
                cursor.close()

        if not rows:
            continue

        # Dependency-posted previous-episodes must carry the healthissue_code too.
        dep_hi_map = _build_healthissue_code_map(rows, mssql, context)
        dep_episodes = _transform_episodes_to_fhir(rows, context, healthissue_code_map=dep_hi_map)
        if dep_episodes:
            fhir_database.batch_upsert_resources(
                resources=dep_episodes, resource_type="EpisodeOfCare"
            )
            posted += len(dep_episodes)

    return posted


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


def _build_eoc_query(checkpoint: str | None, config: RunModeConfig) -> str:
    """Query arc.ifdb_episode_of_care, including soft-deleted rows.

    include_deleted=True so a soft-deleted episode (DELETED=1 → archive
    dw_is_deleted=1) still reaches the transform and is tomb-stoned (isDeleted
    extension + status=entered-in-error). Filtering dw_is_deleted=0 here would
    leave deleted episodes active in Aidbox forever — the source delete would
    silently never propagate.

    Ordered by OBJECT_ID (safeguard A) so a referenced previous-episode — which
    has a lower OBJECT_ID — is normally processed in an earlier/same batch.
    """
    return build_fhir_archive_query(
        "arc.ifdb_episode_of_care",
        checkpoint,
        config,
        include_deleted=True,
        order_by="[OBJECT_ID]",
    )


@dg.asset(
    name="fhir_server_eoc_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_episode_of_care_ifdb",
        "arc_healthissue_ifdb",
        "fhir_server_patient_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch EpisodeOfCare from arc.ifdb_episode_of_care in batches, transform "
        "to FHIR, and post to FHIR server. Memory-bounded streaming. "
        "Checkpoint owner for fhir_server.EpisodeOfCare."
    ),
)
def fhir_server_eoc_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for EpisodeOfCare resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "EpisodeOfCare",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves references to soft-deleted PATIENT_IDs
    # (e.g. a deleted CPR-duplicate) to the active patient posted under the same
    # CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        query = _build_eoc_query(run_ctx.checkpoint, config)
        context.log.info(f"Querying arc.ifdb_episode_of_care (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} EpisodeOfCare rows "
                        f"(total so far: {total_fetched})"
                    )

                    healthissue_code_map = _build_healthissue_code_map(
                        batch, mssql_target_staging, context
                    )
                    episodes = _transform_episodes_to_fhir(
                        batch, context, healthissue_code_map=healthissue_code_map
                    )

                    if episodes:
                        # Safeguard B: make sure referenced previous-episodes
                        # exist in FHIR (fetched from archive) before resolving.
                        prev_ids = _collect_previous_eoc_identifiers(episodes)
                        _ensure_previous_episodes_from_archive(
                            prev_ids, mssql_target_staging, fhir_database, context
                        )

                        _post_episodes_to_fhir(
                            context=context,
                            episodes=episodes,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            mssql=None,
                            resolve_missing_dependencies=False,
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(episodes)

            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "EpisodeOfCare", config, total_fetched
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
