# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CarePlan assets for Alle Markoerer (All Markers).

This module transforms ALL EVENTMARKER data into FHIR CarePlan resources,
providing a complete view of all event markers for each patient.

Architecture Pattern:
- Two-asset ETL: Extract -> Transform + Post
- Extract asset: DSI/oracle/ifdb.py (eventmarker extraction)
- Transform + Post asset: This file

Key characteristics:
- One CarePlan per PATIENT_ID containing ALL their markers
- No filtering or grouping by marker type
- Preserves original marker codes and timestamps
- Useful for comprehensive patient timeline analysis
- Enables post-FHIR filtering and analysis of any marker type

Supports full load, daily, and monthly partitions.

Memory Management (Full Load):
For full loads with ~12M rows and ~1-2M patients:
1. Extract asset returns ONLY counts (no patient IDs) to avoid OOM
2. This asset uses database-level pagination (ROWNUM) to fetch patient IDs in batches
3. Each batch: fetch patient IDs -> extract markers -> stage -> transform -> post
4. Never holds all patient IDs in memory - true streaming approach
5. Reference resolution caches are loaded ONCE and reused across all batches
   (multi-type caching in FHIRDatabaseResource)
"""

from collections import defaultdict
from datetime import datetime
from typing import Any

import dagster as dg
from core.parallel import ChunkedPipelineStage, run_chunked_transform_post
from core.partitions import daily_partitions, full_load_partitions, monthly_partitions
from core.resources import FHIRDatabaseResource, IFDBOracleResource, MSSQLResource
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    ResolveValuesFn,
)
from DSI.oracle.ifdb import FULL_LOAD_BATCH_SIZE as ALLE_MARKOERER_FULL_LOAD_BATCH_SIZE
from DSI.oracle.ifdb import _extract_markers_for_patient_batch as extract_markers_for_patient_batch
from DSI.oracle.ifdb import _get_patient_ids_batch as get_patient_ids_batch
from fhir.resources.R4B.careplan import (
    CarePlan,
    CarePlanActivity,
    CarePlanActivityDetail,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_ACTIVITY_EPISODE_OF_CARE,
    EXTENSION_ACTIVITY_IDENTIFIER,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CAREPLAN_ALLE_MARKOERER,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_EVENTMARKER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_PATIENT_RIGHTS_TYPE,
    SYSTEM_SKS,
    SYSTEM_SOR,
)

# =============================================================================
# CarePlan Building Functions
# =============================================================================


# CarePlan must be chunked after marker staging, not on raw marker rows. One
# CarePlan contains all markers for a patient, so row-level chunking would split
# the semantic grain and create multiple CarePlans per patient.
CAREPLAN_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.careplan.alle_markoerer.transform_post",
    env_prefix="IFDB_CAREPLAN_PIPELINE",
    default_chunk_size=5_000,
    cleanup_resource_types={"CarePlan"},
)


def _build_alle_markoerer_careplan_dict(
    patient_id: Any,
    markers: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Build an Alle Markoerer CarePlan dict for a single patient.

    Args:
        patient_id: Patient identifier
        markers: All markers for this patient
    """
    # Sort markers by timestamp
    sorted_markers = sorted(markers, key=lambda m: m.get("MarkoerDatotid") or datetime.min)

    # Get period from first and last marker
    period_start = None
    period_end = None
    if sorted_markers:
        period_start = sorted_markers[0].get("MarkoerDatotid")
        period_end = sorted_markers[-1].get("MarkoerDatotid")

    # Get author (SOR code from first marker)
    author_sor = sorted_markers[0].get("ADMINISTRATIVE_UNIT_SORCODE") if sorted_markers else None

    # Build CarePlan identifier: PATIENT_ID_alle_markoerer
    identifier = f"{patient_id}_alle_markoerer"

    return {
        "identifier": identifier,
        "patient_id": patient_id,
        "period_start": period_start,
        "period_end": period_end,
        "status": "active",  # Alle markoerer is always "active" as it's a living document
        "author_sor": author_sor,
        "patient_rights_type": "alle_markoerer",
        "marker_count": len(sorted_markers),
        "activities": [
            {
                "object_id": m.get("OBJECT_ID"),
                "marker_code": m.get("MARKER_CODE"),
                "marker_code_full": m.get("MARKER_CODE_FULL"),
                "marker_type": m.get("MARKER_TYPE"),
                "event_text": m.get("EVENT_TEXT"),
                "timestamp": m.get("MarkoerDatotid"),
                "status": "completed",
                "sor_code": m.get("ADMINISTRATIVE_UNIT_SORCODE"),
                "staff_id": m.get("RESPONSIBLE_STAFF_PERSID"),
                "episode_of_care_id": m.get("EPISODE_OF_CARE_ID"),
            }
            for m in sorted_markers
        ],
    }


def _group_markers_by_patient(
    markers: list[dict[str, Any]],
    logger: dg.DagsterLogManager,
) -> list[dict[str, Any]]:
    """
    Group all markers by PATIENT_ID to create one CarePlan per patient.

    Returns:
        List of CarePlan dicts ready for FHIR transformation
    """
    if not markers:
        return []

    # Group by PATIENT_ID
    patient_groups: dict[Any, list[dict]] = defaultdict(list)
    for marker in markers:
        patient_id = marker.get("PATIENT_ID")
        if patient_id:
            patient_groups[patient_id].append(marker)

    # Create one CarePlan per patient
    careplans: list[dict[str, Any]] = []
    for patient_id, patient_markers in patient_groups.items():
        careplan = _build_alle_markoerer_careplan_dict(patient_id, patient_markers)
        careplans.append(careplan)

    # Log statistics
    logger.info(f"Created {len(careplans)} Alle Markoerer CarePlans")
    total_markers = sum(cp.get("marker_count", 0) for cp in careplans)
    logger.info(f"  Total markers: {total_markers}")
    if careplans:
        avg_markers = total_markers / len(careplans)
        logger.info(f"  Average markers per patient: {avg_markers:.1f}")

    return careplans


# =============================================================================
# Staging Logic
# =============================================================================


def _stage_careplans(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
) -> list[dict[str, Any]]:
    """
    Stage extracted data into CarePlan structures.

    Process:
    1. Group all markers by PATIENT_ID
    2. Create one CarePlan per patient
    """
    markers = extracted_data.get("markers", [])

    if not markers:
        context.log.info("No markers to stage - returning empty result")
        return []

    context.log.info(f"Staging {len(markers)} markers into CarePlan structures...")

    # Group markers by patient
    careplans = _group_markers_by_patient(markers, context.log)

    # Log statistics
    total_activities = sum(len(cp.get("activities", [])) for cp in careplans)

    context.log.info("Statistics:")
    context.log.info(f"  Total CarePlans: {len(careplans)}")
    context.log.info(f"  Total activities: {total_activities}")

    return careplans


# =============================================================================
# FHIR Transformation
# =============================================================================


def _build_activity(activity: dict[str, Any]) -> CarePlanActivity:
    """Build a FHIR CarePlanActivity from activity dict."""
    marker_code = activity.get("marker_code")
    marker_type = activity.get("marker_type")
    timestamp = activity.get("timestamp")
    activity_status = activity.get("status", "completed")

    # Extensions for activity identification
    extensions: list[Extension] = []

    # Add source identifier (OBJECT_ID)
    object_id = activity.get("object_id")
    if object_id:
        extensions.append(
            Extension(
                url=EXTENSION_ACTIVITY_IDENTIFIER,
                valueIdentifier=Identifier(
                    system=SYSTEM_IFDB_EVENTMARKER,
                    value=str(object_id),
                ),
            )
        )

    # Add EpisodeOfCare reference
    episode_of_care_id = activity.get("episode_of_care_id")
    if episode_of_care_id:
        extensions.append(
            Extension(
                url=EXTENSION_ACTIVITY_EPISODE_OF_CARE,
                valueReference=Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                        value=str(episode_of_care_id),
                    )
                ),
            )
        )

    # Outcome code (marker code with type)
    outcome_code = None
    if marker_code:
        display = f"{marker_code}"
        if marker_type:
            display = f"{marker_type}: {marker_code}"
        outcome_code = CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_SKS,
                    code=marker_code,
                    display=display,
                )
            ]
        )

    # Scheduled period (timestamp)
    scheduled_period = None
    if timestamp:
        scheduled_period = Period(
            start=fhir_datetime(timestamp),
            end=fhir_datetime(timestamp),
        )

    # Performers (Organization from SOR, Practitioner from staff)
    performers: list[Reference] = []
    sor_code = activity.get("sor_code")
    if sor_code:
        performers.append(
            Reference(
                identifier=Identifier(
                    system=SYSTEM_SOR,
                    value=str(sor_code),
                )
            )
        )
    staff_id = activity.get("staff_id")
    if staff_id:
        performers.append(
            Reference(
                identifier=Identifier(
                    system=SYSTEM_IFDB_STAFF,
                    value=str(staff_id),
                )
            )
        )

    # Event text
    event_text = activity.get("event_text")

    # Build activity detail
    detail = CarePlanActivityDetail(
        status=activity_status,
        scheduledPeriod=scheduled_period,
        performer=performers if performers else None,
        description=event_text,
    )

    return CarePlanActivity(
        extension=extensions if extensions else None,
        outcomeCodeableConcept=[outcome_code] if outcome_code else None,
        detail=detail,
    )


def _transform_careplan_to_fhir(careplan_data: dict[str, Any]) -> CarePlan:
    """Transform a staged CarePlan dict to a FHIR CarePlan resource."""
    # Identifiers
    careplan_id = careplan_data.get("identifier")

    identifiers = [
        Identifier(
            system=SYSTEM_CAREPLAN_ALLE_MARKOERER,
            value=str(careplan_id),
        )
    ]

    # Subject (Patient reference via identifier - will be resolved later)
    patient_id = careplan_data.get("patient_id")
    subject = Reference(
        identifier=Identifier(
            system=SYSTEM_IFDB_PATIENT_ID,
            value=str(patient_id),
        )
    )

    # Status and Intent
    status = careplan_data.get("status", "active")
    intent = "plan"

    # Category - patient rights type
    patient_rights_type = careplan_data.get("patient_rights_type", "alle_markoerer")

    categories = [
        CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_PATIENT_RIGHTS_TYPE,
                    code=patient_rights_type,
                    display="Alle markoerer",
                )
            ]
        )
    ]

    # Period
    period_start = careplan_data.get("period_start")
    period_end = careplan_data.get("period_end")
    period = None
    if period_start or period_end:
        period = Period(
            start=fhir_datetime(period_start) if period_start else None,
            end=fhir_datetime(period_end) if period_end else None,
        )

    # Author (Organization via SOR)
    author_sor = careplan_data.get("author_sor")
    author = None
    if author_sor:
        author = Reference(
            identifier=Identifier(
                system=SYSTEM_SOR,
                value=str(author_sor),
            )
        )

    # Activities
    activities_data = careplan_data.get("activities", [])
    activities = [_build_activity(a) for a in activities_data]

    # Build CarePlan
    return CarePlan(
        identifier=identifiers,
        status=status,
        intent=intent,
        category=categories,
        subject=subject,
        period=period,
        author=author,
        activity=activities if activities else None,
    )


def _transform_careplans_to_fhir(
    careplans_data: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[CarePlan]:
    """Transform list of staged CarePlan dicts to FHIR CarePlan resources."""
    careplans = []

    for cp_data in careplans_data:
        try:
            careplan = _transform_careplan_to_fhir(cp_data)
            careplans.append(careplan)
        except Exception as e:
            context.log.warning(f"Failed to transform CarePlan {cp_data.get('identifier')}: {e}")

    return careplans


# =============================================================================
# Reference Resolution Configuration
# =============================================================================

# Top-level reference fields
CAREPLAN_REFERENCE_CONFIGS = [
    # Patient reference (REQUIRED) - field is "subject" for CarePlan
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    # Author (Organization via SOR) - OPTIONAL
    ReferenceFieldConfig(
        field_name="author",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Author organization from SOR code",
    ),
]

# Nested reference fields
CAREPLAN_NESTED_REFERENCE_CONFIGS = [
    # Activity performer (Organization or Practitioner) - OPTIONAL
    NestedReferenceFieldConfig(
        field_path="activity.detail.performer",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
            SYSTEM_IFDB_STAFF: "Practitioner",
        },
        is_required=False,
        description="Activity performer (Organization or Practitioner)",
    ),
    # Activity EpisodeOfCare extension reference - OPTIONAL
    NestedReferenceFieldConfig(
        field_path="activity.extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        is_required=False,
        description="Activity EpisodeOfCare reference from EVENTMARKER.EPISODE_OF_CARE_ID",
    ),
]


# =============================================================================
# Post Functions
# =============================================================================


def _post_careplans_to_fhir_batch(
    context: dg.AssetExecutionContext,
    careplans: list[CarePlan],
    fhir_database: FHIRDatabaseResource,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dict[str, int]:
    """
    Post a batch of CarePlan resources to FHIR database.

    This is used for batched full load processing where we need to accumulate
    statistics across multiple batches.

    Note: Reference resolution caches are now persisted across batches
    (multi-type caching in FHIRDatabaseResource), so each reference type
    is only loaded ONCE per full load run.

    Returns:
        Dict with 'posted' and 'failed' counts
    """
    if not careplans:
        return {"posted": 0, "failed": 0}

    # Resolve missing Patient and EpisodeOfCare dependencies before reference resolution
    if resolve_missing_dependencies and mssql and careplans:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_careplan_dependencies,
        )

        resolve_careplan_dependencies(
            careplans=careplans,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

    # Full-load batches keep the default cache-backed self-ID resolver path.
    careplans = prepare_resources_for_upsert(
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        preferred_id_systems=[SYSTEM_CAREPLAN_ALLE_MARKOERER],
        reference_configs=CAREPLAN_REFERENCE_CONFIGS,
        nested_reference_configs=CAREPLAN_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=context.log,
    )

    # Post and capture result
    result = post_resources_direct(
        context=context,
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        source_name="ifdb_alle_markoerer",
    )

    # Extract counts from metadata
    metadata = result.metadata or {}
    posted = 0
    failed = 0

    # Try to get counts from metadata (depends on post_resources_direct implementation)
    if "posted_count" in metadata:
        val = metadata["posted_count"]
        posted = val.value if hasattr(val, "value") else val
    elif "resources_posted" in metadata:
        val = metadata["resources_posted"]
        posted = val.value if hasattr(val, "value") else val
    elif "inserted" in metadata:
        inserted = metadata["inserted"]
        inserted = inserted.value if hasattr(inserted, "value") else inserted
        updated = metadata.get("updated", 0)
        if hasattr(updated, "value"):
            updated = updated.value
        posted = inserted + updated
    else:
        # Assume all were posted if no specific counts
        posted = len(careplans)

    if "failed_count" in metadata:
        val = metadata["failed_count"]
        failed = val.value if hasattr(val, "value") else val
    elif "failed" in metadata:
        val = metadata["failed"]
        failed = val.value if hasattr(val, "value") else val

    return {"posted": posted, "failed": failed}


def _post_careplans_to_fhir(
    context: dg.AssetExecutionContext,
    careplans: list[CarePlan],
    fhir_database: FHIRDatabaseResource,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """
    Post CarePlan resources to FHIR database with reference resolution.

    This is used for daily/monthly processing where we can post all at once.

    Process:
    1. Resolve missing dependencies (Patient, EpisodeOfCare)
    2. Resolve all configured references (Patient, Organization, etc.)
    3. Resolve existing CarePlan FHIR IDs (for upsert - prevents duplicates)
    4. Post to FHIR database using direct DB insertion
    """
    # Resolve missing Patient and EpisodeOfCare dependencies before reference resolution
    if resolve_missing_dependencies and mssql and careplans:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_careplan_dependencies,
        )

        resolve_careplan_dependencies(
            careplans=careplans,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
        )

    # Daily/monthly deltas only need self-ID lookup for this partition's
    # CarePlans; references stay on the default resolver path.
    careplans = prepare_resources_for_upsert(
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        preferred_id_systems=[SYSTEM_CAREPLAN_ALLE_MARKOERER],
        reference_configs=CAREPLAN_REFERENCE_CONFIGS,
        nested_reference_configs=CAREPLAN_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        force_reload=True,
        logger=context.log,
    )

    # Post resources using direct DB insertion
    return post_resources_direct(
        context=context,
        resources=careplans,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        source_name="ifdb_alle_markoerer",
    )


def _run_careplan_delta_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    fhir_database: FHIRDatabaseResource,
    mssql: MSSQLResource,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Stage CarePlans once, then transform and post them through the core chunk runner."""
    careplans_data = _stage_careplans(context, extracted_data)
    # The staged CarePlans contain the fields needed from the marker extract.
    # Drop the original pickle-loaded marker list before building FHIR objects.
    extracted_data.clear()
    del extracted_data

    if not careplans_data:
        context.log.info("No CarePlans to transform - returning empty result")
        return dg.MaterializeResult(
            metadata={
                "careplans_total": 0,
                "careplans_posted": 0,
            }
        )

    context.log.info(
        f"Processing {len(careplans_data):,} staged CarePlans through "
        f"core chunked transform+post (chunk_size={CAREPLAN_CHUNK_STAGE.chunk_size:,})"
    )

    def transform(chunk: list[dict[str, Any]], _setup: None) -> list[CarePlan]:
        context.log.info(f"Transforming {len(chunk):,} CarePlans to FHIR...")
        careplans = _transform_careplans_to_fhir(chunk, context)
        context.log.info(f"  -> Created {len(careplans):,} FHIR CarePlan resources")
        return careplans

    def post(
        careplans: list[CarePlan],
        _chunk: list[dict[str, Any]],
        _setup: None,
    ) -> dg.MaterializeResult:
        return _post_careplans_to_fhir(
            context,
            careplans,
            fhir_database,
            mssql=mssql,
            reference_fallback_fn=reference_fallback_fn,
        )

    return run_chunked_transform_post(
        CAREPLAN_CHUNK_STAGE,
        rows=careplans_data,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


# =============================================================================
# Asset Definitions
# =============================================================================


@dg.asset(
    name="fhir_careplan_full_load_ifdb",
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_full_load"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",  # Patients must exist
        "fhir_server_eoc_dfm",  # EpisodeOfCare references must exist
    ],
    description=(
        "Transform and post FHIR CarePlan resources (full load). "
        "Uses database pagination to process ~12M rows without OOM."
    ),
)
def fhir_careplan_full_load_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    fhir_database: FHIRDatabaseResource,
    ifdb_oracle: IFDBOracleResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """
    Transform and post CarePlans for all patients using database pagination.

    For full loads, extracted_data contains ONLY metadata:
    - total_patient_count: Number of distinct patients
    - total_marker_count: Total number of markers
    - full_load_mode: True (indicates paginated processing)
    - batch_size: Batch size for processing

    Processing flow (streaming/pagination):
    1. Use ROWNUM pagination to fetch patient IDs in batches from DB
    2. For each batch: extract markers -> stage -> transform -> post
    3. Never hold all patient IDs in memory - true streaming
    """

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    # Check if this is full load mode (paginated processing)
    if not extracted_data.get("full_load_mode", False):
        # Fallback for non-full-load data (delta processing)
        context.log.warning("Full load asset received non-full-load data, processing normally")
        return _run_careplan_delta_pipeline(
            context=context,
            extracted_data=extracted_data,
            fhir_database=fhir_database,
            mssql=mssql_target_staging,
            reference_fallback_fn=patient_cpr_fallback,
        )

    # Paginated processing for full load - NO patient_ids in memory!
    batch_size = extracted_data.get("batch_size", ALLE_MARKOERER_FULL_LOAD_BATCH_SIZE)
    total_patient_count = extracted_data.get("total_patient_count", 0)
    total_marker_count = extracted_data.get("total_marker_count", 0)

    context.log.info("=" * 60)
    context.log.info("Starting PAGINATED full load processing (streaming)...")
    context.log.info(f"  Total patients: {total_patient_count:,}")
    context.log.info(f"  Total markers: {total_marker_count:,}")
    context.log.info(f"  Batch size: {batch_size:,}")

    total_batches = (
        (total_patient_count + batch_size - 1) // batch_size if total_patient_count > 0 else 0
    )
    context.log.info(f"  Expected batches: {total_batches}")
    context.log.info("=" * 60)

    # Accumulate statistics
    total_careplans_created = 0
    total_careplans_posted = 0
    total_careplans_failed = 0
    total_markers_processed = 0
    total_patients_processed = 0
    batch_num = 0

    # Pre-load CarePlan identifier cache once for the entire full load.
    # Individual batches omit a resolver hook to reuse this cache.
    # This prevents stale-cache duplicates when retrying a full load.
    fhir_database.preload_identifier_cache(
        resource_type="CarePlan",
        identifier_systems=[SYSTEM_CAREPLAN_ALLE_MARKOERER],
        force_reload=True,
    )

    # Process using database pagination - fetch patient IDs in batches
    offset = 0
    while True:
        batch_num += 1

        # Step 0: Fetch patient IDs for this batch using DB pagination
        context.log.info(
            f"Batch {batch_num}: Fetching patient IDs (offset={offset:,}, limit={batch_size:,})..."
        )
        batch_patient_ids = get_patient_ids_batch(
            ifdb_oracle=ifdb_oracle,
            offset=offset,
            batch_size=batch_size,
        )

        if not batch_patient_ids:
            context.log.info("  -> No more patients, stopping pagination")
            break

        batch_patients_count = len(batch_patient_ids)
        total_patients_processed += batch_patients_count
        context.log.info(f"  -> Got {batch_patients_count:,} patient IDs")

        # Step 1: Extract markers for this batch of patients
        context.log.info("  Extracting markers...")
        batch_markers = extract_markers_for_patient_batch(
            ifdb_oracle=ifdb_oracle,
            patient_ids=batch_patient_ids,
            logger=context.log,
        )
        context.log.info(f"  -> Extracted {len(batch_markers):,} markers")
        total_markers_processed += len(batch_markers)

        # Clear patient_ids to free memory
        del batch_patient_ids

        if not batch_markers:
            context.log.info("  -> No markers in this batch, skipping")
            offset += batch_size
            continue

        # Step 2: Stage into CarePlan structures
        batch_extracted_data = {"markers": batch_markers}
        careplans_data = _stage_careplans(context, batch_extracted_data)
        context.log.info(f"  -> Staged {len(careplans_data):,} CarePlans")

        # Clear markers to free memory
        del batch_markers
        del batch_extracted_data

        if not careplans_data:
            offset += batch_size
            continue

        # Step 3: Transform to FHIR
        careplans = _transform_careplans_to_fhir(careplans_data, context)
        total_careplans_created += len(careplans)
        context.log.info(f"  -> Transformed {len(careplans):,} FHIR CarePlans")

        # Clear careplans_data to free memory
        del careplans_data

        # Step 4: Post to FHIR (caches are reused across batches)
        try:
            batch_result = _post_careplans_to_fhir_batch(
                context,
                careplans,
                fhir_database,
                mssql=mssql_target_staging,
                reference_fallback_fn=patient_cpr_fallback,
            )
            total_careplans_posted += batch_result.get("posted", 0)
            total_careplans_failed += batch_result.get("failed", 0)
            context.log.info(
                f"  -> Posted {batch_result.get('posted', 0):,} CarePlans "
                f"(failed: {batch_result.get('failed', 0):,})"
            )
        except Exception as e:
            context.log.error(f"  -> Batch posting failed: {e}")
            total_careplans_failed += len(careplans)

        # Clear careplans to free memory
        del careplans

        # Log progress
        if total_patient_count > 0:
            progress_pct = (total_patients_processed / total_patient_count) * 100
            context.log.info(
                f"  -> Progress: {progress_pct:.1f}% "
                f"({total_careplans_posted:,} posted, {total_careplans_failed:,} failed)"
            )

        # Move to next batch
        offset += batch_size

    # Final summary
    context.log.info("=" * 60)
    context.log.info("Full load processing complete!")
    context.log.info(f"  Total batches processed: {batch_num}")
    context.log.info(f"  Total patients processed: {total_patients_processed:,}")
    context.log.info(f"  Total markers processed: {total_markers_processed:,}")
    context.log.info(f"  Total CarePlans created: {total_careplans_created:,}")
    context.log.info(f"  Total CarePlans posted: {total_careplans_posted:,}")
    context.log.info(f"  Total CarePlans failed: {total_careplans_failed:,}")
    context.log.info("=" * 60)

    return dg.MaterializeResult(
        metadata={
            "total_batches": batch_num,
            "total_patients": total_patients_processed,
            "total_markers_processed": total_markers_processed,
            "careplans_created": total_careplans_created,
            "careplans_posted": total_careplans_posted,
            "careplans_failed": total_careplans_failed,
            "batch_size": batch_size,
        }
    )


@dg.asset(
    name="fhir_careplan_daily_ifdb",
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_daily"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",  # Patients must exist
        "fhir_server_eoc_dfm",  # EpisodeOfCare references must exist
    ],
    description="Transform and post FHIR CarePlan resources (daily delta)",
)
def fhir_careplan_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Transform extracted marker data to FHIR CarePlan and post to FHIR database."""
    partition_key = context.partition_key
    context.log.info(f"Processing extracted data (daily: {partition_key})...")
    return _run_careplan_delta_pipeline(
        context=context,
        extracted_data=extracted_data,
        fhir_database=fhir_database,
        mssql=mssql_target_staging,
        reference_fallback_fn=build_patient_cpr_fallback(mssql_target_staging, fhir_database),
    )


@dg.asset(
    name="fhir_careplan_monthly_ifdb",
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_monthly"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",  # Patients must exist
        "fhir_server_eoc_dfm",  # EpisodeOfCare references must exist
    ],
    description="Transform and post FHIR CarePlan resources (monthly delta)",
)
def fhir_careplan_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Transform extracted marker data to FHIR CarePlan and post to FHIR database."""
    partition_key = context.partition_key
    context.log.info(f"Processing extracted data (monthly: {partition_key})...")
    return _run_careplan_delta_pipeline(
        context=context,
        extracted_data=extracted_data,
        fhir_database=fhir_database,
        mssql=mssql_target_staging,
        reference_fallback_fn=build_patient_cpr_fallback(mssql_target_staging, fhir_database),
    )


# NOTE: Job definitions are in jobs/fhir_careplan_jobs.py
