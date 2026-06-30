# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Observation assets for IFDB data.

This module provides assets that transform extracted event marker data into FHIR Observation
resources and post them to the FHIR server.

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required

    Uses NestedReferenceFieldConfig for:
    - EpisodeOfCare (extension.valueReference) - optional
"""

from typing import Any

import dagster as dg
from core.parallel import ChunkedPipelineStage, run_chunked_transform_post
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from core.resources import (
    FHIRDatabaseResource,
    FHIRServerResource,
    IFDBOracleResource,
)
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, parse_clob_datetime
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

# =============================================================================
# Reference Resolution Configuration
# =============================================================================

# Top-level reference fields
OBSERVATION_REFERENCE_CONFIGS = [
    # Patient reference (REQUIRED) - field is "subject" for Observation
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
]

# Nested reference fields (extension-based)
OBSERVATION_NESTED_REFERENCE_CONFIGS = [
    # EpisodeOfCare reference in extension (OPTIONAL)
    NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        extension_url=EXTENSION_EPISODE_OF_CARE,
        is_required=False,
        description="Episode of care reference from extension",
    ),
]


# Chunked transform+post stage. Override with IFDB_OBSERVATION_PIPELINE_CHUNK_SIZE.
OBSERVATION_IFDB_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.observation.transform_post",
    env_prefix="IFDB_OBSERVATION_PIPELINE",
    default_chunk_size=25_000,
    cleanup_resource_types={"Observation"},
)


# ============================================================================
# Helper Functions
# ============================================================================


def _transform_observations_to_fhir(
    rows: list[dict[str, Any]], context: dg.AssetExecutionContext
) -> list[Observation]:
    """Transform observation rows to FHIR Observation resources."""
    observations: list[Observation] = []

    for row_dict in rows:
        object_id = row_dict.get("OBJECT_ID")
        patient_id = row_dict.get("PATIENT_ID")
        episode_id = row_dict.get("EPISODE_OF_CARE_ID")
        # TIME is the column name in IFDB.EVENTMARKER (not EVENT_DATETIME)
        event_datetime = row_dict.get("TIME")
        event_code = row_dict.get("EVENT_CODE")
        event_text = row_dict.get("EVENT_TEXT")

        # Parse datetime fields with CLOB handling
        effective_datetime = None
        if event_datetime:
            if isinstance(event_datetime, str):
                parsed_dt = parse_clob_datetime(event_datetime)
                effective_datetime = fhir_datetime(parsed_dt) if parsed_dt else None
            else:
                effective_datetime = fhir_datetime(event_datetime)

        # Create extension for episode of care reference
        extensions = []
        if episode_id:
            eoc_ref = Reference(
                identifier=Identifier(
                    system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                    value=str(episode_id),
                )
            )
            eoc_extension = Extension(url=EXTENSION_EPISODE_OF_CARE, valueReference=eoc_ref)
            extensions.append(eoc_extension)

        is_deleted = row_dict.get("DELETED")
        deleted_value = int(is_true_flag(is_deleted))
        extensions.append(build_is_deleted_extension(deleted_value))

        obs = Observation(
            identifier=(
                [Identifier(system=SYSTEM_IFDB_OBJECT_ID, value=str(object_id))]
                if object_id
                else None
            ),
            status="final",
            code=CodeableConcept(
                coding=[Coding(code=event_code, display=event_text)],
                text=event_text,
            ),
            subject=(
                Reference(
                    identifier=Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=str(patient_id))
                )
                if patient_id
                else None
            ),
            effectiveDateTime=effective_datetime,
            extension=extensions if extensions else None,
        )
        observations.append(obs)

    return observations


def _post_observations_to_fhir(
    context: dg.AssetExecutionContext,
    observations: list[Observation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post observations to FHIR database with reference resolution."""
    logger = dg.get_dagster_logger()

    # Collect all patient and episode IDs that need to exist
    patient_ids_needed: set[str] = set()
    eoc_ids_needed: set[str] = set()

    for obs in observations:
        if obs.subject and getattr(obs.subject, "identifier", None):
            patient_ids_needed.add(str(obs.subject.identifier.value))
        if obs.extension:
            for ext in obs.extension:
                if (
                    ext.url == EXTENSION_EPISODE_OF_CARE
                    and ext.valueReference
                    and getattr(ext.valueReference, "identifier", None)
                ):
                    eoc_ids_needed.add(str(ext.valueReference.identifier.value))

    if resolve_missing_dependencies and ifdb_oracle:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_episodes_of_care,
            resolve_missing_patients,
        )

        if eoc_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(eoc_ids_needed)} episodes of care exist")
            resolve_missing_episodes_of_care(
                eoc_ids=eoc_ids_needed,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

        if patient_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
            resolve_missing_patients(
                patient_ids=patient_ids_needed,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

    observations = prepare_resources_for_upsert(
        resources=observations,
        fhir_database=fhir_database,
        resource_type="Observation",
        preferred_id_systems=[SYSTEM_IFDB_OBJECT_ID],
        reference_configs=OBSERVATION_REFERENCE_CONFIGS,
        nested_reference_configs=OBSERVATION_NESTED_REFERENCE_CONFIGS,
        use_cache_resolution=True,
        force_reload=True,
        logger=logger,
    )

    # Post resources using direct DB insertion
    return post_resources_direct(
        context=context,
        resources=observations,
        fhir_database=fhir_database,
        resource_type="Observation",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


def _run_observation_ifdb_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Chunked transform+post pipeline shared by all Observation_IFDB assets.

    Asset input is a ``dict[str, Any]`` produced by the ifdb_extract layer;
    the row list lives under the ``"markers"`` key. The wrapper extracts it
    once with a defensive default, then forwards to run_chunked_transform_post.
    No partition-level setup is required — Observation_IFDB has no auxiliary
    Oracle lookups beyond what is already in each row.

    The pinned ``test_observation_ifdb_markers_pin`` test must keep passing
    after this wrapper goes live, proving the markers list is forwarded
    verbatim and the outer dict never leaks into transform/post.
    """
    rows = extracted_data.get("markers", [])

    def transform(
        chunk: list[dict[str, Any]],
        _setup: None,
    ) -> list[Observation]:
        return _transform_observations_to_fhir(chunk, context)

    def post(
        observations: list[Observation],
        chunk: list[dict[str, Any]],
        _setup: None,
    ) -> dg.MaterializeResult:
        return _post_observations_to_fhir(
            context=context,
            observations=observations,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
            resolve_missing_dependencies=True,
        )

    return run_chunked_transform_post(
        OBSERVATION_IFDB_CHUNK_STAGE,
        rows=rows,
        setup_fn=None,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


# ============================================================================
# Daily Partition Assets
# ============================================================================


@dg.asset(
    name="fhir_observation_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_daily"])
        ),
    },
    deps=["fhir_server_eoc_dfm"],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={
        TAG_CHUNK_SIZE: "400",
        TAG_MAX_WORKERS: "8",
    },
)
def fhir_observation_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Observation resources for daily partition."""
    return _run_observation_ifdb_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


# ============================================================================
# Monthly Partition Assets
# ============================================================================


@dg.asset(
    name="fhir_observation_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_monthly"])
        ),
    },
    deps=["fhir_server_eoc_dfm"],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={
        TAG_CHUNK_SIZE: "1000",
        TAG_MAX_WORKERS: "8",
    },
)
def fhir_observation_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Observation resources for monthly partition."""
    return _run_observation_ifdb_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


# ============================================================================
# Full Load Partition Assets
# ============================================================================


@dg.asset(
    name="fhir_observation_full_load_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "eventmarker_alle_markoerer_full_load"])
        ),
    },
    deps=["fhir_server_eoc_dfm"],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={
        TAG_CHUNK_SIZE: "1000",
        TAG_MAX_WORKERS: "16",
    },
)
def fhir_observation_full_load_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: dict[str, Any],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Observation resources for full load."""
    return _run_observation_ifdb_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
