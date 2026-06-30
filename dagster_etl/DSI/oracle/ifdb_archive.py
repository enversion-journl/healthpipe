# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive MERGE assets for IFDB resources.

MERGEs ext.* → arc.* using the shared execute_archive_merge() function
driven by ArchiveMergeConfig. Checkpoint is tracked via DfmManager.

The archive checkpoint is set to the flow-start timestamp
(from dfm_ifdb_flow_start) after a successful merge, NOT to
MAX(delta_column). This ensures the archive checkpoint matches the
upper-bound used by the extract — so the next delta extract starts
exactly where this one stopped.

Encounter: delta (upsert on OBJECT_ID)
Location:  full-load (upsert on OBJECT_ID+PERIOD_START, soft-delete detection)
Patient:   delta (upsert on PATIENT_ID)
EpisodeOfCare: delta (upsert on OBJECT_ID)
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from DSI.oracle.ifdb_extract import (
    DFM_IFDB_FLOW_SCHEMA,
    DFM_IFDB_FLOW_TABLE,
    ENCOUNTER_COLUMNS,
    EOC_COLUMNS,
    LOCATION_COLUMNS,
    PATIENT_COLUMNS,
)

_IFDB_FLOW_START_KEY = (DFM_IFDB_FLOW_SCHEMA, DFM_IFDB_FLOW_TABLE)

# ---------------------------------------------------------------------------
# Encounter archive config
# ---------------------------------------------------------------------------

_ENCOUNTER_MERGE_KEY = ("OBJECT_ID",)
_ENCOUNTER_NON_KEY = tuple(c for c in ENCOUNTER_COLUMNS if c != "OBJECT_ID")

ENCOUNTER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="encounter_ifdb",
    target_schema="arc",
    target_table="encounter_ifdb",
    merge_keys=_ENCOUNTER_MERGE_KEY,
    non_key_columns=_ENCOUNTER_NON_KEY,
    flow_start_key=_IFDB_FLOW_START_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# ---------------------------------------------------------------------------
# Location archive config
# ---------------------------------------------------------------------------

_LOCATION_MERGE_KEYS = ("OBJECT_ID", "PERIOD_START")
_LOCATION_NON_KEY = tuple(c for c in LOCATION_COLUMNS if c not in _LOCATION_MERGE_KEYS)

LOCATION_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="location_ifdb",
    target_schema="arc",
    target_table="location_ifdb",
    merge_keys=_LOCATION_MERGE_KEYS,
    non_key_columns=_LOCATION_NON_KEY,
    flow_start_key=_IFDB_FLOW_START_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="DW_IsDeleted",
    upstream_is_full_load=True,
)


# ---------------------------------------------------------------------------
# Patient archive config
# ---------------------------------------------------------------------------

_PATIENT_MERGE_KEY = ("PATIENT_ID",)
_PATIENT_NON_KEY = tuple(c for c in PATIENT_COLUMNS if c != "PATIENT_ID")

PATIENT_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="patient_ifdb",
    target_schema="arc",
    target_table="patient_ifdb",
    merge_keys=_PATIENT_MERGE_KEY,
    non_key_columns=_PATIENT_NON_KEY,
    flow_start_key=_IFDB_FLOW_START_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
)

# ---------------------------------------------------------------------------
# EpisodeOfCare archive config
# ---------------------------------------------------------------------------

_EOC_MERGE_KEY = ("OBJECT_ID",)
_EOC_NON_KEY = tuple(c for c in EOC_COLUMNS if c != "OBJECT_ID")

EOC_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="episode_of_care_ifdb",
    target_schema="arc",
    target_table="episode_of_care_ifdb",
    merge_keys=_EOC_MERGE_KEY,
    non_key_columns=_EOC_NON_KEY,
    flow_start_key=_IFDB_FLOW_START_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)


# ---------------------------------------------------------------------------
# Archive Assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_encounter_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_encounter_ifdb"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "encounter",
    },
    description=(
        "MERGE upsert ext.encounter_ifdb → arc.encounter_ifdb; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_encounter_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Encounter."""
    result = execute_archive_merge(context, mssql_target_staging, ENCOUNTER_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.encounter_ifdb "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.encounter_ifdb",
            target_table="arc.encounter_ifdb",
        )
    )


@dg.asset(
    name="arc_location_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_location_ifdb"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "location",
    },
    description=(
        "MERGE upsert ext.location_ifdb → arc.location_ifdb (full-load, "
        "soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_location_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Location with soft-delete detection."""
    result = execute_archive_merge(context, mssql_target_staging, LOCATION_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.location_ifdb "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.location_ifdb",
            target_table="arc.location_ifdb",
        )
    )


@dg.asset(
    name="arc_patient_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_patient_ifdb"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "patient",
    },
    description=(
        "MERGE upsert ext.patient_ifdb → arc.patient_ifdb; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_patient_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Patient."""
    result = execute_archive_merge(context, mssql_target_staging, PATIENT_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.patient_ifdb "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.patient_ifdb",
            target_table="arc.patient_ifdb",
        )
    )


@dg.asset(
    name="arc_episode_of_care_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_episode_of_care_ifdb"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "episode_of_care",
    },
    description=(
        "MERGE upsert ext.episode_of_care_ifdb → arc.episode_of_care_ifdb; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_episode_of_care_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB EpisodeOfCare."""
    result = execute_archive_merge(context, mssql_target_staging, EOC_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.episode_of_care_ifdb "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.episode_of_care_ifdb",
            target_table="arc.episode_of_care_ifdb",
        )
    )


# ---------------------------------------------------------------------------
# Asset lists
# ---------------------------------------------------------------------------

ifdb_archive_assets = [
    arc_encounter_ifdb,
    arc_location_ifdb,
    arc_patient_ifdb,
    arc_episode_of_care_ifdb,
]
