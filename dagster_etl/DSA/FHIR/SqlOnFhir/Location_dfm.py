# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Location extraction using DFM checkpoint.

Reads Location flat view from Aidbox sof schema filtered by DFM checkpoint
(instead of Dagster partitions) and MERGEs into fhir.Location.
"""

from datetime import UTC, datetime

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    extract_and_merge_sof_view,
)

_CFG = SOF_RESOURCE_CONFIGS["location_view"]


@dg.asset(
    name="sof_location_dfm",
    group_name="fhir_sof_dfm",
    compute_kind="postgres",
    deps=["fhir_sof_infrastructure", "fhir_server_location_dfm"],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    description=(
        "Extract Location flat view from Aidbox sof schema (DFM checkpoint) "
        "and MERGE into fhir.Location"
    ),
)
def sof_location_dfm(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """SOF Location extraction using DFM checkpoint."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir",
        "Location",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        partition_start = run_ctx.checkpoint
        partition_end: str | None = None

        if config.mode == "ids":
            context.log.warning(
                "SOF assets do not support mode='ids' — falling back to delta behavior. "
                "Use mode='period' or 'full_load' to reprocess SOF data."
            )

        if config.mode == "full_load":
            partition_start = None
        elif config.mode == "period" and config.date_from and config.date_to:
            partition_start = config.date_from
            partition_end = config.date_to

        row_count, rows_affected = extract_and_merge_sof_view(
            fhir_database,
            mssql_target_staging,
            _CFG["view_name"],
            _CFG["resource_table"],
            _CFG["id_column"],
            _CFG["mssql_table"],
            _CFG["primary_keys"],
            partition_start=partition_start,
            partition_end=partition_end,
            context=context,
        )

        new_checkpoint = datetime.now(UTC).isoformat()
        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fhir", "Location", value=new_checkpoint, mode=config.mode)
            logged_checkpoint = new_checkpoint

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=logged_checkpoint,
            rows_affected=rows_affected,
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "rows_affected": rows_affected,
            "mode": config.mode,
            "checkpoint_used": run_ctx.checkpoint,
            "source_view": f"sof.{_CFG['view_name']}",
            "target_table": f"fhir.{_CFG['mssql_table']}",
        }
    )
