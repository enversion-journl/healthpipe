# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: ServiceRequest extraction using DFM checkpoint.

Reads the ServiceRequest flat view (and its tillaegskode child view) from the
Aidbox sof schema filtered by DFM checkpoint instead of Dagster partitions, and
MERGEs into fhir.ServiceRequest / fhir.ServiceRequest_tillaegskode.

Both producers of fhir.ServiceRequest are on the DFM path (the
henvisning/RIS pipeline; the rehabilitation-plan slice was already there), so
this SOF sync can live in the DFM SOF+DSO job. See §4.3 of
docs/archive/2026-08-06_fhir_ifdb_arkiv_migrering_estimat.md for why the SOF
layer must wait for the *last* producer of a flat table.
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

_CFG = SOF_RESOURCE_CONFIGS["servicerequest_view"]
_SR_TILLAEGSKODE_CFG = SOF_RESOURCE_CONFIGS["servicerequest_tillaegskode_view"]

_SR_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_service_request_dfm",
    "fhir_server_rehabilitation_plan_dfm",
]


def _run_sof_service_request_extract(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
    cfg: dict,
    checkpoint_resource: str,
) -> dg.MaterializeResult:
    """Shared DFM-checkpointed SOF extract for ServiceRequest and its child view."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir",
        checkpoint_resource,
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
            cfg["view_name"],
            cfg["resource_table"],
            cfg["id_column"],
            cfg["mssql_table"],
            cfg["primary_keys"],
            partition_start=partition_start,
            partition_end=partition_end,
            context=context,
            null_sentinel_columns=cfg.get("null_sentinel_columns"),
        )

        new_checkpoint = datetime.now(UTC).isoformat()
        logged_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fhir", checkpoint_resource, value=new_checkpoint, mode=config.mode)
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
            "source_view": f"sof.{cfg['view_name']}",
            "target_table": f"fhir.{cfg['mssql_table']}",
        }
    )


@dg.asset(
    name="sof_service_request_dfm",
    group_name="fhir_sof_dfm",
    compute_kind="postgres",
    deps=_SR_PRODUCERS,
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    description=(
        "Extract ServiceRequest flat view from Aidbox sof schema (DFM checkpoint) "
        "and MERGE into fhir.ServiceRequest"
    ),
)
def sof_service_request_dfm(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """SOF ServiceRequest extraction using DFM checkpoint."""
    return _run_sof_service_request_extract(
        context, fhir_database, mssql_target_staging, config, _CFG, "ServiceRequest"
    )


@dg.asset(
    name="sof_service_request_tillaegskode_dfm",
    group_name="fhir_sof_dfm",
    compute_kind="postgres",
    # The child view is derived from the parent's resources, so it follows the
    # parent SOF asset rather than re-reading the producers (same pattern as the
    # Condition/Procedure child views).
    deps=["fhir_sof_infrastructure", "sof_service_request_dfm"],
    tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
    description=(
        "Extract ServiceRequest_tillaegskode child view from Aidbox sof schema "
        "(DFM checkpoint) and MERGE into fhir.ServiceRequest_tillaegskode"
    ),
)
def sof_service_request_tillaegskode_dfm(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """SOF ServiceRequest_tillaegskode extraction using DFM checkpoint."""
    return _run_sof_service_request_extract(
        context,
        fhir_database,
        mssql_target_staging,
        config,
        _SR_TILLAEGSKODE_CFG,
        "ServiceRequest_tillaegskode",
    )
