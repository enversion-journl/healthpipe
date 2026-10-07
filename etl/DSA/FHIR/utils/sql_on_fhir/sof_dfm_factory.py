# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Factory for DFM-checkpointed SOF assets.

Every SOF asset does the same thing: read a flat view from the Aidbox ``sof``
schema for a window, MERGE it into the matching ``fhir.*`` table, then advance a
checkpoint. Only the view config, the checkpoint name and the upstream producers
differ. The partitioned variants each spelled that out by hand; on the DFM path
we generate them instead, so migrating the remaining resources is a one-line
change per view rather than a copied 40-line asset.

The end state this serves: once every producer of a flat table is on the DFM
path, its SOF (and DSO) assets move into the ``fhir_sof_dfm`` group and the
partitioned ``fhir_etl_ifdb_daily_job`` can be retired — one FHIR job, not a
base job plus a follow-up job.
"""

# NOTE: no ``from __future__ import annotations`` here. Dagster resolves the
# generated asset's ``config: RunModeConfig`` parameter from its *runtime*
# annotation; under PEP 563 it would be the string "RunModeConfig" and asset
# construction fails with DagsterInvalidConfigDefinitionError.

from collections.abc import Sequence
from datetime import UTC, datetime

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_RESOURCE_CONFIGS,
    extract_and_merge_sof_view,
)


def create_sof_dfm_asset(
    *,
    asset_name: str,
    config_key: str,
    checkpoint_resource: str,
    deps: Sequence[str],
    description: str | None = None,
) -> dg.AssetsDefinition:
    """Build a DFM-checkpointed SOF asset for one view.

    Args:
        asset_name: Dagster asset name, e.g. ``"sof_procedure_dfm"``.
        config_key: Key into ``SOF_RESOURCE_CONFIGS``, e.g. ``"procedure_view"``.
        checkpoint_resource: ``meta.DFMState`` table name under the ``fhir``
            scope, e.g. ``"Procedure"``. Must be unique per flat table.
        deps: Upstream asset names. List *every* producer of the flat table —
            an omitted producer can post after this asset has read the view,
            which delays those rows by a cycle.
        description: Optional override; a sensible default is generated.
    """
    cfg = SOF_RESOURCE_CONFIGS[config_key]

    @dg.asset(
        name=asset_name,
        group_name="fhir_sof_dfm",
        compute_kind="postgres",
        deps=list(deps),
        tags={"domain": "fhir", "layer": "dsa", "source": "sof"},
        description=description
        or (
            f"Extract {cfg['mssql_table']} flat view from Aidbox sof schema "
            f"(DFM checkpoint) and MERGE into fhir.{cfg['mssql_table']}"
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        fhir_database: FHIRDatabaseResource,
        mssql_target_staging: MSSQLResource,
        config: RunModeConfig,
    ) -> dg.MaterializeResult:
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

            # Stamp the next checkpoint BEFORE reading the view. The window is
            # ``ts >= partition_start``, so a resource written to Aidbox between
            # the query snapshot and a post-read stamp would be missed by this
            # run and fall below the next run's window — lost from the flat
            # table for good. Stamping first makes consecutive windows overlap
            # by the read duration instead; the MERGE is idempotent, so the
            # overlap costs nothing.
            new_checkpoint = datetime.now(UTC).isoformat()

            if config.mode == "ids":
                context.log.warning(
                    "SOF assets do not support mode='ids' — falling back to delta "
                    "behavior. Use mode='period' or 'full_load' to reprocess SOF data."
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
                normalized_datetime_columns=cfg.get("normalized_datetime_columns"),
            )

            logged_checkpoint: str | None = None
            if config.should_advance_checkpoint:
                dfm.set_checkpoint(
                    "fhir", checkpoint_resource, value=new_checkpoint, mode=config.mode
                )
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

    return _asset


__all__ = ["create_sof_dfm_asset"]
