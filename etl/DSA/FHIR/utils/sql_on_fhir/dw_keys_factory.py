# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Factory for the assets that put the borger and organisation dw-keys on fhir.*."""

# NOTE: no ``from __future__ import annotations`` here: Dagster resolves the asset's
# ``config: RunModeConfig`` parameter from its runtime annotation.

import time

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.mssql_application_lock import application_lock
from core.resources import MSSQLResource
from DSA.FHIR.utils.sql_on_fhir.dw_keys import (
    DWKeysConfig,
    key_columns,
    organisation_read_bound,
    resolve_scope,
    run_dw_keys,
)
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    FHIR_TABLE_LOCK_TIMEOUT_MS,
    fhir_table_lock_name,
)

GROUP_NAME = "fhir_dw_keys"


def create_dw_keys_asset(cfg: DWKeysConfig, *, sof_asset: str) -> dg.AssetsDefinition:
    """Build the asset that keeps the dw-keys on ``fhir.<cfg.table>`` up to date.

    DFM checkpoints under ``fhir``: ``<table>_DWKeys`` holds the highest
    ``dw_timestamp_update`` after the asset's own writes, ``<table>_DWKeys_Borger`` (only for
    a table with borger keys) and ``<table>_DWKeys_Organisation`` the read bound on the
    dimension it reads: held back to a dim_Borger run in flight, and one hour before the run
    start for dim.Organisation and for the table an inherited organisation is read from.
    """
    deps = [sof_asset]
    sources = []
    if cfg.has_borger:
        deps.append("dim_Borger")
        sources.append("dim.Borger")
    if any(organisation.via is None for organisation in cfg.organisations):
        deps.append("dim_Organisation")
        sources.append("dim.Organisation")
    for organisation in cfg.organisations:
        if organisation.via is not None:
            deps.append(organisation.via.asset_name)
            sources.append(f"fhir.{organisation.via.table}")
    source_tables = ", ".join(sources)

    @dg.asset(
        name=cfg.asset_name,
        group_name=GROUP_NAME,
        compute_kind="mssql",
        deps=deps,
        tags={"domain": "fhir", "layer": "dsa"},
        description=(
            f"Sets {', '.join(key_columns(cfg))} on fhir.{cfg.table} from {source_tables} and "
            "writes only the rows where a key changes."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        config: RunModeConfig,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        started = time.monotonic()
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "fhir", cfg.checkpoint_fhir, dagster_run_id=context.run_id, mode=config.mode
        )
        try:
            with application_lock(
                mssql_target_staging,
                fhir_table_lock_name(cfg.table),
                timeout_ms=FHIR_TABLE_LOCK_TIMEOUT_MS,
            ):
                checkpoints = {cfg.checkpoint_fhir: run_ctx.checkpoint}
                if cfg.has_borger:
                    checkpoints[cfg.checkpoint_borger] = dfm.get_checkpoint(
                        "fhir", cfg.checkpoint_borger
                    )
                checkpoints[cfg.checkpoint_organisation] = dfm.get_checkpoint(
                    "fhir", cfg.checkpoint_organisation
                )
                full, reason = resolve_scope(config.mode, checkpoints)
                if reason:
                    context.log.info(f"{cfg.asset_name}: full pass ({reason})")

                # Read bounds for the dimensions, taken before the dimensions are read.
                borger_bound = (
                    dfm.safe_reader_checkpoint(run_ctx.started_at, writer=("dim", "Borger"))
                    if cfg.has_borger
                    else None
                )
                organisation_bound = organisation_read_bound(run_ctx.started_at)

                with mssql_target_staging.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        result = run_dw_keys(
                            cursor,
                            conn,
                            cfg,
                            full=full,
                            checkpoints=checkpoints,
                            log=context.log,
                        )
                    finally:
                        cursor.close()

                if config.should_advance_checkpoint:
                    if result["fhir_watermark"]:
                        dfm.set_checkpoint(
                            "fhir",
                            cfg.checkpoint_fhir,
                            value=result["fhir_watermark"],
                            mode=config.mode,
                        )
                    if borger_bound:
                        dfm.set_checkpoint(
                            "fhir", cfg.checkpoint_borger, value=borger_bound, mode=config.mode
                        )
                    if organisation_bound:
                        dfm.set_checkpoint(
                            "fhir",
                            cfg.checkpoint_organisation,
                            value=organisation_bound,
                            mode=config.mode,
                        )

            dfm.complete_dfm_run(
                run_ctx.log_id,
                checkpoint_after=(
                    result["fhir_watermark"] if config.should_advance_checkpoint else None
                ),
                rows_affected=result["rows_written"],
            )
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

        return dg.MaterializeResult(
            metadata={
                "mode": config.mode,
                "full_pass": full,
                "candidates": result["candidates"],
                "row_count": result["rows_written"],
                "batches": result["batches"],
                "checkpoints_used": dg.MetadataValue.json(checkpoints),
                "distribution": dg.MetadataValue.json(result["distribution"] or {}),
                "seconds": round(time.monotonic() - started, 1),
                "source_table": source_tables,
                "target_table": f"fhir.{cfg.table}",
            }
        )

    return _asset


__all__ = ["GROUP_NAME", "create_dw_keys_asset"]
