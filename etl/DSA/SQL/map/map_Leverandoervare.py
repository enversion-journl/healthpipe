# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Map Update: Leverandoervare

Assign DW_EK to Leverandoervare via IDENTITY.
Logic from: erp_dims_and_facts.yml (canonical natural key: clean_code_Leverandoervare)

Uses the unified dimension staging table:
- stg.dim_Leverandoervare: Combined MySupply + ILS vare data
- Stores bk_kreditor and bk_leverandoer_varenummer as traceability components.

INSERT-only: existing map rows are never updated. Leverandoervare <-> ILSVare
relations live in bridge.LeverandoervareILSVare, not on the map.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_map",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_Leverandoervare"]),
    ],
    tags={"domain": "erp", "layer": "maps", "module": "datawarehouse"},
    description="Assign DW_EK to Leverandoervare via IDENTITY using canonical natural key clean_code_Leverandoervare",
)
def map_Leverandoervare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map table maintenance: Insert new Leverandoervare combinations from unified staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "map", "Leverandoervare", dagster_run_id=context.run_id, mode=config.mode
    )
    new_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting Leverandoervare map update from unified stg.dim_Leverandoervare..."
                )

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Leverandoervare]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Unified stg.dim_Leverandoervare has {stg_count} rows")

                cursor.execute(
                    """
                INSERT INTO [map].[Leverandoervare] (
                    bk_kreditor,
                    bk_leverandoer_varenummer,
                    clean_code_Leverandoervare,
                    dw_timestamp_insert,
                    dw_timestamp_update
                )
                SELECT
                    v.bk_kreditor,
                    v.bk_leverandoer_varenummer,
                    v.clean_code_Leverandoervare,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM [stg].[dim_Leverandoervare] v
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [map].[Leverandoervare] m
                    WHERE m.clean_code_Leverandoervare = v.clean_code_Leverandoervare
                );
            """
                )

                new_count = cursor.rowcount
                context.log.info(f"Inserted {new_count} new Leverandoervare entries")

                if new_count > 0:
                    cursor.execute(
                        """
                    SELECT TOP 5 bk_kreditor, bk_leverandoer_varenummer
                    FROM map.Leverandoervare
                    ORDER BY dw_timestamp_insert DESC
                """
                    )
                    recent_inserts = cursor.fetchall()
                    context.log.info(
                        f"Sample new entries: {[(r[0], r[1]) for r in recent_inserts]}"
                    )

                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM map.Leverandoervare")
                total_count = cursor.fetchone()[0]

                context.log.info(
                    f"Map update complete: {new_count} new, {total_count} total entries"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=new_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "new_rows": new_count,
            "total_rows": total_count,
            "table": "map.Leverandoervare",
            "natural_key": "clean_code_Leverandoervare",
            "source_table": "stg.dim_Leverandoervare",
            "mode": config.mode,
        }
    )
