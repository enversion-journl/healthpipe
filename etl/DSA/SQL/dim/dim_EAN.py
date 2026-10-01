# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: ean

Populate dim.ean from stg.dim_EAN with resolved surrogate keys.
Logic: Type 1 SCD - overwrite all attributes on each run.

Resolves:
- dw_ek_EAN from map.ean

Flat dimension (no parent hierarchy, no Organization FK).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["map_EAN"]),
        AssetKey(["stg_dim_EAN"]),
    ],
    tags={"domain": "erp", "layer": "dimensions", "module": "datawarehouse", "scd": "type1"},
    description="Populate dim.ean (Type 1 SCD) with ean and navn_nemhandelsregister",
)
def dim_EAN(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: resolve EKs and MERGE into dim.ean."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "ean", dagster_run_id=context.run_id, mode=config.mode)

    inserted = 0
    updated = 0
    total = 0
    has_name = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building temp table with resolved EKs...")

                # Use LEFT JOIN to capture both resolved and unmapped rows in one pass
                cursor.execute(
                    """
                DROP TABLE IF EXISTS #EAN_Resolved;

                SELECT
                    m.dw_ek_EAN,
                    stg.ean,
                    stg.navn_nemhandelsregister
                INTO #EAN_Resolved
                FROM [stg].[dim_EAN] stg
                LEFT JOIN [map].[ean] m
                    ON m.clean_code_EAN = stg.clean_code_EAN
            """
                )

                # Check for unmapped rows (dw_ek_EAN IS NULL means no map match)
                cursor.execute("SELECT COUNT(*) FROM #EAN_Resolved WHERE dw_ek_EAN IS NULL")
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_EAN: {unmapped_rows} staging rows have no match in map.ean. "
                        "Ensure map_EAN runs before dim_EAN and staging CleanCodes are valid."
                    )

                cursor.execute("SELECT COUNT(*) FROM #EAN_Resolved")
                resolved_count = cursor.fetchone()[0]
                context.log.info(f"Resolved {resolved_count} rows for dim.ean")

                cursor.execute(
                    """
                MERGE INTO [dim].[ean] AS target
                USING #EAN_Resolved AS source
                ON target.dw_ek_EAN = source.dw_ek_EAN
                WHEN MATCHED THEN
                    UPDATE SET
                        ean = source.ean,
                        navn_nemhandelsregister = source.navn_nemhandelsregister,
                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_EAN, ean, navn_nemhandelsregister,
                        dw_timestamp_insert, dw_timestamp_update
                    )
                    VALUES (
                        source.dw_ek_EAN, source.ean, source.navn_nemhandelsregister,
                        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                    )
                OUTPUT $action;
            """
                )

                merge_results = cursor.fetchall()
                inserted = sum(1 for r in merge_results if r[0] == "INSERT")
                updated = sum(1 for r in merge_results if r[0] == "UPDATE")

                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #EAN_Resolved")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN navn_nemhandelsregister IS NOT NULL
                             AND navn_nemhandelsregister <> '' THEN 1 ELSE 0 END) AS has_name
                FROM [dim].[ean]
                WHERE dw_ek_EAN <> -1
            """
                )
                stats = cursor.fetchone()
                total = stats[0]
                has_name = stats[1]
            finally:
                cursor.close()

        context.log.info(
            f"dim.ean: {inserted} inserted, {updated} updated, {total} total "
            f"({has_name} with navn_nemhandelsregister)"
        )

        rows_affected = inserted + updated
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "ean", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "ean")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "has_navn_nemhandelsregister": has_name,
            "table": "dim.ean",
            "scd_type": "Type 1",
        }
    )
