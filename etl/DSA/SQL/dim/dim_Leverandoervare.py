# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Leverandoervare

Sync dimension from map + add descriptive attributes.
Logic from: erp_dims_and_facts.yml (Type 1 SCD with composite natural key)

Uses the delta-driven staging table:
- stg.dim_Leverandoervare: changed (kreditor, leverandoervarenummer) entities enriched
  with MySupply + ILS attributes and per-source presence flags.

Source-flags are derived in staging from EXISTS over the full current archive
picture, so a plain assignment from stg.dim_Leverandoervare is correct on every run.

Privacy: This dimension uses ONLY MySupply/ILS sources.
KMD ItemDescription/ItemName must NEVER be added here — see guardrail test.
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
        AssetKey(["map_Leverandoervare"]),
        AssetKey(["erp", "stg_dim_Leverandoervare"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Leverandoervare from map + delta stg.dim_Leverandoervare attributes",
)
def dim_Leverandoervare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add attributes from delta staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim", "Leverandoervare", dagster_run_id=context.run_id, mode=config.mode
    )
    rows_affected = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting Leverandoervare dimension sync from unified stg.dim_Leverandoervare..."
                )

                cursor.execute("SELECT COUNT(*) FROM [map].[Leverandoervare]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Leverandoervare entries to sync")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Leverandoervare] v
                LEFT JOIN [map].[Leverandoervare] m
                    ON m.clean_code_Leverandoervare = v.clean_code_Leverandoervare
                WHERE m.dw_ek_Leverandoervare IS NULL
                """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Leverandoervare rows without map match"
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Leverandoervare_Attributes;

                SELECT
                    m.dw_ek_Leverandoervare,
                    m.bk_kreditor AS cvr,
                    LEFT(m.bk_leverandoer_varenummer, 255) AS varenummer_kreditor,
                    v.varenavn,
                    v.varenavn_mysupply,
                    v.dw_source_mysupply,
                    v.dw_source_ils
                INTO #Leverandoervare_Attributes
                FROM [stg].[dim_Leverandoervare] v
                LEFT JOIN [map].[Leverandoervare] m ON m.clean_code_Leverandoervare = v.clean_code_Leverandoervare
                WHERE m.dw_ek_Leverandoervare IS NOT NULL;
                """,
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Leverandoervare_Attributes
                ON #Leverandoervare_Attributes (dw_ek_Leverandoervare)
                """
                )

                cursor.execute(
                    """
                MERGE [dim].[Leverandoervare] AS tgt
                USING #Leverandoervare_Attributes AS src
                ON tgt.dw_ek_Leverandoervare = src.dw_ek_Leverandoervare

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.cvr = src.cvr,
                        tgt.varenummer_kreditor = src.varenummer_kreditor,
                        tgt.varenavn = src.varenavn,
                        tgt.varenavn_mysupply = src.varenavn_mysupply,
                        tgt.dw_source_mysupply = src.dw_source_mysupply,
                        tgt.dw_source_ils = src.dw_source_ils,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Leverandoervare, cvr, varenummer_kreditor, varenavn, varenavn_mysupply,
                        dw_source_mysupply, dw_source_ils, dw_timestamp_insert
                    )
                    VALUES (
                        src.dw_ek_Leverandoervare, src.cvr, src.varenummer_kreditor, src.varenavn,
                        src.varenavn_mysupply, src.dw_source_mysupply,
                        src.dw_source_ils, CAST(SYSDATETIME() AS datetime2(0))
                    );
                    """
                )
                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Leverandoervare_Attributes")
                cursor.execute("UPDATE STATISTICS [dim].[Leverandoervare]")

                context.log.info(f"Dimension sync complete: {rows_affected} rows merged")
            finally:
                cursor.close()

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Leverandoervare", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Leverandoervare")

        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "dim.Leverandoervare",
            "composite_natural_key": "clean_code_Leverandoervare",
            "source_table": "stg.dim_Leverandoervare (delta)",
            "unmapped_stg_rows": unmapped_rows,
            "scd_type": "Type 1 (update in place)",
            "checkpoint_after": logged_checkpoint,
            "mode": config.mode,
        }
    )
