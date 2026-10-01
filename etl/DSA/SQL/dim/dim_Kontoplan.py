# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Kontoplan

Sync dimension from map + add descriptive attributes from enriched SydAX staging.
Logic from: erp_dims_and_facts.yml (Type 1 SCD)

Join Path:
  map.Kontoplan (driver) -> stg.dim_Kontoplan_sydax (enriched in stg layer)
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Kontoplan"]),
        AssetKey(["erp", "stg_dim_Kontoplan_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Kontoplan from map + add attributes from enriched SydAX staging",
)
def dim_Kontoplan(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: sync from map and enrich from staging."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Kontoplan", dagster_run_id=context.run_id, mode=config.mode)

    logged_cp: str | None = None
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Kontoplan dimension sync with full hierarchy...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Kontoplan]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Kontoplan entries to sync")

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [stg].[dim_Kontoplan_sydax] src
                LEFT JOIN [map].[Kontoplan] m
                    ON m.clean_code_Kontoplan = src.clean_code_Kontoplan
                WHERE m.dw_ek_Kontoplan IS NULL
            """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Kontoplan_sydax rows without map match"
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Kontoplan_Attributes;

                SELECT
                    m.dw_ek_Kontoplan,
                    m.clean_code_Kontoplan,
                    COALESCE(src.finanskonto_id, m.bk_finanskonto_id) AS finanskonto_id,
                    COALESCE(src.kontonummer, m.bk_kontonummer) AS kontonummer,
                    src.f1,
                    src.f2,
                    src.f3,
                    src.d1,
                    src.g1,
                    src.s1,
                    src.s2,
                    src.s3,
                    src.s4,
                    src.s5,
                    src.s6,
                    src.s7,
                    src.g2,
                    src.a1,
                    src.a2,
                    src.tekst_s1,
                    src.tekst_s2,
                    src.tekst_s3,
                    src.tekst_s4,
                    src.tekst_s5,
                    src.tekst_s6,
                    src.tekst_s7,
                    src.tekst_g1,
                    src.tekst_g2,
                    src.tekst_a1,
                    src.tekst_a2,
                    src.tekst_f1,
                    src.tekst_f2,
                    src.tekst_f3,
                    src.tekst_d1,
                    src.omkostningsstednummer,
                    src.omkostningssted_niveau
                INTO #Kontoplan_Attributes
                FROM [stg].[dim_Kontoplan_sydax] src
                LEFT JOIN [map].[Kontoplan] m
                    ON m.clean_code_Kontoplan = src.clean_code_Kontoplan
                WHERE m.dw_ek_Kontoplan IS NOT NULL;
            """
                )

                cursor.execute(
                    """
                CREATE UNIQUE CLUSTERED INDEX IX_tmp_Kontoplan_Attributes
                ON #Kontoplan_Attributes (dw_ek_Kontoplan)
                """
                )
                context.log.info("Temp table #Kontoplan_Attributes created successfully")

                cursor.execute(
                    """
                SELECT
                    COUNT(*) AS total,
                    COUNT(s1) AS has_s1,
                    COUNT(f1) AS has_f1,
                    COUNT(g2) AS has_g2,
                    COUNT(omkostningsstednummer) AS has_omkost,
                    COUNT(tekst_f1) AS has_tekst_f1,
                    COUNT(tekst_d1) AS has_tekst_d1
                FROM #Kontoplan_Attributes
            """
                )
                attr_stats = cursor.fetchone()
                context.log.info(
                    f"Attribute coverage: s1={attr_stats[1]}/{attr_stats[0]}, "
                    f"f1={attr_stats[2]}/{attr_stats[0]}, g2={attr_stats[3]}/{attr_stats[0]}, "
                    f"Omkost={attr_stats[4]}/{attr_stats[0]}, tekst_f1={attr_stats[5]}/{attr_stats[0]}, "
                    f"tekst_d1={attr_stats[6]}/{attr_stats[0]}"
                )

                cursor.execute(
                    """
                MERGE [dim].[Kontoplan] AS tgt
                USING #Kontoplan_Attributes AS src
                ON tgt.dw_ek_Kontoplan = src.dw_ek_Kontoplan

                WHEN MATCHED THEN
                    UPDATE SET
                        tgt.clean_code_Kontoplan = src.clean_code_Kontoplan,
                        tgt.finanskonto_id = src.finanskonto_id,
                        tgt.kontonummer = src.kontonummer,
                        tgt.f1 = src.f1, tgt.f2 = src.f2, tgt.f3 = src.f3, tgt.d1 = src.d1,
                        tgt.s1 = src.s1, tgt.s2 = src.s2, tgt.s3 = src.s3, tgt.s4 = src.s4,
                        tgt.s5 = src.s5, tgt.s6 = src.s6, tgt.s7 = src.s7,
                        tgt.g1 = src.g1, tgt.g2 = src.g2,
                        tgt.a1 = src.a1, tgt.a2 = src.a2,
                        tgt.tekst_s1 = src.tekst_s1, tgt.tekst_s2 = src.tekst_s2,
                        tgt.tekst_s3 = src.tekst_s3, tgt.tekst_s4 = src.tekst_s4,
                        tgt.tekst_s5 = src.tekst_s5, tgt.tekst_s6 = src.tekst_s6,
                        tgt.tekst_s7 = src.tekst_s7,
                        tgt.tekst_g1 = src.tekst_g1, tgt.tekst_g2 = src.tekst_g2,
                        tgt.tekst_a1 = src.tekst_a1, tgt.tekst_a2 = src.tekst_a2,
                        tgt.tekst_f1 = src.tekst_f1, tgt.tekst_f2 = src.tekst_f2,
                        tgt.tekst_f3 = src.tekst_f3,
                        tgt.tekst_d1 = src.tekst_d1,
                        tgt.omkostningsstednummer = src.omkostningsstednummer,
                        tgt.omkostningssted_niveau = src.omkostningssted_niveau,
                        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (
                        dw_ek_Kontoplan, clean_code_Kontoplan, finanskonto_id, kontonummer,
                        f1, f2, f3, d1,
                        s1, s2, s3, s4, s5, s6, s7,
                        g1, g2, a1, a2,
                        tekst_s1, tekst_s2, tekst_s3, tekst_s4, tekst_s5, tekst_s6, tekst_s7,
                        tekst_g1, tekst_g2, tekst_a1, tekst_a2,
                        tekst_f1, tekst_f2, tekst_f3, tekst_d1,
                        omkostningsstednummer, omkostningssted_niveau,
                        dw_timestamp_insert
                    )
                    VALUES (
                        src.dw_ek_Kontoplan, src.clean_code_Kontoplan, src.finanskonto_id, src.kontonummer,
                        src.f1, src.f2, src.f3, src.d1,
                        src.s1, src.s2, src.s3, src.s4, src.s5, src.s6, src.s7,
                        src.g1, src.g2, src.a1, src.a2,
                        src.tekst_s1, src.tekst_s2, src.tekst_s3, src.tekst_s4,
                        src.tekst_s5, src.tekst_s6, src.tekst_s7,
                        src.tekst_g1, src.tekst_g2, src.tekst_a1, src.tekst_a2,
                        src.tekst_f1, src.tekst_f2, src.tekst_f3, src.tekst_d1,
                        src.omkostningsstednummer, src.omkostningssted_niveau,
                        CAST(SYSDATETIME() AS datetime2(0))
                    );
            """
                )

                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Kontoplan_Attributes")

                cursor.execute("UPDATE STATISTICS [dim].[Kontoplan]")

                context.log.info(f"Dimension sync complete: {merge_count} rows merged")
            finally:
                cursor.close()

        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Kontoplan", mode=config.mode)
            logged_cp = dfm.get_checkpoint("dim", "Kontoplan")

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=logged_cp, rows_affected=merge_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": merge_count,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Kontoplan",
            "source_tables": "stg.dim_Kontoplan_sydax + map.Kontoplan",
            "scd_type": "Type 1 (update in place)",
            "checkpoint_after": logged_cp,
            "mode": config.mode,
        }
    )
