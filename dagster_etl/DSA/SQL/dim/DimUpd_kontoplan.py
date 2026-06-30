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
  map.Kontoplan (driver) -> stg.dim_kontoplan_sydax (enriched in stg layer)
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_kontoplan"]),
        AssetKey(["erp", "stg_dim_kontoplan_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Kontoplan from map + add attributes from enriched SydAX staging",
)
def dim_kontoplan(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: sync from map and enrich from staging."""

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
            FROM [stg].[dim_kontoplan_sydax] src
            LEFT JOIN [map].[Kontoplan] m
                ON m.CleanCode_Kontoplan = src.CleanCode_Kontoplan
            WHERE m.DW_EK_Kontoplan IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(
                    f"Found {unmapped_rows} stg.dim_kontoplan_sydax rows without map match"
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Kontoplan_Attributes;

            SELECT
                m.DW_EK_Kontoplan,
                m.CleanCode_Kontoplan,
                COALESCE(src.FinanskontoId, m.bk_FinanskontoId) AS FinanskontoId,
                COALESCE(src.Kontonummer, m.bk_Kontonummer) AS Kontonummer,
                src.F1,
                src.F2,
                src.F3,
                src.D1,
                src.G1,
                src.S1,
                src.S2,
                src.S3,
                src.S4,
                src.S5,
                src.S6,
                src.S7,
                src.G2,
                src.A1,
                src.A2,
                src.tekst_S1,
                src.tekst_S2,
                src.tekst_S3,
                src.tekst_S4,
                src.tekst_S5,
                src.tekst_S6,
                src.tekst_S7,
                src.tekst_G1,
                src.tekst_G2,
                src.tekst_A1,
                src.tekst_A2,
                src.tekst_F1,
                src.tekst_F2,
                src.tekst_F3,
                src.tekst_D1,
                src.Omkostningsstednummer,
                src.Omkostningssted_niveau
            INTO #Kontoplan_Attributes
            FROM [stg].[dim_kontoplan_sydax] src
            LEFT JOIN [map].[Kontoplan] m
                ON m.CleanCode_Kontoplan = src.CleanCode_Kontoplan
            WHERE m.DW_EK_Kontoplan IS NOT NULL;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Kontoplan_Attributes
            ON #Kontoplan_Attributes (DW_EK_Kontoplan)
            """
            )
            context.log.info("Temp table #Kontoplan_Attributes created successfully")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total,
                COUNT(S1) AS has_s1,
                COUNT(F1) AS has_f1,
                COUNT(G2) AS has_g2,
                COUNT(Omkostningsstednummer) AS has_omkost,
                COUNT(tekst_F1) AS has_tekst_f1,
                COUNT(tekst_D1) AS has_tekst_d1
            FROM #Kontoplan_Attributes
        """
            )
            attr_stats = cursor.fetchone()
            context.log.info(
                f"Attribute coverage: S1={attr_stats[1]}/{attr_stats[0]}, "
                f"F1={attr_stats[2]}/{attr_stats[0]}, G2={attr_stats[3]}/{attr_stats[0]}, "
                f"Omkost={attr_stats[4]}/{attr_stats[0]}, tekst_F1={attr_stats[5]}/{attr_stats[0]}, "
                f"tekst_D1={attr_stats[6]}/{attr_stats[0]}"
            )

            cursor.execute(
                """
            MERGE [dim].[Kontoplan] AS tgt
            USING #Kontoplan_Attributes AS src
            ON tgt.DW_EK_Kontoplan = src.DW_EK_Kontoplan

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.CleanCode_Kontoplan = src.CleanCode_Kontoplan,
                    tgt.FinanskontoId = src.FinanskontoId,
                    tgt.Kontonummer = src.Kontonummer,
                    tgt.F1 = src.F1, tgt.F2 = src.F2, tgt.F3 = src.F3, tgt.D1 = src.D1,
                    tgt.S1 = src.S1, tgt.S2 = src.S2, tgt.S3 = src.S3, tgt.S4 = src.S4,
                    tgt.S5 = src.S5, tgt.S6 = src.S6, tgt.S7 = src.S7,
                    tgt.G1 = src.G1, tgt.G2 = src.G2,
                    tgt.A1 = src.A1, tgt.A2 = src.A2,
                    tgt.tekst_S1 = src.tekst_S1, tgt.tekst_S2 = src.tekst_S2,
                    tgt.tekst_S3 = src.tekst_S3, tgt.tekst_S4 = src.tekst_S4,
                    tgt.tekst_S5 = src.tekst_S5, tgt.tekst_S6 = src.tekst_S6,
                    tgt.tekst_S7 = src.tekst_S7,
                    tgt.tekst_G1 = src.tekst_G1, tgt.tekst_G2 = src.tekst_G2,
                    tgt.tekst_A1 = src.tekst_A1, tgt.tekst_A2 = src.tekst_A2,
                    tgt.tekst_F1 = src.tekst_F1, tgt.tekst_F2 = src.tekst_F2,
                    tgt.tekst_F3 = src.tekst_F3,
                    tgt.tekst_D1 = src.tekst_D1,
                    tgt.Omkostningsstednummer = src.Omkostningsstednummer,
                    tgt.Omkostningssted_niveau = src.Omkostningssted_niveau,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Kontoplan, CleanCode_Kontoplan, FinanskontoId, Kontonummer,
                    F1, F2, F3, D1,
                    S1, S2, S3, S4, S5, S6, S7,
                    G1, G2, A1, A2,
                    tekst_S1, tekst_S2, tekst_S3, tekst_S4, tekst_S5, tekst_S6, tekst_S7,
                    tekst_G1, tekst_G2, tekst_A1, tekst_A2,
                    tekst_F1, tekst_F2, tekst_F3, tekst_D1,
                    Omkostningsstednummer, Omkostningssted_niveau,
                    DW_Timestamp_Insert
                )
                VALUES (
                    src.DW_EK_Kontoplan, src.CleanCode_Kontoplan, src.FinanskontoId, src.Kontonummer,
                    src.F1, src.F2, src.F3, src.D1,
                    src.S1, src.S2, src.S3, src.S4, src.S5, src.S6, src.S7,
                    src.G1, src.G2, src.A1, src.A2,
                    src.tekst_S1, src.tekst_S2, src.tekst_S3, src.tekst_S4,
                    src.tekst_S5, src.tekst_S6, src.tekst_S7,
                    src.tekst_G1, src.tekst_G2, src.tekst_A1, src.tekst_A2,
                    src.tekst_F1, src.tekst_F2, src.tekst_F3, src.tekst_D1,
                    src.Omkostningsstednummer, src.Omkostningssted_niveau,
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Kontoplan_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Kontoplan]")

            context.log.info(f"Dimension sync complete: {rows_affected} rows merged")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Kontoplan",
            "source_tables": "stg.dim_kontoplan_sydax + map.Kontoplan",
            "scd_type": "Type 1 (update in place)",
        }
    )
