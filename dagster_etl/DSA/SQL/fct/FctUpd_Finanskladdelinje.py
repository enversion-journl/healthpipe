# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Finanskladdelinje

Fact table for financial journal lines with DW_EK lookups from maps.
Grain: ref_LedgertransId

Reads from stg.fct_finanskladdelinje_sydax (Layer 1 staging).

DW_EK_Finanskladdelinje must resolve (no -1). Dimension FKs (Finanskladde, Kontoplan, Organisation)
may be -1 when unmatched.
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="fct_finanskladdelinje",
    group_name="erp_facts",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_finanskladdelinje_sydax"]),
        "map_finanskladde",
        "map_kontoplan",
        "map_okosted",
        "dim_finanskladde",
        "dim_kontoplan",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "journal_line",
    },
    description=(
        "Fact table for financial journal lines — required DW_EK_Finanskladdelinje; "
        "optional dim FKs via maps. MERGE on DW_EK_Finanskladdelinje."
    ),
)
def fct_finanskladdelinje(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    """Map lookups + MERGE into fct.Finanskladdelinje on surrogate key."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Finanskladdelinje fact table load...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_finanskladdelinje_sydax]")
            staging_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {staging_count}")

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Finanskladdelinje_Facts;

            SELECT
                src.ref_LedgertransId,
                COALESCE(lmap.DW_EK_Finanskladdelinje, -1) AS DW_EK_Finanskladdelinje,
                src.bk_ref_LedgertransId,
                COALESCE(kmap.DW_EK_Finanskladde, -1) AS DW_EK_Finanskladde,
                COALESCE(kpmap.DW_EK_Kontoplan, -1) AS DW_EK_Kontoplan,
                COALESCE(omap.DW_EK_Okosted, -1) AS DW_EK_Okosted,
                src.Debet,
                src.Kredit,
                src.DW_EK_Dato_Linje,
                src.DW_EK_Dato_Oprettet,
                src.DW_haendelsestidspunkt
            INTO #Finanskladdelinje_Facts
            FROM [stg].[fct_finanskladdelinje_sydax] src

            LEFT JOIN [map].[Finanskladdelinje] lmap
                ON lmap.CleanCode_Finanskladdelinje = src.CleanCode_Finanskladdelinje
               AND src.CleanCode_Finanskladdelinje IS NOT NULL
               AND src.CleanCode_Finanskladdelinje <> ''

            LEFT JOIN [map].[Finanskladde] kmap
                ON kmap.CleanCode_Finanskladde = src.CleanCode_Finanskladde
               AND src.CleanCode_Finanskladde IS NOT NULL
               AND src.CleanCode_Finanskladde <> ''

            LEFT JOIN [map].[Kontoplan] kpmap
                ON kpmap.CleanCode_Kontoplan = src.CleanCode_Kontoplan
               AND src.CleanCode_Kontoplan IS NOT NULL
               AND src.CleanCode_Kontoplan <> ''

            LEFT JOIN [map].[Okosted] omap
                ON omap.CleanCode_Okosted = src.Omkostningsstednummer
               AND src.Omkostningsstednummer IS NOT NULL
               AND src.Omkostningsstednummer <> '';
            """
            )

            cursor.execute(
                """
            IF EXISTS (
                SELECT 1
                FROM #Finanskladdelinje_Facts
                WHERE DW_EK_Finanskladdelinje = -1 OR DW_EK_Finanskladdelinje IS NULL
            )
                THROW 50002,
                    'fct_finanskladdelinje: unresolved DW_EK_Finanskladdelinje (-1 or NULL) after map join',
                    1;
            """
            )

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_source_rows,
                COUNT(DISTINCT ref_LedgertransId) AS distinct_ids
            FROM #Finanskladdelinje_Facts
        """
            )
            source_stats = cursor.fetchone()
            duplicate_source_rows = source_stats[0] - source_stats[1]
            if duplicate_source_rows > 0:
                message = (
                    "Duplicate grain violation in fct_finanskladdelinje: "
                    f"{duplicate_source_rows} duplicate source rows found in "
                    "#Finanskladdelinje_Facts. Expected one row per ref_LedgertransId."
                )
                context.log.error(message)
                raise ValueError(message)

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladdelinje_Facts
            ON #Finanskladdelinje_Facts (DW_EK_Finanskladdelinje)
            """
            )

            cursor.execute(
                """
            MERGE [fct].[Finanskladdelinje] AS tgt
            USING #Finanskladdelinje_Facts AS src
            ON tgt.DW_EK_Finanskladdelinje = src.DW_EK_Finanskladdelinje

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.bk_ref_LedgertransId = src.bk_ref_LedgertransId,
                    tgt.DW_EK_Finanskladde = src.DW_EK_Finanskladde,
                    tgt.DW_EK_Kontoplan = src.DW_EK_Kontoplan,
                    tgt.DW_EK_Okosted = src.DW_EK_Okosted,
                    tgt.Debet = src.Debet,
                    tgt.Kredit = src.Kredit,
                    tgt.DW_EK_Dato_Linje = src.DW_EK_Dato_Linje,
                    tgt.DW_EK_Dato_Oprettet = src.DW_EK_Dato_Oprettet,
                    tgt.DW_haendelsestidspunkt = src.DW_haendelsestidspunkt,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Finanskladdelinje,
                    bk_ref_LedgertransId,
                    DW_EK_Finanskladde,
                    DW_EK_Kontoplan,
                    DW_EK_Okosted,
                    Debet,
                    Kredit,
                    DW_EK_Dato_Linje,
                    DW_EK_Dato_Oprettet,
                    DW_haendelsestidspunkt,
                    DW_Timestamp_Insert,
                    DW_Timestamp_Update
                )
                VALUES (
                    src.DW_EK_Finanskladdelinje,
                    src.bk_ref_LedgertransId,
                    src.DW_EK_Finanskladde,
                    src.DW_EK_Kontoplan,
                    src.DW_EK_Okosted,
                    src.Debet,
                    src.Kredit,
                    src.DW_EK_Dato_Linje,
                    src.DW_EK_Dato_Oprettet,
                    src.DW_haendelsestidspunkt,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_facts,
                SUM(CASE WHEN DW_EK_Finanskladde = -1 THEN 1 ELSE 0 END) AS missing_finanskladde_fk,
                SUM(CASE WHEN DW_EK_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                SUM(CASE WHEN DW_EK_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk
            FROM [fct].[Finanskladdelinje]
        """
            )
            diag = cursor.fetchone()

            cursor.execute("DROP TABLE IF EXISTS #Finanskladdelinje_Facts")
            cursor.execute("UPDATE STATISTICS [fct].[Finanskladdelinje]")

            context.log.info(
                f"Finanskladdelinje fact load complete: {rows_affected} rows — "
                f"FK -1 counts: Finanskladde={diag[1]}, Kontoplan={diag[2]}, "
                f"Okosted={diag[3]} (of {diag[0]} total)"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Finanskladdelinje",
            "grain": "ref_LedgertransId",
            "staging_source": "stg.fct_finanskladdelinje_sydax (Layer 1)",
            "duplicate_source_rows": duplicate_source_rows,
            "missing_finanskladde_fk": diag[1],
            "missing_kontoplan_fk": diag[2],
            "missing_okosted_fk": diag[3],
        }
    )
