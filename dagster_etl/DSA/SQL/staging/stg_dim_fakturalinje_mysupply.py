# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Fakturalinje from MySupply (Dimension Layer)

Deduplicate MySupply fakturalinje data at CleanCode_Fakturalinje level.
Derived from pre_stg_fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Allowance/charges are 1:n per line, split into Rabat (Indikering='false')
# and Tillaeg (Indikering='true'/NULL). Rows with zero/null Total are excluded.
# Aarsag is a composite '{Aarsag}:{Total}' per position. Columns are '#'-delimited,
# position-aligned (ORDER BY id), '#' stripped from source values and ':' stripped
# from Aarsag so the composite stays parseable.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
LAC_DIM_CTE_SQL = """\
;WITH lac_base AS (
    SELECT *
    FROM [ext].[statistic_invoice_line_allowance_charge_mysupply]
    WHERE FakturaUdstederGebyrKategoriTotal IS NOT NULL
      AND TRY_CAST(FakturaUdstederGebyrKategoriTotal AS DECIMAL(18,4)) <> 0
),
lac_rabat AS (
    SELECT
        statistic_invoice_line_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([FakturaUdstederGebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(FakturaUdstederGebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrRabatAarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrRabatKategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategoriFinanskonto, ''), '#', '')
                 AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrRabatFinanskonto
    FROM lac_base
    WHERE LOWER(FakturaUdstederGebyrKategoriIndikering) = 'false'
    GROUP BY statistic_invoice_line_id
),
lac_tillaeg AS (
    SELECT
        statistic_invoice_line_id,
        NULLIF(STRING_AGG(
            CAST(CONCAT(
                REPLACE(REPLACE(ISNULL([FakturaUdstederGebyrKategoriÅrsag], ''), '#', ''), ':', ''), ':',
                REPLACE(ISNULL(FakturaUdstederGebyrKategoriTotal, ''), '#', '')
            ) AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrTillaegAarsag,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategori, ''), '#', '') AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrTillaegKategori,
        NULLIF(STRING_AGG(
            CAST(REPLACE(ISNULL(FakturaUdstederGebyrKategoriFinanskonto, ''), '#', '')
                 AS NVARCHAR(MAX)), '#')
            WITHIN GROUP (ORDER BY id), '') AS FakturaUdstederGebyrTillaegFinanskonto
    FROM lac_base
    WHERE LOWER(FakturaUdstederGebyrKategoriIndikering) <> 'false'
       OR FakturaUdstederGebyrKategoriIndikering IS NULL
    GROUP BY statistic_invoice_line_id
)
"""


@asset(
    key=AssetKey(["erp", "stg_dim_fakturalinje_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_line_allowance_charge_mysupply_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "mysupply",
        "entity": "fakturalinje",
    },
    description="Deduplicate MySupply fakturalinje at CleanCode_Fakturalinje level for dim.Fakturalinje",
)
def stg_dim_fakturalinje_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply fakturalinje dimension data.

    Deduplicates pre_stg_fakturalinje_mysupply at CleanCode_Fakturalinje grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info(
                "Creating staging table stg.dim_fakturalinje_mysupply from base layer..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_fakturalinje_mysupply];")

            cursor.execute(
                LAC_DIM_CTE_SQL
                + """
                SELECT
                    ranked.CleanCode_Fakturalinje,
                    ranked.CleanCode_FakturaHeader,
                    ranked.CleanCode_FakturaLines,
                    ranked.CleanCode_FakturaMetadata,
                    ranked.CleanCode_FakturaHeader AS bk_FakturaHeader,
                    ranked.CleanCode_FakturaLines AS bk_FakturaLines,
                    ranked.CleanCode_FakturaMetadata AS bk_FakturaMetadata,
                    ranked.Item_FakturaUdstederVareNr,
                    ranked.Item_FakturaUdstederVareNavn,
                    ranked.FakturaLinjeNr,
                    ranked.AntalEnhederFaktureret_Type,
                    ranked.Item_FakturaUdstederensVareNr,
                    ranked.Item_FakturaUdstederensVareNr_Type,
                    ranked.BasePrice_EnhedsStørrelse,
                    ranked.BasePrice_EnhedsStørrelse_Type,
                    ranked.Item_FakturaUdstederVareNavnKort,
                    ranked.Line_DimensionsKontoStreng,
                    ranked.LeveranceKrav,
                    ranked.LeveringsDato_2,
                    ranked.Item_KundensVareNr,
                    ranked.Item_KundensVareNr_Type,
                    ranked.Item_KundensVareBeskrivelse,
                    ranked.Item_FakturaUdstederensVareBeskrivelse,
                    ranked.Item_FabrikantensVareNr,
                    ranked.Item_FabrikantensVareNr_Type,
                    ranked.Item_FabrikantensVareBeskrivelse,
                    ranked.Item_StandardiseretVareNr,
                    ranked.Item_StandardiseretVareNr_Type,
                    ranked.Item_StandardiseretVareBeskrivelse,
                    ranked.Item_VareKlassifikation,
                    ranked.Item_VareKlassifikation_Type,
                    ranked.Item_AfgiftsKategori,
                    ranked.Item_AfgiftsKategoriProcent,
                    lac_r.FakturaUdstederGebyrRabatAarsag,
                    lac_r.FakturaUdstederGebyrRabatKategori,
                    lac_r.FakturaUdstederGebyrRabatFinanskonto,
                    lac_t.FakturaUdstederGebyrTillaegAarsag,
                    lac_t.FakturaUdstederGebyrTillaegKategori,
                    lac_t.FakturaUdstederGebyrTillaegFinanskonto
                INTO [stg].[dim_fakturalinje_mysupply]
                FROM (
                    SELECT
                        CleanCode_Fakturalinje,
                        CleanCode_FakturaHeader,
                        CleanCode_FakturaLines,
                        CleanCode_FakturaMetadata,
                        Item_FakturaUdstederVareNr,
                        Item_FakturaUdstederVareNavn,
                        FakturaLinjeNr,
                        AntalEnhederFaktureret_Type,
                        Item_FakturaUdstederensVareNr,
                        Item_FakturaUdstederensVareNr_Type,
                        [BasePrice_EnhedsStørrelse],
                        [BasePrice_EnhedsStørrelse_Type],
                        Item_FakturaUdstederVareNavnKort,
                        Line_DimensionsKontoStreng,
                        LeveranceKrav,
                        LeveringsDato_2,
                        Item_KundensVareNr,
                        Item_KundensVareNr_Type,
                        Item_KundensVareBeskrivelse,
                        Item_FakturaUdstederensVareBeskrivelse,
                        Item_FabrikantensVareNr,
                        Item_FabrikantensVareNr_Type,
                        Item_FabrikantensVareBeskrivelse,
                        Item_StandardiseretVareNr,
                        Item_StandardiseretVareNr_Type,
                        Item_StandardiseretVareBeskrivelse,
                        Item_VareKlassifikation,
                        Item_VareKlassifikation_Type,
                        Item_AfgiftsKategori,
                        Item_AfgiftsKategoriProcent,
                        statistic_invoice_line_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Fakturalinje
                            ORDER BY Header_DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE CleanCode_Fakturalinje IS NOT NULL
                ) ranked
                LEFT JOIN lac_rabat lac_r
                    ON lac_r.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                LEFT JOIN lac_tillaeg lac_t
                    ON lac_t.statistic_invoice_line_id = ranked.statistic_invoice_line_id
                WHERE ranked.rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_fakturalinje_mysupply with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_fakturalinje_mysupply_CleanCode
                ON [stg].[dim_fakturalinje_mysupply](CleanCode_Fakturalinje);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Item_FakturaUdstederVareNavn) AS has_vare_navn,
                    COUNT(FakturaUdstederGebyrRabatAarsag) AS has_gebyr_rabat,
                    COUNT(FakturaUdstederGebyrTillaegAarsag) AS has_gebyr_tillaeg
                FROM [stg].[dim_fakturalinje_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with VareNavn, "
                f"{stats[2]}/{stats[0]} with GebyrRabat, "
                f"{stats[3]}/{stats[0]} with GebyrTillaeg"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_fakturalinje_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply + ext.statistic_invoice_line_allowance_charge_mysupply",
            "deduplication": "ROW_NUMBER() on CleanCode_Fakturalinje",
            "has_vare_navn": stats[1],
            "has_gebyr_rabat": stats[2],
            "has_gebyr_tillaeg": stats[3],
            "layer": "dim (Layer 1)",
        }
    )
