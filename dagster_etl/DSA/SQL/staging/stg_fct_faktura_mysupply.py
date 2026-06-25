# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Faktura from MySupply (Fact Layer)

Aggregate MySupply faktura data at CleanCode_Faktura level for fact table.
CleanCode_Faktura = 'CleanCode_{metadata_id}_{header_id}' (metadata + header grain).
Derived from pre_stg_fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Summed allowance/charges per header. GebyrKategoriIndikering separates
# charges (true/NULL -> GebyrBeloeb) from discounts (false -> RabatBeloeb);
# a single SUM would add discounts to fees.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
HAC_SUM_CTE_SQL = """\
;WITH hac_sum AS (
    SELECT
        statistic_invoice_header_id,
        SUM(CASE WHEN LOWER(GebyrKategoriIndikering) = 'false' THEN 0
                 ELSE ISNULL(TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)), 0) END
        ) AS GebyrBeloeb,
        SUM(CASE WHEN LOWER(GebyrKategoriIndikering) = 'false'
                 THEN ISNULL(TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)), 0) ELSE 0 END
        ) AS RabatBeloeb
    FROM [ext].[statistic_invoice_header_allowance_charge_mysupply]
    GROUP BY statistic_invoice_header_id
)
"""

# Measure projection used after LEFT JOIN hac_sum hac. ISNULL keeps headers
# without any allowance rows at 0, matching headers whose rows all sum to 0.
HAC_MEASURE_COLUMNS_SQL = """\
ISNULL(hac.GebyrBeloeb, 0) AS GebyrBeloeb,
                    ISNULL(hac.RabatBeloeb, 0) AS RabatBeloeb"""


@asset(
    key=AssetKey(["erp", "stg_fct_faktura_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_daily"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_monthly"]),
        AssetKey(["ext_statistic_invoice_header_allowance_charge_mysupply_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "faktura",
    },
    description="Aggregate MySupply faktura data at metadata+header grain for fact table - derived from pre_stg_fakturalinje_mysupply",
)
def stg_fct_faktura_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply faktura (invoice) fact data.

    Aggregates pre_stg_fakturalinje_mysupply at metadata_id + header_id grain:
    - CleanCode_Faktura = 'CleanCode_{metadata_id}_{header_id}' for map.Faktura (header key)
    - bk_FakturaMetadata / bk_FakturaHeader exposed for map insert
    - CleanCode_Kreditor kept for dimension joins
    - Selects most recent record (by Header_DW_LoadTimestamp DESC)
    - Deduplicates to one row per CleanCode_Faktura
    - Includes invoice-level measures: MomsProcent, FakturaTotal, etc.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_faktura_mysupply from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_faktura_mysupply];")

            cursor.execute(
                HAC_SUM_CTE_SQL
                + f"""
                SELECT
                    CleanCode_Faktura,
                    bk_FakturaMetadata,
                    bk_FakturaHeader,
                    CleanCode_Kreditor,
                    CleanCode_EAN_Regningsmodtager,
                    FakturaNr,
                    FakturaDato,
                    OrdreDato,
                    BetalingsDato,
                    MomsProcent,
                    MomsGrundlag,
                    MomsBeloeb,
                    LinjeTotal,
                    HeaderFakturaTotal,
                    MetadataFakturaTotal,
                    FakturaValutaKode,
                    VAT_AfgiftsKategori,
                    MomsGrundlag_Valuta,
                    MomsBeloeb_Valuta,
                    FakturaLinjeTotal_Valuta,
                    FakturaTotal_Valuta,
                    ZeroRated_MomsGrundlag,
                    ZeroRated_MomsGrundlag_Valuta,
                    ZeroRated_MomsBeloeb,
                    ZeroRated_MomsBeloeb_Valuta,
                    {HAC_MEASURE_COLUMNS_SQL}
                INTO [stg].[fct_faktura_mysupply]
                FROM (
                    SELECT
                        statistic_invoice_header_id,
                        CONCAT('CleanCode_',
                            CAST(metadata_id AS NVARCHAR(50)), '_',
                            CAST(statistic_invoice_header_id AS NVARCHAR(50))
                        ) AS CleanCode_Faktura,
                        CleanCode_FakturaMetadata AS bk_FakturaMetadata,
                        CleanCode_FakturaHeader AS bk_FakturaHeader,
                        CleanCode_Kreditor,
                        CleanCode_EAN_Regningsmodtager,
                        FakturaNr,
                        FakturaDato,
                        OrdreDato,
                        BetalingsDato,
                        MomsProcent,
                        MomsGrundlag,
                        MomsBeloeb,
                        LinjeTotal,
                        HeaderFakturaTotal,
                        MetadataFakturaTotal,
                        FakturaValutaKode,
                        VAT_AfgiftsKategori,
                        MomsGrundlag_Valuta,
                        MomsBeloeb_Valuta,
                        FakturaLinjeTotal_Valuta,
                        FakturaTotal_Valuta,
                        ZeroRated_MomsGrundlag,
                        ZeroRated_MomsGrundlag_Valuta,
                        ZeroRated_MomsBeloeb,
                        ZeroRated_MomsBeloeb_Valuta,
                        ROW_NUMBER() OVER (
                            PARTITION BY metadata_id, statistic_invoice_header_id
                            ORDER BY Header_DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_fakturalinje_mysupply]
                    WHERE metadata_id IS NOT NULL
                      AND statistic_invoice_header_id IS NOT NULL
                ) ranked
                LEFT JOIN hac_sum hac
                    ON hac.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_faktura_mysupply with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_fct_faktura_mysupply_CompositeKey
                ON [stg].[fct_faktura_mysupply](CleanCode_Faktura);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(FakturaNr) AS has_faktura_nr,
                    COUNT(MomsProcent) AS has_moms_procent,
                    COUNT(COALESCE(HeaderFakturaTotal, MetadataFakturaTotal)) AS has_faktura_total
                FROM [stg].[fct_faktura_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with FakturaNr, "
                f"{stats[2]}/{stats[0]} with MomsProcent, "
                f"{stats[3]}/{stats[0]} with FakturaTotal"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_faktura_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_fakturalinje_mysupply",
            "composite_key": "CleanCode_Faktura (CleanCode_{metadata_id}_{header_id})",
            "deduplication": "ROW_NUMBER() on (metadata_id, header_id) ORDER BY Header_DW_LoadTimestamp DESC",
            "has_faktura_nr": stats[1],
            "has_moms_procent": stats[2],
            "has_faktura_total": stats[3],
            "layer": "stg_fct (Layer 1)",
        }
    )
