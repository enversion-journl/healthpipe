# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Faktura from MySupply (Fact Layer)

Aggregate MySupply faktura data at clean_code_Faktura level for fact table.
clean_code_Faktura = 'CleanCode_{metadata_id}_{header_id}' (metadata + header grain).
Derived from pre_stg_Fakturalinje_mysupply.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Summed allowance/charges per header. GebyrKategoriIndikering separates
# charges (true/NULL -> gebyr_beloeb) from discounts (false -> rabatbeloeb);
# a single SUM would add discounts to fees.
# Shared with integration tests (test_allowance_charge_transforms.py) — keep importable.
HAC_SUM_CTE_SQL = """\
;WITH hac_sum AS (
    SELECT
        statistic_invoice_header_id,
        SUM(CASE WHEN LOWER(GebyrKategoriIndikering) = 'false' THEN 0
                 ELSE ISNULL(TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)), 0) END
        ) AS gebyr_beloeb,
        SUM(CASE WHEN LOWER(GebyrKategoriIndikering) = 'false'
                 THEN ISNULL(TRY_CAST(GebyrKategoriTotal AS DECIMAL(18,4)), 0) ELSE 0 END
        ) AS rabatbeloeb
    FROM [arc].[mysupply_statistic_invoice_header_allowance_charge]
    GROUP BY statistic_invoice_header_id
)
"""

# Measure projection used after LEFT JOIN hac_sum hac. ISNULL keeps headers
# without any allowance rows at 0, matching headers whose rows all sum to 0.
HAC_MEASURE_COLUMNS_SQL = """\
ISNULL(hac.gebyr_beloeb, 0) AS gebyr_beloeb,
                    ISNULL(hac.rabatbeloeb, 0) AS rabatbeloeb"""


@asset(
    key=AssetKey(["erp", "stg_fct_Faktura_mysupply"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Fakturalinje_mysupply"]),
        AssetKey(["arc_mysupply_statistic_invoice_header_allowance_charge"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "mysupply",
        "entity": "faktura",
    },
    description="Aggregate MySupply faktura data at metadata+header grain for fact table - derived from pre_stg_Fakturalinje_mysupply",
)
def stg_fct_Faktura_mysupply(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for MySupply faktura (invoice) fact data.

    Aggregates pre_stg_Fakturalinje_mysupply at metadata_id + header_id grain:
    - clean_code_Faktura = 'CleanCode_{metadata_id}_{header_id}' for map.Faktura (header key)
    - bk_faktura_metadata / bk_faktura_header exposed for map insert
    - clean_code_Kreditor kept for dimension joins
    - Selects most recent record (by header_dw_timestamp_update DESC)
    - Deduplicates to one row per clean_code_Faktura
    - Includes invoice-level measures: moms_procent, faktura_total, etc.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_Faktura_mysupply from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Faktura_mysupply];")

            cursor.execute(
                HAC_SUM_CTE_SQL
                + f"""
                SELECT
                    clean_code_Faktura,
                    bk_faktura_metadata,
                    bk_faktura_header,
                    clean_code_Kreditor,
                    clean_code_EAN_Regningsmodtager,
                    faktura_nr,
                    FakturaDato,
                    OrdreDato,
                    BetalingsDato,
                    moms_procent,
                    moms_grundlag,
                    momsbeloeb,
                    linje_total,
                    HeaderFakturaTotal,
                    MetadataFakturaTotal,
                    faktura_valuta_kode,
                    vat_afgifts_kategori,
                    MomsGrundlag_Valuta,
                    MomsBeloeb_Valuta,
                    FakturaLinjeTotal_Valuta,
                    FakturaTotal_Valuta,
                    zero_rated_moms_grundlag,
                    ZeroRated_MomsGrundlag_Valuta,
                    zero_rated_momsbeloeb,
                    ZeroRated_MomsBeloeb_Valuta,
                    {HAC_MEASURE_COLUMNS_SQL}
                INTO [stg].[fct_Faktura_mysupply]
                FROM (
                    SELECT
                        statistic_invoice_header_id,
                        CONCAT('CleanCode_',
                            CAST(metadata_id AS NVARCHAR(50)), '_',
                            CAST(statistic_invoice_header_id AS NVARCHAR(50))
                        ) AS clean_code_Faktura,
                        clean_code_FakturaMetadata AS bk_faktura_metadata,
                        clean_code_FakturaHeader AS bk_faktura_header,
                        clean_code_Kreditor,
                        clean_code_EAN_Regningsmodtager,
                        faktura_nr,
                        FakturaDato,
                        OrdreDato,
                        BetalingsDato,
                        moms_procent,
                        moms_grundlag,
                        momsbeloeb,
                        linje_total,
                        HeaderFakturaTotal,
                        MetadataFakturaTotal,
                        faktura_valuta_kode,
                        vat_afgifts_kategori,
                        MomsGrundlag_Valuta,
                        MomsBeloeb_Valuta,
                        FakturaLinjeTotal_Valuta,
                        FakturaTotal_Valuta,
                        zero_rated_moms_grundlag,
                        ZeroRated_MomsGrundlag_Valuta,
                        zero_rated_momsbeloeb,
                        ZeroRated_MomsBeloeb_Valuta,
                        ROW_NUMBER() OVER (
                            PARTITION BY metadata_id, statistic_invoice_header_id
                            ORDER BY header_dw_timestamp_update DESC
                        ) AS rn
                    FROM [stg].[pre_Fakturalinje_mysupply]
                    WHERE metadata_id IS NOT NULL
                      AND statistic_invoice_header_id IS NOT NULL
                ) ranked
                LEFT JOIN hac_sum hac
                    ON hac.statistic_invoice_header_id = ranked.statistic_invoice_header_id
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_Faktura_mysupply with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_fct_faktura_mysupply_CompositeKey
                ON [stg].[fct_Faktura_mysupply](clean_code_Faktura);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(faktura_nr) AS has_faktura_nr,
                    COUNT(moms_procent) AS has_moms_procent,
                    COUNT(COALESCE(HeaderFakturaTotal, MetadataFakturaTotal)) AS has_faktura_total
                FROM [stg].[fct_Faktura_mysupply]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with faktura_nr, "
                f"{stats[2]}/{stats[0]} with moms_procent, "
                f"{stats[3]}/{stats[0]} with faktura_total"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Faktura_mysupply",
            "row_count": row_count,
            "source_table": "stg.pre_Fakturalinje_mysupply",
            "composite_key": "clean_code_Faktura (CleanCode_{metadata_id}_{header_id})",
            "deduplication": "ROW_NUMBER() on (metadata_id, header_id) ORDER BY header_dw_timestamp_update DESC",
            "has_faktura_nr": stats[1],
            "has_moms_procent": stats[2],
            "has_faktura_total": stats[3],
            "layer": "stg_fct (Layer 1)",
        }
    )
