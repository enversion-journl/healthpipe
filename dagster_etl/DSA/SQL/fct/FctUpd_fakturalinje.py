# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Fakturalinje

Fact table for invoice lines with DW_EK_Kreditor, DW_EK_Vare lookups from maps.
DW_EK_UNSPSC resolved via stg.kmd_fakturalinje_unspsc_match (Option 2, tiered):
  Tier 1: CleanCode_Kreditor + CleanCode_Faktura + line amount + qty + unit price
  Tier 2: CleanCode_Kreditor + CleanCode_Faktura + line amount + qty
  Tier 3: CleanCode_Kreditor + CleanCode_Faktura + invoice total (metadata.FakturaTotal)
  Match table built in staging from KMD arc tables (aggregate → expand → dedup).
  MySupply CleanCode_Kreditor from metadata.LeverandorCVR (maps to KMD SupplierCompanyTaxID).

Reads from stg.fct_fakturalinje_mysupply (Layer 1 staging).
"""

import traceback

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

RESOLVE_SQL = """
DROP TABLE IF EXISTS #Fakturalinje_Facts;

SELECT
    src.CleanCode_Fakturalinje,
    src.bk_FakturaLines,
    src.bk_FakturaHeader,
    src.bk_FakturaMetadata,

    COALESCE(kred_map.DW_EK_Kreditor, -1) AS DW_EK_Kreditor,
    COALESCE(vare_map.DW_EK_Vare, -1) AS DW_EK_Vare,
    COALESCE(fak_map.DW_EK_Faktura, -1) AS DW_EK_Faktura,

    COALESCE(ean_map.DW_EK_EAN, -1) AS DW_EK_EAN_Regningsmodtager,
    COALESCE(kmd1.DW_EK_UNSPSC, kmd2.DW_EK_UNSPSC, kmd3.DW_EK_UNSPSC, -1) AS DW_EK_UNSPSC,

    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.FakturaDato)), -1)
        AS DW_EK_Dato_Faktura,
    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.OrdreDato)), -1)
        AS DW_EK_Dato_Ordre,
    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, src.BetalingsDato)), -1)
        AS DW_EK_Dato_Betaling,
    CAST(-1 AS INT) AS DW_EK_Dato_Modtagelse,

    CAST(src.FakturaNr AS NVARCHAR(100)) AS FakturaNr,
    TRY_CONVERT(DECIMAL(18, 4), src.AntalEnhederFaktureret) AS AntalEnhederFaktureret,
    TRY_CONVERT(DECIMAL(18, 4), src.LinjeBeloeb) AS LinjeBeloeb,
    TRY_CONVERT(DECIMAL(18, 4), src.BasePrice_EnhedsPris) AS EnhedsPris,
    CASE WHEN ISNUMERIC(src.LinjeBeloeb_Valuta) = 0
         THEN LEFT(CAST(src.LinjeBeloeb_Valuta AS NVARCHAR(10)), 3)
         ELSE NULL END AS Valutakode_Linje,
    CASE WHEN ISNUMERIC(src.BasePrice_EnhedsPris_Valuta) = 0
         THEN LEFT(CAST(src.BasePrice_EnhedsPris_Valuta AS NVARCHAR(10)), 3)
         ELSE NULL END AS Valutakode_Enhedspris,
    CASE WHEN ISNUMERIC(src.FakturaValutaKode) = 0
         THEN LEFT(CAST(src.FakturaValutaKode AS NVARCHAR(10)), 3)
         ELSE NULL END AS Valutakode_Faktura,

    -- Summed allowance/charges (already DECIMAL from staging)
    src.GebyrBeloeb,
    src.RabatBeloeb

INTO #Fakturalinje_Facts
FROM [stg].[fct_fakturalinje_mysupply] src

LEFT JOIN [map].[Kreditor] kred_map
    ON kred_map.CleanCode_Kreditor = src.CleanCode_Kreditor
    AND src.CleanCode_Kreditor IS NOT NULL
    AND src.CleanCode_Kreditor <> ''

LEFT JOIN [map].[Vare] vare_map
    ON vare_map.CleanCode_Vare = src.CleanCode_Vare
   AND src.CleanCode_Vare IS NOT NULL
   AND src.CleanCode_Vare <> ''

LEFT JOIN [map].[EAN] ean_map
    ON ean_map.CleanCode_EAN = src.CleanCode_EAN_Regningsmodtager
   AND src.CleanCode_EAN_Regningsmodtager IS NOT NULL
   AND src.CleanCode_EAN_Regningsmodtager <> ''

LEFT JOIN [map].[Faktura] fak_map
    ON fak_map.CleanCode_Faktura = CONCAT('CleanCode_', src.bk_FakturaMetadata, '_', src.bk_FakturaHeader)
   AND src.bk_FakturaMetadata IS NOT NULL
   AND src.bk_FakturaMetadata <> ''
   AND src.bk_FakturaHeader IS NOT NULL
   AND src.bk_FakturaHeader <> ''

LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd1
    ON kmd1.[MatchTier] = 1
   AND kmd1.[CleanCode_Kreditor] = src.[CleanCode_Kreditor]
   AND kmd1.[CleanCode_Faktura] = src.[CleanCode_Faktura]
   AND kmd1.[LineAmount_Sum] = TRY_CONVERT(DECIMAL(18, 4), src.[LinjeBeloeb])
   AND kmd1.[InvoicedQuantity] = TRY_CONVERT(DECIMAL(18, 4), src.[AntalEnhederFaktureret])
   AND kmd1.[UnitPrice] = TRY_CONVERT(DECIMAL(18, 4), src.[BasePrice_EnhedsPris])
   AND src.[CleanCode_Kreditor] IS NOT NULL
   AND src.[CleanCode_Kreditor] <> ''
   AND src.[CleanCode_Faktura] IS NOT NULL
   AND src.[CleanCode_Faktura] <> ''

LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd2
    ON kmd2.[MatchTier] = 2
   AND kmd2.[CleanCode_Kreditor] = src.[CleanCode_Kreditor]
   AND kmd2.[CleanCode_Faktura] = src.[CleanCode_Faktura]
   AND kmd2.[LineAmount_Sum] = TRY_CONVERT(DECIMAL(18, 4), src.[LinjeBeloeb])
   AND kmd2.[InvoicedQuantity] = TRY_CONVERT(DECIMAL(18, 4), src.[AntalEnhederFaktureret])
   AND src.[CleanCode_Kreditor] IS NOT NULL
   AND src.[CleanCode_Kreditor] <> ''
   AND src.[CleanCode_Faktura] IS NOT NULL
   AND src.[CleanCode_Faktura] <> ''

LEFT JOIN [stg].[kmd_fakturalinje_unspsc_match] kmd3
    ON kmd3.[MatchTier] = 3
   AND kmd3.[CleanCode_Kreditor] = src.[CleanCode_Kreditor]
   AND kmd3.[CleanCode_Faktura] = src.[CleanCode_Faktura]
   AND kmd3.[InvoiceTotal] = TRY_CONVERT(
        DECIMAL(18, 4),
        COALESCE(src.[MetadataFakturaTotal], src.[HeaderFakturaTotal])
   )
   AND src.[LinjeBeloeb] IS NULL
   AND src.[BasePrice_EnhedsPris] IS NULL
   AND src.[CleanCode_Kreditor] IS NOT NULL
   AND src.[CleanCode_Kreditor] <> ''
   AND src.[CleanCode_Faktura] IS NOT NULL
   AND src.[CleanCode_Faktura] <> '';
"""

RESOLVE_INDEX_SQL = """
CREATE UNIQUE CLUSTERED INDEX IX_tmp_Fakturalinje_Facts
    ON #Fakturalinje_Facts (CleanCode_Fakturalinje);
"""

MERGE_SQL = """
MERGE [fct].[Fakturalinje] AS tgt
USING #Fakturalinje_Facts AS src
ON tgt.CleanCode_Fakturalinje = src.CleanCode_Fakturalinje

WHEN MATCHED THEN
    UPDATE SET
        tgt.bk_FakturaLines = src.bk_FakturaLines,
        tgt.bk_FakturaHeader = src.bk_FakturaHeader,
        tgt.bk_FakturaMetadata = src.bk_FakturaMetadata,
        tgt.DW_EK_Kreditor = src.DW_EK_Kreditor,
        tgt.DW_EK_Vare = src.DW_EK_Vare,
        tgt.DW_EK_Faktura = src.DW_EK_Faktura,
        tgt.DW_EK_EAN_Regningsmodtager = src.DW_EK_EAN_Regningsmodtager,
        tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC,
        tgt.DW_EK_Dato_Faktura = src.DW_EK_Dato_Faktura,
        tgt.DW_EK_Dato_Ordre = src.DW_EK_Dato_Ordre,
        tgt.DW_EK_Dato_Betaling = src.DW_EK_Dato_Betaling,
        tgt.DW_EK_Dato_Modtagelse = src.DW_EK_Dato_Modtagelse,
        tgt.FakturaNr = src.FakturaNr,
        tgt.AntalEnhederFaktureret = src.AntalEnhederFaktureret,
        tgt.LinjeBeloeb = src.LinjeBeloeb,
        tgt.EnhedsPris = src.EnhedsPris,
        tgt.Valutakode_Linje = src.Valutakode_Linje,
        tgt.Valutakode_Enhedspris = src.Valutakode_Enhedspris,
        tgt.Valutakode_Faktura = src.Valutakode_Faktura,
        tgt.GebyrBeloeb = src.GebyrBeloeb,
        tgt.RabatBeloeb = src.RabatBeloeb,
        tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        bk_FakturaLines, bk_FakturaHeader, bk_FakturaMetadata, CleanCode_Fakturalinje,
        DW_EK_Kreditor, DW_EK_Vare, DW_EK_Faktura,
        DW_EK_EAN_Regningsmodtager, DW_EK_UNSPSC,
        DW_EK_Dato_Faktura, DW_EK_Dato_Ordre, DW_EK_Dato_Betaling, DW_EK_Dato_Modtagelse,
        FakturaNr, AntalEnhederFaktureret, LinjeBeloeb, EnhedsPris,
        Valutakode_Linje, Valutakode_Enhedspris, Valutakode_Faktura,
        GebyrBeloeb, RabatBeloeb,
        DW_Timestamp_Insert, DW_Timestamp_Update
    )
    VALUES (
        src.bk_FakturaLines, src.bk_FakturaHeader, src.bk_FakturaMetadata, src.CleanCode_Fakturalinje,
        src.DW_EK_Kreditor, src.DW_EK_Vare, src.DW_EK_Faktura,
        src.DW_EK_EAN_Regningsmodtager, src.DW_EK_UNSPSC,
        src.DW_EK_Dato_Faktura, src.DW_EK_Dato_Ordre, src.DW_EK_Dato_Betaling, src.DW_EK_Dato_Modtagelse,
        src.FakturaNr, src.AntalEnhederFaktureret, src.LinjeBeloeb, src.EnhedsPris,
        src.Valutakode_Linje, src.Valutakode_Enhedspris, src.Valutakode_Faktura,
        src.GebyrBeloeb, src.RabatBeloeb,
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
"""

DIAG_SQL = """
SELECT
    COUNT(*) AS total_facts,
    SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
    SUM(CASE WHEN DW_EK_Vare = -1 THEN 1 ELSE 0 END) AS missing_vare_fk,
    SUM(CASE WHEN DW_EK_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
    SUM(CASE WHEN DW_EK_EAN_Regningsmodtager = -1 THEN 1 ELSE 0 END) AS missing_ean_fk,
    SUM(CASE WHEN DW_EK_UNSPSC = -1 THEN 1 ELSE 0 END) AS missing_unspsc_fk
FROM [fct].[Fakturalinje];
"""


@asset(
    group_name="erp_facts",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_fakturalinje_mysupply"]),
        AssetKey(["erp", "stg_kmd_fakturalinje_unspsc_match"]),
        "map_kreditor",
        "map_vare",
        "map_faktura",
        "map_fakturalinje",
        "fct_postering",
        "dim_vare",
        "map_ean",
        "dim_ean",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "invoice_line",
    },
    description=(
        "Fact table for invoice lines - reads from fct_fakturalinje_mysupply, "
        "looks up DW_EK_Kreditor, DW_EK_Vare, DW_EK_Faktura, DW_EK_EAN_Regningsmodtager "
        "from maps; DW_EK_UNSPSC via stg.kmd_fakturalinje_unspsc_match"
    ),
)
def fct_fakturalinje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Calculate fact table with DW_EK lookups from maps and KMD UNSPSC match."""

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Starting fact table load with DW_EK lookups, KMD UNSPSC match, and FK diagnostics..."
                )

                try:
                    cursor.execute("SELECT COUNT(*) FROM [stg].[pre_fakturalinje_mysupply]")
                    pre_staging_count = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM [stg].[pre_fakturalinje_mysupply] "
                        "WHERE statistic_invoice_line_id IS NOT NULL"
                    )
                    line_count = cursor.fetchone()[0]
                    context.log.info(
                        f"Pre-staging counts: Total={pre_staging_count}, With Lines={line_count}"
                    )
                except Exception as e:
                    error_msg = f"Failed to count pre-staging records: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute(RESOLVE_SQL)
                    cursor.execute(RESOLVE_INDEX_SQL)
                    context.log.info("Temp table #Fakturalinje_Facts created successfully")
                except Exception as e:
                    error_msg = (
                        f"Failed to create temp table with DW_EK lookups and KMD UNSPSC match: {e}"
                    )
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute(MERGE_SQL)
                    context.log.info("MERGE operation completed successfully")
                except Exception as e:
                    error_msg = f"Failed to MERGE into fct.Fakturalinje: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                rows_affected = cursor.rowcount
                conn.commit()
                context.log.info("Database transaction committed")

                try:
                    cursor.execute(DIAG_SQL)
                    columns = [col[0] for col in cursor.description]
                    diag = dict(zip(columns, cursor.fetchone(), strict=False))

                    context.log.info(
                        f"FK Matching: Total={diag['total_facts']}, "
                        f"Missing Kreditor={diag['missing_kreditor_fk']}, "
                        f"Missing Vare={diag['missing_vare_fk']}, "
                        f"Missing Faktura={diag['missing_faktura_fk']}, "
                        f"Missing EAN={diag['missing_ean_fk']}, "
                        f"Missing UNSPSC={diag['missing_unspsc_fk']}"
                    )

                    for key, label in [
                        ("missing_kreditor_fk", "Kreditor"),
                        ("missing_vare_fk", "Vare"),
                        ("missing_faktura_fk", "Faktura"),
                        ("missing_ean_fk", "EAN"),
                        ("missing_unspsc_fk", "UNSPSC"),
                    ]:
                        if diag[key] and diag[key] > 0:
                            context.log.warning(
                                f"{diag[key]} invoice lines without {label} FK (using -1/UNKNOWN)"
                            )
                except Exception as e:
                    error_msg = f"Failed to query fact table for FK diagnostics: {str(e)}"
                    context.log.error(error_msg)
                    context.log.error(f"Full traceback: {traceback.format_exc()}")
                    raise ValueError(error_msg) from e

                try:
                    cursor.execute("DROP TABLE IF EXISTS #Fakturalinje_Facts")
                except Exception as e:
                    context.log.warning(f"Failed to drop temp tables (may auto-cleanup): {e}")

                try:
                    cursor.execute("UPDATE STATISTICS [fct].[Fakturalinje]")
                except Exception as e:
                    context.log.warning(f"Failed to update statistics: {e}")

                context.log.info(f"Fact table load complete: {rows_affected} rows processed")
            finally:
                cursor.close()

    except Exception as e:
        error_msg = f"Fakturalinje fact table load failed: {str(e)}"
        context.log.error(error_msg)
        context.log.error(f"Full traceback: {traceback.format_exc()}")
        raise ValueError(error_msg) from e

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Fakturalinje",
            "grain": "CleanCode_Fakturalinje (header_id + line_id)",
            "staging_source": "stg.fct_fakturalinje_mysupply (Layer 1)",
            "dw_ek_lookups": (
                "DW_EK_Kreditor, DW_EK_Vare, DW_EK_Faktura, "
                "DW_EK_EAN_Regningsmodtager from maps; "
                "DW_EK_UNSPSC from stg.kmd_fakturalinje_unspsc_match"
            ),
            "missing_unspsc_fk": diag.get("missing_unspsc_fk"),
            "surrogate_key": "DW_EK_Fakturalinje (IDENTITY)",
        }
    )
