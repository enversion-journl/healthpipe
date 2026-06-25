# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Update: IndkoebsanalyseLinje (KMD SpendAnalysisLines)

MERGE via stable DW_EK from map.IndkoebsanalyseLinje.
DW_EK_UNSPSC resolved in staging (direct KMD UNSPSC path, not via dim.Vare).
"""

from core.etl_watermark_manager import EtlWatermarkManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

RESOLVE_SQL = """
DROP TABLE IF EXISTS #IndkoebsanalyseLinje_Resolved;

SELECT
    mf.[DW_EK_IndkoebsanalyseLinje],
    stg.[CleanCode_IndkoebsanalyseLinje],
    stg.[bk_SpendAnalysisLine],
    stg.[DW_EK_UNSPSC],
    stg.[DW_EK_Dato_SpendAnalysis],
    stg.[DW_EK_Dato_Issue],
    stg.[DW_EK_Dato_OrderIssue],
    stg.[DW_EK_Dato_ActualDelivery],
    stg.[DW_EK_Dato_PaymentDue],
    stg.[DW_EK_Dato_Posting],
    stg.[IsCreditnote],
    stg.[LineAmount],
    stg.[InvoicedQuantity],
    stg.[UnitPrice],
    stg.[LineTaxAmount],
    stg.[CO2Emission],
    stg.[DW_haendelsestidspunkt]
INTO #IndkoebsanalyseLinje_Resolved
FROM [stg].[fct_indkoebsanalyse_linje_kmd] stg
INNER JOIN [map].[IndkoebsanalyseLinje] mf
    ON mf.[CleanCode_IndkoebsanalyseLinje] = stg.[CleanCode_IndkoebsanalyseLinje];
"""

RESOLVE_INDEX_SQL = """
CREATE UNIQUE CLUSTERED INDEX IX_tmp_IndkoebsanalyseLinje_Resolved
    ON #IndkoebsanalyseLinje_Resolved ([DW_EK_IndkoebsanalyseLinje]);
"""

MERGE_SQL = """
MERGE [fct].[IndkoebsanalyseLinje] AS tgt
USING #IndkoebsanalyseLinje_Resolved AS src
    ON tgt.[DW_EK_IndkoebsanalyseLinje] = src.[DW_EK_IndkoebsanalyseLinje]

WHEN MATCHED THEN UPDATE SET
    tgt.[CleanCode_IndkoebsanalyseLinje] = src.[CleanCode_IndkoebsanalyseLinje],
    tgt.[bk_SpendAnalysisLine] = src.[bk_SpendAnalysisLine],
    tgt.[DW_EK_UNSPSC] = src.[DW_EK_UNSPSC],
    tgt.[DW_EK_Dato_SpendAnalysis] = src.[DW_EK_Dato_SpendAnalysis],
    tgt.[DW_EK_Dato_Issue] = src.[DW_EK_Dato_Issue],
    tgt.[DW_EK_Dato_OrderIssue] = src.[DW_EK_Dato_OrderIssue],
    tgt.[DW_EK_Dato_ActualDelivery] = src.[DW_EK_Dato_ActualDelivery],
    tgt.[DW_EK_Dato_PaymentDue] = src.[DW_EK_Dato_PaymentDue],
    tgt.[DW_EK_Dato_Posting] = src.[DW_EK_Dato_Posting],
    tgt.[IsCreditnote] = src.[IsCreditnote],
    tgt.[LineAmount] = src.[LineAmount],
    tgt.[InvoicedQuantity] = src.[InvoicedQuantity],
    tgt.[UnitPrice] = src.[UnitPrice],
    tgt.[LineTaxAmount] = src.[LineTaxAmount],
    tgt.[CO2Emission] = src.[CO2Emission],
    tgt.[DW_haendelsestidspunkt] = src.[DW_haendelsestidspunkt],
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN INSERT (
    [DW_EK_IndkoebsanalyseLinje],
    [CleanCode_IndkoebsanalyseLinje],
    [bk_SpendAnalysisLine],
    [DW_EK_UNSPSC],
    [DW_EK_Dato_SpendAnalysis],
    [DW_EK_Dato_Issue],
    [DW_EK_Dato_OrderIssue],
    [DW_EK_Dato_ActualDelivery],
    [DW_EK_Dato_PaymentDue],
    [DW_EK_Dato_Posting],
    [IsCreditnote],
    [LineAmount],
    [InvoicedQuantity],
    [UnitPrice],
    [LineTaxAmount],
    [CO2Emission],
    [DW_haendelsestidspunkt],
    [DW_Timestamp_Insert],
    [DW_Timestamp_Update]
) VALUES (
    src.[DW_EK_IndkoebsanalyseLinje],
    src.[CleanCode_IndkoebsanalyseLinje],
    src.[bk_SpendAnalysisLine],
    src.[DW_EK_UNSPSC],
    src.[DW_EK_Dato_SpendAnalysis],
    src.[DW_EK_Dato_Issue],
    src.[DW_EK_Dato_OrderIssue],
    src.[DW_EK_Dato_ActualDelivery],
    src.[DW_EK_Dato_PaymentDue],
    src.[DW_EK_Dato_Posting],
    src.[IsCreditnote],
    src.[LineAmount],
    src.[InvoicedQuantity],
    src.[UnitPrice],
    src.[LineTaxAmount],
    src.[CO2Emission],
    src.[DW_haendelsestidspunkt],
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
);
"""

DIAG_SQL = """
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN DW_EK_UNSPSC = -1 THEN 1 ELSE 0 END) AS missing_unspsc
FROM [fct].[IndkoebsanalyseLinje];
"""

UNMAPPED_STG_SQL = """
SELECT TOP 1 1 AS unmapped_exists
FROM [stg].[fct_indkoebsanalyse_linje_kmd] src
LEFT JOIN [map].[IndkoebsanalyseLinje] m
    ON m.CleanCode_IndkoebsanalyseLinje = src.CleanCode_IndkoebsanalyseLinje
WHERE m.DW_EK_IndkoebsanalyseLinje IS NULL;
"""


@asset(
    name="fct_indkoebsanalyse_linje",
    group_name="facts_indkoebsanalyse",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_fct_indkoebsanalyse_linje_kmd"]),
        "map_IndkoebsanalyseLinje",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "spend_analysis_line",
    },
    description="MERGE fct.IndkoebsanalyseLinje from KMD arc via map stable DW_EK",
)
def fct_indkoebsanalyse_linje(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Load KMD SpendAnalysisLines fact: MERGE with stable DW_EK and watermark logging."""

    wm = EtlWatermarkManager(mssql_target_staging, context)
    watermark_before = wm.get_watermark("fact", "fct", "IndkoebsanalyseLinje")
    log_id = wm.log_start_run(
        "fact",
        "fct",
        "IndkoebsanalyseLinje",
        watermark_before=watermark_before,
        dagster_run_id=context.run_id,
    )

    try:
        with mssql_target_staging.get_connection(timeout=0) as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_indkoebsanalyse_linje_kmd]")
                stg_count = cursor.fetchone()[0]
                context.log.info(f"Staging has {stg_count} rows")

                if stg_count == 0:
                    context.log.info("No rows to load — skipping")
                    result = MaterializeResult(
                        metadata={"table": "fct.IndkoebsanalyseLinje", "row_count": 0}
                    )
                    wm.advance_watermark("fact", "fct", "IndkoebsanalyseLinje")
                    watermark_after = wm.get_watermark("fact", "fct", "IndkoebsanalyseLinje")
                    wm.log_complete_run(log_id, watermark_after=watermark_after, rows_affected=0)
                    return result

                cursor.execute(UNMAPPED_STG_SQL)
                if cursor.fetchone() is not None:
                    raise ValueError(
                        "fct_indkoebsanalyse_linje: at least one staging row has no match "
                        "in map.IndkoebsanalyseLinje. Ensure map_IndkoebsanalyseLinje runs first."
                    )

                cursor.execute(RESOLVE_SQL)
                resolved_count = cursor.rowcount
                context.log.info(f"Resolved {resolved_count} rows for MERGE")

                cursor.execute(RESOLVE_INDEX_SQL)

                cursor.execute(MERGE_SQL)
                merged = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #IndkoebsanalyseLinje_Resolved;")
                cursor.execute("UPDATE STATISTICS [fct].[IndkoebsanalyseLinje];")

                cursor.execute(DIAG_SQL)
                diag = cursor.fetchone()
                context.log.info(f"Total={diag[0]}, missing_unspsc={diag[1]}")
            finally:
                cursor.close()

        result = MaterializeResult(
            metadata={
                "table": "fct.IndkoebsanalyseLinje",
                "row_count": resolved_count,
                "merged": merged,
                "pattern": "MERGE (stable DW_EK)",
                "total_rows": diag[0],
                "missing_unspsc": diag[1],
            }
        )

        wm.advance_watermark("fact", "fct", "IndkoebsanalyseLinje")
        watermark_after = wm.get_watermark("fact", "fct", "IndkoebsanalyseLinje")
        wm.log_complete_run(log_id, watermark_after=watermark_after, rows_affected=resolved_count)

    except Exception:
        wm.log_fail_run(log_id)
        raise

    return result
