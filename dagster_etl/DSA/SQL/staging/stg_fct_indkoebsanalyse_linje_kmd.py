# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: stg.fct_indkoebsanalyse_linje_kmd

Delta load from arc.spend_analysis_lines_kmd_uddata filtered by fct.IndkoebsanalyseLinje
watermark. Resolves DW_EK_UNSPSC via arc.unspsc_kmd_uddata (DW_IsCurrent=1) and map.UNSPSC.
"""

import dagster as dg
from core.etl_watermark_manager import EtlWatermarkManager
from core.resources import MSSQLResource
from dagster import AssetKey, RetryPolicy

BUILD_SQL = """
DECLARE @wm NVARCHAR(50) = ?;

DROP TABLE IF EXISTS [stg].[fct_indkoebsanalyse_linje_kmd];

SELECT
    CONCAT('KMD_SAL_', CAST(sal.[DW_EK_SpendAnalysisLine] AS NVARCHAR(20)))
        AS CleanCode_IndkoebsanalyseLinje,
    CAST(sal.[DW_EK_SpendAnalysisLine] AS NVARCHAR(20)) AS bk_SpendAnalysisLine,
    COALESCE(um.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC,
    COALESCE(sal.[DW_SK_Date_SpendAnalysis], -1) AS DW_EK_Dato_SpendAnalysis,
    COALESCE(sal.[DW_SK_Date_Issue], -1) AS DW_EK_Dato_Issue,
    COALESCE(sal.[DW_SK_Date_OrderIssue], -1) AS DW_EK_Dato_OrderIssue,
    COALESCE(sal.[DW_SK_Date_ActualDelivery], -1) AS DW_EK_Dato_ActualDelivery,
    COALESCE(sal.[DW_SK_Date_PaymentDue], -1) AS DW_EK_Dato_PaymentDue,
    COALESCE(sal.[DW_SK_Date_Posting], -1) AS DW_EK_Dato_Posting,
    CAST(COALESCE(inv.[IsCreditnote], 0) AS BIT) AS IsCreditnote,
    sal.[LineAmount],
    sal.[InvoicedQuantity],
    sal.[UnitPrice],
    sal.[LineTaxAmount],
    sal.[CO2Emission],
    COALESCE(
        TRY_CONVERT(DATETIME2, sal.[DW_ValidDate]),
        sal.[DW_Timestamp_Update]
    ) AS DW_haendelsestidspunkt,
    sal.[DW_SK_UNSPSC] AS DW_SK_UNSPSC_source
INTO [stg].[fct_indkoebsanalyse_linje_kmd]
FROM [arc].[spend_analysis_lines_kmd_uddata] sal
LEFT JOIN [arc].[unspsc_kmd_uddata] ku
    ON ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]
    AND ku.[DW_IsCurrent] = 1
LEFT JOIN [map].[UNSPSC] um
    ON um.[CleanCode_UNSPSC] = ku.[UNSPSCClassID]
LEFT JOIN [arc].[invoice_kmd_uddata] inv
    ON inv.[DW_SK_Invoice] = sal.[DW_SK_Invoice]
    AND inv.[DW_IsCurrent] = 1
WHERE @wm IS NULL
   OR sal.[DW_Timestamp_Update] >= CAST(@wm AS DATETIME2);

CREATE CLUSTERED INDEX IX_stg_fct_indkoebsanalyse_linje_kmd_cc
    ON [stg].[fct_indkoebsanalyse_linje_kmd](CleanCode_IndkoebsanalyseLinje);
"""


@dg.asset(
    key=AssetKey(["erp", "stg_fct_indkoebsanalyse_linje_kmd"]),
    group_name="staging_indkoebsanalyse",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        dg.AssetKey(["staging_infra_erp"]),
        dg.AssetKey(["arc_spend_analysis_lines_kmd_uddata"]),
        dg.AssetKey(["arc_unspsc_kmd_uddata"]),
        dg.AssetKey(["arc_invoice_kmd_uddata"]),
        dg.AssetKey(["map_UNSPSC"]),
        dg.AssetKey(["dim_UNSPSC"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "kmd_uddata",
        "entity": "indkoebsanalyse_linje",
    },
    description=(
        "Fact staging for KMD SpendAnalysisLines. Delta from arc filtered by "
        "fct.IndkoebsanalyseLinje watermark; UNSPSC via arc+map join."
    ),
)
def stg_fct_indkoebsanalyse_linje_kmd(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    wm = EtlWatermarkManager(mssql_target_staging, context)
    watermark = wm.get_watermark("fact", "fct", "IndkoebsanalyseLinje")

    with mssql_target_staging.get_connection(timeout=0) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(BUILD_SQL, (watermark,))
            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_indkoebsanalyse_linje_kmd]")
            row_count = cursor.fetchone()[0]
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM [stg].[fct_indkoebsanalyse_linje_kmd]
                WHERE [DW_SK_UNSPSC_source] IS NOT NULL
                  AND [DW_EK_UNSPSC] = -1
            """
            )
            missing_unspsc = cursor.fetchone()[0]
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"stg.fct_indkoebsanalyse_linje_kmd: {row_count} rows, "
        f"missing_unspsc_fk={missing_unspsc}"
    )

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "target_table": "stg.fct_indkoebsanalyse_linje_kmd",
            "watermark_used": watermark,
            "missing_unspsc_fk": missing_unspsc,
        }
    )
