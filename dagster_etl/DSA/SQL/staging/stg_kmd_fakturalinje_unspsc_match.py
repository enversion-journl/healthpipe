# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging: stg.kmd_fakturalinje_unspsc_match

Pre-computed KMD → MySupply UNSPSC match keys for fct.Fakturalinje (Option 2).
Aggregates SpendAnalysisLines micro-lines per (DW_SK_Invoice, DW_SK_InvoiceLine),
expands invoice SK to CleanCode_Kreditor + CleanCode_Faktura (metadata.LeverandorCVR builder
from pre_stg_fakturalinje_mysupply), deduplicates on tiered match keys.

Match tiers:
  1 — CVR + FAKTURA + LineAmount + InvoicedQuantity + UnitPrice
  2 — CVR + FAKTURA + LineAmount + InvoicedQuantity (UnitPrice omitted)
  3 — CVR + FAKTURA + InvoiceTotal (LineExtensionTotalAmount), only when one UNSPSC

Full rebuild each run — aggregation spans invoice lines and requires complete arc state.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

# CVR/FAKTURA builders mirror pre_stg_fakturalinje_mysupply header_dedup logic.
BUILD_SQL = """
TRUNCATE TABLE [stg].[kmd_fakturalinje_unspsc_match];

;WITH KmdLines AS (
    SELECT
        sal.[DW_SK_Invoice],
        sal.[DW_SK_InvoiceLine],
        TRY_CONVERT(DECIMAL(18, 4), sal.[UnitPrice]) AS UnitPrice,
        TRY_CONVERT(DECIMAL(18, 4), sal.[LineAmount]) AS LineAmount,
        TRY_CONVERT(DECIMAL(18, 4), sal.[InvoicedQuantity]) AS InvoicedQuantity,
        um.[DW_EK_UNSPSC],
        sal.[DW_ValidDate]
    FROM [arc].[spend_analysis_lines_kmd_uddata] sal
    INNER JOIN [arc].[unspsc_kmd_uddata] ku
        ON ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]
        AND ku.[DW_IsCurrent] = 1
    INNER JOIN [map].[UNSPSC] um
        ON um.[CleanCode_UNSPSC] = ku.[UNSPSCClassID]
    WHERE sal.[DW_SK_Invoice] IS NOT NULL
      AND sal.[DW_SK_InvoiceLine] IS NOT NULL
),
KmdAggregated AS (
    SELECT
        DW_SK_Invoice,
        DW_SK_InvoiceLine,
        SUM(LineAmount) AS LineAmount_Sum,
        SUM(InvoicedQuantity) AS InvoicedQuantity_Sum,
        MAX(UnitPrice) AS UnitPrice,
        MAX(DW_EK_UNSPSC) AS DW_EK_UNSPSC,
        MAX(DW_ValidDate) AS DW_ValidDate_Max
    FROM KmdLines
    GROUP BY
        DW_SK_Invoice,
        DW_SK_InvoiceLine
),
KmdExpanded AS (
    SELECT
        'CVR_' + CASE
            WHEN LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) LIKE 'DK[0-9]%'
            THEN SUBSTRING(
                LTRIM(RTRIM(inv.[SupplierCompanyTaxID])),
                3,
                LEN(LTRIM(RTRIM(inv.[SupplierCompanyTaxID])))
            )
            ELSE LTRIM(RTRIM(inv.[SupplierCompanyTaxID]))
        END AS CleanCode_Kreditor,
        'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], ' ', '')))) AS CleanCode_Faktura,
        TRY_CONVERT(DECIMAL(18, 4), inv.[LineExtensionTotalAmount]) AS InvoiceTotal,
        agg.LineAmount_Sum,
        agg.InvoicedQuantity_Sum,
        agg.UnitPrice,
        agg.DW_EK_UNSPSC,
        agg.DW_ValidDate_Max
    FROM KmdAggregated agg
    INNER JOIN [arc].[invoice_kmd_uddata] inv
        ON inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]
        AND inv.[DW_IsCurrent] = 1
    WHERE inv.[InvoiceNumber] IS NOT NULL
      AND LTRIM(RTRIM(inv.[InvoiceNumber])) <> ''
      AND inv.[SupplierCompanyTaxID] IS NOT NULL
      AND LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) <> ''
),
KmdCandidatesTier1 AS (
    SELECT
        CleanCode_Kreditor,
        CleanCode_Faktura,
        LineAmount_Sum,
        InvoicedQuantity_Sum AS InvoicedQuantity,
        UnitPrice,
        DW_EK_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY
                CleanCode_Kreditor,
                CleanCode_Faktura,
                LineAmount_Sum,
                InvoicedQuantity_Sum,
                UnitPrice
            ORDER BY DW_ValidDate_Max DESC
        ) AS rn
    FROM KmdExpanded
),
KmdCandidatesTier2 AS (
    SELECT
        CleanCode_Kreditor,
        CleanCode_Faktura,
        LineAmount_Sum,
        InvoicedQuantity_Sum AS InvoicedQuantity,
        DW_EK_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY
                CleanCode_Kreditor,
                CleanCode_Faktura,
                LineAmount_Sum,
                InvoicedQuantity_Sum
            ORDER BY DW_ValidDate_Max DESC
        ) AS rn
    FROM KmdExpanded
),
KmdInvoiceTier3 AS (
    SELECT
        CleanCode_Kreditor,
        CleanCode_Faktura,
        InvoiceTotal,
        MAX(DW_EK_UNSPSC) AS DW_EK_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY CleanCode_Kreditor, CleanCode_Faktura, InvoiceTotal
            ORDER BY MAX(DW_ValidDate_Max) DESC
        ) AS rn
    FROM KmdExpanded
    WHERE InvoiceTotal IS NOT NULL
    GROUP BY CleanCode_Kreditor, CleanCode_Faktura, InvoiceTotal
    HAVING COUNT(DISTINCT DW_EK_UNSPSC) = 1
)
INSERT INTO [stg].[kmd_fakturalinje_unspsc_match] (
    CleanCode_Kreditor,
    CleanCode_Faktura,
    MatchTier,
    LineAmount_Sum,
    InvoicedQuantity,
    UnitPrice,
    InvoiceTotal,
    DW_EK_UNSPSC
)
SELECT
    CleanCode_Kreditor,
    CleanCode_Faktura,
    1 AS MatchTier,
    LineAmount_Sum,
    InvoicedQuantity,
    UnitPrice,
    NULL AS InvoiceTotal,
    DW_EK_UNSPSC
FROM KmdCandidatesTier1
WHERE rn = 1
UNION ALL
SELECT
    CleanCode_Kreditor,
    CleanCode_Faktura,
    2 AS MatchTier,
    LineAmount_Sum,
    InvoicedQuantity,
    NULL AS UnitPrice,
    NULL AS InvoiceTotal,
    DW_EK_UNSPSC
FROM KmdCandidatesTier2
WHERE rn = 1
UNION ALL
SELECT
    CleanCode_Kreditor,
    CleanCode_Faktura,
    3 AS MatchTier,
    NULL AS LineAmount_Sum,
    NULL AS InvoicedQuantity,
    NULL AS UnitPrice,
    InvoiceTotal,
    DW_EK_UNSPSC
FROM KmdInvoiceTier3
WHERE rn = 1;
"""


@dg.asset(
    key=dg.AssetKey(["erp", "stg_kmd_fakturalinje_unspsc_match"]),
    group_name="staging_unspsc",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_spend_analysis_lines_kmd_uddata"]),
        dg.AssetKey(["arc_invoice_kmd_uddata"]),
        dg.AssetKey(["arc_unspsc_kmd_uddata"]),
        dg.AssetKey(["map_UNSPSC"]),
    ],
    tags={"domain": "erp", "layer": "staging", "entity": "fakturalinje_unspsc"},
    description=(
        "KMD UNSPSC match lookup for fct.Fakturalinje: aggregated invoice-line "
        "keys expanded to CleanCode_Kreditor+CleanCode_Faktura with tiered DW_EK_UNSPSC."
    ),
)
def stg_kmd_fakturalinje_unspsc_match(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "kmd_fakturalinje_unspsc_match",
        dagster_run_id=context.run_id,
        mode=config.mode,
        checkpoint_from=("arc", "spend_analysis_lines_kmd_uddata"),
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL)
                cursor.execute("SELECT COUNT(*) FROM [stg].[kmd_fakturalinje_unspsc_match]")
                row_count = cursor.fetchone()[0]
                conn.commit()
            finally:
                cursor.close()

        context.log.info(
            f"stg.kmd_fakturalinje_unspsc_match: {row_count} match rows "
            f"(arc checkpoint={run_ctx.checkpoint})"
        )
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "target_table": "stg.kmd_fakturalinje_unspsc_match",
            "arc_checkpoint": run_ctx.checkpoint,
            "mode": config.mode,
        }
    )
