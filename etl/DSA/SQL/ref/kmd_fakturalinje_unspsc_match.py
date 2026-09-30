# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.KMDFakturalinjeUNSPSCMatch — KMD-only UNSPSC match lookup.

Pre-computed tiered match keys from KMD SpendAnalysisLines. MySupply joins in
fct_apply_fakturalinje_unspsc (primary + header cvr branches).

Delta: MERGE scoped to KMD invoices changed since the ref DFM checkpoint.
full_load / first run rebuilds the entire KMD match set.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.unspsc_match_shared import MATCH_CURRENCY_SCALE, sql_match_currency
from utils.sql.tax_id_sql import sql_strip_tax_id_country_prefix

MAX_CHECKPOINT_SQL = """
SELECT CONVERT(
    NVARCHAR(50),
    MAX(ts),
    121
)
FROM (
    SELECT MAX([dw_timestamp_update]) AS ts
    FROM [arc].[kmd_uddata_spend_analysis_lines]
    UNION ALL
    SELECT MAX([dw_timestamp_update]) AS ts
    FROM [arc].[kmd_uddata_invoice]
) s
"""


def build_match_rows_cte(*, invoice_filter_sql: str = "") -> str:
    """Return CTE chain ending in MatchRows (tiered union)."""
    sal_filter = invoice_filter_sql.strip()
    if sal_filter and not sal_filter.upper().startswith("AND"):
        sal_filter = f"AND {sal_filter}"

    return f"""
;WITH KmdLines AS (
    SELECT
        sal.[DW_SK_Invoice],
        sal.[DW_SK_InvoiceLine],
        sal.[DW_SK_SpendAnalysisLineItem],
        sal.[DW_EK_SpendAnalysisLine],
        sal.[DW_SK_UNSPSC],
        TRY_CONVERT(DECIMAL(18, 4), sal.[UnitPrice]) AS unit_price,
        TRY_CONVERT(DECIMAL(18, 4), sal.[LineAmount]) AS LineAmount,
        TRY_CONVERT(DECIMAL(18, 4), sal.[InvoicedQuantity]) AS invoiced_quantity,
        um.[dw_ek_UNSPSC],
        sal.[DW_ValidDate]
    FROM [arc].[kmd_uddata_spend_analysis_lines] sal
    INNER JOIN [arc].[kmd_uddata_unspsc] ku
        ON ku.[DW_SK_UNSPSC] = sal.[DW_SK_UNSPSC]
        AND ku.[DW_IsCurrent] = 1
    INNER JOIN [map].[UNSPSC] um
        ON um.[clean_code_UNSPSC] = ku.[UNSPSCClassID]
    WHERE sal.[DW_SK_Invoice] IS NOT NULL
      AND sal.[DW_SK_InvoiceLine] IS NOT NULL
      {sal_filter}
),
KmdDeduped AS (
    SELECT
        DW_SK_Invoice,
        DW_SK_InvoiceLine,
        unit_price,
        LineAmount,
        invoiced_quantity,
        dw_ek_UNSPSC,
        DW_ValidDate
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    DW_SK_Invoice,
                    DW_SK_InvoiceLine,
                    DW_SK_SpendAnalysisLineItem,
                    LineAmount,
                    invoiced_quantity,
                    unit_price,
                    DW_SK_UNSPSC
                ORDER BY DW_ValidDate DESC, DW_EK_SpendAnalysisLine DESC
            ) AS dedup_rn
        FROM KmdLines
    ) ranked
    WHERE dedup_rn = 1
),
KmdAggregated AS (
    SELECT
        DW_SK_Invoice,
        DW_SK_InvoiceLine,
        -- ROUND(2): align KMD micro-line sums with MySupply DKK line amounts (2 dp)
        ROUND(SUM(LineAmount), {MATCH_CURRENCY_SCALE}) AS line_amount_sum,
        SUM(invoiced_quantity) AS InvoicedQuantity_Sum,
        ROUND(MAX(unit_price), {MATCH_CURRENCY_SCALE}) AS unit_price,
        MAX(dw_ek_UNSPSC) AS dw_ek_UNSPSC,
        MAX(DW_ValidDate) AS DW_ValidDate_Max
    FROM KmdDeduped
    GROUP BY
        DW_SK_Invoice,
        DW_SK_InvoiceLine
),
KmdExpanded AS (
    SELECT
        'CVR_' + {sql_strip_tax_id_country_prefix("inv.[SupplierCompanyTaxID]")} AS clean_code_kreditor,
        'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], ' ', '')))) AS clean_code_faktura,
        {sql_match_currency("inv.[LineExtensionTotalAmount]")} AS invoice_total,
        agg.line_amount_sum,
        agg.InvoicedQuantity_Sum,
        agg.unit_price,
        agg.dw_ek_UNSPSC,
        agg.DW_ValidDate_Max
    FROM KmdAggregated agg
    INNER JOIN [arc].[kmd_uddata_invoice] inv
        ON inv.[DW_SK_Invoice] = agg.[DW_SK_Invoice]
        AND inv.[DW_IsCurrent] = 1
    WHERE inv.[InvoiceNumber] IS NOT NULL
      AND LTRIM(RTRIM(inv.[InvoiceNumber])) <> ''
      AND inv.[SupplierCompanyTaxID] IS NOT NULL
      AND LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) <> ''
),
KmdCandidatesTier1 AS (
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        line_amount_sum,
        InvoicedQuantity_Sum AS invoiced_quantity,
        unit_price,
        dw_ek_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY
                clean_code_kreditor,
                clean_code_faktura,
                line_amount_sum,
                InvoicedQuantity_Sum,
                unit_price
            ORDER BY DW_ValidDate_Max DESC
        ) AS rn
    FROM KmdExpanded
),
KmdCandidatesTier2 AS (
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        line_amount_sum,
        InvoicedQuantity_Sum AS invoiced_quantity,
        dw_ek_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY
                clean_code_kreditor,
                clean_code_faktura,
                line_amount_sum,
                InvoicedQuantity_Sum
            ORDER BY DW_ValidDate_Max DESC
        ) AS rn
    FROM KmdExpanded
),
KmdInvoiceTier3 AS (
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        invoice_total,
        MAX(dw_ek_UNSPSC) AS dw_ek_UNSPSC,
        ROW_NUMBER() OVER (
            PARTITION BY clean_code_kreditor, clean_code_faktura, invoice_total
            ORDER BY MAX(DW_ValidDate_Max) DESC
        ) AS rn
    FROM KmdExpanded
    WHERE invoice_total IS NOT NULL
    GROUP BY clean_code_kreditor, clean_code_faktura, invoice_total
    HAVING COUNT(DISTINCT dw_ek_UNSPSC) = 1
),
MatchRows AS (
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        CAST(1 AS TINYINT) AS match_tier,
        line_amount_sum,
        invoiced_quantity,
        unit_price,
        CAST(NULL AS DECIMAL(18, 4)) AS invoice_total,
        dw_ek_UNSPSC
    FROM KmdCandidatesTier1
    WHERE rn = 1
    UNION ALL
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        CAST(2 AS TINYINT) AS match_tier,
        line_amount_sum,
        invoiced_quantity,
        CAST(NULL AS DECIMAL(18, 4)) AS unit_price,
        CAST(NULL AS DECIMAL(18, 4)) AS invoice_total,
        dw_ek_UNSPSC
    FROM KmdCandidatesTier2
    WHERE rn = 1
    UNION ALL
    SELECT
        clean_code_kreditor,
        clean_code_faktura,
        CAST(3 AS TINYINT) AS match_tier,
        CAST(NULL AS DECIMAL(18, 4)) AS line_amount_sum,
        CAST(NULL AS DECIMAL(18, 4)) AS invoiced_quantity,
        CAST(NULL AS DECIMAL(18, 4)) AS unit_price,
        invoice_total,
        dw_ek_UNSPSC
    FROM KmdInvoiceTier3
    WHERE rn = 1
)
"""


def build_affected_invoices_cte(*, checkpoint_param: str) -> str:
    """Invoices touched by KMD arc delta since checkpoint."""
    return f"""
;WITH AffectedInvoices AS (
    SELECT DISTINCT sal.[DW_SK_Invoice]
    FROM [arc].[kmd_uddata_spend_analysis_lines] sal
    WHERE sal.[DW_SK_Invoice] IS NOT NULL
      AND sal.[dw_timestamp_update] > '{checkpoint_param}'
    UNION
    SELECT DISTINCT sal.[DW_SK_Invoice]
    FROM [arc].[kmd_uddata_spend_analysis_lines] sal
    INNER JOIN [arc].[kmd_uddata_invoice] inv
        ON inv.[DW_SK_Invoice] = sal.[DW_SK_Invoice]
    WHERE sal.[DW_SK_Invoice] IS NOT NULL
      AND inv.[dw_timestamp_update] > '{checkpoint_param}'
)
"""


def build_ref_merge_sql(*, full_scope: bool, checkpoint_param: str) -> str:
    """Build temp #ref_matches and MERGE into ref.KMDFakturalinjeUNSPSCMatch."""
    if full_scope:
        invoice_filter = ""
        soft_delete_predicate = "tgt.[dw_is_deleted] = 0"
    else:
        invoice_filter = "sal.[DW_SK_Invoice] IN (SELECT DW_SK_Invoice FROM AffectedInvoices)"
        soft_delete_predicate = (
            "tgt.[dw_is_deleted] = 0 "
            "AND EXISTS ("
            "SELECT 1 FROM #AffectedPairs ap "
            "WHERE ap.clean_code_kreditor = tgt.clean_code_kreditor "
            "AND ap.clean_code_faktura = tgt.clean_code_faktura)"
        )

    match_cte = build_match_rows_cte(invoice_filter_sql=invoice_filter)

    affected_pairs_block = ""
    if not full_scope:
        affected_pairs_cte = build_affected_invoices_cte(checkpoint_param=checkpoint_param)
        affected_pairs_block = f"""
{affected_pairs_cte}
SELECT DISTINCT clean_code_kreditor, clean_code_faktura
INTO #AffectedPairs
FROM (
    SELECT
        'CVR_' + {sql_strip_tax_id_country_prefix("inv.[SupplierCompanyTaxID]")} AS clean_code_kreditor,
        'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(inv.[InvoiceNumber], ' ', '')))) AS clean_code_faktura
    FROM [arc].[kmd_uddata_invoice] inv
    WHERE inv.[DW_IsCurrent] = 1
      AND inv.[DW_SK_Invoice] IN (SELECT DW_SK_Invoice FROM AffectedInvoices)
      AND inv.[InvoiceNumber] IS NOT NULL
      AND LTRIM(RTRIM(inv.[InvoiceNumber])) <> ''
      AND inv.[SupplierCompanyTaxID] IS NOT NULL
      AND LTRIM(RTRIM(inv.[SupplierCompanyTaxID])) <> ''
) pairs;
"""

    # Guard: on a full-scope rebuild an empty MatchRows would make every existing
    # active row "NOT MATCHED BY SOURCE" and soft-delete the entire table. Abort
    # instead, so a transient-empty / half-loaded source (empty arc.kmd_uddata_*
    # or map.UNSPSC at run time) can never silently wipe ref. In delta scope the
    # soft-delete is bounded by #AffectedPairs, so an empty delta is harmless and
    # must NOT abort.
    empty_source_guard = (
        """
IF NOT EXISTS (SELECT 1 FROM #ref_matches)
    THROW 50000, 'ref_kmd_fakturalinje_unspsc_match: full-scope build produced 0 match rows — aborting before MERGE to avoid soft-deleting the whole ref table. Verify arc.kmd_uddata_invoice / kmd_uddata_spend_analysis_lines / map.UNSPSC are populated.', 1;
"""
        if full_scope
        else ""
    )

    return f"""
DROP TABLE IF EXISTS #ref_matches;
DROP TABLE IF EXISTS #AffectedPairs;
{affected_pairs_block}
{match_cte}
SELECT *
INTO #ref_matches
FROM MatchRows;
{empty_source_guard}
MERGE [ref].[KMDFakturalinjeUNSPSCMatch] AS tgt
USING #ref_matches AS src
ON tgt.[clean_code_kreditor] = src.[clean_code_kreditor]
   AND tgt.[clean_code_faktura] = src.[clean_code_faktura]
   AND tgt.[match_tier] = src.[match_tier]
   AND (
       tgt.[line_amount_sum] = src.[line_amount_sum]
       OR (tgt.[line_amount_sum] IS NULL AND src.[line_amount_sum] IS NULL)
   )
   AND (
       tgt.[invoiced_quantity] = src.[invoiced_quantity]
       OR (tgt.[invoiced_quantity] IS NULL AND src.[invoiced_quantity] IS NULL)
   )
   AND (
       tgt.[unit_price] = src.[unit_price]
       OR (tgt.[unit_price] IS NULL AND src.[unit_price] IS NULL)
   )
   AND (
       tgt.[invoice_total] = src.[invoice_total]
       OR (tgt.[invoice_total] IS NULL AND src.[invoice_total] IS NULL)
   )

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        clean_code_kreditor,
        clean_code_faktura,
        match_tier,
        line_amount_sum,
        invoiced_quantity,
        unit_price,
        invoice_total,
        dw_ek_UNSPSC,
        dw_is_deleted,
        dw_timestamp_insert,
        dw_timestamp_update
    )
    VALUES (
        src.clean_code_kreditor,
        src.clean_code_faktura,
        src.match_tier,
        src.line_amount_sum,
        src.invoiced_quantity,
        src.unit_price,
        src.invoice_total,
        src.dw_ek_UNSPSC,
        0,
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    )

WHEN MATCHED AND (
       tgt.[dw_ek_UNSPSC] <> src.[dw_ek_UNSPSC]
    OR tgt.[dw_is_deleted] = 1
) THEN
    UPDATE SET
        tgt.[dw_ek_UNSPSC] = src.[dw_ek_UNSPSC],
        tgt.[dw_is_deleted] = 0,
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY SOURCE AND {soft_delete_predicate} THEN
    UPDATE SET
        tgt.[dw_is_deleted] = 1,
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0));
"""


@dg.asset(
    key=dg.AssetKey(["erp", "ref_kmd_fakturalinje_unspsc_match"]),
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        dg.AssetKey(["arc_kmd_uddata_spend_analysis_lines"]),
        dg.AssetKey(["arc_kmd_uddata_invoice"]),
        dg.AssetKey(["arc_kmd_uddata_unspsc"]),
        dg.AssetKey(["map_UNSPSC"]),
    ],
    tags={"domain": "erp", "layer": "ref", "entity": "fakturalinje_unspsc"},
    description=(
        "KMD-only UNSPSC match lookup in ref schema: tiered clean_code_kreditor+"
        "clean_code_faktura keys with dw_ek_UNSPSC; own DFM checkpoint."
    ),
)
def ref_kmd_fakturalinje_unspsc_match(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "KMDFakturalinjeUNSPSCMatch",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint
    cp_param = checkpoint or "1900-01-01"
    full_scope = config.mode == "full_load" or checkpoint is None

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                merge_sql = build_ref_merge_sql(
                    full_scope=full_scope,
                    checkpoint_param=cp_param,
                )
                cursor.execute(merge_sql)
                merge_affected = cursor.rowcount

                cursor.execute(
                    "SELECT COUNT(*) FROM [ref].[KMDFakturalinjeUNSPSCMatch] WHERE [dw_is_deleted] = 0"
                )
                row_count = cursor.fetchone()[0]

                cursor.execute(MAX_CHECKPOINT_SQL)
                max_row = cursor.fetchone()
                new_checkpoint = max_row[0] if max_row and max_row[0] else checkpoint

                conn.commit()
            finally:
                cursor.close()

        if config.should_advance_checkpoint and new_checkpoint is not None:
            dfm.set_checkpoint(
                "ref",
                "KMDFakturalinjeUNSPSCMatch",
                value=new_checkpoint,
                mode=config.mode,
            )

        context.log.info(
            f"ref.KMDFakturalinjeUNSPSCMatch: {merge_affected} rows touched by MERGE, "
            f"{row_count} active match rows (scope={'full' if full_scope else 'delta'}, "
            f"checkpoint {checkpoint} → {new_checkpoint})"
        )
        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=merge_affected,
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "row_count": row_count,
            "target_table": "ref.KMDFakturalinjeUNSPSCMatch",
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "full_scope": full_scope,
            "mode": config.mode,
        }
    )
