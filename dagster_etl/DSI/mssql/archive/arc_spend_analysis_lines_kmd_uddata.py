# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive: ext.spend_analysis_lines_kmd_uddata
     → arc.spend_analysis_lines_kmd_uddata

UPSERT archive with DFM checkpoint. Business logic mirrors
arc_spend_analysis_lines_kmd_uddata.py.

Before the MERGE, a clustered index is created on the ext table to prevent
the full-table scan that causes timeouts on the old pipeline.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource

_NON_KEY_COLUMNS: tuple[str, ...] = (
    "DW_SK_Compliance",
    "DW_SK_Date_SpendAnalysis",
    "DW_SK_Date_Issue",
    "DW_SK_Date_OrderIssue",
    "DW_SK_Date_ActualDelivery",
    "DW_SK_Date_PaymentDue",
    "DW_SK_Date_Posting",
    "DW_SK_Economy",
    "DW_SK_PostingText",
    "DW_SK_EconomyCustom",
    "DW_SK_EconomyLine",
    "DW_SK_AppendixNumber",
    "DW_SK_ExchangeRates",
    "DW_SK_ExpenceType",
    "DW_SK_GeoData",
    "DW_SK_Invoice",
    "DW_SK_InvoiceLine",
    "DW_SK_AllowanceChargeLine",
    "DW_SK_InvoiceType",
    "DW_SK_LocalPurchase",
    "DW_SK_NACE",
    "DW_SK_Online",
    "DW_SK_EtradeObligation",
    "DW_SK_OrderType",
    "DW_SK_Organic",
    "DW_SK_Organization",
    "DW_SK_PriceAgreement",
    "DW_SK_ProcurementAgreement",
    "DW_SK_ProcurementType",
    "DW_SK_SpendAnalysisLineItem",
    "DW_SK_Supplier",
    "DW_SK_TenderGroup",
    "DW_SK_UNSPSC",
    "LineAmount",
    "InvoicedQuantity",
    "AgreementItemPrice",
    "RatePercentNumeric",
    "LineTaxAmount",
    "UnitPrice",
    "UnitPriceExchange",
    "ObservedUnitPrice",
    "PriceAgreementItemInScopeIndicator",
    "LinePercentOfTotal",
    "IsDiffLine",
    "CO2Emission",
    "DW_ValidDate",
    "DW_ArchiveID",
    "DW_Audit",
    "DW_Audit_Insert",
    "DW_Audit_Update",
)

SPEND_ANALYSIS_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="spend_analysis_lines_kmd_uddata",
    target_schema="arc",
    target_table="spend_analysis_lines_kmd_uddata",
    merge_keys=("DW_EK_SpendAnalysisLine",),
    non_key_columns=_NON_KEY_COLUMNS,
    checkpoint_query=(
        "SELECT CAST(MAX([computed_max_audit]) AS NVARCHAR(50)) "
        "FROM [arc].[spend_analysis_lines_kmd_uddata]"
    ),
    strategy="upsert",
)


@dg.asset(
    name="arc_spend_analysis_lines_kmd_uddata",
    group_name="kmd_uddata_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_spend_analysis_lines_kmd_uddata"])],
    tags={
        "domain": "kmd_uddata",
        "layer": "archive",
        "source": "kmd_uddata",
        "entity": "spend_analysis_lines",
    },
    description=(
        "UPSERT ext → arc.spend_analysis_lines_kmd_uddata. "
        "Creates a clustered index on the ext source before MERGE to avoid timeout."
    ),
    retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)
def arc_spend_analysis_lines_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    _create_ext_index(context, mssql_target_staging)

    result = execute_archive_merge(
        context, mssql_target_staging, SPEND_ANALYSIS_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.spend_analysis_lines_kmd_uddata "
        f"(inserted={result.rows_inserted}, updated={result.rows_updated}); "
        f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.spend_analysis_lines_kmd_uddata",
            target_table="arc.spend_analysis_lines_kmd_uddata",
        )
    )


def _create_ext_index(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
) -> None:
    """Create a clustered index on the ext source table before MERGE.

    The MERGE joins on DW_EK_SpendAnalysisLine (BIGINT PK).  Without an
    index the query planner does a full scan of the ext table, which
    causes timeouts when the table has many rows.
    """
    ix_sql = (
        "IF NOT EXISTS ("
        "  SELECT 1 FROM sys.indexes"
        "  WHERE name = 'CIX_ext_spend_analysis_lines_kmd_uddata'"
        "    AND object_id = OBJECT_ID("
        "      N'[ext].[spend_analysis_lines_kmd_uddata]')"
        ")"
        " CREATE CLUSTERED INDEX [CIX_ext_spend_analysis_lines_kmd_uddata]"
        "   ON [ext].[spend_analysis_lines_kmd_uddata]"
        "   ([DW_EK_SpendAnalysisLine])"
    )
    context.log.info(
        "Creating clustered index on ext.spend_analysis_lines_kmd_uddata "
        "([DW_EK_SpendAnalysisLine]) for MERGE performance."
    )
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(ix_sql)
            conn.commit()
        finally:
            cursor.close()
