# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
KMD UdData DFM archive layer — 3 UPSERT archives from ext.kmd_uddata_* into
arc.kmd_uddata_*, keyed on entity/SCD2 surrogate keys with audit-MAX checkpoints.

All three archives use execute_archive_merge + meta.DFMState (scope=staging),
the same DFM pattern as UNSPSC.

Column naming: arc mirrors the KMD source 1:1 — ext is dropped and recreated by
every extract with the source's column names, and the archive merge uses one
shared column list for both sides. KMD's own DW-audit columns (DW_ArchiveID,
DW_ValidFrom, DW_Audit_Insert, ...) are SOURCE data and keep KMD's spelling;
they are exempt from the repo naming standard, which only applies to columns
this DWH adds (dw_timestamp_insert/dw_timestamp_update, computed_max_audit).
"""

from collections.abc import Callable

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from dagster import AssetsDefinition

_SPEND_ANALYSIS_NON_KEY_COLUMNS: tuple[str, ...] = (
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

_INVOICE_NON_KEY_COLUMNS: tuple[str, ...] = (
    "DW_EK_Invoice",
    "GUID",
    "SupplierCompanyTaxID",
    "InvoiceNumber",
    "InvoiceUUID",
    "IsCreditnote",
    "Filename",
    "FileFormat",
    "InvoiceSize",
    "HasItemID",
    "HasUnitCode",
    "HasItemDescription",
    "HasOrderID",
    "HasOrderIssueDate",
    "HasUnitPrice",
    "InvoiceNote",
    "DeliveryLocationPostalZone",
    "DeliveryLocationAddressCityName",
    "DeliveryLocationStreetName",
    "DeliveryLocationBuildingNumber",
    "AccountingLocationCode",
    "LineExtensionTotalAmount",
    "PostalZoneSupplierFromInvoice",
    "SupplierCityFromInvoice",
    "SupplierAddressFromInvoice",
    "SupplierBuildingNumberFromInvoice",
    "BuyersOrderID",
    "BuyerReference",
    "BuyersAddress",
    "BuyersContact",
    "BuyersContactID",
    "BuyersEAN",
    "ContractNumber",
    "SupplierName",
    "SupplierContact",
    "SupplierContactID",
    "SupplierOrderID",
    "PaymentChannelCode",
    "PenaltySurchargeRateNumeric",
    "PersonalSecure",
    "PaymentPeriodDays",
    "ForeignCurrency",
    "InvoiceSizeID",
    "IsDiscarded",
    "DW_ArchiveID",
    "DW_ValidFrom",
    "DW_ValidTo",
    "DW_IsCurrent",
    "DW_Audit_Insert",
    "DW_Audit_Update",
    "DW_Audit",
)


def _audit_max_checkpoint(table: str) -> str:
    return f"SELECT CAST(MAX([computed_max_audit]) AS NVARCHAR(50)) FROM [arc].[{table}]"


def _kmd_arc_config(
    target_table: str,
    *,
    merge_keys: tuple[str, ...],
    non_key_columns: tuple[str, ...],
    enable_bulk_insert_on_first_load: bool = False,
) -> ArchiveMergeConfig:
    """``enable_bulk_insert_on_first_load`` opts a table into the single
    INSERT...SELECT fast path for its (empty-target) first load. Safe for the
    tables that enable it because their merge key is exactly the arc PK (a
    KMD DW surrogate key), so ext rows unique on the merge key cannot violate
    it. The two large tables (invoice, spend_analysis_lines) enable it; the
    small unspsc table uses the plain MERGE.
    """
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=target_table,
        target_schema="arc",
        target_table=target_table,
        merge_keys=merge_keys,
        non_key_columns=non_key_columns,
        checkpoint_query=_audit_max_checkpoint(target_table),
        strategy="upsert",
        enable_bulk_insert_on_first_load=enable_bulk_insert_on_first_load,
    )


UNSPSC_ARCHIVE_CONFIG = _kmd_arc_config(
    "kmd_uddata_unspsc",
    merge_keys=("DW_SK_UNSPSC",),
    non_key_columns=(
        "DW_EK_UNSPSC",
        "UNSPSCClassID",
        "UNSPSCClassDescription",
        "UNSPSCLevel",
        "TenderCategoryKey",
        "UNSPSCClassLev1ID",
        "UNSPSCClassLev1Description",
        "UNSPSCClassLev1Label",
        "UNSPSCClassLev2ID",
        "UNSPSCClassLev2Description",
        "UNSPSCClassLev2Label",
        "UNSPSCClassLev3ID",
        "UNSPSCClassLev3Description",
        "UNSPSCClassLev3Label",
        "UNSPSCClassLev4ID",
        "UNSPSCClassLev4Description",
        "UNSPSCClassLev4Label",
        "DW_ArchiveID",
        "DW_ValidFrom",
        "DW_ValidTo",
        "DW_IsCurrent",
        "DW_Audit_Insert",
        "DW_Audit_Update",
        "DW_Audit",
    ),
)

SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG = _kmd_arc_config(
    "kmd_uddata_spend_analysis_lines",
    merge_keys=("DW_EK_SpendAnalysisLine",),
    non_key_columns=_SPEND_ANALYSIS_NON_KEY_COLUMNS,
    enable_bulk_insert_on_first_load=True,
)

INVOICE_ARCHIVE_CONFIG = _kmd_arc_config(
    "kmd_uddata_invoice",
    merge_keys=("DW_SK_Invoice",),
    non_key_columns=_INVOICE_NON_KEY_COLUMNS,
    enable_bulk_insert_on_first_load=True,
)

kmd_uddata_archive_configs: list[ArchiveMergeConfig] = [
    UNSPSC_ARCHIVE_CONFIG,
    SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG,
    INVOICE_ARCHIVE_CONFIG,
]


# KMD er begyndt at levere dubletter i fact.SpendAnalysisLines: flere raekker
# med samme DW_EK_SpendAnalysisLine der kun afviger paa surrogat-FK'er, som
# peger paa dublerede dimension-members (identisk forretningsindhold). arc
# upsertes paa DW_EK_SpendAnalysisLine OG bulk-insert'er paa foerste load, saa
# begge stier braekker paa dubletter (MERGE: "update the same row more than
# once"; bulk-insert: PK-brud). Vi dedup'er derfor ext ned til én raekke pr.
# DW_EK: behold den senest auditerede version (GREATEST-af-audit via en
# portabel VALUES/MAX, saa den virker uden SQL 2022 GREATEST), med det laveste
# surrogat DW_SK_SpendAnalysisLineItem som deterministisk tiebreaker naar
# audit-vaerdierne er ens (dubletternes typiske tilfaelde).
_DEDUP_SPEND_ANALYSIS_EXT_SQL = """
WITH ranked AS (
    SELECT ROW_NUMBER() OVER (
        PARTITION BY [DW_EK_SpendAnalysisLine]
        ORDER BY
            (SELECT MAX(v) FROM (VALUES
                ([DW_Audit_Insert]), ([DW_Audit_Update]), ([DW_Audit])
            ) AS x(v)) DESC,
            [DW_SK_SpendAnalysisLineItem] ASC
    ) AS rn
    FROM [ext].[kmd_uddata_spend_analysis_lines]
)
DELETE FROM ranked WHERE rn > 1;
"""


def _prepare_spend_analysis_ext(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
) -> None:
    """Dedup ext paa DW_EK, saa clustered index paa ext before MERGE — dedup
    haandhaever unikheden paa merge-noeglen som MERGE/bulk-insert antager,
    indekset undgaar full-table scan timeouts."""
    ix_sql = (
        "IF NOT EXISTS ("
        "  SELECT 1 FROM sys.indexes"
        "  WHERE name = 'CIX_ext_kmd_uddata_spend_analysis_lines'"
        "    AND object_id = OBJECT_ID("
        "      N'[ext].[kmd_uddata_spend_analysis_lines]')"
        ")"
        " CREATE CLUSTERED INDEX [CIX_ext_kmd_uddata_spend_analysis_lines]"
        "   ON [ext].[kmd_uddata_spend_analysis_lines]"
        "   ([DW_EK_SpendAnalysisLine])"
    )
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # Dedup FOER indeks: en mindre, unik-paa-merge-noegle heap er
            # baade korrekt input til MERGE/bulk-insert og hurtigere at indeksere.
            cursor.execute(_DEDUP_SPEND_ANALYSIS_EXT_SQL)
            dropped = cursor.rowcount
            if dropped and dropped > 0:
                context.log.warning(
                    f"Fjernede {dropped} KMD-dublet(ter) fra "
                    "ext.kmd_uddata_spend_analysis_lines (samme DW_EK_SpendAnalysisLine)."
                )
            context.log.info(
                "Creating clustered index on ext.kmd_uddata_spend_analysis_lines "
                "([DW_EK_SpendAnalysisLine]) for MERGE performance."
            )
            cursor.execute(ix_sql)
            conn.commit()
        finally:
            cursor.close()


def _make_archive_asset(
    merge_config: ArchiveMergeConfig,
    *,
    pre_merge: Callable[[dg.AssetExecutionContext, MSSQLResource], None] | None = None,
    retry_policy: dg.RetryPolicy | None = None,
) -> AssetsDefinition:
    """Return a single @dg.asset for the given KMD UdData ArchiveMergeConfig."""
    entity = merge_config.target_table.removeprefix("kmd_uddata_")
    description = f"UPSERT ext.{merge_config.source_table} → arc.{merge_config.target_table}."
    if pre_merge is not None:
        description += " Creates ext clustered index before MERGE when needed."

    asset_kwargs: dict = {
        "name": f"arc_{merge_config.target_table}",
        "group_name": "kmd_uddata_archive",
        "compute_kind": "mssql",
        "deps": [dg.AssetKey([f"ext_{merge_config.source_table}"])],
        "tags": {
            "domain": "kmd_uddata",
            "layer": "archive",
            "source": "kmd_uddata",
            "entity": entity,
        },
        "description": description,
    }
    if retry_policy is not None:
        asset_kwargs["retry_policy"] = retry_policy

    @dg.asset(**asset_kwargs)
    def _arc_asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: RunModeConfig,
    ) -> dg.MaterializeResult:
        if pre_merge is not None:
            pre_merge(context, mssql_target_staging)

        result = execute_archive_merge(context, mssql_target_staging, merge_config, config)

        context.log.info(
            f"MERGEd into arc.{merge_config.target_table} — "
            f"inserted={result.rows_inserted}, updated={result.rows_updated}; "
            f"mode={config.mode}, checkpoint → {result.new_checkpoint}"
        )

        return dg.MaterializeResult(
            metadata=build_archive_metadata(
                result,
                config,
                source_table=f"ext.{merge_config.source_table}",
                target_table=f"arc.{merge_config.target_table}",
            )
        )

    return _arc_asset


arc_kmd_uddata_unspsc = _make_archive_asset(UNSPSC_ARCHIVE_CONFIG)

arc_kmd_uddata_spend_analysis_lines = _make_archive_asset(
    SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG,
    pre_merge=_prepare_spend_analysis_ext,
    retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)

arc_kmd_uddata_invoice = _make_archive_asset(INVOICE_ARCHIVE_CONFIG)

kmd_uddata_archive_assets: list[AssetsDefinition] = [
    arc_kmd_uddata_unspsc,
    arc_kmd_uddata_spend_analysis_lines,
    arc_kmd_uddata_invoice,
]

__all__ = [
    "UNSPSC_ARCHIVE_CONFIG",
    "SPEND_ANALYSIS_LINES_ARCHIVE_CONFIG",
    "INVOICE_ARCHIVE_CONFIG",
    "kmd_uddata_archive_configs",
    "arc_kmd_uddata_unspsc",
    "arc_kmd_uddata_spend_analysis_lines",
    "arc_kmd_uddata_invoice",
    "kmd_uddata_archive_assets",
]
