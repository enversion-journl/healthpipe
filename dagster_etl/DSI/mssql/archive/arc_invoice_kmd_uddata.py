# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive Merge Asset: KMD dimension.Invoice

UPSERTs ext.invoice_kmd_uddata into arc.invoice_kmd_uddata keyed on
DW_SK_Invoice (the SCD2 surrogate key, unique per row). DW_EK_Invoice is
the entity key and may repeat across SCD2 versions in the source view.
Advances the audit-MAX watermark.
"""

import dagster as dg
from core.etl_watermark_manager import EtlWatermarkManager
from core.resources import MSSQLResource

_NON_KEY_COLUMNS: tuple[str, ...] = (
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

_UPDATE_ASSIGNMENTS = ",\n            ".join(f"tgt.[{c}] = src.[{c}]" for c in _NON_KEY_COLUMNS)
_INSERT_COLUMNS = ", ".join(f"[{c}]" for c in ("DW_SK_Invoice", *_NON_KEY_COLUMNS))
_INSERT_VALUES = ", ".join(f"src.[{c}]" for c in ("DW_SK_Invoice", *_NON_KEY_COLUMNS))


MERGE_SQL = f"""
    MERGE [arc].[invoice_kmd_uddata] AS tgt
    USING [ext].[invoice_kmd_uddata] AS src
        ON tgt.[DW_SK_Invoice] = src.[DW_SK_Invoice]
    WHEN MATCHED THEN UPDATE SET
            {_UPDATE_ASSIGNMENTS},
            tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
    WHEN NOT MATCHED THEN INSERT (
        {_INSERT_COLUMNS},
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
    VALUES (
        {_INSERT_VALUES},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
"""

NEW_WATERMARK_SQL = """
    SELECT CAST(MAX([computed_max_audit]) AS NVARCHAR(50))
    FROM [arc].[invoice_kmd_uddata];
"""


@dg.asset(
    name="arc_invoice_kmd_uddata",
    group_name="kmd_uddata_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_invoice_kmd_uddata"])],
    tags={
        "domain": "kmd_uddata",
        "layer": "archive",
        "source": "kmd_uddata",
        "entity": "invoice",
    },
    description=(
        "MERGE upsert ext.invoice_kmd_uddata → arc.invoice_kmd_uddata; "
        "advance audit-MAX watermark."
    ),
)
def arc_invoice_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """UPSERT ext → arc; advance watermark to MAX(computed_max_audit) from arc."""

    wm = EtlWatermarkManager(mssql_target_staging, context)
    watermark_before = wm.get_watermark("archive", "arc", "invoice_kmd_uddata")
    log_id = wm.log_start_run(
        "archive",
        "arc",
        "invoice_kmd_uddata",
        watermark_before=watermark_before,
        dagster_run_id=context.run_id,
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(MERGE_SQL)
                row_count = cursor.rowcount

                cursor.execute(NEW_WATERMARK_SQL)
                row = cursor.fetchone()
                new_watermark: str | None = row[0] if row is not None else None

                conn.commit()
            finally:
                cursor.close()

        context.log.info(
            f"MERGEd {row_count} rows into arc.invoice_kmd_uddata; "
            f"new watermark = {new_watermark}"
        )

        if new_watermark is not None:
            wm.advance_watermark("archive", "arc", "invoice_kmd_uddata", value=new_watermark)
        else:
            context.log.warning("arc.invoice_kmd_uddata is empty after MERGE; watermark unchanged.")

        wm.log_complete_run(
            log_id,
            watermark_after=new_watermark,
            rows_affected=row_count,
        )

    except Exception:
        wm.log_fail_run(log_id)
        raise

    return dg.MaterializeResult(
        metadata={
            "row_count": row_count,
            "source_table": "ext.invoice_kmd_uddata",
            "target_table": "arc.invoice_kmd_uddata",
            "watermark_before": watermark_before,
            "watermark_after": new_watermark,
            "strategy": "upsert",
        }
    )
