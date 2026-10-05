# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: KMD ref UNSPSC match → fct apply-pass on fct.Fakturalinje.

Seeds isolated KMD arc rows plus a paired fct/stg row, materialises
ref_kmd_fakturalinje_unspsc_match (delta) and fct_apply_fakturalinje_unspsc,
then asserts dw_ek_UNSPSC is stamped.

Gated by USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'.
"""

import os
import sys
from datetime import datetime

import DSA.SQL.fct.apply_fakturalinje_unspsc  # noqa: F401
import DSA.SQL.ref.fakturalinje_matchkeys  # noqa: F401
import DSA.SQL.ref.kmd_fakturalinje_unspsc_match  # noqa: F401
import pytest
from core.dfm_config import RunModeConfig
from dagster import build_asset_context

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true"
        or "dev" not in (os.environ.get("TEST_ENV") or ""),
        reason="Requires USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'",
    ),
]

_TEST_INVOICE_SK = 99_988_801
_TEST_INVOICE_LINE_SK = 1
_TEST_SAL_EK = 99_988_802
_TEST_LINE_ITEM_SK = 99_988_803
_TEST_UNSPSC_SK = 99_988_901
_TEST_UNSPSC_CODE = "99889001"
_TEST_CVR_RAW = "DK99887766"
_TEST_FAKTURA_NR = "UNSPIT9988"
_TEST_CLEANCODE_KREDITOR = "CVR_99887766"
_TEST_CLEANCODE_FAKTURA = "FAKTURA_UNSPIT9988"
_TEST_CLEANCODE_FAKTURALINJE = "UNSPIT9988_ITEST_LINE"
_TEST_LINE_AMOUNT = 100.50
_TEST_QTY = 2
_TEST_UNIT_PRICE = 50.25

_ref_module = sys.modules["DSA.SQL.ref.kmd_fakturalinje_unspsc_match"]
_mk_module = sys.modules["DSA.SQL.ref.fakturalinje_matchkeys"]
_apply_module = sys.modules["DSA.SQL.fct.apply_fakturalinje_unspsc"]
_ref_fn = _ref_module.ref_kmd_fakturalinje_unspsc_match.op.compute_fn.decorated_fn
_mk_fn = _mk_module.ref_fakturalinje_matchkeys.op.compute_fn.decorated_fn
_apply_fn = _apply_module.fct_apply_fakturalinje_unspsc.op.compute_fn.decorated_fn


def _tables_ready(cursor) -> bool:
    cursor.execute(
        """
        SELECT
            OBJECT_ID('ref.KMDFakturalinjeUNSPSCMatch') AS ref_match,
            OBJECT_ID('arc.kmd_uddata_spend_analysis_lines') AS sal,
            OBJECT_ID('arc.kmd_uddata_invoice') AS inv,
            OBJECT_ID('arc.kmd_uddata_unspsc') AS ku,
            OBJECT_ID('map.UNSPSC') AS map_u,
            OBJECT_ID('dim.UNSPSC') AS dim_u,
            OBJECT_ID('fct.Fakturalinje') AS fct_fl,
            OBJECT_ID('map.Fakturalinje') AS map_fl,
            OBJECT_ID('ref.FakturalinjeMatchKeys') AS mk,
            -- The apply-pass scopes its delta on the match-key table's own copy of
            -- the invoice-date key; skip rather than fail if the migration that adds
            -- it (20260824_matchkeys_dato_faktura_and_delta_index) has not run yet.
            COL_LENGTH('ref.FakturalinjeMatchKeys', 'dw_ek_Dato_faktura') AS mk_dato
        """
    )
    return all(x is not None for x in cursor.fetchone())


def _ensure_stg_fct_table(cursor) -> None:
    cursor.execute(
        """
        IF OBJECT_ID(N'[stg].[fct_Fakturalinje_mysupply]', 'U') IS NULL
        BEGIN
            CREATE TABLE [stg].[fct_Fakturalinje_mysupply] (
                [clean_code_Fakturalinje] NVARCHAR(500) NOT NULL,
                [bk_faktura_lines] NVARCHAR(500) NULL,
                [bk_faktura_header] NVARCHAR(500) NULL,
                [bk_faktura_metadata] NVARCHAR(500) NULL,
                [clean_code_kreditor] NVARCHAR(200) NULL,
                [clean_code_Kreditor_header] NVARCHAR(200) NULL,
                [clean_code_faktura] NVARCHAR(500) NULL,
                [faktura_dato] NVARCHAR(50) NULL,
                [antal_enheder_faktureret] DECIMAL(18, 4) NULL,
                [linje_beloeb] DECIMAL(18, 4) NULL,
                [base_price_enheds_pris] DECIMAL(18, 4) NULL,
                [header_faktura_total] DECIMAL(18, 4) NULL,
                [metadata_faktura_total] DECIMAL(18, 4) NULL
            );
        END
        """
    )


def _seed_fixture(cursor) -> None:
    now = datetime.now()
    _ensure_stg_fct_table(cursor)

    cursor.execute(
        """
        IF NOT EXISTS (SELECT 1 FROM [map].[UNSPSC] WHERE clean_code_UNSPSC = ?)
        INSERT INTO [map].[UNSPSC] (
            clean_code_UNSPSC, dw_source_ils, dw_source_kmd,
            dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (?, 0, 1, SYSDATETIME(), SYSDATETIME());
        """,
        (_TEST_UNSPSC_CODE, _TEST_UNSPSC_CODE),
    )
    cursor.execute(
        """
        DECLARE @map_ek INT = (
            SELECT dw_ek_UNSPSC FROM [map].[UNSPSC] WHERE clean_code_UNSPSC = ?
        );
        IF NOT EXISTS (SELECT 1 FROM [dim].[UNSPSC] WHERE dw_ek_UNSPSC = @map_ek)
        INSERT INTO [dim].[UNSPSC] (
            dw_ek_UNSPSC, unspsc_kode, dw_source_ils, dw_source_kmd,
            beskrivelse_kmd, dw_timestamp_insert, dw_timestamp_update
        )
        VALUES (@map_ek, ?, 0, 1, N'ITEST UNSPSC', SYSDATETIME(), SYSDATETIME());
        """,
        (_TEST_UNSPSC_CODE, _TEST_UNSPSC_CODE),
    )
    cursor.execute(
        "SELECT dw_ek_UNSPSC FROM [map].[UNSPSC] WHERE clean_code_UNSPSC = ?",
        (_TEST_UNSPSC_CODE,),
    )
    map_unspsc_ek = cursor.fetchone()[0]

    cursor.execute(
        """
        MERGE [arc].[kmd_uddata_unspsc] AS tgt
        USING (
            SELECT
                ? AS DW_SK_UNSPSC,
                ? AS DW_EK_UNSPSC,
                ? AS UNSPSCClassID,
                CAST(1 AS BIT) AS DW_IsCurrent,
                ? AS dw_timestamp_insert,
                ? AS dw_timestamp_update
        ) AS src
        ON tgt.DW_SK_UNSPSC = src.DW_SK_UNSPSC
        WHEN MATCHED THEN UPDATE SET
            tgt.UNSPSCClassID = src.UNSPSCClassID,
            tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC,
            tgt.DW_IsCurrent = src.DW_IsCurrent,
            tgt.dw_timestamp_update = src.dw_timestamp_update
        WHEN NOT MATCHED THEN INSERT (
            DW_SK_UNSPSC, DW_EK_UNSPSC, UNSPSCClassID, DW_IsCurrent,
            dw_timestamp_insert, dw_timestamp_update
        ) VALUES (
            src.DW_SK_UNSPSC, src.DW_EK_UNSPSC, src.UNSPSCClassID, src.DW_IsCurrent,
            src.dw_timestamp_insert, src.dw_timestamp_update
        );
        """,
        (_TEST_UNSPSC_SK, map_unspsc_ek, _TEST_UNSPSC_CODE, now, now),
    )

    cursor.execute(
        """
        MERGE [arc].[kmd_uddata_invoice] AS tgt
        USING (
            SELECT
                ? AS DW_SK_Invoice,
                ? AS DW_EK_Invoice,
                ? AS SupplierCompanyTaxID,
                ? AS InvoiceNumber,
                ? AS LineExtensionTotalAmount,
                CAST(1 AS BIT) AS DW_IsCurrent,
                ? AS dw_timestamp_insert,
                ? AS dw_timestamp_update
        ) AS src
        ON tgt.DW_SK_Invoice = src.DW_SK_Invoice
        WHEN MATCHED THEN UPDATE SET
            tgt.SupplierCompanyTaxID = src.SupplierCompanyTaxID,
            tgt.InvoiceNumber = src.InvoiceNumber,
            tgt.LineExtensionTotalAmount = src.LineExtensionTotalAmount,
            tgt.DW_IsCurrent = src.DW_IsCurrent,
            tgt.dw_timestamp_update = src.dw_timestamp_update
        WHEN NOT MATCHED THEN INSERT (
            DW_SK_Invoice, DW_EK_Invoice, SupplierCompanyTaxID, InvoiceNumber,
            LineExtensionTotalAmount, DW_IsCurrent,
            dw_timestamp_insert, dw_timestamp_update
        ) VALUES (
            src.DW_SK_Invoice, src.DW_EK_Invoice, src.SupplierCompanyTaxID,
            src.InvoiceNumber, src.LineExtensionTotalAmount, src.DW_IsCurrent,
            src.dw_timestamp_insert, src.dw_timestamp_update
        );
        """,
        (
            _TEST_INVOICE_SK,
            _TEST_INVOICE_SK,
            _TEST_CVR_RAW,
            _TEST_FAKTURA_NR,
            _TEST_LINE_AMOUNT,
            now,
            now,
        ),
    )

    cursor.execute(
        """
        MERGE [arc].[kmd_uddata_spend_analysis_lines] AS tgt
        USING (
            SELECT
                ? AS DW_EK_SpendAnalysisLine,
                ? AS DW_SK_Invoice,
                ? AS DW_SK_InvoiceLine,
                ? AS DW_SK_SpendAnalysisLineItem,
                ? AS DW_SK_UNSPSC,
                CAST(? AS NUMERIC(38, 6)) AS LineAmount,
                CAST(? AS NUMERIC(38, 6)) AS invoiced_quantity,
                CAST(? AS NUMERIC(38, 6)) AS unit_price,
                CAST('2024-06-01' AS DATE) AS DW_ValidDate,
                ? AS dw_timestamp_insert,
                ? AS dw_timestamp_update
        ) AS src
        ON tgt.DW_EK_SpendAnalysisLine = src.DW_EK_SpendAnalysisLine
        WHEN MATCHED THEN UPDATE SET
            tgt.DW_SK_Invoice = src.DW_SK_Invoice,
            tgt.DW_SK_InvoiceLine = src.DW_SK_InvoiceLine,
            tgt.DW_SK_SpendAnalysisLineItem = src.DW_SK_SpendAnalysisLineItem,
            tgt.DW_SK_UNSPSC = src.DW_SK_UNSPSC,
            tgt.LineAmount = src.LineAmount,
            tgt.invoiced_quantity = src.invoiced_quantity,
            tgt.unit_price = src.unit_price,
            tgt.DW_ValidDate = src.DW_ValidDate,
            tgt.dw_timestamp_update = src.dw_timestamp_update
        WHEN NOT MATCHED THEN INSERT (
            DW_EK_SpendAnalysisLine, DW_SK_Invoice, DW_SK_InvoiceLine,
            DW_SK_SpendAnalysisLineItem, DW_SK_UNSPSC,
            LineAmount, invoiced_quantity, unit_price, DW_ValidDate,
            dw_timestamp_insert, dw_timestamp_update
        ) VALUES (
            src.DW_EK_SpendAnalysisLine, src.DW_SK_Invoice, src.DW_SK_InvoiceLine,
            src.DW_SK_SpendAnalysisLineItem, src.DW_SK_UNSPSC,
            src.LineAmount, src.invoiced_quantity, src.unit_price, src.DW_ValidDate,
            src.dw_timestamp_insert, src.dw_timestamp_update
        );
        """,
        (
            _TEST_SAL_EK,
            _TEST_INVOICE_SK,
            _TEST_INVOICE_LINE_SK,
            _TEST_LINE_ITEM_SK,
            _TEST_UNSPSC_SK,
            _TEST_LINE_AMOUNT,
            _TEST_QTY,
            _TEST_UNIT_PRICE,
            now,
            now,
        ),
    )

    cursor.execute(
        """
        DELETE FROM [stg].[fct_Fakturalinje_mysupply]
        WHERE clean_code_Fakturalinje = ?;

        INSERT INTO [stg].[fct_Fakturalinje_mysupply] (
            clean_code_Fakturalinje,
            clean_code_kreditor,
            clean_code_Kreditor_header,
            clean_code_faktura,
            faktura_dato,
            antal_enheder_faktureret,
            linje_beloeb,
            base_price_enheds_pris,
            header_faktura_total,
            metadata_faktura_total
        )
        VALUES (?, ?, ?, ?, '2024-06-01', ?, ?, ?, ?, ?);
        """,
        (
            _TEST_CLEANCODE_FAKTURALINJE,
            _TEST_CLEANCODE_FAKTURALINJE,
            _TEST_CLEANCODE_KREDITOR,
            _TEST_CLEANCODE_KREDITOR,
            _TEST_CLEANCODE_FAKTURA,
            _TEST_QTY,
            _TEST_LINE_AMOUNT,
            _TEST_UNIT_PRICE,
            _TEST_LINE_AMOUNT,
            _TEST_LINE_AMOUNT,
        ),
    )

    # map.Fakturalinje owns dw_ek_Fakturalinje (IDENTITY); fct.Fakturalinje and
    # ref.FakturalinjeMatchKeys share that key. Seed the map row first, then
    # stamp the same dw_ek_Fakturalinje onto the fct row so the matchkeys→fct
    # apply-join has something to bridge on (post migration 20260706).
    cursor.execute(
        """
        DELETE FROM [map].[Fakturalinje] WHERE clean_code_Fakturalinje = ?;

        INSERT INTO [map].[Fakturalinje] (
            clean_code_Fakturalinje, bk_faktura_header, bk_faktura_metadata
        )
        VALUES (?, ?, ?);
        """,
        (
            _TEST_CLEANCODE_FAKTURALINJE,
            _TEST_CLEANCODE_FAKTURALINJE,
            "ITEST_HDR",
            "ITEST_META",
        ),
    )
    cursor.execute(
        "SELECT dw_ek_Fakturalinje FROM [map].[Fakturalinje] WHERE clean_code_Fakturalinje = ?",
        (_TEST_CLEANCODE_FAKTURALINJE,),
    )
    dw_ek_fakturalinje = cursor.fetchone()[0]

    cursor.execute(
        """
        DELETE FROM [fct].[Fakturalinje] WHERE clean_code_Fakturalinje = ?;

        INSERT INTO [fct].[Fakturalinje] (
            dw_ek_Fakturalinje,
            clean_code_Fakturalinje,
            dw_ek_Kreditor,
            dw_ek_Leverandoervare,
            dw_ek_Faktura,
            dw_ek_EAN_regningsmodtager,
            dw_ek_UNSPSC,
            dw_ek_Dato_faktura,
            faktura_nr,
            antal_enheder_faktureret,
            linje_beloeb,
            enheds_pris,
            dw_timestamp_insert,
            dw_timestamp_update
        )
        VALUES (?, ?, -1, -1, -1, -1, -1, 20240601, ?, ?, ?, ?, SYSDATETIME(), SYSDATETIME());
        """,
        (
            dw_ek_fakturalinje,
            _TEST_CLEANCODE_FAKTURALINJE,
            _TEST_CLEANCODE_FAKTURALINJE,
            _TEST_FAKTURA_NR,
            _TEST_QTY,
            _TEST_LINE_AMOUNT,
            _TEST_UNIT_PRICE,
        ),
    )

    cursor.execute(
        """
        DELETE FROM [ref].[KMDFakturalinjeUNSPSCMatch]
        WHERE clean_code_kreditor = ? AND clean_code_faktura = ?;
        """,
        (_TEST_CLEANCODE_KREDITOR, _TEST_CLEANCODE_FAKTURA),
    )


def _cleanup_fixture(cursor) -> None:
    cursor.execute(
        """
        DELETE mk FROM [ref].[FakturalinjeMatchKeys] mk
        INNER JOIN [map].[Fakturalinje] m ON m.dw_ek_Fakturalinje = mk.dw_ek_Fakturalinje
        WHERE m.clean_code_Fakturalinje = ?;
        """,
        (_TEST_CLEANCODE_FAKTURALINJE,),
    )
    cursor.execute(
        "DELETE FROM [fct].[Fakturalinje] WHERE clean_code_Fakturalinje = ?",
        (_TEST_CLEANCODE_FAKTURALINJE,),
    )
    cursor.execute(
        "DELETE FROM [map].[Fakturalinje] WHERE clean_code_Fakturalinje = ?",
        (_TEST_CLEANCODE_FAKTURALINJE,),
    )
    cursor.execute(
        "DELETE FROM [stg].[fct_Fakturalinje_mysupply] WHERE clean_code_Fakturalinje = ?",
        (_TEST_CLEANCODE_FAKTURALINJE,),
    )
    cursor.execute(
        """
        DELETE FROM [ref].[KMDFakturalinjeUNSPSCMatch]
        WHERE clean_code_kreditor = ? AND clean_code_faktura = ?;
        """,
        (_TEST_CLEANCODE_KREDITOR, _TEST_CLEANCODE_FAKTURA),
    )
    cursor.execute(
        "DELETE FROM [arc].[kmd_uddata_spend_analysis_lines] WHERE DW_EK_SpendAnalysisLine = ?",
        (_TEST_SAL_EK,),
    )
    cursor.execute(
        "DELETE FROM [arc].[kmd_uddata_invoice] WHERE DW_SK_Invoice = ?",
        (_TEST_INVOICE_SK,),
    )
    cursor.execute(
        "DELETE FROM [arc].[kmd_uddata_unspsc] WHERE DW_SK_UNSPSC = ?",
        (_TEST_UNSPSC_SK,),
    )
    cursor.execute(
        "DELETE FROM [dim].[UNSPSC] WHERE unspsc_kode = ? AND dw_ek_UNSPSC <> -1",
        (_TEST_UNSPSC_CODE,),
    )
    cursor.execute(
        "DELETE FROM [map].[UNSPSC] WHERE clean_code_UNSPSC = ?",
        (_TEST_UNSPSC_CODE,),
    )


@pytest.mark.order(6)
def test_fakturalinje_unspsc_ref_to_apply_pass(real_mssql_target_resource):
    """ref match build + apply-pass stamps dw_ek_UNSPSC on a seeded fct row."""
    writes = os.environ.get("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not writes:
        pytest.skip("Requires ENABLE_TEST_WRITES=true to seed fixture rows")

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if not _tables_ready(cursor):
                pytest.skip("Fakturalinje UNSPSC DDL not deployed in staging database")
            _seed_fixture(cursor)
            conn.commit()
        finally:
            cursor.close()

    ctx = build_asset_context()
    run_config = RunModeConfig(mode="delta", advance_checkpoint=False)

    try:
        _ref_fn(ctx, real_mssql_target_resource, run_config)
        _mk_fn(ctx, real_mssql_target_resource, run_config)
        _apply_fn(ctx, real_mssql_target_resource, run_config)

        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    """
                    SELECT
                        f.dw_ek_UNSPSC,
                        d.unspsc_kode,
                        r.match_tier
                    FROM [fct].[Fakturalinje] f
                    INNER JOIN [ref].[KMDFakturalinjeUNSPSCMatch] r
                        ON r.clean_code_kreditor = ?
                       AND r.clean_code_faktura = ?
                       AND r.dw_is_deleted = 0
                       AND r.match_tier = 1
                    INNER JOIN [dim].[UNSPSC] d
                        ON d.dw_ek_UNSPSC = f.dw_ek_UNSPSC
                    WHERE f.clean_code_Fakturalinje = ?
                    """,
                    (
                        _TEST_CLEANCODE_KREDITOR,
                        _TEST_CLEANCODE_FAKTURA,
                        _TEST_CLEANCODE_FAKTURALINJE,
                    ),
                )
                row = cursor.fetchone()
            finally:
                cursor.close()

        assert row is not None, "Expected seeded fakturalinje after apply-pass"
        assert row[0] != -1, "dw_ek_UNSPSC should resolve via tier-1 match"
        assert row[1] == _TEST_UNSPSC_CODE
        assert row[2] == 1
    finally:
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _cleanup_fixture(cursor)
                conn.commit()
            finally:
                cursor.close()
