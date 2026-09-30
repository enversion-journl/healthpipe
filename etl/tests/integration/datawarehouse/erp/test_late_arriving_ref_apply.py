# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: late-arriving KMD ref self-heals an existing fct.Fakturalinje.

Regression for the matchkeys decoupling: a fakturalinje is loaded and the
apply-pass runs while ref.KMDFakturalinjeUNSPSCMatch has NO matching row, so the
line stays dw_ek_UNSPSC = -1. Later the matching KMD invoice/SAL arrives, ref is
rebuilt, and the apply-pass runs again WITHOUT re-touching stg/fct — the old line
must now be stamped, proving the ref-change delta reaches it via
ref.FakturalinjeMatchKeys.

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

# Distinct SK/clean-code space from test_fakturalinje_unspsc_apply_pipeline.py so
# the two ordered, data-leaving tests never collide.
_TEST_INVOICE_SK = 99_988_811
_TEST_INVOICE_LINE_SK = 1
_TEST_SAL_EK = 99_988_812
_TEST_LINE_ITEM_SK = 99_988_813
_TEST_UNSPSC_SK = 99_988_911
_TEST_UNSPSC_CODE = "99889011"
_TEST_CVR_RAW = "DK99887767"
_TEST_FAKTURA_NR = "UNSPIT9989"
_TEST_CLEANCODE_KREDITOR = "CVR_99887767"
_TEST_CLEANCODE_FAKTURA = "FAKTURA_UNSPIT9989"
_TEST_CLEANCODE_FAKTURALINJE = "UNSPIT9989_ITEST_LINE"
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
            OBJECT_ID('ref.FakturalinjeMatchKeys') AS mk,
            OBJECT_ID('arc.kmd_uddata_spend_analysis_lines') AS sal,
            OBJECT_ID('arc.kmd_uddata_invoice') AS inv,
            OBJECT_ID('arc.kmd_uddata_unspsc') AS ku,
            OBJECT_ID('map.UNSPSC') AS map_u,
            OBJECT_ID('dim.UNSPSC') AS dim_u,
            OBJECT_ID('fct.Fakturalinje') AS fct_fl,
            OBJECT_ID('map.Fakturalinje') AS map_fl,
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


def _seed_line_without_match(cursor) -> None:
    """Phase A: seed map/stg/fct line (-1) + arc UNSPSC/map bits, but NO KMD
    invoice/SAL rows — so the ref build produces no matching row for this line."""
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

    # map.Fakturalinje owns dw_ek_Fakturalinje (IDENTITY); fct + matchkeys share
    # that key. Seed the map row, then stamp its dw_ek_Fakturalinje onto fct.
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

    # Ensure no stale ref match exists for this key from a previous run.
    cursor.execute(
        """
        DELETE FROM [ref].[KMDFakturalinjeUNSPSCMatch]
        WHERE clean_code_kreditor = ? AND clean_code_faktura = ?;
        """,
        (_TEST_CLEANCODE_KREDITOR, _TEST_CLEANCODE_FAKTURA),
    )


def _seed_matching_kmd(cursor) -> None:
    """Phase B: the KMD invoice/SAL arrive — seed exactly the arc rows that make
    the ref build produce a tier-1 match for this line."""
    now = datetime.now()

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


def _remove_matching_kmd(cursor) -> None:
    """Remove the KMD invoice + SAL so the next ref rebuild soft-deletes the match
    (dw_is_deleted = 1) — a deletion-only ref change with fct untouched."""
    cursor.execute(
        "DELETE FROM [arc].[kmd_uddata_spend_analysis_lines] WHERE DW_EK_SpendAnalysisLine = ?",
        (_TEST_SAL_EK,),
    )
    cursor.execute(
        "DELETE FROM [arc].[kmd_uddata_invoice] WHERE DW_SK_Invoice = ?",
        (_TEST_INVOICE_SK,),
    )


def _fetch_unspsc(resource):
    with resource.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT dw_ek_UNSPSC FROM [fct].[Fakturalinje] WHERE clean_code_Fakturalinje = ?",
                (_TEST_CLEANCODE_FAKTURALINJE,),
            )
            return cur.fetchone()[0]
        finally:
            cur.close()


def _cleanup(resource) -> None:
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                DELETE mk FROM [ref].[FakturalinjeMatchKeys] mk
                INNER JOIN [map].[Fakturalinje] m
                    ON m.dw_ek_Fakturalinje = mk.dw_ek_Fakturalinje
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
            conn.commit()
        finally:
            cursor.close()


@pytest.mark.order(7)
def test_late_arriving_ref_backfills_existing_fakturalinje(real_mssql_target_resource):
    """Line loaded while ref empty (-1); ref match arrives later; next apply stamps
    it WITHOUT re-touching stg/fct."""
    writes = os.environ.get("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not writes:
        pytest.skip("Requires ENABLE_TEST_WRITES=true to seed fixture rows")

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if not _tables_ready(cursor):
                pytest.skip("Fakturalinje UNSPSC DDL not deployed in staging database")
        finally:
            cursor.close()

    ctx = build_asset_context()
    cfg = RunModeConfig(mode="delta", advance_checkpoint=False)

    try:
        # Phase A: line loaded, ref has no match → apply leaves dw_ek_UNSPSC = -1.
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _seed_line_without_match(cursor)
                conn.commit()
            finally:
                cursor.close()

        _ref_fn(ctx, real_mssql_target_resource, cfg)
        _mk_fn(ctx, real_mssql_target_resource, cfg)
        _apply_fn(ctx, real_mssql_target_resource, cfg)

        assert (
            _fetch_unspsc(real_mssql_target_resource) == -1
        ), "Line must stay unmatched while ref has no matching row"

        # Phase B: KMD arrives — seed the matching invoice/SAL, rebuild ref, and
        # re-run apply WITHOUT touching stg/fct. matchkeys is re-materialised but
        # its keys are unchanged, so this proves the heal comes from the ref delta.
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _seed_matching_kmd(cursor)
                conn.commit()
            finally:
                cursor.close()

        _ref_fn(ctx, real_mssql_target_resource, cfg)
        _mk_fn(ctx, real_mssql_target_resource, cfg)
        _apply_fn(ctx, real_mssql_target_resource, cfg)

        assert (
            _fetch_unspsc(real_mssql_target_resource) != -1
        ), "Old line must be stamped once the late KMD ref arrives (self-heal)"
    finally:
        _cleanup(real_mssql_target_resource)


@pytest.mark.order(8)
def test_ref_deletion_demotes_existing_fakturalinje(real_mssql_target_resource):
    """A matched line is demoted to -1 once its KMD ref match is removed.

    Regression for the demote path over ref.FakturalinjeMatchKeys: the apply-pass
    reacts to a deletion-only ref change (fct untouched) and resets dw_ek_UNSPSC.
    #RefChangedKeys triggers on ANY ref change (no dw_is_deleted filter), and the
    tier LEFT JOINs match live rows only, so a fully soft-deleted match resolves to
    -1 and REFRESH writes that -1 back. There is no separate demote pass: this test
    is what pins that REFRESH alone still clears the stale code.
    """
    writes = os.environ.get("ENABLE_TEST_WRITES", "false").lower() == "true"
    if not writes:
        pytest.skip("Requires ENABLE_TEST_WRITES=true to seed fixture rows")

    with real_mssql_target_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            if not _tables_ready(cursor):
                pytest.skip("Fakturalinje UNSPSC DDL not deployed in staging database")
        finally:
            cursor.close()

    ctx = build_asset_context()
    cfg = RunModeConfig(mode="delta", advance_checkpoint=False)

    try:
        # Phase 1: line + matching KMD → apply stamps a real UNSPSC code.
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _seed_line_without_match(cursor)
                _seed_matching_kmd(cursor)
                conn.commit()
            finally:
                cursor.close()

        _ref_fn(ctx, real_mssql_target_resource, cfg)
        _mk_fn(ctx, real_mssql_target_resource, cfg)
        _apply_fn(ctx, real_mssql_target_resource, cfg)

        assert (
            _fetch_unspsc(real_mssql_target_resource) != -1
        ), "Line must be stamped once the KMD ref match exists"

        # Phase 2: KMD match removed → ref rebuild soft-deletes it → apply demotes.
        # stg/fct are NOT touched: the demote is driven purely by the ref change.
        with real_mssql_target_resource.get_connection() as conn:
            cursor = conn.cursor()
            try:
                _remove_matching_kmd(cursor)
                conn.commit()
            finally:
                cursor.close()

        _ref_fn(ctx, real_mssql_target_resource, cfg)
        _mk_fn(ctx, real_mssql_target_resource, cfg)
        _apply_fn(ctx, real_mssql_target_resource, cfg)

        assert (
            _fetch_unspsc(real_mssql_target_resource) == -1
        ), "Line must be demoted to -1 once its ref match is soft-deleted"
    finally:
        _cleanup(real_mssql_target_resource)
