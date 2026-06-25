# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S8 — Measure grain & fan-out regression.

This PR moved the header / zero-rated VAT measures off ``fct.Fakturalinje`` and
onto ``fct.Faktura`` because they originate from the invoice header and were
repeated verbatim on every line, causing an N× overcount when summed at line
grain. It also added per-grain ``GebyrBeloeb`` / ``RabatBeloeb`` measures summed
via pre-aggregating CTEs (``hac_sum`` / ``lac_sum``).

These two scenarios materialise a genuinely multi-line invoice and assert the
measures land at the correct grain — i.e. the exact regressions that would
reappear if a moved column came back to the line, or if a header-grain
allowance were joined (and fanned out) at line grain:

  1. ``test_header_measures_do_not_fan_out_to_lines`` — the moved columns must
     not exist on ``fct.Fakturalinje`` (so they structurally cannot be summed
     across the N lines), while the header measures appear exactly once on
     ``fct.Faktura``.
  2. ``test_gebyr_rabat_measures_at_correct_grain`` — a header-level charge and
     a header-level discount land once on ``fct.Faktura``; the per-line charges
     sum from the LINE allowance rows on ``fct.Fakturalinje`` and are NOT the
     header value multiplied by line count.

These are the only execution-order scenarios that seed non-empty allowance /
charge ext rows — every other scenario leaves the conftest stubs empty.
"""

from __future__ import annotations

from decimal import Decimal

import pytest
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    make_invoice,
    seed_mysupply,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

DAY = "2025-06-01"
CVR = "99900081"
N_LINES = 3

# Columns this PR moved OFF the line grain (fct.Fakturalinje) and ONTO the
# header grain (fct.Faktura). If any of these reappears on fct.Fakturalinje,
# the header value fans out N× when the line table is aggregated — the bug the
# PR fixes (see migration 20260611_fakturalinje_remove_header_measures.sql and
# 20260611_fakturalinje_move_zerorated_to_faktura.sql).
MOVED_TO_FAKTURA = [
    "LinjeTotal",
    "VAT_AfgiftsKategori",
    "Valutakode_MomsGrundlag",
    "Valutakode_MomsBeloeb",
    "Valutakode_LinjeTotal",
    "Valutakode_FakturaTotal",
    "ZeroRated_MomsGrundlag",
    "Valutakode_ZeroRated_MomsGrundlag",
    "ZeroRated_MomsBeloeb",
    "Valutakode_ZeroRated_MomsBeloeb",
]


def _column_exists(cursor, schema: str, table: str, column: str) -> bool:
    cursor.execute(
        """
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME = ?
        """,
        (schema, table, column),
    )
    return cursor.fetchone() is not None


def _seed_allowance_charge(
    target_conn,
    *,
    header_id: int,
    header_charges: list[tuple[str, str]],
    line_charges: dict[int, list[tuple[str, str]]],
) -> None:
    """Insert allowance/charge rows directly into the ext stub tables.

    The allowance_charge extracts are excluded from the runner (the local source
    DB has a stale schema for them), so the staging CTEs read from the ext stub
    tables the conftest provisions. We populate those directly. ``clean_dw``
    truncates them before each test, so a plain insert is idempotent.

    ``header_charges`` / per-line lists are ``(total, indikering)`` pairs where
    ``indikering == 'false'`` -> RabatBeloeb (discount) and anything else ->
    GebyrBeloeb (charge), matching the production HAC/LAC CTE split.
    """
    cur = target_conn.cursor()
    try:
        seq = 0
        for total, indikering in header_charges:
            seq += 1
            cur.execute(
                """
                INSERT INTO [ext].[statistic_invoice_header_allowance_charge_mysupply]
                    (id, statistic_invoice_header_id, GebyrKategori,
                     GebyrKategoriIndikering, [GebyrKategoriÅrsag],
                     GebyrKategoriTotal, GebyrKategoriFinanskonto, DW_LoadTimestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, SYSDATETIME())
                """,
                (
                    header_id * 100 + seq,
                    header_id,
                    "TESTFEE",
                    indikering,
                    "Testaarsag",
                    total,
                    "9999",
                ),
            )
        for line_id, charges in line_charges.items():
            for total, indikering in charges:
                seq += 1
                cur.execute(
                    """
                    INSERT INTO [ext].[statistic_invoice_line_allowance_charge_mysupply]
                        (id, statistic_invoice_line_id, FakturaUdstederGebyrKategori,
                         FakturaUdstederGebyrKategoriIndikering,
                         [FakturaUdstederGebyrKategoriÅrsag],
                         FakturaUdstederGebyrKategoriTotal,
                         FakturaUdstederGebyrKategoriFinanskonto, DW_LoadTimestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, SYSDATETIME())
                    """,
                    (
                        line_id * 100 + seq,
                        line_id,
                        "TESTFEE",
                        indikering,
                        "Testaarsag",
                        total,
                        "9999",
                    ),
                )
        target_conn.commit()
    finally:
        cur.close()


class TestS8MeasureGrainFanout:
    def test_header_measures_do_not_fan_out_to_lines(
        self, clean_dw, pipeline_runner, mysupply_conn, target_conn
    ):
        faktura_nr = "TEST-EXEC-FANOUT-HDR"
        invoice = make_invoice(
            idx=80,
            cvr=CVR,
            faktura_nr=faktura_nr,
            dato=DAY,
            n_lines=N_LINES,
            moms_grundlag="800.00",
            moms_beloeb="200.00",
            faktura_total="1000.00",
        )
        seed_mysupply(mysupply_conn, [invoice])

        pipeline_runner.run_daily(DAY)

        cursor = target_conn.cursor()
        try:
            # One header row, N line rows — genuinely multi-line, so a header
            # value left on the line WOULD fan out when summed.
            cursor.execute(
                "SELECT COUNT(*) FROM [fct].[Faktura] WHERE [FakturaNr] = ?",
                (faktura_nr,),
            )
            assert cursor.fetchone()[0] == 1, "expected exactly one fct.Faktura row"

            cursor.execute(
                "SELECT COUNT(*) FROM [fct].[Fakturalinje] WHERE [FakturaNr] = ?",
                (faktura_nr,),
            )
            assert (
                cursor.fetchone()[0] == N_LINES
            ), f"expected {N_LINES} fct.Fakturalinje rows for a {N_LINES}-line invoice"

            # Header measures present on fct.Faktura exactly once (not × N_LINES).
            cursor.execute(
                """
                SELECT SUM(CAST(MomsGrundlag AS DECIMAL(18, 4))),
                       SUM(CAST(MomsBeloeb AS DECIMAL(18, 4)))
                FROM [fct].[Faktura] WHERE [FakturaNr] = ?
                """,
                (faktura_nr,),
            )
            grundlag, beloeb = cursor.fetchone()
            assert grundlag == Decimal("800"), f"MomsGrundlag should be 800 once, got {grundlag}"
            assert beloeb == Decimal("200"), f"MomsBeloeb should be 200 once, got {beloeb}"

            # The moved columns must NOT exist on fct.Fakturalinje — so they
            # cannot be summed across the N lines (the old fan-out path) — and
            # MUST exist on fct.Faktura.
            for col in MOVED_TO_FAKTURA:
                assert not _column_exists(
                    cursor, "fct", "Fakturalinje", col
                ), f"{col} must not exist on fct.Fakturalinje (moved to fct.Faktura)"
                assert _column_exists(
                    cursor, "fct", "Faktura", col
                ), f"{col} should exist on fct.Faktura"
        finally:
            cursor.close()

    def test_gebyr_rabat_measures_at_correct_grain(
        self, clean_dw, pipeline_runner, mysupply_conn, target_conn
    ):
        faktura_nr = "TEST-EXEC-FANOUT-GEBYR"
        invoice = make_invoice(
            idx=81,
            cvr=CVR,
            faktura_nr=faktura_nr,
            dato=DAY,
            n_lines=N_LINES,
        )
        seed_mysupply(mysupply_conn, [invoice])

        # Header allowance: one 300 charge (gebyr) + one 100 discount (rabat).
        # Line allowance: each of the N lines carries a separate 10 charge.
        # The header (300) and line (10×N=30) totals are deliberately distinct
        # so a fan-out (300 appearing per line, summing to 300×N) is unmistakable.
        _seed_allowance_charge(
            target_conn,
            header_id=invoice.header_id,
            header_charges=[("300.00", "true"), ("100.00", "false")],
            line_charges={line.line_id: [("10.00", "true")] for line in invoice.lines},
        )

        pipeline_runner.run_daily(DAY)

        cursor = target_conn.cursor()
        try:
            # fct.Faktura: header allowance sums land once (NOT × N_LINES).
            cursor.execute(
                """
                SELECT COUNT(*), SUM(GebyrBeloeb), SUM(RabatBeloeb)
                FROM [fct].[Faktura] WHERE [FakturaNr] = ?
                """,
                (faktura_nr,),
            )
            faktura_cnt, faktura_gebyr, faktura_rabat = cursor.fetchone()
            assert faktura_cnt == 1, "expected exactly one fct.Faktura row"
            assert faktura_gebyr == Decimal(
                "300"
            ), f"fct.Faktura.GebyrBeloeb should be the header charge 300 once, got {faktura_gebyr}"
            assert faktura_rabat == Decimal(
                "100"
            ), f"fct.Faktura.RabatBeloeb should be the header discount 100 once, got {faktura_rabat}"

            # fct.Fakturalinje: per-line gebyr summed from the LINE allowances
            # (10 each → 30), NOT the header charge fanned out (300 × N_LINES).
            cursor.execute(
                """
                SELECT COUNT(*), SUM(GebyrBeloeb), SUM(RabatBeloeb)
                FROM [fct].[Fakturalinje] WHERE [FakturaNr] = ?
                """,
                (faktura_nr,),
            )
            linje_cnt, linje_gebyr, linje_rabat = cursor.fetchone()
            assert linje_cnt == N_LINES, f"expected {N_LINES} fct.Fakturalinje rows"
            assert linje_gebyr == Decimal(
                "30"
            ), f"SUM(line GebyrBeloeb) should be 10×{N_LINES}=30, got {linje_gebyr}"
            assert linje_rabat == Decimal(
                "0"
            ), f"SUM(line RabatBeloeb) should be 0 (no line discounts), got {linje_rabat}"

            # Explicit fan-out guard: the header charge is neither equal to the
            # line aggregate nor the header value multiplied by the line count.
            assert faktura_gebyr != linje_gebyr
            assert linje_gebyr != faktura_gebyr * N_LINES
        finally:
            cursor.close()
