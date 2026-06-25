# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S1 — Steady-state daily operation across three consecutive partitions.

Day 1 seeds entities A and B; day 2 re-seeds only A (updated date); day 3
re-seeds A plus new entity C. Validates no-null-overwrite for absent entity B,
stable DW_EK identities for A, and correct keying for C.
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    TEST_NAMESPACE_PREFIX,
    assert_full_coverage_pass,
    assert_key_stable,
    snapshot,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    NAMESPACE_KONTO_BASE,
    NAMESPACE_LEDGERTRANS_BASE,
    SydAxBudgetpost,
    SydAxFinanskladdelinje,
    make_invoice,
    make_posting,
    seed_mysupply,
    seed_sydax_budgetposts,
    seed_sydax_finanskladdelinjer,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

DAY1 = "2025-06-01"
DAY2 = "2025-06-02"
DAY3 = "2025-06-03"

# MySupply invoices are immutable documents keyed by header id — "entity A
# active on day 2" means a NEW invoice from the same kreditor (CVR), not the
# same FakturaNr re-delivered (a re-delivery gets a new header id and hence,
# by design since the 20260604 header-identity remodel, a new DW_EK_Faktura).
FAKTURA_A1 = "TEST-EXEC-A001"
FAKTURA_A2 = "TEST-EXEC-A002"
FAKTURA_A3 = "TEST-EXEC-A003"
FAKTURA_B = "TEST-EXEC-B001"
FAKTURA_C = "TEST-EXEC-C001"

CVR_A = "99900001"
CVR_B = "99900002"
CVR_C = "99900003"

NS_FILTER = f"[FakturaNr] LIKE '{TEST_NAMESPACE_PREFIX}%'"

EXPECT_UNRESOLVED = {
    # DW_EK_Postering / DW_EK_Kontoplan removed from fct.Faktura (finding 2.2).
    "fct.Faktura": {
        "DW_EK_EAN_Regningsmodtager",
    },
    # DW_EK_Vare is NOT excused: stg_dim_vare_mysupply/stg_dim_vare/dim_vare
    # are in the runner, so the seeded Item_FakturaUdstederVareNr must key.
    # DW_EK_Kontoplan removed from fct.Fakturalinje (finding 2.2).
    "fct.Fakturalinje": {
        "DW_EK_EAN_Regningsmodtager",
    },
    "fct.Postering": {
        "DW_EK_Faktura",
        "DW_EK_Okosted",
        "DW_EK_Finanskladdelinje",
    },
    # No Omkostningssted link is seeded — DW_EK_Okosted stays -1.
    "fct.Budget": {"DW_EK_Okosted"},
    "fct.Finanskladdelinje": {"DW_EK_Okosted"},
}


def _capture_dim_map_state(conn, faktura_nr: str) -> dict[str, dict[str, object]]:
    """Capture dim.Faktura and map.Faktura rows for a business FakturaNr."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT [DW_EK_Faktura], [FakturaNr], [LeverandorCVR], [FirmaNavn]
            FROM [dim].[Faktura]
            WHERE [FakturaNr] = ?
            """,
            (faktura_nr,),
        )
        dim_row = cursor.fetchone()
        assert dim_row is not None, f"dim.Faktura missing row for {faktura_nr}"
        dim_cols = [column[0] for column in cursor.description]
        dim = dict(zip(dim_cols, dim_row, strict=True))

        cursor.execute(
            """
            SELECT [DW_EK_Faktura], [CleanCode_Faktura],
                   [bk_FakturaMetadata], [bk_FakturaHeader]
            FROM [map].[Faktura]
            WHERE [DW_EK_Faktura] = ?
            """,
            (dim["DW_EK_Faktura"],),
        )
        map_row = cursor.fetchone()
        assert (
            map_row is not None
        ), f"map.Faktura missing row for DW_EK_Faktura={dim['DW_EK_Faktura']}"
        map_cols = [column[0] for column in cursor.description]
        map_data = dict(zip(map_cols, map_row, strict=True))

        return {"dim": dim, "map": map_data}
    finally:
        cursor.close()


def _assert_dim_map_unchanged(
    before: dict[str, dict[str, object]],
    after: dict[str, dict[str, object]],
    faktura_nr: str,
) -> None:
    """Assert day-1 dim/map attribute values were not null-overwritten on day 2."""
    skip_cols = {"DW_Timestamp_Insert", "DW_Timestamp_Update"}
    for layer in ("dim", "map"):
        for column, day1_value in before[layer].items():
            if column in skip_cols:
                continue
            day2_value = after[layer][column]
            assert day2_value == day1_value, (
                f"{layer}.{faktura_nr}.{column}: expected day-1 value "
                f"{day1_value!r}, got {day2_value!r}"
            )
            if day1_value is not None:
                assert day2_value is not None, (
                    f"{layer}.{faktura_nr}.{column}: day-1 had {day1_value!r}, "
                    "but day-2 null-overwrote it"
                )


def _assert_entity_keyed(conn, faktura_nr: str, cvr: str) -> None:
    """Assert a new entity has positive DW_EK keys and joins dim/map correctly."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT [DW_EK_Faktura], [FakturaNr], [DW_EK_Kreditor]
            FROM [fct].[Faktura]
            WHERE [FakturaNr] = ?
            """,
            (faktura_nr,),
        )
        fct_row = cursor.fetchone()
        assert fct_row is not None, f"fct.Faktura missing row for {faktura_nr}"
        dw_ek_faktura, _, dw_ek_kreditor = fct_row
        assert dw_ek_faktura > 0, f"{faktura_nr}: DW_EK_Faktura must be positive"
        assert dw_ek_kreditor > 0, f"{faktura_nr}: DW_EK_Kreditor must be positive"

        cursor.execute(
            """
            SELECT [LeverandorCVR]
            FROM [dim].[Faktura]
            WHERE [DW_EK_Faktura] = ?
            """,
            (dw_ek_faktura,),
        )
        dim_row = cursor.fetchone()
        assert dim_row is not None, f"dim.Faktura missing row for DW_EK={dw_ek_faktura}"
        assert (
            dim_row[0] == cvr
        ), f"{faktura_nr}: expected LeverandorCVR={cvr!r}, got {dim_row[0]!r}"

        cursor.execute(
            """
            SELECT [CleanCode_Faktura]
            FROM [map].[Faktura]
            WHERE [DW_EK_Faktura] = ?
            """,
            (dw_ek_faktura,),
        )
        map_row = cursor.fetchone()
        assert map_row is not None, f"map.Faktura missing row for DW_EK={dw_ek_faktura}"
        assert map_row[0], f"{faktura_nr}: CleanCode_Faktura must be populated"
    finally:
        cursor.close()


def _seed_day1(mysupply_conn, sydax_conn) -> None:
    invoices = [
        make_invoice(idx=1, cvr=CVR_A, faktura_nr=FAKTURA_A1, dato=DAY1),
        make_invoice(idx=2, cvr=CVR_B, faktura_nr=FAKTURA_B, dato=DAY1),
    ]
    postings = [
        make_posting(idx=1, cvr=CVR_A, faktura_nr=FAKTURA_A1, dato=DAY1),
        make_posting(idx=2, cvr=CVR_B, faktura_nr=FAKTURA_B, dato=DAY1),
    ]
    seed_mysupply(mysupply_conn, invoices)
    seed_sydax_postings(sydax_conn, postings)

    # Budget + Finanskladdelinje rows reuse posting idx=1's Finanskonto so
    # DW_EK_Kontoplan can resolve via map_kontoplan.
    seed_sydax_budgetposts(
        sydax_conn,
        [
            SydAxBudgetpost(
                ref_ledgerbudget_id=NAMESPACE_LEDGERTRANS_BASE + 801,
                budgetmodel_id=NAMESPACE_KONTO_BASE + 801,
                budgetpost_status_id=NAMESPACE_KONTO_BASE + 801,
                finanskonto_id=NAMESPACE_KONTO_BASE + 1,
                dato=DAY1,
            ),
        ],
    )
    seed_sydax_finanskladdelinjer(
        sydax_conn,
        [
            SydAxFinanskladdelinje(
                finanskladdelinje_id=NAMESPACE_LEDGERTRANS_BASE + 802,
                ref_ledgertrans_id=NAMESPACE_LEDGERTRANS_BASE + 802,
                finanskladde_id=NAMESPACE_KONTO_BASE + 802,
                ref_ledgerjournaltable_id=NAMESPACE_KONTO_BASE + 802,
                finanskonto_id=NAMESPACE_KONTO_BASE + 1,
                dato=DAY1,
            ),
        ],
    )


def _seed_day2(mysupply_conn, sydax_conn) -> None:
    # Kreditor A active again with a NEW invoice; B absent.
    invoices = [
        make_invoice(idx=3, cvr=CVR_A, faktura_nr=FAKTURA_A2, dato=DAY2),
    ]
    postings = [
        make_posting(idx=3, cvr=CVR_A, faktura_nr=FAKTURA_A2, dato=DAY2),
    ]
    seed_mysupply(mysupply_conn, invoices)
    seed_sydax_postings(sydax_conn, postings)


def _seed_day3(mysupply_conn, sydax_conn) -> None:
    invoices = [
        make_invoice(idx=5, cvr=CVR_A, faktura_nr=FAKTURA_A3, dato=DAY3),
        make_invoice(idx=6, cvr=CVR_C, faktura_nr=FAKTURA_C, dato=DAY3),
    ]
    postings = [
        make_posting(idx=5, cvr=CVR_A, faktura_nr=FAKTURA_A3, dato=DAY3),
        make_posting(idx=6, cvr=CVR_C, faktura_nr=FAKTURA_C, dato=DAY3),
    ]
    seed_mysupply(mysupply_conn, invoices)
    seed_sydax_postings(sydax_conn, postings)


class TestS1SteadyStateDaily:
    def test_steady_state_daily(
        self, clean_dw, pipeline_runner, mysupply_conn, sydax_conn, target_conn
    ):
        _seed_day1(mysupply_conn, sydax_conn)
        pipeline_runner.run_daily(DAY1)
        snap_day1 = snapshot(target_conn, namespace_filter_sql=NS_FILTER)
        b_day1_state = _capture_dim_map_state(target_conn, FAKTURA_B)

        _seed_day2(mysupply_conn, sydax_conn)
        pipeline_runner.run_daily(DAY2)
        snap_day2 = snapshot(target_conn, namespace_filter_sql=NS_FILTER)

        assert_key_stable(
            snap_day1,
            snap_day2,
            schema="fct",
            table="Faktura",
            business_key="FakturaNr",
        )
        b_day2_state = _capture_dim_map_state(target_conn, FAKTURA_B)
        _assert_dim_map_unchanged(b_day1_state, b_day2_state, FAKTURA_B)

        # Kreditor A's day-2 invoice must reuse the DW_EK_Kreditor minted on
        # day 1 — the map MERGE on CleanCode must not assign a new identity.
        faktura_rows = {r["FakturaNr"]: r for r in snap_day2.table_rows("fct", "Faktura")}
        assert (
            faktura_rows[FAKTURA_A2]["DW_EK_Kreditor"] == faktura_rows[FAKTURA_A1]["DW_EK_Kreditor"]
        ), ("Day-2 invoice from the same kreditor got a different " "DW_EK_Kreditor than day 1")

        _seed_day3(mysupply_conn, sydax_conn)
        pipeline_runner.run_daily(DAY3)
        snap_day3 = snapshot(target_conn, namespace_filter_sql=NS_FILTER)

        assert_key_stable(
            snap_day1,
            snap_day3,
            schema="fct",
            table="Faktura",
            business_key="FakturaNr",
        )
        assert_key_stable(
            snap_day2,
            snap_day3,
            schema="fct",
            table="Faktura",
            business_key="FakturaNr",
        )
        _assert_entity_keyed(target_conn, FAKTURA_C, CVR_C)

        # The Budget and Finanskladdelinje seeds must actually land —
        # otherwise their coverage assertions pass vacuously.
        cursor = target_conn.cursor()
        try:
            for table, ns_col in (
                ("Budget", "bk_ref_LedgerbudgetId"),
                ("Finanskladdelinje", "bk_ref_LedgertransId"),
            ):
                cursor.execute(
                    f"SELECT COUNT(*) FROM [fct].[{table}] "
                    f"WHERE TRY_CAST([{ns_col}] AS BIGINT) >= ?",
                    (NAMESPACE_LEDGERTRANS_BASE,),
                )
                count = cursor.fetchone()[0]
                assert count >= 1, (
                    f"fct.{table} has no test-namespace rows — the seeded "
                    f"source rows did not reach the fact table"
                )
        finally:
            cursor.close()

        assert_full_coverage_pass(
            target_conn,
            namespace_filter_sql=NS_FILTER,
            expect_unresolved=EXPECT_UNRESOLVED,
        )
