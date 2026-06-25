# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S2 — Go-live sequence: full-load of historical data followed by daily increments.

Seeds invoices across two months, runs full load, then adds a daily increment
and verifies historical facts are untouched while the DW row count grows.
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    TEST_NAMESPACE_PREFIX,
    assert_full_coverage_pass,
    assert_key_stable,
    get_row_count,
    snapshot,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    make_invoice,
    make_posting,
    seed_mysupply,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

HIST_DAY1 = "2025-03-15"
HIST_DAY2 = "2025-04-20"
DAILY_DAY = "2025-06-01"

FAKTURA_HIST1 = "TEST-EXEC-H010"
FAKTURA_HIST2 = "TEST-EXEC-H011"
FAKTURA_DAILY = "TEST-EXEC-D012"

CVR_HIST1 = "99900010"
CVR_HIST2 = "99900011"
CVR_DAILY = "99900012"

NS_FILTER = f"[FakturaNr] LIKE '{TEST_NAMESPACE_PREFIX}%'"

EXPECT_UNRESOLVED = {
    # DW_EK_Postering / DW_EK_Kontoplan removed from fct.Faktura (finding 2.2).
    "fct.Faktura": {
        "DW_EK_EAN_Regningsmodtager",
    },
    # DW_EK_Kontoplan removed from fct.Fakturalinje (finding 2.2).
    "fct.Fakturalinje": {
        "DW_EK_EAN_Regningsmodtager",
        "DW_EK_Vare",
    },
    "fct.Postering": {
        "DW_EK_Faktura",
        "DW_EK_Okosted",
        "DW_EK_Finanskladdelinje",
    },
}


def _seed_historical(mysupply_conn, sydax_conn) -> None:
    invoices = [
        make_invoice(idx=10, cvr=CVR_HIST1, faktura_nr=FAKTURA_HIST1, dato=HIST_DAY1),
        make_invoice(idx=11, cvr=CVR_HIST2, faktura_nr=FAKTURA_HIST2, dato=HIST_DAY2),
    ]
    postings = [
        make_posting(idx=10, cvr=CVR_HIST1, faktura_nr=FAKTURA_HIST1, dato=HIST_DAY1),
        make_posting(idx=11, cvr=CVR_HIST2, faktura_nr=FAKTURA_HIST2, dato=HIST_DAY2),
    ]
    seed_mysupply(mysupply_conn, invoices)
    seed_sydax_postings(sydax_conn, postings)


def _seed_daily_increment(mysupply_conn, sydax_conn) -> None:
    invoices = [
        make_invoice(idx=12, cvr=CVR_DAILY, faktura_nr=FAKTURA_DAILY, dato=DAILY_DAY),
    ]
    postings = [
        make_posting(idx=12, cvr=CVR_DAILY, faktura_nr=FAKTURA_DAILY, dato=DAILY_DAY),
    ]
    seed_mysupply(mysupply_conn, invoices)
    seed_sydax_postings(sydax_conn, postings)


class TestS2FullLoadThenDaily:
    def test_full_load_then_daily(
        self, clean_dw, pipeline_runner, mysupply_conn, sydax_conn, target_conn
    ):
        _seed_historical(mysupply_conn, sydax_conn)
        pipeline_runner.run_full_load()
        snap_after_full = snapshot(target_conn, namespace_filter_sql=NS_FILTER)
        count_after_full = get_row_count(target_conn, "fct", "Faktura")

        _seed_daily_increment(mysupply_conn, sydax_conn)
        pipeline_runner.run_daily(DAILY_DAY)
        snap_after_daily = snapshot(target_conn, namespace_filter_sql=NS_FILTER)
        count_after_daily = get_row_count(target_conn, "fct", "Faktura")

        assert_key_stable(
            snap_after_full,
            snap_after_daily,
            schema="fct",
            table="Faktura",
            business_key="FakturaNr",
        )
        assert count_after_daily > count_after_full, (
            f"fct.Faktura row count should increase after daily increment: "
            f"full_load={count_after_full}, daily={count_after_daily}"
        )

        assert_full_coverage_pass(
            target_conn,
            namespace_filter_sql=NS_FILTER,
            expect_unresolved=EXPECT_UNRESOLVED,
        )
