# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S6 — Monthly vs daily partition equivalence.

Loads the same source rows via three consecutive daily runs and one monthly run,
then compares business keys and FK resolution equivalence (not raw IDENTITY values).
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    TEST_NAMESPACE_PREFIX,
    assert_full_coverage_pass,
    snapshot,
    truncate_all_dw_tables,
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

DAY1 = "2025-06-01"
DAY2 = "2025-06-02"
DAY3 = "2025-06-03"
MONTH = "2025-06"

CVR_50 = "99900050"
CVR_51 = "99900051"
CVR_52 = "99900052"

FAKTURA_50 = "TEST-EXEC-EQ050"
FAKTURA_51 = "TEST-EXEC-EQ051"
FAKTURA_52 = "TEST-EXEC-EQ052"

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

# DW_EK_Kontoplan removed from fct.Faktura (finding 2.2) — the account link is no
# longer stored on the invoice fact, so equivalence is checked on DW_EK_Kreditor.
FAKTURA_FK_COLS = ("DW_EK_Kreditor",)
POSTERING_FK_COLS = ("DW_EK_Kreditor", "DW_EK_Kontoplan", "DW_EK_Faktura")


def _assert_fk_resolution_equivalent(
    daily_rows: list[dict],
    monthly_rows: list[dict],
    business_key: str,
    fk_columns: tuple[str, ...],
    table_label: str,
) -> None:
    """Assert each business key has the same resolved/unresolved FK pattern in both runs."""
    daily_idx = {row[business_key]: row for row in daily_rows}
    monthly_idx = {row[business_key]: row for row in monthly_rows}

    for bk_val, daily_row in daily_idx.items():
        monthly_row = monthly_idx[bk_val]
        for col in fk_columns:
            daily_resolved = daily_row[col] != -1
            monthly_resolved = monthly_row[col] != -1
            assert daily_resolved == monthly_resolved, (
                f"{table_label} {business_key}={bk_val!r}: {col} resolution mismatch "
                f"(daily={daily_row[col]}, monthly={monthly_row[col]})"
            )


class TestS6MonthlyDailyEquivalence:
    def test_monthly_daily_equivalence(
        self,
        clean_dw,
        pipeline_runner,
        mysupply_conn,
        sydax_conn,
        target_conn,
        real_mssql_target_resource,
    ):
        invoices = [
            make_invoice(idx=50, cvr=CVR_50, faktura_nr=FAKTURA_50, dato=DAY1),
            make_invoice(idx=51, cvr=CVR_51, faktura_nr=FAKTURA_51, dato=DAY2),
            make_invoice(idx=52, cvr=CVR_52, faktura_nr=FAKTURA_52, dato=DAY3),
        ]
        postings = [
            make_posting(idx=50, cvr=CVR_50, faktura_nr=FAKTURA_50, dato=DAY1),
            make_posting(idx=51, cvr=CVR_51, faktura_nr=FAKTURA_51, dato=DAY2),
            make_posting(idx=52, cvr=CVR_52, faktura_nr=FAKTURA_52, dato=DAY3),
        ]
        seed_mysupply(mysupply_conn, invoices)
        seed_sydax_postings(sydax_conn, postings)

        pipeline_runner.run_daily(DAY1)
        pipeline_runner.run_daily(DAY2)
        pipeline_runner.run_daily(DAY3)

        snapshot_daily = snapshot(target_conn, namespace_filter_sql=NS_FILTER)

        truncate_all_dw_tables(target_conn)

        pipeline_runner.run_monthly(MONTH)

        snapshot_monthly = snapshot(target_conn, namespace_filter_sql=NS_FILTER)

        daily_faktura = snapshot_daily.table_rows("fct", "Faktura")
        monthly_faktura = snapshot_monthly.table_rows("fct", "Faktura")
        assert len(daily_faktura) == len(monthly_faktura), (
            f"Faktura row count mismatch: daily={len(daily_faktura)}, "
            f"monthly={len(monthly_faktura)}"
        )

        daily_nrs = {row["FakturaNr"] for row in daily_faktura}
        monthly_nrs = {row["FakturaNr"] for row in monthly_faktura}
        assert daily_nrs == monthly_nrs, f"FakturaNr set mismatch: {daily_nrs ^ monthly_nrs}"

        _assert_fk_resolution_equivalent(
            daily_faktura,
            monthly_faktura,
            business_key="FakturaNr",
            fk_columns=FAKTURA_FK_COLS,
            table_label="fct.Faktura",
        )

        daily_postering = snapshot_daily.table_rows("fct", "Postering")
        monthly_postering = snapshot_monthly.table_rows("fct", "Postering")
        assert len(daily_postering) == len(monthly_postering), (
            f"Postering row count mismatch: daily={len(daily_postering)}, "
            f"monthly={len(monthly_postering)}"
        )

        daily_ids = {row["ref_LedgertransId"] for row in daily_postering}
        monthly_ids = {row["ref_LedgertransId"] for row in monthly_postering}
        assert (
            daily_ids == monthly_ids
        ), f"ref_LedgertransId set mismatch: {daily_ids ^ monthly_ids}"

        _assert_fk_resolution_equivalent(
            daily_postering,
            monthly_postering,
            business_key="ref_LedgertransId",
            fk_columns=POSTERING_FK_COLS,
            table_label="fct.Postering",
        )

        assert_full_coverage_pass(
            target_conn,
            namespace_filter_sql=NS_FILTER,
            expect_unresolved=EXPECT_UNRESOLVED,
        )
