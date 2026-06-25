# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S5 — Cross-source ordering within one daily run.

Seeds a MySupply invoice and a SydAx posting with the same FakturaNr in the
same partition, then verifies the invoice resolves its account via the newest
related posting (ref.Faktura_To_Postering / vw_fct.Faktura.DW_EK_Kontoplan_Postering)
after a single daily pipeline run. (fct.Faktura stores no account FK — finding 2.2.)
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    TEST_NAMESPACE_PREFIX,
    assert_full_coverage_pass,
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

DAY = "2025-06-01"
FAKTURA_NR = "TEST-EXEC-CROSS001"
CVR = "99900040"

NS_FILTER = f"[FakturaNr] LIKE '{TEST_NAMESPACE_PREFIX}%'"

# Tripwire migrated for finding 2.2: fct.Faktura no longer stores DW_EK_Kontoplan
# (no fact-to-fact FK). The invariant is preserved one hop out — a single daily
# run MUST link the invoice to its account-resolved newest posting via
# ref.Faktura_To_Postering, which is exactly what vw_fct.Faktura joins to expose
# DW_EK_Kontoplan_Postering. The assertion below resolves the account that way.
EXPECT_UNRESOLVED = {
    "fct.Faktura": {
        "DW_EK_EAN_Regningsmodtager",
    },
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


class TestS5CrossSourceOrdering:
    def test_cross_source_kontoplan_inheritance(
        self, clean_dw, pipeline_runner, mysupply_conn, sydax_conn, target_conn
    ):
        invoice = make_invoice(
            idx=40,
            cvr=CVR,
            faktura_nr=FAKTURA_NR,
            dato=DAY,
            n_lines=1,
        )
        posting = make_posting(
            idx=40,
            cvr=CVR,
            faktura_nr=FAKTURA_NR,
            dato=DAY,
        )
        seed_mysupply(mysupply_conn, [invoice])
        seed_sydax_postings(sydax_conn, [posting])

        pipeline_runner.run_daily(DAY)

        cursor = target_conn.cursor()
        try:
            cursor.execute(
                """
                SELECT [DW_EK_Kontoplan]
                FROM [fct].[Postering]
                WHERE [FakturaNr] = ?
                """,
                (FAKTURA_NR,),
            )
            posting_row = cursor.fetchone()
            assert (
                posting_row is not None
            ), f"No fct.Postering row found for FakturaNr={FAKTURA_NR!r}"
            assert (
                posting_row[0] != -1
            ), f"fct.Postering.DW_EK_Kontoplan should be resolved, got {posting_row[0]}"

            # Tripwire (finding 2.2): the account is no longer stored on
            # fct.Faktura. The equivalent invariant is that a single daily run
            # links the invoice to its account-resolved newest posting via
            # ref.Faktura_To_Postering — exactly the join vw_fct.Faktura uses to
            # expose DW_EK_Kontoplan_Postering. Resolve it here the same way.
            cursor.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN COALESCE(p.[DW_EK_Kontoplan], -1) = -1
                                THEN 1 ELSE 0 END) AS unresolved
                FROM [fct].[Faktura] f
                LEFT JOIN [ref].[Faktura_To_Postering] b
                  ON b.[DW_EK_Faktura] = f.[DW_EK_Faktura]
                 AND b.[is_newest_postering_for_faktura] = 1
                 AND b.[DW_IsDeleted] = 0
                LEFT JOIN [fct].[Postering] p
                  ON p.[DW_EK_Postering] = b.[DW_EK_Postering]
                WHERE f.[FakturaNr] LIKE 'TEST-EXEC-CROSS%'
                """
            )
            total, unresolved = cursor.fetchone()
            assert total and total > 0, "No faktura rows found for TEST-EXEC-CROSS"
            assert (unresolved or 0) == 0, (
                f"{unresolved}/{total} TEST-EXEC-CROSS invoices do not resolve an "
                "account via the newest related posting (ref.Faktura_To_Postering). "
                "Cross-source linkage failed within a single daily run — check that "
                "fct_faktura still depends on fct_postering and that "
                "fct_apply_faktura_postering_relations stamps the bridge."
            )
        finally:
            cursor.close()

        assert_full_coverage_pass(
            target_conn,
            namespace_filter_sql=NS_FILTER,
            expect_unresolved=EXPECT_UNRESOLVED,
        )
