# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S7 — Full-load re-run after source entity removal.

When a posting is deleted from the SydAx source and full load re-runs, map/dim
rows must survive (INSERT-only) and fct rows must not be deleted by MERGE.
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    assert_row_count,
    get_row_count,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    NAMESPACE_BRIDGE_BASE,
    NAMESPACE_LEDGERTRANS_BASE,
    make_invoice,
    make_posting,
    remove_sydax_posting,
    seed_mysupply,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

DAY = "2025-06-01"

FAKTURA_60 = "TEST-EXEC-S7-060"
FAKTURA_61 = "TEST-EXEC-S7-061"

CVR_60 = "99900060"
CVR_61 = "99900061"

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


class TestS7FullLoadOmitsEntity:
    def test_full_load_omits_entity(
        self, clean_dw, pipeline_runner, mysupply_conn, sydax_conn, target_conn
    ):
        invoices = [
            make_invoice(idx=60, cvr=CVR_60, faktura_nr=FAKTURA_60, dato=DAY),
            make_invoice(idx=61, cvr=CVR_61, faktura_nr=FAKTURA_61, dato=DAY),
        ]
        postings = [
            make_posting(idx=60, cvr=CVR_60, faktura_nr=FAKTURA_60, dato=DAY),
            make_posting(idx=61, cvr=CVR_61, faktura_nr=FAKTURA_61, dato=DAY),
        ]
        seed_mysupply(mysupply_conn, invoices)
        seed_sydax_postings(sydax_conn, postings)

        pipeline_runner.run_full_load()

        map_postering_count = get_row_count(target_conn, "map", "Postering")
        fct_postering_count = get_row_count(target_conn, "fct", "Postering")
        dim_postering_count = get_row_count(target_conn, "dim", "Postering")

        remove_sydax_posting(sydax_conn, NAMESPACE_BRIDGE_BASE + 60)

        pipeline_runner.run_full_load()

        assert_row_count(target_conn, "map", "Postering", map_postering_count)
        assert_row_count(target_conn, "dim", "Postering", dim_postering_count)
        assert_row_count(target_conn, "fct", "Postering", fct_postering_count)

        cursor = target_conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM [fct].[Postering] WHERE ref_LedgertransId = ?",
                (NAMESPACE_LEDGERTRANS_BASE + 60,),
            )
            assert (
                cursor.fetchone()[0] == 1
            ), "Posting for removed entity should survive full-load re-run"
        finally:
            cursor.close()
