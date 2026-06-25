# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Full-load re-run must preserve DW_EK identities (map MERGE on CleanCode)."""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    assert_key_stable,
    assert_row_count,
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


class TestS3FullLoadRerun:
    def test_full_load_rerun_preserves_identity(
        self, clean_dw, pipeline_runner, mysupply_conn, sydax_conn, target_conn
    ):
        invoices = [
            make_invoice(
                idx=20,
                cvr="99900020",
                faktura_nr="TEST-EXEC-S3-020",
                dato="2025-06-01",
            ),
            make_invoice(
                idx=21,
                cvr="99900021",
                faktura_nr="TEST-EXEC-S3-021",
                dato="2025-06-15",
            ),
        ]
        postings = [
            make_posting(
                idx=20,
                cvr="99900020",
                faktura_nr="TEST-EXEC-S3-020",
                dato="2025-06-01",
            ),
            make_posting(
                idx=21,
                cvr="99900021",
                faktura_nr="TEST-EXEC-S3-021",
                dato="2025-06-15",
            ),
        ]

        seed_mysupply(mysupply_conn, invoices)
        seed_sydax_postings(sydax_conn, postings)

        pipeline_runner.run_full_load()

        snapshot_1 = snapshot(target_conn)
        map_faktura_count = get_row_count(target_conn, "map", "Faktura")
        map_postering_count = get_row_count(target_conn, "map", "Postering")
        map_kreditor_count = get_row_count(target_conn, "map", "Kreditor")
        map_kontoplan_count = get_row_count(target_conn, "map", "Kontoplan")
        fct_faktura_count = get_row_count(target_conn, "fct", "Faktura")
        fct_postering_count = get_row_count(target_conn, "fct", "Postering")

        pipeline_runner.run_full_load()

        snapshot_2 = snapshot(target_conn)

        assert_key_stable(snapshot_1, snapshot_2, "fct", "Faktura", "FakturaNr")
        assert_key_stable(snapshot_1, snapshot_2, "fct", "Postering", "ref_LedgertransId")

        assert_row_count(target_conn, "fct", "Faktura", fct_faktura_count)
        assert_row_count(target_conn, "fct", "Postering", fct_postering_count)

        assert_row_count(target_conn, "map", "Faktura", map_faktura_count)
        assert_row_count(target_conn, "map", "Postering", map_postering_count)
        assert_row_count(target_conn, "map", "Kreditor", map_kreditor_count)
        assert_row_count(target_conn, "map", "Kontoplan", map_kontoplan_count)
