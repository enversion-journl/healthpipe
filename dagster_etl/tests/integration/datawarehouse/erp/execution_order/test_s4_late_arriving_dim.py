# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Late-arriving dimension: day-1 facts with -1 keys should heal on day 2 (known gap)."""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    assert_keys_resolved,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    NAMESPACE_KONTO_BASE,
    NAMESPACE_KREDITOR_BASE,
    make_posting,
    seed_sydax_finanskonto,
    seed_sydax_kreditor,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]


class TestS4LateArrivingDim:
    @pytest.mark.xfail(
        strict=True,
        reason=(
            "G1: late-arriving dimension gap — a day-2 run does not re-key day-1 "
            "facts, so DW_EK_Kreditor/Kontoplan remain -1 (ext.* holds only "
            "partition deltas and -1 FKs do not self-heal). When archive-layer "
            "re-keying lands, this test XPASSes and strict=True fails the build — "
            "remove this marker once the gap is closed."
        ),
    )
    def test_late_arriving_dim_healing(self, clean_dw, pipeline_runner, sydax_conn, target_conn):
        kreditor_id = NAMESPACE_KREDITOR_BASE + 99
        konto_id = NAMESPACE_KONTO_BASE + 99

        posting = make_posting(
            idx=30,
            cvr="99900099",
            faktura_nr="TEST-EXEC-LATE001",
            dato="2025-06-01",
            konto_id=konto_id,
            kreditor_id=kreditor_id,
        )
        seed_sydax_postings(sydax_conn, [posting])

        cursor = sydax_conn.cursor()
        try:
            cursor.execute(
                "DELETE FROM [Kreditor].[Kreditor] WHERE KreditorId = ?",
                (kreditor_id,),
            )
            cursor.execute(
                "DELETE FROM [Finans].[Finanskonto] WHERE FinanskontoId = ?",
                (konto_id,),
            )
            sydax_conn.commit()
        finally:
            cursor.close()

        pipeline_runner.run_daily("2025-06-01")

        result = assert_keys_resolved(
            target_conn,
            "fct",
            "Postering",
            namespace_filter_sql="namespace",
            expect_unresolved={
                "DW_EK_Kreditor",
                "DW_EK_Kontoplan",
                "DW_EK_Faktura",
                "DW_EK_Okosted",
                "DW_EK_Finanskladdelinje",
            },
        )
        assert result["DW_EK_Kreditor"] > 0, "Expected -1 for late Kreditor"
        assert result["DW_EK_Kontoplan"] > 0, "Expected -1 for late Kontoplan"

        seed_sydax_kreditor(sydax_conn, kreditor_id, "99900099", created_on="2025-06-02 08:00:00")
        seed_sydax_finanskonto(sydax_conn, konto_id, created_on="2025-06-02 08:00:00")

        pipeline_runner.run_daily("2025-06-02")

        # Desired behaviour: the day-2 run re-keys the day-1 facts now that the
        # late Kreditor/Kontoplan dims exist. Today it does not (G1), so
        # assert_keys_resolved raises because those columns are still -1 — that
        # raised AssertionError is the expected failure for the xfail(strict=True)
        # marker. When re-keying is implemented this passes and strict mode fails
        # the build, prompting removal of the marker.
        result2 = assert_keys_resolved(
            target_conn,
            "fct",
            "Postering",
            namespace_filter_sql="namespace",
            expect_unresolved={
                "DW_EK_Faktura",
                "DW_EK_Okosted",
                "DW_EK_Finanskladdelinje",
            },
        )

        assert result2["DW_EK_Kreditor"] == 0, "Expected Kreditor key to heal after day 2"
        assert result2["DW_EK_Kontoplan"] == 0, "Expected Kontoplan key to heal after day 2"
