# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
S9 — Postering grain / dedup integrity.

Restores coverage lost when test_fct_postering_dedup.py was deleted: the
postering pipeline must collapse multiple source rows sharing one
``ref_LedgertransId`` to exactly one ``fct.Postering`` row. The staging
``grain_dedup`` CTE (PARTITION BY ref_LedgertransId ORDER BY DW_LoadTimestamp
DESC) keeps the latest, and FctUpd_postering enforces one row per
``ref_LedgertransId`` (its unique clustered index on #Postering_Facts plus the
explicit "Duplicate grain violation in fct_postering" guard).

This drives the dedup through the real pipeline from the SydAx source, so a
regression in either the staging dedup or the fact-grain guard surfaces here:
either ``fct.Postering`` ends up with >1 row (dedup broke) or the run raises
the grain-violation guard (dedup produced duplicates into the fact).
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    NAMESPACE_LEDGERTRANS_BASE,
    make_posting,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

DAY = "2025-06-01"
CVR = "99900095"
FAKTURA_NR = "TEST-EXEC-DEDUP001"
SHARED_REF_LEDGERTRANS_ID = NAMESPACE_LEDGERTRANS_BASE + 95


class TestS9PosteringDedup:
    def test_duplicate_ref_ledgertransid_dedups_to_one_fact_row(
        self, clean_dw, pipeline_runner, sydax_conn, target_conn
    ):
        # Two distinct source postings (different FinansposteringId /
        # Kreditortransaktion) that share one ref_LedgertransId — e.g. the same
        # ledger transaction re-extracted/restated.
        posting_a = make_posting(
            idx=95,
            cvr=CVR,
            faktura_nr=FAKTURA_NR,
            dato=DAY,
            ref_ledgertrans_id=SHARED_REF_LEDGERTRANS_ID,
        )
        posting_b = make_posting(
            idx=96,
            cvr=CVR,
            faktura_nr=FAKTURA_NR,
            dato=DAY,
            ref_ledgertrans_id=SHARED_REF_LEDGERTRANS_ID,
        )
        seed_sydax_postings(sydax_conn, [posting_a, posting_b])

        # If staging dedup fails, FctUpd_postering raises "Duplicate grain
        # violation in fct_postering" and run_daily fails here.
        pipeline_runner.run_daily(DAY)

        cursor = target_conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM [fct].[Postering] WHERE [ref_LedgertransId] = ?",
                (SHARED_REF_LEDGERTRANS_ID,),
            )
            assert cursor.fetchone()[0] == 1, (
                "Two source postings sharing one ref_LedgertransId must dedup to "
                "exactly one fct.Postering row"
            )
        finally:
            cursor.close()
