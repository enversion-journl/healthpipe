# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Adversarial B-probes: materialize assets in deliberately wrong orders.

Each probe isolates a mechanism behind S-suite findings by running
``pipeline_runner.run_assets()`` with production asset keys out of sequence.
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.dw_ek_assert import (
    assert_keys_resolved,
)
from tests.integration.datawarehouse.erp.execution_order.runner import (
    DOWNSTREAM_KEYS,
    ENRICHED_DAILY_KEYS,
    EXTRACT_DAILY_DRIVER_KEYS,
)
from tests.integration.datawarehouse.erp.execution_order.seeds import (
    make_posting,
    seed_sydax_postings,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
]

PARTITION = "2025-06-01"

# Everything except the fact assets and the bridge/apply pass. Staging fact
# tables ("erp/stg_fct_*") must stay IN — only the top-level fct_* assets and
# ref_*/apply are excluded.
STAGING_AND_MAP_KEYS = [
    key
    for key in DOWNSTREAM_KEYS
    if not key.startswith(("fct_", "ref_")) and "apply" not in key.lower()
]

STAGING_ONLY_KEYS = [key for key in DOWNSTREAM_KEYS if key.startswith("erp/")]

POSTERING_UNRESOLVED_FKS = {
    "DW_EK_Postering",
    "DW_EK_Kreditor",
    "DW_EK_Kontoplan",
    "DW_EK_Faktura",
    "DW_EK_Okosted",
    "DW_EK_Finanskladdelinje",
}


def _run_extracts_and_enriched(pipeline_runner) -> None:
    pipeline_runner.run_assets(EXTRACT_DAILY_DRIVER_KEYS, partition=PARTITION)
    pipeline_runner.run_assets(ENRICHED_DAILY_KEYS, partition=PARTITION)


def _run_postering_before_map(
    pipeline_runner,
    sydax_conn,
    *,
    idx: int,
    cvr: str,
    faktura_nr: str,
) -> None:
    """Seed one posting, extract + stage, then materialize fct_postering without maps."""
    posting = make_posting(
        idx=idx,
        cvr=cvr,
        faktura_nr=faktura_nr,
        dato=PARTITION,
    )
    seed_sydax_postings(sydax_conn, [posting])

    _run_extracts_and_enriched(pipeline_runner)
    pipeline_runner.run_assets(STAGING_ONLY_KEYS)
    pipeline_runner.run_assets(["fct_postering"])


# TestP1FakturaBeforePostering removed (finding 2.2): fct.Faktura no longer stores
# DW_EK_Kontoplan, so the "fct_faktura before fct_postering -> Kontoplan = -1"
# mechanism is moot — there is no inherited account column to be -1. The account
# is resolved at query time via vw_fct.Faktura / ref.Faktura_To_Postering; the
# single-daily-run resolution invariant now lives in test_s5.


class TestP2FactBeforeMap:
    """Baseline: materialize fact before map → all map-derived FKs = -1."""

    def test_postering_before_map(self, clean_dw, pipeline_runner, sydax_conn, target_conn):
        _run_postering_before_map(
            pipeline_runner,
            sydax_conn,
            idx=72,
            cvr="99900072",
            faktura_nr="TEST-EXEC-P2",
        )

        result = assert_keys_resolved(
            target_conn,
            "fct",
            "Postering",
            namespace_filter_sql="namespace",
            expect_unresolved=POSTERING_UNRESOLVED_FKS,
        )
        assert result["DW_EK_Postering"] > 0, "Should be -1 without map"
        assert result["DW_EK_Kreditor"] > 0, "Should be -1 without map"
        assert result["DW_EK_Kontoplan"] > 0, "Should be -1 without map"


class TestP3MapAfterFact:
    """S4 mechanism: -1 stays -1 even when the map arrives later."""

    def test_map_after_fact_no_healing(self, clean_dw, pipeline_runner, sydax_conn, target_conn):
        _run_postering_before_map(
            pipeline_runner,
            sydax_conn,
            idx=73,
            cvr="99900073",
            faktura_nr="TEST-EXEC-P3",
        )

        # map_okosted consumes stg_okosted as a managed input, so the maps
        # cannot be materialized without their staging upstreams. Staging +
        # maps preserves the probe's point: the fact is NOT re-run.
        pipeline_runner.run_assets(STAGING_AND_MAP_KEYS)

        result = assert_keys_resolved(
            target_conn,
            "fct",
            "Postering",
            namespace_filter_sql="namespace",
            expect_unresolved=POSTERING_UNRESOLVED_FKS,
        )
        assert result["DW_EK_Postering"] > 0, "-1 should persist: map arrived but fact not re-run"
