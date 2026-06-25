# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Drift guard: the execution-order suite's PipelineRunner must stay in sync
with the production ERP job selections in ``jobs/erp_jobs.py``.

The runner's asset key lists are hand-maintained (dg.materialize cannot mix
partitioned and unpartitioned assets in one call, so the job selections
cannot be used verbatim). This test pins the contract in both directions:

1. Every key the runner materialises is part of the corresponding
   production job selection — the suite never tests an ordering production
   does not run.
2. Every MySupply/SydAx-fed asset in the jobs' staging/map/dim/fact groups
   is covered by the runner — a new asset added to the jobs fails this test
   until it is either added to the runner or consciously excluded below.

Pure definitions test — no database required.
"""

from __future__ import annotations

import pytest
from tests.integration.datawarehouse.erp.execution_order.runner import (
    DOWNSTREAM_KEYS,
    ENRICHED_DAILY_KEYS,
    ENRICHED_FULL_LOAD_KEYS,
    ENRICHED_MONTHLY_KEYS,
    EXTRACT_DAILY_DRIVER_KEYS,
    EXTRACT_FULL_LOAD_KEYS,
    EXTRACT_MONTHLY_DRIVER_KEYS,
)

# Assets in the jobs' erp_staging / erp_maps / erp_dimensions / erp_facts
# groups that the execution-order suite deliberately does NOT cover.
# Everything here is fed by the ILS source (or the ILS-only lager /
# vareudlevering / rekvirent / aftale star), which is outside the
# MySupply + SydAx scope of the suite.
KNOWN_EXCLUSIONS: frozenset[str] = frozenset(
    {
        # ILS pre-staging / staging
        "erp/pre_stg_bridge_vare_ils",
        "erp/pre_stg_vareaftaler_ils",
        "erp/pre_stg_vareudlevering_ils",
        "erp/stg_dim_aftale_ils",
        "erp/stg_dim_kreditor_ils",
        "erp/stg_dim_vare_ils",
        "erp/stg_dim_vareudlevering_ils",
        "erp/stg_fct_vareudlevering_ils",
        "erp/stg_map_ilsvare_ils",
        "stg_dim_lager",
        "stg_dim_rekvirent",
        # ILS maps / dims / facts
        "map_aftale",
        "map_ilsvare",
        "map_lager",
        "map_rekvirent",
        "map_vareudlevering",
        "dim_aftale",
        "dim_ilsvare",
        "dim_lager",
        "dim_rekvirent",
        "dim_vareudlevering_egenskaber",
        "fct_vareudlevering",
    }
)

# Assets the runner materialises that production runs in a DIFFERENT job:
# stg_okosted / map_okosted belong to the organisation job
# (jobs/organization_jobs.py), not the ERP jobs. The runner includes them to
# simulate "the organisation job has run" — fct.Postering / fct.Budget /
# fct.Finanskladdelinje resolve DW_EK_Okosted against map.Okosted, which the
# organisation job (not the ERP jobs) populates, so an ERP run before it leaves
# DW_EK_Okosted = -1 until the fact's own rows re-arrive.
CROSS_JOB_DEPENDENCIES: frozenset[str] = frozenset({"stg_okosted", "map_okosted"})

COVERED_GROUPS = ("erp_staging", "erp_maps", "erp_dimensions", "erp_facts")

RUNNER_MODE_KEYS = {
    "erp_daily_etl": EXTRACT_DAILY_DRIVER_KEYS + ENRICHED_DAILY_KEYS,
    "erp_monthly_etl": EXTRACT_MONTHLY_DRIVER_KEYS + ENRICHED_MONTHLY_KEYS,
    "erp_full_load_etl": EXTRACT_FULL_LOAD_KEYS + ENRICHED_FULL_LOAD_KEYS,
}


@pytest.fixture(scope="module")
def job_selections():
    """Resolve {job_name: (selected user-string keys, asset graph)}."""
    from definitions import defs

    out = {}
    for job_name in RUNNER_MODE_KEYS:
        job = defs.get_job_def(job_name)
        keys = {k.to_user_string() for k in job.asset_layer.selected_asset_keys}
        out[job_name] = (keys, job.asset_layer.asset_graph)
    return out


@pytest.mark.parametrize("job_name", sorted(RUNNER_MODE_KEYS))
def test_runner_extract_keys_are_in_job_selection(job_selections, job_name):
    """Direction 1 (extract layer): runner keys ⊆ job selection."""
    job_keys, _ = job_selections[job_name]
    missing = [k for k in RUNNER_MODE_KEYS[job_name] if k not in job_keys]
    assert not missing, (
        f"Runner materialises assets that {job_name} does not select: "
        f"{missing}. Update runner.py or the job selection."
    )


@pytest.mark.parametrize("job_name", sorted(RUNNER_MODE_KEYS))
def test_runner_downstream_keys_are_in_job_selection(job_selections, job_name):
    """Direction 1 (downstream layers): runner keys ⊆ every job selection."""
    job_keys, _ = job_selections[job_name]
    missing = [k for k in DOWNSTREAM_KEYS if k not in job_keys and k not in CROSS_JOB_DEPENDENCIES]
    assert not missing, (
        f"Runner downstream assets not selected by {job_name}: {missing}. "
        f"Update runner.py or the job selection."
    )


def test_job_group_assets_are_covered_by_runner(job_selections):
    """Direction 2: every staging/map/dim/fact asset in the daily job is
    either covered by the runner or consciously excluded."""
    job_keys, asset_graph = job_selections["erp_daily_etl"]

    downstream = set(DOWNSTREAM_KEYS)
    uncovered = []
    for key_str in sorted(job_keys):
        node = asset_graph.get(_to_asset_key(key_str))
        if node.group_name not in COVERED_GROUPS:
            continue
        if key_str in downstream or key_str in KNOWN_EXCLUSIONS:
            continue
        uncovered.append(f"{node.group_name}: {key_str}")

    assert not uncovered, (
        "Assets in the production job's staging/map/dim/fact groups are "
        "neither covered by the execution-order runner nor listed in "
        "KNOWN_EXCLUSIONS:\n  " + "\n  ".join(uncovered)
    )


def test_exclusions_do_not_overlap_runner():
    """Hygiene: an asset cannot be both covered and excluded."""
    overlap = KNOWN_EXCLUSIONS & set(DOWNSTREAM_KEYS)
    assert not overlap, f"KNOWN_EXCLUSIONS overlaps DOWNSTREAM_KEYS: {overlap}"


def _to_asset_key(key_str: str):
    import dagster as dg

    return dg.AssetKey.from_user_string(key_str)
