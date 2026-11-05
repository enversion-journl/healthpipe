# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Registered LPR3 graph and overlap-safe schedule."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from core.resources import FileShareResource, MSSQLResource
from DSI.csv_files.lpr3 import lpr3_extract_assets
from DSI.csv_files.lpr3_archive import lpr3_archive_assets
from DSI.csv_files.lpr3_datasets import ARCHIVE_DATASETS
from jobs.lpr3_jobs import lpr3_ingest_job
from schedules.lpr3_schedules import lpr3_ingest_daily_schedule

pytestmark = pytest.mark.unit


def test_job_contains_all_archives_and_independent_freshness():
    defs = dg.Definitions(
        assets=[*lpr3_extract_assets, *lpr3_archive_assets],
        jobs=[lpr3_ingest_job],
        resources={
            "mssql_target_staging": MSSQLResource(
                host="unused", port=1433, database="unused", user="unused", password="unused"
            ),
            "lpr3_share": FileShareResource(base_dir="unused"),
        },
    )
    dg.Definitions.validate_loadable(defs)
    graph = defs.resolve_asset_graph()
    assert {
        key for key in graph.get_all_asset_keys() if key.to_user_string().startswith("arc_lpr3_")
    } == {dg.AssetKey(f"arc_{d.arc_table}") for d in ARCHIVE_DATASETS}
    assert not graph.get(dg.AssetKey("lpr3_leverance_alder")).parent_keys
    previous = dg.AssetKey("lpr3_archive_noegler")
    for dataset in ARCHIVE_DATASETS:
        key = dg.AssetKey(f"arc_{dataset.arc_table}")
        assert graph.get(key).parent_keys == {previous}
        previous = key
    assert graph.get(dg.AssetKey("lpr3_leverance_checkpoint")).parent_keys == {previous}


@pytest.mark.parametrize("active", [True, False])
def test_schedule_skips_only_while_an_lpr3_run_is_active(active, monkeypatch):
    monkeypatch.delenv("DAGSTER_BRANCH_POSTFIX", raising=False)
    records = MagicMock(return_value=[object()] if active else [])
    with dg.instance_for_test() as instance:
        monkeypatch.setattr(type(instance), "get_run_records", records)
        result = lpr3_ingest_daily_schedule(dg.build_schedule_context(instance=instance))
    assert isinstance(result, dg.SkipReason if active else dg.RunRequest)
    run_filter = records.call_args.kwargs["filters"]
    assert run_filter.job_name == "lpr3_ingest_job"
    assert dg.DagsterRunStatus.STARTED in run_filter.statuses
    assert dg.DagsterRunStatus.CANCELING in run_filter.statuses


def test_schedule_guard_filters_on_the_postfixed_job_name(monkeypatch):
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-lpr3")
    records = MagicMock(return_value=[])
    with dg.instance_for_test() as instance:
        monkeypatch.setattr(type(instance), "get_run_records", records)
        lpr3_ingest_daily_schedule(dg.build_schedule_context(instance=instance))
    assert records.call_args.kwargs["filters"].job_name == "lpr3_ingest_job_feature_lpr3"
