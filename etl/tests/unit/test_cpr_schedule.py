# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The daily CPR schedule skips while an earlier cpr_ingest_job run is active."""

from unittest.mock import MagicMock

import dagster as dg
import pytest
from schedules.cpr_schedules import cpr_ingest_daily_schedule

pytestmark = pytest.mark.unit


@pytest.mark.parametrize("active", [True, False])
def test_schedule_skips_only_while_an_ingest_run_is_active(monkeypatch, active):
    records = MagicMock(return_value=[object()] if active else [])
    with dg.instance_for_test() as instance:
        monkeypatch.setattr(type(instance), "get_run_records", records)
        result = cpr_ingest_daily_schedule(dg.build_schedule_context(instance=instance))
    assert isinstance(result, dg.SkipReason if active else dg.RunRequest)
    run_filter = records.call_args.kwargs["filters"]
    assert run_filter.job_name == "cpr_ingest_job"
    assert dg.DagsterRunStatus.QUEUED in run_filter.statuses
    assert dg.DagsterRunStatus.STARTED in run_filter.statuses


def test_schedule_guard_filters_on_the_postfixed_job_name(monkeypatch):
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", "feature-cpr")
    records = MagicMock(return_value=[])
    with dg.instance_for_test() as instance:
        monkeypatch.setattr(type(instance), "get_run_records", records)
        cpr_ingest_daily_schedule(dg.build_schedule_context(instance=instance))
    assert records.call_args.kwargs["filters"].job_name == "cpr_ingest_job_feature_cpr"
