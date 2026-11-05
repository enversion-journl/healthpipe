# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The sensor that starts the full load after the morning chain.

Bugs these tests catch:
- a second full load starts while one is still running, so two runs take the same window
- a dead run left in STARTED blocks the sensor every morning
- the request lacks koer_indtil, so the full load runs into the next morning's chain
- a re-evaluated tick for the same morning run starts the full load twice
- the sensor switches on by itself after a merge
"""

from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import dagster as dg
import pytest
from DSA.SQL.dfm.ifdb_fhir_fuldload import ifdb_fhir_fuldload_job
from jobs.ifdb_dfm_jobs import ifdb_dfm_daily_job
from sensors import ifdb_fhir_fuldload_sensor as module

pytestmark = pytest.mark.unit


def _context(run_id="daily-1", active_run_id=None):
    context = MagicMock()
    context.dagster_run.run_id = run_id
    records = [SimpleNamespace(dagster_run=SimpleNamespace(run_id=active_run_id))]
    context.instance.get_run_records.return_value = records if active_run_id else []
    return context


def test_an_active_full_load_gives_skip():
    context = _context(active_run_id="fuldload-7")
    result = module.evaluate(context, fuldload_job_name="ifdb_fhir_fuldload_job_feature_x")

    assert isinstance(result, dg.SkipReason) and "fuldload-7" in result.skip_message
    run_filter = context.instance.get_run_records.call_args.kwargs["filters"]
    assert run_filter.job_name == "ifdb_fhir_fuldload_job_feature_x"
    assert dg.DagsterRunStatus.STARTED in run_filter.statuses
    assert dg.DagsterRunStatus.QUEUED in run_filter.statuses


def test_a_run_older_than_a_day_does_not_count_as_active():
    context = _context()
    module.evaluate(context, fuldload_job_name="ifdb_fhir_fuldload_job")

    created_after = context.instance.get_run_records.call_args.kwargs["filters"].created_after
    assert created_after is not None
    assert created_after > datetime.now(UTC) - timedelta(hours=25)


def test_a_successful_morning_chain_starts_one_full_load_with_the_time_budget():
    result = module.evaluate(_context("daily-42"), fuldload_job_name="ifdb_fhir_fuldload_job")

    assert isinstance(result, dg.RunRequest)
    assert result.job_name == "ifdb_fhir_fuldload_job"
    assert result.run_key == "ifdb_fhir_fuldload:daily-42"
    config = result.run_config["ops"]["ifdb_fhir_fuldload"]["config"]
    assert config["koer_indtil"] == "05:30"
    assert config["max_vinduer"] > 1


def test_the_sensor_is_stopped_by_default_and_watches_the_daily_job():
    sensor = module.build_ifdb_fhir_fuldload_sensor(ifdb_dfm_daily_job, ifdb_fhir_fuldload_job)

    assert sensor.default_status == dg.DefaultSensorStatus.STOPPED
    assert sensor.job_name == "ifdb_fhir_fuldload_job"
