# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for DQA automation: the post-ETL sensors (AND-join + daily/weekly
split), the staggered Saturday DSO crons, and the auto-enable status helper.

Pure/mock -- no DB, no Dagster run.
"""

from datetime import datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from dqa import dso_jobs
from dqa.jobs import dqa_jobs
from dqa.schedules import build_dso_weekly_schedules
from dqa.sensors import (
    _TZ,
    DQA_AFTER_ETL_SENSOR_SPECS,
    _evaluate,
    build_dqa_after_etl_sensor,
)

pytestmark = pytest.mark.unit

# 2022-01-01 was a Saturday; 2022-01-04 a Tuesday.
_SAT = datetime(2022, 1, 1, 8, 0, tzinfo=_TZ).timestamp()
_TUE = datetime(2022, 1, 4, 8, 0, tzinfo=_TZ).timestamp()

_REAL_DQA_JOBS = {j.name for j in dqa_jobs} | {j.name for j in dso_jobs}


def _ctx(start_epoch, succeeded_jobs):
    """Mock RunStatusSensorContext whose monitored run started at start_epoch and
    where every job in ``succeeded_jobs`` has a same-day SUCCESS run."""
    inst = MagicMock()
    inst.get_run_record_by_id.return_value = SimpleNamespace(start_time=start_epoch)

    def _records(filters=None, limit=50):
        return (
            [SimpleNamespace(start_time=start_epoch)] if filters.job_name in succeeded_jobs else []
        )

    inst.get_run_records.side_effect = _records
    return SimpleNamespace(
        dagster_run=SimpleNamespace(run_id="run-1"), instance=inst, log=MagicMock()
    )


# --- spec integrity --------------------------------------------------------------


def test_specs_reference_real_dqa_jobs_and_no_legacy_etl():
    monitored_all = []
    for spec in DQA_AFTER_ETL_SENSOR_SPECS:
        monitored_all += spec["monitored"]
        requested = ([spec["daily_job"]] if spec["daily_job"] else []) + spec["weekly_jobs"]
        for jn in requested:
            assert jn in _REAL_DQA_JOBS, f"{jn} is not a real DQA/DSO job"
    # No legacy ETL jobs are monitored anymore.
    assert "erp_daily_etl" not in monitored_all
    assert "fhir_sof_daily_job" not in monitored_all


def test_fhir_spec_and_joins_both_downstream_jobs_incl_dso_fhir():
    fhir = next(s for s in DQA_AFTER_ETL_SENSOR_SPECS if s["name"] == "dqa_fhir_after_etl")
    assert set(fhir["monitored"]) == {"fhir_ifdb_dfm_sof_dso_job", "fhir_etl_ifdb_daily_job"}
    assert fhir["daily_job"] == "dqa_fhir_daily_job"
    assert set(fhir["weekly_jobs"]) == {"dqa_fhir_weekly_job", "dqa_dso_fhir_job"}


def test_erp_spec_uses_dfm_job():
    erp = next(s for s in DQA_AFTER_ETL_SENSOR_SPECS if s["name"] == "dqa_erp_daily_after_dfm")
    assert erp["monitored"] == ["erp_daily_dfm_job"]
    assert erp["daily_job"] == "dqa_dwh_daily_job"
    assert erp["weekly_jobs"] == ["dqa_dwh_weekly_job"]


# --- _evaluate: AND-join + daily/weekly gating -----------------------------------

_FHIR_ARGS = dict(
    name="dqa_fhir_after_etl",
    monitored_names=["fhir_ifdb_dfm_sof_dso_job", "fhir_etl_ifdb_daily_job"],
    daily_name="dqa_fhir_daily_job",
    weekly_names=["dqa_fhir_weekly_job", "dqa_dso_fhir_job"],
)


def test_evaluate_skips_until_both_monitored_jobs_succeed():
    # Only one of the two downstream jobs has succeeded today -> wait.
    ctx = _ctx(_SAT, {"fhir_ifdb_dfm_sof_dso_job"})
    result = _evaluate(ctx, **_FHIR_ARGS)
    assert isinstance(result, dg.SkipReason)


def test_evaluate_saturday_requests_daily_and_weekly():
    ctx = _ctx(_SAT, {"fhir_ifdb_dfm_sof_dso_job", "fhir_etl_ifdb_daily_job"})
    result = _evaluate(ctx, **_FHIR_ARGS)
    assert isinstance(result, list)
    jobs = {r.job_name for r in result}
    assert jobs == {"dqa_fhir_daily_job", "dqa_fhir_weekly_job", "dqa_dso_fhir_job"}
    # run_keys are day-scoped so the two success events collapse to one run each.
    assert all("2022-01-01" in r.run_key for r in result)


def test_evaluate_weekday_requests_daily_only():
    ctx = _ctx(_TUE, {"fhir_ifdb_dfm_sof_dso_job", "fhir_etl_ifdb_daily_job"})
    result = _evaluate(ctx, **_FHIR_ARGS)
    assert isinstance(result, list)
    assert {r.job_name for r in result} == {"dqa_fhir_daily_job"}


def test_evaluate_arc_style_weekly_only_skips_on_weekday():
    # daily_name=None (arc has no daily); Saturday -> request weekly, weekday -> skip.
    arc = dict(
        name="dqa_arc_ifdb_after_archive",
        monitored_names=["fhir_ifdb_dfm_base_job"],
        daily_name=None,
        weekly_names=["dqa_arc_ifdb_weekly_job"],
    )
    sat = _evaluate(_ctx(_SAT, {"fhir_ifdb_dfm_base_job"}), **arc)
    assert isinstance(sat, list) and {r.job_name for r in sat} == {"dqa_arc_ifdb_weekly_job"}

    tue = _evaluate(_ctx(_TUE, {"fhir_ifdb_dfm_base_job"}), **arc)
    assert isinstance(tue, dg.SkipReason)


def test_build_sensor_constructs_with_real_jobs():
    # Smoke: the builder wires a SensorDefinition (distinct monitored vs requested).
    sensor = build_dqa_after_etl_sensor(
        name="t_sensor",
        monitored_jobs=[next(j for j in dqa_jobs if j.name == "dqa_dwh_daily_job")],
        daily_job=next(j for j in dqa_jobs if j.name == "dqa_fhir_daily_job"),
        weekly_jobs=[next(j for j in dqa_jobs if j.name == "dqa_fhir_weekly_job")],
    )
    assert sensor.name == "t_sensor"


# --- DSO weekly staggered crons --------------------------------------------------


def test_dso_weekly_schedules_stagger_and_exclude_fhir():
    scheds = build_dso_weekly_schedules(dso_jobs)
    names = {s.name for s in scheds}
    assert names == {
        f"dqa_dso_{d}_job_schedule"
        for d in ("erp", "shared", "security", "codesystem", "conceptmap")
    }
    # fhir is sensor-driven, never cron'd.
    assert not any("fhir" in n for n in names)
    # All Saturday, staggered >= 30 minutes apart.
    minutes = []
    for s in scheds:
        m, h, _, _, dow = s.cron_schedule.split()
        assert dow == "6"  # Saturday
        minutes.append(int(h) * 60 + int(m))
    minutes.sort()
    assert all(minutes[i + 1] - minutes[i] >= 30 for i in range(len(minutes) - 1))


# --- auto-enable status ----------------------------------------------------------


def test_get_sensor_default_status_follows_auto_enable():
    from schedules import schedule_default_status as sds

    with patch.object(sds, "should_auto_enable_schedules", return_value=True):
        assert sds.get_sensor_default_status() == dg.DefaultSensorStatus.RUNNING
    with patch.object(sds, "should_auto_enable_schedules", return_value=False):
        assert sds.get_sensor_default_status() == dg.DefaultSensorStatus.STOPPED
