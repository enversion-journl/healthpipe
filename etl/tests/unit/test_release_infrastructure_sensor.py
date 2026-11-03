# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the release infrastructure sensor and the schedule gate."""

from datetime import timedelta

import dagster as dg
import pytest
from dagster._core.storage.dagster_run import DagsterRun, make_new_run_id
from sensors import release_infrastructure_sensor
from sensors.release_infrastructure_sensor import (
    RELEASE_TAG,
    build_release_infrastructure_sensor,
    gate_schedules_on_release_infrastructure,
    release_identity,
)

RELEASE_POSTFIX = "release-1-4-0"
INFRASTRUCTURE_JOB = "full_infrastructure_setup"


@dg.op
def _noop():
    pass


@dg.job
def _nightly_job():
    _noop()


_cron_schedule = dg.ScheduleDefinition(
    name="cron_schedule",
    cron_schedule="0 4 * * *",
    job=_nightly_job,
    run_config={"ops": {"_noop": {}}},
    tags={"layer": "dwh"},
)


@dg.schedule(name="decorated_schedule", cron_schedule="0 5 * * *", job=_nightly_job)
def _decorated_schedule(context: dg.ScheduleEvaluationContext):
    return dg.RunRequest(run_key="nightly", tags={"cycle": "2026-10-22"})


def _add_run(instance: dg.DagsterInstance, status: dg.DagsterRunStatus, release: str) -> None:
    instance.add_run(
        DagsterRun(
            job_name=INFRASTRUCTURE_JOB,
            run_id=make_new_run_id(),
            status=status,
            tags={RELEASE_TAG: release},
        )
    )


def _stale_limit_exceeded(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(release_infrastructure_sensor, "_STALE_RUN_LIMIT", timedelta(0))


@pytest.mark.unit
@pytest.mark.parametrize("schedule", [_cron_schedule, _decorated_schedule])
@pytest.mark.parametrize(
    "runs,expected",
    [
        ([], "raise"),
        ([dg.DagsterRunStatus.FAILURE], "raise"),
        ([(dg.DagsterRunStatus.SUCCESS, "release-1-4-0@000000000000")], "raise"),
        ([dg.DagsterRunStatus.STARTING], "skip"),
        ([dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.STARTED], "skip"),
        ([dg.DagsterRunStatus.STARTED, "stale"], "raise"),
        ([dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.SUCCESS], "run"),
    ],
)
def test_gate_opens_only_on_a_successful_run_for_this_release(
    monkeypatch: pytest.MonkeyPatch, schedule: dg.ScheduleDefinition, runs: list, expected: str
) -> None:
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", RELEASE_POSTFIX)
    release = release_identity()
    [gated] = gate_schedules_on_release_infrastructure([schedule], INFRASTRUCTURE_JOB)
    with dg.instance_for_test() as instance:
        for run in runs:
            if run == "stale":
                _stale_limit_exceeded(monkeypatch)
                continue
            status, run_release = run if isinstance(run, tuple) else (run, release)
            _add_run(instance, status, run_release)
        context = dg.build_schedule_context(instance=instance)

        if expected == "raise":
            with pytest.raises(dg.DagsterInvariantViolationError, match=release):
                gated.evaluate_tick(context)
            return
        result = gated.evaluate_tick(context)
        if expected == "skip":
            assert result.run_requests == []
            assert release in result.skip_message
            return
        assert result.run_requests == schedule.evaluate_tick(context).run_requests
        assert gated.name == schedule.name


@pytest.mark.unit
@pytest.mark.parametrize("postfix", [None, "", "feature-jira-123"])
def test_schedules_outside_a_release_are_not_gated(
    monkeypatch: pytest.MonkeyPatch, postfix: str | None
) -> None:
    if postfix is None:
        monkeypatch.delenv("DAGSTER_BRANCH_POSTFIX", raising=False)
    else:
        monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", postfix)
    schedules = [_cron_schedule, _decorated_schedule]

    assert gate_schedules_on_release_infrastructure(schedules, INFRASTRUCTURE_JOB) == schedules


@pytest.mark.unit
@pytest.mark.parametrize(
    "postfix,is_release", [("release-1-4-0", True), ("main-1-4-0", True), ("feature-x", False)]
)
def test_sensor_requests_one_run_per_release(
    monkeypatch: pytest.MonkeyPatch, postfix: str, is_release: bool
) -> None:
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", postfix)
    job = dg.define_asset_job(name=INFRASTRUCTURE_JOB, selection="*")
    sensor = build_release_infrastructure_sensor(job)

    result = sensor.evaluate_tick(dg.build_sensor_context())

    if not is_release:
        assert result.run_requests == []
        return
    [request] = result.run_requests
    assert request.run_key == release_identity()
    assert request.tags[RELEASE_TAG] == request.run_key


@pytest.mark.unit
def test_a_rebuild_under_the_same_tag_is_a_new_release(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    monkeypatch.setenv("DAGSTER_BRANCH_POSTFIX", RELEASE_POSTFIX)
    monkeypatch.setattr(release_infrastructure_sensor, "_CODE_ROOT", tmp_path)
    migration = tmp_path / "infrastructure" / "ddl" / "migrations" / "20261022_x.sql"
    migration.parent.mkdir(parents=True)
    identities = []
    for content in ("ALTER TABLE a ADD b INT", "ALTER TABLE a ADD c INT"):
        migration.write_text(content)
        release_infrastructure_sensor._code_version.cache_clear()
        identities.append(release_identity())
    release_infrastructure_sensor._code_version.cache_clear()

    assert identities[0] != identities[1]
