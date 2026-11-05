# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Release infrastructure: run full_infrastructure_setup once per release, before any schedule.

QA (``release-*``) and prod (``main-*``) deployments start their schedules as soon as the
code location loads. The sensor requests the infrastructure job once per release identity
(the image tag plus a hash of the deployed code, because CI rebuilds a release under the
same tag), which is the run key and the ``release`` run tag. Every schedule in the location
is gated on a successful run with that tag: while it is queued or running, ticks skip;
when it failed, never ran or is in progress for longer than the stale limit (the run
process can die without a status change), ticks fail and name the release. A skipped or
failed tick is
not retried. Re-executing the failed run keeps the tag and opens the gate. Development,
feature and local locations are not gated.
"""

import functools
import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import dagster as dg
from dagster._core.definitions.resource_annotation import get_resource_args
from dagster._core.definitions.schedule_definition import DecoratedScheduleFunction
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from dagster._utils import ensure_gen
from schedules.schedule_default_status import (
    AUTO_ENABLE_BRANCH_PREFIXES,
    get_sensor_default_status,
)

RELEASE_TAG = "release"

_IN_PROGRESS_STATUSES = {
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
}


_STALE_RUN_LIMIT = timedelta(hours=3)

_CODE_ROOT = Path(__file__).resolve().parents[1]


def _release_postfix() -> str | None:
    postfix = os.getenv("DAGSTER_BRANCH_POSTFIX", "").strip().lower()
    return postfix if postfix.startswith(AUTO_ENABLE_BRANCH_PREFIXES) else None


@functools.cache
def _code_version() -> str:
    digest = hashlib.sha256()
    for path in sorted(_CODE_ROOT.rglob("*")):
        if path.suffix in (".py", ".sql", ".json") and path.is_file():
            digest.update(path.relative_to(_CODE_ROOT).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()[:12]


def release_identity() -> str | None:
    """``<image tag>@<code hash>`` on a QA/prod (``release-*``/``main-*``) deployment."""
    postfix = _release_postfix()
    return f"{postfix}@{_code_version()}" if postfix else None


def build_release_infrastructure_sensor(
    infrastructure_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Request the infrastructure job once per release deployment."""

    @dg.sensor(
        name="release_infrastructure_sensor",
        job=infrastructure_job,
        default_status=get_sensor_default_status(),
        minimum_interval_seconds=60,
        description=(
            f"On a release-*/main-* deployment: run {infrastructure_job.name} once per "
            "release. Until it succeeds, every schedule tick skips or fails."
        ),
    )
    def _sensor(context: dg.SensorEvaluationContext):
        release = release_identity()
        if release is None:
            return dg.SkipReason("Not a release-*/main-* deployment.")
        return dg.RunRequest(run_key=release, tags={RELEASE_TAG: release})

    return _sensor


def build_release_infrastructure_failure_sensor(
    infrastructure_job: dg.JobDefinition | UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Alarm when the release infrastructure run fails and the schedules stay closed."""

    @dg.run_status_sensor(
        name="release_infrastructure_failure_sensor",
        run_status=dg.DagsterRunStatus.FAILURE,
        monitored_jobs=[infrastructure_job],
        default_status=get_sensor_default_status(),
        description=(
            f"On {infrastructure_job.name} FAILURE for a release: alarm. No schedule "
            "runs until a run for the release succeeds."
        ),
    )
    def _sensor(context: dg.RunStatusSensorContext):
        run = context.dagster_run
        release = run.tags.get(RELEASE_TAG)
        if release is None:
            return dg.SkipReason(f"Run '{run.run_id}' is not a release infrastructure run.")
        context.log.error(
            f"{run.job_name} failed for release {release} (run {run.run_id}). No schedule "
            "in this code location runs until the run is re-executed and succeeds."
        )
        return dg.SkipReason(f"Alarm logged for release {release}.")

    return _sensor


def _check_release_infrastructure(
    context: dg.ScheduleEvaluationContext, infrastructure_job_name: str, schedule_name: str
) -> dg.SkipReason | None:
    """None when the release's infrastructure run succeeded; a skip while it is in progress."""
    release = release_identity()
    if release is None:
        return None
    records = context.instance.get_run_records(
        filters=dg.RunsFilter(job_name=infrastructure_job_name, tags={RELEASE_TAG: release})
    )
    if any(r.dagster_run.status == dg.DagsterRunStatus.SUCCESS for r in records):
        return None
    in_progress = [r for r in records if r.dagster_run.status in _IN_PROGRESS_STATUSES]
    stale_before = datetime.now(UTC) - _STALE_RUN_LIMIT
    if any(r.create_timestamp >= stale_before for r in in_progress):
        return dg.SkipReason(f"{infrastructure_job_name} for release {release} is still running.")
    if in_progress:
        raise dg.DagsterInvariantViolationError(
            f"{infrastructure_job_name} for release {release} has been in progress for more "
            f"than {_STALE_RUN_LIMIT}, so {schedule_name} does not run. If the run process is "
            "dead, mark the run as failed and re-execute it."
        )
    raise dg.DagsterInvariantViolationError(
        f"{infrastructure_job_name} has no successful run for release {release}, so "
        f"{schedule_name} does not run. Check release_infrastructure_sensor, then re-execute "
        f"the failed run or launch {infrastructure_job_name} with tag {RELEASE_TAG}={release}."
    )


def _gate_schedule(
    schedule: dg.ScheduleDefinition, infrastructure_job_name: str
) -> dg.ScheduleDefinition:
    original = schedule._execution_fn

    def _gated(fn):
        def _execution_fn(context: dg.ScheduleEvaluationContext):
            skip = _check_release_infrastructure(context, infrastructure_job_name, schedule.name)
            if skip is not None:
                yield skip
                return
            yield from ensure_gen(fn(context))

        return _execution_fn

    required_resource_keys: set[str] | None = set(schedule.required_resource_keys)
    if isinstance(original, DecoratedScheduleFunction):
        execution_fn = original._replace(wrapped_fn=_gated(original.wrapped_fn))
        # Dagster rejects explicit keys when the function takes its resources as parameters.
        if get_resource_args(original.decorated_fn):
            required_resource_keys = None
    else:
        execution_fn = _gated(original)
    return dg.ScheduleDefinition(
        name=schedule.name,
        cron_schedule=schedule.cron_schedule,
        job=schedule.job,
        tags=schedule.tags,
        metadata=schedule.metadata,
        description=schedule.description,
        execution_timezone=schedule.execution_timezone,
        default_status=schedule.default_status,
        required_resource_keys=required_resource_keys,
        execution_fn=execution_fn,
    )


def gate_schedules_on_release_infrastructure(
    schedules: list[dg.ScheduleDefinition], infrastructure_job_name: str
) -> list[dg.ScheduleDefinition]:
    """On a release deployment, gate every schedule on the release's infrastructure run."""
    if _release_postfix() is None:
        return schedules
    return [_gate_schedule(s, infrastructure_job_name) for s in schedules]
