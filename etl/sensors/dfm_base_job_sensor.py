# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Sensors tying the DFM base job to its downstream jobs.

``build_dfm_base_job_success_sensor`` — on SUCCESS of ``fhir_ifdb_dfm_base_job``
(Oracle → ext → arc → FHIR server) launches two downstream jobs:
  1. fhir_etl_ifdb_daily_job (partitioned) — remaining daily FHIR resources
  2. fhir_ifdb_dfm_sof_dso_job (unpartitioned) — SOF + DSO for DFM resources

``build_dfm_base_job_failure_sensor`` — on FAILURE of the base job, raises a
clear operator alarm (the downstream jobs are gated on success and will NOT run
this tick). Optionally, when ``DFM_BASE_FAILURE_TRIGGER_DAILY`` is enabled, it
triggers the legacy daily job best-effort (see below).

Partition date anchoring
-------------------------
The daily job is partitioned by date. The partition is derived from the
**monitored base run's start time** (Europe/Copenhagen), NOT the sensor's
wall-clock ``now()``. ``run_status_sensor`` is cursor-based, so when the daemon
processes a backlog of successes (e.g. after weekend downtime) each success
must map to ITS OWN day — using ``now()`` would map Friday's and Saturday's
successes to Monday's partition, and Friday/Saturday would never materialize.
Anchoring to the run's start time (the base job runs at 06:00 CET, far from
midnight) also avoids midnight-crossing skew.

Schedule-triggered only
------------------------
Only schedule-triggered runs fire the downstream chain. Ad-hoc runs (manual
"Materialize All", subset launches) are skipped — the success sensor checks for
the ``dagster/schedule_name`` tag that Dagster attaches to schedule-initiated
runs.

Note on re-executions: Dagster re-executions inherit the parent run's tags
(including ``dagster/schedule_name``), so re-executing a base run re-fires the
downstream chain. This is **intended** recovery behaviour: re-executing a
*failed* base run that now succeeds must re-trigger downstream. Re-executing an
already-green run re-fires too — harmless, since the downstream MERGE/upserts
are idempotent. We deliberately do not try to suppress this: the only reliable
re-execution marker (``dagster/parent_run_id``) is also set on the legitimate
failed→retry case we want to preserve.

Built via factories in ``definitions.py`` so the sensors receive the correct
(potentially branch-postfixed) job objects as ``monitored_jobs`` /
``request_jobs``.
"""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)
from schedules.schedule_default_status import get_sensor_default_status

_SCHEDULE_TAG = "dagster/schedule_name"
_TZ = ZoneInfo("Europe/Copenhagen")
_FAILURE_TRIGGER_ENV = "DFM_BASE_FAILURE_TRIGGER_DAILY"


def _resolve_business_date(
    context: dg.RunStatusSensorContext,
    run_id: str,
) -> str:
    """Derive the partition date from the monitored run's start time.

    Anchoring to the base run (which starts at 06:00 CET) rather than the
    sensor's wall-clock ``now()`` keeps the partition correct when the daemon
    processes a backlog of successes (each maps to its own day) and avoids
    midnight-crossing skew. Falls back to ``now()`` only if the run record has
    no start time (should not happen for a completed run).
    """
    run_record = context.instance.get_run_record_by_id(run_id)
    start_time = getattr(run_record, "start_time", None) if run_record else None
    if start_time:
        business_dt = datetime.fromtimestamp(start_time, tz=_TZ)
    else:
        context.log.warning(
            f"Run '{run_id}' has no start_time on its record; falling back to "
            f"wall-clock now() for the partition date. Backlogged runs may map "
            f"to the wrong day."
        )
        business_dt = datetime.now(tz=_TZ)
    return business_dt.strftime("%Y-%m-%d")


def _failure_trigger_enabled() -> bool:
    """Whether base-job failure should best-effort trigger the legacy daily job."""
    return os.getenv(_FAILURE_TRIGGER_ENV, "").strip().lower() in ("1", "true", "yes")


def build_dfm_base_job_success_sensor(
    base_job: UnresolvedAssetJobDefinition,
    daily_job: UnresolvedAssetJobDefinition,
    sof_dso_job: UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the DFM base-job success sensor with concrete job references.

    Must be called **after** branch-postfixing so the sensor receives the
    correctly-named job objects.  This keeps the UI targets accurate and
    avoids registering stale un-postfixed jobs.
    """

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=[base_job],
        request_jobs=[daily_job, sof_dso_job],
        name="dfm_base_job_success_sensor",
        default_status=get_sensor_default_status(),
        description=(
            "On fhir_ifdb_dfm_base_job SUCCESS (schedule-triggered only): "
            "launch the remaining partitioned daily job (CarePlan) and the "
            "DFM SOF+DSO job (incl. SFI_datapoint refresh). Ad-hoc / manual "
            "runs are ignored."
        ),
    )
    def dfm_base_job_success_sensor(
        context: dg.RunStatusSensorContext,
    ) -> dg.SensorResult | dg.SkipReason:
        """Trigger downstream jobs after a schedule-initiated base job succeeds."""
        run_tags = context.dagster_run.tags or {}
        if _SCHEDULE_TAG not in run_tags:
            return dg.SkipReason(
                f"Run '{context.dagster_run.run_id}' was not started by a "
                f"schedule (no '{_SCHEDULE_TAG}' tag). Downstream jobs only "
                f"trigger for scheduled runs."
            )

        parent_run_id = context.dagster_run.run_id
        # Anchor the partition to the base run's day, not the sensor's now().
        partition_date = _resolve_business_date(context, parent_run_id)

        context.log.info(
            f"DFM base job '{context.dagster_run.job_name}' succeeded "
            f"(schedule={run_tags[_SCHEDULE_TAG]}, run_id={parent_run_id}). "
            f"Triggering {daily_job.name} (partition={partition_date}) "
            f"and {sof_dso_job.name}."
        )

        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    job_name=daily_job.name,
                    run_key=(
                        f"dfm_base_success:{parent_run_id}:" f"{daily_job.name}:{partition_date}"
                    ),
                    partition_key=partition_date,
                    tags={
                        "dagster/triggered_by": "dfm_base_job_success_sensor",
                        "dagster/parent_run_id": parent_run_id,
                        "ifdb_extraction_mode": "scheduled_catchup",
                    },
                ),
                dg.RunRequest(
                    job_name=sof_dso_job.name,
                    run_key=(f"dfm_base_success:{parent_run_id}:" f"{sof_dso_job.name}"),
                    tags={
                        "dagster/triggered_by": "dfm_base_job_success_sensor",
                        "dagster/parent_run_id": parent_run_id,
                    },
                ),
            ]
        )

    return dfm_base_job_success_sensor


def build_dfm_base_job_failure_sensor(
    base_job: UnresolvedAssetJobDefinition,
    daily_job: UnresolvedAssetJobDefinition,
) -> dg.SensorDefinition:
    """Build the DFM base-job FAILURE sensor.

    By default this is an **alarm**: when the base job fails, neither the daily
    job nor the SOF/DSO job run this tick (they are gated on base success), so a
    clear error is logged. This closes the "silent black-hole" gap where a base
    failure left the whole FHIR day un-materialized with no signal — this
    Dagster-native sensor alarms immediately and independently on base failure.

    Optional best-effort recovery: set ``DFM_BASE_FAILURE_TRIGGER_DAILY=true``
    to ALSO trigger the legacy daily job on base failure. The legacy resources
    (Condition, Observation, Procedure, …) reference Patient/EpisodeOfCare/
    Location/Encounter, but the dependency is on those resources *existing*
    (persistent upserts + the on-demand ``resolve_missing_*`` resolver), not on
    today's base run succeeding — so the daily job can run best-effort even when
    a late base step failed. The SOF/DSO-DFM job is intentionally NOT triggered,
    as it syncs *today's* DFM output. Default is OFF (strict gating).

    Must be called **after** branch-postfixing.
    """

    @dg.run_status_sensor(
        run_status=dg.DagsterRunStatus.FAILURE,
        monitored_jobs=[base_job],
        request_jobs=[daily_job],
        name="dfm_base_job_failure_sensor",
        default_status=get_sensor_default_status(),
        description=(
            "On fhir_ifdb_dfm_base_job FAILURE: alarm (downstream daily + "
            "SOF/DSO are gated on success and will NOT run this tick). "
            f"If {_FAILURE_TRIGGER_ENV} is enabled, best-effort trigger the "
            "legacy daily job (not SOF/DSO)."
        ),
    )
    def dfm_base_job_failure_sensor(
        context: dg.RunStatusSensorContext,
    ) -> dg.SensorResult | dg.SkipReason:
        run_tags = context.dagster_run.tags or {}
        parent_run_id = context.dagster_run.run_id

        context.log.error(
            f"DFM base job '{context.dagster_run.job_name}' FAILED "
            f"(run_id={parent_run_id}). Downstream {daily_job.name} and the "
            f"DFM SOF/DSO job are gated on base SUCCESS and will NOT run for "
            f"this tick. Investigate and re-execute the base run to resume the "
            f"chain (re-execution re-fires the success sensor)."
        )

        if not _failure_trigger_enabled():
            return dg.SkipReason(
                f"Base job failed (run_id={parent_run_id}); alarm-only "
                f"({_FAILURE_TRIGGER_ENV} not enabled). No downstream triggered."
            )

        # Best-effort recovery path (opt-in). Only for schedule-lineage runs, so
        # a failed manual experiment doesn't kick off the daily pipeline.
        if _SCHEDULE_TAG not in run_tags:
            return dg.SkipReason(
                f"Base job failed (run_id={parent_run_id}) and was not "
                f"schedule-triggered; best-effort daily trigger skipped."
            )

        partition_date = _resolve_business_date(context, parent_run_id)
        context.log.warning(
            f"{_FAILURE_TRIGGER_ENV} enabled — best-effort triggering "
            f"{daily_job.name} (partition={partition_date}) despite base "
            f"failure. Legacy resources resolve base references via existing "
            f"data + on-demand resolution. SOF/DSO-DFM is NOT triggered."
        )
        return dg.SensorResult(
            run_requests=[
                dg.RunRequest(
                    job_name=daily_job.name,
                    run_key=(
                        f"dfm_base_failure:{parent_run_id}:" f"{daily_job.name}:{partition_date}"
                    ),
                    partition_key=partition_date,
                    tags={
                        "dagster/triggered_by": "dfm_base_job_failure_sensor",
                        "dagster/parent_run_id": parent_run_id,
                        "ifdb_extraction_mode": "scheduled_catchup",
                    },
                ),
            ]
        )

    return dfm_base_job_failure_sensor
