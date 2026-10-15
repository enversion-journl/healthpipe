# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA post-ETL sensors -- run the DQA jobs right after the ETL that feeds them.

Each spec builds ONE run-status sensor per pipeline that, once its ETL has
settled, requests the pipeline's DQA jobs:

- ``daily_job``   -- requested on **every** qualifying run (the cheap intra-table
  checks), and
- ``weekly_jobs`` -- requested only when the triggering ETL ran on a **Saturday**
  (the heavy cross-table / cross-DB checks + the DSO reconciliation), so they stay
  weekly without a separate cron that could race the ETL.

**AND-join.** A spec may list several ``monitored`` ETL jobs; the sensor fires the
DQA jobs only once **all** of them have succeeded for the same business day. The
FHIR pipeline is a single job today (``ifdb_dfm_daily_job``) so the join is
trivial there, but the machinery stays: a pipeline that fans back out into
several jobs must not fire its DQA checks off the first one to finish.
``run_key`` is keyed on the business day, so repeated success events (and any
daemon retries) collapse to exactly one DQA run per day per requested job.

Business day is anchored to each monitored run's **start time** (Europe/Copenhagen),
not the sensor's wall clock, so a backlog processed after daemon downtime still maps
each run to its own day (mirrors ``sensors/dfm_base_job_sensor``).

Why sensors instead of a cron: the pipeline starts at 06:00 but its finish time
varies with data volume, so a fixed clock would race it. The
non-FHIR DSO reconciliation domains have scattered sync sources and no single
"after ETL" signal, so those stay on a staggered Saturday cron (see ``schedules.py``).

Sensors default to RUNNING in dev/prod (``get_sensor_default_status``) and STOPPED
elsewhere.
"""

from __future__ import annotations

import calendar
from datetime import date, datetime
from zoneinfo import ZoneInfo

import dagster as dg
from schedules.schedule_default_status import get_sensor_default_status

_TZ = ZoneInfo("Europe/Copenhagen")
_SATURDAY = 5  # datetime.weekday(): Monday=0 .. Sunday=6

# Per pipeline: the ETL job(s) that must have succeeded (AND-join) before the DQA
# jobs run, the daily DQA job (fired every day), and the weekly DQA jobs (fired on
# Saturday only). Un-postfixed names; definitions.py resolves them to the deployed
# objects and skips a spec whose jobs are absent in the deployment.
DQA_AFTER_ETL_SENSOR_SPECS: list[dict] = [
    {
        "name": "dqa_fhir_after_etl",
        # The flat fhir.* tables and their DSO copies are settled when the one
        # IFDB -> FHIR job finishes; there is no second job to wait for.
        "monitored": ["ifdb_dfm_daily_job"],
        "daily_job": "dqa_fhir_daily_job",
        # Saturday only: heavy referential/source/server checks, plus the DSO<->DSA
        # reconciliation for the FHIR domain.
        "weekly_jobs": ["dqa_fhir_weekly_job", "dqa_dso_fhir_job"],
    },
    {
        "name": "dqa_erp_daily_after_dfm",
        # The DFM ERP job builds dim/fct AND runs the ERP DSO sync in one job.
        "monitored": ["erp_daily_dfm_job"],
        "daily_job": "dqa_dwh_daily_job",
        "weekly_jobs": ["dqa_dwh_weekly_job"],
        "default_status": dg.DefaultSensorStatus.STOPPED,
    },
    {
        "name": "dqa_drg_after_publish",
        # drg_publish_job rebuilds the DRG DSA layer AND runs the DRG DSO sync.
        # The DRG DQA subjects (dwh_drgkontakt + the two group bridges) live in
        # the DWH track, so this just triggers the shared DWH DQA jobs after the
        # DRG publish settles. STOPPED until the sentinel caps are validated
        # against a real run (and the first psyk delivery); see dwh_subjects.
        "monitored": ["drg_publish_job"],
        "daily_job": "dqa_dwh_daily_job",
        "weekly_jobs": ["dqa_dwh_weekly_job"],
        "default_status": dg.DefaultSensorStatus.STOPPED,
    },
    {
        "name": "dqa_haiba_after_publish",
        # dm.HAIBA_* only changes when the publish job ran (delivery-driven,
        # sensor-requested), so every HAIBA check runs at DAILY cadence and
        # this fires per publish — a weekly tier would just re-check unchanged
        # tables. No weekly_jobs: publishes are not guaranteed to land on a
        # Saturday, so the DSO<->DSA reconciliation (dqa_dso_haiba_job) runs
        # on its own Saturday cron instead (see schedules.py).
        "monitored": ["haiba_publish_job"],
        "daily_job": "dqa_dwh_haiba_daily_job",
        "weekly_jobs": [],
    },
    {
        "name": "dqa_arc_ifdb_after_archive",
        # The arc.* IFDB tables are settled once the IFDB -> FHIR job succeeds.
        # Triggering here (Saturday only) instead of a cron removes the race where
        # the flow-start-bounded reconciliation reads a checkpoint the archive merge
        # hasn't caught up to. No daily arc checks.
        "monitored": ["ifdb_dfm_daily_job"],
        "daily_job": None,
        "weekly_jobs": ["dqa_arc_ifdb_weekly_job"],
    },
]


def _business_date(context: dg.RunStatusSensorContext) -> date:
    """The Europe/Copenhagen date the monitored run STARTED on.

    Anchored to the run record's start time (not wall-clock now()) so a backlog
    processed after daemon downtime maps each run to its own day. Falls back to
    now() only if the record carries no start time (should not happen for a
    completed run).
    """
    record = context.instance.get_run_record_by_id(context.dagster_run.run_id)
    start_time = getattr(record, "start_time", None) if record else None
    if start_time:
        return datetime.fromtimestamp(start_time, tz=_TZ).date()
    context.log.warning(
        f"Run '{context.dagster_run.run_id}' has no start_time on its record; "
        f"falling back to wall-clock now() for the DQA day gate."
    )
    return datetime.now(tz=_TZ).date()


def _job_succeeded_on(instance, job_name: str, business_date: date) -> bool:
    """True if ``job_name`` has a SUCCESS run that started on ``business_date`` (local)."""
    records = instance.get_run_records(
        filters=dg.RunsFilter(job_name=job_name, statuses=[dg.DagsterRunStatus.SUCCESS]),
        limit=50,
    )
    for r in records:
        st = getattr(r, "start_time", None)
        if st and datetime.fromtimestamp(st, tz=_TZ).date() == business_date:
            return True
    return False


def _tags(name: str, parent: str, cadence: str) -> dict:
    return {
        "dagster/triggered_by": name,
        "dagster/parent_run_id": parent,
        "dqa_cadence": cadence,
    }


def _evaluate(context, *, name, monitored_names, daily_name, weekly_names):
    """Core sensor logic (kept plain for unit testing, no Dagster sensor wrapper).

    Returns a list of RunRequests, or a SkipReason when the AND-join is incomplete
    or there is nothing to request today.
    """
    parent = context.dagster_run.run_id
    business_date = _business_date(context)

    # AND-join: fire only once every monitored ETL job has succeeded for this day.
    pending = [
        n for n in monitored_names if not _job_succeeded_on(context.instance, n, business_date)
    ]
    if pending:
        return dg.SkipReason(
            f"{name}: waiting for {pending} to also succeed for {business_date.isoformat()}."
        )

    day = business_date.isoformat()
    requests: list[dg.RunRequest] = []
    if daily_name:
        requests.append(
            dg.RunRequest(
                run_key=f"{name}-daily-{day}",
                job_name=daily_name,
                tags=_tags(name, parent, "daily"),
            )
        )
    if business_date.weekday() == _SATURDAY:
        for wn in weekly_names:
            requests.append(
                dg.RunRequest(
                    run_key=f"{name}-{wn}-{day}",
                    job_name=wn,
                    tags=_tags(name, parent, "weekly"),
                )
            )

    if not requests:
        return dg.SkipReason(
            f"{name}: nothing to request for {calendar.day_name[business_date.weekday()]} "
            f"{day} (no daily job and not Saturday)."
        )
    return requests


def build_dqa_after_etl_sensor(
    *,
    name: str,
    monitored_jobs: list,
    daily_job=None,
    weekly_jobs: list | None = None,
    description: str = "",
    default_status: dg.DefaultSensorStatus | None = None,
):
    """A run-status sensor that requests a pipeline's DQA jobs after its ETL settles.

    Fires when any ``monitored_jobs`` run succeeds, but only requests the DQA jobs
    once ALL of them have succeeded for the same (start-time) business day. Requests
    ``daily_job`` every day and ``weekly_jobs`` on Saturdays only.
    """
    weekly_jobs = weekly_jobs or []
    request_jobs = ([daily_job] if daily_job is not None else []) + list(weekly_jobs)
    monitored_names = [j.name for j in monitored_jobs]
    daily_name = daily_job.name if daily_job is not None else None
    weekly_names = [j.name for j in weekly_jobs]

    @dg.run_status_sensor(
        name=name,
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=monitored_jobs,
        request_jobs=request_jobs,
        default_status=default_status or get_sensor_default_status(),
        description=description,
    )
    def _sensor(context: dg.RunStatusSensorContext):
        return _evaluate(
            context,
            name=name,
            monitored_names=monitored_names,
            daily_name=daily_name,
            weekly_names=weekly_names,
        )

    return _sensor
