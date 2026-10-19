# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily LPR3 delivery detection; weekly and monthly files share one checkpoint."""

import dagster as dg
from core.branch_postfix import get_postfixed_name
from jobs.lpr3_jobs import lpr3_freshness_job, lpr3_ingest_job

_ACTIVE_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
    dg.DagsterRunStatus.CANCELING,
]


@dg.schedule(
    name="lpr3_ingest_daily_schedule",
    job=lpr3_ingest_job,
    cron_schedule="30 2 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description=(
        "Check for a newer LPR3 delivery daily at 02:30; ingest only on a new packaging "
        "timestamp. Skips while an earlier lpr3_ingest_job run is still active."
    ),
)
def lpr3_ingest_daily_schedule(context: dg.ScheduleEvaluationContext):
    # A full load can outlast a day; a second run would fail on the landing claim.
    active = context.instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=get_postfixed_name(lpr3_ingest_job.name), statuses=_ACTIVE_STATUSES
        ),
        limit=1,
    )
    if active:
        return dg.SkipReason("An lpr3_ingest_job run is still active")
    return dg.RunRequest()


lpr3_freshness_daily_schedule = dg.ScheduleDefinition(
    name="lpr3_freshness_daily_schedule",
    job=lpr3_freshness_job,
    cron_schedule="0 7 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=dg.DefaultScheduleStatus.STOPPED,
    description="Alarm daily at 07:00 when the newest LPR3 delivery on the share is stale.",
)
