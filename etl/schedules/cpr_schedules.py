# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedule for the CPR load: daily at 22 CET, one delivery per run; a day without a new file is a no-op."""

import dagster as dg
from core.branch_postfix import get_postfixed_name
from jobs.cpr_jobs import cpr_ingest_job, ingest_run_active
from schedules.schedule_default_status import get_schedule_default_status


@dg.schedule(
    name="cpr_ingest_daily_schedule",
    job=cpr_ingest_job,
    cron_schedule="0 22 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Load the oldest unregistered CPR delivery, daily at "
        "22:00 CET, after DOL has fetched the evening's extract. A day without a new "
        "file is a no-op. Skips while an earlier cpr_ingest_job run is still active."
    ),
)
def cpr_ingest_daily_schedule(context: dg.ScheduleEvaluationContext):
    if ingest_run_active(context.instance, get_postfixed_name(cpr_ingest_job.name)):
        return dg.SkipReason("A cpr_ingest_job run is still active")
    return dg.RunRequest()


__all__ = ["cpr_ingest_daily_schedule"]
