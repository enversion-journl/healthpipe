# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL ETL Schedules

Schedules for SQL domain ETL jobs (erp, organization).

Note: Schedules for partitioned jobs must use custom schedule functions that
return RunRequest with partition_key set. Using ScheduleDefinition alone
will result in "Cannot access partition_key for a non-partitioned run" errors.
"""

import dagster as dg
from jobs.erp_jobs import erp_daily_job
from jobs.organization_jobs import organisation_cs_job
from schedules.schedule_default_status import get_schedule_default_status

SCHEDULE_DEFAULT_STATUS = get_schedule_default_status()


# ERP daily schedule - runs at 2 AM Copenhagen time
# Uses custom schedule function to provide partition_key for partitioned assets
@dg.schedule(
    job=erp_daily_job,
    cron_schedule="0 2 * * *",  # 2:00 AM every day
    execution_timezone="Europe/Copenhagen",
    default_status=SCHEDULE_DEFAULT_STATUS,
)
def erp_daily_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """
    Schedule for ERP daily ETL job.

    Returns a RunRequest with the partition_key set to the scheduled execution date.
    This is required because the job contains partitioned assets that need a partition_key.
    Since daily_partitions has end_offset=1, the current day's partition is available.

    Uses scheduled_catchup mode to load last 25 hours of data (01:00 yesterday to 02:00 today).
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(
        partition_key=scheduled_date,
        tags={"erp_extraction_mode": "scheduled_catchup"},
    )


organization_daily_schedule = dg.ScheduleDefinition(
    job=organisation_cs_job,
    cron_schedule="0 1 * * *",  # 1:00 AM every day
    execution_timezone="Europe/Copenhagen",
    default_status=SCHEDULE_DEFAULT_STATUS,
)

__all__ = [
    "erp_daily_schedule",
    "organization_daily_schedule",
]
