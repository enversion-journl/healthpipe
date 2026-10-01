# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL ETL Schedules

Schedules for SQL domain ETL jobs (organization).

Note: The legacy partitioned ERP daily schedule was retired together with the
partitioned ERP ETL jobs; daily ERP processing now runs via
``erp_daily_dfm_schedule`` (see ``schedules/erp_daily_dfm_schedules.py``).
"""

import dagster as dg
from jobs.organization_jobs import organisation_cs_job
from schedules.schedule_default_status import get_schedule_default_status

SCHEDULE_DEFAULT_STATUS = get_schedule_default_status()


organization_daily_schedule = dg.ScheduleDefinition(
    job=organisation_cs_job,
    cron_schedule="0 1 * * *",  # 1:00 AM every day
    execution_timezone="Europe/Copenhagen",
    default_status=SCHEDULE_DEFAULT_STATUS,
)

__all__ = [
    "organization_daily_schedule",
]
