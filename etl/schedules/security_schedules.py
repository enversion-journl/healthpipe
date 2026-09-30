# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Security (DPA / RBAC) daily pipeline schedule."""

import dagster as dg
from jobs.security_jobs import security_daily_job
from schedules.schedule_default_status import get_schedule_default_status

security_daily_schedule = dg.ScheduleDefinition(
    job=security_daily_job,
    cron_schedule="0 3 * * *",  # 03:00 every day
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
)
