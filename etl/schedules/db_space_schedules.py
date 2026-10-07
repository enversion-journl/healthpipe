# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Daily disk-space monitor at 01:00 Europe/Copenhagen."""

import dagster as dg
from jobs.db_space_jobs import db_space_daily_job
from schedules.schedule_default_status import get_schedule_default_status

db_space_daily_schedule = dg.ScheduleDefinition(
    name="db_space_daily_schedule",
    job=db_space_daily_job,
    cron_schedule="0 1 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description="01:00 local time: snapshot DSA + DSO disk occupancy.",
)
