# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Linked server daily schedule."""

import dagster as dg
from jobs.infrastructure_jobs import linked_server_job
from schedules.schedule_default_status import get_schedule_default_status

linked_server_daily_schedule = dg.ScheduleDefinition(
    job=linked_server_job,
    cron_schedule="0 22 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
)
