# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedule for dim.Dato daily refresh job."""

import dagster as dg
from jobs.dim_dato_jobs import dim_dato_daily_job

dim_dato_daily_schedule = dg.ScheduleDefinition(
    job=dim_dato_daily_job,
    cron_schedule="0 0 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
