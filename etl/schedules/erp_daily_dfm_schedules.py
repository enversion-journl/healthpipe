# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ERP daily DFM pipeline schedule."""

import dagster as dg
from jobs.erp_daily_dfm_jobs import erp_daily_dfm_job

erp_daily_dfm_schedule = dg.ScheduleDefinition(
    job=erp_daily_dfm_job,
    cron_schedule="0 23 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=dg.DefaultScheduleStatus.STOPPED,
)
