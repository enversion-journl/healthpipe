# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR ETL schedules.

Only the weekly CodeSystem refresh lives here. The IFDB → FHIR pipeline runs as
a single job on ``ifdb_dfm_daily_schedule`` (see ``schedules/ifdb_dfm_schedules``);
the partitioned daily job it used to build RunRequests for is gone.
"""

import dagster as dg
from jobs.codesystem_jobs import fhir_codesystem_job
from schedules.schedule_default_status import get_schedule_default_status

SCHEDULE_DEFAULT_STATUS = get_schedule_default_status()


# Clinical SKS/SOR CodeSystem refresh -> DSO + FHIR terminology server.
# Weekly (not daily) because the content-complete SOR PUT is ~70 MB. The job is
# unpartitioned, so a plain ScheduleDefinition is sufficient. Runs Sunday 04:00,
# clear of the daily org (01:00) and erp (02:00) jobs.
fhir_codesystem_weekly_schedule = dg.ScheduleDefinition(
    job=fhir_codesystem_job,
    cron_schedule="0 4 * * 0",  # 04:00 every Sunday
    execution_timezone="Europe/Copenhagen",
    default_status=SCHEDULE_DEFAULT_STATUS,
)


__all__ = [
    "fhir_codesystem_weekly_schedule",
]
