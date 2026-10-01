# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR ETL schedules.

The direct 06:00 schedule for ``fhir_etl_ifdb_daily_job`` has been retired in
favor of sensor-based orchestration: ``dfm_base_job_success_sensor`` triggers
the daily job after ``fhir_ifdb_dfm_base_job`` succeeds (so the daily resources
that reference Patient/EpisodeOfCare/Encounter/Location run only after those
exist). ``fhir_etl_schedule`` therefore is NOT registered as a Dagster schedule
— it is kept as a plain helper that builds the daily job's RunRequest payload
(partition key + catchup tag), exercised by the schedule contract tests.
"""

import dagster as dg
from jobs.codesystem_jobs import fhir_codesystem_job
from schedules.schedule_default_status import get_schedule_default_status

SCHEDULE_DEFAULT_STATUS = get_schedule_default_status()


def fhir_etl_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest:
    """Build the RunRequest payload for the (retired) partitioned IFDB FHIR job.

    NOT a ScheduleDefinition and NOT registered in ``all_schedules`` — the daily
    job is sensor-triggered (see module docstring). Kept as a plain function so
    the partition-key/tag contract stays covered by tests; registering it would
    double-trigger the daily job alongside the sensor.
    """
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return dg.RunRequest(
        partition_key=scheduled_date,
        tags={"ifdb_extraction_mode": "scheduled_catchup"},
    )


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
    "fhir_etl_schedule",
    "fhir_codesystem_weekly_schedule",
]
