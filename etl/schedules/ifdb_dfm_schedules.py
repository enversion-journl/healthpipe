# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""IFDB DFM pipeline schedule."""

import dagster as dg
from jobs.ifdb_dfm_jobs import fhir_ifdb_dfm_base_job
from schedules.schedule_default_status import get_schedule_default_status

ifdb_dfm_base_schedule = dg.ScheduleDefinition(
    job=fhir_ifdb_dfm_base_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Trigger the DFM FHIR base job daily at 06:00 CET. "
        "On success, the run_status_sensor triggers the remaining daily job "
        "(CarePlan) and the DFM SOF+DSO job (incl. SFI_datapoint refresh)."
    ),
)
