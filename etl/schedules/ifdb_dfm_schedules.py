# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""IFDB DFM pipeline schedule."""

import dagster as dg
from jobs.ifdb_dfm_jobs import ifdb_dfm_daily_job
from schedules.schedule_default_status import get_schedule_default_status

ifdb_dfm_daily_schedule = dg.ScheduleDefinition(
    job=ifdb_dfm_daily_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Trigger the whole IFDB → FHIR pipeline daily at 06:00 CET: "
        "Oracle → ext → arc → FHIR server → SOF → DSO, then the Borger chain "
        "and the SFI_datapoint refresh. One job — no downstream sensor chain."
    ),
)
