# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedule for the udredningsret ingest job.

Daily at 02:00 CET although SDS delivers weekly (Monday evening, plus one
monthly file): the extract's delivery detection makes a day without a new file
a no-op that reads nothing (directory listing + one checkpoint lookup), so the
cost of checking daily is negligible, while a delayed weekly cadence would risk
the ~3 months the share keeps. The time is before ``ifdb_dfm_base_schedule``
(06:00) and after the nightly HAIBA/DRG windows are expected to be idle; the
load of a real delivery takes ~25 min on healthpipe-dev (extract ~15 min, keys ~6,
merge ~2).

Not a dependency of anything downstream yet: staging (US-3+) has its own jobs.
"""

import dagster as dg
from jobs.udredningsret_jobs import udredningsret_ingest_job
from schedules.schedule_default_status import get_schedule_default_status

udredningsret_ingest_daily_schedule = dg.ScheduleDefinition(
    job=udredningsret_ingest_job,
    cron_schedule="0 2 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Check the udredningsret share daily at 02:00 CET and ingest a delivery only when "
        "its packaging timestamp is newer than the DFM delivery checkpoint. A day without "
        "a new file is a no-op that reads nothing."
    ),
)

__all__ = ["udredningsret_ingest_daily_schedule"]
