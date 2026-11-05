# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Practitioner daily schedule.

Drives both Practitioner sources in one run: SilkeborgData share and EPJI -> arc ->
FHIR server -> fhir.* -> reporting database. See jobs/practitioner_jobs.py for why
every layer and both sources are in the same job.

Daily, not monthly: SilkeborgData's delivery day is not contractually fixed, and a
monthly schedule that misses the release would leave the archive stale for up to a
month — silently. The SilkeborgData asset is a no-op on days without a delivery
folder newer than the DFM checkpoint, so the cost of looking every day is one
directory listing.
EPJI is a full snapshot on every run.

10:00, in working hours, and that is the point. One run posts to Aidbox, rebuilds
the flat fhir.* tables and writes the reporting database. A failure in any of that
is seen and can be acted on the same morning. The alternative — finishing before
anyone arrives — buys nothing here, because nothing downstream waits on
Practitioner: no other job selects its assets.

Nothing else runs at 10:00. The nightly block ends with ifdb_dfm_base at 06:00
and the evening block starts with linked_server at 22:00, so the run has the SMB
mount, staging, Aidbox and DSO to itself. HAIBA (05:00) reads the same
/mnt/ftproot mount and is long finished.

The settling window is no longer what decides the hour. The delivery files are
written ~02:21-02:52, so at 10:00 the newest is about seven hours old against the
60 minutes is_delivery_settled() asks for — a delivery would have to slip past
09:00 before the run defers it to the next day. If that ever starts happening,
move this schedule later rather than lowering MIN_FILE_AGE_MINUTES: the age check
is what stops a half-written file from being archived.
"""

import dagster as dg
from jobs.practitioner_jobs import practitioner_daily_job
from schedules.schedule_default_status import get_schedule_default_status

practitioner_daily_schedule = dg.ScheduleDefinition(
    job=practitioner_daily_job,
    cron_schedule="0 10 * * *",  # 10:00 every day — see docstring on working hours
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Run the Practitioner chain end to end for SilkeborgData and EPJI — "
        "extract, archive, FHIR server, flat tables, reporting database — daily at "
        "10:00 Europe/Copenhagen. In working hours deliberately: the run touches "
        "Aidbox and the reporting database, and a failure should be seen the same "
        "day. A day without a new SilkeborgData delivery folder is a no-op for "
        "that source."
    ),
)

__all__ = ["practitioner_daily_schedule"]
