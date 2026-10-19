# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedule for the HAIBA ingest job.

05:00 CET, one hour before ``ifdb_dfm_daily_schedule``. That ordering is the
point of the time: a delivery that landed overnight has its stub patients in
Aidbox before the 06:00 DFM base job, so the SOF+Borger chain the base-job
sensor triggers projects them the same morning and haiba_publish_sensor can
publish that same day with the citizen keys already resolved.

Daily rather than weekly even though SDS delivers weekly: the delivery
detection in DSI/csv_files/haiba.py makes the run without a new delivery a
fast no-op — the newest usable file per HAI type is compared against the
delivery checkpoint in meta.DFMState on filename date + mtime, no CSV rows are
streamed into ext, and the archive declares a no-op off the manifest table the
extract emptied (ext.<table>_leverance_manifest, read by haiba_archive.py). A
new delivery is picked up at the next 05:00 run.

The detection is ALL-OR-NOTHING per ext table — one new HAI type re-reads
every type of that table. Do NOT "optimise" that into a per-type skip: the
archive merges with detect_deletes=True and upstream_is_full_load=True, i.e.
it treats ext as a complete snapshot, and a partial ext soft-deletes every row
of the missing types.
"""

import dagster as dg
from jobs.haiba_jobs import haiba_ingest_job
from schedules.schedule_default_status import get_schedule_default_status

haiba_ingest_daily_schedule = dg.ScheduleDefinition(
    job=haiba_ingest_job,
    cron_schedule="0 5 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Load the newest HAIBA delivery per HAI type and create stub patients, "
        "daily at 05:00 CET — before the 06:00 DFM base job, so the stubs are "
        "projected to ref.PatientToBorger the same morning. A day without a "
        "new delivery is a data no-op."
    ),
)

__all__ = ["haiba_ingest_daily_schedule"]
