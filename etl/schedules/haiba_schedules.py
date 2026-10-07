# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedule for the HAIBA ingest job.

05:00 CET, one hour before ``ifdb_dfm_base_schedule``. That ordering is the
point of the time: a delivery that landed overnight has its stub patients in
Aidbox before the 06:00 DFM base job, so the SOF+Borger chain the base-job
sensor triggers projects them the same morning and haiba_publish_sensor can
publish that same day with the citizen keys already resolved.

Daily rather than weekly even though SDS delivers weekly. The extract has no
"already loaded" short-circuit — it re-reads the newest usable file per HAI
type every run — but everything downstream is idempotent: guard_delivery_
direction refuses an older delivery, the archive MERGE runs with
update_only_on_change (leverance_dato deliberately outside change_detect_
columns), the delivery register upserts its row rather than appending, the
revision log only records real value changes, and the stub creation resolves
against Aidbox and posts nothing. A day without a new delivery therefore
changes no data. It is not free — the newest files are re-streamed into ext and
the archive re-probes the merge keys — so this trades a daily re-read for not
having to detect deliveries, which is a separate work item.

Do NOT "optimise" that by skipping HAI types already recorded in
meta.HAIBALeverance. The archive merges with detect_deletes=True and
upstream_is_full_load=True, i.e. it treats ext as a complete snapshot: a
partial ext soft-deletes every row of the missing types.
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
