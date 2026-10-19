# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Schedules for the DRG ingest queue.

Automation is ONE SERIALISED QUEUE PER LEVERANCETYPE. Within a leverancetype
two runs must never overlap — somatik M and somatik U write to the same
``ext.drg_*`` tables, which are dropped and recreated per run — while somatik
and psyk share nothing and run in parallel.

The schedule builds the queue from the share (``os.listdir`` + the filename
regex; no tar is opened, no database is read) and requests only its HEAD. The
rest travels in a run tag, and ``sensors/drg_queue_sensor.py`` requests the
next entry when the previous run succeeds. Three properties fall out of that:

* takstsystemer run in sequence, because the next run is not requested until
  the previous one finished — no concurrency_key and no instance configuration;
* the U entries sit after every M entry, so a failed M stops the queue before U
  is reached. The "U only after M" gating is a property of the queue order, not
  a separate trigger;
* the chain is self-healing. A lost sensor tick costs at most a day: the next
  schedule tick rebuilds the queue from scratch, and everything already
  ingested is a tier-1 no-op.

The queue starts at 20:00 so it can cross midnight and still finish before
``ifdb_dfm_daily_schedule`` at 06:00 mints dw_ek_Borger for the ingest runs'
stub Patients. Publishing is not on this clock — ``drg_publish_sensor`` fires
after that chain.

A run with nothing pending is a fast no-op, so the queue deliberately contains
every takstsystem every night — including retired ones.
"""

import json
from datetime import date

import dagster as dg
from DSI.csv_files.drg import discover_archives, drg_dir, matches_kadence
from jobs.drg_jobs import (
    drg_freshness_job,
    drg_ingest_psyk_maaned_job,
    drg_ingest_somatik_maaned_job,
)
from schedules.schedule_default_status import get_schedule_default_status

QUEUE_TAG = "drg/queue"
LEVERANCETYPE_TAG = "drg/leverancetype"
# The unit this run handles, as "<takstsystem>/<kadence>". Carried as a tag so
# it is visible in the Dagster UI and readable by the sensors without having to
# dig the op key out of the run config.
UNIT_TAG = "drg/enhed"
# The cycle's date (the schedule tick's Europe/Copenhagen date), carried through
# every link so the whole cycle shares one run_key day — the queue starts at
# 20:00 and crosses midnight, and a per-link UTC date would split it.
CYCLE_DATE_TAG = "drg/cyklus-dato"

# Cadences each leverancetype delivers in, in QUEUE ORDER. Psyk ships M only.
KADENCER = {"DRG": ("M", "U"), "DRGPSYK": ("M",)}

EXT_ASSET = {"DRG": "ext_drg", "DRGPSYK": "ext_drgpsyk"}

_ACTIVE_STATUSES = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]


def build_queue(archives: list, leverancetype: str) -> list[list[str]]:
    """Every (takstsystem, kadence) unit to walk this cycle, in order.

    All M entries first, then all U. A unit is included only if the share
    actually holds a file of that cadence for it — a takstsystem that has never
    had a U series (a brand-new tariff year) must not get a U run that could
    only ever be a no-op.
    """
    of_type = [a for a in archives if a.leverancetype == leverancetype]
    systems = sorted({a.takstsystem for a in of_type})
    queue: list[list[str]] = []
    for kadence in KADENCER[leverancetype]:
        for takstsystem in systems:
            if any(a.takstsystem == takstsystem and matches_kadence(a, kadence) for a in of_type):
                queue.append([takstsystem, kadence])
    return queue


def queue_run_request(
    leverancetype: str, entry: list[str], remaining: list[list[str]], run_day: date
) -> dg.RunRequest:
    """Request one unit, carrying the rest of the queue forward in a tag.

    The op key is the bare asset name, also on a feature-branch deployment: the
    postfix is on the asset key, not on the op.
    """
    takstsystem, kadence = entry
    ext_asset = EXT_ASSET[leverancetype]
    return dg.RunRequest(
        run_key=f"{leverancetype}-{kadence}-{takstsystem}-{run_day.isoformat()}",
        run_config={
            "ops": {ext_asset: {"config": {"kadence": kadence, "takstsystemer": [takstsystem]}}}
        },
        tags={
            QUEUE_TAG: json.dumps(remaining),
            LEVERANCETYPE_TAG: leverancetype,
            UNIT_TAG: f"{takstsystem}/{kadence}",
            CYCLE_DATE_TAG: run_day.isoformat(),
        },
    )


def _build_queue_schedule(leverancetype: str, job, cron: str, name: str):
    @dg.schedule(
        name=name,
        job=job,
        cron_schedule=cron,
        execution_timezone="Europe/Copenhagen",
        default_status=get_schedule_default_status(),
        description=(
            f"Build the {leverancetype} ingest queue from the share and request "
            f"its first unit. The rest is walked by drg_queue_sensor as each run "
            f"succeeds, so the takstsystemer are processed one at a time."
        ),
    )
    def _schedule(context: dg.ScheduleEvaluationContext):
        # An M load can run past midnight. The run_key only dedupes within a
        # day, so without this a long load would be overtaken by the next tick
        # and the two would clear each other's ext.
        active = context.instance.get_run_records(
            filters=dg.RunsFilter(
                tags={LEVERANCETYPE_TAG: leverancetype}, statuses=_ACTIVE_STATUSES
            ),
            limit=1,
        )
        if active:
            return dg.SkipReason(
                f"En {leverancetype}-koersel er stadig i gang — koeen startes ikke forfra"
            )

        archives = discover_archives(drg_dir(), leverancetype)
        queue = build_queue(archives, leverancetype)
        if not queue:
            return dg.SkipReason(f"Ingen {leverancetype}-leverancer fundet paa sharet")

        run_day = context.scheduled_execution_time.date()
        context.log.info(f"{leverancetype}-koe: {queue}")
        return queue_run_request(leverancetype, queue[0], queue[1:], run_day)

    return _schedule


drg_somatik_ingest_schedule = _build_queue_schedule(
    "DRG",
    drg_ingest_somatik_maaned_job,
    # 20:00: the whole queue — every takstsystem in sequence — must finish
    # before ifdb_dfm_daily_schedule at 06:00, and a full monthly load is hours
    # of work, so the evening start gives 10 hours instead of 01:00's five.
    "0 20 * * *",
    "drg_somatik_ingest_schedule",
)

drg_psyk_ingest_schedule = _build_queue_schedule(
    "DRGPSYK",
    drg_ingest_psyk_maaned_job,
    # Same hour: psyk uses its own ext tables, so the two queues do not collide.
    "0 20 * * *",
    "drg_psyk_ingest_schedule",
)

drg_freshness_daily_schedule = dg.ScheduleDefinition(
    name="drg_freshness_daily_schedule",
    job=drg_freshness_job,
    # After the ingest window: the check reports on the SHARE, so it is
    # deliberately independent of whether the night's loads succeeded.
    cron_schedule="0 7 * * *",
    execution_timezone="Europe/Copenhagen",
    default_status=get_schedule_default_status(),
    description=(
        "Alarm on a stale DRG delivery series. Independent of the ingest jobs "
        "by design — a dead feed must be visible whether or not anything loads."
    ),
)

__all__ = [
    "LEVERANCETYPE_TAG",
    "QUEUE_TAG",
    "UNIT_TAG",
    "build_queue",
    "drg_freshness_daily_schedule",
    "drg_psyk_ingest_schedule",
    "drg_somatik_ingest_schedule",
    "queue_run_request",
]
