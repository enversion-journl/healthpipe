# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Walks the DRG ingest queue, one takstsystem per run.

The schedule requests only the queue's head and carries the rest in a run tag
(see ``schedules/drg_schedules.py``). This sensor is the other half: on SUCCESS
of any DRG ingest run it takes the next entry and requests it.

It is deliberately dumb. It reads neither the share nor any checkpoint — it
walks a list somebody else computed — which keeps the delivery decision inside
the assets, where it is visible in asset metadata and testable.

Why a chain rather than a concurrency tag: the runs of one leverancetype must
not overlap, because they share ext tables. Requesting the next run only after
the previous has FINISHED gives that for free, with no instance configuration.
It also gives the M-before-U gating for free — U entries sit after every M
entry, so a failed M stops the queue before U is reached.

A failure or a lost tick is not a dead end: the next schedule tick rebuilds the
queue from the top, and everything already ingested is a tier-1 no-op.

``build_drg_queue_failure_sensor`` is the other side of that: because the chain
advances on SUCCESS only, a failed run stops the cycle, and the units after it
are never requested. The failed run is red in the UI — but the runs that never
happened are an ABSENCE, which is the hard thing to notice. The failure sensor
names them.
"""

import json
from datetime import date
from typing import Any

import dagster as dg
from schedules.drg_schedules import CYCLE_DATE_TAG, LEVERANCETYPE_TAG, QUEUE_TAG, UNIT_TAG
from schedules.schedule_default_status import get_sensor_default_status


def build_drg_queue_sensor(ingest_jobs: list, job_for_unit: dict[tuple[str, str], Any]):
    """Chain sensor over the DRG ingest jobs.

    ``job_for_unit`` maps (leverancetype, kadence) to the job object that
    handles it, so the sensor can switch from the monthly to the weekly job
    when the queue reaches its U entries. Built in definitions.py so it
    references the branch-postfixed jobs.
    """

    @dg.run_status_sensor(
        name="drg_queue_sensor",
        run_status=dg.DagsterRunStatus.SUCCESS,
        monitored_jobs=ingest_jobs,
        request_jobs=list({j.name: j for j in job_for_unit.values()}.values()),
        default_status=get_sensor_default_status(),
        description=(
            "Request the next (takstsystem, kadence) unit in the DRG ingest "
            "queue when the previous run succeeds."
        ),
    )
    def _sensor(context: dg.RunStatusSensorContext):
        tags = context.dagster_run.tags
        leverancetype = tags.get(LEVERANCETYPE_TAG)
        raw_queue = tags.get(QUEUE_TAG)
        if not leverancetype or raw_queue is None:
            # A manually launched run carries no queue. Nothing to continue.
            return dg.SkipReason("Koerslen er ikke en del af en DRG-koe")

        try:
            remaining = json.loads(raw_queue)
        except json.JSONDecodeError:
            return dg.SkipReason(f"Ulaeseligt koe-tag: {raw_queue!r}")
        if not remaining:
            return dg.SkipReason(f"{leverancetype}-koeen er toemt")

        entry, rest = remaining[0], remaining[1:]
        takstsystem, kadence = entry
        job = job_for_unit.get((leverancetype, kadence))
        if job is None:
            return dg.SkipReason(f"Intet job for {leverancetype}/{kadence}")

        # Imported here so the sensor module does not depend on the schedule's
        # import order at definition time.
        from schedules.drg_schedules import queue_run_request

        # The cycle date comes from the tag, so every link shares the run_key
        # day the schedule chose — create_timestamp is UTC and per-link, which
        # would split a midnight-crossing cycle.
        run_day = date.fromisoformat(tags[CYCLE_DATE_TAG])
        request = queue_run_request(leverancetype, entry, rest, run_day)
        context.log.info(
            f"{leverancetype}: naeste enhed {takstsystem}/{kadence} "
            f"({len(rest)} tilbage i koeen)"
        )
        return dg.RunRequest(
            run_key=request.run_key,
            run_config=request.run_config,
            tags=request.tags,
            job_name=job.name,
        )

    return _sensor


def queue_halt_message(
    leverancetype: str, unit: str, remaining: list, job_name: str, run_id: str
) -> str:
    """What a failed run cut short, in words.

    Kept separate from the sensor so the wording — the part an operator reads
    at 07:00 — is testable without Dagster scaffolding.
    """
    if remaining:
        ikke_behandlet = ", ".join(f"{t}/{k}" for t, k in remaining)
        return (
            f"{leverancetype}-koeen STOPPEDE paa {unit} (job={job_name}, "
            f"run_id={run_id}). Foelgende enheder behandles IKKE i denne "
            f"cyklus: {ikke_behandlet}. Naeste schedule-tick bygger koeen "
            f"forfra; alt allerede indlaest er en no-op."
        )
    return (
        f"{leverancetype}-koeen fejlede paa sin SIDSTE enhed {unit} "
        f"(job={job_name}, run_id={run_id}). Ingen enheder stod i koe bagefter, "
        f"saa cyklussen er komplet paa naer denne."
    )


def build_drg_queue_failure_sensor(ingest_jobs: list):
    """Alarm when a DRG ingest run fails and takes the rest of the cycle with it.

    The failing run is visible on its own — red, with the exception and logs.
    What is NOT visible is the consequence: the chain sensor fires on SUCCESS,
    so every unit still queued behind the failure is silently never requested.
    For somatik that includes the weekly entries, which sit after every monthly
    one. This sensor reads the remaining queue off the failed run's tag and says
    out loud what will not be processed. It is a Dagster-native FAILURE sensor
    needing no external transport, so unlike nightly_failure_sensor it does not
    depend on the ADO-to-GitLab port.

    Recovery needs no action: the next schedule tick rebuilds the queue from the
    top, and everything already ingested is a tier-1 no-op. The alarm is about
    knowing a cycle was cut short, not about repairing it.
    """

    @dg.run_status_sensor(
        name="drg_queue_failure_sensor",
        run_status=dg.DagsterRunStatus.FAILURE,
        monitored_jobs=ingest_jobs,
        default_status=get_sensor_default_status(),
        description=(
            "On DRG ingest FAILURE: alarm, naming the units still queued behind "
            "the failure that will NOT be processed this cycle."
        ),
    )
    def _sensor(context: dg.RunStatusSensorContext):
        tags = context.dagster_run.tags
        leverancetype = tags.get(LEVERANCETYPE_TAG)
        unit = tags.get(UNIT_TAG, "ukendt enhed")
        raw_queue = tags.get(QUEUE_TAG)

        if not leverancetype or raw_queue is None:
            # A manually launched run is not part of a cycle: it stops nothing.
            return dg.SkipReason("Koerslen er ikke en del af en DRG-koe")

        try:
            remaining = json.loads(raw_queue)
        except json.JSONDecodeError:
            remaining = []

        context.log.error(
            queue_halt_message(
                leverancetype,
                unit,
                remaining,
                context.dagster_run.job_name,
                context.dagster_run.run_id,
            )
        )
        return dg.SkipReason(f"Alarm logget for {leverancetype}/{unit}")

    return _sensor


__all__ = [
    "build_drg_queue_failure_sensor",
    "build_drg_queue_sensor",
    "queue_halt_message",
]
