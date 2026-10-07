# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA schedules -- the DSO<->DSA reconciliation, weekly on Saturday.

The per-track daily/weekly DQA jobs are **sensor**-triggered after their ETL (see
``sensors.py``), so they are not cron'd here -- a fixed clock would race the
sensor-chained 06:00 pipeline.

The DSO reconciliation is the exception. It is detection-only and heavy
(correlated ``NOT EXISTS`` over the ``STAGING_LINK`` linked server across ~80
tables, some very large), its sync sources are scattered across several jobs and
days, and DSO drift is slow -- so it runs **weekly on Saturday**, each domain on
its own staggered slot (>= 30 min apart) to avoid hammering the linked server
simultaneously. The **fhir** domain is the exception to the exception: it is
sensor-triggered with the other FHIR weekly checks (gated on both FHIR jobs), so
it is excluded here.

Schedules reference the (un-postfixed) DSO job objects; ``definitions.py`` runs
them through ``postfix_schedules`` so they bind to the deployed jobs.
"""

from __future__ import annotations

import dagster as dg
from schedules.schedule_default_status import get_schedule_default_status

# DSO domain -> Saturday cron. >= 30 min apart, after the 06:00 pipelines settle.
# 'fhir' is intentionally absent: it is sensor-triggered (see sensors.py), gated on
# both FHIR jobs so its reconciliation sees the fully-synced flat tables.
_DSO_WEEKLY_CRONS: dict[str, str] = {
    "erp": "0 8 * * 6",  # Sat 08:00
    "shared": "30 8 * * 6",  # Sat 08:30
    "security": "0 9 * * 6",  # Sat 09:00
    "codesystem": "30 9 * * 6",  # Sat 09:30
    "conceptmap": "0 10 * * 6",  # Sat 10:00
    # HAIBA publishes are delivery-driven (haiba_publish_sensor), so unlike the
    # other tracks its reconciliation cannot ride a "monitored job ran on a
    # Saturday" gate — a cron is the only trigger that is guaranteed to fire.
    # Between publishes the DSO copy cannot drift on its own, so weekly is enough.
    "haiba": "30 10 * * 6",  # Sat 10:30
    "drg": "0 11 * * 6",  # Sat 11:00 (10:30 taken by haiba; keep the 30-min stagger)
}

_ALWAYS_STOPPED_DOMAINS: frozenset[str] = frozenset({"erp"})


def _dso_domain(job_name: str) -> str | None:
    """``dqa_dso_erp_job`` -> ``erp``; None if not a dqa_dso_<domain>_job name."""
    if job_name.startswith("dqa_dso_") and job_name.endswith("_job"):
        return job_name[len("dqa_dso_") : -len("_job")]
    return None


def build_dso_weekly_schedules(dso_jobs: list) -> list[dg.ScheduleDefinition]:
    """One staggered Saturday schedule per DSO domain job (except sensor-driven fhir)."""
    schedules: list[dg.ScheduleDefinition] = []
    for job in dso_jobs:
        domain = _dso_domain(job.name)
        cron = _DSO_WEEKLY_CRONS.get(domain) if domain else None
        if cron is None:
            continue  # fhir (sensor-driven) or an unmapped/new domain
        status = (
            dg.DefaultScheduleStatus.STOPPED
            if domain in _ALWAYS_STOPPED_DOMAINS
            else get_schedule_default_status()
        )
        schedules.append(
            dg.ScheduleDefinition(
                name=f"{job.name}_schedule",
                job=job,
                cron_schedule=cron,
                execution_timezone="Europe/Copenhagen",
                default_status=status,
                description=(
                    f"Weekly DSO<->DSA reconciliation ({job.name}) -- Saturday "
                    f"{cron.split()[1]}:{cron.split()[0].zfill(2)} CET (staggered)."
                ),
            )
        )
    return schedules
