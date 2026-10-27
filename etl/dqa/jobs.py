# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DQA jobs -- separated by track (FHIR vs DWH) and cadence (daily vs weekly).

One job per (track, cadence) that has at least one enabled check, e.g.
``dqa_fhir_daily_job`` / ``dqa_fhir_weekly_job``. DWH jobs appear automatically
once DWH subjects are added to the config. Selecting an anchor asset includes
the ``@asset_check``s bound to it, so each job runs exactly its track+cadence
checks.

- Daily jobs: cheap intra-table checks -- intended to run right after the ETL.
- Weekly jobs: heavy referential / reconciliation / chain checks -- Saturday.
"""

from __future__ import annotations

import dagster as dg
from dqa.config import ALL_SUBJECTS
from dqa.model import Cadence, DqaSubject, Track

_CADENCES = (Cadence.DAILY, Cadence.WEEKLY)

# How many checks run in parallel per DQA run. DQA checks push almost all work
# INTO the databases (COUNT / NOT EXISTS / cross-DB stages), so op-parallelism
# here means concurrent heavy queries — not pod CPU. We therefore override the
# global default executor (max_concurrent=8) with deliberately conservative caps,
# tiered by how heavy each check family is. Bump these after observing load on QA
# if there is headroom.
_DAILY_EXECUTOR = dg.multiprocess_executor.configured({"max_concurrent": 4})  # cheap intra-table
_WEEKLY_EXECUTOR = dg.multiprocess_executor.configured(
    {"max_concurrent": 3}  # referential / reconciliation, incl. cross-DB to Oracle + Aidbox
)
_ARC_EXECUTOR = dg.multiprocess_executor.configured(
    {"max_concurrent": 2}  # each arc check stages a large Oracle source (e.g. 14.4M-row Location)
)


def _executor_for(track: Track, cadence: Cadence):
    if cadence is Cadence.DAILY:
        return _DAILY_EXECUTOR
    if track is Track.ARCHIVE:
        return _ARC_EXECUTOR
    return _WEEKLY_EXECUTOR


def _anchor_keys(
    subjects: list[DqaSubject], track: Track, source: str | None, cadence: Cadence
) -> list[str]:
    return [
        f"dqa_{s.name}_{cadence.value}"
        for s in subjects
        if s.track is track
        and s.source == source
        and any(spec.enabled and spec.cadence is cadence for spec in s.checks)
    ]


def build_dqa_jobs(subjects: list[DqaSubject]) -> list[dg.UnresolvedAssetJobDefinition]:
    """One job per (track, source, cadence) that has >=1 enabled check.

    ``source`` splits a multi-source track's jobs (e.g. ARCHIVE -> dqa_arc_ifdb_*);
    subjects with ``source=None`` (fhir/dwh) keep the plain dqa_<track>_<cadence>_job.
    """
    jobs: list[dg.UnresolvedAssetJobDefinition] = []
    tracks = sorted({s.track for s in subjects}, key=lambda t: t.value)
    for track in tracks:
        # Distinct sources within the track, None-first for stable ordering.
        sources = sorted(
            {s.source for s in subjects if s.track is track},
            key=lambda x: (x is not None, x or ""),
        )
        for source in sources:
            seg = f"{track.value}_{source}" if source else track.value
            for cadence in _CADENCES:
                keys = _anchor_keys(subjects, track, source, cadence)
                if not keys:
                    continue
                jobs.append(
                    dg.define_asset_job(
                        name=f"dqa_{seg}_{cadence.value}_job",
                        selection=dg.AssetSelection.assets(*keys),
                        executor_def=_executor_for(track, cadence),
                        description=(
                            f"DQA {seg.upper()} {cadence.value} checks ({len(keys)} subjects)."
                        ),
                        tags={
                            "domain": "dqa",
                            "dqa_track": track.value,
                            "dqa_source": source or "",
                            "dqa_cadence": cadence.value,
                        },
                    )
                )
    return jobs


dqa_jobs = build_dqa_jobs(ALL_SUBJECTS)
