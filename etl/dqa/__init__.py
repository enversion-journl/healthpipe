# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Data Quality Assurance (DQA) framework.

Config-driven data-quality checks: one ``DqaSubject`` per primary table, a small
registry of reusable check types, and a factory that emits one Dagster anchor
asset per (subject, cadence) plus the bound ``@asset_check``s.

``definitions.py`` wires this in via::

    from dqa import dqa_assets, dqa_asset_checks

Design docs: ``etl/docs/architecture/dqa/``.
"""

from dqa.config import ALL_SUBJECTS
from dqa.dso import build_dso_all
from dqa.factory import build_all
from dqa.schedules import build_dso_weekly_schedules

# Built once at import: the Dagster anchor assets + asset checks for all subjects.
dqa_assets, dqa_asset_checks = build_all(ALL_SUBJECTS)

# DSO-vs-DSA reconciliation subsystem (own anchors + checks + per-domain jobs),
# derived from the DSO-sync configs. Kept separate from the daily/weekly jobs.
dso_assets, dso_asset_checks, dso_jobs = build_dso_all()

# Staggered Saturday crons for the DSO reconciliation domains (fhir excluded --
# it is sensor-triggered alongside the FHIR weekly checks). The per-track daily /
# weekly DQA jobs are sensor-triggered (see sensors.py), not cron'd.
dqa_schedules = build_dso_weekly_schedules(dso_jobs)

__all__ = [
    "dqa_assets",
    "dqa_asset_checks",
    "dso_assets",
    "dso_asset_checks",
    "dso_jobs",
    "dqa_schedules",
]
