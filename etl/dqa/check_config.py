# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Per-run (runtime) configuration for DQA asset checks.

This module deliberately omits ``from __future__ import annotations``: that import
stringifies the field annotations, which breaks Dagster's ``dg.Config`` schema
resolution (the annotation would arrive as the string ``"int | None"``).
"""

import dagster as dg


class DqaCheckConfig(dg.Config):
    """Optional run config exposed on every DQA asset check.

    ``max_keys`` overrides the remediation-capture guard
    (``DQA_REMEDIATION_MAX_KEYS``, default 100k) for **this run only** -- set it in
    the Dagster Launchpad to let a check enqueue a backlog larger than the guard
    without editing the code-location environment. It only affects checks that
    enqueue to ``dqa.RemediationQueue`` (``source_reconciliation`` /
    ``archive_reconciliation`` with ``capture_replay``, ``referential_integrity``,
    ``patient_arc_integrity``); it is a harmless no-op on every other check. ``None`` -> use the env-var/default guard.

    NB: this is the *capture* guard only. The replay job has its own ``max_keys``
    config that must be raised separately before it will process that many keys.
    """

    max_keys: int | None = None
