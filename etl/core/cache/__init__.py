# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared identifier cache: cross-process coordinated lookup storage.

See README.md in this package for when to adopt this and how.

Public API:
    SharedIdentifierCache  — SQLite-backed read-only lookup over (system, value) → id
    CacheCoordinator       — "build once, reuse by all workers" orchestrator
"""

from core.cache.coordinator import CacheCoordinator
from core.cache.sqlite_cache import SharedIdentifierCache

__all__ = ["CacheCoordinator", "SharedIdentifierCache"]
