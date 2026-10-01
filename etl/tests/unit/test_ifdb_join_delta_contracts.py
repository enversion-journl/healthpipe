# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""A join-delta IFDB extract must never be paired with archive delete detection.

Its ext table holds only the rows whose parent moved inside the delta window, so
NOT MATCHED BY SOURCE would soft-delete everything outside it -- for Location
that is ~14.9M rows on the first nightly run, silently, with no error.
"""

import pytest
from core.dfm_config import ArchiveMergeConfig
from DSI.oracle import ifdb_archive
from DSI.oracle.ifdb_extract import IFDB_EXTRACT_CONFIGS

pytestmark = pytest.mark.unit

# ifdb_archive exposes one module-level config per table rather than a list.
_ARCHIVE_CONFIGS = [v for v in vars(ifdb_archive).values() if isinstance(v, ArchiveMergeConfig)]

_JOIN_DELTA = [
    c
    for c in IFDB_EXTRACT_CONFIGS
    if not c.is_full_load and c.select_from_expr and "JOIN" in c.select_from_expr.upper()
]


def test_the_fixture_matches_something():
    """Guard the guard: an empty parametrize list would pass without asserting."""
    assert _JOIN_DELTA


@pytest.mark.parametrize("extract", _JOIN_DELTA, ids=lambda c: c.target_table)
def test_join_delta_archive_never_detects_deletes(extract):
    archive = next(a for a in _ARCHIVE_CONFIGS if a.source_table == extract.target_table)

    assert not archive.detect_deletes
    assert not archive.upstream_is_full_load
