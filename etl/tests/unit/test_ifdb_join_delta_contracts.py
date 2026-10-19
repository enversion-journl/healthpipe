# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Et afgrænset IFDB-udtræk må aldrig slette rækker uden for udtrækket.

Its ext table holds only the rows whose parent moved inside the delta window, so
NOT MATCHED BY SOURCE would soft-delete everything outside it -- for Location
that is ~14.9M rows on the first nightly run, silently, with no error.
"""

from unittest.mock import MagicMock, patch

import pytest
from core.archive_merge import execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
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

    if archive.source_table != "ifdb_pas_diagnose_henvisning_ref":
        assert not archive.detect_deletes
    assert not archive.upstream_is_full_load


@pytest.mark.parametrize(
    "mode,source_rows,expected",
    [
        ("delta", 100, False),
        ("period", 100, False),
        ("ids", 100, False),
        ("full_load", 100, True),
        ("full_load", 0, False),
    ],
)
def test_referral_link_cleanup_requires_a_nonempty_complete_extract(mode, source_rows, expected):
    cfg = ifdb_archive.PAS_DIAGNOSE_HENVISNING_REF_ARCHIVE_CONFIG
    conn, cur, resource, context, manager = [MagicMock() for _ in range(5)]
    resource.get_connection.return_value.__enter__.return_value = conn
    conn.cursor.return_value = cur
    manager.start_dfm_run.return_value.log_id = 1
    manager.get_checkpoint.return_value = None

    def fetchone():
        sql = cur.execute.call_args.args[0]
        if "SELECT TOP (1) 1" in sql:
            return (1,)  # Existing archive: exercise MERGE, not the empty-target fast path.
        if "SELECT COUNT(*) -" in sql:
            return (0,)  # No duplicate source keys.
        if sql.startswith("SELECT COUNT(*)"):
            return (source_rows,)
        return (5, 3, 0)

    cur.fetchone.side_effect = fetchone
    run = RunModeConfig(
        mode=mode,
        date_from="2026-01-01",
        date_to="2026-01-02",
        id_filter_column="DIAGNOSE_ID",
        id_filter_values=["SYNTHETIC"],
        advance_checkpoint=False,
    )
    with patch("core.archive_merge.DfmManager", return_value=manager):
        result = execute_archive_merge(context, resource, cfg, run)
    merge = next(
        c.args[0]
        for c in cur.execute.call_args_list
        if "MERGE [arc].[ifdb_pas_diagnose_henvisning_ref]" in c.args[0]
    )
    assert ("WHEN NOT MATCHED BY SOURCE" in merge) is expected
    assert result.detect_deletes is expected
    manager.set_checkpoint.assert_not_called()
