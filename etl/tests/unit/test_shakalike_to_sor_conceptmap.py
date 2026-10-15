# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SHAK-a-like -> SOR ConceptMap builder.

Covers in-place SOR identifier updates (no source timestamp): the snapshot
builder emits the new target, and MERGE uses delete_stale so (code, old SOR)
is removed.
"""

import pytest
from DSA.ConceptMap.shakalike_to_sor.shakalike_to_sor import (
    build_shakalike_to_sor_mappings,
    cm_shakalike_to_sor,
)
from DSA.ConceptMap.utils import merge_conceptmap_table

pytestmark = pytest.mark.unit


def test_cm_shakalike_to_sor_reads_from_archive_not_ext():
    # cs_shakalike reads arc.epji_location (dw_is_deleted = 0) as the
    # persistent source of truth; this asset must use the same entry point
    # rather than ext.epji_location directly, so a future change to
    # deletion semantics only has one place to update.
    dep_keys = {key.to_user_string() for key in cm_shakalike_to_sor.dependency_keys}
    assert "arc_epji_shakalike" in dep_keys
    assert "extract_epji_shakalike" not in dep_keys


class TestBuildShakalikeToSorMappings:
    def test_row_shape(self):
        rows = build_shakalike_to_sor_mappings([("4202011", "Medicinsk Afd", "215811000016008")])
        assert rows == [("4202011", "Medicinsk Afd", "equivalent", "215811000016008", None)]

    def test_unmapped_rows_dropped(self):
        rows = build_shakalike_to_sor_mappings([("1", "A", ""), ("2", "B", "20")])
        assert [r[0] for r in rows] == ["2"]

    def test_sor_identifier_change_emits_new_target(self):
        """In-place update of location_identifier.value: same code, new SOR."""
        before = build_shakalike_to_sor_mappings([("4202011", "Afd", "111")])
        after = build_shakalike_to_sor_mappings([("4202011", "Afd", "222")])
        assert before[0][3] == "111"
        assert after[0][3] == "222"
        assert after[0][0] == before[0][0]


class _RecordingCursor:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, *params):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)

    def fetchall(self):
        return []


class TestMergeShakalikeDeleteStale:
    _ROWS = [("1", None, "equivalent", "10", None)]

    def _merge_sql(self, cursor):
        return next(s for s in cursor.statements if "MERGE INTO [conceptmap].[SHAKAlikeToSOR]" in s)

    def test_delete_stale_adds_delete_clause(self):
        cursor = _RecordingCursor()
        result = merge_conceptmap_table(cursor, "SHAKAlikeToSOR", self._ROWS, delete_stale=True)
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(cursor)
        assert "deleted" in result

    def test_default_keeps_stale_rows(self):
        cursor = _RecordingCursor()
        result = merge_conceptmap_table(cursor, "SHAKAlikeToSOR", self._ROWS)
        assert "WHEN NOT MATCHED BY SOURCE" not in self._merge_sql(cursor)
        assert "deleted" not in result
