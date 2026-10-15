# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SHAK-a-like CodeSystem builder."""

import pytest
from DSA.CodeSystem.shakalike.shakalike import (
    build_shakalike_codesystem,
    derive_shakalike_hierarchy,
)
from DSA.CodeSystem.utils import merge_codesystem_tables

pytestmark = pytest.mark.unit


class TestBuildShakalikeCodesystem:
    def _rows(self):
        cols = ["location_pk", "code", "display", "sor_code", "identifier_system"]
        rows = [
            ("1", "01.02", "Sygehus", "111", "http://example/SorIdentifier"),
            ("2", "01.02.01", "Afdeling", "222", "http://example/SorIdentifier"),
        ]
        return rows, cols

    def test_concepts(self):
        rows, cols = self._rows()
        concepts, _, _ = build_shakalike_codesystem(rows, cols)
        by_code = {c["CODE"]: c["DISPLAY"] for c in concepts}
        assert by_code == {"01.02": "Sygehus", "01.02.01": "Afdeling"}

    def test_sor_code_is_not_a_property(self):
        rows, cols = self._rows()
        _, properties, _ = build_shakalike_codesystem(rows, cols)
        assert all(p["KEY"] != "sor_code" for p in properties)
        keys = {(p["CODE"], p["KEY"]) for p in properties}
        assert ("01.02", "location_pk") in keys
        assert ("01.02", "identifier_system") in keys

    def test_hierarchy_derived_from_codes(self):
        rows, cols = self._rows()
        _, _, hierarchy = build_shakalike_codesystem(rows, cols)
        assert {"CODE": "01.02.01", "PARENT_CODE": "01.02"} in hierarchy

    def test_hierarchy_leaves_root_code_without_parent(self):
        _, _, hierarchy = build_shakalike_codesystem(
            [("1", "01.02.01.4.1", "Leaf", "", "")],
            ["location_pk", "code", "display", "sor_code", "identifier_system"],
        )
        assert hierarchy == []

    def test_hierarchy_walks_past_missing_intermediate_level(self):
        # "01.02.01.4" is missing; the code must still attach to "01.02.01"
        # rather than becoming an orphaned root.
        codes = {"01", "01.02", "01.02.01", "01.02.01.4.1"}
        hierarchy = derive_shakalike_hierarchy(codes)
        assert {"CODE": "01.02.01.4.1", "PARENT_CODE": "01.02.01"} in hierarchy
        assert {"CODE": "01.02.01", "PARENT_CODE": "01.02"} in hierarchy
        assert {"CODE": "01.02", "PARENT_CODE": "01"} in hierarchy
        assert len(hierarchy) == 3

    def test_display_fallback_to_code(self):
        concepts, _, _ = build_shakalike_codesystem(
            [("1", "9999", "", "10", "")],
            ["location_pk", "code", "display", "sor_code", "identifier_system"],
        )
        assert concepts[0]["DISPLAY"] == "9999"


class _RecordingCursor:
    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, *params):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)

    def nextset(self):
        return True

    def fetchone(self):
        return (0, 0, 0)

    def fetchall(self):
        return []


class TestShakalikeMergeSemantics:
    def _merge_sql(self, cursor, table):
        return next(s for s in cursor.statements if f"[codesystem].[{table}]" in s and "MERGE" in s)

    def test_granular_delete_stale(self):
        cursor = _RecordingCursor()
        result = merge_codesystem_tables(
            cursor,
            "shakalike",
            [{"CODE": "1355", "DISPLAY": "Sygehus"}],
            [{"CODE": "1355", "KEY": "location_pk", "VALUE": "1", "TYPE": "String"}],
            [{"CODE": "135501", "PARENT_CODE": "1355"}],
            delete_stale_properties=True,
            delete_stale_hierarchy=True,
        )
        assert "WHEN NOT MATCHED BY SOURCE" not in self._merge_sql(cursor, "SHAKAlikeConcept")
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(
            cursor, "SHAKAlikeProperties"
        )
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(
            cursor, "SHAKAlikeHierarchy"
        )
        assert "concepts_deleted" not in result
        assert "properties_deleted" in result
        assert "hierarchy_deleted" in result
