# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SHAK CodeSystem builder and hierarchy derivation."""

import pytest
from DSA.CodeSystem.shak.shak import build_shak_codesystem, derive_shak_hierarchy
from DSA.CodeSystem.utils import merge_codesystem_tables

pytestmark = pytest.mark.unit


class TestDeriveShakHierarchy:
    def test_prefix_chain_7_6_4(self):
        codes = {"1355", "135501", "1355010"}
        edges = {(h["CODE"], h["PARENT_CODE"]) for h in derive_shak_hierarchy(codes)}
        assert ("1355010", "135501") in edges
        assert ("135501", "1355") in edges
        # 4-digit hospital code is a root.
        assert all(h["CODE"] != "1355" for h in derive_shak_hierarchy(codes))

    def test_suffix_parent(self):
        codes = {"600835", "600835S"}
        edges = {(h["CODE"], h["PARENT_CODE"]) for h in derive_shak_hierarchy(codes)}
        assert ("600835S", "600835") in edges

    def test_suffix_falls_back_to_prefix_when_base_absent(self):
        # numeric base 600835 absent -> fall back to 4-digit prefix 6008
        codes = {"6008", "600835S"}
        edges = {(h["CODE"], h["PARENT_CODE"]) for h in derive_shak_hierarchy(codes)}
        assert ("600835S", "6008") in edges

    def test_orphan_has_no_edge(self):
        codes = {"4202011"}  # no shorter prefix present
        assert derive_shak_hierarchy(codes) == []

    def test_no_dangling_parents(self):
        codes = {"1355", "135501", "1355010", "600835S"}
        parents = {h["PARENT_CODE"] for h in derive_shak_hierarchy(codes)}
        assert parents <= codes


class TestBuildShakCodesystem:
    def _rows(self):
        cols = [
            "code",
            "display",
            "SorCode",
            "SorDisplay",
            "isDirect",
            "institutionType",
            "validFrom",
        ]
        rows = [
            ("1355", "Sygehus", "111", "Org 111", "True", "Hovedsygehus", "2007-01-01 00:00:00"),
            ("135501", "Afdeling", "222", "Org 222", "True", "", ""),
        ]
        return rows, cols

    def test_concepts(self):
        rows, cols = self._rows()
        concepts, _, _ = build_shak_codesystem(rows, cols)
        by_code = {c["CODE"]: c["DISPLAY"] for c in concepts}
        assert by_code == {"1355": "Sygehus", "135501": "Afdeling"}

    def test_properties_skip_sorcode_and_blanks_and_type_date(self):
        rows, cols = self._rows()
        _, properties, _ = build_shak_codesystem(rows, cols)
        keys = {(p["CODE"], p["KEY"], p["TYPE"]) for p in properties}
        # SorCode/SorDisplay/isDirect are mapping columns, never SHAK properties.
        assert all(p["KEY"] not in ("SorCode", "SorDisplay", "isDirect") for p in properties)
        assert ("1355", "institutionType", "String") in keys
        assert ("1355", "validFrom", "Date") in keys
        # second row had blank institutionType/validFrom -> no property rows
        assert all(p["CODE"] != "135501" for p in properties)

    def test_hierarchy_derived_from_codes(self):
        rows, cols = self._rows()
        _, _, hierarchy = build_shak_codesystem(rows, cols)
        assert {"CODE": "135501", "PARENT_CODE": "1355"} in hierarchy

    def test_display_fallback_to_code(self):
        concepts, _, _ = build_shak_codesystem([("9999", "", "1")], ["code", "display", "SorCode"])
        assert concepts[0]["DISPLAY"] == "9999"


class _RecordingCursor:
    """Captures executed SQL so MERGE clauses can be asserted without a DB."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, *params):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)

    def nextset(self):
        return True

    def fetchone(self):
        # Counts row from _run_merge_with_action_counts: (inserted, updated, deleted)
        return (0, 0, 0)

    def fetchall(self):
        return []


class TestShakMergeSemantics:
    """cs_shak keeps stale concepts but refreshes properties/hierarchy snapshot-style."""

    def _merge_sql(self, cursor, table):
        return next(s for s in cursor.statements if f"[codesystem].[{table}]" in s and "MERGE" in s)

    def test_granular_delete_stale(self):
        cursor = _RecordingCursor()
        result = merge_codesystem_tables(
            cursor,
            "shak",
            [{"CODE": "1355", "DISPLAY": "Sygehus"}],
            [{"CODE": "1355", "KEY": "institutionType", "VALUE": "x", "TYPE": "String"}],
            [{"CODE": "135501", "PARENT_CODE": "1355"}],
            delete_stale_properties=True,
            delete_stale_hierarchy=True,
        )
        assert "WHEN NOT MATCHED BY SOURCE" not in self._merge_sql(cursor, "SHAKConcept")
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(cursor, "SHAKProperties")
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(cursor, "SHAKHierarchy")
        assert "concepts_deleted" not in result
        assert "properties_deleted" in result
        assert "hierarchy_deleted" in result
