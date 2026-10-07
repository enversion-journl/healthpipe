# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SHAK->SOR ConceptMap builder and FHIR resource assembly."""

import pytest
from DSA.ConceptMap.shak_to_sor.shak_to_sor import build_shak_to_sor_mappings
from DSA.ConceptMap.utils import merge_conceptmap_table
from infrastructure.assets.fhir_conceptmap_shak_to_sor import build_fhir_conceptmap

pytestmark = pytest.mark.unit


class TestBuildShakToSorMappings:
    def test_row_shape_and_target_display(self):
        # (Code, display, SorCode, SorDisplay, isDirect) — target display comes
        # straight from ext.shaksor_map (SorEntityName), so it works for closed
        # SOR codes.
        shak_rows = [("4202011", "Medicinsk Afd", "215811000016008", "Sygehus Lillebaelt", "True")]
        rows = build_shak_to_sor_mappings(shak_rows)
        assert rows == [
            (
                "4202011",
                "Medicinsk Afd",
                "equivalent",
                "215811000016008",
                "Sygehus Lillebaelt",
            )
        ]

    def test_one_row_per_shak_code(self):
        shak_rows = [
            ("1", "A", "10", "Org A", "True"),
            ("2", "B", "20", "Org B", "True"),
            ("3", "C", "30", "Org C", "True"),
        ]
        rows = build_shak_to_sor_mappings(shak_rows)
        assert len(rows) == 3
        assert {r[0] for r in rows} == {"1", "2", "3"}

    def test_indirect_fallback_is_inexact(self):
        # A SHAK code whose only candidates were indirect keeps its mapping but
        # is downgraded to 'inexact' so consumers can tell it apart.
        rows = build_shak_to_sor_mappings([("5000", "Afd", "99", "Org", "False")])
        assert rows[0][2] == "inexact"

    def test_closed_target_keeps_display(self):
        # Even when the SOR org is closed (not in codesystem/ref), the name from
        # SHAKSORMap is retained as TargetDisplay.
        rows = build_shak_to_sor_mappings(
            [("6650", "Hospitalsenheden Vest", "381131000016004", "Hospitalsenheden Vest", "True")]
        )
        assert rows[0][4] == "Hospitalsenheden Vest"

    def test_missing_target_display_is_none(self):
        rows = build_shak_to_sor_mappings([("1", "A", "10", "", "True")])
        assert rows[0][4] is None

    def test_rows_without_target_dropped(self):
        rows = build_shak_to_sor_mappings(
            [("1", "A", "", "X", "True"), ("2", "B", "20", "Y", "True")]
        )
        assert [r[0] for r in rows] == ["2"]


class _RecordingCursor:
    """Captures executed SQL so MERGE clauses can be asserted without a DB."""

    def __init__(self):
        self.statements: list[str] = []

    def execute(self, sql, *params):
        self.statements.append(sql)

    def executemany(self, sql, rows):
        self.statements.append(sql)

    def fetchall(self):
        return []


class TestMergeConceptmapDeleteStale:
    _ROWS = [("1", None, "equivalent", "10", None)]

    def _merge_sql(self, cursor):
        return next(s for s in cursor.statements if "MERGE INTO [conceptmap].[SHAKToSOR]" in s)

    def test_delete_stale_adds_delete_clause(self):
        cursor = _RecordingCursor()
        result = merge_conceptmap_table(cursor, "SHAKToSOR", self._ROWS, delete_stale=True)
        assert "WHEN NOT MATCHED BY SOURCE THEN DELETE" in self._merge_sql(cursor)
        assert "deleted" in result

    def test_default_keeps_stale_rows(self):
        cursor = _RecordingCursor()
        result = merge_conceptmap_table(cursor, "SHAKToSOR", self._ROWS)
        assert "WHEN NOT MATCHED BY SOURCE" not in self._merge_sql(cursor)
        assert "deleted" not in result


class TestBuildFhirConceptmap:
    META = {
        "id": "shak-to-sor",
        "url": "https://example.org/fhir/ConceptMap/shak-to-sor",
        "name": "SHAKToSor",
        "title": "SHAK to SOR",
        "status": "active",
        "description": "desc",
        "sourceUri": "urn:dk:shak",
        "targetUri": "https://sundhedsdatastyrelsen.dk/sor",
    }

    def test_structure(self):
        rows = [("4202011", "Medicinsk Afd", "equivalent", "215811000016008", "SLB")]
        cm = build_fhir_conceptmap(self.META, rows)
        assert cm["resourceType"] == "ConceptMap"
        assert cm["id"] == "shak-to-sor"
        assert cm["sourceUri"] == "urn:dk:shak"
        assert cm["targetUri"] == "https://sundhedsdatastyrelsen.dk/sor"
        group = cm["group"][0]
        assert group["source"] == "urn:dk:shak"
        element = group["element"][0]
        assert element["code"] == "4202011"
        assert element["display"] == "Medicinsk Afd"
        target = element["target"][0]
        assert target["code"] == "215811000016008"
        assert target["equivalence"] == "equivalent"
        assert target["display"] == "SLB"

    def test_unknown_relationship_defaults_equivalent(self):
        rows = [("1", "A", "weird", "10", None)]
        cm = build_fhir_conceptmap(self.META, rows)
        assert cm["group"][0]["element"][0]["target"][0]["equivalence"] == "equivalent"

    def test_rows_without_codes_skipped(self):
        rows = [("", "A", "equal", "10", None), ("2", "B", "equal", "", None)]
        cm = build_fhir_conceptmap(self.META, rows)
        assert cm["group"][0]["element"] == []
