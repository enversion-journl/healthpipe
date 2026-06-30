# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for build_sks_codesystem(): ext.sks_register rows → (concepts, properties, hierarchy)."""

from DSA.CodeSystem.sks.sks import build_sks_codesystem

COLUMNS = ["Code", "Display", "ParentCode", "classification", "Inactive"]


def _row(code, display, parent, classification, inactive="false"):
    """Build a tuple matching COLUMNS order."""
    return (code, display, parent, classification, inactive)


class TestBuildSksCodesystem:
    def test_empty_input(self):
        concepts, properties, hierarchy = build_sks_codesystem([], COLUMNS)
        assert concepts == []
        assert properties == []
        assert hierarchy == []

    def test_single_root_code(self):
        rows = [_row("DA01", "Diagnosis A01", "", "dia")]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)

        assert len(concepts) == 1
        assert concepts[0] == {"CODE": "DA01", "DISPLAY": "Diagnosis A01"}

        assert len(properties) == 1
        assert properties[0] == {
            "CODE": "DA01",
            "KEY": "classification",
            "VALUE": "dia",
            "TYPE": "String",
        }

        assert hierarchy == []

    def test_code_with_parent(self):
        rows = [_row("DA01", "Diag", "DA", "dia")]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)

        assert len(hierarchy) == 1
        assert hierarchy[0] == {"CODE": "DA01", "PARENT_CODE": "DA"}

    def test_multiple_codes(self):
        rows = [
            _row("DA", "Root diagnosis", "", "dia"),
            _row("DA01", "Sub diagnosis", "DA", "dia"),
            _row("KAFA", "Operation A", "", "opr"),
        ]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)

        assert len(concepts) == 3
        codes = {c["CODE"] for c in concepts}
        assert codes == {"DA", "DA01", "KAFA"}

        assert len(properties) == 3
        classifications = {p["VALUE"] for p in properties}
        assert classifications == {"dia", "opr"}

        assert len(hierarchy) == 1
        assert hierarchy[0]["CODE"] == "DA01"
        assert hierarchy[0]["PARENT_CODE"] == "DA"

    def test_multiple_classifications_for_same_code(self):
        rows = [
            _row("ZZ100", "Multi", "", "til"),
            _row("ZZ100", "Multi", "", "pro"),
        ]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)

        # Two concept entries (deduplicated later by merge_codesystem_tables)
        assert len(concepts) == 2
        classification_props = [p for p in properties if p["KEY"] == "classification"]
        assert {p["VALUE"] for p in classification_props} == {"til", "pro"}

    def test_inactive_code_gets_inactive_property(self):
        rows = [_row("OLD1", "Expired code", "", "dia", "true")]
        _, properties, _ = build_sks_codesystem(rows, COLUMNS)

        inactive_props = [p for p in properties if p["KEY"] == "inactive"]
        assert len(inactive_props) == 1
        assert inactive_props[0] == {
            "CODE": "OLD1",
            "KEY": "inactive",
            "VALUE": "true",
            "TYPE": "Boolean",
        }

    def test_active_code_no_inactive_property(self):
        rows = [_row("DA01", "Active code", "", "dia", "false")]
        _, properties, _ = build_sks_codesystem(rows, COLUMNS)
        assert all(p["KEY"] != "inactive" for p in properties)

    def test_blank_code_skipped(self):
        rows = [
            _row("", "Empty code row", "", "dia"),
            _row("DA01", "Valid", "", "dia"),
        ]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)
        assert len(concepts) == 1
        assert concepts[0]["CODE"] == "DA01"

    def test_missing_display_falls_back_to_code(self):
        rows = [_row("DA01", "", "", "dia")]
        concepts, _, _ = build_sks_codesystem(rows, COLUMNS)
        assert concepts[0]["DISPLAY"] == "DA01"

    def test_none_display_falls_back_to_code(self):
        rows = [_row("DA01", None, None, "dia")]
        concepts, _, _ = build_sks_codesystem(rows, COLUMNS)
        assert concepts[0]["DISPLAY"] == "DA01"

    def test_no_classification_no_property(self):
        rows = [_row("DA01", "Diag", "", "")]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)
        assert len(concepts) == 1
        assert properties == []

    def test_whitespace_handling(self):
        rows = [_row("  DA01  ", " Diagnosis ", " DA ", " dia ")]
        concepts, properties, hierarchy = build_sks_codesystem(rows, COLUMNS)
        assert concepts[0]["CODE"] == "DA01"
        assert concepts[0]["DISPLAY"] == "Diagnosis"
        assert properties[0]["VALUE"] == "dia"
        assert hierarchy[0]["PARENT_CODE"] == "DA"
