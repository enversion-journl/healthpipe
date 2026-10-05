# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the content-complete SKS CodeSystem tree-building logic.

Tests _build_concept_lookup() and _build_concept_tree() which convert
flat staging table rows into a recursive FHIR CodeSystem concept tree.
"""

import pytest
from infrastructure.assets.fhir_codesystem_sks_complete import (
    SKS_PROPERTY_DEFINITIONS,
    _build_concept_lookup,
    _build_concept_tree,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# _build_concept_lookup
# ---------------------------------------------------------------------------


class TestBuildConceptLookup:
    def test_basic(self):
        """Concepts and properties are indexed correctly."""
        concepts = [("DA01", "Kolera"), ("DA02", "Tyfus")]
        props = [
            ("DA01", "classification", "dia", "String"),
            ("DA02", "classification", "dia", "String"),
        ]

        lookup = _build_concept_lookup(concepts, props)

        assert len(lookup) == 2
        assert lookup["DA01"]["code"] == "DA01"
        assert lookup["DA01"]["display"] == "Kolera"
        assert lookup["DA01"]["property"] == [{"code": "classification", "valueString": "dia"}]

    def test_missing_display_falls_back_to_code(self):
        """When display is None or empty, code is used as display."""
        concepts = [("X01", None), ("X02", "")]
        props = []

        lookup = _build_concept_lookup(concepts, props)

        assert lookup["X01"]["display"] == "X01"
        assert lookup["X02"]["display"] == "X02"

    def test_no_properties(self):
        """Concepts without properties have no 'property' key."""
        concepts = [("A01", "Test concept")]
        props = []

        lookup = _build_concept_lookup(concepts, props)

        assert "property" not in lookup["A01"]
        assert lookup["A01"]["code"] == "A01"
        assert lookup["A01"]["display"] == "Test concept"

    def test_empty_code_skipped(self):
        """Rows with empty or whitespace-only codes are skipped."""
        concepts = [("", "Empty"), ("  ", "Whitespace"), ("A01", "Valid")]
        props = [("", "classification", "x", "String")]

        lookup = _build_concept_lookup(concepts, props)

        assert len(lookup) == 1
        assert "A01" in lookup

    def test_boolean_property_emitted_as_value_boolean(self):
        """Boolean-typed properties become valueBoolean, not valueString."""
        concepts = [("OLD1", "Expired code")]
        props = [("OLD1", "inactive", "true", "Boolean")]

        lookup = _build_concept_lookup(concepts, props)

        assert lookup["OLD1"]["property"] == [{"code": "inactive", "valueBoolean": True}]

    def test_multiple_classifications_per_code(self):
        """A code in several classifications carries one property per value."""
        concepts = [("ZZ100", "Multi")]
        props = [
            ("ZZ100", "classification", "pro", "String"),
            ("ZZ100", "classification", "til", "String"),
        ]

        lookup = _build_concept_lookup(concepts, props)

        assert lookup["ZZ100"]["property"] == [
            {"code": "classification", "valueString": "pro"},
            {"code": "classification", "valueString": "til"},
        ]

    def test_mixed_string_and_boolean_properties(self):
        """A code can carry classification(s) plus the inactive flag."""
        concepts = [("OLD1", "Expired")]
        props = [
            ("OLD1", "classification", "dia", "String"),
            ("OLD1", "inactive", "true", "Boolean"),
        ]

        lookup = _build_concept_lookup(concepts, props)

        assert lookup["OLD1"]["property"] == [
            {"code": "classification", "valueString": "dia"},
            {"code": "inactive", "valueBoolean": True},
        ]


# ---------------------------------------------------------------------------
# _build_concept_tree
# ---------------------------------------------------------------------------


class TestBuildConceptTree:
    def test_flat_no_hierarchy(self):
        """All concepts are roots when there is no hierarchy."""
        lookup = {
            "A": {"code": "A", "display": "Alpha"},
            "B": {"code": "B", "display": "Beta"},
            "C": {"code": "C", "display": "Charlie"},
        }
        hierarchy = []

        tree = _build_concept_tree(lookup, hierarchy)

        assert len(tree) == 3
        codes = [n["code"] for n in tree]
        assert codes == ["A", "B", "C"]
        for node in tree:
            assert "concept" not in node

    def test_simple_hierarchy(self):
        """Parent-child nesting works for a single level."""
        lookup = {
            "DA": {"code": "DA", "display": "Diagnoser"},
            "DA01": {"code": "DA01", "display": "Kolera"},
            "DA02": {"code": "DA02", "display": "Tyfus"},
        }
        hierarchy = [("DA", "DA01"), ("DA", "DA02")]

        tree = _build_concept_tree(lookup, hierarchy)

        assert len(tree) == 1
        root = tree[0]
        assert root["code"] == "DA"
        assert len(root["concept"]) == 2
        assert root["concept"][0]["code"] == "DA01"
        assert root["concept"][1]["code"] == "DA02"

    def test_multi_parent_child_emitted_once(self):
        """A code reachable from multiple parents (or via a duplicated
        (parent, child) row) must appear EXACTLY ONCE in the tree. Aidbox's
        csd-1 invariant requires every code in a CodeSystem (concept +
        descendants) to be distinct; a FHIR concept tree is a tree, not a DAG.
        """
        lookup = {
            "A": {"code": "A", "display": "A"},
            "B": {"code": "B", "display": "B"},
            "C": {"code": "C", "display": "C"},  # child of BOTH A and B
        }
        # C under two parents, plus a duplicated (A, C) row
        hierarchy = [("A", "C"), ("B", "C"), ("A", "C")]

        tree = _build_concept_tree(lookup, hierarchy)

        seen: list[str] = []

        def _walk(nodes):
            for n in nodes:
                seen.append(n["code"])
                _walk(n.get("concept", []))

        _walk(tree)

        assert len(seen) == len(set(seen)), f"duplicate codes in tree: {seen}"
        assert seen.count("C") == 1, f"C must appear once, got {seen.count('C')}"
        assert set(seen) == {"A", "B", "C"}, f"every code present once: {seen}"

    def test_multi_level_nesting(self):
        """Three-level nesting (grandparent -> parent -> child)."""
        lookup = {
            "D": {"code": "D", "display": "Top"},
            "DA": {"code": "DA", "display": "Mid"},
            "DA01": {"code": "DA01", "display": "Leaf"},
        }
        hierarchy = [("D", "DA"), ("DA", "DA01")]

        tree = _build_concept_tree(lookup, hierarchy)

        assert len(tree) == 1
        assert tree[0]["code"] == "D"
        assert len(tree[0]["concept"]) == 1
        mid = tree[0]["concept"][0]
        assert mid["code"] == "DA"
        assert len(mid["concept"]) == 1
        assert mid["concept"][0]["code"] == "DA01"
        assert "concept" not in mid["concept"][0]

    def test_orphan_parent_ignored(self):
        """Hierarchy references to codes not in lookup are silently ignored."""
        lookup = {
            "A": {"code": "A", "display": "Alpha"},
            "B": {"code": "B", "display": "Beta"},
        }
        # "MISSING" is not in lookup
        hierarchy = [("MISSING", "A"), ("MISSING", "B")]

        tree = _build_concept_tree(lookup, hierarchy)

        # A and B should remain roots since parent is missing
        assert len(tree) == 2
        assert tree[0]["code"] == "A"
        assert tree[1]["code"] == "B"

    def test_deterministic_order(self):
        """Children are sorted by code, and roots are sorted by code."""
        lookup = {
            "Z": {"code": "Z", "display": "Zulu"},
            "A": {"code": "A", "display": "Alpha"},
            "M": {"code": "M", "display": "Mike"},
            "Z01": {"code": "Z01", "display": "Z-child-1"},
            "Z03": {"code": "Z03", "display": "Z-child-3"},
            "Z02": {"code": "Z02", "display": "Z-child-2"},
        }
        hierarchy = [("Z", "Z03"), ("Z", "Z01"), ("Z", "Z02")]

        tree = _build_concept_tree(lookup, hierarchy)

        root_codes = [n["code"] for n in tree]
        assert root_codes == ["A", "M", "Z"]
        z_node = tree[2]
        child_codes = [c["code"] for c in z_node["concept"]]
        assert child_codes == ["Z01", "Z02", "Z03"]

    def test_properties_preserved_in_tree(self):
        """Properties from the lookup are preserved through tree building."""
        lookup = {
            "P": {"code": "P", "display": "Parent"},
            "C": {
                "code": "C",
                "display": "Child",
                "property": [{"code": "classification", "valueString": "diagnosis"}],
            },
        }
        hierarchy = [("P", "C")]

        tree = _build_concept_tree(lookup, hierarchy)

        child = tree[0]["concept"][0]
        assert child["property"] == [{"code": "classification", "valueString": "diagnosis"}]


# ---------------------------------------------------------------------------
# Full CodeSystem dict structure
# ---------------------------------------------------------------------------


class TestFullCodesystemDict:
    def _build_codesystem(self, concepts, props, hierarchy):
        """Helper to build a full CodeSystem dict (same logic as the asset)."""
        lookup = _build_concept_lookup(concepts, props)
        concept_tree = _build_concept_tree(lookup, hierarchy)
        return {
            "resourceType": "CodeSystem",
            "id": "sks",
            "url": "urn:oid:1.2.208.176.2.4",
            "name": "SKS",
            "content": "complete",
            "count": len(lookup),
            "property": SKS_PROPERTY_DEFINITIONS,
            "concept": concept_tree,
        }

    def test_structure_has_correct_keys(self):
        """Full CodeSystem dict has the expected top-level keys."""
        cs = self._build_codesystem(
            [("A", "Alpha"), ("B", "Beta")],
            [("A", "classification", "dia", "String")],
            [],
        )

        assert cs["resourceType"] == "CodeSystem"
        assert cs["id"] == "sks"
        assert cs["content"] == "complete"
        assert cs["url"] == "urn:oid:1.2.208.176.2.4"
        # Must match the shared constant exactly: classification + inactive
        assert len(cs["property"]) == 2
        prop_codes = {p["code"] for p in cs["property"]}
        assert prop_codes == {"classification", "inactive"}
        assert cs["property"] == SKS_PROPERTY_DEFINITIONS
        assert isinstance(cs["concept"], list)

    def test_count_matches_total_concepts(self):
        """count field matches the total number of concepts."""
        concepts = [(f"C{i:03d}", f"Concept {i}") for i in range(50)]
        cs = self._build_codesystem(concepts, [], [])

        assert cs["count"] == 50
        assert len(cs["concept"]) == 50  # all roots when no hierarchy
