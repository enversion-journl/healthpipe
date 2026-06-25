# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the content-complete SOR CodeSystem dict assembly.

The generic tree-building helpers (_build_concept_lookup, _build_concept_tree)
are already tested in test_sks_codesystem_complete.py — these tests focus on
SOR-specific property definitions and CodeSystem structure.
"""

from infrastructure.assets.fhir_codesystem_sks_complete import (
    _build_concept_lookup,
    _build_concept_tree,
)
from infrastructure.assets.fhir_codesystem_sor_complete import SOR_PROPERTY_DEFINITIONS


class TestSorPropertyDefinitions:
    def test_has_15_properties(self):
        assert len(SOR_PROPERTY_DEFINITIONS) == 15

    def test_all_have_required_fields(self):
        for prop in SOR_PROPERTY_DEFINITIONS:
            assert "code" in prop
            assert "description" in prop
            assert "type" in prop

    def test_date_properties_use_datetime_type(self):
        date_props = [
            p for p in SOR_PROPERTY_DEFINITIONS if p["code"] in ("openingDate", "closingDate")
        ]
        assert len(date_props) == 2
        for prop in date_props:
            assert prop["type"] == "dateTime"

    def test_non_date_properties_use_string_type(self):
        non_date_props = [
            p for p in SOR_PROPERTY_DEFINITIONS if p["code"] not in ("openingDate", "closingDate")
        ]
        assert len(non_date_props) == 13
        for prop in non_date_props:
            assert prop["type"] == "string"

    def test_expected_property_codes(self):
        codes = {p["code"] for p in SOR_PROPERTY_DEFINITIONS}
        expected = {
            "entityType",
            "entityTypeId",
            "municipality",
            "region",
            "owner",
            "ownerId",
            "cvrNumber",
            "website",
            "email",
            "phone",
            "address",
            "postalCode",
            "city",
            "openingDate",
            "closingDate",
        }
        assert codes == expected


class TestSorFullCodesystemDict:
    def _build_codesystem(self, concepts, props, hierarchy):
        lookup = _build_concept_lookup(concepts, props)
        concept_tree = _build_concept_tree(lookup, hierarchy)
        return {
            "resourceType": "CodeSystem",
            "id": "sor",
            "url": "https://sundhedsdatastyrelsen.dk/sor",
            "name": "SOR",
            "content": "complete",
            "count": len(lookup),
            "property": SOR_PROPERTY_DEFINITIONS,
            "concept": concept_tree,
        }

    def test_structure_has_correct_keys(self):
        cs = self._build_codesystem(
            [("21000016006", "Region Syddanmark"), ("21000016007", "OUH")],
            [("21000016006", "entityType", "Region", "String")],
            [],
        )

        assert cs["resourceType"] == "CodeSystem"
        assert cs["id"] == "sor"
        assert cs["content"] == "complete"
        assert cs["url"] == "https://sundhedsdatastyrelsen.dk/sor"
        assert cs["name"] == "SOR"
        assert len(cs["property"]) == 15
        assert isinstance(cs["concept"], list)

    def test_count_matches_total_concepts(self):
        concepts = [(f"SOR{i:05d}", f"Org {i}") for i in range(25)]
        cs = self._build_codesystem(concepts, [], [])

        assert cs["count"] == 25
        assert len(cs["concept"]) == 25

    def test_hierarchy_creates_nested_tree(self):
        cs = self._build_codesystem(
            [("R1", "Region"), ("H1", "Hospital"), ("A1", "Afdeling")],
            [("R1", "entityType", "Region", "String"), ("H1", "entityType", "Sygehus", "String")],
            [("R1", "H1"), ("H1", "A1")],
        )

        assert cs["count"] == 3
        assert len(cs["concept"]) == 1
        root = cs["concept"][0]
        assert root["code"] == "R1"
        assert len(root["concept"]) == 1
        hospital = root["concept"][0]
        assert hospital["code"] == "H1"
        assert len(hospital["concept"]) == 1
        assert hospital["concept"][0]["code"] == "A1"

    def test_properties_preserved_in_concepts(self):
        cs = self._build_codesystem(
            [("21000016006", "Region Syddanmark")],
            [
                ("21000016006", "entityType", "Region", "String"),
                ("21000016006", "municipality", "Odense", "String"),
            ],
            [],
        )

        concept = cs["concept"][0]
        assert len(concept["property"]) == 2
        prop_codes = {p["code"] for p in concept["property"]}
        assert prop_codes == {"entityType", "municipality"}

    def test_date_property_emitted_as_value_datetime(self):
        """Date-typed property rows must produce valueDateTime, not valueString."""
        cs = self._build_codesystem(
            [("SOR1", "Test Org")],
            [("SOR1", "openingDate", "2020-01-01", "Date")],
            [],
        )

        concept = cs["concept"][0]
        assert len(concept["property"]) == 1
        prop = concept["property"][0]
        assert prop["code"] == "openingDate"
        assert "valueDateTime" in prop, f"Expected valueDateTime, got keys: {list(prop.keys())}"
        assert prop["valueDateTime"] == "2020-01-01"
        assert "valueString" not in prop

    def test_datetime_type_variant_also_emits_value_datetime(self):
        """Type 'dateTime' (mixed-case) is also dispatched to valueDateTime."""
        cs = self._build_codesystem(
            [("SOR2", "Another Org")],
            [("SOR2", "closingDate", "2023-12-31", "dateTime")],
            [],
        )

        concept = cs["concept"][0]
        prop = concept["property"][0]
        assert prop["code"] == "closingDate"
        assert "valueDateTime" in prop
        assert prop["valueDateTime"] == "2023-12-31"
        assert "valueString" not in prop

    def test_sql_datetime_value_normalized_to_valid_fhir_date(self):
        """SQL datetime values (space separator + time) must be normalized to a
        valid FHIR date. Aidbox rejects 'YYYY-MM-DD HH:MM:SS' as an invalid
        dateTime ('invalid time separator' / 'no timezone'), which previously
        broke the whole SOR CodeSystem PUT."""
        cs = self._build_codesystem(
            [("SOR3", "Org")],
            [("SOR3", "closingDate", "2041-05-10 00:00:00", "Date")],
            [],
        )
        prop = cs["concept"][0]["property"][0]
        assert prop["code"] == "closingDate"
        assert (
            " " not in prop["valueDateTime"]
        ), f"valueDateTime must be valid FHIR (no space): {prop['valueDateTime']!r}"
        assert prop["valueDateTime"] == "2041-05-10"
        assert "valueString" not in prop

    def test_unparseable_date_value_is_skipped_not_emitted(self):
        """A date-typed value that is not a real date must be dropped, not emitted
        as an invalid valueDateTime — one bad row otherwise fails the entire PUT.
        Other properties on the same concept are unaffected."""
        cs = self._build_codesystem(
            [("SOR4", "Org")],
            [
                ("SOR4", "closingDate", "not-a-date", "Date"),
                ("SOR4", "entityType", "privat", "String"),
            ],
            [],
        )
        codes = {p["code"] for p in cs["concept"][0].get("property", [])}
        assert "closingDate" not in codes, "unparseable date must be skipped"
        assert "entityType" in codes, "other properties must be preserved"

    def test_empty_and_none_date_values_are_skipped(self):
        """Empty / NULL date values must not produce 'valueDateTime': '' or 'None'."""
        for bad in ("", None):
            cs = self._build_codesystem(
                [("SOR5", "Org")],
                [("SOR5", "closingDate", bad, "Date")],
                [],
            )
            props = cs["concept"][0].get("property", [])
            assert all(
                p["code"] != "closingDate" for p in props
            ), f"date value {bad!r} must be skipped, got: {props}"
