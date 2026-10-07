# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SOR CodeSystem builder — activeOwnOrganization property.

The property marks entities that belong to Example Region's own current
organisation: open (no closingDate), taken effect (openingDate <= now) and
reachable from SOR_ROOT through a chain of open entities.
"""

import pytest
from DSA.CodeSystem.sor.sor import (
    ACTIVE_OWN_PROPERTY_KEY,
    KNOWN_PROPERTY_KEYS,
    SOR_ROOT,
    build_sor_codesystem,
    build_sor_codesystem_streaming,
)

pytestmark = pytest.mark.unit


def _wide_row(code, parent="", opening="2020-01-01 00:00:00", closing="", name=None):
    return {
        "code": code,
        "display": name or f"Enhed {code}",
        "parent_code": parent,
        "entityType": "klinisk enhed",
        "openingDate": opening,
        "closingDate": closing,
    }


def _flags(properties):
    return {p["CODE"]: p["VALUE"] for p in properties if p["KEY"] == ACTIVE_OWN_PROPERTY_KEY}


class TestActiveOwnProperty:
    def test_open_entities_under_root_are_true(self):
        rows = [
            _wide_row(SOR_ROOT, name="Example Region"),
            _wide_row("H1", parent=SOR_ROOT),
            _wide_row("a1", parent="H1"),
        ]
        _, properties, _ = build_sor_codesystem(rows)
        flags = _flags(properties)
        assert flags == {SOR_ROOT: "true", "H1": "true", "a1": "true"}

    def test_entities_outside_root_tree_are_false(self):
        rows = [
            _wide_row(SOR_ROOT),
            _wide_row("PRAKSIS1"),  # egen institutionsejer, ingen parent-kæde til RSD
            _wide_row("PRAKSIS2", parent="PRAKSIS1"),
        ]
        _, properties, _ = build_sor_codesystem(rows)
        flags = _flags(properties)
        assert flags[SOR_ROOT] == "true"
        assert flags["PRAKSIS1"] == "false"
        assert flags["PRAKSIS2"] == "false"

    def test_closed_entity_is_false(self):
        rows = [
            _wide_row(SOR_ROOT),
            _wide_row("H1", parent=SOR_ROOT, closing="2023-06-30 00:00:00"),
        ]
        _, properties, _ = build_sor_codesystem(rows)
        assert _flags(properties)["H1"] == "false"

    def test_closed_entity_breaks_the_chain_for_open_children(self):
        rows = [
            _wide_row(SOR_ROOT),
            _wide_row("H1", parent=SOR_ROOT, closing="2023-06-30 00:00:00"),
            _wide_row("a1", parent="H1"),  # åben, men forælderen er lukket
        ]
        _, properties, _ = build_sor_codesystem(rows)
        flags = _flags(properties)
        assert flags["H1"] == "false"
        assert flags["a1"] == "false"

    def test_future_opening_date_is_false(self):
        rows = [
            _wide_row(SOR_ROOT),
            _wide_row("H1", parent=SOR_ROOT, opening="2999-01-01 00:00:00"),
        ]
        _, properties, _ = build_sor_codesystem(rows)
        assert _flags(properties)["H1"] == "false"

    def test_missing_root_makes_everything_false(self):
        rows = [
            _wide_row("H1"),
            _wide_row("a1", parent="H1"),
        ]
        _, properties, _ = build_sor_codesystem(rows)
        assert set(_flags(properties).values()) == {"false"}

    def test_property_emitted_for_every_concept_as_boolean(self):
        rows = [
            _wide_row(SOR_ROOT),
            _wide_row("H1", parent=SOR_ROOT),
            _wide_row("X1"),
        ]
        concepts, properties, _ = build_sor_codesystem(rows)
        flag_props = [p for p in properties if p["KEY"] == ACTIVE_OWN_PROPERTY_KEY]
        assert len(flag_props) == len(concepts)
        assert all(p["TYPE"] == "Boolean" for p in flag_props)
        assert all(p["VALUE"] in ("true", "false") for p in flag_props)

    def test_normalised_format_also_gets_the_property(self):
        rows = [
            {"CODE": SOR_ROOT, "KEY": "display", "VALUE": "Example Region", "TYPE": "String"},
            {"CODE": "H1", "KEY": "display", "VALUE": "Sygehus", "TYPE": "String"},
            {"CODE": "H1", "KEY": "parentCode", "VALUE": SOR_ROOT, "TYPE": "String"},
        ]
        _, properties, _ = build_sor_codesystem(rows)
        flags = _flags(properties)
        assert flags == {SOR_ROOT: "true", "H1": "true"}

    def test_key_registered_in_known_property_keys(self):
        assert ACTIVE_OWN_PROPERTY_KEY in KNOWN_PROPERTY_KEYS


class TestStreamingBuildEquivalence:
    """The memory-lean streaming build (used by the cs_sor asset) must produce
    exactly the same concepts/properties/hierarchy as the list-based build —
    a drift between the two would silently change the codesystem."""

    def test_streaming_matches_list_build(self):
        rows = [
            _wide_row(SOR_ROOT, name="Example Region"),
            _wide_row("H1", parent=SOR_ROOT),
            _wide_row("a1", parent="H1", closing="2023-06-30 00:00:00"),
            _wide_row("FUT1", parent=SOR_ROOT, opening="2999-01-01 00:00:00"),
            _wide_row("PRAKSIS1"),
            {"code": "", "display": "no code", "parent_code": "", "entityType": ""},
        ]
        columns = ["code", "display", "parent_code", "entityType", "openingDate", "closingDate"]
        tuple_rows = [tuple(r.get(c, "") for c in columns) for r in rows]

        concepts_l, properties_l, hierarchy_l = build_sor_codesystem(rows)
        concepts_s, properties_iter, hierarchy_s = build_sor_codesystem_streaming(
            tuple_rows, columns
        )
        properties_s = list(properties_iter)

        def key(items, fields):
            return sorted(tuple(i[f] for f in fields) for i in items)

        assert key(concepts_s, ["CODE", "DISPLAY"]) == key(concepts_l, ["CODE", "DISPLAY"])
        assert key(properties_s, ["CODE", "KEY", "VALUE", "TYPE"]) == key(
            properties_l, ["CODE", "KEY", "VALUE", "TYPE"]
        )
        assert key(hierarchy_s, ["CODE", "PARENT_CODE"]) == key(
            hierarchy_l, ["CODE", "PARENT_CODE"]
        )

    def test_streaming_properties_unique_per_code_and_key(self):
        rows = [_wide_row(SOR_ROOT), _wide_row("H1", parent=SOR_ROOT)]
        columns = ["code", "display", "parent_code", "entityType", "openingDate", "closingDate"]
        tuple_rows = [tuple(r.get(c, "") for c in columns) for r in rows]

        _, properties_iter, _ = build_sor_codesystem_streaming(tuple_rows, columns)
        keys = [(p["CODE"], p["KEY"]) for p in properties_iter]
        assert len(keys) == len(set(keys)), "duplicate (code, property_code) would fail the MERGE"
