# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the codesystem export model — the sets we deliver to Magenta.

SOR is checked against a real subtree lifted out of the phase-1 CSV Magenta
received, so the expected values are Magenta's, not ours. The generic format
keeps the phase-1 columns that describe the unit itself, names them in OS2mo's
vocabulary and writes dates as ISO, so the file is translated column by column
and every value must still match. The phase-1 Level and Niv columns are left
out: the parent key is the file's only hierarchy information.

Sibling ORDER is deliberately not asserted against the file: the phase-1
export's within-level order is not reproducible from the codesystem, so the
builder sorts siblings by code for determinism instead. What must hold is that
a parent always precedes its children.
"""

import csv
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from DSO.export.os2mo.codesystems import CODESYSTEM_EXPORTS, OKOSTED_EXPORT, SOR_EXPORT
from utils.os2mo.codesystem_export import (
    CodesystemExportSpec,
    MappingSource,
    build_export_rows,
    export_columns,
    format_export_date,
    format_export_end_date,
    format_flag,
    map_slots_needed,
    parse_export_date,
    parse_export_timestamp,
    unreadable_values,
)

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "os2mo" / "sor_fase1_subtree.csv"
SOR_ROOT = "6101000016001"

# The phase-1 file's columns, and their names in the generic format.
PHASE1_RENAMES = {
    "CodeSOR": "user_key",
    "ParentCodeSOR": "parent_user_key",
    "FromDate": "validity_from",
    "ToDate": "validity_to",
    "Name": "name",
    "Type": "org_unit_type",
}

# Phase-1 columns the generic format deliberately leaves out.
PHASE1_HIERARCHY_COLUMNS = {"Level", *(f"Niv{level}" for level in range(1, 9))}


def _fixture_rows() -> list[dict[str, str]]:
    with open(FIXTURE, encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def _phase1_date_to_iso(value: str) -> str:
    """ "20150730 00:00:00 " -> "2015-07-30"; the open sentinel 9999… -> ""."""
    raw = value.strip()
    if raw.startswith("9999"):
        return ""
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def _generic(row: dict[str, str]) -> dict[str, str]:
    """A phase-1 row, translated into the generic format."""
    out = {
        PHASE1_RENAMES[key]: value
        for key, value in row.items()
        if key not in PHASE1_HIERARCHY_COLUMNS
    }
    out["validity_from"] = _phase1_date_to_iso(row["FromDate"])
    out["validity_to"] = _phase1_date_to_iso(row["ToDate"])
    # Not in the phase-1 file. Every delivered SOR unit is active; SOR has no
    # deleted flag, and the fixture carries no warehouse timestamps.
    out["active"] = "true"
    out["deleted"] = ""
    out["created"] = ""
    out["updated"] = ""
    return out


def _csv_date_to_codesystem(value: str) -> str:
    """Turn the CSV's "YYYYMMDD HH:MM:SS " back into codesystem's format."""
    raw = value.strip()
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]} {raw[9:]}"


def _sor_source_rows() -> list[dict[str, str]]:
    """Codesystem-shaped input from the fixture — only what the codesystem stores."""
    return [
        {
            "code": row["CodeSOR"],
            "display": row["Name"],
            "parent_code": row["ParentCodeSOR"],
            "type": row["Type"],
            "valid_from": _csv_date_to_codesystem(row["FromDate"]),
            "valid_to": "",
            "active": "true",
            "status_active": "true",
        }
        for row in _fixture_rows()
    ]


def _sor_rows() -> list[dict]:
    return build_export_rows(SOR_EXPORT, _sor_source_rows())


def _without_mappings(row: dict) -> dict:
    return {key: value for key, value in row.items() if key != "_mappings"}


class TestSorPhase1Parity:
    def test_every_unit_matches_the_phase1_file(self):
        expected = {row["CodeSOR"]: _generic(row) for row in _fixture_rows()}
        built = {row["user_key"]: _without_mappings(row) for row in _sor_rows()}

        assert set(built) == set(expected)
        for code, expected_row in expected.items():
            assert built[code] == expected_row, f"SOR {code} does not match the phase-1 file"

    def test_the_columns_are_the_phase1_layout_in_os2mo_names(self):
        with open(FIXTURE, encoding="utf-8-sig", newline="") as handle:
            phase1 = handle.readline().strip().split(";")
        translated = [PHASE1_RENAMES[c] for c in phase1 if c not in PHASE1_HIERARCHY_COLUMNS]
        # The unit's own phase-1 columns in order, then status and timestamps.
        assert export_columns(0) == [
            *translated,
            "active",
            "deleted",
            "created",
            "updated",
        ]

    def test_sor_carries_no_mapping_columns(self):
        assert map_slots_needed(SOR_EXPORT, _sor_rows()) == 0


class TestOrdering:
    def test_parents_always_precede_their_children(self):
        seen: set[str] = set()
        for row in _sor_rows():
            if row["parent_user_key"]:
                assert row["parent_user_key"] in seen, f"{row['user_key']} came before its parent"
            seen.add(row["user_key"])

    def test_root_is_first(self):
        built = _sor_rows()
        assert built[0]["user_key"] == SOR_ROOT
        assert built[0]["parent_user_key"] == ""

    def test_order_is_stable_across_input_permutations(self):
        source = _sor_source_rows()
        assert build_export_rows(SOR_EXPORT, source) == build_export_rows(
            SOR_EXPORT, list(reversed(source))
        )


class TestSorSelection:
    def test_inactive_units_are_excluded(self):
        source = _sor_source_rows()
        for row in source:
            if row["code"] != SOR_ROOT:
                row["active"] = "false"
        assert [row["user_key"] for row in build_export_rows(SOR_EXPORT, source)] == [SOR_ROOT]

    def test_children_of_an_inactive_parent_are_unreachable(self):
        source = _sor_source_rows()
        for row in source:
            if row["code"] == "8081000016006":  # Psykiatrien — the level-2 branch
                row["active"] = "false"

        codes = {row["user_key"] for row in build_export_rows(SOR_EXPORT, source)}
        assert "8081000016006" not in codes
        assert "243221000016006" not in codes
        assert "372211000016002" in codes

    def test_missing_root_refuses_to_build_a_delivery(self):
        source = [row for row in _sor_source_rows() if row["code"] != SOR_ROOT]
        with pytest.raises(ValueError, match=SOR_ROOT):
            build_export_rows(SOR_EXPORT, source)

    def test_boolean_active_flag_is_accepted(self):
        source = _sor_source_rows()
        for row in source:
            row["active"] = True
        assert len(build_export_rows(SOR_EXPORT, source)) == len(source)


# A small cost-centre forest: two roots, one inactive code, three levels.
OKOSTED_SOURCE = [
    {
        "code": "RSD",
        "display": "Example Region",
        "parent_code": None,
        "valid_from": "2020-01-01",
    },
    {"code": "OUH", "display": "OUH", "parent_code": "RSD", "valid_from": "2020-01-01"},
    {
        "code": "4711",
        "display": "Kardiologi",
        "parent_code": "OUH",
        "valid_from": "2021-03-01",
        "valid_to": "2025-12-31",
    },
    {"code": "FAEL", "display": "Fællesudgifter", "parent_code": "", "valid_from": "2019-01-01"},
]

OKOSTED_MAPPINGS = {
    "OUH": [("12345", "SOR", "equivalent")],
    "4711": [("67890", "SOR", "narrower"), ("12345", "SOR", "narrower")],
}


def _okosted_rows(mappings=None) -> list[dict]:
    return build_export_rows(OKOSTED_EXPORT, OKOSTED_SOURCE, mappings)


class TestOkosted:
    def test_the_whole_codesystem_is_delivered(self):
        # No active filter: a closed code is delivered with its own validity_to.
        assert {row["user_key"] for row in _okosted_rows()} == {"RSD", "OUH", "4711", "FAEL"}

    def test_every_parentless_code_is_a_root(self):
        roots = [row for row in _okosted_rows() if row["parent_user_key"] == ""]
        assert sorted(row["user_key"] for row in roots) == ["FAEL", "RSD"]

    def test_a_closed_code_carries_its_end_date(self):
        built = {row["user_key"]: row for row in _okosted_rows()}
        assert built["4711"]["validity_to"] == "2025-12-31"
        assert built["OUH"]["validity_to"] == ""

    def test_okosted_has_no_type(self):
        assert {row["org_unit_type"] for row in _okosted_rows()} == {""}

    def test_mappings_travel_with_their_code_sorted(self):
        built = {row["user_key"]: row for row in _okosted_rows(OKOSTED_MAPPINGS)}
        assert built["4711"]["_mappings"] == [
            ("12345", "SOR", "narrower"),
            ("67890", "SOR", "narrower"),
        ]
        assert built["RSD"]["_mappings"] == []

    def test_the_header_widens_to_the_most_mapped_code(self):
        assert map_slots_needed(OKOSTED_EXPORT, _okosted_rows(OKOSTED_MAPPINGS)) == 2

    def test_the_header_keeps_its_minimum_without_mappings(self):
        assert map_slots_needed(OKOSTED_EXPORT, _okosted_rows()) == 1
        assert export_columns(1)[-3:] == [
            "map_code_1",
            "map_system_1",
            "map_relation_1",
        ]

    def test_a_parent_cycle_does_not_hang(self):
        source = [
            {"code": "A", "display": "A", "parent_code": "B"},
            {"code": "B", "display": "B", "parent_code": "A"},
        ]
        # Neither is a root, so neither is reachable — and the walk terminates.
        assert build_export_rows(OKOSTED_EXPORT, source) == []


class TestUnreadableValues:
    def test_present_but_unreadable_values_are_counted(self):
        rows = [
            {"valid_from": "2024-05-01T00:00:00Z", "valid_to": "", "status_active": "yes-ish"},
            {"valid_from": "2024-05-01", "valid_to": "not a date", "status_deleted": "0"},
        ]
        assert unreadable_values(rows) == {"valid_from": 1, "valid_to": 1, "status_active": 1}

    def test_empty_values_are_not_unreadable(self):
        assert unreadable_values([{"valid_from": "", "valid_to": None}]) == {}


class TestStatusFlags:
    """Okosted's dates never close a code; its status travels as flags."""

    def test_okosted_flags_come_from_aktiv_and_slettet(self):
        source = [
            {"code": "A", "display": "A", "status_active": "1", "status_deleted": "0"},
            {"code": "B", "display": "B", "status_active": "0", "status_deleted": "1"},
        ]
        built = {row["user_key"]: row for row in build_export_rows(OKOSTED_EXPORT, source)}
        assert (built["A"]["active"], built["A"]["deleted"]) == ("true", "false")
        assert (built["B"]["active"], built["B"]["deleted"]) == ("false", "true")

    def test_a_missing_flag_is_empty_not_false(self):
        built = build_export_rows(OKOSTED_EXPORT, [{"code": "A", "display": "A"}])
        assert (built[0]["active"], built[0]["deleted"]) == ("", "")

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("1", "true"),
            ("True", "true"),
            (True, "true"),
            ("0", "false"),
            ("false", "false"),
            (None, ""),
            ("", ""),
            ("maybe", ""),
        ],
    )
    def test_format_flag(self, value, expected):
        assert format_flag(value) == expected


class TestOpenEndedSentinel:
    def test_a_year_9999_end_date_is_open(self):
        source = [
            {"code": "A", "display": "A", "valid_from": "1900-01-01", "valid_to": "9999-12-31"}
        ]
        built = build_export_rows(OKOSTED_EXPORT, source)
        assert built[0]["validity_from"] == "1900-01-01"
        assert built[0]["validity_to"] == ""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("9999-12-31", ""),
            ("9000-01-01", ""),
            ("8999-12-31", "8999-12-31"),
            ("2025-12-31 00:00:00", "2025-12-31"),
            (None, ""),
        ],
    )
    def test_format_export_end_date(self, value, expected):
        assert format_export_end_date(value) == expected


class TestTimestamps:
    """created/updated let Magenta's integration act on the rows that changed."""

    SOURCE = [
        {
            "code": "RSD",
            "display": "Region",
            "parent_code": "",
            "created": datetime(2024, 1, 1, 2, 0, 0),
            "updated": datetime(2024, 1, 1, 2, 0, 0),
        },
        {
            "code": "OUH",
            "display": "OUH",
            "parent_code": "RSD",
            "created": datetime(2024, 1, 1, 2, 0, 0),
            "updated": datetime(2024, 5, 1, 2, 0, 0),
        },
        {
            "code": "4711",
            "display": "Kardiologi",
            "parent_code": "OUH",
            "created": datetime(2024, 2, 1, 2, 0, 0, 123456),
            "updated": datetime(2024, 2, 1, 2, 0, 0),
        },
    ]

    def _built(self):
        return {row["user_key"]: row for row in build_export_rows(OKOSTED_EXPORT, self.SOURCE)}

    def test_created_is_iso_to_the_second(self):
        assert self._built()["4711"]["created"] == "2024-02-01T02:00:00"

    def test_updated_is_passed_through(self):
        assert self._built()["OUH"]["updated"] == "2024-05-01T02:00:00"

    def test_an_ancestors_updated_is_not_inherited(self):
        # The file carries no ancestor names, so OUH changing on 2024-05-01
        # leaves 4711's row — and its updated — as they were.
        assert self._built()["4711"]["updated"] == "2024-02-01T02:00:00"

    def test_missing_timestamps_render_empty(self):
        assert {row["created"] for row in _okosted_rows()} == {""}

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (datetime(2024, 1, 1, 2, 3, 4, 999), datetime(2024, 1, 1, 2, 3, 4)),
            ("2024-01-01T02:03:04", datetime(2024, 1, 1, 2, 3, 4)),
            ("", None),
            (None, None),
            ("garbage", None),
        ],
    )
    def test_parse_export_timestamp(self, value, expected):
        assert parse_export_timestamp(value) == expected


class TestSpecs:
    def test_asset_prefixes_and_names_are_unique(self):
        assert len({spec.asset_prefix for spec in CODESYSTEM_EXPORTS}) == len(CODESYSTEM_EXPORTS)
        assert len({spec.name for spec in CODESYSTEM_EXPORTS}) == len(CODESYSTEM_EXPORTS)

    def test_each_built_set_gets_its_own_file_name(self):
        built_at = datetime(2026, 10, 6, 1, 30, 7)
        assert SOR_EXPORT.filename(built_at) == "rsyd_sor_20261006_013007.csv"
        assert OKOSTED_EXPORT.filename(built_at) == "rsyd_okosted_20261006_013007.csv"

    @pytest.mark.parametrize("field", ["name", "from_property", "type_property"])
    def test_identifiers_that_reach_sql_must_be_plain(self, field):
        with pytest.raises(ValueError):
            replace(SOR_EXPORT, **{field: "x]; DROP TABLE y --"})

    def test_a_mapping_table_must_be_plain(self):
        with pytest.raises(ValueError):
            MappingSource(table="a]; --", system="SOR")

    def test_min_map_slots_needs_a_mapping(self):
        with pytest.raises(ValueError):
            CodesystemExportSpec(
                name="X",
                asset_prefix="x",
                source_deps=(),
                from_property="a",
                to_property="b",
                min_map_slots=1,
            )


class TestDateFormats:
    def test_open_units_have_an_empty_validity_to(self):
        # Empty is open-ended, as null is in OS2mo's validity.
        assert {row["validity_to"] for row in _sor_rows()} == {""}

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("2015-07-30 00:00:00", "2015-07-30"),
            ("2026-03-31 12:00:00", "2026-03-31"),
            ("2015-07-30", "2015-07-30"),
            ("", ""),
            (None, ""),
            ("not a date", ""),
        ],
    )
    def test_format_export_date(self, value, expected):
        assert format_export_date(value) == expected

    def test_the_rendering_round_trips(self):
        for row in _sor_rows():
            parsed = parse_export_date(row["validity_from"])
            assert parsed is not None
            assert parsed.isoformat() == row["validity_from"]

    @pytest.mark.parametrize("value", ["", "   ", None, "not a date", "20150730 00:00:00 "])
    def test_an_unrenderable_value_parses_to_none(self, value):
        assert parse_export_date(value) is None
