# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the codesystem export model — the sets we deliver to Magenta.

SOR is checked against a real subtree lifted out of the phase-1 CSV Magenta
received, so the expected values are Magenta's, not ours. The generic format
keeps the phase-1 columns that describe the unit itself, renames them to
Example Region's own delivery names and writes dates as ISO, so the file is
translated column by column and every value must still match. The phase-1
Level column becomes ``niveau`` — the generic format computes it from the
delivered tree rather than copying phase-1's own value, but on this fixture
the two still agree row for row. The phase-1 Niv1..Niv8 ancestor-name columns
are left out: ``kode_parent`` is the file's only ancestry information.

Sibling ORDER is deliberately not asserted against the file: the phase-1
export's within-level order is not reproducible from the codesystem, so the
builder sorts siblings by code for determinism instead. What must hold is that
a parent always precedes its children.

The specs used here are built locally rather than imported from
``DSO.export.os2mo.codesystems``: that module's production specs are a
downstream consumer of this model, exercised on their own terms elsewhere.
"""

import csv
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from utils.os2mo.codesystem_export import (
    BASE_COLUMNS,
    CodeSource,
    CodesystemExportSpec,
    build_export_rows,
    export_columns,
    format_export_date,
    format_export_end_date,
    format_flag,
    parse_export_date,
    parse_export_timestamp,
    unreadable_values,
)

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).parent.parent / "fixtures" / "os2mo" / "sor_fase1_subtree.csv"
SOR_ROOT = "6101000016001"

# A local stand-in for the production SOR spec (DSO.export.os2mo.codesystems):
# same property names, so the phase-1 parity checks below exercise the same
# shape of spec without depending on that consumer module.
SOR_SPEC = CodesystemExportSpec(
    name="SOR",
    asset_prefix="sor",
    source_deps=("cs_sor",),
    from_property="openingDate",
    to_property="closingDate",
    type_property="entityType",
    scope_property="rsdOrganization",
    scope_value="true",
    status_active_property="activeOwnOrganization",
    root_code=SOR_ROOT,
)

# The phase-1 file's columns, and their names in the generic format.
PHASE1_RENAMES = {
    "CodeSOR": "kode",
    "ParentCodeSOR": "kode_parent",
    "FromDate": "gyldig_fra",
    "ToDate": "gyldig_til",
    "Name": "navn",
    "Type": "type",
    "Level": "niveau",
}

# Phase-1's ancestor-name columns the generic format deliberately leaves out.
PHASE1_HIERARCHY_COLUMNS = {f"Niv{level}" for level in range(1, 9)}


def _fixture_rows() -> list[dict[str, str]]:
    with open(FIXTURE, encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle, delimiter=";"))


def _phase1_date_to_iso(value: str) -> str:
    """ "20150730 00:00:00 " -> "2015-07-30"; the open sentinel "99991231 …" -> "9999-12-31"."""
    raw = value.strip()
    return f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}"


def _generic(row: dict[str, str]) -> dict[str, str]:
    """A phase-1 row, translated into the generic format."""
    out = {
        PHASE1_RENAMES[key]: value
        for key, value in row.items()
        if key not in PHASE1_HIERARCHY_COLUMNS
    }
    out["gyldig_fra"] = _phase1_date_to_iso(row["FromDate"])
    out["gyldig_til"] = _phase1_date_to_iso(row["ToDate"])
    # Not in the phase-1 file. Every unit in the fixture is active; SOR has no
    # deleted flag, and the fixture carries no warehouse timestamps.
    out["aktiv"] = "true"
    out["dw_is_deleted"] = ""
    out["dw_timestamp_insert"] = ""
    out["dw_timestamp_update"] = ""
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
            "scope": "true",
            "status_active": "true",
        }
        for row in _fixture_rows()
    ]


def _sor_rows() -> list[dict]:
    return build_export_rows(SOR_SPEC, _sor_source_rows())


class TestSorPhase1Parity:
    def test_every_unit_matches_the_phase1_file(self):
        expected = {row["CodeSOR"]: _generic(row) for row in _fixture_rows()}
        built = {row["kode"]: row for row in _sor_rows()}

        assert set(built) == set(expected)
        for code, expected_row in expected.items():
            assert built[code] == expected_row, f"SOR {code} does not match the phase-1 file"

    def test_the_columns_are_the_phase1_layout_in_the_delivery_names(self):
        with open(FIXTURE, encoding="utf-8-sig", newline="") as handle:
            phase1 = handle.readline().strip().split(";")
        translated = [PHASE1_RENAMES[c] for c in phase1 if c not in PHASE1_HIERARCHY_COLUMNS]
        # The unit's own phase-1 columns (niveau included), then status flags
        # and timestamps.
        assert export_columns() == [
            *translated,
            "aktiv",
            "dw_is_deleted",
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]


class TestOrdering:
    def test_parents_always_precede_their_children(self):
        seen: set[str] = set()
        for row in _sor_rows():
            if row["kode_parent"]:
                assert row["kode_parent"] in seen, f"{row['kode']} came before its parent"
            seen.add(row["kode"])

    def test_root_is_first(self):
        built = _sor_rows()
        assert built[0]["kode"] == SOR_ROOT
        assert built[0]["kode_parent"] == ""

    def test_order_is_stable_across_input_permutations(self):
        source = _sor_source_rows()
        assert build_export_rows(SOR_SPEC, source) == build_export_rows(
            SOR_SPEC, list(reversed(source))
        )


class TestSorSelection:
    def test_units_out_of_scope_are_excluded(self):
        source = _sor_source_rows()
        for row in source:
            if row["code"] != SOR_ROOT:
                row["scope"] = "false"
        assert [row["kode"] for row in build_export_rows(SOR_SPEC, source)] == [SOR_ROOT]

    def test_children_of_a_parent_out_of_scope_are_unreachable(self):
        source = _sor_source_rows()
        for row in source:
            if row["code"] == "8081000016006":  # Psykiatrien — the level-2 branch
                row["scope"] = "false"

        codes = {row["kode"] for row in build_export_rows(SOR_SPEC, source)}
        assert "8081000016006" not in codes
        assert "243221000016006" not in codes
        assert "372211000016002" in codes

    def test_a_closed_unit_in_scope_is_delivered_inactive_with_its_children(self):
        source = _sor_source_rows()
        for row in source:
            if row["code"] == "8081000016006":  # Psykiatrien — the level-2 branch
                row["status_active"] = "false"
                row["valid_to"] = "2023-06-30 00:00:00"

        built = {row["kode"]: row for row in build_export_rows(SOR_SPEC, source)}
        assert built["8081000016006"]["aktiv"] == "false"
        assert built["8081000016006"]["gyldig_til"] == "2023-06-30"
        assert "243221000016006" in built

    def test_missing_root_refuses_to_build_a_delivery(self):
        source = [row for row in _sor_source_rows() if row["code"] != SOR_ROOT]
        with pytest.raises(ValueError, match=SOR_ROOT):
            build_export_rows(SOR_SPEC, source)

    def test_boolean_scope_flag_is_accepted(self):
        source = _sor_source_rows()
        for row in source:
            row["scope"] = True
        assert len(build_export_rows(SOR_SPEC, source)) == len(source)


def _spec(**overrides):
    defaults = dict(
        name="Okosted",
        asset_prefix="okosted",
        source_deps=("cs_okosted",),
        from_property="startsdate",
        to_property="enddate",
        type_property="enhedstype",
        status_active_property="aktiv",
        status_deleted_property="slettet",
    )
    defaults.update(overrides)
    return CodesystemExportSpec(**defaults)


# A local stand-in for the production Okosted spec (DSO.export.os2mo.codesystems).
OKOSTED_SPEC = _spec()

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


def _okosted_rows() -> list[dict]:
    return build_export_rows(OKOSTED_SPEC, OKOSTED_SOURCE)


class TestOkosted:
    def test_the_whole_codesystem_is_delivered(self):
        # No active filter: a closed code is delivered with its own gyldig_til.
        assert {row["kode"] for row in _okosted_rows()} == {"RSD", "OUH", "4711", "FAEL"}

    def test_every_parentless_code_is_a_root(self):
        roots = [row for row in _okosted_rows() if row["kode_parent"] == ""]
        assert sorted(row["kode"] for row in roots) == ["FAEL", "RSD"]

    def test_a_closed_code_carries_its_end_date(self):
        built = {row["kode"]: row for row in _okosted_rows()}
        assert built["4711"]["gyldig_til"] == "2025-12-31"
        assert built["OUH"]["gyldig_til"] == "9999-12-31"

    def test_okosted_has_no_type(self):
        assert {row["type"] for row in _okosted_rows()} == {""}

    def test_a_parent_cycle_does_not_hang(self):
        source = [
            {"code": "A", "display": "A", "parent_code": "B"},
            {"code": "B", "display": "B", "parent_code": "A"},
        ]
        # Neither is a root, so neither is reachable — and the walk terminates.
        assert build_export_rows(OKOSTED_SPEC, source) == []


INVOICE_RECEIVERS = CodeSource(
    schema="arc", table="mysupply_statistic_invoice_header", column="RegningsModtagerID"
)

# A local stand-in for the production EAN spec: flat, no dates, type or flags.
EAN_SPEC = CodesystemExportSpec(
    name="EAN",
    asset_prefix="ean",
    source_deps=("cs_ean",),
    from_property=None,
    to_property=None,
    code_source=INVOICE_RECEIVERS,
    display_property="participantName",
)

# As build_code_source_query returns them: no parent, and display NULL for a
# receiver NemHandel does not know.
EAN_SOURCE = [
    {"code": "5790000000002", "display": "Firma B", "parent_code": None},
    {"code": "5790000000001", "display": "Firma A", "parent_code": None},
    {"code": "5790000000003", "display": None, "parent_code": None},
]


class TestEan:
    def test_a_flat_codesystem_is_all_roots_at_niveau_1(self):
        built = build_export_rows(EAN_SPEC, EAN_SOURCE)
        assert [row["kode"] for row in built] == ["5790000000001", "5790000000002", "5790000000003"]
        assert {row["kode_parent"] for row in built} == {""}
        assert {row["niveau"] for row in built} == {"1"}

    def test_columns_without_a_source_property_are_empty(self):
        for row in build_export_rows(EAN_SPEC, EAN_SOURCE):
            for column in ("type", "aktiv", "dw_is_deleted"):
                assert row[column] == ""

    def test_without_source_dates_a_code_is_valid_from_1900_to_9999(self):
        for row in build_export_rows(EAN_SPEC, EAN_SOURCE):
            assert (row["gyldig_fra"], row["gyldig_til"]) == ("1900-01-01", "9999-12-31")

    def test_navn_is_the_name_and_empty_without_one(self):
        built = {row["kode"]: row for row in build_export_rows(EAN_SPEC, EAN_SOURCE)}
        assert built["5790000000001"]["navn"] == "Firma A"
        assert built["5790000000003"]["navn"] == ""


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
        built = {row["kode"]: row for row in build_export_rows(OKOSTED_SPEC, source)}
        assert (built["A"]["aktiv"], built["A"]["dw_is_deleted"]) == ("true", "false")
        assert (built["B"]["aktiv"], built["B"]["dw_is_deleted"]) == ("false", "true")

    def test_a_missing_flag_is_empty_not_false(self):
        built = build_export_rows(OKOSTED_SPEC, [{"code": "A", "display": "A"}])
        assert (built[0]["aktiv"], built[0]["dw_is_deleted"]) == ("", "")

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
    @pytest.mark.parametrize("valid_to", ["9999-12-31", "9000-01-01", "", None, "not a date"])
    def test_every_open_end_is_delivered_as_9999_12_31(self, valid_to):
        source = [{"code": "A", "display": "A", "valid_from": "2020-01-01", "valid_to": valid_to}]
        assert build_export_rows(OKOSTED_SPEC, source)[0]["gyldig_til"] == "9999-12-31"

    @pytest.mark.parametrize("valid_from", ["", None, "not a date"])
    def test_a_missing_start_is_delivered_as_1900_01_01(self, valid_from):
        source = [{"code": "A", "display": "A", "valid_from": valid_from}]
        assert build_export_rows(OKOSTED_SPEC, source)[0]["gyldig_fra"] == "1900-01-01"

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
    """dw_timestamp_insert/dw_timestamp_update: when a code entered and when its row changed."""

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
        return {row["kode"]: row for row in build_export_rows(OKOSTED_SPEC, self.SOURCE)}

    def test_created_is_iso_to_the_second(self):
        assert self._built()["4711"]["dw_timestamp_insert"] == "2024-02-01T02:00:00"

    def test_updated_is_passed_through(self):
        assert self._built()["OUH"]["dw_timestamp_update"] == "2024-05-01T02:00:00"

    def test_an_ancestors_updated_is_not_inherited(self):
        # The file carries no ancestor names, so OUH changing on 2024-05-01
        # leaves 4711's row — and its dw_timestamp_update — as they were.
        assert self._built()["4711"]["dw_timestamp_update"] == "2024-02-01T02:00:00"

    def test_missing_timestamps_render_empty(self):
        assert {row["dw_timestamp_insert"] for row in _okosted_rows()} == {""}

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
    def test_each_built_set_gets_its_own_file_name(self):
        built_at = datetime(2026, 10, 6, 1, 30, 7)
        assert SOR_SPEC.filename(built_at) == "rsyd_sor_20261006_013007.csv"
        assert OKOSTED_SPEC.filename(built_at) == "rsyd_okosted_20261006_013007.csv"

    @pytest.mark.parametrize("field", ["name", "from_property", "to_property", "type_property"])
    def test_identifiers_that_reach_sql_must_be_plain(self, field):
        with pytest.raises(ValueError):
            replace(SOR_SPEC, **{field: "x]; DROP TABLE y --"})

    @pytest.mark.parametrize("field", ["schema", "table", "column"])
    def test_a_code_source_must_be_plain_identifiers(self, field):
        with pytest.raises(ValueError):
            replace(INVOICE_RECEIVERS, **{field: "x]; DROP TABLE y --"})

    def test_a_code_source_spec_refuses_properties_it_would_not_read(self):
        with pytest.raises(ValueError, match="type_property"):
            replace(EAN_SPEC, type_property="entityType")

    def test_a_display_property_needs_a_code_source(self):
        with pytest.raises(ValueError, match="code_source"):
            replace(EAN_SPEC, code_source=None)


class TestDateFormats:
    def test_open_units_are_valid_until_9999_12_31(self):
        assert {row["gyldig_til"] for row in _sor_rows()} == {"9999-12-31"}

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
            parsed = parse_export_date(row["gyldig_fra"])
            assert parsed is not None
            assert parsed.isoformat() == row["gyldig_fra"]

    @pytest.mark.parametrize("value", ["", "   ", None, "not a date", "20150730 00:00:00 "])
    def test_an_unrenderable_value_parses_to_none(self, value):
        assert parse_export_date(value) is None


def test_row_keys_are_exactly_the_delivered_columns():
    rows = build_export_rows(_spec(), [{"code": "A_NIVEAU_1", "display": "Rod"}])
    assert list(rows[0].keys()) == BASE_COLUMNS
    assert BASE_COLUMNS == [
        "kode",
        "kode_parent",
        "gyldig_fra",
        "gyldig_til",
        "navn",
        "type",
        "niveau",
        "aktiv",
        "dw_is_deleted",
        "dw_timestamp_insert",
        "dw_timestamp_update",
    ]


def test_niveau_is_depth_in_the_delivered_tree_per_root():
    source = [
        {"code": "A_NIVEAU_1", "display": "Rod A"},
        {"code": "B_NIVEAU_1", "display": "Rod B"},
        {"code": "A.X_NIVEAU_2", "display": "Barn", "parent_code": "A_NIVEAU_1"},
        {"code": "A.X.Y_NIVEAU_3", "display": "Barnebarn", "parent_code": "A.X_NIVEAU_2"},
    ]
    niveau = {r["kode"]: r["niveau"] for r in build_export_rows(_spec(), source)}
    assert niveau == {
        "A_NIVEAU_1": "1",
        "B_NIVEAU_1": "1",
        "A.X_NIVEAU_2": "2",
        "A.X.Y_NIVEAU_3": "3",
    }


def test_type_comes_from_the_type_property():
    rows = build_export_rows(
        _spec(), [{"code": "A_NIVEAU_1", "display": "Rod", "type": "juridisk enhed"}]
    )
    assert rows[0]["type"] == "juridisk enhed"


def test_mapping_machinery_is_gone():
    import utils.os2mo.codesystem_export as m

    assert not hasattr(m, "MappingSource")
    assert not hasattr(m, "map_slots_needed")
    assert "mappings" not in CodesystemExportSpec.__dataclass_fields__
