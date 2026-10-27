# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the (nummer, niveau) node identity in build_okosted_hierarchy."""

import pytest
from DSA.CodeSystem.okosted.okosted import MAX_CODE_LENGTH, build_okosted_hierarchy

pytestmark = pytest.mark.unit


def _codes(concepts):
    return {c["CODE"] for c in concepts}


def _edges(hierarchy):
    return {(h["CODE"], h["PARENT_CODE"]) for h in hierarchy}


def _props(properties, code):
    return {p["KEY"]: p["VALUE"] for p in properties if p["CODE"] == code}


FER_PLACEHOLDER_ROW = {
    # The deleted placeholder row from the source: FER > FER.FON > FER.
    # Same nummer, two different units (jeId vs afId) — must become two nodes.
    "omkostningsstedhierarkiid": 20491,
    "nummer1": "FER",
    "tekst1": "Fælles FER",
    "jeid1": 102105358334,
    "nummer2": "FER.FON",
    "tekst2": "FER Fonde",
    "veid2": 14,
    "nummer3": "FER",
    "tekst3": "Medcxxxxxxxx",
    "afid3": 2724,
    "aktiv": 0,
    "slettet": 1,
    "startsdate": "2020-01-01",
}

FER_LIVE_ROW = {
    "omkostningsstedhierarkiid": 20492,
    "nummer1": "FER",
    "tekst1": "Fælles FER",
    "jeid1": 102105358334,
    "nummer2": "FER.FON",
    "tekst2": "FER Fonde",
    "veid2": 14,
    "nummer3": "FER.ØKO",
    "tekst3": "FER Økonomi",
    "afid3": 2800,
    "nummer4": "4412",
    "tekst4": "Et omkostningssted",
    "osid4": 9001,
    "aktiv": 1,
    "slettet": 0,
    "startsdate": "2024-01-01",
}


def test_duplicate_nummer_becomes_two_nodes_and_no_cycle():
    concepts, properties, hierarchy, counters = build_okosted_hierarchy(
        [FER_PLACEHOLDER_ROW, FER_LIVE_ROW]
    )
    assert _codes(concepts) == {
        "FER_NIVEAU_1",
        "FER.FON_NIVEAU_2",
        "FER_NIVEAU_3",
        "FER.ØKO_NIVEAU_3",
        "4412_NIVEAU_4",
    }
    edges = _edges(hierarchy)
    assert ("FER.FON_NIVEAU_2", "FER_NIVEAU_1") in edges
    assert ("FER_NIVEAU_3", "FER.FON_NIVEAU_2") in edges
    assert ("4412_NIVEAU_4", "FER.ØKO_NIVEAU_3") in edges
    # One edge per child, root has none.
    children = [h["CODE"] for h in hierarchy]
    assert len(children) == len(set(children))
    assert "FER_NIVEAU_1" not in children


def test_rsd_self_reference_becomes_edge_between_two_nodes():
    row = {
        "omkostningsstedhierarkiid": 3391,
        "nummer1": "RSD",
        "tekst1": "Example Region",
        "jeid1": 1,
        "nummer2": "RSD",
        "tekst2": "Example Region",
        "veid2": 2,
        "aktiv": 1,
        "slettet": 0,
        "startsdate": "2024-01-01",
    }
    concepts, _, hierarchy, _ = build_okosted_hierarchy([row])
    assert _codes(concepts) == {"RSD_NIVEAU_1", "RSD_NIVEAU_2"}
    assert _edges(hierarchy) == {("RSD_NIVEAU_2", "RSD_NIVEAU_1")}


def test_every_edge_goes_exactly_one_level_up():
    _, _, hierarchy, _ = build_okosted_hierarchy([FER_PLACEHOLDER_ROW, FER_LIVE_ROW])
    for h in hierarchy:
        child_level = int(h["CODE"].rsplit("_NIVEAU_", 1)[1])
        parent_level = int(h["PARENT_CODE"].rsplit("_NIVEAU_", 1)[1])
        assert parent_level == child_level - 1


def test_display_comes_from_nodes_own_cell_newest_row_wins():
    old = dict(
        FER_LIVE_ROW,
        omkostningsstedhierarkiid=1,
        tekst2="FER Fonde (gammelt navn)",
        startsdate="2020-01-01",
    )
    new = dict(FER_LIVE_ROW, omkostningsstedhierarkiid=2, startsdate="2025-01-01")
    concepts, _, _, _ = build_okosted_hierarchy([old, new])
    display = {c["CODE"]: c["DISPLAY"] for c in concepts}
    assert display["FER.FON_NIVEAU_2"] == "FER Fonde"
    # Empty tekst falls back to the raw nummer.
    bare = dict(FER_LIVE_ROW, omkostningsstedhierarkiid=3, tekst4=None, startsdate="2026-01-01")
    concepts, _, _, _ = build_okosted_hierarchy([bare])
    assert {c["CODE"]: c["DISPLAY"] for c in concepts}["4412_NIVEAU_4"] == "4412"


def test_parent_conflict_resolved_by_newest_row_and_counted():
    under_a = {
        "omkostningsstedhierarkiid": 10,
        "nummer1": "RSD",
        "jeid1": 1,
        "nummer2": "RSD",
        "veid2": 2,
        "nummer3": "A",
        "afid3": 30,
        "nummer4": "X",
        "osid4": 40,
        "startsdate": "2020-01-01",
        "aktiv": 1,
        "slettet": 0,
    }
    under_b = dict(
        under_a, omkostningsstedhierarkiid=11, nummer3="B", afid3=31, startsdate="2025-01-01"
    )
    _, _, hierarchy, counters = build_okosted_hierarchy([under_a, under_b])
    assert ("X_NIVEAU_4", "B_NIVEAU_3") in _edges(hierarchy)
    assert ("X_NIVEAU_4", "A_NIVEAU_3") not in _edges(hierarchy)
    assert counters["parent_conflicts"] == 1


def test_gap_in_path_makes_node_a_root_and_is_counted():
    row = {
        "omkostningsstedhierarkiid": 12,
        "nummer1": "RSD",
        "jeid1": 1,
        # nummer2 missing entirely — a gap in the path.
        "nummer3": "C",
        "afid3": 33,
        "startsdate": "2024-01-01",
        "aktiv": 1,
        "slettet": 0,
    }
    concepts, _, hierarchy, counters = build_okosted_hierarchy([row])
    assert "C_NIVEAU_3" in _codes(concepts)
    assert "C_NIVEAU_3" not in {h["CODE"] for h in hierarchy}
    assert counters["path_gaps"] == 1


def test_node_properties_nummer_status_and_dates():
    _, properties, _, _ = build_okosted_hierarchy([FER_PLACEHOLDER_ROW, FER_LIVE_ROW])
    fer3 = _props(properties, "FER_NIVEAU_3")
    assert fer3["nummer"] == "FER"
    # FER_NIVEAU_3 is the deepest cell of the placeholder row -> own row -> status.
    assert fer3["aktiv"] == "0" and fer3["slettet"] == "1"
    assert fer3["OmkostningsstedHierarkiId"] == "20491"
    # FER_NIVEAU_1 is never a row's deepest cell here -> no status properties.
    fer1 = _props(properties, "FER_NIVEAU_1")
    assert fer1["nummer"] == "FER"
    assert "aktiv" not in fer1 and "slettet" not in fer1
    assert "nummer_kolonne" not in fer1  # identity lives in the code's _NIVEAU_ suffix
    assert fer3["startsdate"] == "2020-01-01"


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("FER_NIVEAU_1", "juridisk enhed"),
        ("FER.FON_NIVEAU_2", "virksomhedsenhed"),
        ("FER.ØKO_NIVEAU_3", "afdeling"),
        ("4412_NIVEAU_4", "omkostningssted"),
    ],
)
def test_enhedstype_follows_typed_id_column(code, expected):
    _, properties, _, _ = build_okosted_hierarchy([FER_PLACEHOLDER_ROW, FER_LIVE_ROW])
    assert _props(properties, code)["enhedstype"] == expected


def test_cell_without_typed_id_becomes_ukendt_and_is_counted():
    row = dict(FER_LIVE_ROW, omkostningsstedhierarkiid=13, osid4=None, startsdate="2026-01-01")
    _, properties, _, counters = build_okosted_hierarchy([row])
    assert _props(properties, "4412_NIVEAU_4")["enhedstype"] == "Ukendt"
    assert counters["unknown_enhedstype"] == 1


def test_raw_nummer_containing_suffix_fails_loud():
    row = dict(FER_LIVE_ROW, nummer4="X_NIVEAU_2")
    with pytest.raises(ValueError, match="reserved level suffix"):
        build_okosted_hierarchy([row])


def test_code_longer_than_120_chars_fails_loud():
    row = dict(FER_LIVE_ROW, nummer4="N" * 115)
    with pytest.raises(ValueError, match="exceeds 120"):
        build_okosted_hierarchy([row])


def test_longest_realistic_code_fits_merge_width():
    # The builder accepts a code exactly at MAX_CODE_LENGTH, and the longest
    # code produced equals that ceiling — it is never truncated below it.
    row = dict(FER_LIVE_ROW, nummer4="N" * (MAX_CODE_LENGTH - len("_NIVEAU_4")))
    concepts, _, _, _ = build_okosted_hierarchy([row])
    assert max(len(c["CODE"]) for c in concepts) == MAX_CODE_LENGTH


def test_okosted_to_sor_csv_source_codes_are_composite():
    import csv as csv_module
    import re
    from pathlib import Path

    csv_path = (
        Path(__file__).resolve().parents[2] / "DSI" / "csv_files" / "data" / "okosted_to_sor.csv"
    )
    with csv_path.open(encoding="utf-8-sig") as handle:
        rows = list(csv_module.DictReader(handle))
    pattern = re.compile(r"^.+_NIVEAU_\d{1,2}$")
    bad = [r["source_code"] for r in rows if not pattern.match(r["source_code"])]
    assert bad == []


def test_stg_entries_carry_raw_nummer_from_property():
    from DSA.SQL.staging.stg_Okosted import build_stg_entries

    entries = build_stg_entries(
        concepts=[("FER_NIVEAU_1", "Fælles FER")],
        hierarchy_lookup={},
        properties_lookup={"FER_NIVEAU_1": {"nummer": "FER", "aktiv": "1"}},
    )
    assert entries[0]["code"] == "FER_NIVEAU_1"
    assert entries[0]["nummer"] == "FER"

    # Missing property surfaces as None (visible gap), never a guessed value.
    entries = build_stg_entries([("X_NIVEAU_2", "X")], {}, {})
    assert entries[0]["nummer"] is None


def test_parse_aktiv_reads_sydax_1_0_not_just_the_word_true():
    from DSA.SQL.staging.stg_Okosted import _parse_aktiv

    assert _parse_aktiv("1") is True
    assert _parse_aktiv("0") is False
    assert _parse_aktiv(None) is True


def test_nummer_miss_is_visible_in_the_entry_and_countable():
    from DSA.SQL.staging.stg_Okosted import build_stg_entries

    entries = build_stg_entries(
        concepts=[("X_NIVEAU_2", "X")],
        hierarchy_lookup={},
        properties_lookup={},
    )
    assert entries[0]["nummer"] is None
    assert sum(1 for e in entries if e["nummer"] is None) == 1
