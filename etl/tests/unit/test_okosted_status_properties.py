# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""cs_okosted carries SydAX's status flags into codesystem.OkostedProperties."""

import pytest
from DSA.CodeSystem.okosted.okosted import build_okosted_hierarchy

pytestmark = pytest.mark.unit


def _properties(**row):
    base = {"omkostningsstedhierarkiid": 1, "nummer1": "RSD", "tekst1": "Region"}
    _, properties, _, _ = build_okosted_hierarchy([{**base, **row}])
    return {p["KEY"]: p["VALUE"] for p in properties if p["CODE"] == "RSD_NIVEAU_1"}


def test_aktiv_and_slettet_are_both_kept():
    props = _properties(aktiv=1, slettet=0)
    assert props["aktiv"] == "1"
    assert props["slettet"] == "0"


def test_a_missing_slettet_adds_no_property():
    assert "slettet" not in _properties(aktiv=1)


def _status(rows, code):
    _, properties, _, _ = build_okosted_hierarchy(rows)
    return {
        p["KEY"]: p["VALUE"]
        for p in properties
        if p["CODE"] == code and p["KEY"] in ("aktiv", "slettet")
    }


def test_a_grouping_level_gets_no_status_from_its_descendants():
    # RSD only appears as an ancestor: it has no row of its own.
    rows = [
        {
            "omkostningsstedhierarkiid": 1,
            "nummer1": "RSD",
            "nummer2": "A",
            "aktiv": 0,
            "slettet": 1,
        },
        {
            "omkostningsstedhierarkiid": 2,
            "nummer1": "RSD",
            "nummer2": "B",
            "aktiv": 1,
            "slettet": 0,
        },
    ]
    assert _status(rows, "RSD_NIVEAU_1") == {}
    assert _status(rows, "A_NIVEAU_2") == {"aktiv": "0", "slettet": "1"}


def test_a_code_in_use_in_one_of_its_own_rows_is_in_use():
    rows = [
        {
            "omkostningsstedhierarkiid": 1,
            "nummer1": "RSD",
            "nummer2": "A",
            "aktiv": 0,
            "slettet": 1,
        },
        {
            "omkostningsstedhierarkiid": 2,
            "nummer1": "RSD",
            "nummer2": "A",
            "aktiv": 1,
            "slettet": 0,
        },
    ]
    assert _status(rows, "A_NIVEAU_2") == {"aktiv": "1", "slettet": "0"}
    # …regardless of the order the rows were read in.
    assert _status(list(reversed(rows)), "A_NIVEAU_2") == {"aktiv": "1", "slettet": "0"}
