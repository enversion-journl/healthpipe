# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The ILS Lager delivery to Magenta: cs_lager's real output through LAGER_EXPORT.

The file keeps the codesystem's three levels — Example Region, sygehus,
lager — so a lager sits at niveau 3 under its sygehus. Each sygehus and lager
carries its own aktiv flag from ILS (dbo.sgh.aktiv / dbo.lgr.aktiv); the region
has none, and neither has a unit whose flag ILS leaves NULL — both are delivered
with an empty aktiv.

The source rows are pivoted from build_lager_hierarchy's output by the property
names the spec names, the way build_source_query pivots codesystem.Lager*. A
property the spec reads but cs_lager does not write therefore shows up here as
an empty column.
"""

from collections import defaultdict

import pytest
from DSA.CodeSystem.lager.lager import build_lager_hierarchy
from DSO.export.os2mo.codesystems import LAGER_EXPORT
from utils.os2mo.codesystem_export import build_export_rows

pytestmark = pytest.mark.unit

SGH_COLUMNS = ["sgh", "navn", "aktiv"]
SGH_ROWS = [
    ["6000", "Sygehus Lillebælt", True],
    ["7000", "Sydvestjysk Sygehus", False],
    ["8000", "Sygehus Sønderjylland", None],
]

LGR_COLUMNS = ["sgh", "lgr", "navn", "aktiv"]
LGR_ROWS = [
    ["6000", "0100", "Centrallager", True],
    ["6000", "0200", "Gammelt lager", False],
    ["7000", "0300", "Depot", True],
]


def _source_rows() -> list[dict]:
    concepts, properties, hierarchy = build_lager_hierarchy(
        SGH_ROWS, LGR_ROWS, SGH_COLUMNS, LGR_COLUMNS
    )
    props: dict[str, dict[str, str]] = defaultdict(dict)
    for prop in properties:
        props[prop["CODE"]][prop["KEY"]] = prop["VALUE"]
    parents = {edge["CODE"]: edge["PARENT_CODE"] for edge in hierarchy}
    return [
        {
            "code": concept["CODE"],
            "display": concept["DISPLAY"],
            "parent_code": parents.get(concept["CODE"]),
            "type": props[concept["CODE"]].get(LAGER_EXPORT.type_property),
            "status_active": props[concept["CODE"]].get(LAGER_EXPORT.status_active_property),
        }
        for concept in concepts
    ]


def _rows_by_code(source_rows: list[dict]) -> dict[str, dict]:
    return {row["kode"]: row for row in build_export_rows(LAGER_EXPORT, source_rows)}


def test_the_file_keeps_three_levels_region_sygehus_lager():
    rows = _rows_by_code(_source_rows())
    assert {
        kode: (row["kode_parent"], row["niveau"], row["type"]) for kode, row in rows.items()
    } == {
        "REGION_SYD": ("", "1", "region"),
        "SGH_6000": ("REGION_SYD", "2", "sygehus"),
        "SGH_7000": ("REGION_SYD", "2", "sygehus"),
        "SGH_8000": ("REGION_SYD", "2", "sygehus"),
        "SGH_6000_LGR_0100": ("SGH_6000", "3", "lager"),
        "SGH_6000_LGR_0200": ("SGH_6000", "3", "lager"),
        "SGH_7000_LGR_0300": ("SGH_7000", "3", "lager"),
    }


def test_each_unit_carries_its_own_aktiv_flag_from_ils():
    rows = _rows_by_code(_source_rows())
    assert {kode: row["aktiv"] for kode, row in rows.items()} == {
        "REGION_SYD": "",
        "SGH_6000": "true",
        "SGH_7000": "false",
        "SGH_8000": "",
        "SGH_6000_LGR_0100": "true",
        "SGH_6000_LGR_0200": "false",
        "SGH_7000_LGR_0300": "true",
    }


def test_navn_is_the_units_ils_name():
    rows = _rows_by_code(_source_rows())
    assert rows["SGH_6000"]["navn"] == "Sygehus Lillebælt"
    assert rows["SGH_6000_LGR_0100"]["navn"] == "Centrallager"


def test_without_the_region_root_no_delivery_is_built():
    """Otherwise the sygehuse would silently become roots at niveau 1."""
    without_root = [row for row in _source_rows() if row["code"] != "REGION_SYD"]
    with pytest.raises(ValueError, match="REGION_SYD"):
        build_export_rows(LAGER_EXPORT, without_root)
