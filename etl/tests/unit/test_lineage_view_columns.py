# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ViewColumns i FHIR-lineage skal svare til de kolonner, vw_fhir-viewet lægger til.

Et aftagerview, der vælger ``<alias>.*`` og derudover navngivne kolonner fra et
opslag, viser kolonner, som ikke står i den flade tabel og derfor heller ikke i
lineage-filens Columns. De dokumenteres under ViewColumns. Testen fanger både en
kolonne, der kommer i viewet uden at blive dokumenteret, og en dokumenteret
kolonne, som viewet ikke længere har.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

_ETL = Path(__file__).resolve().parents[2]
_VIEWS_SQL = _ETL / "infrastructure" / "ddl" / "views" / "fhir" / "01_vw_fhir.sql"
_LINEAGE_DIR = _ETL / "data_lineage" / "fhir"

_VIEW_BLOCK = re.compile(
    r"CREATE OR ALTER VIEW \[vw_fhir\]\.\[(?P<view>\w+)\]\s+AS\s+SELECT\s+(?P<select>.*?)\s+FROM\s+",
    re.IGNORECASE | re.DOTALL,
)
_ALIAS = re.compile(r"\bAS\s+\[(\w+)\]", re.IGNORECASE)


def _tilfoejede_viewkolonner() -> dict[str, set[str]]:
    """vw_fhir-view -> navngivne kolonner ud over ``<alias>.*`` fra den flade tabel."""
    resultat: dict[str, set[str]] = {}
    for m in _VIEW_BLOCK.finditer(_VIEWS_SQL.read_text(encoding="utf-8")):
        select = m.group("select")
        if not re.match(r"\w+\.\*", select.strip()):
            continue
        aliaser = set(_ALIAS.findall(select))
        if aliaser:
            resultat[m.group("view")] = aliaser
    return resultat


def _dokumenterede_viewkolonner() -> dict[str, set[str]]:
    resultat: dict[str, set[str]] = {}
    for path in sorted(_LINEAGE_DIR.glob("*.yml")):
        node = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("dso_lineage") or {}
        view_columns = node.get("ViewColumns")
        if view_columns:
            resultat[node["TableName"].split(".", 1)[1]] = set(view_columns)
    return resultat


def test_viewets_tilfoejede_kolonner_er_dokumenteret():
    tilfoejet = _tilfoejede_viewkolonner()
    assert tilfoejet, "Fandt ingen views med tilføjede kolonner; er mønsteret brudt?"
    assert _dokumenterede_viewkolonner() == tilfoejet


def test_viewcolumns_kolliderer_ikke_med_tabelkolonner():
    for path in sorted(_LINEAGE_DIR.glob("*.yml")):
        node = (yaml.safe_load(path.read_text(encoding="utf-8")) or {}).get("dso_lineage") or {}
        overlap = set(node.get("ViewColumns") or {}) & set(node.get("Columns") or {})
        assert not overlap, f"{path.name}: {sorted(overlap)} står både i Columns og ViewColumns"
