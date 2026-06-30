# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_ilsvare lineage yml (task 28149)."""

from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[2]  # dagster_etl/
YML = REPO / "data_lineage" / "dim" / "dim_ilsvare.yml"


def _doc():
    return yaml.safe_load(YML.read_text(encoding="utf-8"))["dso_lineage"]


def test_yaml_parses():
    assert _doc()["TableName"] == "dim.IlsVare"


def test_swap_columns_renamed():
    cols = _doc()["Columns"]
    for new in ("Gruppering", "Gruppering_Navn", "Art", "Art_Navn"):
        assert new in cols, new
    for old in ("Vareart", "Vareart_Navn", "Varegruppe", "Varegruppe_Navn"):
        assert old not in cols, old


def test_new_columns_present():
    cols = _doc()["Columns"]
    for col in ("FaellesVarenummer", "EAN_Varenummer", "Billede", "OprBruger",
                "OprBruger_Navn", "Weblink", "MaaIkkeLukkes", "Hygiejne"):
        assert col in cols, col


def test_zz_oper_in_source_systems():
    text = YML.read_text(encoding="utf-8")
    assert "dbo.zz_oper" in text
