# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_ILSVare lineage yml (task 28149)."""

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
YML = REPO / "data_lineage" / "dim" / "dim_ILSVare.yml"


def _doc():
    return yaml.safe_load(YML.read_text(encoding="utf-8"))["dso_lineage"]


def test_yaml_parses():
    assert _doc()["TableName"] == "dim.ILSVare"


def test_swap_columns_renamed():
    cols = _doc()["Columns"]
    for new in ("gruppering", "gruppering_navn", "art", "art_navn"):
        assert new in cols, new
    for old in ("vareart", "vareart_navn", "varegruppe", "varegruppe_navn"):
        assert old not in cols, old


def test_new_columns_present():
    cols = _doc()["Columns"]
    for col in (
        "faelles_varenummer",
        "ean_varenummer",
        "billede",
        "opr_bruger",
        "opr_bruger_navn",
        "weblink",
        "maa_ikke_lukkes",
        "hygiejne",
    ):
        assert col in cols, col


def test_zz_oper_in_source_systems():
    text = YML.read_text(encoding="utf-8")
    assert "dbo.zz_oper" in text
