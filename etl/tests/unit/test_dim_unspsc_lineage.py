# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim_UNSPSC lineage yml udbudskategori columns (#28710)."""

from pathlib import Path

import pytest
import yaml

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
YML = REPO / "data_lineage" / "dim" / "dim_UNSPSC.yml"


def _doc():
    return yaml.safe_load(YML.read_text(encoding="utf-8"))["dso_lineage"]


def test_yaml_parses():
    assert _doc()["TableName"] == "dim.UNSPSC"


def test_new_columns_present():
    cols = _doc()["Columns"]
    for col in (
        "dw_source_udbud",
        "beskrivelse_udbud",
        "udbudstype",
        "udbudskategori_niveau_1",
        "udbudskategori_niveau_2",
        "udbudskategori_niveau_3",
        "udbudskategori_niveau_4",
    ):
        assert col in cols, col


def test_udbudskategori_source_system_listed():
    text = YML.read_text(encoding="utf-8")
    assert "udbudskategori" in text


def test_source_refs_point_at_file_columns_not_assets():
    # Lineage rule: source refs must name the true source columns, never internal assets.
    cols = _doc()["Columns"]
    assert cols["beskrivelse_udbud"]["Source"] == [
        "udbudskategori.unspsc_udbudskategori.UNSPSC_Tekst"
    ]
    assert cols["udbudskategori_niveau_1"]["Source"] == [
        "udbudskategori.unspsc_udbudskategori.Udbudskategori_niv1"
    ]
