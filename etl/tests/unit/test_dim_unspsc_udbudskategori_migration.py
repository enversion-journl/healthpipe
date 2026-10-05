# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim.UNSPSC + map.UNSPSC udbudskategori schema change (#28710)."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
DIMS = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"
MAPS = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "01_maps.sql"
MIG = REPO / "infrastructure" / "ddl" / "migrations" / "20260629_01_dim_unspsc_udbudskategori.sql"

# Udbud descriptive columns — same name across DDL and the historical migration.
UDBUD_DESC_COLS = [
    "Beskrivelse_Udbud",
    "Udbudstype",
    "Udbudskategori_niveau_1",
    "Udbudskategori_niveau_2",
    "Udbudskategori_niveau_3",
    "Udbudskategori_niveau_4",
]


class TestCreateDdl:
    def test_dim_create_has_new_columns(self):
        text = DIMS.read_text(encoding="utf-8")
        start = text.index("CREATE TABLE [dim].[UNSPSC]")
        end = text.index("PK_Dim_UNSPSC", start)
        block = text[start:end]
        # Source flag uses the dw_source_* convention after the naming overhaul.
        assert "[dw_source_udbud]" in block
        for col in UDBUD_DESC_COLS:
            assert f"[{col.lower()}]" in block, col

    def test_map_create_has_source_udbud(self):
        text = MAPS.read_text(encoding="utf-8")
        start = text.index("CREATE TABLE [map].[UNSPSC]")
        end = text.index("PK_Map_UNSPSC", start)
        assert "[dw_source_udbud]" in text[start:end]


class TestMigration:
    def test_lives_in_autorun_dir(self):
        assert MIG.exists()

    def test_adds_each_dim_column_idempotently(self):
        text = MIG.read_text(encoding="utf-8")
        # This historical migration adds the flag under its original name
        # (Source_Udbud); it is renamed to DW_Source_Udbud by the later
        # 20260630 rename migration.
        assert "COLUMN_NAME = 'Source_Udbud'" in text
        assert "ADD [Source_Udbud]" in text
        for col in UDBUD_DESC_COLS:
            assert f"COLUMN_NAME = '{col}'" in text, col
            assert f"ADD [{col}]" in text, col

    def test_adds_map_source_udbud_idempotently(self):
        text = MIG.read_text(encoding="utf-8")
        assert "COLUMN_NAME = 'DW_Source_Udbud'" in text
        assert "ADD [DW_Source_Udbud]" in text

    def test_targets_both_tables(self):
        text = MIG.read_text(encoding="utf-8")
        assert "[dim].[UNSPSC]" in text
        assert "[map].[UNSPSC]" in text
