# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for dim.IlsVare schema change: CREATE + migration.

The migration lives in the centralized auto-run directory
(infrastructure/ddl/migrations/), applied automatically against BOTH staging
and DSO by the staging_migrations / dso_migrations assets. A single combined
file is used; the ext.vare_enriched_ils drop is guarded and is a no-op on DSO.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
DDL = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"
MIG = REPO / "infrastructure" / "ddl" / "migrations" / "20260625_01_add_ilsvare_columns.sql"

NEW_DIM_COLS = [
    "FaellesVarenummer",
    "EAN_Varenummer",
    "Billede",
    "OprBruger",
    "OprBruger_Navn",
    "Weblink",
    "MaaIkkeLukkes",
    "Hygiejne",
]
RENAMES = [
    ("Vareart", "Gruppering"),
    ("Vareart_Navn", "Gruppering_Navn"),
    ("Varegruppe", "Art"),
    ("Varegruppe_Navn", "Art_Navn"),
]

# NEW_DIM_COLS / RENAMES above are the historical (pre-naming-overhaul) names —
# they stay PascalCase because TestMigration below asserts them against the
# immutable applied migration file. The live DDL (dim.ILSVare) uses the new
# snake_case column names; map old -> new explicitly since it's not a plain
# .lower() (word-merge insertions like EAN_Varenummer -> ean_varenummer,
# OprBruger -> opr_bruger).
NEW_DIM_COLS_DDL = {
    "FaellesVarenummer": "faelles_varenummer",
    "EAN_Varenummer": "ean_varenummer",
    "Billede": "billede",
    "OprBruger": "opr_bruger",
    "OprBruger_Navn": "opr_bruger_navn",
    "Weblink": "weblink",
    "MaaIkkeLukkes": "maa_ikke_lukkes",
    "Hygiejne": "hygiejne",
}


class TestCreateDdl:
    def test_create_has_new_columns(self):
        text = DDL.read_text(encoding="utf-8")
        for col in NEW_DIM_COLS:
            assert f"[{NEW_DIM_COLS_DDL[col]}]" in text, col

    def test_create_has_renamed_columns(self):
        text = DDL.read_text(encoding="utf-8")
        for _, new in RENAMES:
            assert f"[{new.lower()}]" in text, new

    def test_create_drops_old_swap_names(self):
        # Old names must no longer appear as column declarations in dim.ILSVare.
        # (Other tables/indexes may still legitimately mention them, so scope to
        # the ILSVare CREATE block.)
        text = DDL.read_text(encoding="utf-8")
        start = text.index("CREATE TABLE [dim].[ILSVare]")
        end = text.index("PK_Dim_ILSVare", start)
        block = text[start:end]
        assert "[vareart]" not in block
        assert "[varegruppe]" not in block

    def test_create_omits_varenavn_concat(self):
        # Redundant computed column removed from the model.
        text = DDL.read_text(encoding="utf-8")
        start = text.index("CREATE TABLE [dim].[ILSVare]")
        end = text.index("PK_Dim_ILSVare", start)
        assert "[varenavn_concat]" not in text[start:end]


class TestMigration:
    def test_lives_in_autorun_dir(self):
        # Must be in the centralized auto-run dir, not the manual one-off dirs.
        assert MIG.exists()
        assert not (
            REPO / "migrations" / "staging" / "2026-06-25_28149_add-ilsvare-columns.sql"
        ).exists()
        assert not (
            REPO / "migrations" / "dso" / "2026-06-25_28149_add-ilsvare-columns.sql"
        ).exists()

    def test_adds_each_new_column_idempotently(self):
        text = MIG.read_text(encoding="utf-8")
        for col in NEW_DIM_COLS:
            assert f"COLUMN_NAME = '{col}'" in text, col
            assert f"ADD [{col}]" in text, col

    def test_renames_swap_columns_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        for old, new in RENAMES:
            assert f"'dim.IlsVare.{old}', '{new}', 'COLUMN'" in text, (old, new)
            # Rename must be double-guarded so a re-run is a no-op.
            assert f"COLUMN_NAME='{old}'" in text, old
            assert f"COLUMN_NAME='{new}'" in text, new

    def test_drops_ext_enriched_guarded_for_reload(self):
        text = MIG.read_text(encoding="utf-8")
        assert "DROP TABLE [ext].[vare_enriched_ils]" in text
        # Guarded so it is a safe no-op on DSO (no ext schema there).
        assert "OBJECT_ID(N'[ext].[vare_enriched_ils]')" in text

    def test_drops_redundant_varenavn_concat_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        assert "DROP COLUMN [Varenavn_Concat]" in text
        assert "COLUMN_NAME = 'Varenavn_Concat'" in text
