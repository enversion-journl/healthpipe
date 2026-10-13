# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Prisloese aftalelinjer + bridge.AftaleLager (spec 2026-08-31): CREATE +
migration paritet.

Migrationen ligger i den centrale auto-run-mappe og appliceres mod BAADE
staging og DSO. Basis-DDL'en (03_facts.sql, 02_dims.sql, 05_bridges.sql) skal
baere samme netto-tilstand, da friske databaser baseline-stemples uden at
koere migrationen. bridge.AftaleLager selv har INGEN migrations-DDL — den nye
tabel oprettes udelukkende af den guardede base-DDL, som infra-assets koerer
hver gang.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
FACTS_DDL = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "03_facts.sql"
DIMS_DDL = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"
BRIDGES_DDL = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "05_bridges.sql"
MIG = (
    REPO / "infrastructure" / "ddl" / "migrations" / "20260831_prisloese_linjer_og_aftalelager.sql"
)


class TestCreateDdl:
    def test_facts_has_har_prisversion_column(self):
        text = FACTS_DDL.read_text(encoding="utf-8")
        col_idx = text.index("[har_prisversion] BIT NOT NULL")
        segment = text[col_idx : col_idx + 200]
        assert "DF_AftaleVarePris_har_prisversion" in segment
        assert "DEFAULT 1" in segment

    def test_dims_aftale_has_alle_maa_benytte_column(self):
        text = DIMS_DDL.read_text(encoding="utf-8")
        col_idx = text.index("[alle_maa_benytte] BIT NOT NULL")
        segment = text[col_idx : col_idx + 200]
        assert "DF_Aftale_alle_maa_benytte" in segment
        assert "DEFAULT 1" in segment

    def test_bridges_has_aftalelager_table(self):
        text = BRIDGES_DDL.read_text(encoding="utf-8")
        assert "[bridge].[AftaleLager]" in text
        for col in (
            "[dw_sk_AftaleLager]",
            "[dw_ek_Aftale]",
            "[dw_ek_Lager]",
            "[clean_code_Aftale]",
            "[sgh_raw]",
            "[sygehus]",
            "[lager]",
            "[maa_benyttes]",
            "[foerste_prioritet]",
            "[dw_is_deleted]",
        ):
            assert col in text, col

    def test_bridges_aftalelager_has_unique_index_on_clean_code_and_sgh_raw(self):
        text = BRIDGES_DDL.read_text(encoding="utf-8")
        aftalelager_start = text.index("[bridge].[AftaleLager]")
        segment = text[aftalelager_start : aftalelager_start + 4000]
        assert "UNIQUE NONCLUSTERED" in segment
        assert "clean_code_Aftale" in segment and "sgh_raw" in segment

    def test_bridges_aftalelager_has_index_on_dw_ek_aftale(self):
        """Review-fund: dw_ek_Aftale er den primaere join-vej fra dim.Aftale
        ind i bridge.AftaleLager — uden indeks scanner ethvert opslag fra
        dim.Aftale hele bridge-tabellen."""
        text = BRIDGES_DDL.read_text(encoding="utf-8")
        assert "IX_Bridge_AftaleLager_Aftale" in text
        idx_start = text.index("IX_Bridge_AftaleLager_Aftale")
        segment = text[idx_start : idx_start + 300]
        assert "[dw_ek_Aftale] ASC" in segment

    def test_facts_has_linjenoegle_index_for_predelete_sweep(self):
        """Review-fund: pre-delete-sweepen (PRE_DELETE_SQL) skopes paa
        linjenoeglen (bk_leverandoernummer, bk_aftale_nummer,
        bk_ils_varenummer) — uden et understoettende indeks scanner sweepen
        hele tabellen hver koersel. Spec'ens "No index changes" veg for
        denne ruling."""
        text = FACTS_DDL.read_text(encoding="utf-8")
        assert "IX_Fct_AftaleVarePris_linjenoegle" in text
        idx_start = text.index("IX_Fct_AftaleVarePris_linjenoegle")
        segment = text[idx_start : idx_start + 300]
        assert "[bk_leverandoernummer]" in segment
        assert "[bk_aftale_nummer]" in segment
        assert "[bk_ils_varenummer]" in segment


class TestMigration:
    def test_migration_exists(self):
        assert MIG.exists()

    def test_migration_is_table_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        assert "OBJECT_ID(N'[fct].[AftaleVarePris]'" in text
        assert "OBJECT_ID(N'[dim].[Aftale]'" in text

    def test_migration_adds_har_prisversion_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        assert "COL_LENGTH('[fct].[AftaleVarePris]', 'har_prisversion') IS NULL" in text
        assert "ALTER TABLE [fct].[AftaleVarePris] ADD [har_prisversion]" in text

    def test_migration_adds_alle_maa_benytte_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        assert "COL_LENGTH('[dim].[Aftale]', 'alle_maa_benytte') IS NULL" in text
        assert "ALTER TABLE [dim].[Aftale] ADD [alle_maa_benytte]" in text

    def test_migration_resets_aftalevarepris_checkpoint(self):
        text = MIG.read_text(encoding="utf-8")
        assert "meta.DFMState" in text
        assert "scope = ''staging''" in text
        assert "schema_name = ''fct'' AND table_name = ''AftaleVarePris''" in text

    def test_migration_adds_linjenoegle_index_guarded(self):
        """Samme indeks som base-DDL'en, men indeks-guardet (sys.indexes) i
        migrationen saa eksisterende tabeller ogsaa faar det."""
        text = MIG.read_text(encoding="utf-8")
        assert "IX_Fct_AftaleVarePris_linjenoegle" in text
        assert "sys.indexes" in text
        idx_start = text.index("CREATE NONCLUSTERED INDEX [IX_Fct_AftaleVarePris_linjenoegle]")
        segment = text[idx_start : idx_start + 300]
        assert "[bk_leverandoernummer]" in segment
        assert "[bk_aftale_nummer]" in segment
        assert "[bk_ils_varenummer]" in segment

    def test_migration_has_no_bridge_create_table(self):
        # bridge.AftaleLager oprettes udelukkende af base-DDL'en — ny tabel,
        # ingen tabel-DDL-migration noedvendig. Migrationen maa gerne NAEVNE
        # navnet (forklarende kommentarer + den guardede
        # IX_Bridge_AftaleLager_Aftale-tilfoejelse, review-fund), men maa
        # ikke indeholde noget der opretter tabellen. Kommentarlinjer
        # ekskluderes (jf. test_no_truncate), saa forklarende prosa om
        # fravaeret ikke selv trigger tjekket.
        sql_lines = [
            ln
            for ln in MIG.read_text(encoding="utf-8").splitlines()
            if not ln.lstrip().startswith("--")
        ]
        assert "CREATE TABLE" not in "\n".join(sql_lines).upper()

    def test_migration_adds_bridge_aftalelager_aftale_index_guarded(self):
        """Samme indeks som base-DDL'en (IX_Bridge_AftaleLager_Aftale paa
        dw_ek_Aftale), men guardet paa BAADE tabel- og indeks-eksistens saa
        en dev-DB der allerede har oprettet bridge.AftaleLager fra en
        tidligere commit (foer indekset fandtes i base-DDL'en) ogsaa faar
        det — base-DDL'ens CREATE-blok koerer ikke igen paa en tabel der
        allerede findes."""
        text = MIG.read_text(encoding="utf-8")
        assert "OBJECT_ID(N'[bridge].[AftaleLager]', 'U') IS NOT NULL" in text
        assert "IX_Bridge_AftaleLager_Aftale" in text
        assert "sys.indexes" in text
        idx_start = text.index("CREATE NONCLUSTERED INDEX [IX_Bridge_AftaleLager_Aftale]")
        segment = text[idx_start : idx_start + 200]
        assert "[dw_ek_Aftale] ASC" in segment

    def test_migration_has_no_cursor_batches(self):
        # execute_sql dropper CURSOR-batches stille — trackeren ville stemple
        # en no-op som applied (memory: migration-silent-failure-nextset).
        assert "CURSOR" not in MIG.read_text(encoding="utf-8").upper()

    def test_no_truncate(self):
        sql_lines = [
            ln
            for ln in MIG.read_text(encoding="utf-8").splitlines()
            if not ln.lstrip().startswith("--")
        ]
        assert "TRUNCATE" not in "\n".join(sql_lines).upper()
