# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""fct.AftaleVarePris lvnr-fanout (spec 2026-08-27): CREATE + migration paritet.

Migrationen ligger i den centrale auto-run-mappe og appliceres mod BAADE
staging og DSO. Basis-DDL'en (03_facts.sql) skal baere samme netto-tilstand,
da friske databaser baseline-stemples uden at koere migrationen.
"""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]  # etl/
DDL = REPO / "infrastructure" / "ddl" / "tables" / "erp" / "03_facts.sql"
MIG = REPO / "infrastructure" / "ddl" / "migrations" / "20260827b_aftalevarepris_lvnr_fanout.sql"

DROPPED_COLS = ["kreditorvarenummer1", "kreditorvarenummer2", "kreditorvarenummer3"]


class TestCreateDdl:
    def test_create_has_kilder_column(self):
        assert "[kilder]" in DDL.read_text(encoding="utf-8")

    def test_create_has_no_kreditorvarenummer_columns(self):
        text = DDL.read_text(encoding="utf-8")
        for col in DROPPED_COLS:
            assert f"[{col}]" not in text, col

    def test_create_has_composite_grain_index(self):
        text = DDL.read_text(encoding="utf-8")
        assert "IX_Fct_AftaleVarePris_grain" in text
        assert "IX_Fct_AftaleVarePris_bk_prrowid" not in text


class TestMigration:
    def test_migration_exists(self):
        assert MIG.exists()

    def test_migration_is_table_guarded(self):
        text = MIG.read_text(encoding="utf-8")
        assert "OBJECT_ID(N'[fct].[AftaleVarePris]'" in text

    def test_migration_adds_kilder_and_drops_columns(self):
        text = MIG.read_text(encoding="utf-8")
        assert "[kilder]" in text
        for col in DROPPED_COLS:
            assert col in text, col

    def test_migration_swaps_unique_index(self):
        text = MIG.read_text(encoding="utf-8")
        assert "IX_Fct_AftaleVarePris_bk_prrowid" in text  # dropped
        assert "IX_Fct_AftaleVarePris_grain" in text  # created

    def test_migration_has_no_cursor_batches(self):
        # execute_sql dropper CURSOR-batches stille — trackeren ville stemple
        # en no-op som applied (memory: migration-silent-failure-nextset).
        assert "CURSOR" not in MIG.read_text(encoding="utf-8").upper()


CP_MIG = (
    REPO
    / "infrastructure"
    / "ddl"
    / "migrations"
    / "20260827c_reset_dfm_checkpoints_lvnr_fanout.sql"
)


class TestCheckpointResetMigration:
    """Backfill-triggeren: nulstiller de tre forbruger-checkpoints saa foerste
    ordinaere koersel efter deploy selv backfiller alternativerne (map/dim/
    bridge) og genopbygger fct under det nye grain."""

    def test_migration_exists(self):
        assert CP_MIG.exists()

    def test_resets_exactly_the_three_consumer_checkpoints(self):
        text = CP_MIG.read_text(encoding="utf-8")
        for schema, table in (
            ("dim", "Leverandoervare"),
            ("bridge", "LeverandoervareILSVare"),
            ("fct", "AftaleVarePris"),
        ):
            assert f"schema_name = ''{schema}'' AND table_name = ''{table}''" in text, (
                schema,
                table,
            )
        # Kun staging-scope — DSO-checkpoints maa ikke beroeres
        assert text.count("scope = ''staging''") == 3
        assert text.count("DELETE FROM meta.DFMState") == 3

    def test_migration_is_guarded_and_cursor_free(self):
        text = CP_MIG.read_text(encoding="utf-8")
        # Hver DELETE bag en OBJECT_ID-vagt (no-op paa DSO/friske databaser)
        assert text.count("IF OBJECT_ID('meta.DFMState', 'U') IS NOT NULL") == 3
        assert "CURSOR" not in text.upper()

    def test_no_truncate(self):
        # Reconciliation sker via MERGE + soft delete — aldrig TRUNCATE
        # (dw_sk-stabilitet mod DSO, dw_ek-stabilitet i map-tabellerne).
        # Kun SQL-linjer tjekkes; migrationens kommentar naevner selv TRUNCATE.
        sql_lines = [
            ln
            for ln in CP_MIG.read_text(encoding="utf-8").splitlines()
            if not ln.lstrip().startswith("--")
        ]
        assert "TRUNCATE" not in "\n".join(sql_lines).upper()
