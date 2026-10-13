# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contracts for dim.Dato ADO 29286 schema and refresh scripts."""

from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO = Path(__file__).resolve().parents[2]
DDL = REPO / "infrastructure" / "ddl" / "tables" / "shared_dimensions" / "02_dims.sql"
MIG = REPO / "infrastructure" / "ddl" / "migrations" / "20260819_dim_dato_calendar_expansion.sql"
REL = REPO / "infrastructure" / "ddl" / "tables" / "shared_dimensions" / "05_dato_relativ.sql"
RENAME_MIG = (
    REPO / "infrastructure" / "ddl" / "migrations" / "20260904_02_dim_dato_kvartal_kolonnenavne.sql"
)


def test_create_ddl_includes_new_dim_dato_columns():
    text = DDL.read_text(encoding="utf-8")
    for col in [
        "[dato]",
        "[aar_startdato]",
        "[aar_afsluttet]",
        "[kvartal_startdato]",
        "[kvartal_slutdato]",
        "[kvartal_afsluttet]",
        "[maaned_lang]",
        "[maaned]",
        "[uge_startdato]",
        "[uge_afsluttet]",
        "[dag_kvartal]",
        "[dag_relativ]",
        "[ugedag_lang]",
        "[ugedag]",
        "[er_hverdag]",
        "[hverdag_weekend]",
        "[er_helligdag]",
        "[helligdag]",
        "[er_beredskabshelligdag]",
        "[beredskabshelligdag]",
        "[aar_relativ]",
        "[kvartal_relativ]",
        "[maaned_relativ]",
        "[uge_relativ]",
    ]:
        assert col in text, col


def test_create_ddl_drops_legacy_weekend_and_uses_int_kvartal_aar_id():
    text = DDL.read_text(encoding="utf-8")
    assert "[weekend]" not in text
    assert "[kvartal_aar_id]          INT NULL" in text


def test_create_ddl_seeds_four_sentinels():
    text = DDL.read_text(encoding="utf-8")
    for key in ("-1", "-2", "99999998", "99999999"):
        assert f"        {key}," in text


def test_migration_handles_blank_sentinel_and_weekend_drop():
    text = MIG.read_text(encoding="utf-8")
    assert "99999998" in text
    assert "DROP COLUMN [weekend]" in text
    assert "sp_rename 'dim.Dato.Dato', 'dato', 'COLUMN'" in text


def test_migration_skips_when_dim_dato_missing():
    text = MIG.read_text(encoding="utf-8")
    assert "IS NULL\n    RETURN;" not in text
    guard = "IF OBJECT_ID(N'[dim].[Dato]', 'U') IS NOT NULL"
    assert text.count(guard) >= 5
    add_section = text.split("-- Add new columns from ADO 29286")[1].split("GO", 1)[0]
    assert guard in add_section
    insert_section = text.split("-- Ensure BLANK sentinel row exists")[1].split("GO", 1)[0]
    assert guard in insert_section
    backfill_section = text.split("-- Backfill calendar attributes for real date rows")[1].split(
        "GO", 1
    )[0]
    assert guard in backfill_section


def test_relative_refresh_contract():
    text = REL.read_text(encoding="utf-8")
    assert "Central European Standard Time" in text
    assert "WHERE d.[dw_ek_Dato] BETWEEN 19700101 AND 20501231" in text
    for col in [
        "[aar_afsluttet]",
        "[kvartal_afsluttet]",
        "[maaned_afsluttet]",
        "[uge_afsluttet]",
        "[aar_relativ]",
        "[kvartal_relativ]",
        "[maaned_relativ]",
        "[uge_relativ]",
        "[dag_relativ]",
    ]:
        assert col in text


def test_no_legacy_kvartals_prefix_in_create_ddl():
    """The quarter columns must follow <periode>_startdato like aar/maaned/uge."""
    text = DDL.read_text(encoding="utf-8")
    assert "kvartals_startdato" not in text
    assert "kvartals_slutdato" not in text


def test_rename_migration_is_guarded_and_idempotent():
    text = RENAME_MIG.read_text(encoding="utf-8")
    for old, new in (
        ("kvartals_startdato", "kvartal_startdato"),
        ("kvartals_slutdato", "kvartal_slutdato"),
    ):
        # Renames only when the old name is still present ...
        assert f"AND name = '{old}'" in text
        # ... and the new name is not already taken (re-run safety).
        assert f"AND name = '{new}'" in text
        assert f"EXEC sp_rename '[dim].[Dato].[{old}]', '{new}', 'COLUMN';" in text
