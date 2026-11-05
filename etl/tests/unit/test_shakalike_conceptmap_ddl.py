# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard the conceptmap.SHAKAlikeToSOR DDL nullability contract."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_STAGING_DDL = _DDL_DIR / "tables" / "conceptmap" / "01_conceptmap_tables.sql"
_DSO_DDL = _DDL_DIR / "tables" / "conceptmap" / "dso" / "01_conceptmap_tables.sql"
_VIEW_DDL = _DDL_DIR / "views" / "conceptmap" / "01_vw_conceptmap.sql"
_MIGRATION = _DDL_DIR / "migrations" / "20260903_01_shakalike_codesystem_conceptmap.sql"


def _block(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE \[conceptmap\]\.\[SHAKAlikeToSOR\] \((.*?)\);",
        text,
        re.DOTALL,
    )
    assert m, f"SHAKAlikeToSOR CREATE TABLE not found in {path.name}"
    return m.group(1)


def _column_line(block: str, column: str) -> str:
    for line in block.splitlines():
        if f"[{column}]" in line:
            return line
    raise AssertionError(f"column {column} not found in block")


def test_staging_dw_ek_columns_are_nullable():
    block = _block(_STAGING_DDL)
    for col in ("dw_ek_SHAKAlike", "dw_ek_Organisation_target"):
        line = _column_line(block, col)
        assert "NOT NULL" not in line, f"staging SHAKAlikeToSOR.{col} must be nullable"
        assert re.search(r"\bNULL\b", line)


def test_staging_pk_is_natural_key():
    block = _block(_STAGING_DDL)
    assert "PRIMARY KEY CLUSTERED ([source_code], [target_code])" in block


def test_dso_dw_ek_is_notnull_pk():
    block = _block(_DSO_DDL)
    line = _column_line(block, "dw_ek_SHAKAlike")
    assert "NOT NULL" in line
    assert "PRIMARY KEY CLUSTERED ([dw_ek_SHAKAlike])" in block


def test_view_keeps_unresolved_target_rows():
    text = _VIEW_DDL.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE OR ALTER VIEW \[conceptmap\]\.\[vw_shakalike_to_sor\] AS(.*?)(?:\nGO\b|\Z)",
        text,
        re.DOTALL,
    )
    assert m, "vw_shakalike_to_sor not found"
    view = m.group(1)
    assert "[dw_ek_SHAKAlike] IS NOT NULL" in view
    assert "[dw_ek_Organisation_target] IS NOT NULL" not in view


def test_migration_exists():
    assert _MIGRATION.is_file()
    sql = _MIGRATION.read_text(encoding="utf-8")
    assert "SHAKAlikeToSOR" in sql
    assert "vw_shakalike_to_sor" in sql
    assert "Unknown SHAK-a-like" in sql


def test_migration_arc_epji_location_is_staging_only():
    """dso_migrations runs this same file; arc.* must not leak onto DSO."""
    sql = _MIGRATION.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE \[arc\]\.\[epji_location\]",
        sql,
    )
    assert m, "arc.epji_location CREATE TABLE not found in migration"
    preceding = sql[: m.start()]
    guard_pos = preceding.rfind("DB_NAME() LIKE N'%staging%'")
    begin_pos = preceding.rfind("BEGIN")
    assert guard_pos != -1 and guard_pos > begin_pos - 200, (
        "arc.epji_location CREATE TABLE must be guarded by "
        "DB_NAME() LIKE N'%staging%' — this migration runs via the shared "
        "migrations/ folder on both DBs, unlike the staging-only baseline "
        "in sources/epji/epji_location.sql"
    )


def test_migration_vw_shakalike_to_sor_is_staging_only():
    """views/conceptmap/ is staging-only baseline; the migration must match."""
    sql = _MIGRATION.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE OR ALTER VIEW \[conceptmap\]\.\[vw_shakalike_to_sor\]",
        sql,
    )
    assert m, "vw_shakalike_to_sor CREATE VIEW not found in migration"
    preceding = sql[: m.start()]
    guard_pos = preceding.rfind("DB_NAME() LIKE N'%staging%'")
    assert guard_pos != -1 and m.start() - guard_pos < 400, (
        "vw_shakalike_to_sor creation must be guarded by "
        "DB_NAME() LIKE N'%staging%' (views/conceptmap/ is staging-only baseline)"
    )
