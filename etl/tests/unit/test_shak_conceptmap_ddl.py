# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard the conceptmap.SHAKToSOR DDL nullability contract.

cm_shak_to_sor MERGEs mapping rows before update_conceptmap_dw_ek resolves the
surrogate keys, so on STAGING the dw_ek_* columns MUST be nullable — otherwise
the first insert fails with "Cannot insert the value NULL into column
'dw_ek_SHAK'". On DSO the table keeps dw_ek_SHAK as a NOT NULL primary key
(only resolved rows are synced via vw_shak_to_sor).
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

_DDL_DIR = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_STAGING_DDL = _DDL_DIR / "tables" / "conceptmap" / "01_conceptmap_tables.sql"
_DSO_DDL = _DDL_DIR / "tables" / "conceptmap" / "dso" / "01_conceptmap_tables.sql"
_MIGRATION = _DDL_DIR / "migrations" / "20260701_shaksor_dw_ek_nullable.sql"
_VIEW_DDL = _DDL_DIR / "views" / "conceptmap" / "01_vw_conceptmap.sql"


def _shaktosor_block(path: Path) -> str:
    text = path.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE TABLE \[conceptmap\]\.\[SHAKToSOR\] \((.*?)\);",
        text,
        re.DOTALL,
    )
    assert m, f"SHAKToSOR CREATE TABLE not found in {path.name}"
    return m.group(1)


def _column_line(block: str, column: str) -> str:
    for line in block.splitlines():
        if f"[{column}]" in line:
            return line
    raise AssertionError(f"column {column} not found in block")


def test_staging_dw_ek_columns_are_nullable():
    block = _shaktosor_block(_STAGING_DDL)
    for col in ("dw_ek_SHAK", "dw_ek_Organisation_target"):
        line = _column_line(block, col)
        assert (
            "NOT NULL" not in line
        ), f"staging SHAKToSOR.{col} must be nullable, got: {line.strip()}"
        assert re.search(
            r"\bNULL\b", line
        ), f"staging SHAKToSOR.{col} should be INT NULL: {line.strip()}"


def test_staging_pk_is_natural_key():
    block = _shaktosor_block(_STAGING_DDL)
    assert "PRIMARY KEY CLUSTERED ([source_code], [target_code])" in block


def test_dso_dw_ek_shak_is_notnull_pk():
    block = _shaktosor_block(_DSO_DDL)
    line = _column_line(block, "dw_ek_SHAK")
    assert "NOT NULL" in line, f"DSO SHAKToSOR.dw_ek_SHAK must stay NOT NULL: {line.strip()}"
    assert "PRIMARY KEY CLUSTERED ([dw_ek_SHAK])" in block


def test_view_keeps_closed_target_rows():
    # ~56% of SHAK codes map to CLOSED SOR entities with no dw_ek_Organisation_target;
    # vw_shak_to_sor must NOT filter on it (only the source key + relationship),
    # otherwise those SHAK codes would be dropped from DSO/FHIR.
    text = _VIEW_DDL.read_text(encoding="utf-8")
    m = re.search(
        r"CREATE OR ALTER VIEW \[conceptmap\]\.\[vw_shak_to_sor\] AS(.*?)(?:\nGO\b|\Z)",
        text,
        re.DOTALL,
    )
    assert m, "vw_shak_to_sor not found"
    view = m.group(1)
    assert "[dw_ek_SHAK] IS NOT NULL" in view
    # The relaxed filter must NOT gate on the target org key (the other *-to-sor
    # views do); otherwise closed-SOR SHAK codes are dropped.
    assert "[dw_ek_Organisation_target] IS NOT NULL" not in view, (
        "vw_shak_to_sor must not filter on dw_ek_Organisation_target "
        "(would drop SHAK codes mapping to closed SOR entities)"
    )


def test_nullable_migration_exists_and_is_staging_scoped():
    assert _MIGRATION.is_file(), "SHAKToSOR nullability migration is missing"
    sql = _MIGRATION.read_text(encoding="utf-8")
    assert "DB_NAME() LIKE" in sql, "migration must be scoped to the staging DB"
    assert "ALTER COLUMN [DW_EK_SHAK] INT NULL" in sql
    assert "ALTER COLUMN [DW_EK_Organisation_Target] INT NULL" in sql
