# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the dim.ILSLeverandoer chain.

The column list is taken from the base DDL, not hard-coded, so a column added
to dim.ILSLeverandoer without wiring it through staging and the MERGE fails
here instead of as "Invalid column name" or a silently stale attribute at
runtime.
"""

import re
from pathlib import Path

import pytest
from data_lineage.tools.lineage_ddl_check.ddl_parser import parse_ddl
from DSA.SQL.common.ils_leverandoer_clean_code import (
    sql_clean_code_ils_leverandoer,
    sql_kreditor_clean_code_from_lnr,
)
from DSA.SQL.dim.dim_ILSLeverandoer import _CHANGE_CHECK, MERGE_SQL, SOURCE_SQL
from DSA.SQL.staging.stg_dim_ILSLeverandoer import CLEAN_CODE_SQL, STG_SQL

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]
TABLES_DIR = ROOT / "infrastructure" / "ddl" / "tables"

_AUDIT = {"dw_timestamp_insert", "dw_timestamp_update"}
_PK = "dw_ek_ILSLeverandoer"


def _dim_columns() -> list[str]:
    return list(parse_ddl(TABLES_DIR, ("dim",))["dim.ILSLeverandoer"].columns)


def _attribute_columns() -> list[str]:
    return [c for c in _dim_columns() if c not in _AUDIT and c != _PK]


def _stg_output_columns() -> set[str]:
    """Column names of the outer SELECT that creates stg.dim_ILSLeverandoer."""
    outer = STG_SQL.split("INTO [stg].[dim_ILSLeverandoer]")[0]
    outer = outer.split("SELECT", 1)[1]
    names = set()
    for item in outer.split(","):
        item = item.strip()
        match = re.search(r"\bAS\s+(\w+)$", item)
        names.add(match.group(1) if match else item)
    return names


def test_merge_updates_guards_and_inserts_every_attribute():
    update, insert = MERGE_SQL.split("WHEN NOT MATCHED BY TARGET")
    insert_columns, insert_values = insert.split("VALUES")
    for col in _attribute_columns():
        assert re.search(rf"\btgt\.{col} = src\.{col}\b", update), col
        assert re.search(rf"\btgt\.{col}\b", _CHANGE_CHECK), col
        assert re.search(rf"\bsrc\.{col}\b", _CHANGE_CHECK), col
        assert re.search(rf"^\s*{col},?$", insert_columns, flags=re.MULTILINE), col
        assert re.search(rf"\bsrc\.{col}\b", insert_values), col


def test_source_select_produces_every_dim_column():
    produced = set(re.findall(r"\bAS\s+(\w+),?\s*$", SOURCE_SQL, flags=re.MULTILINE))
    produced |= set(re.findall(r"^\s*[ms]\.(\w+),?\s*$", SOURCE_SQL, flags=re.MULTILINE))
    missing = set(_dim_columns()) - _AUDIT - produced
    assert not missing, missing


def test_source_select_only_reads_columns_staging_produces():
    read_from_stg = set(re.findall(r"\bs\.(\w+)", SOURCE_SQL))
    missing = read_from_stg - _stg_output_columns()
    assert not missing, missing


def test_dedup_partitions_on_the_minted_clean_code():
    # Partitioning on raw lnr would let 'abc' and 'ABC ' both survive the
    # ROW_NUMBER and then collide on the staging table's unique index.
    assert f"PARTITION BY {CLEAN_CODE_SQL}" in STG_SQL


def test_clean_code_is_null_when_lnr_is_blank():
    # A blank lnr must not mint the bare 'ILSLEV_' prefix as a map member.
    expr = sql_clean_code_ils_leverandoer("lev.lnr")
    assert re.search(r"WHEN NULLIF\(.*lev\.lnr.*, ''\) IS NOT NULL", expr)
    assert "ELSE NULL" in expr


# A statement that writes map.Kreditor, whatever its spacing, casing or brackets.
WRITES_MAP_KREDITOR = re.compile(
    r"\b(INSERT\s+INTO|MERGE(\s+INTO)?|INTO|UPDATE|DELETE\s+FROM)\s+\[?map\]?\.\[?Kreditor\]?(?!\w)",
    re.IGNORECASE,
)
# Every join to map.Kreditor, with the join type in front of it (empty = inner).
JOINS_MAP_KREDITOR = re.compile(
    r"(\w+)?\s+JOIN\s+\[?map\]?\.\[?Kreditor\]?(?!\w)",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO [map].[Kreditor] (clean_code_Kreditor) VALUES ('CVR_1')",
        "insert into map.Kreditor(clean_code_Kreditor) select 1",
        "MERGE [map].[Kreditor] AS target USING src ON 1 = 1",
        "MERGE INTO map.Kreditor AS target",
        "SELECT * INTO  [map].[Kreditor] FROM x",
        "UPDATE [map].Kreditor SET dw_timestamp_update = 1",
        "DELETE FROM [map].[Kreditor] WHERE 1 = 1",
    ],
)
def test_map_kreditor_write_guard_catches_every_form(statement):
    assert WRITES_MAP_KREDITOR.search(statement), statement


def test_outrigger_candidates_are_looked_up_never_minted():
    # The cascade may only look a CVR up in map.Kreditor, never create one: no
    # INSERT INTO / MERGE / SELECT INTO / UPDATE / DELETE against map.Kreditor
    # in the staging, source and MERGE SQL this test imports (map_ILSLeverandoer
    # writes map.ILSLeverandoer only). Every join to map.Kreditor is a LEFT JOIN:
    # an inner join would drop every lnr without a CVR, the reason the
    # dimension exists.
    assert not WRITES_MAP_KREDITOR.search(SOURCE_SQL + MERGE_SQL + STG_SQL)
    join_types = [m.group(1) for m in JOINS_MAP_KREDITOR.finditer(SOURCE_SQL)]
    assert join_types
    assert all((t or "").upper() == "LEFT" for t in join_types), join_types


@pytest.mark.parametrize("role", ["se_nr", "cvr_nr", "lnr"])
def test_person_rows_get_no_kreditor_candidate(role):
    # giro = 'Person' rows carry birth dates or CPR numbers in se_nr; a
    # candidate from them would link a person to a CVR-keyed dim.Kreditor row.
    alias = f"AS clean_code_Kreditor_{role}"
    inner = STG_SQL.split("INTO [stg].[dim_ILSLeverandoer]")[1]
    before_alias = inner[: inner.index(alias)]
    expression = before_alias[before_alias.rindex(",\n") :]
    assert "CASE WHEN lev.giro = 'Person' THEN NULL ELSE" in expression


def _like_pattern(expr: str) -> re.Pattern:
    """The bracket-only LIKE pattern in the lnr candidate, as an anchored regex."""
    pattern = re.search(r"LIKE '([^']*)'", expr).group(1)
    assert "%" not in pattern and "_" not in pattern
    return re.compile(pattern)


@pytest.mark.parametrize(
    "lnr, is_cvr",
    [
        ("25682742", True),
        ("10000000", True),
        ("99999999", True),
        ("01234567", False),  # leading zero: below 10000000
        ("1234567", False),  # seven digits
        ("123456789", False),  # nine digits, e.g. the internal 777777777
        ("K100252427", False),  # lettered supplier number
        ("2568274A", False),
    ],
)
def test_lnr_candidate_only_for_eight_digit_numbers(lnr, is_cvr):
    expr = sql_kreditor_clean_code_from_lnr("lev.lnr", "lev.land")
    assert bool(_like_pattern(expr).fullmatch(lnr)) is is_cvr


def test_lnr_candidate_requires_danish_supplier():
    expr = sql_kreditor_clean_code_from_lnr("lev.lnr", "lev.land")
    assert "AND LTRIM(RTRIM(lev.land)) = 'DK'" in expr
