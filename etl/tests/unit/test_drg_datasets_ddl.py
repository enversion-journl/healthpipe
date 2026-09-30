# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard: the DRG archive DDL must match the dataset specification.

The DDL (infrastructure/ddl/tables/drg/00_archive.sql) is generated from
DSI/csv_files/drg_datasets.py. This test keeps the two in lockstep: columns
(name + order), primary keys, NOT NULL on key columns, and that dropped
volatile IDs NEVER appear in arc (they go out of sync across tables after a
merge).
"""

import re
from pathlib import Path

import pytest
from DSI.csv_files.drg_datasets import DATASETS

pytestmark = pytest.mark.unit

DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "drg"
    / "00_archive.sql"
)

TAIL_COLUMNS = ("leverance_dato", "dw_is_deleted", "dw_timestamp_insert", "dw_timestamp_update")


def _parse_tables(sql: str) -> dict[str, dict]:
    """Return {table_name: {'columns': [...], 'pk': [...], 'not_null': {...}}}."""
    tables: dict[str, dict] = {}
    pattern = re.compile(
        r"CREATE TABLE \[arc\]\.\[(?P<name>\w+)\] \((?P<body>.*?)\n    \);",
        re.DOTALL,
    )
    for m in pattern.finditer(sql):
        body = m.group("body")
        columns: list[str] = []
        not_null: set[str] = set()
        for line in body.splitlines():
            line = line.strip()
            col = re.match(
                r"\[(\w+)\]\s+(NVARCHAR\(\d+\)|DATE|BIT|DATETIME2\(0\)|BIGINT[^,]*)\s+(NOT NULL|NULL)",
                line,
            )
            if col:
                columns.append(col.group(1))
                if col.group(3) == "NOT NULL":
                    not_null.add(col.group(1))
        pk_match = re.search(r"PRIMARY KEY CLUSTERED \(\s*(.*?)\s*\)", body, re.DOTALL)
        pk = re.findall(r"\[(\w+)\]", pk_match.group(1)) if pk_match else []
        tables[m.group("name")] = {"columns": columns, "pk": pk, "not_null": not_null}
    return tables


@pytest.fixture(scope="module")
def ddl_tables() -> dict[str, dict]:
    return _parse_tables(DDL_PATH.read_text(encoding="utf-8"))


def test_alle_datasaet_har_en_arc_tabel(ddl_tables):
    missing = [d.ext_table for d in DATASETS if d.ext_table not in ddl_tables]
    assert not missing, f"arc-DDL mangler tabeller: {missing}"


def test_kolonner_matcher_spec(ddl_tables):
    for d in DATASETS:
        expected = ["takstsystem", *d.arc_columns, *TAIL_COLUMNS]
        actual = ddl_tables[d.ext_table]["columns"]
        assert actual == expected, (
            f"arc.{d.ext_table}: kolonnerne i DDL matcher ikke drg_datasets.py.\n"
            f"  DDL:  {actual}\n  Spec: {expected}"
        )


def test_primaernoegle_matcher_merge_keys(ddl_tables):
    for d in DATASETS:
        expected = ["takstsystem", *d.merge_keys]
        assert (
            ddl_tables[d.ext_table]["pk"] == expected
        ), f"arc.{d.ext_table}: PK er {ddl_tables[d.ext_table]['pk']}, forventede {expected}"


def test_noeglekolonner_er_not_null(ddl_tables):
    for d in DATASETS:
        not_null = ddl_tables[d.ext_table]["not_null"]
        for key in ("takstsystem", *d.merge_keys):
            assert key in not_null, f"arc.{d.ext_table}: noeglekolonnen {key} skal vaere NOT NULL"


def test_droppede_idkolonner_findes_ikke_i_arc(ddl_tables):
    for d in DATASETS:
        leaked = set(d.arc_dropped) & set(ddl_tables[d.ext_table]["columns"])
        assert (
            not leaked
        ), f"arc.{d.ext_table}: volatile ID-/DW-kolonner er utilsigtet med i DDL: {sorted(leaked)}"


def test_default_constraints_er_navngivet():
    """DEFAULT constraints must be named DF_<table>_<column>, not auto-generated."""
    sql = DDL_PATH.read_text(encoding="utf-8")
    assert "NOT NULL DEFAULT (0)" not in sql
    assert "NOT NULL DEFAULT SYSDATETIME()" not in sql
    assert "CONSTRAINT [DF_drg_kontakt_dw_is_deleted]" in sql
    assert "CONSTRAINT [DF_drg_pris_revision_dw_timestamp_insert]" in sql


def test_psyk_helbredsforloeb_undtagelsen():
    """DW_EK_Helbredsforloeb is kept ONLY on the psyk tables (no forloeb UUID there)."""
    for d in DATASETS:
        if d.ext_table in ("drgpsyk_kontakt", "drgpsyk_forloeb"):
            assert "DW_EK_Helbredsforloeb" in d.arc_columns
        else:
            assert "DW_EK_Helbredsforloeb" not in d.arc_columns
            assert "DW_EK_HelbredsForloeb" not in d.arc_columns
