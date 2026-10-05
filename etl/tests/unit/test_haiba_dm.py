# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the dm HAIBA layer: SOR resolution to dw_ek_Organisation.

The bugs these guard against are silent — a wrong column name or a missing
COALESCE produces a table full of NULLs or a failed run halfway through a
multi-million-row rebuild.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.dm.haiba import DM_TABLES, build_rebuild_sql, build_stats_sql

pytestmark = pytest.mark.unit

DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "haiba"
    / "01_dm.sql"
)


def _ddl_columns(table: str) -> set[str]:
    sql = DDL_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE TABLE \[dm\]\.\[{re.escape(table)}\] \((.*?)\n    \);", sql, re.DOTALL
    )
    assert match, f"no CREATE TABLE block for dm.{table}"
    return set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", match.group(1), re.MULTILINE))


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_rebuild_writes_every_column_the_ddl_declares(t):
    """A column in the DDL but missing from the INSERT stays NULL forever;
    one in the INSERT but missing from the DDL fails the run. Columns owned by
    the contact-relation pass (AB#27834) are excluded — see below."""
    sql = build_rebuild_sql(t)
    # Parse the INSERT column list specifically — the statement is preceded by
    # the #org build, so splitting on the first SELECT picks up the wrong one.
    clause = re.search(
        rf"INSERT INTO \[dm\]\.\[{re.escape(t.dm_table)}\] " rf"WITH \(TABLOCK\) \(([^)]*)\)",
        sql,
    )
    assert clause, "no INSERT column list found"
    inserted = set(re.findall(r"\[(\w+)\]", clause.group(1)))
    declared = _ddl_columns(t.dm_table)
    managed = {"dw_timestamp_insert", "dw_timestamp_update"}
    # AB#27834's relation columns are written afterwards by
    # DSA/SQL/dm/haiba_kontakt.py, not by the rebuild. They are covered by
    # test_haiba_kontakt.py instead; listing them here would force the rebuild
    # to insert placeholders it has no value for.
    relation = {c for c in declared if c.startswith(("encounter_id_", "location_id_", "match_"))}
    assert inserted == declared - managed - relation


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_lookup_uses_kode_not_code(t):
    """dim.Organisation's SOR column is `kode`. `code` was renamed and would
    fail with 'Invalid column name'."""
    sql = build_rebuild_sql(t)
    assert "CAST([kode] AS NVARCHAR(50))" in sql
    assert "[code]" not in sql


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_every_sor_role_gets_its_own_join_and_sentinel(t):
    """Seven SOR columns across the four tables, each with its own key.
    Reusing one join for two roles would silently give both the same key."""
    sql = build_rebuild_sql(t)
    for i, (raw, _) in enumerate(t.sor_roles):
        assert f"o{i}.[kode] = a.[{raw}]" in sql
        assert f"COALESCE(o{i}.[dw_ek_Organisation], -1)" in sql
    # One join per SOR role, plus the borger lookup on case tables.
    assert sql.count("LEFT JOIN") == len(t.sor_roles) + (1 if t.cpr_column else 0)


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_blank_sor_never_joins(t):
    """An empty SOR code marks a national aggregate. Without the <> '' guard it
    would join to any dim row with a blank kode instead of falling to -1."""
    sql = build_rebuild_sql(t)
    for raw, _ in t.sor_roles:
        assert f"AND a.[{raw}] <> ''" in sql


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_stats_separate_blank_from_unresolved(t):
    """Counting them together reports 85% 'missing' for Acute_HIP every week —
    which is the national-aggregate rows working as designed."""
    sql = build_stats_sql(t)
    assert "tom_sor" in sql and "uoploest" in sql
    for raw, key in t.sor_roles:
        assert f"[{raw}] IS NULL OR [{raw}] = ''" in sql
        assert f"[{raw}] <> '' AND [{key}] = -1" in sql


def test_all_seven_sor_columns_are_covered():
    """HAIBA carries seven SOR-bearing columns; missing one leaves a whole role
    unresolved with no error."""
    roles = {(t.dm_table, raw) for t in DM_TABLES for raw, _ in t.sor_roles}
    assert len(roles) == 8  # 1 + 1 agg, 2 infection cases, 4 SSI
    assert {raw for _, raw in roles} == {
        "SORIdentifier",
        "HAI_Opholdsadresse",
        "HAI_Kontaktansvar",
        "HAI_producent",
        "HAI_kontaktansvarlig",
        "reo_producent",
        "reo_kontaktansvarlig",
    }


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_rebuild_uses_the_bulk_load_path(t):
    """The first version ran over 90 minutes on 8M rows and had to be killed;
    the archive's fast path loads the same data in 51 seconds. The difference
    is TABLOCK plus not maintaining nonclustered indexes during the insert."""
    sql = build_rebuild_sql(t)
    assert "WITH (TABLOCK)" in sql
    for ix in t.nonclustered_indexes:
        assert f"ALTER INDEX [{ix}] ON [dm].[{t.dm_table}] DISABLE;" in sql
        assert f"ALTER INDEX [{ix}] ON [dm].[{t.dm_table}] REBUILD;" in sql
    # Disable must precede the insert and rebuild must follow it.
    if t.nonclustered_indexes:
        assert sql.index("DISABLE") < sql.index("INSERT INTO") < sql.index("REBUILD")


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_lookup_goes_through_an_indexed_temp_table(t):
    """Joining dim.Organisation (165,477 rows) directly with a cast in the
    predicate produced Nested Loops over a Table Spool on the 8M-row table:
    600+ seconds of CPU without finishing. The cast belongs in the temp table
    build, not in the join."""
    sql = build_rebuild_sql(t)
    assert "INTO #org" in sql
    assert "CREATE UNIQUE CLUSTERED INDEX [IX_org_kode] ON #org([kode]);" in sql
    # No conversion left in any join predicate.
    for i, _ in enumerate(t.sor_roles):
        assert f"LEFT JOIN #org AS o{i}" in sql
    assert "CAST(o0." not in sql
    assert sql.index("INTO #org") < sql.index("INSERT INTO [dm]")


def test_rebuild_guards_against_a_silently_emptied_table():
    """A multi-statement batch can report success while a later statement
    failed — TRUNCATE lands, INSERT does not, and the asset goes green on an
    empty table. That happened once; the guard makes it impossible."""
    import inspect

    from DSA.SQL.dm import haiba as mod

    src = inspect.getsource(mod._rebuild)
    assert "nextset" in src, "result sets must be drained so errors surface"
    assert "is empty after rebuild" in src, "row-count guard missing"
    assert "SELECT COUNT(*) FROM [arc]" in src, "source count needed for the guard"
