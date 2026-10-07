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
from DSA.SQL.common.dato_dimension import dato_key
from DSA.SQL.dm.haiba import (
    DM_TABLES,
    build_konvertering_sql,
    build_rebuild_sql,
    build_stats_sql,
)

pytestmark = pytest.mark.unit

DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "haiba"
    / "02_dm.sql"
)


def _ddl_columns(table: str) -> set[str]:
    sql = DDL_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE TABLE \[dm\]\.\[{re.escape(table)}\] \((.*?)\n    \);", sql, re.DOTALL
    )
    assert match, f"no CREATE TABLE block for dm.{table}"
    return set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", match.group(1), re.MULTILINE))


def _insert_columns(sql: str, table: str) -> set[str]:
    # Parse the INSERT column list specifically — the statement is preceded by
    # the map mint and the #org build, so splitting on the first SELECT or
    # INSERT picks up the wrong one.
    clause = re.search(
        rf"INSERT INTO \[dm\]\.\[{re.escape(table)}\] " rf"WITH \(TABLOCK\) \(([^)]*)\)",
        sql,
    )
    assert clause, f"no INSERT column list found for dm.{table}"
    return set(re.findall(r"\[(\w+)\]", clause.group(1)))


_MANAGED = {"dw_timestamp_insert", "dw_timestamp_update"}


def _relation_columns(declared: set[str]) -> set[str]:
    """The encounter-relation columns are written afterwards by
    DSA/SQL/dm/haiba_kontakt.py, not by the rebuild — the keys onto the fact,
    the quality onto egenskaber. They are covered by test_haiba_kontakt.py
    instead; listing them here would force the rebuild to insert placeholders
    it has no value for."""
    return {c for c in declared if c.startswith(("encounter_id_", "location_id_", "match_"))}


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_rebuild_writes_every_column_the_fact_ddl_declares(t):
    """A column in the DDL but missing from the INSERT stays NULL forever;
    one in the INSERT but missing from the DDL fails the run."""
    inserted = _insert_columns(build_rebuild_sql(t), t.dm_table)
    declared = _ddl_columns(t.dm_table)
    assert inserted == declared - _MANAGED - _relation_columns(declared)


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_rebuild_writes_every_column_the_egenskaber_ddl_declares(t):
    """Same guard for the transaction dimension."""
    inserted = _insert_columns(build_rebuild_sql(t), t.egenskaber_table)
    declared = _ddl_columns(t.egenskaber_table)
    assert inserted == declared - _MANAGED - _relation_columns(declared)


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_no_cpr_or_org_text_reaches_dm(t):
    """The whole point of the split: raw CPR lives only in map/arc,
    and the organisational text columns live in dim.Organisation. A rebuild
    that writes either has regressed the model. HAI_RegionKode is deliberately
    NOT forbidden — like RegionKode on the agg side it carries the national-
    aggregate distinction and stays on the case egenskaber tables."""
    sql = build_rebuild_sql(t)
    written = _insert_columns(sql, t.dm_table) | _insert_columns(sql, t.egenskaber_table)
    forbidden = {
        "cprnr",
        "ejerforhold",
        "region",
        "Sundhedsinstitution",
        "Matrikel",
        "OverliggendeEnhed",
        "AfdelingAfsnit",
        "HAI_Region",
        "HAI_ejerforhold",
        "HAI_Matrikel",
        "HAI_Sundhedsinstitution",
        "HAI_OverliggendeEnhed",
        "HAI_AfdelingAfsnit",
    }
    assert not written & forbidden


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_raw_region_code_survives_on_egenskaber(t):
    """The DDL header promises the raw source value is always preserved: when
    a SOR lookup lands on -1, the region code is the only thing left that can
    say where the case belongs — and it carries the 8888/9999 national-
    aggregate distinction."""
    region_column = "RegionKode" if t.schema.kind == "agg" else "HAI_RegionKode"
    assert region_column in t.egenskaber_columns


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_mint_is_append_only(t):
    """A clean_code keeps its dw_ek forever — the mint may only INSERT rows
    that do not exist yet. An UPDATE, DELETE or TRUNCATE against the map would
    re-key history and silently re-point every downstream reference."""
    sql = build_rebuild_sql(t)
    assert f"INSERT INTO [map].[{t.dm_table}] ([clean_code])" in sql
    assert "NOT EXISTS" in sql
    for verb in ("UPDATE [map]", "DELETE", f"TRUNCATE TABLE [map].[{t.dm_table}]"):
        assert verb not in sql, f"mint must be append-only, found: {verb}"
    # The mint must run before the dm inserts that join against it.
    assert sql.index("INSERT INTO [map]") < sql.index("INSERT INTO [dm]")


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_clean_code_joins_use_every_arc_key_column(t):
    """clean_code is the arc primary key '|'-joined. Dropping a component
    would collide rows from different grains onto one dw_ek."""
    sql = build_rebuild_sql(t)
    for key in t.schema.key_columns:
        assert f"a.[{key}]" in t.clean_code_expr
    assert t.clean_code_expr in sql
    assert "N'|'" in t.clean_code_expr


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_typed_conversions_guard_the_blank_gotchas(t):
    """TRY_CONVERT(INT, '') returns 0 — a "successful" conversion of a blank,
    so measures need the NULLIF. Dates go through the shared dato_key helper,
    which maps a blank to the 99999998 BLANK sentinel instead of conflating it
    with -1 UNKNOWN — reo_date/HAI_date are structurally blank on real rows.
    The COALESCE is what makes that hold: the extract stores blank NON-KEY
    fields as NULL (iter_rows: `value or None`), and dato_key alone sends
    NULL to -1 — without the fold, every case without a reoperation would
    read as a data problem."""
    sql = build_rebuild_sql(t)
    for src, _, typ in t.measures:
        assert f"TRY_CONVERT({typ}, NULLIF(a.[{src}], ''))" in sql
    for raw, _ in t.date_roles:
        assert dato_key(f"COALESCE(a.[{raw}], N'')") in sql
        assert dato_key(f"a.[{raw}]") not in sql


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_no_measure_uses_bit(t):
    """TRY_CONVERT(BIT, '2') returns 1 — any non-zero numeric string converts
    "successfully", so the conversion counter provably cannot see a deviating
    encoding behind BIT. TINYINT keeps the deviation visible in the data."""
    for src, _, typ in t.measures:
        assert typ != "BIT", f"{src}: BIT blinds the conversion counter"


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_conversion_losses_are_counted_per_column_in_one_scan(t):
    """A format change from SDS (decimal comma, a new date shape) must surface
    as a counted loss, not as silently NULLed measures behind a green run.
    One conditional SUM per column in a single pass — not a UNION ALL of full
    scans. Dates are checked with DATETIME2 to match dato_key, so the counter
    and the key derivation cannot disagree on what parses."""
    sql = build_konvertering_sql(t)
    for raw, _ in t.date_roles:
        assert f"TRY_CONVERT(DATETIME2, NULLIF([{raw}], '')) IS NULL" in sql
        assert f"AS [{raw}]" in sql
    for src, _, typ in t.measures:
        assert f"TRY_CONVERT({typ}, NULLIF([{src}], '')) IS NULL" in sql
        assert f"AS [{src}]" in sql
    assert sql.count(f"FROM [arc].[{t.arc_table}]") == 1
    assert "UNION ALL" not in sql


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
        # Raw SOR lives on egenskaber (e), the resolved key on the fact (f).
        assert f"e.[{raw}] IS NULL OR e.[{raw}] = ''" in sql
        assert f"e.[{raw}] <> '' AND f.[{key}] = -1" in sql


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
    for tbl, ix in t.nonclustered_indexes:
        assert f"ALTER INDEX [{ix}] ON [dm].[{tbl}] DISABLE;" in sql
        assert f"ALTER INDEX [{ix}] ON [dm].[{tbl}] REBUILD;" in sql
    # Disable must precede the dm inserts and rebuild must follow them — the
    # map mint deliberately runs earlier, so anchor on the first dm insert.
    if t.nonclustered_indexes:
        assert sql.index("DISABLE") < sql.index("INSERT INTO [dm]") < sql.index("REBUILD")


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


@pytest.mark.parametrize("t", DM_TABLES, ids=lambda t: t.dm_table)
def test_org_lookup_includes_only_live_organisations(t):
    """The schema only guarantees kode uniqueness among live rows
    (UQ_dim_Organisation_kode_Live filters on dw_is_deleted = 0): a retired
    organisation keeps its row, and its kode can be reissued under a new key.
    Without the filter the unique index build on #org fails the whole run —
    and new facts could resolve to the retired organisation."""
    sql = build_rebuild_sql(t)
    org_build = sql.split("CREATE UNIQUE CLUSTERED INDEX [IX_org_kode]")[0]
    assert "FROM [dim].[Organisation]\nWHERE [dw_is_deleted] = 0;" in org_build


def test_rebuild_guards_against_a_silently_emptied_table():
    """A multi-statement batch can report success while a later statement
    failed — TRUNCATE lands, INSERT does not, and the asset goes green on an
    empty table. That happened once; the guard makes it impossible."""
    import inspect

    from DSA.SQL.dm import haiba as mod

    src = inspect.getsource(mod._rebuild)
    assert "nextset" in src, "result sets must be drained so errors surface"
    assert "is empty after rebuild" in src, "row-count guard missing"
    assert "FROM [arc]" in src, "source count needed for the guard"
    # dw_is_deleted is carried, not filtered, so fact == egenskaber == arc
    # row for row; a shortfall means the mint or the map join dropped rows.
    assert "row-count mismatch" in src, "fact/egenskaber/arc equality guard missing"


def test_rebuild_surfaces_the_silent_failure_modes():
    """Two failure modes every other guard is blind to: a clean_code component
    changing shape re-keys the whole history behind green row counts (the map
    is append-only, so it just mints a second generation), and a CPR carrying
    two live dw_ek_Borger keys silently splits per-citizen aggregates and
    hides contacts from the kontakt stamp. Both must be measured per run."""
    import inspect

    from DSA.SQL.dm import haiba as mod

    src = inspect.getsource(mod._rebuild)
    assert "map_rows_before" in src, "mint volume must be measured per run"
    assert "map_rows_minted" in src, "minted rows belong in the metadata"
    assert "REKEY_WARN_FRACTION" in src, "re-key warning threshold missing"
    assert "cpr_med_flere_borgere" in src, "CPR-split count belongs in metadata"
    assert "COUNT(DISTINCT [dw_ek_Borger]) > 1" in src
