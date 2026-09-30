# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the HAIBA archive layer: column parity between the extract
schemas, the MERGE configs and the arc DDL, plus the revision-log SQL.

The failure these guard against is silent at definition time and only shows up
as "Invalid column name" partway through a multi-million-row MERGE.
"""

import dataclasses
import re
from pathlib import Path

import pytest
from core.archive_merge import build_archive_merge_sql
from DSI.csv_files.haiba import SCHEMAS
from DSI.csv_files.haiba_archive import (
    AGG_INFEKTION_CONFIG,
    AGG_ORTOPAEDI_CONFIG,
    CASES_INFEKTION_CONFIG,
    CASES_SSI_CONFIG,
    LEVERANCE_COLUMNS,
    build_leverance_sql,
    build_revision_sql,
)

pytestmark = pytest.mark.unit

CONFIGS = {
    "haiba_agg_infektion": AGG_INFEKTION_CONFIG,
    "haiba_agg_ortopaedi": AGG_ORTOPAEDI_CONFIG,
    "haiba_cases_infektion": CASES_INFEKTION_CONFIG,
    "haiba_cases_ssi": CASES_SSI_CONFIG,
}

DDL_PATH = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "tables"
    / "haiba"
    / "00_archive.sql"
)


def _ddl_columns(table: str) -> set[str]:
    """Column names declared in one CREATE TABLE block of the archive DDL."""
    sql = DDL_PATH.read_text(encoding="utf-8")
    match = re.search(
        rf"CREATE TABLE \[arc\]\.\[{re.escape(table)}\] \((.*?)\n    \);",
        sql,
        re.DOTALL,
    )
    assert match, f"no CREATE TABLE block for arc.{table}"
    return set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", match.group(1), re.MULTILINE))


@pytest.mark.parametrize("schema", SCHEMAS, ids=lambda s: s.name)
def test_merge_config_covers_every_extract_column(schema):
    """A column added to the extract but left out of the config would be
    written to ext and then silently dropped by the MERGE."""
    config = CONFIGS[schema.ext_table]
    assert set(config.merge_keys) | set(config.non_key_columns) == set(schema.all_columns)
    assert not set(config.merge_keys) & set(config.non_key_columns)


@pytest.mark.parametrize("schema", SCHEMAS, ids=lambda s: s.name)
def test_arc_ddl_declares_every_merged_column(schema):
    """The MERGE names its columns explicitly; one missing from the arc table
    fails at runtime, not at definition time."""
    config = CONFIGS[schema.ext_table]
    declared = _ddl_columns(schema.ext_table)
    merged = set(config.merge_keys) | set(config.non_key_columns)
    assert merged <= declared, f"missing from arc DDL: {sorted(merged - declared)}"


@pytest.mark.parametrize("schema", SCHEMAS, ids=lambda s: s.name)
def test_merge_keys_are_not_null_in_arc_ddl(schema):
    """Key columns must be NOT NULL: SQL Server rejects NULL in a primary key,
    and a NULL never matches itself in the MERGE ON clause, so the row would be
    re-inserted every run. SORIdentifier is empty on up to 85% of rows."""
    sql = DDL_PATH.read_text(encoding="utf-8")
    block = re.search(
        rf"CREATE TABLE \[arc\]\.\[{re.escape(schema.ext_table)}\] \((.*?)\n    \);",
        sql,
        re.DOTALL,
    ).group(1)
    for key in CONFIGS[schema.ext_table].merge_keys:
        line = re.search(rf"^\s+\[{re.escape(key)}\]\s+.*$", block, re.MULTILINE)
        assert line, f"{key} not declared in arc.{schema.ext_table}"
        assert "NOT NULL" in line.group(), f"{key} must be NOT NULL"


@pytest.mark.parametrize("schema", SCHEMAS, ids=lambda s: s.name)
def test_delete_detection_is_armed(schema):
    """detect_deletes is silently disabled at runtime unless the upstream is
    declared a full load — the engine only logs a warning."""
    config = CONFIGS[schema.ext_table]
    assert config.detect_deletes
    assert config.upstream_is_full_load


def test_revision_sql_covers_every_measure_null_safely():
    """A measure moving to or from NULL is a revision too. Comparing with <>
    would skip those rows, so the log must use the NULL-safe EXCEPT form."""
    sql = build_revision_sql(SCHEMAS[1])  # agg_ortopaedi: three measures

    for measure in ("antal90", "antal365", "operationer"):
        assert f"'{measure}'" in sql
    assert sql.count("EXCEPT SELECT") == 3
    assert "<>" not in sql
    assert "[ext].[haiba_agg_ortopaedi]" in sql
    assert "[arc].[haiba_agg_ortopaedi]" in sql


def test_revision_sql_joins_on_the_merge_key():
    """Joining on anything narrower would attribute one row's revision to
    another row."""
    sql = build_revision_sql(SCHEMAS[0])  # agg_infektion

    for key in AGG_INFEKTION_CONFIG.merge_keys:
        assert f"tgt.[{key}] = src.[{key}]" in sql


# --------------------------------------------------------------- backfill ---


@pytest.mark.parametrize("schema", SCHEMAS, ids=lambda s: s.name)
def test_backfill_sql_only_inserts_missing_keys(schema):
    """A pinned older delivery run through the normal MERGE would regress every
    changed measure and soft-delete everything newer. The backfill path must
    therefore never update or delete."""
    from DSI.csv_files.haiba_archive import build_backfill_sql

    sql = build_backfill_sql(schema)

    assert "WHERE NOT EXISTS" in sql
    # Word boundaries matter: the column names dw_timestamp_update and
    # dw_is_deleted contain the very keywords we are ruling out.
    assert not re.search(r"\bUPDATE\b", sql, re.IGNORECASE)
    assert not re.search(r"\bDELETE\b", sql, re.IGNORECASE)
    assert not re.search(r"\bMERGE\b", sql, re.IGNORECASE)
    # Rows SDS no longer publishes are flagged, not presented as current.
    assert "[dw_is_deleted]" in sql
    for key in CONFIGS[schema.ext_table].merge_keys:
        assert f"tgt.[{key}] = src.[{key}]" in sql


def _guard(ext: str, registered: str | None, arc_column: str | None = None, *, backfill=False):
    """Run the direction guard against a stubbed ext, register and arc column."""
    from unittest.mock import patch

    from DSI.csv_files.haiba import AGG_INFEKTION
    from DSI.csv_files.haiba_archive import guard_delivery_direction

    with (
        patch(
            "DSI.csv_files.haiba_archive._max_delivery",
            side_effect=[ext, arc_column],
        ),
        patch(
            "DSI.csv_files.haiba_archive._recorded_delivery",
            return_value=registered,
        ),
    ):
        return guard_delivery_direction(None, None, AGG_INFEKTION, backfill=backfill)


def test_guard_blocks_merging_an_older_delivery_over_a_newer_archive():
    """The destructive case: a pin left on the extract. Detectable before a
    single row is written, because both sides carry a delivery date."""
    with pytest.raises(ValueError, match="backfill_historical"):
        _guard("2025-12-27", "2026-08-15")


def test_guard_blocks_backfilling_from_a_current_delivery():
    """Backfill on a current delivery would flag live rows as deleted."""
    with pytest.raises(ValueError, match="not older"):
        _guard("2026-08-15", "2026-08-15", backfill=True)


def test_guard_allows_a_genuine_backfill():
    assert _guard("2025-12-27", "2026-08-15", backfill=True) == ("2025-12-27", "2026-08-15")


def test_guard_prefers_the_register_over_the_delivery_column():
    """arc.leverance_dato only advances when a row's values change, and the case
    tables go weeks without one. Trusting the column would let a stale pin
    through as if the archive were behind."""
    with pytest.raises(ValueError, match="backfill_historical"):
        _guard("2026-01-03", registered="2026-08-15", arc_column="2025-12-27")


def test_guard_falls_back_to_the_column_before_the_register_has_a_row():
    """First run after the register was introduced: arc holds data, the register
    does not yet. The guard must still see the archive's real position."""
    with pytest.raises(ValueError, match="backfill_historical"):
        _guard("2025-12-27", registered=None, arc_column="2026-08-15")


def _change_guard(config) -> str:
    """The WHEN MATCHED AND (...) predicate from a generated upsert."""
    sql = build_archive_merge_sql(config)
    return sql.split("WHEN MATCHED AND (")[1].split(") THEN")[0]


@pytest.mark.parametrize("table", sorted(CONFIGS), ids=lambda t: t)
def test_delivery_stamp_is_written_but_does_not_count_as_a_change(table):
    """leverance_dato differs for every row on every delivery, so leaving it in
    the change check makes update_only_on_change a no-op — 6.7M agg rows
    rewritten weekly, and dw_timestamp_update useless as a revision signal."""
    config = CONFIGS[table]

    assert "leverance_dato" not in _change_guard(config)
    # Still written, so a genuinely revised row records the delivery that did it.
    assert "tgt.[leverance_dato] = src.[leverance_dato]" in build_archive_merge_sql(config)


def test_measures_still_count_as_a_change():
    """Narrowing the change check must not narrow it to nothing: a revised
    measure is the one thing the archive exists to notice."""
    guard = _change_guard(AGG_INFEKTION_CONFIG)

    assert "tgt.[antal]" in guard
    assert "tgt.[risikodoegn]" in guard


def test_change_detection_defaults_to_every_non_key_column():
    """change_detect_columns is opt-in. Every other archive config in the repo
    leaves it unset and must keep comparing the full non-key set."""
    default = dataclasses.replace(AGG_INFEKTION_CONFIG, change_detect_columns=None)

    assert "leverance_dato" in _change_guard(default)


META_DDL_PATH = DDL_PATH.with_name("02_meta.sql")


def test_register_columns_match_the_control_table_ddl():
    """Same failure mode as the arc parity tests above: a column named only in
    the SQL surfaces as "Invalid column name" at runtime, not at import."""
    sql = META_DDL_PATH.read_text(encoding="utf-8")
    declared = set(re.findall(r"^\s{8}\[(\w+)\]\s+\w", sql, re.MULTILINE))

    assert set(LEVERANCE_COLUMNS) <= declared, sorted(set(LEVERANCE_COLUMNS) - declared)
    assert "dw_timestamp_update" in declared


def test_register_upsert_keys_match_the_primary_key():
    """The MERGE's ON clause and the PK must be the same pair, or a re-run of one
    delivery either duplicates its row or violates the key."""
    ddl = META_DDL_PATH.read_text(encoding="utf-8")
    upsert = build_leverance_sql()

    assert "PRIMARY KEY CLUSTERED ([kildetabel], [leverance_dato])" in ddl
    assert "tgt.[kildetabel] = src.[kildetabel]" in upsert
    assert "tgt.[leverance_dato] = src.[leverance_dato]" in upsert


def test_the_register_is_a_control_table_not_an_archive_table():
    """It mirrors no delivery file and takes no part in the MERGE, so it belongs
    in meta. In arc it would read as another archived source table."""
    sql = META_DDL_PATH.read_text(encoding="utf-8")

    assert "[meta].[HAIBALeverance]" in sql
    assert "[arc]." not in sql
    assert "[meta]." in build_leverance_sql()
