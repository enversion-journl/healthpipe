# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Invariant tests for the ILS DFM extract + archive configs."""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit

EXPECTED_TABLES = {
    # Pattern A — full load (15)
    "ils_vareart",
    "ils_varegrp",
    "ils_varetype",
    "ils_varetype2",
    "ils_projekt",
    "ils_moms",
    "ils_lev",
    "ils_sgh",
    "ils_lgr",
    "ils_rekv",
    "ils_rekvgrp",
    "ils_postdi",
    "ils_lafth",
    "ils_lafthsgh",
    "ils_zz_oper",
    # Pattern B — delta on sysdato (8)
    "ils_laftdpris",
    "ils_laftdstam",
    "ils_laftd",
    "ils_udlev",
    "ils_lgrvare",
    "ils_pmdm",
    "ils_indkfak",
    "ils_ordh",
    # Pattern C — manual exclusion list (1)
    "ils_rekvundtagne",
    # Pattern D — append-only log, delta on Dato (1)
    "ils_changelog",
}

PATTERN_A_TABLES = {
    "ils_vareart",
    "ils_varegrp",
    "ils_varetype",
    "ils_varetype2",
    "ils_projekt",
    "ils_moms",
    "ils_lev",
    "ils_sgh",
    "ils_lgr",
    "ils_rekv",
    "ils_rekvgrp",
    "ils_postdi",
    "ils_lafth",
    "ils_lafthsgh",
    "ils_zz_oper",
}
PATTERN_DELTA_TABLES = {
    "ils_laftdpris",
    "ils_laftdstam",
    "ils_laftd",
    "ils_udlev",
    "ils_lgrvare",
    "ils_pmdm",
    "ils_indkfak",
    "ils_ordh",
}
PATTERN_C_TABLES = {"ils_rekvundtagne"}
PATTERN_D_TABLES = {"ils_changelog"}

# arc tables that predate the ILS DFM migration and are archived elsewhere.
ALREADY_ARCHIVED = {"ils_lgrbeh", "ils_afdlgrbeh", "ils_unspsc", "ils_vare"}


@pytest.fixture
def ils_dfm_extract_configs():
    from DSI.mssql.extract.ils_extract_configs import ILS_DFM_EXTRACT_CONFIGS

    return ILS_DFM_EXTRACT_CONFIGS


@pytest.fixture
def ils_dfm_archive_assets():
    from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_assets

    return ils_dfm_archive_assets


def test_extract_configs_cover_25_tables(ils_dfm_extract_configs):
    targets = {c.target_table for c in ils_dfm_extract_configs}
    assert targets == EXPECTED_TABLES


def test_full_load_tables_carry_no_delta_machinery(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.is_full_load:
            assert c.delta_column is None, c.target_table
            assert c.delta_expression is None, c.target_table
            assert c.checkpoint_from is None, c.target_table


def test_pattern_a_and_c_are_full_load(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.target_table in PATTERN_A_TABLES | PATTERN_C_TABLES:
            assert c.is_full_load is True, c.target_table


def test_rekvundtagne_extract_uses_cross_db_select(ils_dfm_extract_configs):
    cfg = next(c for c in ils_dfm_extract_configs if c.target_table == "ils_rekvundtagne")
    assert cfg.select_from_expr is not None
    assert "rekvUndtagne" in cfg.select_from_expr
    assert cfg.columns == ("sgh", "rnr", "navn")
    assert cfg.index_keys == ("sgh", "rnr")


def test_delta_tables_use_sysdato_and_read_checkpoint_from_arc(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.is_full_load or c.target_table in PATTERN_D_TABLES:
            continue
        assert c.target_table in PATTERN_DELTA_TABLES, c.target_table
        assert c.delta_column == "sysdato", c.target_table
        assert c.checkpoint_type == "datetime", c.target_table
        assert c.checkpoint_from is not None, c.target_table
        schema, table = c.checkpoint_from
        assert schema == "arc", c.target_table
        # The extract target is the source-prefixed arc name (no _dfm postfix).
        assert table == c.target_table, c.target_table


def test_changelog_extract_looks_back_over_day_grain_dato(ils_dfm_extract_configs):
    """A day-grain Dato with `Dato > checkpoint` drops rows logged later that day."""
    cfg = next(c for c in ils_dfm_extract_configs if c.target_table == "ils_changelog")
    assert cfg.lookback_days >= 1


def test_extract_source_schema_is_dbo(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        assert c.source_schema == "dbo", c.target_table


def test_archive_assets_are_25(ils_dfm_archive_assets):
    assert len(ils_dfm_archive_assets) == 25


def test_archive_assets_all_in_ils_archive_group(ils_dfm_archive_assets):
    for asset in ils_dfm_archive_assets:
        # `group_name` lives on the asset's spec
        groups = {spec.group_name for spec in asset.specs}
        assert groups == {"ils_archive"}, asset


def test_archive_assets_included_in_all_archive_assets(ils_dfm_archive_assets):
    from DSI.mssql.archive import all_archive_assets

    archive_keys = {
        tuple(spec.key.path) for asset in ils_dfm_archive_assets for spec in asset.specs
    }
    all_keys = {tuple(spec.key.path) for asset in all_archive_assets for spec in asset.specs}
    assert archive_keys.issubset(all_keys)


def test_already_archived_tables_not_re_added(ils_dfm_extract_configs):
    new_target_tables = {c.target_table for c in ils_dfm_extract_configs}
    assert new_target_tables.isdisjoint(ALREADY_ARCHIVED), (
        f"Must not re-archive existing arc tables: " f"{new_target_tables & ALREADY_ARCHIVED}"
    )


def test_archive_merge_keys():
    from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

    for cfg in ils_dfm_archive_configs:
        assert cfg.strategy == "upsert", cfg.target_table
        assert cfg.update_only_on_change is True, cfg.target_table
        assert cfg.detect_deletes is False, cfg.target_table
        if cfg.target_table == "ils_rekvundtagne":
            assert cfg.merge_keys == ("sgh", "rnr"), cfg.target_table
            assert cfg.non_key_columns == ("navn",), cfg.target_table
            assert cfg.upstream_is_full_load is True, cfg.target_table
        else:
            assert cfg.merge_keys == ("prrowid",), cfg.target_table
        if cfg.target_table == "ils_changelog":
            assert cfg.upstream_is_full_load is False
            # The extract's lookback is subtracted from this checkpoint.
            assert "MAX([Dato])" in cfg.checkpoint_query
            assert "[arc].[ils_changelog]" in cfg.checkpoint_query
            # ext DATETIME2 vs arc datetime: the pro2 stamps never compare equal.
            assert not {"pro2created", "pro2modified"} & set(cfg.change_detect_columns)


def test_merge_key_never_in_non_key_columns():
    """The UPDATE SET must never try to overwrite the merge key."""
    from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

    for cfg in ils_dfm_archive_configs:
        overlap = set(cfg.merge_keys) & set(cfg.non_key_columns)
        assert not overlap, f"{cfg.target_table}: merge/non-key overlap {overlap}"


def test_archive_source_table_matches_extract_target(ils_dfm_extract_configs):
    from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

    # The `_dfm` cutover postfix was dropped in 20260701_drop_ext_dfm_postfix.sql,
    # so the arc source_table equals the extract target (source == arc name).
    extract_targets = {c.target_table for c in ils_dfm_extract_configs}
    for cfg in ils_dfm_archive_configs:
        assert cfg.target_table == cfg.source_table, cfg.target_table
        assert not cfg.target_table.endswith("_dfm"), cfg.target_table
        assert cfg.source_table in extract_targets, cfg.target_table


def test_ils_archive_job_registered():
    from jobs.ils_jobs import ils_archive_job

    assert ils_archive_job.name == "ils_archive_job"


def test_ils_archive_in_erp_daily_dfm_job():
    from jobs.erp_daily_dfm_jobs import erp_daily_dfm_job

    selection_str = str(erp_daily_dfm_job.selection)
    assert "ils_archive" in selection_str


def test_ddl_column_lists_match_non_key_columns():
    """Each arc.ils_* DDL block in sources/ils/ must contain exactly the
    columns declared in the corresponding ArchiveMergeConfig.non_key_columns
    tuple (plus 'prrowid' and the standard audit columns).

    This guards against silent column-list drift between the DDL and the
    MERGE.
    """
    import re
    from pathlib import Path

    from DSI.mssql.archive.ils_archive_configs import ils_dfm_archive_configs

    ddl_dir = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "sources" / "ils"
    ddl = "\n".join(p.read_text(encoding="utf-8") for p in sorted(ddl_dir.glob("*.sql")))

    audit_cols = {"dw_timestamp_insert", "dw_timestamp_update"}

    for cfg in ils_dfm_archive_configs:
        # Extract the body of the CREATE TABLE block. Non-greedy capture from
        # the CREATE TABLE up to the first `END` of its IF NOT EXISTS wrapper.
        # NB: non-greedy `(.*?)\)` would close on the first `)` it sees, which
        # would land inside `NVARCHAR(10)` — anchor on `END` instead.
        pattern = re.compile(
            rf"CREATE TABLE \[arc\]\.\[{cfg.target_table}\](.*?)\bEND\b",
            re.DOTALL,
        )
        match = pattern.search(ddl)
        assert match, f"No CREATE TABLE block found for arc.{cfg.target_table}"

        body = match.group(1)
        # Pull every bracketed identifier from the body.
        col_names = set(re.findall(r"\[([^\]]+)\]", body))
        # Strip constraint names (PK_/DF_/UQ_ are bracketed too) and audit columns.
        col_names = {name for name in col_names if not name.startswith(("PK_", "DF_", "UQ_"))}
        col_names -= audit_cols

        expected = set(cfg.non_key_columns) | set(cfg.merge_keys)
        assert col_names == expected, (
            f"DDL drift for arc.{cfg.target_table}:\n"
            f"  missing in DDL: {expected - col_names}\n"
            f"  extra in DDL:   {col_names - expected}"
        )
