# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the ILS DFM archive configs (parallel to legacy ils.py)."""

from __future__ import annotations

import pytest

EXPECTED_TABLES = {
    # Pattern A — full load (10)
    "vareart_ils_dfm",
    "varegrp_ils_dfm",
    "varetype_ils_dfm",
    "moms_ils_dfm",
    "lev_ils_dfm",
    "sgh_ils_dfm",
    "lgr_ils_dfm",
    "rekv_ils_dfm",
    "rekvgrp_ils_dfm",
    "lafth_ils_dfm",
    # Pattern B — delta on sysdato (7)
    "laftdpris_ils_dfm",
    "laftdstam_ils_dfm",
    "laftd_ils_dfm",
    "udlev_ils_dfm",
    "lgrvare_ils_dfm",
    "pmdm_ils_dfm",
    "indkfak_ils_dfm",
}

PATTERN_A_TABLES = {
    "vareart_ils_dfm",
    "varegrp_ils_dfm",
    "varetype_ils_dfm",
    "moms_ils_dfm",
    "lev_ils_dfm",
    "sgh_ils_dfm",
    "lgr_ils_dfm",
    "rekv_ils_dfm",
    "rekvgrp_ils_dfm",
    "lafth_ils_dfm",
}
PATTERN_DELTA_TABLES = {
    "laftdpris_ils_dfm",
    "laftdstam_ils_dfm",
    "laftd_ils_dfm",
    "udlev_ils_dfm",
    "lgrvare_ils_dfm",
    "pmdm_ils_dfm",
    "indkfak_ils_dfm",
}

ALREADY_ARCHIVED = {"lgrbeh_ils", "afdlgrbeh_ils", "unspsc_ils", "vare_ils"}


@pytest.fixture
def ils_dfm_extract_configs():
    from DSI.mssql.extract.extract_configs_ils import ILS_DFM_EXTRACT_CONFIGS

    return ILS_DFM_EXTRACT_CONFIGS


@pytest.fixture
def ils_dfm_archive_assets():
    from DSI.mssql.archive import ils_dfm_archive_assets

    return ils_dfm_archive_assets


def test_extract_configs_cover_17_tables(ils_dfm_extract_configs):
    targets = {c.target_table for c in ils_dfm_extract_configs}
    assert targets == EXPECTED_TABLES


def test_pattern_a_is_full_load(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.target_table in PATTERN_A_TABLES:
            assert c.is_full_load is True, c.target_table
            assert c.delta_column is None, c.target_table
            assert c.delta_expression is None, c.target_table
            assert c.checkpoint_from is None, c.target_table


def test_partitioned_tables_use_sysdato(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.target_table in PATTERN_DELTA_TABLES:
            assert c.is_full_load is False, c.target_table
            assert c.delta_column == "sysdato", c.target_table
            assert c.delta_expression is None, c.target_table
            assert c.checkpoint_type == "datetime", c.target_table


def test_delta_tables_read_checkpoint_from_arc(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        if c.target_table in PATTERN_DELTA_TABLES:
            assert c.checkpoint_from is not None, c.target_table
            schema, table = c.checkpoint_from
            assert schema == "arc", c.target_table
            # Drop the _dfm postfix to derive the arc table name
            assert table == c.target_table.replace("_dfm", ""), c.target_table


def test_extract_source_schema_is_dbo(ils_dfm_extract_configs):
    for c in ils_dfm_extract_configs:
        assert c.source_schema == "dbo", c.target_table


def test_archive_assets_are_17(ils_dfm_archive_assets):
    assert len(ils_dfm_archive_assets) == 17


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


def test_already_archived_tables_not_re_added(ils_dfm_extract_configs, ils_dfm_archive_assets):
    new_target_tables = {c.target_table.replace("_dfm", "") for c in ils_dfm_extract_configs}
    assert new_target_tables.isdisjoint(ALREADY_ARCHIVED), (
        f"Must not re-archive existing arc tables: " f"{new_target_tables & ALREADY_ARCHIVED}"
    )


def test_archive_keyed_on_prrowid():
    from DSI.mssql.archive import ils_dfm_archive_configs

    for cfg in ils_dfm_archive_configs:
        assert cfg.merge_keys == ("prrowid",), cfg.target_table
        assert cfg.strategy == "upsert", cfg.target_table
        assert cfg.update_only_on_change is True, cfg.target_table
        assert cfg.detect_deletes is False, cfg.target_table


def test_archive_source_table_uses_dfm_postfix():
    from DSI.mssql.archive import ils_dfm_archive_configs

    for cfg in ils_dfm_archive_configs:
        assert cfg.source_table.endswith("_ils_dfm"), cfg.target_table
        assert not cfg.target_table.endswith("_dfm"), cfg.target_table
        assert cfg.target_table == cfg.source_table.replace("_dfm", "")


def test_ils_archive_job_registered():
    from jobs.ils_jobs import ils_archive_job

    assert ils_archive_job.name == "ils_archive_job"


def test_ils_archive_in_erp_daily_dfm_job():
    from jobs.erp_daily_dfm_jobs import erp_daily_dfm_job

    selection_str = str(erp_daily_dfm_job.selection)
    assert "ils_archive" in selection_str


def test_ddl_column_lists_match_non_key_columns():
    """Each arc.*_ils DDL block in 00_archive.sql must contain exactly the
    columns declared in the corresponding ArchiveMergeConfig.non_key_columns
    tuple (plus 'prrowid' and the standard audit columns).

    This guards against silent column-list drift between the DDL and the
    MERGE.
    """
    import re
    from pathlib import Path

    from DSI.mssql.archive import ils_dfm_archive_configs

    ddl_path = (
        Path(__file__).resolve().parents[2]
        / "infrastructure"
        / "ddl"
        / "tables"
        / "erp"
        / "00_archive.sql"
    )
    ddl = ddl_path.read_text(encoding="utf-8")

    audit_cols = {"DW_Timestamp_Insert", "DW_Timestamp_Update"}

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
        # Strip the constraint name (it's bracketed too) and audit columns.
        col_names.discard(f"PK_arc_{cfg.target_table}")
        col_names -= audit_cols

        expected = set(cfg.non_key_columns) | set(cfg.merge_keys)
        assert col_names == expected, (
            f"DDL drift for arc.{cfg.target_table}:\n"
            f"  missing in DDL: {expected - col_names}\n"
            f"  extra in DDL:   {col_names - expected}"
        )
