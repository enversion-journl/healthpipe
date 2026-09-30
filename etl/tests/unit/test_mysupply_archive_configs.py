# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Invariant tests for MySupply DFM extract + archive merge configs.

Validates design invariants for the DFM pipeline that lands in arc.mysupply_*
(see docs/superpowers/specs/2026-06-19-mysupply-archive-with-dfm-design.md).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.unit


@pytest.fixture(scope="module")
def mysupply_dfm_extract_configs():
    from DSI.mssql.extract.mysupply_extract_configs import MYSUPPLY_DFM_EXTRACT_CONFIGS

    return list(MYSUPPLY_DFM_EXTRACT_CONFIGS)


@pytest.fixture(scope="module")
def mysupply_archive_configs():
    from DSI.mssql.archive.mysupply_archive_configs import (
        HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        HEADER_ARCHIVE_CONFIG,
        LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        LINE_ARCHIVE_CONFIG,
        METADATA_ARCHIVE_CONFIG,
    )

    return [
        HEADER_ARCHIVE_CONFIG,
        METADATA_ARCHIVE_CONFIG,
        LINE_ARCHIVE_CONFIG,
        HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
    ]


def test_each_extract_reads_its_own_arc_checkpoint(mysupply_dfm_extract_configs):
    """Per DFM doctrine, each asset reads its own checkpoint for self-healing."""
    for cfg in mysupply_dfm_extract_configs:
        assert cfg.checkpoint_from == ("arc", cfg.target_table), cfg.target_table


def test_metadata_correlated_subquery_qualifies_outer_id(mysupply_dfm_extract_configs):
    """Regression pin for a column-binding bug.

    Both `dbo.metadata` (outer) and `dbo.statistic_invoice_header` (inner alias h)
    have a column named `id`. An unqualified `[id]` in the subquery's WHERE clause
    silently binds to `h.id` (innermost scope), making the join `h.metadata_id = h.id`
    which is essentially never true → metadata extract returns 0 rows on every run.
    """
    cfg = next(c for c in mysupply_dfm_extract_configs if c.target_table == "mysupply_metadata")
    assert "[dbo].[metadata].[id]" in cfg.delta_expression, (
        "metadata delta_expression must qualify the outer table's id column to avoid "
        "ambiguity with the inner header table's id PK"
    )


def test_archive_source_matches_extract_target(
    mysupply_archive_configs, mysupply_dfm_extract_configs
):
    """Each arc config must read the ext table its sibling DFM extract writes."""
    extract_targets = {cfg.target_table for cfg in mysupply_dfm_extract_configs}
    for cfg in mysupply_archive_configs:
        assert cfg.source_schema == "ext", cfg.target_table
        assert cfg.source_table == cfg.target_table, cfg.target_table
        assert cfg.source_table in extract_targets, cfg.target_table


def test_merge_key_never_in_non_key_columns(mysupply_archive_configs):
    """The UPDATE SET must never try to overwrite the merge key."""
    for cfg in mysupply_archive_configs:
        overlap = set(cfg.merge_keys) & set(cfg.non_key_columns)
        assert not overlap, f"{cfg.target_table}: merge/non-key overlap {overlap}"


def test_archive_does_not_detect_deletes(mysupply_archive_configs):
    """Delta extracts only see rows in the 7-day window; NOT MATCHED BY SOURCE
    would incorrectly soft-delete rows outside that window."""
    for cfg in mysupply_archive_configs:
        assert cfg.detect_deletes is False, cfg.target_table
        assert cfg.upstream_is_full_load is False, cfg.target_table


def test_archive_uses_update_only_on_change(mysupply_archive_configs):
    """The 7-day tail re-pulls unchanged rows; change detection prevents noisy
    dw_timestamp_update bumps on rows whose business cols didn't change."""
    for cfg in mysupply_archive_configs:
        assert cfg.update_only_on_change is True, cfg.target_table


def test_extractor_metadata_columns_excluded_from_non_key(mysupply_archive_configs):
    """SourceSystem / dw_load_timestamp / dw_partition_date live only in ext.
    dw_load_timestamp changes every extract — including it would defeat
    update_only_on_change."""
    excluded = {"SourceSystem", "dw_load_timestamp", "dw_partition_date"}
    for cfg in mysupply_archive_configs:
        assert excluded.isdisjoint(set(cfg.non_key_columns)), cfg.target_table


def test_archive_flow_start_key_matches_sibling_dfm_state_row(mysupply_archive_configs):
    """Each archive config must read its checkpoint from the dfm_state row
    that its sibling dfm_*_flow_start asset writes (checkpoint_query unused)."""
    for cfg in mysupply_archive_configs:
        assert cfg.checkpoint_query is None, cfg.target_table
        assert cfg.flow_start_key == ("dfm", f"{cfg.target_table}_flow_start"), cfg.target_table
