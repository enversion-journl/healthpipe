# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for MySupply DFM extract + archive merge configs.

Validates design invariants for the parallel DFM pipeline that lands in
arc.*_mysupply (see docs/superpowers/specs/2026-06-19-mysupply-archive-with-dfm-design.md).
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# DFM extract configs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysupply_dfm_extract_configs():
    from DSI.mssql.extract.extract_configs_mysupply import MYSUPPLY_DFM_EXTRACT_CONFIGS

    return {cfg.target_table: cfg for cfg in MYSUPPLY_DFM_EXTRACT_CONFIGS}


EXPECTED_EXTRACT_TARGETS = {
    "statistic_invoice_header_mysupply_dfm",
    "metadata_mysupply_dfm",
    "statistic_invoice_line_mysupply_dfm",
    "statistic_invoice_header_allowance_charge_mysupply_dfm",
    "statistic_invoice_line_allowance_charge_mysupply_dfm",
}


def test_extract_configs_cover_five_tables(mysupply_dfm_extract_configs):
    assert set(mysupply_dfm_extract_configs) == EXPECTED_EXTRACT_TARGETS


@pytest.mark.parametrize("target_table", sorted(EXPECTED_EXTRACT_TARGETS))
def test_extract_targets_use_dfm_postfix(mysupply_dfm_extract_configs, target_table):
    """The new pipeline must not clash with ext.*_mysupply produced by the legacy
    partition-based extract. Postfix _dfm guarantees that until the legacy is removed."""
    assert target_table.endswith("_mysupply_dfm")


@pytest.mark.parametrize("target_table", sorted(EXPECTED_EXTRACT_TARGETS))
def test_extract_uses_7_day_tail(mysupply_dfm_extract_configs, target_table):
    cfg = mysupply_dfm_extract_configs[target_table]
    assert cfg.lookback_days == 7
    assert cfg.checkpoint_type == "datetime"
    assert cfg.is_full_load is False


@pytest.mark.parametrize("target_table", sorted(EXPECTED_EXTRACT_TARGETS))
def test_extract_uses_try_cast_on_fakturadato(mysupply_dfm_extract_configs, target_table):
    """FakturaDato is NVARCHAR(MAX); TRY_CAST avoids extract-wide failure on bad strings."""
    cfg = mysupply_dfm_extract_configs[target_table]
    assert cfg.delta_expression is not None
    assert "TRY_CAST" in cfg.delta_expression
    assert "FakturaDato" in cfg.delta_expression


def test_header_filters_directly_on_fakturadato(mysupply_dfm_extract_configs):
    """The header has its own FakturaDato column — no subquery needed."""
    cfg = mysupply_dfm_extract_configs["statistic_invoice_header_mysupply_dfm"]
    assert cfg.delta_expression == "TRY_CAST([FakturaDato] AS DATETIME2)"


@pytest.mark.parametrize(
    "target_table",
    [
        "metadata_mysupply_dfm",
        "statistic_invoice_line_mysupply_dfm",
        "statistic_invoice_header_allowance_charge_mysupply_dfm",
        "statistic_invoice_line_allowance_charge_mysupply_dfm",
    ],
)
def test_dependents_use_correlated_subquery(mysupply_dfm_extract_configs, target_table):
    """Dependents have no business-date column — they filter via correlated subquery
    through the FK chain to the header's FakturaDato."""
    cfg = mysupply_dfm_extract_configs[target_table]
    expr = cfg.delta_expression
    assert "SELECT" in expr
    assert "FROM [dbo].[statistic_invoice_header]" in expr


def test_line_allowance_charge_uses_two_hop_join(mysupply_dfm_extract_configs):
    """The nested FK chain line_allowance_charge → line → header requires a JOIN."""
    cfg = mysupply_dfm_extract_configs["statistic_invoice_line_allowance_charge_mysupply_dfm"]
    assert "INNER JOIN [dbo].[statistic_invoice_line]" in cfg.delta_expression


def test_metadata_correlated_subquery_qualifies_outer_id():
    """Regression test for a column-binding bug.

    Both `dbo.metadata` (outer) and `dbo.statistic_invoice_header` (inner alias h)
    have a column named `id`. An unqualified `[id]` in the subquery's WHERE clause
    silently binds to `h.id` (innermost scope), making the join `h.metadata_id = h.id`
    which is essentially never true → metadata extract returns 0 rows on every run.

    The other 3 dependents are not affected because their outer FK columns
    (statistic_invoice_header_id, statistic_invoice_line_id) don't exist in the
    inner tables, so they fall through to the outer scope correctly.
    """
    from DSI.mssql.extract.extract_configs_mysupply import MYSUPPLY_DFM_EXTRACT_CONFIGS

    cfg = next(c for c in MYSUPPLY_DFM_EXTRACT_CONFIGS if c.target_table == "metadata_mysupply_dfm")
    # The outer `metadata.id` reference must be explicitly qualified.
    assert "[dbo].[metadata].[id]" in cfg.delta_expression, (
        "metadata delta_expression must qualify the outer table's id column to avoid "
        "ambiguity with the inner header table's id PK"
    )


@pytest.mark.parametrize(
    "target_table,expected_checkpoint_table",
    [
        ("statistic_invoice_header_mysupply_dfm", "statistic_invoice_header_mysupply"),
        ("metadata_mysupply_dfm", "metadata_mysupply"),
        ("statistic_invoice_line_mysupply_dfm", "statistic_invoice_line_mysupply"),
        (
            "statistic_invoice_header_allowance_charge_mysupply_dfm",
            "statistic_invoice_header_allowance_charge_mysupply",
        ),
        (
            "statistic_invoice_line_allowance_charge_mysupply_dfm",
            "statistic_invoice_line_allowance_charge_mysupply",
        ),
    ],
)
def test_each_extract_reads_its_own_arc_checkpoint(
    mysupply_dfm_extract_configs, target_table, expected_checkpoint_table
):
    """Per DFM doctrine, each asset reads its own checkpoint for self-healing."""
    cfg = mysupply_dfm_extract_configs[target_table]
    assert cfg.checkpoint_from == ("arc", expected_checkpoint_table)


# ---------------------------------------------------------------------------
# Archive merge configs
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mysupply_archive_configs():
    from DSI.mssql.archive.mysupply_archive_configs import (
        HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        HEADER_ARCHIVE_CONFIG,
        LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        LINE_ARCHIVE_CONFIG,
        METADATA_ARCHIVE_CONFIG,
    )

    return {
        "statistic_invoice_header_mysupply": HEADER_ARCHIVE_CONFIG,
        "metadata_mysupply": METADATA_ARCHIVE_CONFIG,
        "statistic_invoice_line_mysupply": LINE_ARCHIVE_CONFIG,
        "statistic_invoice_header_allowance_charge_mysupply": HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        "statistic_invoice_line_allowance_charge_mysupply": LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
    }


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_archive_target_has_no_dfm_postfix(mysupply_archive_configs, arc_table):
    """Arc tables don't clash with anything legacy, so no postfix."""
    cfg = mysupply_archive_configs[arc_table]
    assert cfg.target_schema == "arc"
    assert cfg.target_table == arc_table
    assert not cfg.target_table.endswith("_dfm")


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_archive_source_uses_dfm_postfix(mysupply_archive_configs, arc_table):
    """Source must match the DFM extract's target table — and must NOT match the
    legacy partition extract's ext.*_mysupply (would shadow it during cutover)."""
    cfg = mysupply_archive_configs[arc_table]
    assert cfg.source_schema == "ext"
    assert cfg.source_table.endswith("_mysupply_dfm")


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_archive_is_upsert_keyed_on_id(mysupply_archive_configs, arc_table):
    cfg = mysupply_archive_configs[arc_table]
    assert cfg.strategy == "upsert"
    assert cfg.merge_keys == ("id",)


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_archive_uses_update_only_on_change(mysupply_archive_configs, arc_table):
    """7-day tail re-pulls unchanged rows; change detection prevents noisy
    DW_Timestamp_Update bumps on rows whose business cols didn't change."""
    cfg = mysupply_archive_configs[arc_table]
    assert cfg.update_only_on_change is True


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_archive_does_not_detect_deletes(mysupply_archive_configs, arc_table):
    """Delta extracts only see rows in the 7-day window; NOT MATCHED BY SOURCE
    would incorrectly soft-delete rows outside that window."""
    cfg = mysupply_archive_configs[arc_table]
    assert cfg.detect_deletes is False
    assert cfg.upstream_is_full_load is False


@pytest.mark.parametrize(
    "arc_table",
    [
        "statistic_invoice_header_mysupply",
        "metadata_mysupply",
        "statistic_invoice_line_mysupply",
        "statistic_invoice_header_allowance_charge_mysupply",
        "statistic_invoice_line_allowance_charge_mysupply",
    ],
)
def test_extractor_metadata_columns_excluded_from_non_key(mysupply_archive_configs, arc_table):
    """SourceSystem / DW_LoadTimestamp / DW_PartitionDate live only in ext.
    DW_LoadTimestamp changes every extract — including it would defeat
    update_only_on_change."""
    cfg = mysupply_archive_configs[arc_table]
    excluded = {"SourceSystem", "DW_LoadTimestamp", "DW_PartitionDate"}
    assert excluded.isdisjoint(set(cfg.non_key_columns))


def test_header_id_not_in_non_key_columns(mysupply_archive_configs):
    """`id` is the merge key — it must not appear in non_key_columns (otherwise the
    UPDATE SET would try to overwrite the key)."""
    for cfg in mysupply_archive_configs.values():
        assert "id" not in cfg.non_key_columns


def test_archive_checkpoint_query_is_none(mysupply_archive_configs):
    """checkpoint_query is unused; flow_start_key drives checkpoint advancement."""
    for cfg in mysupply_archive_configs.values():
        assert cfg.checkpoint_query is None


def test_archive_flow_start_key_matches_sibling_dfm_state_row(mysupply_archive_configs):
    """Each archive config must read its checkpoint from the dfm_state row
    that its sibling dfm_*_mysupply_flow_start asset writes."""
    for arc_target, cfg in mysupply_archive_configs.items():
        expected = ("dfm", f"{arc_target}_flow_start")
        assert (
            cfg.flow_start_key == expected
        ), f"arc.{arc_target} flow_start_key={cfg.flow_start_key!r}, expected {expected!r}"


# ---------------------------------------------------------------------------
# Registration chain
# ---------------------------------------------------------------------------


def test_mysupply_archive_assets_list_has_five_entries():
    from DSI.mssql.archive import mysupply_archive_assets

    assert len(mysupply_archive_assets) == 5


def test_mysupply_assets_included_in_all_archive_assets():
    from DSI.mssql.archive import all_archive_assets, mysupply_archive_assets

    all_keys = {key for asset in all_archive_assets for key in asset.keys}
    for asset in mysupply_archive_assets:
        for key in asset.keys:
            assert key in all_keys


def test_mysupply_extract_assets_included_in_all_factory_extract_assets():
    from DSI.mssql.extract import all_factory_extract_assets, mysupply_dfm_extract_assets

    all_keys = {key for asset in all_factory_extract_assets for key in asset.keys}
    for asset_def in mysupply_dfm_extract_assets:
        for key in asset_def.keys:
            assert key in all_keys


def test_mysupply_archive_assets_all_in_mysupply_archive_group():
    from DSI.mssql.archive import mysupply_archive_assets

    for asset in mysupply_archive_assets:
        # AssetsDefinition.group_names_by_key is keyed by AssetKey
        groups = set(asset.group_names_by_key.values())
        assert groups == {"mysupply_archive"}, f"{list(asset.keys)} has groups {groups}"


def test_mysupply_archive_job_registered():
    from jobs import mysupply_archive_job

    assert mysupply_archive_job.name == "mysupply_archive_job"


def test_mysupply_archive_in_erp_daily_dfm_job():
    """The mysupply_archive group should run as part of the daily DFM batch."""
    import inspect

    from jobs import erp_daily_dfm_jobs

    module_src = inspect.getsource(erp_daily_dfm_jobs)
    assert '"mysupply_archive"' in module_src
