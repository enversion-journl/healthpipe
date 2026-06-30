# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the per-table MySupply DFM flow_start assets.

Validates that each of the 5 MySupply DFM source tables has its own
dfm_*_flow_start asset, and that each asset's dfm_state row name matches
the flow_start_key its sibling archive config reads.
"""

from __future__ import annotations

import pytest

EXPECTED_ARC_TARGETS = [
    "mysupply_statistic_invoice_header",
    "mysupply_metadata",
    "mysupply_statistic_invoice_line",
    "mysupply_statistic_invoice_header_allowance_charge",
    "mysupply_statistic_invoice_line_allowance_charge",
]


@pytest.fixture(scope="module")
def flow_start_assets():
    from DSI.mssql.extract.extract_configs_mysupply import mysupply_flow_start_assets

    return mysupply_flow_start_assets


def test_five_flow_start_assets(flow_start_assets):
    assert len(flow_start_assets) == 5


@pytest.mark.parametrize("arc_target", EXPECTED_ARC_TARGETS)
def test_flow_start_asset_name_for_each_table(flow_start_assets, arc_target):
    expected_name = f"dfm_{arc_target}_flow_start"
    names = {next(iter(a.keys)).to_user_string() for a in flow_start_assets}
    assert expected_name in names, f"Missing flow_start asset {expected_name!r}; found {names}"


@pytest.mark.parametrize("arc_target", EXPECTED_ARC_TARGETS)
def test_flow_start_asset_group_and_tags(flow_start_assets, arc_target):
    expected_name = f"dfm_{arc_target}_flow_start"
    asset = next(
        a for a in flow_start_assets if next(iter(a.keys)).to_user_string() == expected_name
    )
    groups = set(asset.group_names_by_key.values())
    assert groups == {"mysupply_archive"}


@pytest.mark.parametrize("arc_target", EXPECTED_ARC_TARGETS)
def test_flow_start_deps_on_staging_infra_dfm(flow_start_assets, arc_target):
    """Like dfm_ifdb_flow_start, MySupply flow_start assets depend on the asset
    that creates meta.dfm_state."""
    import dagster as dg

    expected_name = f"dfm_{arc_target}_flow_start"
    asset = next(
        a for a in flow_start_assets if next(iter(a.keys)).to_user_string() == expected_name
    )
    dep_keys = {dep.asset_key for spec in asset.specs for dep in spec.deps}
    assert dg.AssetKey(["staging_infra_dfm"]) in dep_keys


@pytest.mark.parametrize("arc_target", EXPECTED_ARC_TARGETS)
def test_extract_deps_on_matching_flow_start(arc_target):
    """Each MySupply ext_*_mysupply_dfm extract asset must depend on its
    sibling flow_start asset so flow_start runs first."""
    import dagster as dg
    from DSI.mssql.extract.extract_configs_mysupply import mysupply_dfm_extract_assets

    # Factory names extract assets as ext_<target_table>; target_table includes
    # the _mysupply_dfm suffix, so the full name is ext_<arc_target>_dfm.
    expected_ext_name = f"ext_{arc_target}_dfm"

    asset = next(
        (
            a
            for a in mysupply_dfm_extract_assets
            if next(iter(a.keys)).to_user_string() == expected_ext_name
        ),
        None,
    )
    assert asset is not None, (
        f"Extract asset {expected_ext_name!r} not found in mysupply_dfm_extract_assets; "
        f"available: {[next(iter(a.keys)).to_user_string() for a in mysupply_dfm_extract_assets]}"
    )

    dep_keys = {dep.asset_key for spec in asset.specs for dep in spec.deps}
    expected_flow_start = dg.AssetKey([f"dfm_{arc_target}_flow_start"])
    assert expected_flow_start in dep_keys, (
        f"{expected_ext_name} is missing dep {expected_flow_start.to_user_string()}; "
        f"has deps: {[k.to_user_string() for k in dep_keys]}"
    )


def test_flow_start_write_coordinate_matches_archive_read_coordinate():
    """Round-trip: the dfm_state row name each flow_start asset writes to must
    equal the table half of its sibling archive config's flow_start_key.

    Grounds both sides to the same single source of truth — the convention
    function `mysupply_flow_start_dfm_table`. If a future refactor renames the
    convention (e.g. swaps "<arc>_flow_start" for "flow_start_<arc>"), this
    test catches the silent divergence because the archive configs hold the
    table name as a literal string, while the extract module routes through
    the convention function.
    """
    from DSI.mssql.archive.mysupply_archive_configs import (
        HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        HEADER_ARCHIVE_CONFIG,
        LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        LINE_ARCHIVE_CONFIG,
        METADATA_ARCHIVE_CONFIG,
    )
    from DSI.mssql.extract.extract_configs_mysupply import (
        MYSUPPLY_DFM_EXTRACT_CONFIGS,
        mysupply_flow_start_dfm_table,
    )

    archive_by_target = {
        "mysupply_statistic_invoice_header": HEADER_ARCHIVE_CONFIG,
        "mysupply_metadata": METADATA_ARCHIVE_CONFIG,
        "mysupply_statistic_invoice_line": LINE_ARCHIVE_CONFIG,
        "mysupply_statistic_invoice_header_allowance_charge": HEADER_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
        "mysupply_statistic_invoice_line_allowance_charge": LINE_ALLOWANCE_CHARGE_ARCHIVE_CONFIG,
    }

    for cfg in MYSUPPLY_DFM_EXTRACT_CONFIGS:
        arc_target = cfg.target_table.removesuffix("_dfm")
        expected_dfm_table = mysupply_flow_start_dfm_table(cfg.target_table)
        archive_cfg = archive_by_target[arc_target]

        assert archive_cfg.flow_start_key == ("dfm", expected_dfm_table), (
            f"Round-trip mismatch for {arc_target}: extract module writes to "
            f"(dfm, {expected_dfm_table!r}) but archive reads "
            f"{archive_cfg.flow_start_key!r}. Rename one side without the "
            f"other and gap-closure silently breaks at runtime."
        )
