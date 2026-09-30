# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the per-table SydAX DFM flow_start assets.

Each Bucket B/C (delta) table has a dfm_sydax_<arc>_flow_start asset, and each
asset's DFMState row name matches the flow_start_key its sibling arc config
reads (the round-trip that makes flow_start gap-free).
"""

import sys
from unittest.mock import MagicMock

if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()

import dagster as dg
import pytest

pytestmark = pytest.mark.unit

# The 9 delta tables that use flow_start (Buckets B + C). ext-side target_table
# -> arc-side name. Identical except for sydax_kreditor,
# whose ext.* is source-prefix orphaned while arc.* keeps its clean name (see
# sydax_archive_configs._EXT_ORPHAN_SOURCE). DFM state keys (flow_start asset
# name + ArchiveMergeConfig.flow_start_key) are keyed off the arc-side name.
EXT_TO_ARC = {
    "sydax_kreditor_legacy_orphan": "sydax_kreditor",
    "sydax_finanskladde": "sydax_finanskladde",
    "sydax_finanskonto_omkostningssted": "sydax_finanskonto_omkostningssted",
    "sydax_finanskonto": "sydax_finanskonto",
    "sydax_finanskladdelinje": "sydax_finanskladdelinje",
    "sydax_kreditortransaktion": "sydax_kreditortransaktion",
    "sydax_finanspostering": "sydax_finanspostering",
    "sydax_finanspostering_kreditortrans_relation": "sydax_finanspostering_kreditortrans_relation",
    "sydax_budgetpost": "sydax_budgetpost",
}
ARC_TARGETS = set(EXT_TO_ARC.values())


@pytest.fixture(scope="module")
def flow_start_assets():
    from DSI.mssql.extract.sydax_extract_configs import sydax_flow_start_assets

    return sydax_flow_start_assets


def test_flow_start_deps_on_staging_infra_dfm(flow_start_assets):
    for a in flow_start_assets:
        dep_keys = {dep.asset_key for spec in a.specs for dep in spec.deps}
        assert dg.AssetKey(["staging_infra_dfm"]) in dep_keys, next(iter(a.keys)).to_user_string()


def test_extract_deps_on_matching_flow_start():
    """Each Bucket B/C ext asset must depend on its sibling flow_start asset."""
    from DSI.mssql.extract.sydax_extract_configs import sydax_archive_extract_assets

    by_key = {next(iter(a.keys)).to_user_string(): a for a in sydax_archive_extract_assets}
    for ext, arc in EXT_TO_ARC.items():
        ext_name = f"ext_{ext}"
        asset = by_key[ext_name]
        dep_keys = {dep.asset_key for spec in asset.specs for dep in spec.deps}
        assert dg.AssetKey([f"dfm_{arc}_flow_start"]) in dep_keys, ext_name


def test_flow_start_key_matches_arc_config():
    """The arc config's flow_start_key reads the row each flow_start asset writes."""
    from core.dfm_config import ArchiveMergeConfig
    from DSI.mssql.archive import sydax_archive_configs as sa
    from DSI.mssql.archive.sydax_archive_configs import sydax_archive_assets  # noqa: F401

    for v in vars(sa).values():
        if isinstance(v, ArchiveMergeConfig) and v.target_table in ARC_TARGETS:
            assert v.flow_start_key == ("dfm", f"{v.target_table}_flow_start"), v.target_table
