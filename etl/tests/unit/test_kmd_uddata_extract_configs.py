# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for KMD UdData DFM extract configs."""

import pytest
from DSI.connectors.factory import create_dfm_extraction_assets
from DSI.mssql.extract.kmd_uddata_extract_configs import KMD_EXTRACT_CONFIGS

pytestmark = pytest.mark.unit


def test_kmd_extract_configs_use_audit_delta_only():
    for cfg in KMD_EXTRACT_CONFIGS:
        assert "GREATEST([DW_Audit_Insert]" in cfg.delta_expression
        assert cfg.checkpoint_from is not None
        assert "issue" not in (cfg.description or "").lower()


def test_kmd_extract_assets_use_standard_factory_without_mysupply_deps():
    assets = create_dfm_extraction_assets(
        configs=KMD_EXTRACT_CONFIGS,
        source_resource_key="kmd_uddata_mssql",
        source_resource_type=object,
        source_system="KMD_UdData",
        group_name="kmd_uddata_archive",
        domain_tag="kmd_uddata",
    )
    assert len(assets) == len(KMD_EXTRACT_CONFIGS)
    for asset_def in assets:
        dep_keys = {str(dep) for dep in asset_def.dependency_keys}
        assert not any("mysupply" in dep.lower() for dep in dep_keys)


def test_fast_load_opt_in_covers_the_two_large_archives():
    """invoice and spend_analysis_lines take the empty-target INSERT...SELECT
    fast path on first load — their merge key is exactly the arc PK (a KMD DW
    surrogate key). The small unspsc table uses the plain MERGE."""
    from DSI.mssql.archive.kmd_uddata_archive_configs import kmd_uddata_archive_configs

    enabled = {
        cfg.target_table
        for cfg in kmd_uddata_archive_configs
        if cfg.enable_bulk_insert_on_first_load
    }
    assert enabled == {"kmd_uddata_invoice", "kmd_uddata_spend_analysis_lines"}


def test_spend_analysis_ext_is_deduped_on_entity_key_before_merge():
    """Plausibel bug: KMD leverer dubletter (samme DW_EK_SpendAnalysisLine, kun
    afvigende surrogat-FK'er mod dublerede dimension-members). arc upsertes OG
    bulk-insert'er paa DW_EK, saa dubletter i ext braekker begge stier. Pre-merge
    skal dedup'e ext ned til én raekke pr. DW_EK inden MERGE/bulk-insert."""
    from DSI.mssql.archive.kmd_uddata_archive_configs import (
        _DEDUP_SPEND_ANALYSIS_EXT_SQL,
    )

    assert "PARTITION BY [DW_EK_SpendAnalysisLine]" in _DEDUP_SPEND_ANALYSIS_EXT_SQL
    assert "DELETE FROM ranked WHERE rn > 1" in _DEDUP_SPEND_ANALYSIS_EXT_SQL
