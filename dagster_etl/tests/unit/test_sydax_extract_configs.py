# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for SYDAX_DFM_EXTRACT_CONFIGS."""

import sys
from unittest.mock import MagicMock

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()


from DSI.mssql.extract.sydax_extract_configs import (
    SYDAX_DFM_EXTRACT_CONFIGS,
    sydax_archive_extract_assets,
)

EXPECTED_TARGETS = {
    "afdeling_sydax_dfm",
    "finanskonto_sydax_dfm",
    "finanskonto_omkostningssted_sydax_dfm",
    "hovedkonto_sydax_dfm",
    "omkostningssted_hierarki_sydax_dfm",
    "gruppering_sydax_dfm",
    "gruppering_autoriseret_sydax_dfm",
    "art_sydax_dfm",
    "funktion_sydax_dfm",
    "dranst_sydax_dfm",
    "kreditor_sydax_dfm",
    "finanspostering_sydax_dfm",
    "kreditortransaktion_sydax_dfm",
    "finanspostering_kreditortrans_relation_sydax_dfm",
    "finanskladde_sydax_dfm",
    "finanskladdelinje_sydax_dfm",
    "finanstransaktionstype_sydax_dfm",
}


def test_seventeen_configs():
    assert len(SYDAX_DFM_EXTRACT_CONFIGS) == 17


def test_target_table_coverage():
    targets = {cfg.target_table for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    assert targets == EXPECTED_TARGETS


def test_all_targets_end_in_sydax_dfm():
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        assert cfg.target_table.endswith("_sydax_dfm"), cfg.target_table


def test_delta_strategy_uniform():
    bridge_target = "finanspostering_kreditortrans_relation_sydax_dfm"
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        assert cfg.is_full_load is False, cfg.target_table
        assert cfg.checkpoint_type == "datetime"
        assert cfg.lookback_days == 3
        if cfg.target_table == bridge_target:
            assert (
                cfg.delta_expression == "COALESCE(bridge.[Modified], bridge.[CreatedOn])"
            ), cfg.target_table
        else:
            assert cfg.delta_expression == "COALESCE([Modified], [CreatedOn])", cfg.target_table


def test_checkpoint_from_points_to_arc():
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        assert cfg.checkpoint_from is not None, cfg.target_table
        schema, table = cfg.checkpoint_from
        assert schema == "arc"
        assert table == cfg.target_table.removesuffix("_dfm")


def test_null_filter_on_ref_id_tables():
    targets_with_null_filter = {
        "hovedkonto_sydax_dfm",
        "gruppering_sydax_dfm",
        "gruppering_autoriseret_sydax_dfm",
        "art_sydax_dfm",
        "funktion_sydax_dfm",
        "dranst_sydax_dfm",
        "finanspostering_sydax_dfm",
        "kreditortransaktion_sydax_dfm",
        "finanskladdelinje_sydax_dfm",
    }
    cfgs_by_target = {cfg.target_table: cfg for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    for tgt in targets_with_null_filter:
        cfg = cfgs_by_target[tgt]
        # The custom WHERE / select_from_expr must mention "IS NOT NULL" on
        # the stable key. Either as a custom_where_clause (not a field on
        # DfmExtractConfig today — implemented via select_from_expr WHERE).
        assert cfg.select_from_expr is not None, tgt
        assert "IS NOT NULL" in cfg.select_from_expr, tgt


def test_bridge_uses_select_from_expr_with_join():
    cfgs_by_target = {cfg.target_table: cfg for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    bridge = cfgs_by_target["finanspostering_kreditortrans_relation_sydax_dfm"]
    assert bridge.select_from_expr is not None
    assert "JOIN" in bridge.select_from_expr.upper()
    assert "ref_LedgertransId" in bridge.select_from_expr


def test_assets_generated():
    assert len(sydax_archive_extract_assets) == 17
