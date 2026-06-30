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
    "sydax_afdeling_dfm",
    "sydax_finanskonto_dfm",
    "sydax_finanskonto_omkostningssted_dfm",
    "sydax_hovedkonto_dfm",
    "sydax_omkostningssted_hierarki_dfm",
    "sydax_gruppering_dfm",
    "sydax_gruppering_autoriseret_dfm",
    "sydax_art_dfm",
    "sydax_funktion_dfm",
    "sydax_dranst_dfm",
    "sydax_kreditor_dfm",
    "sydax_finanspostering_dfm",
    "sydax_kreditortransaktion_dfm",
    "sydax_finanspostering_kreditortrans_relation_dfm",
    "sydax_finanskladde_dfm",
    "sydax_finanskladdelinje_dfm",
    "sydax_finanstransaktionstype_dfm",
}


def test_seventeen_configs():
    assert len(SYDAX_DFM_EXTRACT_CONFIGS) == 17


def test_target_table_coverage():
    targets = {cfg.target_table for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    assert targets == EXPECTED_TARGETS


def test_all_targets_end_in_sydax_dfm():
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        assert cfg.target_table.startswith("sydax_") and cfg.target_table.endswith(
            "_dfm"
        ), cfg.target_table


# Per-table delta strategy (see the 2026-06-25 design spec).
_FULL_LOAD = {
    "sydax_dranst_dfm",
    "sydax_art_dfm",
    "sydax_finanstransaktionstype_dfm",
    "sydax_gruppering_autoriseret_dfm",
    "sydax_funktion_dfm",
    "sydax_hovedkonto_dfm",
    "sydax_gruppering_dfm",
    "sydax_afdeling_dfm",
    "sydax_omkostningssted_hierarki_dfm",
}
_RDWTS_DELTA = {
    "sydax_kreditor_dfm",
    "sydax_finanskladde_dfm",
    "sydax_finanskonto_omkostningssted_dfm",
    "sydax_finanskonto_dfm",
    "sydax_finanskladdelinje_dfm",
}
_BUSINESS_TS_DELTA = {
    "sydax_kreditortransaktion_dfm",
    "sydax_finanspostering_dfm",
    "sydax_finanspostering_kreditortrans_relation_dfm",
}


def test_buckets_partition_all_17():
    assert _FULL_LOAD | _RDWTS_DELTA | _BUSINESS_TS_DELTA == EXPECTED_TARGETS
    assert len(_FULL_LOAD) + len(_RDWTS_DELTA) + len(_BUSINESS_TS_DELTA) == 17


def test_full_load_tables_have_no_delta():
    by = {c.target_table: c for c in SYDAX_DFM_EXTRACT_CONFIGS}
    for tgt in _FULL_LOAD:
        cfg = by[tgt]
        assert cfg.is_full_load is True, tgt
        assert cfg.delta_expression is None, tgt
        assert cfg.checkpoint_from is None, tgt


def test_rdwts_delta_tables():
    by = {c.target_table: c for c in SYDAX_DFM_EXTRACT_CONFIGS}
    for tgt in _RDWTS_DELTA:
        cfg = by[tgt]
        assert cfg.is_full_load is False, tgt
        assert cfg.delta_expression == "[RdwTs]", tgt
        assert cfg.checkpoint_type == "datetime"
        assert cfg.lookback_days == 3


def test_business_ts_delta_tables():
    by = {c.target_table: c for c in SYDAX_DFM_EXTRACT_CONFIGS}
    assert by["sydax_finanspostering_dfm"].delta_expression == "[Oprettelsestidspunkt]"
    assert by["sydax_finanspostering_kreditortrans_relation_dfm"].delta_expression == (
        "post.[Oprettelsestidspunkt]"
    )
    kt = by["sydax_kreditortransaktion_dfm"].delta_expression
    # GREATEST emulation over the three activity timestamps.
    assert "MAX(v)" in kt
    for col in ("SidstUdlignetDato", "GodkendtDato", "Oprettelsestidspunkt"):
        assert col in kt, col


def test_checkpoint_from_points_to_arc_for_delta_tables():
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        if cfg.is_full_load:
            continue
        assert cfg.checkpoint_from is not None, cfg.target_table
        schema, table = cfg.checkpoint_from
        assert schema == "arc"
        assert table == cfg.target_table.removesuffix("_dfm")


def test_null_filter_on_nullable_key_tables():
    targets_with_null_filter = {
        # full-load masters keyed on a nullable RECID/ref_RECID
        "sydax_hovedkonto_dfm",
        "sydax_gruppering_dfm",
        "sydax_gruppering_autoriseret_dfm",
        "sydax_art_dfm",
        "sydax_funktion_dfm",
        "sydax_dranst_dfm",
        "sydax_afdeling_dfm",
        # delta transaction tables keyed on a nullable ERP key
        "sydax_finanspostering_dfm",
        "sydax_kreditortransaktion_dfm",
        "sydax_finanskladdelinje_dfm",
        "sydax_finanspostering_kreditortrans_relation_dfm",
    }
    cfgs_by_target = {cfg.target_table: cfg for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    for tgt in targets_with_null_filter:
        cfg = cfgs_by_target[tgt]
        assert cfg.select_from_expr is not None, tgt
        assert "IS NOT NULL" in cfg.select_from_expr, tgt


def test_bridge_uses_select_from_expr_with_join():
    cfgs_by_target = {cfg.target_table: cfg for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    bridge = cfgs_by_target["sydax_finanspostering_kreditortrans_relation_dfm"]
    assert bridge.select_from_expr is not None
    assert "JOIN" in bridge.select_from_expr.upper()
    assert "ref_LedgertransId" in bridge.select_from_expr


def test_assets_generated():
    assert len(sydax_archive_extract_assets) == 17
