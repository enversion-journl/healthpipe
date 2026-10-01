# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Invariant tests for SYDAX_DFM_EXTRACT_CONFIGS."""

import sys
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()


from DSI.mssql.extract.sydax_extract_configs import SYDAX_DFM_EXTRACT_CONFIGS


def test_all_targets_are_source_prefixed_without_dfm():
    # The `_dfm` cutover postfix was dropped in 20260701_drop_ext_dfm_postfix.sql.
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        assert cfg.target_table.startswith("sydax_") and not cfg.target_table.endswith(
            "_dfm"
        ), cfg.target_table


def test_full_load_tables_carry_no_delta_machinery():
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        if cfg.is_full_load:
            assert cfg.delta_expression is None, cfg.target_table
            assert cfg.checkpoint_from is None, cfg.target_table
        else:
            assert cfg.delta_expression is not None, cfg.target_table


def test_checkpoint_from_points_to_arc_for_delta_tables():
    # ext.* is source-prefix orphaned for sydax_kreditor (sydax.py is the live
    # ext writer under the suffix-style name) while arc.* keeps its clean name
    # — the checkpoint must point at the arc name, not the ext target_table.
    for cfg in SYDAX_DFM_EXTRACT_CONFIGS:
        if cfg.is_full_load:
            continue
        assert cfg.checkpoint_from is not None, cfg.target_table
        schema, table = cfg.checkpoint_from
        assert schema == "arc"
        assert table == cfg.target_table.removesuffix("_legacy_orphan")


def test_null_filter_on_nullable_key_tables():
    """Contract pin: sources keyed on a nullable RECID/ERP key must filter
    NULL keys at extract time, or the archive MERGE hits PK violations."""
    targets_with_null_filter = {
        # full-load masters keyed on a nullable RECID/ref_RECID
        "sydax_hovedkonto_legacy_orphan",
        "sydax_gruppering_legacy_orphan",
        "sydax_gruppering_autoriseret",
        "sydax_art_legacy_orphan",
        "sydax_funktion_legacy_orphan",
        "sydax_dranst_legacy_orphan",
        "sydax_afdeling_legacy_orphan",
        # delta transaction tables keyed on a nullable ERP key
        "sydax_finanspostering",
        "sydax_kreditortransaktion",
        "sydax_finanskladdelinje",
        "sydax_finanspostering_kreditortrans_relation",
    }
    cfgs_by_target = {cfg.target_table: cfg for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    for tgt in targets_with_null_filter:
        cfg = cfgs_by_target[tgt]
        assert cfg.select_from_expr is not None, tgt
        assert "IS NOT NULL" in cfg.select_from_expr, tgt
