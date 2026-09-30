# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the 21 SydAX ArchiveMergeConfig instances."""

import sys
from unittest.mock import MagicMock

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()

import pytest
from DSI.mssql.archive.sydax_archive_configs import (
    AFDELING_SYDAX_ARCHIVE_CONFIG,
    ART_SYDAX_ARCHIVE_CONFIG,
    BUDGETBELOEBTYPE_SYDAX_ARCHIVE_CONFIG,
    BUDGETMODEL_SYDAX_ARCHIVE_CONFIG,
    BUDGETPOST_SYDAX_ARCHIVE_CONFIG,
    BUDGETPOSTSTATUS_SYDAX_ARCHIVE_CONFIG,
    DRANST_SYDAX_ARCHIVE_CONFIG,
    FINANSKLADDE_SYDAX_ARCHIVE_CONFIG,
    FINANSKLADDELINJE_SYDAX_ARCHIVE_CONFIG,
    FINANSKONTO_OMKOSTNINGSSTED_SYDAX_ARCHIVE_CONFIG,
    FINANSKONTO_SYDAX_ARCHIVE_CONFIG,
    FINANSPOSTERING_KREDITORTRANS_RELATION_SYDAX_ARCHIVE_CONFIG,
    FINANSPOSTERING_SYDAX_ARCHIVE_CONFIG,
    FINANSTRANSAKTIONSTYPE_SYDAX_ARCHIVE_CONFIG,
    FUNKTION_SYDAX_ARCHIVE_CONFIG,
    GRUPPERING_AUTORISERET_SYDAX_ARCHIVE_CONFIG,
    GRUPPERING_SYDAX_ARCHIVE_CONFIG,
    HOVEDKONTO_SYDAX_ARCHIVE_CONFIG,
    KREDITOR_SYDAX_ARCHIVE_CONFIG,
    KREDITORTRANSAKTION_SYDAX_ARCHIVE_CONFIG,
    OMKOSTNINGSSTED_HIERARKI_SYDAX_ARCHIVE_CONFIG,
)

pytestmark = pytest.mark.unit

ALL_CONFIGS = [
    AFDELING_SYDAX_ARCHIVE_CONFIG,
    FINANSKONTO_SYDAX_ARCHIVE_CONFIG,
    FINANSKONTO_OMKOSTNINGSSTED_SYDAX_ARCHIVE_CONFIG,
    HOVEDKONTO_SYDAX_ARCHIVE_CONFIG,
    OMKOSTNINGSSTED_HIERARKI_SYDAX_ARCHIVE_CONFIG,
    GRUPPERING_SYDAX_ARCHIVE_CONFIG,
    GRUPPERING_AUTORISERET_SYDAX_ARCHIVE_CONFIG,
    ART_SYDAX_ARCHIVE_CONFIG,
    FUNKTION_SYDAX_ARCHIVE_CONFIG,
    DRANST_SYDAX_ARCHIVE_CONFIG,
    KREDITOR_SYDAX_ARCHIVE_CONFIG,
    FINANSPOSTERING_SYDAX_ARCHIVE_CONFIG,
    KREDITORTRANSAKTION_SYDAX_ARCHIVE_CONFIG,
    FINANSPOSTERING_KREDITORTRANS_RELATION_SYDAX_ARCHIVE_CONFIG,
    FINANSKLADDE_SYDAX_ARCHIVE_CONFIG,
    FINANSKLADDELINJE_SYDAX_ARCHIVE_CONFIG,
    FINANSTRANSAKTIONSTYPE_SYDAX_ARCHIVE_CONFIG,
    BUDGETPOST_SYDAX_ARCHIVE_CONFIG,
    BUDGETMODEL_SYDAX_ARCHIVE_CONFIG,
    BUDGETPOSTSTATUS_SYDAX_ARCHIVE_CONFIG,
    BUDGETBELOEBTYPE_SYDAX_ARCHIVE_CONFIG,
]


def test_archive_source_matches_extract_target():
    """Every arc config must read the ext table a DFM extract config writes.

    This is the cross-config contract the retired MERGE-SQL snapshots pinned:
    a rename on one side silently produces an empty (or failing) MERGE at
    runtime.
    """
    from DSI.mssql.extract.sydax_extract_configs import SYDAX_DFM_EXTRACT_CONFIGS

    extract_targets = {cfg.target_table for cfg in SYDAX_DFM_EXTRACT_CONFIGS}
    for cfg in ALL_CONFIGS:
        assert cfg.source_table in extract_targets, (
            f"arc.{cfg.target_table} reads ext.{cfg.source_table}, but no SydAX "
            f"extract config targets that table"
        )


def test_all_upsert_with_kildesystem_first():
    for cfg in ALL_CONFIGS:
        assert cfg.strategy == "upsert", cfg.target_table
        assert cfg.merge_keys[0] == "Kildesystem", cfg.target_table
        assert len(cfg.merge_keys) >= 2, cfg.target_table


def test_no_overlap_between_keys_and_non_keys():
    for cfg in ALL_CONFIGS:
        overlap = set(cfg.merge_keys) & set(cfg.non_key_columns)
        assert not overlap, f"{cfg.target_table}: merge/non-key overlap {overlap}"


def test_detect_deletes_disabled():
    for cfg in ALL_CONFIGS:
        assert cfg.detect_deletes is False, cfg.target_table
        assert cfg.source_delete_column is None, cfg.target_table


# ext.* is source-prefix orphaned for these 8 tables (sydax.py is the live ext
# writer under the suffix-style name) while the arc.* target keeps its clean
# name — see sydax_archive_configs._EXT_ORPHAN_SOURCE.
_EXT_ORPHAN_TARGETS = {
    "sydax_afdeling",
    "sydax_hovedkonto",
    "sydax_gruppering",
    "sydax_art",
    "sydax_funktion",
    "sydax_dranst",
    "sydax_kreditor",
    "sydax_finanstransaktionstype",
}


def test_target_schema_and_naming():
    for cfg in ALL_CONFIGS:
        assert cfg.target_schema == "arc"
        assert cfg.target_table.startswith("sydax_"), cfg.target_table
        assert cfg.source_schema == "ext"
        if cfg.target_table in _EXT_ORPHAN_TARGETS:
            assert cfg.source_table == f"{cfg.target_table}_legacy_orphan", cfg.target_table
        else:
            assert cfg.source_table == cfg.target_table, cfg.target_table


def test_fast_load_opt_in_covers_large_delta_tables():
    """The empty-target INSERT...SELECT fast path is opted in only for the
    large delta-extracted tables, whose merge_keys equal the arc PK (so ext
    rows unique on the merge key cannot violate it). Bucket A full loads use
    the plain MERGE."""
    enabled = {cfg.target_table for cfg in ALL_CONFIGS if cfg.enable_bulk_insert_on_first_load}
    assert enabled == {
        "sydax_budgetpost",
        "sydax_finanskladde",
        "sydax_finanskladdelinje",
        "sydax_finanskonto",
        "sydax_finanskonto_omkostningssted",
        "sydax_finanspostering",
        "sydax_finanspostering_kreditortrans_relation",
    }
    for cfg in ALL_CONFIGS:
        if cfg.enable_bulk_insert_on_first_load:
            assert not cfg.upstream_is_full_load, cfg.target_table


def test_checkpoint_strategy_per_bucket():
    """Every SydAX arc table is either a full load or flow_start-driven, so none
    carries a per-table checkpoint_query (flow_start owns advancement)."""
    for cfg in ALL_CONFIGS:
        assert cfg.checkpoint_query is None, cfg.target_table
        # Exactly one of: full load (Bucket A) or flow_start (Buckets B+C).
        has_full = cfg.upstream_is_full_load
        has_flow = cfg.flow_start_key is not None
        assert has_full ^ has_flow, cfg.target_table
        if has_flow:
            assert cfg.flow_start_key == ("dfm", f"{cfg.target_table}_flow_start")
