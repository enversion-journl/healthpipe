# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the 17 SydAX ArchiveMergeConfig instances."""

import sys
from unittest.mock import MagicMock

# mssql_python is a native driver that is not installed in the test environment.
# Stub it before any DSI.mssql imports to avoid ModuleNotFoundError.
if "mssql_python" not in sys.modules:
    sys.modules["mssql_python"] = MagicMock()

from DSI.mssql.archive.sydax_archive_configs import (
    AFDELING_SYDAX_ARCHIVE_CONFIG,
    ART_SYDAX_ARCHIVE_CONFIG,
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
]


def test_seventeen_configs():
    assert len(ALL_CONFIGS) == 17


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


def test_target_schema_and_naming():
    for cfg in ALL_CONFIGS:
        assert cfg.target_schema == "arc"
        assert cfg.target_table.endswith("_sydax"), cfg.target_table
        assert cfg.source_schema == "ext"
        assert cfg.source_table == f"{cfg.target_table}_dfm", cfg.target_table


def test_checkpoint_query_pattern():
    for cfg in ALL_CONFIGS:
        assert cfg.checkpoint_query is not None, cfg.target_table
        assert "MAX(COALESCE([Modified], [CreatedOn]))" in cfg.checkpoint_query
        assert f"[arc].[{cfg.target_table}]" in cfg.checkpoint_query
