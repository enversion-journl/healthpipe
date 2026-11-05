# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the EPJI LOCATION DFM extract config.

The hand-rolled extract asset was replaced with the shared DFM extract
factory (create_dfm_extraction_assets) — same pattern as IFDB. Dedup and
conflict-visibility for a duplicate EXTERNAL_ID moved to arc_epji_shakalike
(ArchiveMergeConfig.dedup_source_on_keys) — see test_epji_archive.py.
"""

import pytest
from DSI.oracle.epji_extract import (
    EPJI_LOCATION_EXTRACT_CONFIG,
    EXT_COLUMNS,
    epji_location_extract_assets,
)

pytestmark = pytest.mark.unit


def test_config_is_full_load_with_no_delta_filter():
    cfg = EPJI_LOCATION_EXTRACT_CONFIG
    assert cfg.is_full_load is True
    assert cfg.delta_column is None
    assert cfg.delta_expression is None


def test_config_columns_match_column_types_and_arc_schema():
    cfg = EPJI_LOCATION_EXTRACT_CONFIG
    assert cfg.columns == EXT_COLUMNS
    assert set(cfg.column_types) == set(EXT_COLUMNS)
    assert cfg.column_types["code"] == "NVARCHAR(100) NOT NULL"
    assert cfg.index_keys == ("code",)


def test_snapshot_sql_display_is_location_name_not_the_code():
    # display must carry LOCATION.NAME, not repeat EXTERNAL_ID (the code) —
    # a dimension whose name column duplicates its key isn't useful in
    # reporting. Falls back to EXTERNAL_ID only when NAME is empty.
    sql = EPJI_LOCATION_EXTRACT_CONFIG.select_from_expr
    assert 'AS "display"' in sql
    assert "COALESCE(NULLIF(TRIM(l.NAME), ''), TRIM(l.EXTERNAL_ID))" in sql


def test_snapshot_sql_filters_to_current_operational_period():
    # LOCATION is historized: a rename closes the old row's OPERATIONAL_TO
    # and opens a new row on the same EXTERNAL_ID. Without this filter, a
    # rename would return two rows per code.
    sql = EPJI_LOCATION_EXTRACT_CONFIG.select_from_expr
    assert "l.OPERATIONAL_TO IS NULL" in sql


def test_snapshot_sql_aliases_are_quoted_lowercase_matching_ext_columns():
    # Unquoted Oracle aliases come back upper-cased in cursor.description;
    # `columns` (used for both DDL and dict lookups) must match exactly.
    sql = EPJI_LOCATION_EXTRACT_CONFIG.select_from_expr
    for col in EXT_COLUMNS:
        assert f'AS "{col}"' in sql


def test_generated_asset_name_and_deps():
    assert len(epji_location_extract_assets) == 1
    asset_def = epji_location_extract_assets[0]
    assert asset_def.key.to_user_string() == "ext_epji_location"
    dep_keys = {key.to_user_string() for key in asset_def.dependency_keys}
    assert "staging_infra_source_epji" in dep_keys
