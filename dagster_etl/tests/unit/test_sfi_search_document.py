# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the SFI metadata search document asset."""

import pytest
from dagster import AssetKey
from DSO.load.sync_codesystem import (
    _build_sfi_search_document_refresh_sql,
    all_codesystem_dso_sync_assets,
    dso_refresh_codesystem_sfi_search_document,
)


@pytest.mark.unit
def test_sfi_search_document_waits_for_sfi_codesystem_sync() -> None:
    asset_key = AssetKey.from_user_string("dso_refresh_codesystem_sfi_search_document")
    deps = dso_refresh_codesystem_sfi_search_document.asset_deps[asset_key]

    assert AssetKey.from_user_string("dso_sync_codesystem_sfi_concept") in deps
    assert AssetKey.from_user_string("dso_sync_codesystem_sfi_properties") in deps
    assert AssetKey.from_user_string("dso_sync_codesystem_sfi_hierarchy") in deps


@pytest.mark.unit
def test_sfi_search_document_is_in_codesystem_dso_sync_group() -> None:
    asset_names = {
        next(iter(asset.keys)).to_user_string() for asset in all_codesystem_dso_sync_assets
    }

    assert "dso_refresh_codesystem_sfi_search_document" in asset_names


@pytest.mark.unit
def test_refresh_sql_merges_into_search_document() -> None:
    sql = _build_sfi_search_document_refresh_sql()
    assert "MERGE [codesystem].[SFISearchDocument] AS tgt" in sql
    assert "FROM [codesystem].[SFIConcept]" in sql
    assert "[codesystem].[SFIProperties]" in sql
    assert "[codesystem].[SFIAliasMember]" in sql
    assert "[codesystem].[SFISearchOverride]" in sql


@pytest.mark.unit
def test_refresh_sql_updates_only_on_hash_diff() -> None:
    """Rows must only rewrite when the SHA2_256 of search_text changes."""
    sql = _build_sfi_search_document_refresh_sql()
    assert "HASHBYTES('SHA2_256'" in sql
    assert "tgt.[search_source_hash] <> src.[search_source_hash]" in sql
    assert "tgt.[search_source_hash] IS NULL" in sql


@pytest.mark.unit
def test_refresh_sql_soft_deletes_disappeared_concepts() -> None:
    """Concepts gone from the sources must soft-delete, never hard-delete."""
    sql = _build_sfi_search_document_refresh_sql()
    assert "WHEN NOT MATCHED BY SOURCE AND tgt.[is_active] = 1" in sql
    assert "[is_active] = 0" in sql
    assert "DELETE" not in sql.upper().replace("SOFT-DELETE", "")
