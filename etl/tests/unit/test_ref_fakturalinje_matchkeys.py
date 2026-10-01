# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ref_fakturalinje_matchkeys asset + SQL builder."""

import sys

import DSA.SQL.ref.fakturalinje_matchkeys  # noqa: F401
import pytest
from DSA.SQL.ref.fakturalinje_matchkeys import build_matchkeys_merge_sql

pytestmark = pytest.mark.unit


class TestMatchkeysMergeSql:
    def test_merges_into_matchkeys_table(self):
        sql = build_matchkeys_merge_sql()
        assert "MERGE [ref].[FakturalinjeMatchKeys]" in sql
        assert "ON tgt.[dw_ek_Fakturalinje] = s.[dw_ek_Fakturalinje]" in sql

    def test_reads_staging_and_map(self):
        sql = build_matchkeys_merge_sql()
        assert "[stg].[fct_Fakturalinje_mysupply]" in sql
        assert "[map].[Fakturalinje]" in sql

    def test_carries_all_match_keys(self):
        sql = build_matchkeys_merge_sql()
        for col in (
            "[clean_code_Kreditor]",
            "[clean_code_Kreditor_header]",
            "[clean_code_Faktura]",
            "[linje_beloeb]",
            "[enheds_pris]",
            "[antal_enheder_faktureret]",
            "[faktura_total]",
        ):
            assert col in sql

    def test_carries_denormalised_faktura_date_key(self):
        # The apply-pass scopes its delta on this column instead of joining
        # fct.Fakturalinje for the invoice date.
        sql = build_matchkeys_merge_sql()
        assert "AS dw_ek_Dato_faktura" in sql
        assert "[dw_ek_Dato_faktura]" in sql

    def test_faktura_date_is_part_of_change_detection(self):
        # This table's watermark is the apply-pass's only delta trigger, so a
        # corrected invoice date that moves a line into scope must bump it.
        sql = build_matchkeys_merge_sql()
        intersect_block = sql.split("WHEN MATCHED AND NOT EXISTS")[1].split("THEN")[0]
        assert intersect_block.count("[dw_ek_Dato_faktura]") == 2

    def test_faktura_total_coalesces_metadata_then_header(self):
        sql = build_matchkeys_merge_sql()
        assert "COALESCE(src.[metadata_faktura_total], src.[header_faktura_total])" in sql

    def test_insert_sets_both_timestamps(self):
        sql = build_matchkeys_merge_sql()
        assert "[dw_timestamp_insert]," in sql
        assert "[dw_timestamp_update]" in sql
        # Two SYSDATETIME() literals in the INSERT VALUES (insert + update).
        assert sql.count("CAST(SYSDATETIME() AS datetime2(0))") >= 3  # update-set + 2 insert

    def test_change_detection_is_null_safe(self):
        sql = build_matchkeys_merge_sql()
        assert "INTERSECT" in sql  # NULL-safe "any column differs"

    def test_no_delete_branch(self):
        sql = build_matchkeys_merge_sql()
        assert "NOT MATCHED BY SOURCE" not in sql
        assert "dw_is_deleted" not in sql


_module = sys.modules["DSA.SQL.ref.fakturalinje_matchkeys"]
ref_fakturalinje_matchkeys_asset = _module.ref_fakturalinje_matchkeys


class TestMatchkeysAsset:
    def test_group_is_erp_ref(self):
        spec = ref_fakturalinje_matchkeys_asset.get_asset_spec()
        assert spec.group_name == "erp_ref"

    def test_depends_on_staging_and_map(self):
        dep_keys = {str(dep) for dep in ref_fakturalinje_matchkeys_asset.dependency_keys}
        assert any("stg_fct_Fakturalinje_mysupply" in d for d in dep_keys)
        assert any("map_Fakturalinje" in d for d in dep_keys)

    def test_registered_in_ref_package(self):
        from DSA.SQL.ref import all_assets

        assert ref_fakturalinje_matchkeys_asset in all_assets
