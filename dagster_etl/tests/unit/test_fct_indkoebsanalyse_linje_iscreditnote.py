# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for IsCreditnote on fct.IndkoebsanalyseLinje (task 28292)."""

import sys

import DSA.SQL.fct.FctUpd_indkoebsanalyse_linje  # noqa: F401
import DSA.SQL.staging.stg_fct_indkoebsanalyse_linje_kmd  # noqa: F401
import pytest

_fct_module = sys.modules["DSA.SQL.fct.FctUpd_indkoebsanalyse_linje"]
_stg_module = sys.modules["DSA.SQL.staging.stg_fct_indkoebsanalyse_linje_kmd"]

RESOLVE_SQL = _fct_module.RESOLVE_SQL
MERGE_SQL = _fct_module.MERGE_SQL
BUILD_SQL = _stg_module.BUILD_SQL


@pytest.mark.unit
class TestIsCreditnote:
    """Verify IsCreditnote flows from staging through to fact MERGE."""

    def test_staging_joins_invoice_archive(self):
        assert "arc].[invoice_kmd_uddata] inv" in BUILD_SQL
        assert "inv.[DW_SK_Invoice] = sal.[DW_SK_Invoice]" in BUILD_SQL
        assert "inv.[DW_IsCurrent] = 1" in BUILD_SQL

    def test_staging_casts_iscreditnote_as_bit(self):
        assert "CAST(COALESCE(inv.[IsCreditnote], 0) AS BIT) AS IsCreditnote" in BUILD_SQL

    def test_resolve_selects_iscreditnote(self):
        assert "stg.[IsCreditnote]" in RESOLVE_SQL

    def test_merge_updates_iscreditnote(self):
        assert "tgt.[IsCreditnote] = src.[IsCreditnote]" in MERGE_SQL

    def test_merge_inserts_iscreditnote(self):
        assert "[IsCreditnote]," in MERGE_SQL
        assert "src.[IsCreditnote]," in MERGE_SQL

    def test_stg_asset_depends_on_arc_invoice(self):
        asset_def = _stg_module.stg_fct_indkoebsanalyse_linje_kmd
        dep_keys = asset_def.dependency_keys
        dep_strs = {"/".join(k.path) for k in dep_keys}
        assert "arc_invoice_kmd_uddata" in dep_strs
