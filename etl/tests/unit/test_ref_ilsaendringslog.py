# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for ref.ILSAendringslog (ILS dbo.changelog as-is) contracts."""

from pathlib import Path

import pytest
from DSA.SQL.ref.ils_aendringslog import ref_ILSAendringslog
from DSA.SQL.ref.ils_aendringslog_sql import (
    SOURCE_COLUMNS,
    TIDSPUNKT_EXPR,
    build_invalid_klok_count_sql,
    build_merge_sql,
)
from DSO.load.sync_configs import SHARED_DIMS_REF_CONFIGS

ROOT = Path(__file__).resolve().parents[2]
BASE_DDL = ROOT / "infrastructure" / "ddl" / "tables" / "ref" / "01_tables.sql"


@pytest.mark.unit
def test_casing_rules():
    assert SOURCE_COLUMNS["tabelnavn"].startswith("LOWER(")
    assert SOURCE_COLUMNS["noegle"].startswith("UPPER(")
    assert SOURCE_COLUMNS["ajourfoert_af"].startswith("UPPER(")
    # Fra/Til are logged values: kept verbatim.
    assert SOURCE_COLUMNS["fra"] == "a.[fravaerdi]"
    assert SOURCE_COLUMNS["til"] == "a.[tilvaerdi]"


@pytest.mark.unit
def test_noegle_is_raw_without_fk_resolution():
    """Key semantics vary per tabelnavn, so the base table joins no dimension."""
    sql = build_merge_sql(full_scope=True)
    assert "JOIN" not in sql.upper()
    assert "dw_ek_" not in sql


@pytest.mark.unit
def test_tidspunkt_guards_out_of_day_klok():
    assert "BETWEEN 0 AND 86399" in TIDSPUNKT_EXPR


@pytest.mark.unit
def test_invalid_klok_count_matches_merge_scope():
    """The warning counts the same arc rows the MERGE read, with the same parameter."""
    for full_scope in (True, False):
        count_sql = build_invalid_klok_count_sql(full_scope=full_scope)
        assert count_sql.count("?") == build_merge_sql(full_scope=full_scope).count("?")


@pytest.mark.unit
def test_merge_scope():
    delta = build_merge_sql(full_scope=False)
    full = build_merge_sql(full_scope=True)
    assert "a.[dw_timestamp_update] > CAST(? AS DATETIME2(0))" in delta
    assert delta.count("?") == 1
    assert "WHERE a.[dw_timestamp_update]" not in full
    assert "?" not in full
    for sql in (delta, full):
        assert "[arc].[ils_changelog]" in sql
        assert "ON tgt.[prrowid] = src.[prrowid]" in sql
        # Append-only log: no delete branch.
        assert "NOT MATCHED BY SOURCE" not in sql
        # Only real changes move the DSO watermark.
        assert "EXCEPT" in sql


@pytest.mark.unit
def test_asset_wiring():
    spec = ref_ILSAendringslog.get_asset_spec()
    assert spec.group_name == "erp_ref"
    dep_keys = {k.to_user_string() for k in ref_ILSAendringslog.dependency_keys}
    assert "arc_ils_changelog" in dep_keys


@pytest.mark.unit
def test_dso_sync_config_registered():
    assert "ref_ILSAendringslog" in SHARED_DIMS_REF_CONFIGS


@pytest.mark.unit
def test_dso_sync_asset_registered():
    from DSO.load.sync_ref_erp import all_dso_sync_ref_erp_assets

    keys = {spec.key.to_user_string() for a in all_dso_sync_ref_erp_assets for spec in a.specs}
    assert "dso_sync_ref_ILSAendringslog" in keys


@pytest.mark.unit
def test_base_ddl_defines_every_merged_column():
    text = BASE_DDL.read_text(encoding="utf-8")
    assert "[ref].[ILSAendringslog]" in text
    for col in SOURCE_COLUMNS:
        assert f"[{col}]" in text, col
