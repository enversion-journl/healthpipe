# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL contract tests for dim.Rekvirent.er_undtaget (AB#29116 PR-C)."""

from pathlib import Path

import pytest
from DSA.SQL.dim.dim_Rekvirent import dim_Rekvirent

ROOT = Path(__file__).resolve().parents[2]
DIM_PY = ROOT / "DSA" / "SQL" / "dim" / "dim_Rekvirent.py"
MIGRATION = ROOT / "infrastructure" / "ddl" / "migrations" / "20260818_dim_rekvirent_erundtaget.sql"
BASE_DDL = ROOT / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"
SECURITY_VIEW = ROOT / "infrastructure" / "ddl" / "views" / "security" / "01_vw_dim_security.sql"
LINEAGE = ROOT / "data_lineage" / "dim" / "dim_rekvirent.yml"


@pytest.mark.unit
def test_dim_asset_depends_on_ref_undtagelse():
    dep_keys = {k.to_user_string() for k in dim_Rekvirent.dependency_keys}
    assert "ref_RekvirentUndtagelse" in dep_keys


@pytest.mark.unit
def test_dim_sql_joins_ref_on_normalized_sgh_rnr():
    src = DIM_PY.read_text(encoding="utf-8")
    assert "[ref].[RekvirentUndtagelse]" in src
    assert "er_undtaget" in src
    assert "u.sgh =" in src
    assert "u.rnr =" in src
    # Must not join exclusion list via dw_ek (orphans have NULL dw_ek).
    assert "u.dw_ek_Rekvirent" not in src


@pytest.mark.unit
def test_dim_merge_writes_erundtaget():
    src = DIM_PY.read_text(encoding="utf-8")
    assert "er_undtaget = source.er_undtaget" in src
    assert "source.er_undtaget," in src or "source.er_undtaget\n" in src


@pytest.mark.unit
def test_ddl_and_migration_add_erundtaget():
    # The base DDL now names every default DF_<table>_<column>; the 2026-08-18
    # migration keeps the name it originally shipped, and
    # 20260825b_name_audit_default_constraints.sql renames it on deployed
    # databases. So the two files legitimately carry different names.
    for path, constraint in (
        (BASE_DDL, "DF_Rekvirent_er_undtaget"),
        (MIGRATION, "DF_dim_Rekvirent_er_undtaget"),
    ):
        text = path.read_text(encoding="utf-8")
        assert "er_undtaget" in text
        assert constraint in text


@pytest.mark.unit
def test_security_view_exposes_erundtaget():
    text = SECURITY_VIEW.read_text(encoding="utf-8")
    view = text.split("[vw_dim].[Rekvirent]")[1].split("GO")[0]
    assert "er_undtaget" in view


@pytest.mark.unit
def test_lineage_documents_erundtaget():
    text = LINEAGE.read_text(encoding="utf-8")
    assert "er_undtaget:" in text
    assert "ref.RekvirentUndtagelse" in text
