# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for vw_fct.Vareudlevering_ReeltForbrug."""

import re
from pathlib import Path

import pytest
from infrastructure.assets.dso_infrastructure import dso_infra_views_erp

_DDL = (
    Path(__file__).resolve().parents[2]
    / "infrastructure"
    / "ddl"
    / "views"
    / "erp"
    / "02_vw_fct.sql"
)

_VIEW_PATTERN = re.compile(
    r"CREATE\s+OR\s+ALTER\s+VIEW\s+\[vw_fct\]\.\[Vareudlevering_ReeltForbrug\]",
    re.IGNORECASE,
)


@pytest.mark.unit
def test_reelt_forbrug_view_exists():
    sql = _DDL.read_text(encoding="utf-8")
    assert _VIEW_PATTERN.search(sql), "vw_fct.Vareudlevering_ReeltForbrug must be defined"


@pytest.mark.unit
def test_reelt_forbrug_filters_via_ref_not_exists():
    sql = _DDL.read_text(encoding="utf-8")
    view_sql = sql.split("[Vareudlevering_ReeltForbrug]")[1].split("GO")[0]
    assert "NOT EXISTS" in view_sql
    assert "[ref].[RekvirentUndtagelse]" in view_sql
    assert "clean_code_Rekvirent" in view_sql
    # Must not hard-filter fact load; base Vareudlevering view stays unfiltered.
    base = sql.split("[Vareudlevering]")[1].split("GO")[0]
    assert "RekvirentUndtagelse" not in base


@pytest.mark.unit
def test_reelt_forbrug_uses_pad_only_not_right_truncate():
    sql = _DDL.read_text(encoding="utf-8")
    view_sql = sql.split("[Vareudlevering_ReeltForbrug]")[1].split("GO")[0]
    assert "REPLICATE('0', 4)" in view_sql
    assert "RIGHT('0000'" not in view_sql


@pytest.mark.unit
def test_dso_views_erp_depends_on_ref():
    dep_keys = {k.to_user_string() for k in dso_infra_views_erp.dependency_keys}
    assert "dso_infra_ref" in dep_keys
