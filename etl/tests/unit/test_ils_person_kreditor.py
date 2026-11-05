# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ILS giro = 'Person' rows never yield a clean_code_Kreditor.

For Person suppliers (home patients, employees with expenses) lev.se_nr holds
a birth date or CPR number, not a CVR. Every ILS chain that derives a
clean_code_Kreditor from se_nr must go through the guarded
``sql_ils_kreditor_cleancode``; these tests fail when a chain builds the bare
se_nr formula again, so a person cannot become, or link to, a dim.Kreditor member.
"""

import re
from pathlib import Path

import pytest
from DSA.SQL.dim.dim_Kreditor import _build_resolve_sql
from DSA.SQL.staging.pre_stg_Vareaftaler_ils import ILS_KREDITOR_CC as VAREAFTALER_CC
from DSA.SQL.staging.stg_dim_ILSLeverandoer import STG_SQL as ILSLEVERANDOER_STG_SQL
from DSA.SQL.staging.stg_dim_Kreditor import _build_stg_sql
from DSA.SQL.staging.stg_fct_AftaleVarePris_ils import _build_staging_sql
from utils.sql.leverandoervare_ilsvare_sql import sql_lev_current_cte
from utils.sql.tax_id_sql import sql_cvr_cleancode_from_se_nr, sql_ils_kreditor_cleancode

pytestmark = pytest.mark.unit

ROOT = Path(__file__).resolve().parents[2]

BARE = sql_cvr_cleancode_from_se_nr("lev.se_nr")
GUARDED = sql_ils_kreditor_cleancode("lev.se_nr")
PERSON_GUARD = "CASE WHEN lev.giro = 'Person' THEN NULL ELSE"


def test_guarded_clean_code_is_null_for_person_rows():
    assert GUARDED.startswith(PERSON_GUARD)
    assert BARE in GUARDED


@pytest.mark.parametrize(
    "sql",
    [
        pytest.param(_build_stg_sql(None), id="stg_dim_Kreditor-full"),
        pytest.param(_build_stg_sql("2026-10-27 03:00:00"), id="stg_dim_Kreditor-delta"),
        pytest.param(_build_resolve_sql(None), id="dim_Kreditor-ils_pick"),
        pytest.param(sql_lev_current_cte(), id="lev_current"),
        pytest.param(_build_staging_sql(""), id="stg_fct_AftaleVarePris_ils"),
        pytest.param(VAREAFTALER_CC, id="pre_stg_Vareaftaler_ils"),
        pytest.param(ILSLEVERANDOER_STG_SQL, id="stg_dim_ILSLeverandoer"),
    ],
)
def test_every_se_nr_clean_code_is_person_guarded(sql):
    # GUARDED contains BARE, so equal counts mean no bare occurrence is left.
    assert sql.count(BARE) >= 1
    assert sql.count(BARE) == sql.count(GUARDED)


def test_pre_stg_vareaftaler_uses_the_guarded_clean_code():
    source = (ROOT / "DSA/SQL/staging/pre_stg_Vareaftaler_ils.py").read_text(encoding="utf-8")
    assert "{ILS_KREDITOR_CC} AS clean_code_Kreditor" in source


# The se_nr formula's distinctive tail, as the chains used to inline it.
_INLINE_SE_NR_FORMULA = re.compile(
    r"'CVR_'\s*\+\s*CASE WHEN REPLACE\(LTRIM\(RTRIM\(CAST\(\w+\.se_nr", re.IGNORECASE
)
# Modules allowed to call the bare helper: its definition, and call sites that
# wrap it in sql_unless_ils_person themselves (cvr_nr / lnr candidates).
_BARE_HELPER_ALLOWED = {
    "utils/sql/tax_id_sql.py",
    "DSA/SQL/common/ils_leverandoer_clean_code.py",
    "DSA/SQL/staging/stg_dim_ILSLeverandoer.py",
}


def _python_sources():
    for top in ("DSA", "utils"):
        yield from (ROOT / top).rglob("*.py")


def test_no_chain_inlines_the_se_nr_clean_code():
    offenders = [
        str(p.relative_to(ROOT))
        for p in _python_sources()
        if _INLINE_SE_NR_FORMULA.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == [], f"use sql_ils_kreditor_cleancode instead: {offenders}"


def test_bare_se_nr_helper_is_only_used_where_guarded():
    offenders = [
        str(p.relative_to(ROOT))
        for p in _python_sources()
        if "sql_cvr_cleancode_from_se_nr(" in p.read_text(encoding="utf-8")
        and str(p.relative_to(ROOT)) not in _BARE_HELPER_ALLOWED
    ]
    assert offenders == [], f"use sql_ils_kreditor_cleancode instead: {offenders}"
