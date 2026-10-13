# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for scripts.validate_erp_grain's fct_stg_coverage check.

Bug fund: COUNT DISTINCT takes exactly ONE expression in T-SQL, and a
WHERE-clause needs one IS NOT NULL per column — the previous SQL generation
built a bare comma-joined column list for BOTH ("COUNT(DISTINCT [a], [b])"
and "WHERE [a], [b] IS NOT NULL"), which is invalid T-SQL and would fail at
execute-time for any multi-column staging_grain_columns spec (e.g.
AftaleVarePris's ["bk_prrowid", "clean_code_AftaleVarePris"]). The fix
CONCAT's multi-column grains for the DISTINCT count and AND's together one
IS NOT NULL per column.

Also verifies the fact-side count excludes soft-deleted rows
(dw_is_deleted = 0) when — and only when — the fact table actually has that
column: post-re-key (e.g. AftaleVarePris's bk_prrowid kildepraefiks-aendring,
spec 2026-08-31), dw_is_deleted=1 residue under the OLD key format would
otherwise guarantee a mismatch against staging, which only carries the NEW
format.
"""

from __future__ import annotations

import pytest
from scripts.validate_erp_grain import FCT_SPECS, _run_fct_stg_coverage

pytestmark = pytest.mark.unit


class _FakeCursor:
    """Replays canned fetchone() results in call order; records executed SQL."""

    def __init__(self, fetchone_results: list):
        self._results = list(fetchone_results)
        self.executes: list[str] = []

    def execute(self, sql, params=None):  # noqa: ANN001
        self.executes.append(sql)

    def fetchone(self):
        return self._results.pop(0)


def _aftalevarepris_spec():
    return next(s for s in FCT_SPECS if s.entity == "AftaleVarePris")


def test_multi_column_grain_uses_concat_for_distinct_count():
    spec = _aftalevarepris_spec()
    assert spec.staging_grain_columns == ["bk_prrowid", "clean_code_AftaleVarePris"]

    cursor = _FakeCursor(
        [
            (1,),  # fct table exists (OBJECT_ID)
            (1,),  # staging table exists (OBJECT_ID)
            (42,),  # stg_distinct
            (1,),  # dw_is_deleted column exists (COL_LENGTH)
            (40,),  # fct_count
        ]
    )
    _run_fct_stg_coverage(cursor, spec, max_samples=5)

    distinct_sql = next(sql for sql in cursor.executes if "COUNT(DISTINCT" in sql)
    assert "COUNT(DISTINCT [bk_prrowid], [clean_code_AftaleVarePris])" not in distinct_sql
    assert (
        "COUNT(DISTINCT CONCAT([bk_prrowid], '|', [clean_code_AftaleVarePris]))" in distinct_sql
    ), "multi-column grain must be CONCAT'ed into a single DISTINCT expression"


def test_multi_column_grain_where_clause_ands_is_not_null_per_column():
    spec = _aftalevarepris_spec()
    cursor = _FakeCursor(
        [
            (1,),
            (1,),
            (42,),
            (1,),
            (40,),
        ]
    )
    _run_fct_stg_coverage(cursor, spec, max_samples=5)

    distinct_sql = next(sql for sql in cursor.executes if "COUNT(DISTINCT" in sql)
    assert (
        "[bk_prrowid], [clean_code_AftaleVarePris] IS NOT NULL" not in distinct_sql
    ), "a bare comma-joined column list before IS NOT NULL is invalid T-SQL"
    assert "[bk_prrowid] IS NOT NULL AND [clean_code_AftaleVarePris] IS NOT NULL" in distinct_sql


def test_fct_side_excludes_soft_deleted_rows_when_column_exists():
    spec = _aftalevarepris_spec()
    cursor = _FakeCursor(
        [
            (1,),
            (1,),
            (42,),
            (1,),  # COL_LENGTH: dw_is_deleted exists
            (40,),
        ]
    )
    _run_fct_stg_coverage(cursor, spec, max_samples=5)

    fct_count_sql = next(
        sql for sql in cursor.executes if sql.strip().startswith("SELECT COUNT(*) FROM [fct]")
    )
    assert "[dw_is_deleted] = 0" in fct_count_sql


def test_fct_side_omits_soft_delete_filter_when_column_absent():
    """A fact table without a dw_is_deleted column (e.g. Faktura, Postering)
    must not get the filter — it would raise an invalid-column SQL error."""
    spec = _aftalevarepris_spec()
    cursor = _FakeCursor(
        [
            (1,),
            (1,),
            (42,),
            (None,),  # COL_LENGTH: no dw_is_deleted column on this table
            (42,),
        ]
    )
    _run_fct_stg_coverage(cursor, spec, max_samples=5)

    fct_count_sql = next(
        sql for sql in cursor.executes if sql.strip().startswith("SELECT COUNT(*) FROM [fct]")
    )
    assert "dw_is_deleted" not in fct_count_sql
