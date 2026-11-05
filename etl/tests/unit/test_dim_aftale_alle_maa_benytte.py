# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for dim.Aftale.alle_maa_benytte (spec 2026-08-31).

Step 1 gate (verified manually, not re-tested here): stg_dim_Aftale_ils reads
the FULL stg.pre_vareaftaler_ils picture each run (no checkpoint filter on
staged rows), and pre_stg_Vareaftaler_ils itself is a full rebuild from arc
each run — so the flag refreshes every run without a dedicated lafthsgh
delta signal.

These tests assert: (1) the staging EXISTS-flag expression is correlated on
lnr + aftale against arc.ils_lafthsgh; (2) dim_Aftale.py carries
alle_maa_benytte through source-build, change-detection and both arms of the
Type 1 MERGE (UPDATE SET and INSERT).

dim.Aftale was converted from Type 2 to Type 1, so the three
SCD2 insert branches (changed / initial / reactivated) collapsed into a
single MERGE and the d./s. aliases became tgt./src. The assertions below were
retargeted accordingly; what they guard is unchanged — the column must not
silently fall out of the dimension.
"""

import importlib
import inspect
import re

import pytest

pytestmark = pytest.mark.unit


def _stg_src() -> str:
    return inspect.getsource(importlib.import_module("DSA.SQL.staging.stg_dim_Aftale_ils"))


def _dim_src() -> str:
    return inspect.getsource(importlib.import_module("DSA.SQL.dim.dim_Aftale"))


def test_staging_has_correlated_exists_flag_against_lafthsgh():
    src = _stg_src()
    assert re.search(
        r"CASE WHEN EXISTS \(\s*SELECT 1 FROM \[arc\]\.\[ils_lafthsgh\]",
        src,
    ), "expected a correlated EXISTS against arc.ils_lafthsgh"
    assert "THEN 0 ELSE 1 END AS alle_maa_benytte" in src
    assert "lhs.lnr" in src and "lhs.aftale" in src, "must correlate on lnr + aftale"


def test_staging_exists_correlation_is_qualified_on_outer_alias():
    """Regression guard: an unqualified lnr/aftale inside the EXISTS binds to
    the innermost scope (arc.ils_lafthsgh AS lhs itself), turning the
    predicate into a tautology (lhs.lnr = lhs.lnr) that is always true — the
    flag would then be 0 for EVERY agreement. The outer table must carry an
    alias (pva) and the correlation must reference it explicitly."""
    src = _stg_src()
    assert "FROM [stg].[pre_vareaftaler_ils] pva" in src, "outer table must be aliased pva"
    assert "= LTRIM(RTRIM(pva.lnr))" in src, "lnr correlation must be qualified with pva."
    assert "= LTRIM(RTRIM(pva.aftale))" in src, "aftale correlation must be qualified with pva."
    exists_match = re.search(
        r"CASE WHEN EXISTS \([\s\S]*?THEN 0 ELSE 1 END AS alle_maa_benytte",
        src,
    )
    assert exists_match, "expected the alle_maa_benytte EXISTS expression"
    exists_sql = exists_match.group(0)
    # Ingen ukvalificeret lnr/aftale maa forekomme i selve WHERE-korrelationen.
    assert "RTRIM(lnr))" not in exists_sql, "lnr reference inside EXISTS must be qualified"
    assert "RTRIM(aftale))" not in exists_sql, "aftale reference inside EXISTS must be qualified"


def test_staging_depends_on_arc_lafthsgh():
    src = _stg_src()
    assert 'AssetKey(["arc_ils_lafthsgh"])' in src


def test_dim_aftale_carries_column_in_source_build():
    src = _dim_src()
    assert "s.alle_maa_benytte" in src
    assert re.search(
        r"SELECT[\s\S]*?s\.alle_maa_benytte[\s\S]*?INTO #Aftale_Source", src
    ), "alle_maa_benytte must be selected into #Aftale_Source"


def test_dim_aftale_change_detection_includes_alle_maa_benytte():
    """Without this in the MERGE change guard, a flipped flag would never be
    written: WHEN MATCHED only fires when some compared attribute differs."""
    src = _dim_src()
    assert re.search(
        r"ISNULL\(tgt\.alle_maa_benytte,\s*1\)\s*<>\s*ISNULL\(src\.alle_maa_benytte,\s*1\)",
        src,
    ), "the MERGE change guard must compare alle_maa_benytte"


def test_dim_aftale_merge_carries_column_in_both_arms():
    """Type 1 has one MERGE instead of three insert branches. The column has to
    appear in the UPDATE SET (so changes propagate) and in both the INSERT
    column list and its VALUES list (so new rows get it)."""
    src = _dim_src()
    assert (
        "tgt.alle_maa_benytte = src.alle_maa_benytte" in src
    ), "UPDATE SET must assign alle_maa_benytte"
    insert_column_list_hits = len(re.findall(r"^\s*alle_maa_benytte,\s*$", src, re.MULTILINE))
    insert_value_hits = len(re.findall(r"^\s*src\.alle_maa_benytte,\s*$", src, re.MULTILINE))
    assert insert_column_list_hits == 1, "expected alle_maa_benytte in the INSERT column list"
    assert insert_value_hits == 1, "expected src.alle_maa_benytte in the INSERT VALUES list"
