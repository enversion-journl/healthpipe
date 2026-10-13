# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Contract tests for the full-rebuild-driven bridge_AftaleLager applier.

Critical properties: staging is a FULL rebuild each run (Task 4, no
checkpoint), so the pre-delete sweep must cover the WHOLE table — no
scoping EXISTS, unlike the delta-scoped LeverandoervareILSVare sibling.
dw_ek_Aftale/dw_ek_Lager resolve via LEFT JOIN + COALESCE(..., -1), never
INNER JOIN + fail-fast.
"""

import importlib
import inspect
import re

import pytest

pytestmark = pytest.mark.unit


def _src() -> str:
    return inspect.getsource(importlib.import_module("DSA.SQL.bridge.bridge_AftaleLager"))


def test_deps_and_group():
    src = _src()
    for dep in (
        'AssetKey(["staging_infra_erp"])',
        'AssetKey(["erp", "stg_bridge_AftaleLager"])',
        'AssetKey(["map_Aftale"])',
        'AssetKey(["map_Lager"])',
    ):
        assert dep in src, f"missing dep: {dep}"
    assert 'group_name="erp_dimension"' in src
    assert re.search(r'start_dfm_run\(\s*"bridge",\s*"AftaleLager"', src)


def test_merge_key_is_clean_code_aftale_and_sgh_raw():
    src = _src()
    assert re.search(
        r"ON\s+tgt\.clean_code_Aftale\s*=\s*src\.clean_code_Aftale\s*"
        r"AND\s+tgt\.sgh_raw\s*=\s*src\.sgh_raw",
        src,
    ), "MERGE must key on (clean_code_Aftale, sgh_raw)"


def test_pre_delete_sweep_is_full_table_not_scoped():
    src = _src()
    sweep_match = re.search(
        r"UPDATE\s+tgt\s*\n\s*SET\s+tgt\.dw_is_deleted\s*=\s*1[\s\S]*?;",
        src,
    )
    assert sweep_match, "expected a full pre-delete sweep UPDATE"
    sweep_sql = sweep_match.group(0)
    assert "EXISTS" not in sweep_sql, "full staging => full scope, no scoping EXISTS"
    # Sweep must be placed BEFORE the MERGE in source order
    assert src.index(sweep_match.group(0)) < src.index("MERGE [bridge].[AftaleLager]")
    # Comment explaining the full-scope reasoning must be present
    assert "INGEN scoping EXISTS" in src or "ingen scoping EXISTS" in src.lower()


def test_map_lookups_use_coalesce_fallback():
    src = _src()
    assert "COALESCE(aftale_map.dw_ek_Aftale, -1)" in src
    assert "COALESCE(lager_map.dw_ek_Lager, -1)" in src


def test_lager_join_has_no_null_guard_after_sygehus_fallback():
    """Ruling 5 (2026-09-01): sygehus-only raekker falder tilbage til
    SGH_{sygehus}-konceptet, saa clean_code_Lager er aldrig NULL — den gamle
    NULL-guard paa join'et er fjernet som doed kode, og -1 betyder nu altid
    aegte map-miss (diagnostikken taeller ALLE -1, ikke kun lager-niveau)."""
    src = _src()
    assert "src.clean_code_Lager IS NOT NULL" not in src
    assert "SUM(CASE WHEN dw_ek_Lager = -1 THEN 1 ELSE 0 END)" in src


def test_no_checkpoint_advance_full_rebuild_each_run():
    src = _src()
    assert "should_advance_checkpoint" not in src
    assert "checkpoint_after=None" in src


def test_duplicate_grain_fail_fast_before_index_and_merge():
    """Mirrors fct_AftaleVarePris.py's duplicate-grain guard: a collision on
    (clean_code_Aftale, sgh_raw) in the resolve temp table must raise a clear
    ValueError BEFORE the unique clustered index is created and BEFORE the
    MERGE runs — not surface as a raw index-constraint error from the engine."""
    src = _src()
    assert (
        "SELECT COUNT(*) - COUNT(DISTINCT CONCAT(clean_code_Aftale, '|', sgh_raw))" in src
    ), "expected a duplicate-grain COUNT(*) - COUNT(DISTINCT ...) check"
    assert "raise ValueError(" in src and "Duplicate grain violation in bridge_AftaleLager" in src

    dup_check_idx = src.index(
        "SELECT COUNT(*) - COUNT(DISTINCT CONCAT(clean_code_Aftale, '|', sgh_raw))"
    )
    index_create_idx = src.index("CREATE UNIQUE CLUSTERED INDEX IX_tmp_AftaleLager_Resolved")
    merge_idx = src.index("MERGE [bridge].[AftaleLager]")

    assert (
        dup_check_idx < index_create_idx
    ), "duplicate-grain check must run before the unique index create"
    assert index_create_idx < merge_idx, "unique index create must still precede the MERGE"
