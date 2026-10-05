# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Wiring/semantic contracts for the delta-driven bridge_LeverandoervareILSVare.

Mark-then-merge correctness (spec §5): soft-delete scope comes from the
explicit delta-entity table, mark and merge run in ONE transaction, and the
seed row is exempt. A NOT MATCHED BY SOURCE delete would wipe everything
outside the delta — it must not exist.
"""

import importlib
import inspect
import re

import pytest

pytestmark = pytest.mark.unit


def _src() -> str:
    return inspect.getsource(
        importlib.import_module("DSA.SQL.bridge.bridge_LeverandoervareILSVare")
    )


def test_deps_and_group():
    src = _src()
    for dep in (
        'AssetKey(["staging_infra_erp"])',
        'AssetKey(["erp", "stg_bridge_Leverandoervare_ILSVare"])',
        'AssetKey(["map_Leverandoervare"])',
        'AssetKey(["map_ILSVare"])',
    ):
        assert dep in src, f"missing dep: {dep}"
    assert 'AssetKey(["dim_Leverandoervare"])' not in src, "er_primaer is gone — no dim deps"
    assert 'AssetKey(["dim_ILSVare"])' not in src
    assert 'group_name="erp_dimension"' in src
    assert re.search(r'start_dfm_run\(\s*"bridge",\s*"LeverandoervareILSVare"', src)


def test_scope_comes_from_delta_entity_table():
    src = _src()
    assert "[stg].[bridge_Leverandoervare_ILSVare_delta_entiteter]" in src
    assert (
        "NOT MATCHED BY SOURCE" not in src
    ), "full-table delete branch would wipe pairs outside the delta scope"


def test_mark_then_merge_soft_delete():
    src = _src()
    assert re.search(r"UPDATE\s+\w*\s*SET[\s\S]*dw_is_deleted\s*=\s*1", src), "mark step"
    assert re.search(r"dw_is_deleted\s*=\s*0", src), "merge must resurrect present pairs"
    assert re.search(
        r"dw_ek_Leverandoervare\s*=\s*-1\s+AND\s+\w+\.?dw_ek_ILSVare\s*=\s*-1", src
    ), "seed row must be exempt from the mark step"


def test_owns_checkpoint():
    src = _src()
    assert re.search(r'set_checkpoint\(\s*"bridge",\s*"LeverandoervareILSVare"', src)
    assert "should_advance_checkpoint" in src
