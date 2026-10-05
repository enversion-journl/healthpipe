# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for infrastructure/security/verify_sor_expansion.py (DB-free)."""

from __future__ import annotations

import re

import pytest
from infrastructure.security.verify_sor_expansion import (
    _DESCENDANTS_CTE_SQL,
    _iter_candidate_codes,
)

pytestmark = pytest.mark.unit


def _dim_organisation_columns(sql: str) -> set[str]:
    """Columns referenced via the o. alias, i.e. against dim.Organisation."""
    return set(re.findall(r"\bo\.\[?(\w+)\]?", sql))


def test_descendants_cte_joins_dim_organisation_on_kode_not_code():
    """dim.Organisation uses [kode] — not [code]."""
    cols = _dim_organisation_columns(_DESCENDANTS_CTE_SQL)
    assert "kode" in cols
    assert "code" not in cols


def test_descendants_cte_keeps_sorhierarchy_child_code_column():
    """SORHierarchy, by contrast, uses child_code/parent_code."""
    assert "h.child_code" in _DESCENDANTS_CTE_SQL
    assert "h.parent_code = d.code" in _DESCENDANTS_CTE_SQL


def test_iter_candidate_codes_respects_max_subtree_and_requires_children():
    children = {"1": ["2", "3"], "2": ["4"]}
    assert list(_iter_candidate_codes(["1"], children, 3)) == [("2", "4")]


def test_iter_candidate_codes_yields_nothing_for_leaf_grants():
    assert list(_iter_candidate_codes(["9"], {"1": ["2"]}, 100)) == []


def test_iter_candidate_codes_dedups_across_overlapping_grants():
    children = {"1": ["2"], "2": ["3"]}
    cands = list(_iter_candidate_codes(["1", "2"], children, 10))
    assert cands.count(("2", "3")) == 1
