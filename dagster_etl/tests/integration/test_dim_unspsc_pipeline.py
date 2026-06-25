# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: dim.UNSPSC end-to-end pipeline.

Gated by USE_REAL_DB_FOR_TESTS=true and TEST_ENV=dev. Seeds controlled fixtures
into arc.unspsc_ils, arc.vare_ils, arc.unspsc_kmd_uddata, runs stg → map → dim
and asserts the resulting dim.UNSPSC contents.

The body of each test is a `pytest.skip` placeholder — the project's
real_resources fixture pattern varies; implement each test against
tests/utils/materialize_helpers.py::materialize_assets_with_dependencies
and the real_resources fixture pattern from neighbouring
tests/integration/*.py files.
"""

import os

import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.erp,
    pytest.mark.real_resources,
    pytest.mark.skipif(
        os.environ.get("USE_REAL_DB_FOR_TESTS") != "true"
        or "dev" not in (os.environ.get("TEST_ENV") or ""),
        reason="Requires USE_REAL_DB_FOR_TESTS=true and TEST_ENV containing 'dev'",
    ),
]


def test_ils_only_code_lands_with_ils_flag():
    """A code present only in ILS gets Source_ILS=1, Source_KMD=0, ILS descriptions populated."""
    pytest.skip("Implement against your real-resources fixture set")


def test_kmd_only_code_lands_with_kmd_flag_and_levels():
    """A code present only in KMD (DW_IsCurrent=1) gets Source_KMD=1 + full level hierarchy."""
    pytest.skip("Implement against your real-resources fixture set")


def test_overlap_code_carries_both_flags_and_descriptions():
    """A code present in both ILS and KMD has both flags 1 and both description sets populated."""
    pytest.skip("Implement against your real-resources fixture set")


def test_unspsc2_dot_zero_is_stripped():
    """vare.unspsc2 = '42271507.0' lands as '42271507' in dim.UNSPSC."""
    pytest.skip("Implement against your real-resources fixture set")


def test_idempotent_no_op_on_second_run():
    """Running the pipeline twice with no arc changes results in zero merged rows on the second run."""
    pytest.skip("Implement against your real-resources fixture set")


def test_source_flags_are_sticky():
    """Flipping a code from ILS-only to also-KMD on a 2nd run advances DW_Source_KMD without touching DW_Source_ILS."""
    pytest.skip("Implement against your real-resources fixture set")


def test_no_row_has_both_flags_zero():
    """Invariant: every dim.UNSPSC row (excluding UNKNOWN -1) has Source_ILS=1 OR Source_KMD=1."""
    pytest.skip("Implement against your real-resources fixture set")
