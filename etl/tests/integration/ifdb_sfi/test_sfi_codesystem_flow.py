# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: SFI CodeSystem flow.

Validates that the SFI codesystem.* tables and asset produce
concepts/properties/hierarchy that match the deterministic SFI catalogue
expected from the XML fixtures.

Real-resource assertions are skipped until the SFI CodeSystem asset
materialisation is covered. Until then the structural tests still verify the fixture contract
(concepts CSV is well-formed and covers every element_code used in the
SFI_datapoint expectation).
"""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


def test_sfi_expected_concepts_cover_all_datapoint_codes(
    sfi_expected_concepts, sfi_expected_datapoint_rows
):
    """Every element_code emitted by the test cases must have a SFI concept row.

    Pure fixture-contract test; runs without real resources.
    """
    concept_codes = {row["code"] for row in sfi_expected_concepts}
    datapoint_codes = {
        row["element_code"] for row in sfi_expected_datapoint_rows if row.get("element_code")
    }
    missing = datapoint_codes - concept_codes
    assert (
        not missing
    ), f"SFI concept catalogue is missing codes used by expected datapoints: {missing}"


def test_sfi_expected_concepts_have_required_properties(sfi_expected_concepts):
    """Each concept row must carry the minimum SFI property set."""
    required = {
        "code",
        "display",
        "datatype",
        "sfi_path",
        "source_element_id",
        "source_element_name",
        "source_model_name",
        "source_model_version",
        "status",
    }
    for row in sfi_expected_concepts:
        missing = required - set(row.keys())
        assert not missing, f"Concept row missing properties: {missing} in {row}"
        assert row["code"], "Concept rows must have a non-empty code"
        assert row["status"] in {
            "active",
            "review",
            "deprecated",
        }, f"Unexpected status for {row['code']}: {row['status']}"


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.CodeSystem.sfi"),
    reason="SFI CodeSystem asset import is unavailable.",
)
def test_sfi_concepts_materialise_to_codesystem_tables(sfi_expected_concepts):
    """Materialise the SFI asset and confirm codesystem.sfi_concept gets the expected rows.

    Skeleton stub for the real materialisation check.
    """
    pytest.skip(
        "Requires resource-backed coverage: wire materialisation of DSA.CodeSystem.sfi and assert "
        "codesystem.sfi_concept matches sfi_expected_concepts."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.CodeSystem.sfi"),
    reason="SFI CodeSystem asset import is unavailable.",
)
def test_sfi_properties_carry_datatype_and_unit(sfi_expected_concepts):
    """codesystem.sfi_properties must contain datatype/unit/sfi_path/status for each concept."""
    pytest.skip(
        "Requires resource-backed coverage: join codesystem.sfi_properties to expected fixture and assert "
        "minimum property set per concept."
    )
