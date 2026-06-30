# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: Observation as one-resource-per-SFI-element.

Tests the Observation path contract:
- one Observation per SFI-element from the XML
- shared ifdb_aktivitet_id across all Observations from the same XML
- stable source identifier including element_nummer / SFI-code
- Procedure-output is preserved alongside (case_01)
- datatype mismatch fails at row level (case_06)
"""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


OBSERVATION_CASES = [
    "case_01_procedure_blodtryk",
    "case_02_observation_saturation",
]


def test_expected_observation_count_per_case(sfi_expected_cases):
    """Pure fixture-contract check: per-case JSON declares the expected Observation count."""
    expected_counts = {
        "case_01_procedure_blodtryk": 2,
        "case_02_observation_saturation": 1,
        "case_06_fejl_datatype_mismatch": 0,
    }
    for case_id, want in expected_counts.items():
        got = len(sfi_expected_cases[case_id].get("expected_observations") or [])
        assert got == want, f"{case_id}: expected {want} observations, fixture says {got}"


def test_observations_in_same_aktivitet_share_ifdb_aktivitet_id(sfi_expected_cases):
    case = sfi_expected_cases["case_01_procedure_blodtryk"]
    ids = {obs["ifdb_aktivitet_id"] for obs in case["expected_observations"]}
    assert len(ids) == 1, "Observations from same XML must share ifdb_aktivitet_id"


def test_observation_identifiers_include_sfi_element_index(sfi_expected_cases):
    """Stable source identifier must include element_nummer/SFI-code suffix."""
    for case_id in OBSERVATION_CASES:
        for obs in sfi_expected_cases[case_id]["expected_observations"]:
            ident = obs["identifier_value"]
            assert (
                "/sfi/" in ident
            ), f"{case_id}: Observation identifier {ident} must encode SFI element index"


@pytest.mark.real_resources
@pytest.mark.observation_test
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
@pytest.mark.parametrize("case_id", OBSERVATION_CASES)
def test_observation_builder_emits_one_per_sfi_element(
    case_id, sfi_xml_fixtures, sfi_expected_cases
):
    """AKTIVITETSRESULTAT contract: an Observation-routed XML yields one Observation per SFI-element."""
    pytest.skip(
        "Requires resource-backed coverage: build IFDB Observation from sfi_xml_fixtures[case_id] using "
        "the refactored builder and assert resource count + identifier shape."
    )


@pytest.mark.real_resources
@pytest.mark.observation_test
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
def test_procedure_output_preserved_when_aktivitetstype_is_sks_procedure(sfi_xml_fixtures):
    """case_01 has aktivitetstype_kode=SKS.a.proKABC10 -> Procedure must still emit."""
    pytest.skip(
        "Requires resource-backed coverage: assert is_sks_procedure_code is still consulted, and Procedure "
        "is emitted alongside the two SFI Observations."
    )


@pytest.mark.real_resources
@pytest.mark.observation_test
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
def test_datatype_mismatch_fails_at_row_level(sfi_xml_fixtures, sfi_expected_cases):
    """case_06: integer datatype with non-numeric Vaerdi must fail this row only."""
    expected = sfi_expected_cases["case_06_fejl_datatype_mismatch"]["expected_failure"]
    assert expected["level"] == "row"
    pytest.skip(
        "Requires resource-backed coverage: drive builder with case_06 + a healthy neighbour row in same "
        "batch; assert case_06 fails and the neighbour succeeds, log contains "
        f"fields {expected['expected_log_fields']}."
    )


@pytest.mark.real_resources
@pytest.mark.observation_test
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
def test_observation_builder_is_idempotent(sfi_xml_fixtures):
    """Same input twice must yield the same source identifier set."""
    pytest.skip(
        "Requires resource-backed coverage: build twice and assert identifier set is identical, no dupes."
    )
