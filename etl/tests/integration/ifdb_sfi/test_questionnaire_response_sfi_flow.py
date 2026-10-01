# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: Questionnaire + QuestionnaireResponse SFI flow.

Tests the Questionnaire/QuestionnaireResponse contract:
- one Questionnaire per modelnavn/modelversion, version-pinned
- one QuestionnaireResponse per AKTIVITETSRESULTAT
- Questionnaire.item.code carries SFI (urn:dk:ifdb:sfi)
- QR.item.linkId matches Questionnaire.item.linkId
- klynge/sekvens/repetition is preserved as nested QR.item structure
"""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


QR_CASES = [
    "case_03_qr_simpel_aktuelt",
    "case_04_qr_kompleks_ernaering",
    "case_05_qr_gcs_total",
]


def test_qr_cases_declare_expected_questionnaire(sfi_expected_cases):
    """Fixture contract: each QR case has expected_questionnaire and expected_questionnaire_response."""
    for case_id in QR_CASES:
        case = sfi_expected_cases[case_id]
        assert case["expected_questionnaire"], f"{case_id}: expected_questionnaire missing"
        assert case[
            "expected_questionnaire_response"
        ], f"{case_id}: expected_questionnaire_response missing"
        assert case["expected_questionnaire"][
            "version"
        ], f"{case_id}: Questionnaire must be version-pinned"


def test_qr_link_ids_unique_per_case(sfi_expected_qr_linkids):
    seen = set()
    for row in sfi_expected_qr_linkids:
        key = (row["case_id"], row["link_id"])
        assert key not in seen, f"Duplicate linkId for case {row['case_id']}: {row['link_id']}"
        seen.add(key)


def test_qr_leaf_link_ids_have_sfi_code(sfi_expected_qr_linkids):
    """Every leaf QR-item linkId in expectations must carry an SFI code (parent groups may not)."""
    for row in sfi_expected_qr_linkids:
        if row["answer_value_type"] == "group":
            continue
        assert row[
            "sfi_code"
        ], f"Leaf linkId {row['link_id']} in {row['case_id']} is missing SFI code"


def test_complex_qr_preserves_klynge_as_parent_item(sfi_expected_qr_linkids):
    """case_04 must have a parent group linkId and a child linkId underneath it."""
    case04 = [r for r in sfi_expected_qr_linkids if r["case_id"] == "case_04_qr_kompleks_ernaering"]
    parents = {r["link_id"] for r in case04 if r["answer_value_type"] == "group"}
    children_with_parents = {r["parent_link_id"] for r in case04 if r["parent_link_id"]}
    assert parents, "case_04 must expose at least one group/klynge parent linkId"
    assert (
        parents & children_with_parents
    ), "case_04 leaf linkId must reference a parent klynge linkId"


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Questionnaire.Questionnaire_IFDB"),
    reason="Questionnaire builder import is unavailable.",
)
@pytest.mark.parametrize("case_id", QR_CASES)
def test_questionnaire_builder_emits_versioned_questionnaire(case_id, sfi_xml_fixtures):
    pytest.skip(
        "Requires resource-backed coverage: build Questionnaire from sfi_xml_fixtures[case_id] and assert "
        "Questionnaire.url + version, plus Questionnaire.item.code carries SFI."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB"),
    reason="QuestionnaireResponse builder import is unavailable.",
)
@pytest.mark.parametrize("case_id", QR_CASES)
def test_qr_builder_links_to_pinned_questionnaire(case_id, sfi_xml_fixtures, sfi_expected_cases):
    expected_ref = sfi_expected_cases[case_id]["expected_questionnaire_response"][
        "questionnaire_reference"
    ]
    assert "|" in expected_ref, "Questionnaire reference must be version-pinned"
    pytest.skip(
        f"Requires resource-backed coverage: build QR and assert QR.questionnaire == {expected_ref!r}."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB"),
    reason="QuestionnaireResponse builder import is unavailable.",
)
def test_qr_klynge_repetition_preserved_for_complex_case(sfi_xml_fixtures):
    pytest.skip(
        "Requires resource-backed coverage: build QR from case_04 and assert that Klynge/Sekvens is "
        "represented as a parent QR.item with the leaf as a nested child."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB"),
    reason="QuestionnaireResponse builder import is unavailable.",
)
def test_qr_item_link_id_matches_questionnaire(sfi_xml_fixtures):
    pytest.skip(
        "Requires resource-backed coverage: for each QR-case, build both Questionnaire and QR and assert "
        "every QR.item.linkId is found in Questionnaire.item.linkId set."
    )
