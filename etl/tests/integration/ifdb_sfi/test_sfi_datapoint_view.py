# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: fhir.SFI_datapoint union view.

The SQL-on-FHIR projections, DDL and union view share one SFI datapoint contract.
The deterministic expected matrix lives in fixtures/ifdb_sfi/expected/
sfi_datapoint_rows.csv.

This module verifies:
- Row count per ifdb_aktivitet_id matches expectation across Observation + QR.
- Query "give me element_code regardless of resource" returns Observation- and
  QR-rows in the same shape.
- Observations from the same XML can be grouped back via ifdb_aktivitet_id.
"""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


def test_expected_datapoint_rows_match_per_case_counts(
    sfi_expected_datapoint_rows, sfi_expected_cases
):
    """Pure fixture-contract: rowcount in sfi_datapoint_rows.csv == per-case JSON expectation."""
    actual_counts: dict[str, int] = {}
    for row in sfi_expected_datapoint_rows:
        actual_counts[row["case_id"]] = actual_counts.get(row["case_id"], 0) + 1
    for case_id, case in sfi_expected_cases.items():
        if case_id == "case_06_fejl_datatype_mismatch":
            assert (
                actual_counts.get(case_id, 0) == 0
            ), f"{case_id}: failure case must not contribute to SFI_datapoint"
            continue
        want = case["expected_sfi_datapoint_rows"]
        got = actual_counts.get(case_id, 0)
        assert got == want, f"{case_id}: expected {want} SFI_datapoint rows, fixture has {got}"


def test_sfi_datapoint_rows_have_required_shape(sfi_expected_datapoint_rows):
    required = {
        "source_resource",
        "resource_id",
        "patient_id",
        "encounter_id",
        "occurred_at",
        "element_system",
        "element_code",
        "value_type",
        "ifdb_aktivitet_id",
    }
    for row in sfi_expected_datapoint_rows:
        missing = required - set(row.keys())
        assert not missing, f"SFI_datapoint row missing columns: {missing}"
        assert row["source_resource"] in {
            "Observation",
            "QuestionnaireResponse",
        }, f"Unexpected source_resource: {row['source_resource']}"
        assert (
            row["element_system"] == "urn:dk:ifdb:sfi"
        ), "element_system must be urn:dk:ifdb:sfi in SFI_datapoint"


def test_observations_from_same_xml_can_be_regrouped(sfi_expected_datapoint_rows):
    """All Observation rows from case_01 must share the same ifdb_aktivitet_id."""
    case01_obs = [
        row
        for row in sfi_expected_datapoint_rows
        if row["case_id"] == "case_01_procedure_blodtryk"
        and row["source_resource"] == "Observation"
    ]
    assert len(case01_obs) == 2
    grouped = {row["ifdb_aktivitet_id"] for row in case01_obs}
    assert len(grouped) == 1, "Same XML must regroup via a single ifdb_aktivitet_id"


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.SqlOnFhir"),
    reason="SQL-on-FHIR projections require resource-backed materialisation.",
)
def test_sfi_datapoint_view_returns_observation_and_qr_in_same_shape(sfi_expected_datapoint_rows):
    pytest.skip(
        "Requires resource-backed coverage: post Observation + QR fixture data through Aidbox + SQL-on-FHIR, "
        "then SELECT * FROM fhir.SFI_datapoint and diff against "
        "sfi_expected_datapoint_rows by (case_id, element_code)."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.SqlOnFhir"),
    reason="SQL-on-FHIR projections require resource-backed materialisation.",
)
def test_sfi_datapoint_query_does_not_require_resource_knowledge():
    """Consumers can query element_code without knowing Observation vs QR."""
    pytest.skip(
        "Requires resource-backed coverage: execute the consumer query and assert it "
        "returns rows for BT_SYS/BT_DIA (Observation) and edGCSTotalScore (QR-item) "
        "in the same column shape."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.SqlOnFhir"),
    reason="SQL-on-FHIR projections require resource-backed materialisation.",
)
def test_pipeline_is_idempotent_across_repeated_materialisation():
    pytest.skip(
        "Requires resource-backed coverage: run the full materialisation twice; assert fhir.SFI_datapoint "
        "row count and identifier set are unchanged."
    )
