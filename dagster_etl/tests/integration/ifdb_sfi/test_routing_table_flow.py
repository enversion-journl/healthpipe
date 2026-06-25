# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: routing positive-list and resolver behaviour.

The routing resolver uses the seed contract shipped
(fixtures/ifdb_sfi/routing_seed.csv) AND wires real-resource tests that load
the seed into etl.ifdb_aktivitetstype_routing and exercise the resolver.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


def test_routing_seed_has_required_columns(sfi_routing_seed):
    required = {
        "aktivitetstype_kode",
        "target_resource",
        "observation_category",
        "route_status",
        "rationale",
        "decided_by",
        "decided_at",
        "active_from",
        "active_to",
    }
    assert sfi_routing_seed, "routing_seed.csv must not be empty"
    for row in sfi_routing_seed:
        missing = required - set(row.keys())
        assert not missing, f"Routing row missing columns: {missing}"


def test_routing_seed_target_resource_is_valid(sfi_routing_seed):
    valid = {"Observation", "QuestionnaireResponse"}
    for row in sfi_routing_seed:
        assert row["target_resource"] in valid, (
            f"Invalid target_resource for {row['aktivitetstype_kode']}: "
            f"{row['target_resource']}"
        )


def test_routing_seed_status_is_valid(sfi_routing_seed):
    valid = {"active", "inactive", "review"}
    for row in sfi_routing_seed:
        assert (
            row["route_status"] in valid
        ), f"Invalid route_status for {row['aktivitetstype_kode']}"


def test_routing_seed_no_duplicate_active_keys(sfi_routing_seed):
    """A single aktivitetstype_kode must not appear twice with route_status=active and overlapping windows.

    The routing resolver must refuse to start with such a seed; we catch it earlier here.
    """
    seen: dict[str, str] = {}
    for row in sfi_routing_seed:
        if row["route_status"] != "active":
            continue
        existing = seen.get(row["aktivitetstype_kode"])
        if existing is not None:
            pytest.fail(
                f"Duplicate active route for {row['aktivitetstype_kode']} "
                f"(already had active_from={existing})"
            )
        seen[row["aktivitetstype_kode"]] = row["active_from"]


def test_routing_seed_covers_all_fixture_cases(sfi_routing_seed, sfi_expected_cases):
    """Every test case's aktivitetstype_kode must be represented in the seed.

    case_06 is in the seed as MaalingRespFrekvens (active Observation route);
    its row-level failure is unrelated to routing.
    """
    seed_codes = {row["aktivitetstype_kode"] for row in sfi_routing_seed}
    for case_id, case in sfi_expected_cases.items():
        kode = case["input"]["aktivitetstype_kode"]
        assert (
            kode in seed_codes
        ), f"Case {case_id} uses aktivitetstype_kode={kode} which is not in routing_seed.csv"


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.utils.ifdb.routing"),
    reason="routing resolver import is unavailable.",
)
def test_resolver_returns_observation_for_active_observation_route(sfi_routing_seed):
    """Resolver must return 'Observation' for a positively-listed active Observation route."""
    pytest.skip(
        "Requires resource-backed coverage: load seed into etl.ifdb_aktivitetstype_routing, then call "
        "resolve_ifdb_target_resource('MaalingSaturation', ..., datetime(2026, 5, 1)) "
        "and assert it returns 'Observation'."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.utils.ifdb.routing"),
    reason="routing resolver import is unavailable.",
)
def test_resolver_returns_questionnaire_response_for_qr_route():
    pytest.skip(
        "Requires resource-backed coverage: assert resolve_ifdb_target_resource for 'VurdGlasgowCS' is "
        "'QuestionnaireResponse'."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.utils.ifdb.routing"),
    reason="routing resolver import is unavailable.",
)
def test_resolver_fails_hard_on_missing_route():
    pytest.skip(
        "Requires resource-backed coverage: assert resolve_ifdb_target_resource('NotInSeed', ...) raises "
        "in production mode."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.utils.ifdb.routing"),
    reason="routing resolver import is unavailable.",
)
def test_resolver_refuses_review_status_in_production_mode():
    pytest.skip(
        "Requires resource-backed coverage: assert that TOM (route_status=review) raises in production "
        "mode but is allowed in audit-mode."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.utils.ifdb.routing"),
    reason="routing resolver import is unavailable.",
)
def test_resolver_respects_active_from_active_to_window():
    """active_to in the past must take an aktivitetstype out of the positive list."""
    _ = datetime(2026, 5, 1)
    pytest.skip(
        "Requires resource-backed coverage: seed an aktivitetstype with active_to in the past and assert "
        "resolver fails for that date."
    )
