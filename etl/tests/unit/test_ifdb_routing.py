# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for IFDB aktivitetstype routing resolver.

Tests cover the routing contract:
  - Active route resolves correctly.
  - Only positive-list routes are used.
  - active_from / active_to date ranges are respected.
  - Missing route fails hard in production mode.
  - Overlapping active routes fail.
  - Review routes are rejected in production, allowed in audit mode.
  - YAML seed loading and validation.
  - Convenience function matches public contract.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from textwrap import dedent

import pytest
from DSA.FHIR.utils.ifdb.routing import (
    MultipleActiveRoutesError,
    NoActiveRouteError,
    RouteStatus,
    RoutingResolver,
    RoutingResult,
    RoutingRule,
    RoutingTableMissingError,
    TargetResource,
    load_routing_rules,
    load_routing_rules_from_db,
    load_routing_rules_from_yaml,
    resolve_ifdb_target_resource,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _rule(
    kode: str = "Maalinger.Blodtryk",
    target: str = "Observation",
    category: str | None = "vital-signs",
    status: str = "active",
    active_from: str = "2026-01-01",
    active_to: str | None = None,
    rationale: str | None = None,
    decided_by: str | None = None,
    decided_at: str | None = None,
) -> RoutingRule:
    """Helper to build a RoutingRule with sensible defaults."""
    return RoutingRule(
        aktivitetstype_kode=kode,
        target_resource=TargetResource(target),
        observation_category=category,
        route_status=RouteStatus(status),
        rationale=rationale,
        decided_by=decided_by,
        decided_at=date.fromisoformat(decided_at) if decided_at else None,
        active_from=date.fromisoformat(active_from),
        active_to=date.fromisoformat(active_to) if active_to else None,
    )


@pytest.fixture
def observation_rule() -> RoutingRule:
    return _rule()


@pytest.fixture
def qr_rule() -> RoutingRule:
    return _rule(
        kode="SFI.RM_Generelle",
        target="QuestionnaireResponse",
        category=None,
        status="active",
    )


@pytest.fixture
def review_rule() -> RoutingRule:
    return _rule(kode="SFI.NewForm", status="review")


@pytest.fixture
def inactive_rule() -> RoutingRule:
    return _rule(kode="Maalinger.Blodtryk", status="inactive", active_to="2025-12-31")


@pytest.fixture
def standard_rules(observation_rule: RoutingRule, qr_rule: RoutingRule) -> list[RoutingRule]:
    return [observation_rule, qr_rule]


# ---------------------------------------------------------------------------
# Test: Active route resolves correctly
# ---------------------------------------------------------------------------


class TestActiveRouteResolution:
    def test_observation_route(self, standard_rules: list[RoutingRule]) -> None:
        resolver = RoutingResolver(rules=standard_rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.OBSERVATION
        assert result.observation_category == "vital-signs"

    def test_questionnaire_response_route(self, standard_rules: list[RoutingRule]) -> None:
        resolver = RoutingResolver(rules=standard_rules)
        result = resolver.resolve("SFI.RM_Generelle", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
        assert result.observation_category is None

    def test_returns_routing_result(self, standard_rules: list[RoutingRule]) -> None:
        resolver = RoutingResolver(rules=standard_rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))
        assert isinstance(result, RoutingResult)
        assert isinstance(result.rule, RoutingRule)
        assert result.rule.aktivitetstype_kode == "Maalinger.Blodtryk"

    def test_datetime_coerced_to_date(self, standard_rules: list[RoutingRule]) -> None:
        resolver = RoutingResolver(rules=standard_rules)
        result = resolver.resolve(
            "Maalinger.Blodtryk",
            occurred_at=datetime(2026, 6, 1, 14, 30, 0),
        )
        assert result.target_resource == TargetResource.OBSERVATION


# ---------------------------------------------------------------------------
# Test: Only positive list routes
# ---------------------------------------------------------------------------


class TestPositiveListOnly:
    def test_unknown_code_fails(self, standard_rules: list[RoutingRule]) -> None:
        resolver = RoutingResolver(rules=standard_rules)
        with pytest.raises(NoActiveRouteError) as exc_info:
            resolver.resolve("Unknown.code", occurred_at=date(2026, 6, 1))
        assert "Unknown.code" in str(exc_info.value)

    def test_empty_rules_fails(self) -> None:
        resolver = RoutingResolver(rules=[])
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))

    def test_no_fallback_for_missing_route(self) -> None:
        """There must be no hidden fallback that guesses Observation or QR."""
        rules = [_rule(kode="Maalinger.Blodtryk")]
        resolver = RoutingResolver(rules=rules)
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("SFI.Ernaering", occurred_at=date(2026, 6, 1))


# ---------------------------------------------------------------------------
# Test: active_from / active_to date ranges
# ---------------------------------------------------------------------------


class TestDateRanges:
    def test_before_active_from_fails(self) -> None:
        rules = [_rule(active_from="2026-06-01")]
        resolver = RoutingResolver(rules=rules)
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 5, 31))

    def test_on_active_from_resolves(self) -> None:
        rules = [_rule(active_from="2026-06-01")]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.OBSERVATION

    def test_on_active_to_resolves(self) -> None:
        rules = [_rule(active_from="2026-01-01", active_to="2026-06-30")]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 30))
        assert result.target_resource == TargetResource.OBSERVATION

    def test_after_active_to_fails(self) -> None:
        rules = [_rule(active_from="2026-01-01", active_to="2026-06-30")]
        resolver = RoutingResolver(rules=rules)
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 7, 1))

    def test_open_ended_route(self) -> None:
        rules = [_rule(active_from="2026-01-01", active_to=None)]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2030, 12, 31))
        assert result.target_resource == TargetResource.OBSERVATION

    def test_temporal_succession(self) -> None:
        """When one route expires and another starts, the right one is picked."""
        rules = [
            _rule(
                active_from="2026-01-01",
                active_to="2026-06-30",
                target="Observation",
            ),
            _rule(
                active_from="2026-07-01",
                active_to=None,
                target="QuestionnaireResponse",
                category=None,
            ),
        ]
        resolver = RoutingResolver(rules=rules)

        result_before = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 15))
        assert result_before.target_resource == TargetResource.OBSERVATION

        result_after = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 7, 15))
        assert result_after.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE


# ---------------------------------------------------------------------------
# Test: Overlapping active routes fail
# ---------------------------------------------------------------------------


class TestOverlappingRoutes:
    def test_two_active_routes_same_date_fails(self) -> None:
        rules = [
            _rule(active_from="2026-01-01", target="Observation"),
            _rule(active_from="2026-01-01", target="QuestionnaireResponse", category=None),
        ]
        resolver = RoutingResolver(rules=rules)
        with pytest.raises(MultipleActiveRoutesError) as exc_info:
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))
        assert exc_info.value.count == 2

    def test_overlapping_date_ranges_fail(self) -> None:
        rules = [
            _rule(active_from="2026-01-01", active_to="2026-06-30"),
            _rule(active_from="2026-03-01", active_to="2026-09-30"),
        ]
        resolver = RoutingResolver(rules=rules)
        with pytest.raises(MultipleActiveRoutesError):
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 5, 1))


# ---------------------------------------------------------------------------
# Test: Review routes
# ---------------------------------------------------------------------------


class TestReviewRoutes:
    def test_review_route_never_resolves(self, review_rule: RoutingRule) -> None:
        """Review routes are curation metadata and never resolve operationally."""
        resolver = RoutingResolver(rules=[review_rule])
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("SFI.NewForm", occurred_at=date(2026, 6, 1))

    def test_active_preferred_over_review(self) -> None:
        """If both active and review routes exist, active wins."""
        rules = [
            _rule(kode="X", status="active", target="Observation"),
            _rule(kode="X", status="review", target="QuestionnaireResponse", category=None),
        ]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("X", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.OBSERVATION

    def test_inactive_route_not_matched(self, inactive_rule: RoutingRule) -> None:
        resolver = RoutingResolver(rules=[inactive_rule])
        with pytest.raises(NoActiveRouteError):
            resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2025, 6, 1))


# ---------------------------------------------------------------------------
# Test: YAML seed loading
# ---------------------------------------------------------------------------


class TestYamlSeedLoading:
    def test_load_from_repo_seed(self) -> None:
        """The standard seed file should load without errors."""
        rules = load_routing_rules_from_yaml()
        assert len(rules) > 0
        for rule in rules:
            assert isinstance(rule, RoutingRule)
            assert rule.aktivitetstype_kode
            assert isinstance(rule.target_resource, TargetResource)
            assert isinstance(rule.route_status, RouteStatus)
            assert isinstance(rule.active_from, date)

    def test_load_missing_file_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            load_routing_rules_from_yaml(tmp_path / "nonexistent.yml")

    def test_load_invalid_yaml_raises(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.yml"
        bad_file.write_text("routes:\n  - aktivitetstype_kode: X\n")
        with pytest.raises((ValueError, KeyError)):
            load_routing_rules_from_yaml(bad_file)

    def test_load_custom_yaml(self, tmp_path: Path) -> None:
        seed = tmp_path / "test_routes.yml"
        seed.write_text(
            dedent(
                """\
                routes:
                  - aktivitetstype_kode: "Test.Route"
                    target_resource: "Observation"
                    observation_category: "exam"
                    route_status: "active"
                    rationale: "test"
                    decided_by: "test"
                    decided_at: "2026-01-01"
                    active_from: "2026-01-01"
                    active_to: null
            """
            )
        )
        rules = load_routing_rules_from_yaml(seed)
        assert len(rules) == 1
        assert rules[0].aktivitetstype_kode == "Test.Route"
        assert rules[0].target_resource == TargetResource.OBSERVATION
        assert rules[0].route_status == RouteStatus.ACTIVE

    def test_invalid_target_resource_raises(self, tmp_path: Path) -> None:
        seed = tmp_path / "bad_target.yml"
        seed.write_text(
            dedent(
                """\
                routes:
                  - aktivitetstype_kode: "Bad"
                    target_resource: "Bundle"
                    route_status: "active"
                    active_from: "2026-01-01"
            """
            )
        )
        with pytest.raises(ValueError, match="target_resource"):
            load_routing_rules_from_yaml(seed)

    def test_invalid_route_status_raises(self, tmp_path: Path) -> None:
        seed = tmp_path / "bad_status.yml"
        seed.write_text(
            dedent(
                """\
                routes:
                  - aktivitetstype_kode: "Bad"
                    target_resource: "Observation"
                    route_status: "deleted"
                    active_from: "2026-01-01"
            """
            )
        )
        with pytest.raises(ValueError, match="route_status"):
            load_routing_rules_from_yaml(seed)


# ---------------------------------------------------------------------------
# Test: Convenience function (public contract)
# ---------------------------------------------------------------------------


class TestResolveIfdbTargetResource:
    def test_returns_observation_string(self) -> None:
        rules = [_rule(kode="X", target="Observation")]
        result = resolve_ifdb_target_resource("X", occurred_at=date(2026, 6, 1), rules=rules)
        assert result == "Observation"

    def test_returns_questionnaire_response_string(self) -> None:
        rules = [_rule(kode="Y", target="QuestionnaireResponse", category=None)]
        result = resolve_ifdb_target_resource("Y", occurred_at=date(2026, 6, 1), rules=rules)
        assert result == "QuestionnaireResponse"

    def test_accepts_modelleringsnavn_for_forward_compat(self) -> None:
        rules = [_rule(kode="X")]
        result = resolve_ifdb_target_resource(
            "X",
            modelleringsnavn="SomeModel",
            modelleringsversion="1.0",
            occurred_at=date(2026, 6, 1),
            rules=rules,
        )
        assert result == "Observation"

    def test_missing_route_raises(self) -> None:
        rules = [_rule(kode="X")]
        with pytest.raises(NoActiveRouteError):
            resolve_ifdb_target_resource("Unknown", occurred_at=date(2026, 6, 1), rules=rules)

    def test_review_route_never_resolves(self) -> None:
        rules = [_rule(kode="X", status="review")]
        with pytest.raises(NoActiveRouteError):
            resolve_ifdb_target_resource(
                "X",
                occurred_at=date(2026, 6, 1),
                rules=rules,
            )


# ---------------------------------------------------------------------------
# Test: Enum values
# ---------------------------------------------------------------------------


class TestEnums:
    def test_target_resource_values(self) -> None:
        assert TargetResource.OBSERVATION.value == "Observation"
        assert TargetResource.QUESTIONNAIRE_RESPONSE.value == "QuestionnaireResponse"

    def test_route_status_values(self) -> None:
        assert RouteStatus.ACTIVE.value == "active"
        assert RouteStatus.INACTIVE.value == "inactive"
        assert RouteStatus.REVIEW.value == "review"


# ---------------------------------------------------------------------------
# Test: Unrouted codes helper
# ---------------------------------------------------------------------------


class TestUnroutedCodes:
    def test_all_codes_routed(self) -> None:
        rules = [
            _rule(kode="A"),
            _rule(kode="B", target="QuestionnaireResponse", category=None),
        ]
        resolver = RoutingResolver(rules=rules)
        unrouted = resolver.get_unrouted_codes(["A", "B"], ref_date=date(2026, 6, 1))
        assert unrouted == []

    def test_some_codes_unrouted(self) -> None:
        rules = [_rule(kode="A")]
        resolver = RoutingResolver(rules=rules)
        unrouted = resolver.get_unrouted_codes(["A", "B", "C"], ref_date=date(2026, 6, 1))
        assert unrouted == ["B", "C"]

    def test_inactive_codes_are_unrouted(self) -> None:
        rules = [_rule(kode="A", status="inactive")]
        resolver = RoutingResolver(rules=rules)
        unrouted = resolver.get_unrouted_codes(["A"], ref_date=date(2026, 6, 1))
        assert unrouted == ["A"]

    def test_review_codes_are_routed(self) -> None:
        """Review routes count as 'covered' for audit purposes."""
        rules = [_rule(kode="A", status="review")]
        resolver = RoutingResolver(rules=rules)
        unrouted = resolver.get_unrouted_codes(["A"], ref_date=date(2026, 6, 1))
        assert unrouted == []


# ---------------------------------------------------------------------------
# Test: Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    def test_whitespace_in_kode_trimmed(self) -> None:
        rules = [_rule(kode="Maalinger.Blodtryk")]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("  Maalinger.Blodtryk  ", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.OBSERVATION

    def test_occurred_at_defaults_to_today(self) -> None:
        rules = [_rule(kode="X", active_from="2020-01-01")]
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("X")
        assert result.target_resource == TargetResource.OBSERVATION

    def test_get_all_active_routes(self) -> None:
        rules = [
            _rule(kode="A"),
            _rule(kode="B", target="QuestionnaireResponse", category=None),
            _rule(kode="C", status="review"),
            _rule(kode="D", status="inactive"),
        ]
        resolver = RoutingResolver(rules=rules)
        active = resolver.get_all_active_routes(ref_date=date(2026, 6, 1))
        kodes = {r.aktivitetstype_kode for r in active}
        assert kodes == {"A", "B"}


# ---------------------------------------------------------------------------
# load_routing_rules_from_db — soft-miss fallback when table is absent
# ---------------------------------------------------------------------------


class _FakeCursorTableMissing:
    """Mimics mssql_python's ProgrammingError for missing routing table."""

    def execute(self, sql: str) -> None:
        raise RuntimeError(
            "[Microsoft][SQL Server]Invalid object name 'meta.IFDBAktivitetstypeRouting'."
        )


class _FakeCursorBoom:
    """Mimics an unrelated DB error (auth, connection, etc.) that MUST re-raise."""

    def execute(self, sql: str) -> None:
        raise RuntimeError("Connection terminated unexpectedly.")


def test_load_routing_rules_from_db_raises_when_table_missing() -> None:
    """A missing routing table is a hard failure on the runtime path.

    The runtime must not silently substitute the repo YAML seed — route
    decisions have to be reproducible from meta.IFDBAktivitetstypeRouting.
    """
    with pytest.raises(RoutingTableMissingError):
        load_routing_rules_from_db(_FakeCursorTableMissing())


def test_load_routing_rules_from_db_reraises_unrelated_errors() -> None:
    """Non-table-missing DB errors must NOT be swallowed."""
    with pytest.raises(RuntimeError, match="Connection terminated"):
        load_routing_rules_from_db(_FakeCursorBoom())


def test_load_routing_rules_with_cursor_does_not_fall_back_to_yaml(tmp_path) -> None:
    """Runtime path: a missing table raises instead of loading the YAML seed."""
    yaml_file = tmp_path / "seed.yml"
    yaml_file.write_text(
        dedent(
            """
            routes:
              - aktivitetstype_kode: "Bootstrap.Test"
                target_resource: "Observation"
                observation_category: "vital-signs"
                route_status: "active"
                rationale: "Bootstrap test"
                decided_by: "test"
                decided_at: "2026-05-13"
                active_from: "2026-05-13"
                active_to: null
            """
        ).strip()
    )
    with pytest.raises(RoutingTableMissingError):
        load_routing_rules(cursor=_FakeCursorTableMissing(), yaml_path=yaml_file)


def test_load_routing_rules_without_cursor_uses_yaml(tmp_path) -> None:
    """Offline path (no cursor): YAML seed is the source, for tests/tooling."""
    yaml_file = tmp_path / "seed.yml"
    yaml_file.write_text(
        dedent(
            """
            routes:
              - aktivitetstype_kode: "Bootstrap.Test"
                target_resource: "Observation"
                observation_category: "vital-signs"
                route_status: "active"
                rationale: "Bootstrap test"
                decided_by: "test"
                decided_at: "2026-05-13"
                active_from: "2026-05-13"
                active_to: null
            """
        ).strip()
    )
    rules = load_routing_rules(yaml_path=yaml_file)
    assert len(rules) == 1
    assert rules[0].aktivitetstype_kode == "Bootstrap.Test"
