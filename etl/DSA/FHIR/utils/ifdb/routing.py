# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB Aktivitetstype routing resolver.

Determines whether an IFDB AKTIVITETSRESULTAT row is on the explicit
Observation allowlist. Missing allowlist entries are handled outside this
strict resolver by ``routing_fallback.py`` and default to QuestionnaireResponse
so production batches preserve source data while curation catches up.

Design principles:
  - Observation routing is data, not hardcoded Python logic.
  - This resolver is strict by design; dispatcher call sites wrap it with a QR
    default when data preservation is required.
  - The routing table lives in MSSQL (meta.IFDBAktivitetstypeRouting) and is
    seeded from the repo-controlled YAML file.
  - The resolver can also run purely from the YAML seed (for unit tests and
    offline validation) without requiring a database connection.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum

_logger = logging.getLogger(__name__)

# DB error fragments that mean "the routing table just doesn't exist yet".
# On the runtime (cursor) path these raise RoutingTableMissingError instead of
# silently substituting the YAML seed — route decisions must be reproducible
# from meta.IFDBAktivitetstypeRouting. Run staging_infra_etl_routing_seed.
_TABLE_MISSING_HINTS = (
    "invalid object name",
    "base table or view not found",
    "no such table",
    "does not exist",
)
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import yaml


class TargetResource(str, Enum):
    """Valid FHIR target resource types for IFDB aktivitetsresultat routing."""

    OBSERVATION = "Observation"
    QUESTIONNAIRE_RESPONSE = "QuestionnaireResponse"


class RouteStatus(str, Enum):
    """Lifecycle status of a routing rule."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    REVIEW = "review"


@dataclass(frozen=True)
class RoutingRule:
    """A single routing rule from the positive list."""

    aktivitetstype_kode: str
    target_resource: TargetResource
    observation_category: str | None
    route_status: RouteStatus
    rationale: str | None
    decided_by: str | None
    decided_at: date | None
    active_from: date
    active_to: date | None


@dataclass(frozen=True)
class RoutingResult:
    """Result of a routing resolution."""

    target_resource: TargetResource
    observation_category: str | None
    rule: RoutingRule


class RoutingError(Exception):
    """Raised when routing fails in production mode."""


class NoActiveRouteError(RoutingError):
    """No active route found for the given aktivitetstype."""

    def __init__(self, aktivitetstype_kode: str, occurred_at: date):
        self.aktivitetstype_kode = aktivitetstype_kode
        self.occurred_at = occurred_at
        super().__init__(
            f"No active route for aktivitetstype_kode={aktivitetstype_kode!r} "
            f"at {occurred_at.isoformat()}. Add an entry to the routing seed."
        )


class MultipleActiveRoutesError(RoutingError):
    """Multiple active routes found for the same aktivitetstype and date."""

    def __init__(self, aktivitetstype_kode: str, occurred_at: date, count: int):
        self.aktivitetstype_kode = aktivitetstype_kode
        self.occurred_at = occurred_at
        self.count = count
        super().__init__(
            f"Multiple ({count}) active routes for "
            f"aktivitetstype_kode={aktivitetstype_kode!r} at {occurred_at.isoformat()}. "
            f"Routing must be unambiguous."
        )


class RoutingTableMissingError(RoutingError):
    """The MSSQL routing table does not exist.

    Raised on the runtime (cursor-backed) path instead of silently falling back
    to the repo YAML seed, so production route decisions are always reproducible
    from meta.IFDBAktivitetstypeRouting.
    """

    def __init__(self, underlying: Exception):
        self.underlying = underlying
        super().__init__(
            "meta.IFDBAktivitetstypeRouting does not exist. Run the "
            "staging_infra_etl_routing / staging_infra_etl_routing_seed "
            "infrastructure assets before running the SFI runtime."
        )


_SEED_PATH = (
    Path(__file__).resolve().parents[4]
    / "infrastructure"
    / "seed"
    / "ifdb_aktivitetstype_routing.yml"
)


def _parse_date(value: Any) -> date:
    """Parse a date from string, date, or datetime."""
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise ValueError(f"Cannot parse date from {value!r} (type={type(value).__name__})")


def _parse_optional_date(value: Any) -> date | None:
    """Parse an optional date, returning None for null/empty."""
    if value is None or value == "" or value == "null":
        return None
    return _parse_date(value)


def load_routing_rules_from_yaml(path: Path | None = None) -> list[RoutingRule]:
    """Load routing rules from the repo-controlled YAML seed file.

    Args:
        path: Path to the YAML file. Defaults to the standard seed location.

    Returns:
        List of RoutingRule instances parsed from the YAML.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        ValueError: If the YAML contains invalid data.
    """
    seed_path = path or _SEED_PATH
    if not seed_path.exists():
        raise FileNotFoundError(f"Routing seed file not found: {seed_path}")

    with open(seed_path) as f:
        data = yaml.safe_load(f)

    if not data or "routes" not in data:
        raise ValueError(f"Routing seed file has no 'routes' key: {seed_path}")

    rules: list[RoutingRule] = []
    for entry in data["routes"]:
        try:
            target = TargetResource(entry["target_resource"])
        except (ValueError, KeyError) as exc:
            raise ValueError(
                f"Invalid target_resource={entry.get('target_resource')!r} "
                f"for aktivitetstype_kode={entry.get('aktivitetstype_kode')!r}"
            ) from exc

        try:
            status = RouteStatus(entry.get("route_status", "review"))
        except ValueError as exc:
            raise ValueError(
                f"Invalid route_status={entry.get('route_status')!r} "
                f"for aktivitetstype_kode={entry.get('aktivitetstype_kode')!r}"
            ) from exc

        rules.append(
            RoutingRule(
                aktivitetstype_kode=str(entry["aktivitetstype_kode"]),
                target_resource=target,
                observation_category=entry.get("observation_category"),
                route_status=status,
                rationale=entry.get("rationale"),
                decided_by=entry.get("decided_by"),
                decided_at=_parse_optional_date(entry.get("decided_at")),
                active_from=_parse_date(entry["active_from"]),
                active_to=_parse_optional_date(entry.get("active_to")),
            )
        )

    return rules


def load_routing_rules_from_db(cursor: Any) -> list[RoutingRule]:
    """Load routing rules from meta.IFDBAktivitetstypeRouting in MSSQL.

    Raises ``RoutingTableMissingError`` when the routing table does not exist,
    so the runtime never silently substitutes the repo YAML seed. An empty but
    existing table returns ``[]`` and is handled by the fallback policy
    (non-matches default to QuestionnaireResponse). Any other DB error re-raises.
    """
    try:
        cursor.execute(
            """
            SELECT
                aktivitetstype_kode,
                target_resource,
                observation_category,
                route_status,
                rationale,
                decided_by,
                decided_at,
                active_from,
                active_to
            FROM meta.IFDBAktivitetstypeRouting
            ORDER BY aktivitetstype_kode, active_from
            """
        )
    except Exception as exc:
        msg = str(exc).lower()
        if any(hint in msg for hint in _TABLE_MISSING_HINTS):
            raise RoutingTableMissingError(exc) from exc
        raise
    columns = [col[0] for col in cursor.description]
    rules: list[RoutingRule] = []
    for row in cursor.fetchall():
        row_dict = dict(zip(columns, row, strict=False))
        rules.append(
            RoutingRule(
                aktivitetstype_kode=str(row_dict["aktivitetstype_kode"]),
                target_resource=TargetResource(row_dict["target_resource"]),
                observation_category=row_dict.get("observation_category"),
                route_status=RouteStatus(row_dict["route_status"]),
                rationale=row_dict.get("rationale"),
                decided_by=row_dict.get("decided_by"),
                decided_at=_parse_optional_date(row_dict.get("decided_at")),
                active_from=_parse_date(row_dict["active_from"]),
                active_to=_parse_optional_date(row_dict.get("active_to")),
            )
        )
    return rules


def load_routing_rules(
    *,
    cursor: Any | None = None,
    yaml_path: Path | None = None,
) -> list[RoutingRule]:
    """Load routing rules.

    Runtime path (``cursor`` provided): the audited MSSQL table is the only
    source of truth. The result is returned as-is, including an empty list for
    an empty table — there is NO silent YAML fallback. A missing table raises
    ``RoutingTableMissingError`` (from ``load_routing_rules_from_db``).

    Offline path (``cursor=None``): loads from the repo YAML seed, for unit
    tests and local tooling only.
    """
    if cursor is not None:
        return load_routing_rules_from_db(cursor)
    return load_routing_rules_from_yaml(yaml_path)


def build_routing_resolver(
    *,
    cursor: Any | None = None,
    yaml_path: Path | None = None,
) -> RoutingResolver:
    """Build a resolver from the preferred routing source.

    Pass ``cursor`` for the runtime (MSSQL-only) path; omit it for the offline
    YAML path used by tests/tooling.
    """
    return RoutingResolver(
        rules=load_routing_rules(cursor=cursor, yaml_path=yaml_path),
    )


@dataclass
class RoutingResolver:
    """Resolves IFDB aktivitetstype codes to FHIR target resources.

    The resolver holds a cached set of routing rules and performs lookups
    against them. Rules are filtered by route_status and date range.

    Usage:
        rules = load_routing_rules_from_yaml()
        resolver = RoutingResolver(rules=rules)
        result = resolver.resolve("Maalinger.Blodtryk", occurred_at=date(2026, 6, 1))
        assert result.target_resource == TargetResource.OBSERVATION
    """

    rules: list[RoutingRule] = field(default_factory=list)

    _index: dict[str, list[RoutingRule]] = field(init=False, repr=False, default_factory=dict)

    def __post_init__(self) -> None:
        self._rebuild_index()

    def _rebuild_index(self) -> None:
        """Build an index from aktivitetstype_kode to sorted rules."""
        idx: dict[str, list[RoutingRule]] = {}
        for rule in self.rules:
            idx.setdefault(rule.aktivitetstype_kode, []).append(rule)
        for key in idx:
            idx[key].sort(key=lambda r: r.active_from)
        self._index = idx

    def resolve(
        self,
        aktivitetstype_kode: str,
        occurred_at: date | datetime | None = None,
    ) -> RoutingResult:
        """Resolve the FHIR target resource for a given aktivitetstype.

        Args:
            aktivitetstype_kode: The AKTIVITETTYPE_KODE from IFDB.
            occurred_at: The clinical date to match against active_from/active_to.
                         Defaults to today if not provided.

        Returns:
            RoutingResult with the resolved target and matched rule.

        Raises:
            NoActiveRouteError: No matching active route found (this includes
                the case where only review-status routes match — review routes
                are curation metadata and never resolve operationally).
            MultipleActiveRoutesError: More than one active route matches.
        """
        ref_date = _to_date(occurred_at) if occurred_at is not None else date.today()
        kode = str(aktivitetstype_kode).strip()

        candidates = self._index.get(kode)
        if not candidates:
            raise NoActiveRouteError(kode, ref_date)

        matches = [
            r
            for r in candidates
            if r.active_from <= ref_date and (r.active_to is None or r.active_to >= ref_date)
        ]

        active_matches = [r for r in matches if r.route_status == RouteStatus.ACTIVE]

        if len(active_matches) > 1:
            raise MultipleActiveRoutesError(kode, ref_date, len(active_matches))

        if len(active_matches) == 1:
            rule = active_matches[0]
            return RoutingResult(
                target_resource=rule.target_resource,
                observation_category=rule.observation_category,
                rule=rule,
            )

        # Only review/inactive routes (or none) match: never operational.
        raise NoActiveRouteError(kode, ref_date)

    def get_all_active_routes(self, ref_date: date | None = None) -> list[RoutingRule]:
        """Return all active routes valid at the given date."""
        d = ref_date or date.today()
        return [
            r
            for r in self.rules
            if r.route_status == RouteStatus.ACTIVE
            and r.active_from <= d
            and (r.active_to is None or r.active_to >= d)
        ]

    def get_unrouted_codes(
        self, all_codes: Sequence[str], ref_date: date | None = None
    ) -> list[str]:
        """Return aktivitetstype codes that have no active or review route."""
        d = ref_date or date.today()
        routed = set()
        for r in self.rules:
            if r.route_status in (RouteStatus.ACTIVE, RouteStatus.REVIEW):
                if r.active_from <= d and (r.active_to is None or r.active_to >= d):
                    routed.add(r.aktivitetstype_kode)
        return sorted(set(all_codes) - routed)


def _to_date(value: date | datetime) -> date:
    """Coerce datetime to date."""
    if isinstance(value, datetime):
        return value.date()
    return value


def resolve_ifdb_target_resource(
    aktivitetstype_kode: str,
    modelleringsnavn: str | None = None,
    modelleringsversion: str | None = None,
    occurred_at: datetime | date | None = None,
    *,
    rules: list[RoutingRule] | None = None,
) -> str:
    """Resolve the target FHIR resource for one IFDB aktivitetstype.

    This is the top-level resolver used by the Observation and
    QuestionnaireResponse paths. It returns a plain string ("Observation" or
    "QuestionnaireResponse").

    The modelleringsnavn and modelleringsversion parameters are accepted
    for forward compatibility but not used for routing in the first
    implementation (routing key = aktivitetstype_kode alone).

    Args:
        aktivitetstype_kode: IFDB AKTIVITETTYPE_KODE.
        modelleringsnavn: IFDB MODELLERINGSNAVN (reserved, not used yet).
        modelleringsversion: IFDB MODELLERINGSVERSION (reserved, not used yet).
        occurred_at: Clinical date for temporal routing.
        rules: Pre-loaded rules. If None, loads from YAML seed.

    Returns:
        "Observation" or "QuestionnaireResponse".

    Raises:
        NoActiveRouteError: No matching active route (review routes never
            resolve).
        MultipleActiveRoutesError: Ambiguous routing.
    """
    if rules is None:
        rules = load_routing_rules_from_yaml()

    resolver = RoutingResolver(rules=rules)
    result = resolver.resolve(aktivitetstype_kode, occurred_at=occurred_at)
    return result.target_resource.value
