# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Runtime routing for IFDB aktivitetsresultat SFI rows.

The routing table is the Observation allowlist.  A row becomes Observation only
when the resolver returns an approved Observation route.  Everything else
defaults to QuestionnaireResponse, because QR preserves arbitrary SFI form
structure without inventing extra Python heuristics.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

from DSA.FHIR.utils.ifdb.routing import (
    MultipleActiveRoutesError,
    NoActiveRouteError,
    RouteStatus,
    RoutingResolver,
    TargetResource,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow


@dataclass(frozen=True, slots=True)
class RoutingDecision:
    """One operational routing decision for an IFDB aktivitetsresultat row."""

    target_resource: TargetResource
    observation_category: str | None
    routing_source: str
    routing_confidence: str
    routing_reason: str


# Historical audit-table value kept for the existing MSSQL check constraint.
# The implementation no longer inspects XML to choose Observation.
_DEFAULT_QR_ROUTING_SOURCE = "xml_fallback"


def _default_questionnaire_response_decision(reason: str) -> RoutingDecision:
    return RoutingDecision(
        target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
        observation_category=None,
        routing_source=_DEFAULT_QR_ROUTING_SOURCE,
        routing_confidence="low",
        routing_reason=reason,
    )


def classify_xml_route(
    parsed_rows: list[AktivitetsresultatRow] | None,
    *,
    activity_type_code: object = None,
    model_name: object = None,
    model_version: object = None,
) -> RoutingDecision:
    """Default a non-allowlisted row to QR without inspecting XML shape."""
    return _default_questionnaire_response_decision(
        "No approved Observation route matched; defaulted to QuestionnaireResponse."
    )


def resolve_target_with_fallback(
    row_dict: dict[str, Any],
    resolver: RoutingResolver,
    parsed_rows: list[AktivitetsresultatRow] | None,
    *,
    occurred_at: date | datetime | None = None,
) -> RoutingDecision:
    """Resolve routing from the allowlist, defaulting non-matches to QR."""
    activity_type_code = row_dict.get("AKTIVITETTYPE_KODE")
    if activity_type_code:
        try:
            result = resolver.resolve(str(activity_type_code), occurred_at=occurred_at)
            if result.rule.route_status != RouteStatus.ACTIVE:
                return _default_questionnaire_response_decision(
                    "Matched route is not active/approved; defaulted to QuestionnaireResponse."
                )
            return RoutingDecision(
                target_resource=result.target_resource,
                observation_category=result.observation_category,
                routing_source="routing_table",
                routing_confidence="high",
                routing_reason="Matched explicit aktivitetstype routing rule.",
            )
        except NoActiveRouteError:
            # Covers both "no route at all" and "only review/inactive routes".
            pass
        except MultipleActiveRoutesError:
            # Overlapping active routes is a routing-config error (the seed has
            # >1 active window for the same kode). Never drop the row over it —
            # the per-row builder handler would otherwise swallow this as an
            # error and lose EVERY datapoint for the activity. Default to QR,
            # which losslessly preserves the full SFI structure, and surface the
            # ambiguity in the routing audit so the config gets fixed.
            return _default_questionnaire_response_decision(
                "Ambiguous: multiple active Observation routes matched; defaulted "
                "to QuestionnaireResponse (routing-config overlap)."
            )

    return classify_xml_route(
        parsed_rows,
        activity_type_code=activity_type_code,
        model_name=row_dict.get("MODELLERINGSNAVN"),
        model_version=row_dict.get("MODELLERINGSVERSION"),
    )
