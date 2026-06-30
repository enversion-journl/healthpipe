# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for IFDB SFI runtime routing.

The strict resolver is still tested in ``test_ifdb_routing``. These tests pin
the operational rule: only the Observation allowlist can create Observations;
everything else defaults to QuestionnaireResponse.
"""

from __future__ import annotations

from datetime import date

import pytest
from DSA.FHIR.utils.ifdb.routing import (
    RouteStatus,
    RoutingResolver,
    RoutingRule,
    TargetResource,
)
from DSA.FHIR.utils.ifdb.routing_fallback import (
    classify_xml_route,
    resolve_target_with_fallback,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import parse_aktivitetsresultat_xml

BLODTRYK_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Element type="long">
    <Navn>BT_SYS</Navn>
    <Ledetekst>Systolisk</Ledetekst>
    <Vaerdi>142</Vaerdi>
    <ElementId>BT_SYS</ElementId>
  </Element>
  <Element type="long">
    <Navn>BT_DIA</Navn>
    <Ledetekst>Diastolisk</Ledetekst>
    <Vaerdi>88</Vaerdi>
    <ElementId>BT_DIA</ElementId>
  </Element>
</Afdelingsydelsesresultat>"""


SELEKTION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Vurdering</Overskrift>
  <Klynge type="selektion">
    <Navn>DiagnoseValg</Navn>
    <Ledetekst>Vælg diagnose</Ledetekst>
    <ValgLedetekst>Hypertension</ValgLedetekst>
  </Klynge>
</Afdelingsydelsesresultat>"""


def _parse(xml: str):
    rows, _ = parse_aktivitetsresultat_xml(xml, aktivitets_id=1)
    return rows


def _resolver_with_qr_route() -> RoutingResolver:
    return RoutingResolver(
        rules=[
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
                observation_category=None,
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            )
        ],
    )


@pytest.mark.unit
def test_missing_route_measurement_xml_defaults_to_qr():
    decision = classify_xml_route(
        _parse(BLODTRYK_XML),
        activity_type_code="KPYdelser.Afdelingsydelser.Maalinger.Blodtryk",
    )

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert decision.observation_category is None
    assert decision.routing_source == "xml_fallback"
    assert decision.routing_confidence == "low"
    assert "No approved Observation route" in decision.routing_reason


@pytest.mark.unit
def test_missing_route_selection_xml_routes_to_qr():
    decision = classify_xml_route(_parse(SELEKTION_XML), activity_type_code="Ukendt")

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert decision.routing_source == "xml_fallback"
    assert "No approved Observation route" in decision.routing_reason


@pytest.mark.unit
def test_missing_route_empty_xml_defaults_to_qr_preservation():
    rows = _parse("<Afdelingsydelsesresultat />")
    decision = classify_xml_route(rows, activity_type_code="Ukendt")

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert decision.routing_confidence == "low"


@pytest.mark.unit
def test_explicit_qr_route_wins_over_default_qr():
    decision = resolve_target_with_fallback(
        {"AKTIVITETTYPE_KODE": "Maalinger.Blodtryk"},
        _resolver_with_qr_route(),
        _parse(BLODTRYK_XML),
        occurred_at=date(2026, 6, 1),
    )

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert decision.routing_source == "routing_table"
    assert decision.routing_confidence == "high"


@pytest.mark.unit
def test_explicit_observation_route_is_only_way_to_observation():
    resolver = RoutingResolver(
        rules=[
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=TargetResource.OBSERVATION,
                observation_category="vital-signs",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            )
        ],
    )
    decision = resolve_target_with_fallback(
        {"AKTIVITETTYPE_KODE": "Maalinger.Blodtryk"},
        resolver,
        _parse(SELEKTION_XML),
        occurred_at=date(2026, 6, 1),
    )

    assert decision.target_resource == TargetResource.OBSERVATION
    assert decision.observation_category == "vital-signs"
    assert decision.routing_source == "routing_table"


@pytest.mark.unit
def test_ambiguous_overlapping_active_routes_default_to_qr_not_dropped():
    """Overlapping active route windows (a config error) must NOT drop the row.

    resolve() raises MultipleActiveRoutesError; the fallback now defaults to QR
    (lossless) instead of letting the error bubble to the per-row builder, which
    would swallow it and lose every datapoint for the activity.
    """
    resolver = RoutingResolver(
        rules=[
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=TargetResource.OBSERVATION,
                observation_category="vital-signs",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            ),
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=TargetResource.OBSERVATION,
                observation_category="procedure",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 2, 1),
                active_to=None,
            ),
        ],
    )

    decision = resolve_target_with_fallback(
        {"AKTIVITETTYPE_KODE": "Maalinger.Blodtryk"},
        resolver,
        _parse(SELEKTION_XML),
        occurred_at=date(2026, 6, 1),
    )

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert "Ambiguous" in decision.routing_reason


@pytest.mark.unit
def test_review_route_defaults_to_qr():
    """Review routes never resolve operationally — they default to QR.

    There is a single routing behavior now (no PRODUCTION/AUDIT modes): a
    review-status route is treated as 'no active route' and the fallback
    defaults it to QuestionnaireResponse.
    """
    resolver = RoutingResolver(
        rules=[
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=TargetResource.OBSERVATION,
                observation_category="vital-signs",
                route_status=RouteStatus.REVIEW,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            )
        ],
    )
    decision = resolve_target_with_fallback(
        {"AKTIVITETTYPE_KODE": "Maalinger.Blodtryk"},
        resolver,
        _parse(BLODTRYK_XML),
        occurred_at=date(2026, 6, 1),
    )

    assert decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE
    assert "No approved Observation route" in decision.routing_reason
