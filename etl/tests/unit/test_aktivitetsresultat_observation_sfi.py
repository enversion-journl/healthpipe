# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the SFI per-element Observation builder.

These tests pin the per-SFI Observation contract:

  * Observation.code.coding[] carries system=urn:dk:ifdb:sfi.
  * One Observation per SFI element, sharing the parent aktivitetsresultat
    identifier so all Observations from the same XML can be re-grouped.
  * Identifier under SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI is stable from
    (OBJEKT_ID, element_nummer).
  * value[x] is typed per SFI datatype (integer / decimal / quantity /
    string / boolean / datetime / coded).
  * No component[] is used for SFI elements (first implementation rule).
  * Procedure rows are emitted as Procedure only when SFI mode runs.
  * Mode toggle defaults to sfi_audit on dev/QA; legacy is the escape hatch.

The tests deliberately use the in-process helper functions, not the Dagster
asset, so they run fast under tests/unit/ without spinning up the pipeline.
"""

from __future__ import annotations

import importlib
import logging
import os
from datetime import date
from typing import Any

import pytest
from DSA.FHIR.utils.ifdb.routing import (
    RouteStatus,
    RoutingResolver,
    RoutingRule,
    TargetResource,
)
from DSA.FHIR.utils.ifdb.sfi_extraction import (
    extract_sfi_datapoints,
    generate_sfi_code,
)
from DSA.FHIR.utils.ifdb.value_mapping import (
    SfiValueTypeMismatch,
    map_sfi_value_to_fhir,
    project_sfi_value_to_sql,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetsresultatRow,
    parse_aktivitetsresultat_xml,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_SOURCE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
    EXTENSION_XML_ELEMENT_META,
    EXTENSION_XML_HIERARCHY,
    EXTENSION_XML_KONTEKST,
    EXTENSION_XML_ROOT_META,
    EXTENSION_XML_ROW_TYPE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_SFI,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


SATURATION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Saturation</Overskrift>
  <Ledetekst>Saturation</Ledetekst>
  <Type>MaalingSaturation</Type>
  <Kladde>False</Kladde>
  <Repetition>
    <Element type="long">
      <Navn>SaturationProcent</Navn>
      <Ledetekst>Saturation i %</Ledetekst>
      <Vaerdi>98</Vaerdi>
      <ElementId>RM_Generelle:edSaturationProcent</ElementId>
    </Element>
  </Repetition>
</Afdelingsydelsesresultat>"""


BLODTRYK_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Ledetekst>Blodtryk</Ledetekst>
  <Type>MaalingBlodtryk</Type>
  <Kladde>False</Kladde>
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


GCS_XML = """<Afdelingsydelsesresultat>
  <Overskrift>GCS</Overskrift>
  <Ledetekst>Glasgow Coma Scale</Ledetekst>
  <Type>GCS</Type>
  <Kladde>False</Kladde>
  <Element type="long">
    <Navn>edGCSTotalScore</Navn>
    <Ledetekst>Total score</Ledetekst>
    <Vaerdi>15</Vaerdi>
    <ElementId>RM_Generelle:edGCSTotalScore</ElementId>
  </Element>
</Afdelingsydelsesresultat>"""

EMPTY_SELECTION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Diagnose</Overskrift>
  <Ledetekst>Diagnose</Ledetekst>
  <Type>DiagnoseValg</Type>
  <Klynge type="selektion">
    <Navn>DiagnoseValg</Navn>
    <Ledetekst>Vælg diagnose</Ledetekst>
  </Klynge>
</Afdelingsydelsesresultat>"""


def _make_resolver(target: TargetResource = TargetResource.OBSERVATION) -> RoutingResolver:
    """Build a tiny in-memory resolver that routes everything to ``target``."""
    return RoutingResolver(
        rules=[
            RoutingRule(
                aktivitetstype_kode="Maalinger.Blodtryk",
                target_resource=target,
                observation_category="vital-signs",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            ),
            RoutingRule(
                aktivitetstype_kode="Maalinger.Saturation",
                target_resource=target,
                observation_category="vital-signs",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            ),
            RoutingRule(
                aktivitetstype_kode="GCS",
                target_resource=target,
                observation_category="exam",
                route_status=RouteStatus.ACTIVE,
                rationale=None,
                decided_by=None,
                decided_at=None,
                active_from=date(2026, 1, 1),
                active_to=None,
            ),
        ],
    )


def _make_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "OBJEKT_ID": 42,
        "PATIENT_ID": 100,
        "ENCOUNTER_ID": 200,
        "EPISODE_OF_CARE_ID": 300,
        "AKTIVITETTYPE_KODE": "Maalinger.Blodtryk",
        "AKTIVITETTYPE_KODETEKST": "Blodtryk",
        "OVERSKRIFT": "Blodtryk",
        "KLADDE": 0,
        "PERIOD_START": "2026-06-01T15:06:00",
        "PERIOD_END": None,
        "SLETTET": 0,
        "MODELLERINGSNAVN": "KPPAS",
        "MODELLERINGSVERSION": "1.0",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# value_mapping
# ---------------------------------------------------------------------------


class TestValueMapping:
    def test_integer(self) -> None:
        assert map_sfi_value_to_fhir("BT_SYS", "integer", "142") == {"valueInteger": 142}

    def test_decimal_without_unit(self) -> None:
        assert map_sfi_value_to_fhir("WEIGHT", "decimal", "72.5") == {
            "valueQuantity": {"value": 72.5}
        }

    def test_decimal_without_unit_validates_as_fhir_observation(self) -> None:
        from fhir.resources.R4B.observation import Observation

        value_field = map_sfi_value_to_fhir("WEIGHT", "decimal", "72.5")
        obs = {
            "resourceType": "Observation",
            "status": "final",
            "code": {"text": "weight"},
            **value_field,
        }
        Observation.parse_obj(obs)

    def test_decimal_with_unit_becomes_quantity(self) -> None:
        out = map_sfi_value_to_fhir("WEIGHT", "decimal", "72.5", unit="kg")
        assert out == {"valueQuantity": {"value": 72.5, "unit": "kg"}}

    def test_quantity(self) -> None:
        out = map_sfi_value_to_fhir("BT_SYS", "quantity", "142", unit="mmHg")
        assert out == {"valueQuantity": {"value": 142.0, "unit": "mmHg"}}

    def test_boolean_true(self) -> None:
        assert map_sfi_value_to_fhir("AT_RISK", "boolean", "true") == {"valueBoolean": True}

    def test_boolean_danish(self) -> None:
        assert map_sfi_value_to_fhir("AT_RISK", "boolean", "Nej") == {"valueBoolean": False}

    def test_string(self) -> None:
        assert map_sfi_value_to_fhir("NOTE", "string", "  fri tekst  ") == {
            "valueString": "fri tekst"
        }

    def test_datetime(self) -> None:
        out = map_sfi_value_to_fhir("WHEN", "datetime", "2026-06-01T10:00:00")
        assert out == {"valueDateTime": "2026-06-01T10:00:00"}

    def test_coded_choice(self) -> None:
        out = map_sfi_value_to_fhir("CHOICE", "coding", "Hypertension")
        assert out == {"valueCodeableConcept": {"text": "Hypertension"}}

    def test_empty_value_returns_none(self) -> None:
        assert map_sfi_value_to_fhir("X", "integer", "") is None
        assert map_sfi_value_to_fhir("X", "integer", None) is None

    def test_integer_mismatch_raises(self) -> None:
        with pytest.raises(SfiValueTypeMismatch) as exc:
            map_sfi_value_to_fhir("BT_SYS", "integer", "abc")
        assert exc.value.sfi_code == "BT_SYS"
        assert exc.value.datatype == "integer"
        assert exc.value.raw_value == "abc"

    def test_boolean_mismatch_raises(self) -> None:
        with pytest.raises(SfiValueTypeMismatch):
            map_sfi_value_to_fhir("AT_RISK", "boolean", "maybe")

    def test_sql_projection_integer(self) -> None:
        out = project_sfi_value_to_sql("BT_SYS", "integer", "142")
        assert out["value_type"] == "integer"
        assert out["value_int"] == 142
        assert out["value_dec"] is None
        assert out["value_string"] is None

    def test_sql_projection_quantity(self) -> None:
        out = project_sfi_value_to_sql("BT_SYS", "quantity", "142", unit="mmHg")
        assert out["value_type"] == "quantity"
        assert out["value_quantity"] == 142.0
        assert out["value_unit"] == "mmHg"


# ---------------------------------------------------------------------------
# sfi_extraction
# ---------------------------------------------------------------------------


class TestGenerateSfiCode:
    def test_uses_element_id_when_present(self) -> None:
        # Code is qualified by MODELLERINGSNAVN.
        code, status = generate_sfi_code("BT_SYS", "Systolisk", "Blodtryk")
        assert code == "Blodtryk.BT_SYS"
        assert status == "active"

    def test_falls_back_to_name_with_review_status(self) -> None:
        code, status = generate_sfi_code(None, "Systolisk", "Blodtryk")
        assert code.startswith("Blodtryk._fb_")
        assert "Systolisk" in code
        assert status == "review"

    def test_unknown_code_returns_review(self) -> None:
        code, status = generate_sfi_code(None, None, None)
        assert status == "review"


class TestExtractSfiDatapoints:
    def test_saturation_produces_one_datapoint(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(SATURATION_XML, aktivitets_id=1)
        dps = extract_sfi_datapoints(rows, model_name="Saturation", model_version="1.0")
        assert len(dps) == 1
        dp = dps[0]
        assert dp.sfi_code == "Saturation.RM_Generelle:edSaturationProcent"
        assert dp.datatype == "integer"
        assert dp.unit == "%"
        assert dp.raw_value == "98"
        assert dp.element_nummer == 1
        assert dp.sfi_status == "active"

    def test_blodtryk_produces_two_datapoints_with_distinct_element_nummer(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=2)
        dps = extract_sfi_datapoints(rows, model_name="Blodtryk")
        assert len(dps) == 2
        assert {dp.sfi_code for dp in dps} == {"Blodtryk.BT_SYS", "Blodtryk.BT_DIA"}
        nrs = sorted(dp.element_nummer for dp in dps)
        assert nrs == [1, 2]

    def test_skips_empty_xml_sentinel(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml("", aktivitets_id=3)
        dps = extract_sfi_datapoints(rows)
        assert dps == []

    def test_gcs_total_score_extracted(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(GCS_XML, aktivitets_id=4)
        dps = extract_sfi_datapoints(rows, model_name="GCS")
        assert len(dps) == 1
        assert dps[0].sfi_code == "GCS.RM_Generelle:edGCSTotalScore"
        assert dps[0].raw_value == "15"


# ---------------------------------------------------------------------------
# SFI Observation builder
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def sfi_module():
    """Reload the SFI builder with SFI audit mode enabled.

    The mode flag is read at import time, so we set the env var and reload
    a fresh copy of the module. The module is restored to legacy in
    ``teardown`` so the rest of the test suite is unaffected.
    """
    prior = os.environ.get("IFDB_SFI_OBSERVATION_MODE")
    os.environ["IFDB_SFI_OBSERVATION_MODE"] = "sfi_audit"
    import DSA.FHIR.Aktivitetsresultat.routing_glue as routing_mod
    import DSA.FHIR.Observation.observation_sfi_builder as mod
    import DSA.FHIR.utils.ifdb.aktivitetsresultat_config as config_mod

    config_mod = importlib.reload(config_mod)
    routing_mod = importlib.reload(routing_mod)
    mod = importlib.reload(mod)
    try:
        yield mod
    finally:
        if prior is None:
            os.environ.pop("IFDB_SFI_OBSERVATION_MODE", None)
        else:
            os.environ["IFDB_SFI_OBSERVATION_MODE"] = prior
        importlib.reload(config_mod)
        importlib.reload(routing_mod)
        importlib.reload(mod)


def _get_logger() -> logging.Logger:
    return logging.getLogger("sfi-test")


_DISALLOWED_ON_SFI_OBSERVATION = {
    EXTENSION_XML_ROOT_META,
    EXTENSION_XML_KONTEKST,
    EXTENSION_AKTIVITETSRESULTAT_SOURCE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
    EXTENSION_XML_HIERARCHY,
    EXTENSION_XML_ELEMENT_META,
    EXTENSION_XML_ROW_TYPE,
}


def _collect_extension_urls(value: Any) -> set[str]:
    urls: set[str] = set()
    if isinstance(value, dict):
        for key, child in value.items():
            if key == "extension" and isinstance(child, list):
                for ext in child:
                    if isinstance(ext, dict):
                        url = ext.get("url")
                        if url:
                            urls.add(str(url))
                        urls |= _collect_extension_urls(ext)
            else:
                urls |= _collect_extension_urls(child)
    elif isinstance(value, list):
        for item in value:
            urls |= _collect_extension_urls(item)
    return urls


class TestSfiObservationBuilder:
    def test_blodtryk_yields_one_observation_per_sfi_element(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        row_dict = _make_row(XML=BLODTRYK_XML)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=row_dict,
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        assert len(obs_dicts) == 2

        codes = {obs["code"]["coding"][0]["code"]: obs for _, obs in obs_dicts}
        # _make_row defaults MODELLERINGSNAVN="KPPAS"; codes are qualified.
        assert set(codes.keys()) == {"KPPAS.BT_SYS", "KPPAS.BT_DIA"}

        for _code, obs in codes.items():
            assert obs["resourceType"] == "Observation"
            assert obs["code"]["coding"][0]["system"] == SYSTEM_IFDB_SFI
            # No component[] for SFI elements.
            assert "component" not in obs

    def test_observations_share_parent_via_extension(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=BLODTRYK_XML),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )

        def _aktivitet_id(obs: dict) -> str | None:
            for ext in obs.get("extension", []):
                if not ext.get("url", "").endswith("sfiInstanceMeta"):
                    continue
                for part in ext.get("extension", []):
                    if part.get("url") == "aktivitetId":
                        return part.get("valueString")
            return None

        parent_values: set[str] = set()
        per_element_values: set[str] = set()
        for _, obs in obs_dicts:
            ids_by_system = {ident["system"]: ident["value"] for ident in obs["identifier"]}
            # Parent OBJEKT_ID is carried in the sfiInstanceMeta extension, NOT as
            # an identifier — re-adding it as an identifier would violate the
            # UNIQUE index idx_observation_urn_source_ifdb_aktivitetsresultat_uniq
            # because SFI Observations are 1:N per OBJEKT_ID.
            assert SYSTEM_IFDB_AKTIVITETSRESULTAT not in ids_by_system
            assert SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI in ids_by_system
            parent_values.add(_aktivitet_id(obs))
            per_element_values.add(ids_by_system[SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI])

        # All Observations from the same XML share the parent ifdb_aktivitet_id.
        assert parent_values == {"42"}
        # Each per-element identifier is unique.
        assert per_element_values == {"42#sfi#1", "42#sfi#2"}

    def test_value_is_typed_integer(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=BLODTRYK_XML),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        for _, obs in obs_dicts:
            assert "valueInteger" in obs
            assert isinstance(obs["valueInteger"], int)

    def test_saturation_one_observation(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(SATURATION_XML, aktivitets_id=999)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(
                OBJEKT_ID=999,
                AKTIVITETTYPE_KODE="Maalinger.Saturation",
                XML=SATURATION_XML,
            ),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        assert len(obs_dicts) == 1
        _, obs = obs_dicts[0]
        # _make_row defaults MODELLERINGSNAVN="KPPAS" → qualified code.
        assert obs["code"]["coding"][0]["code"] == "KPPAS.RM_Generelle:edSaturationProcent"
        assert obs["valueInteger"] == 98

    def test_gcs_total_score(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(GCS_XML, aktivitets_id=7)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(
                OBJEKT_ID=7,
                AKTIVITETTYPE_KODE="GCS",
                XML=GCS_XML,
            ),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        assert len(obs_dicts) == 1
        _, obs = obs_dicts[0]
        assert obs["code"]["coding"][0]["code"] == "KPPAS.RM_Generelle:edGCSTotalScore"
        assert obs["valueInteger"] == 15
        # category from the resolver rule
        assert obs["category"][0]["coding"][0]["code"] == "exam"

    def test_qr_routed_row_returns_no_observations(self, sfi_module) -> None:
        """When routing says QuestionnaireResponse, this path emits zero Observations.

        This keeps Observation and QuestionnaireResponse routing mutually
        exclusive for the same source row.
        """
        rows, _ = parse_aktivitetsresultat_xml(GCS_XML, aktivitets_id=7)
        qr_resolver = _make_resolver(target=TargetResource.QUESTIONNAIRE_RESPONSE)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(
                OBJEKT_ID=7,
                AKTIVITETTYPE_KODE="GCS",
                XML=GCS_XML,
            ),
            xml_rows=rows,
            resolved=None,
            routing_resolver=qr_resolver,
            logger=_get_logger(),
        )
        assert obs_dicts == []

    def test_unrouted_row_defaults_to_qr_no_observations(self, sfi_module) -> None:
        empty_resolver = RoutingResolver(rules=[])
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=BLODTRYK_XML),
            xml_rows=rows,
            resolved=None,
            routing_resolver=empty_resolver,
            logger=_get_logger(),
        )
        assert obs_dicts == []

    def test_empty_selektion_does_not_emit_value_less_observation(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(EMPTY_SELECTION_XML, aktivitets_id=88)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(
                OBJEKT_ID=88,
                AKTIVITETTYPE_KODE="Maalinger.Blodtryk",
                XML=EMPTY_SELECTION_XML,
            ),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        assert obs_dicts == []

    def test_routing_decision_sink_records_runtime_route(self, sfi_module) -> None:
        empty_resolver = RoutingResolver(rules=[])
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        routing_decisions: list[dict[str, str | None]] = []

        obs_list, proc_list, cond_list, errors = sfi_module._build_sfi_observation_dicts_for_rows(
            rows=[_make_row(XML=BLODTRYK_XML)],
            parsed_data_by_index=[rows],
            logger=_get_logger(),
            resolved=None,
            routing_resolver=empty_resolver,
            routing_decision_sink=routing_decisions,
        )

        assert errors == 0
        assert obs_list == []
        assert proc_list == []
        assert routing_decisions == [
            {
                "ifdb_aktivitet_id": "42",
                "aktivitetstype_kode": "Maalinger.Blodtryk",
                "target_resource": "QuestionnaireResponse",
                "observation_category": None,
                "routing_source": "xml_fallback",
                "routing_confidence": "low",
                "routing_reason": "No approved Observation route matched; defaulted to QuestionnaireResponse.",
            }
        ]

    def test_routing_summary_collapses_fallback_logs(self, sfi_module, caplog) -> None:
        decisions = [
            {
                "ifdb_aktivitet_id": "1",
                "aktivitetstype_kode": "A",
                "target_resource": "QuestionnaireResponse",
                "observation_category": None,
                "routing_source": "xml_fallback",
                "routing_confidence": "low",
                "routing_reason": "default",
            },
            {
                "ifdb_aktivitet_id": "2",
                "aktivitetstype_kode": "A",
                "target_resource": "QuestionnaireResponse",
                "observation_category": None,
                "routing_source": "xml_fallback",
                "routing_confidence": "low",
                "routing_reason": "default",
            },
            {
                "ifdb_aktivitet_id": "3",
                "aktivitetstype_kode": "B",
                "target_resource": "Observation",
                "observation_category": "vital-signs",
                "routing_source": "routing_table",
                "routing_confidence": "high",
                "routing_reason": "matched",
            },
        ]

        with caplog.at_level(logging.INFO, logger="sfi-test"):
            sfi_module._log_sfi_routing_summary(
                _get_logger(),
                decisions,
                label="chunk 1/1",
            )

        assert "[SFI] Routing summary chunk 1/1" in caplog.text
        assert "Observation=1" in caplog.text
        assert "QuestionnaireResponse=2" in caplog.text
        assert "fallback_rows=2" in caplog.text
        assert "A=2" in caplog.text
        assert "Fallback route for AKTIVITETTYPE_KODE" not in caplog.text

    def test_source_identifier_helper_is_stable(self) -> None:
        from DSA.FHIR.utils.ifdb.aktivitetsresultat_identifiers import _sfi_identifier_value

        a = _sfi_identifier_value("42", 3)
        b = _sfi_identifier_value("42", 3)
        assert a == b == "42#sfi#3"

    def test_no_component_carries_sfi(self, sfi_module) -> None:
        """First-implementation rule: no Observation may use component[] for SFI."""
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=BLODTRYK_XML),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        for _, obs in obs_dicts:
            assert "component" not in obs

    def test_sfi_observation_has_no_redundant_xml_lineage_extensions(self, sfi_module) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=BLODTRYK_XML),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )

        found = set()
        for _, obs in obs_dicts:
            found |= _collect_extension_urls(obs)

        violations = {
            url
            for url in found
            for base in _DISALLOWED_ON_SFI_OBSERVATION
            if url == base or url.startswith(f"{base}#")
        }
        assert not violations

    def test_procedure_preserves_value_xml_in_sfi_mode(self, sfi_module) -> None:
        """A Procedure row may also emit numeric Observations when routed so."""
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=42)

        # Procedure is an event side-output. The XML values still follow the
        # normal route, so an allowlisted procedure-measurement can yield
        # Procedure + numeric per-SFI Observations.
        proc_resolver = RoutingResolver(
            rules=[
                RoutingRule(
                    aktivitetstype_kode="SKS.a.proKABC10",
                    target_resource=TargetResource.OBSERVATION,
                    observation_category="procedure",
                    route_status=RouteStatus.ACTIVE,
                    rationale=None,
                    decided_by=None,
                    decided_at=None,
                    active_from=date(2026, 1, 1),
                    active_to=None,
                )
            ],
        )

        # Reach for the routing resolver explicitly: the SFI helper uses
        # `_get_routing_resolver` which loads YAML. We test it with a patched resolver.
        from DSA.FHIR.Observation import observation_sfi_builder

        original_loader = observation_sfi_builder._get_routing_resolver
        observation_sfi_builder._get_routing_resolver = lambda: proc_resolver
        routing_decisions: list[dict[str, str | None]] = []
        try:
            obs_list, proc_list, _cond_list, _err = (
                sfi_module._build_sfi_observation_dicts_for_rows(
                    rows=[
                        _make_row(
                            AKTIVITETTYPE_KODE="SKS.a.proKABC10",
                            XML=BLODTRYK_XML,
                        )
                    ],
                    parsed_data_by_index=[rows],
                    logger=_get_logger(),
                    resolved=None,
                    routing_decision_sink=routing_decisions,
                )
            )
        finally:
            observation_sfi_builder._get_routing_resolver = original_loader

        assert len(obs_list) == 2
        assert len(proc_list) == 1
        _, proc = proc_list[0]
        assert proc["resourceType"] == "Procedure"
        assert proc["code"]["coding"][0]["code"] == "KABC10"
        assert proc["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE
        assert routing_decisions[0]["target_resource"] == "Observation"
        assert routing_decisions[0]["routing_source"] == "routing_table"
        assert "Procedure emitted in parallel" in routing_decisions[0]["routing_reason"]

    def test_type_mismatch_skips_only_that_element(self, sfi_module) -> None:
        bad_xml = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Element type="long">
    <Navn>BT_SYS</Navn>
    <Vaerdi>not_a_number</Vaerdi>
    <ElementId>BT_SYS</ElementId>
  </Element>
  <Element type="long">
    <Navn>BT_DIA</Navn>
    <Vaerdi>88</Vaerdi>
    <ElementId>BT_DIA</ElementId>
  </Element>
</Afdelingsydelsesresultat>"""
        rows, _ = parse_aktivitetsresultat_xml(bad_xml, aktivitets_id=42)
        obs_dicts = sfi_module._build_sfi_observations_for_row(
            row_dict=_make_row(XML=bad_xml),
            xml_rows=rows,
            resolved=None,
            routing_resolver=_make_resolver(),
            logger=_get_logger(),
        )
        # BT_SYS fails to coerce, BT_DIA succeeds — partial emission stays row-local.
        assert len(obs_dicts) == 1
        _, obs = obs_dicts[0]
        assert obs["code"]["coding"][0]["code"] == "KPPAS.BT_DIA"

    def test_duplicate_element_occurrence_raises(self, sfi_module) -> None:
        """Two value-bearing datapoints minting the same Observation id is an
        upstream-invariant violation (the parser guarantees a unique
        element_nummer per document). It must fail loudly, not collapse one
        datapoint silently (Y3)."""
        from DSA.FHIR.Observation.observation_sfi_builder import (
            SfiDuplicateOccurrenceError,
        )

        class _StubResolved:
            miss_counts = {"Patient": 0, "Encounter": 0, "EpisodeOfCare": 0}

            def observation_id_sfi(self, objekt_id: Any, element_nummer: Any) -> str:
                return f"obs-{objekt_id}-{element_nummer}"

            def __getattr__(self, _name: str) -> Any:
                return lambda *a, **k: None

        # Same element_nummer on two value-bearing rows -> identical resource id.
        dup_rows = [
            AktivitetsresultatRow(
                aktivitets_id=42,
                opdateret_tid=None,
                dw_id_audit_update=0,
                dw_is_deleted=0,
                element_nummer=1,
                element_navn="BT_SYS",
                element_vaerdi="120",
                element_type="long",
                raekke_type="Element",
            ),
            AktivitetsresultatRow(
                aktivitets_id=42,
                opdateret_tid=None,
                dw_id_audit_update=0,
                dw_is_deleted=0,
                element_nummer=1,
                element_navn="BT_DIA",
                element_vaerdi="80",
                element_type="long",
                raekke_type="Element",
            ),
        ]

        with pytest.raises(SfiDuplicateOccurrenceError):
            sfi_module._build_sfi_observations_for_row(
                row_dict=_make_row(XML="<x/>"),
                xml_rows=dup_rows,
                resolved=_StubResolved(),
                routing_resolver=_make_resolver(),
                logger=_get_logger(),
            )

    def test_default_mode_is_sfi_audit(self) -> None:
        """Default mode is sfi_audit.

        Legacy remains a valid escape-hatch value but is no longer the default
        — we want the SFI per-element pipeline to run on dev/QA without each
        caller having to set the env var. Production rollout will flip this
        to sfi_production once the routing seed is complete and approved.
        """
        import DSA.FHIR.utils.ifdb.aktivitetsresultat_config as config_mod
        from DSA.FHIR.Aktivitetsresultat.routing_glue import _sfi_mode_enabled

        assert config_mod.SFI_MODE_LEGACY == "legacy"
        assert config_mod.SFI_MODE_AUDIT == "sfi_audit"
        assert config_mod.SFI_MODE_PRODUCTION == "sfi_production"
        if os.environ.get("IFDB_SFI_OBSERVATION_MODE") in (None, ""):
            assert config_mod._SFI_MODE == "sfi_audit"
            assert _sfi_mode_enabled() is True
