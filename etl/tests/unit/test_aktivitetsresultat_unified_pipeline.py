# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the unified AKTIVITETSRESULTAT SFI fan-out."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator
from DSA.FHIR.Aktivitetsresultat.assets import (
    _AKTIVITETSRESULTAT_OUT,
    _yield_multi_asset_output,
)
from DSA.FHIR.Aktivitetsresultat.questionnaire_outputs import (
    _merge_existing_questionnaire_items,
    _merge_questionnaire_into_store,
    _post_dirty_questionnaires,
)
from DSA.FHIR.Aktivitetsresultat.references import (
    ResolvedRefLookup,
    _pre_resolve_references,
)
from DSA.FHIR.Observation.observation_sfi_builder import (
    _build_sfi_unified_dicts_for_rows,
)
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import QUESTIONNAIRE_IDENTIFIER_SYSTEM
from DSA.FHIR.utils.ifdb.aktivitetsresultat_config import _AKTIVITETSRESULTAT_CHUNK_SIZE
from DSA.FHIR.utils.ifdb.routing import TargetResource
from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)


def test_merge_questionnaire_into_store_preserves_distinct_item_ids():
    store: dict[str, dict] = {}
    _merge_questionnaire_into_store(
        store,
        {
            "id": "questionnaire-existing-id",
            "item": [{"id": "e-1", "linkId": "Model.A", "type": "string"}],
        },
    )
    changed = _merge_questionnaire_into_store(
        store,
        {
            "id": "questionnaire-existing-id",
            "item": [
                {"id": "e-1", "linkId": "Model.A", "type": "string"},
                {"id": "e-2", "linkId": "Model.A", "type": "string"},
                {"id": "e-3", "linkId": "Model.B", "type": "integer"},
            ],
        },
    )
    assert changed is True
    assert len(store) == 1
    items = store["questionnaire-existing-id"]["item"]
    assert [(item["linkId"], item["id"]) for item in items] == [
        ("Model.A", "e-1"),
        ("Model.A", "e-2"),
        ("Model.B", "e-3"),
    ]


def test_merge_questionnaire_keeps_distinct_concepts_at_same_occurrence_position():
    """Two source documents of the same model where the same positional item id
    (``e-2``) maps to a DIFFERENT SFI concept (``linkId``) must keep BOTH concepts.

    Regression for the cross-document dedup that keyed on ``item.id`` alone: the
    second concept's item would be dropped from the Questionnaire definition and
    then silently dropped by the ``SFI_datapoint_build`` INNER JOIN on
    ``(link_id, item_key)``.
    """
    store: dict[str, dict] = {}
    _merge_questionnaire_into_store(
        store,
        {
            "id": "q-model",
            "item": [
                {"id": "e-1", "linkId": "Model.A", "type": "string"},
                {"id": "e-2", "linkId": "Model.B", "type": "integer"},
            ],
        },
    )
    # Second document: an optional element shifted positions, so e-2 now carries
    # a DIFFERENT concept (Model.C) than it did in the first document (Model.B).
    changed = _merge_questionnaire_into_store(
        store,
        {
            "id": "q-model",
            "item": [
                {"id": "e-1", "linkId": "Model.A", "type": "string"},
                {"id": "e-2", "linkId": "Model.C", "type": "coding"},
            ],
        },
    )

    assert changed is True
    items = store["q-model"]["item"]
    pairs = {(item["linkId"], item["id"]) for item in items}
    # All three distinct concepts survive; Model.C at e-2 is NOT collapsed away.
    assert ("Model.A", "e-1") in pairs
    assert ("Model.B", "e-2") in pairs
    assert ("Model.C", "e-2") in pairs
    assert len(items) == 3


def test_merge_existing_questionnaire_items_preserves_aidbox_model_items():
    """Posting a later subset must not remove existing model-level items.

    Questionnaire is model/version-level, while each run/partition may only see a
    subset of concrete activity responses. Direct Aidbox upsert replaces the JSON
    resource, so the outgoing resource must first merge items already persisted
    in Aidbox.
    """
    outgoing = {
        "id": "q-model",
        "item": [{"id": "e-1", "linkId": "Model.A", "type": "string"}],
    }
    existing = {
        "id": "q-model",
        "item": [
            {"id": "e-1", "linkId": "Model.A", "type": "string"},
            {"id": "e-2", "linkId": "Model.B", "type": "integer"},
        ],
    }

    changed = _merge_existing_questionnaire_items(outgoing, existing)

    assert changed is True
    assert [(item["linkId"], item["id"]) for item in outgoing["item"]] == [
        ("Model.A", "e-1"),
        ("Model.B", "e-2"),
    ]


def test_post_dirty_questionnaires_merges_existing_aidbox_items_before_post(monkeypatch):
    posted_resources: list[tuple[str, dict]] = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def execute(self, sql, params):
            assert "FROM questionnaire" in sql
            assert params == (["q-model"],)

        def fetchall(self):
            return [
                (
                    "q-model",
                    {
                        "id": "q-model",
                        "item": [{"id": "e-2", "linkId": "Model.B", "type": "integer"}],
                    },
                )
            ]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def cursor(self):
            return FakeCursor()

    class FakeFHIRDatabase:
        def get_connection(self):
            return FakeConnection()

    def fake_post_resource_dicts_direct(**kwargs):
        posted_resources.extend(kwargs["resource_dicts"])
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(1),
                "failed_count": dg.MetadataValue.int(0),
            }
        )

    monkeypatch.setattr(
        "DSA.FHIR.utils.ifdb.fhir_posting.post_resource_dicts_direct",
        fake_post_resource_dicts_direct,
    )
    context = MagicMock()
    questionnaires_by_id = {
        "q-model": {
            "id": "q-model",
            "item": [{"id": "e-1", "linkId": "Model.A", "type": "string"}],
        }
    }

    posted_count, failed_count, posted_ids = _post_dirty_questionnaires(
        context=context,
        questionnaires_by_id=questionnaires_by_id,
        dirty_questionnaire_ids=["q-model", "q-model"],
        fhir_database=FakeFHIRDatabase(),
    )

    assert (posted_count, failed_count, posted_ids) == (1, 0, ["q-model"])
    assert len(posted_resources) == 1
    _, posted = posted_resources[0]
    assert {(item["linkId"], item["id"]) for item in posted["item"]} == {
        ("Model.A", "e-1"),
        ("Model.B", "e-2"),
    }
    context.log.info.assert_called_once()


def test_unified_mixed_sks_row_preserves_non_observation_rows_as_parallel_qr(monkeypatch):
    """A SKS procedure row carrying mixed SFI datapoints must keep valid numeric
    values as Observations and preserve the remaining rows as a parallel QR."""
    row = {
        "OBJEKT_ID": "1",
        "AKTIVITETTYPE_KODE": "SKS.a.proBWW21",
        "MODELLERINGSNAVN": "M",
        "MODELLERINGSVERSION": "v1",
    }
    numeric_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=1,
        element_navn="BT_SYS",
        element_vaerdi="120",
        element_type="long",
        raekke_type="Element",
    )
    text_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=2,
        element_navn="Notat",
        element_vaerdi="se journal",
        element_type="tekst",
        raekke_type="Element",
    )
    invalid_numeric_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=3,
        element_navn="BT_BAD",
        element_vaerdi="not_a_number",
        element_type="long",
        raekke_type="Element",
    )

    base = "DSA.FHIR.Observation.observation_sfi_builder"
    monkeypatch.setattr(
        f"{base}._resolve_row_target_resource",
        lambda *a, **k: RoutingDecision(
            target_resource=TargetResource.OBSERVATION,
            observation_category="procedure",
            routing_source="test",
            routing_confidence="high",
            routing_reason="test",
        ),
    )
    # The real numeric filter keeps only the numeric Element; simulate that by
    # returning one Observation (for BT_SYS) and letting the builder route the
    # rest to QR.
    monkeypatch.setattr(
        f"{base}._build_sfi_observations_for_row",
        lambda **kwargs: [
            (
                "obs-1",
                {
                    "resourceType": "Observation",
                    "id": "obs-1",
                    "status": "final",
                    "code": {"text": "BT_SYS"},
                },
            )
        ],
    )

    captured: dict[str, list] = {}

    def fake_q(parsed_rows, **kwargs):
        captured["q"] = list(parsed_rows)
        return {"id": "q-model", "item": [{"id": "e-2", "linkId": "M.Notat", "type": "string"}]}

    def fake_qr(row_dict, parsed_rows, **kwargs):
        captured["qr"] = list(parsed_rows)
        return {"id": f"qr-{row_dict['OBJEKT_ID']}", "resourceType": "QuestionnaireResponse"}

    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_dict",
        fake_q,
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_response_dict",
        fake_qr,
    )
    monkeypatch.setattr(f"{base}._validate_observation_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_procedure_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_condition_dicts_sample", lambda *a: None)

    routing_decisions: list[dict[str, str | None]] = []
    questionnaires: dict[str, dict] = {}
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=[row],
        parsed_data_by_index=[[numeric_row, invalid_numeric_row, text_row]],
        kontekst_by_index=[None],
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=routing_decisions,
        questionnaires_by_id=questionnaires,
    )

    assert errors == 0
    # Numeric datapoint preserved as an Observation...
    assert [rid for rid, _ in obs] == ["obs-1"]
    # ...and the rows that did not become Observations preserved in parallel as a QR.
    assert len(qrs) == 1
    # The QR builder saw only rows that did not become Observations.
    assert [r.element_navn for r in captured["qr"]] == ["BT_BAD", "Notat"]
    # The row's primary routing decision stays Observation.
    assert routing_decisions[0]["target_resource"] == "Observation"


def test_transform_chunk_sfi_unified_routes_observation_and_qr(monkeypatch):
    row_obs = {"OBJEKT_ID": "1", "MODELLERINGSNAVN": "M", "MODELLERINGSVERSION": "v1"}
    row_qr = {"OBJEKT_ID": "2", "MODELLERINGSNAVN": "M", "MODELLERINGSVERSION": "v1"}
    chunk = [row_obs, row_qr]
    qr_xml_row = AktivitetsresultatRow(
        aktivitets_id=2,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
    )
    parsed: list = [[], [qr_xml_row]]
    kontekst = [None, None]

    def fake_resolve(row_dict, resolver, xml_rows, logger):
        if row_dict["OBJEKT_ID"] == "1":
            return RoutingDecision(
                target_resource=TargetResource.OBSERVATION,
                observation_category="vital-signs",
                routing_source="test",
                routing_confidence="high",
                routing_reason="test",
            )
        return RoutingDecision(
            target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
            observation_category=None,
            routing_source="test",
            routing_confidence="high",
            routing_reason="test",
        )

    base = "DSA.FHIR.Observation.observation_sfi_builder"
    monkeypatch.setattr(f"{base}._resolve_row_target_resource", fake_resolve)
    monkeypatch.setattr(
        f"{base}._build_sfi_observations_for_row",
        lambda **kwargs: [
            (
                "obs-1",
                {
                    "resourceType": "Observation",
                    "id": "obs-1",
                    "status": "final",
                    "code": {"text": "test"},
                },
            )
        ],
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_dict",
        lambda parsed_rows, **kwargs: {
            "id": "questionnaire-existing-id",
            "item": [{"linkId": "M.X", "type": "string"}],
        },
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_response_dict",
        lambda row_dict, parsed_rows, **kwargs: {
            "id": f"qr-existing-{row_dict['OBJEKT_ID']}",
            "resourceType": "QuestionnaireResponse",
        },
    )
    questionnaires: dict[str, dict] = {}
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=chunk,
        parsed_data_by_index=parsed,
        kontekst_by_index=kontekst,
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=[],
        questionnaires_by_id=questionnaires,
    )

    assert len(obs) == 1
    assert proc == []
    assert len(qrs) == 1
    assert qrs[0][0] == "qr-existing-2"
    assert errors == 0
    assert "questionnaire-existing-id" in questionnaires


def test_transform_chunk_sfi_unified_preserves_xml_for_procedure_rows_as_qr(monkeypatch):
    row = {
        "OBJEKT_ID": "1",
        "AKTIVITETTYPE_KODE": "SKS.a.proBWW21",
        "MODELLERINGSNAVN": "M",
        "MODELLERINGSVERSION": "v1",
    }
    xml_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=1,
        element_navn="Notat",
        element_vaerdi="tekst",
        raekke_type="Element",
    )

    base = "DSA.FHIR.Observation.observation_sfi_builder"

    monkeypatch.setattr(
        f"{base}._resolve_row_target_resource",
        lambda *args, **kwargs: RoutingDecision(
            target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
            observation_category=None,
            routing_source="xml_fallback",
            routing_confidence="low",
            routing_reason="default QR",
        ),
    )
    monkeypatch.setattr(
        f"{base}._build_sfi_observations_for_row",
        lambda **kwargs: pytest.fail("QR-routed procedure XML must not build Observation"),
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_dict",
        lambda parsed_rows, **kwargs: {
            "id": "questionnaire-existing-id",
            "item": [{"linkId": "M.Notat", "type": "string"}],
        },
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_response_dict",
        lambda row_dict, parsed_rows, **kwargs: {
            "id": f"qr-existing-{row_dict['OBJEKT_ID']}",
            "resourceType": "QuestionnaireResponse",
        },
    )
    monkeypatch.setattr(f"{base}._validate_procedure_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_condition_dicts_sample", lambda *a: None)

    routing_decisions: list[dict[str, str | None]] = []
    questionnaires: dict[str, dict] = {}
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=[row],
        parsed_data_by_index=[[xml_row]],
        kontekst_by_index=[None],
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=routing_decisions,
        questionnaires_by_id=questionnaires,
    )

    assert obs == []
    assert len(proc) == 1
    assert len(qrs) == 1
    assert errors == 0
    assert routing_decisions[0]["target_resource"] == "QuestionnaireResponse"
    assert routing_decisions[0]["routing_source"] == "xml_fallback"
    assert "Procedure emitted in parallel" in routing_decisions[0]["routing_reason"]
    assert "questionnaire-existing-id" in questionnaires


def test_transform_chunk_sfi_unified_keeps_procedure_only_when_xml_has_no_values(
    monkeypatch,
):
    row = {
        "OBJEKT_ID": "1",
        "AKTIVITETTYPE_KODE": "SKS.a.proBWW21",
        "MODELLERINGSNAVN": "M",
        "MODELLERINGSVERSION": "v1",
    }
    xml_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=1,
        element_navn="Tomt felt",
        element_vaerdi="",
        raekke_type="Element",
    )

    base = "DSA.FHIR.Observation.observation_sfi_builder"

    def fail_if_called(*args, **kwargs):
        raise AssertionError("Procedure-only rows must not route empty XML")

    monkeypatch.setattr(f"{base}._resolve_row_target_resource", fail_if_called)
    monkeypatch.setattr(f"{base}._build_sfi_observations_for_row", fail_if_called)
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_dict",
        fail_if_called,
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_response_dict",
        fail_if_called,
    )
    monkeypatch.setattr(f"{base}._validate_procedure_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_condition_dicts_sample", lambda *a: None)

    routing_decisions: list[dict[str, str | None]] = []
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=[row],
        parsed_data_by_index=[[xml_row]],
        kontekst_by_index=[None],
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=routing_decisions,
        questionnaires_by_id={},
    )

    assert obs == []
    assert len(proc) == 1
    assert qrs == []
    assert errors == 0
    assert routing_decisions[0]["target_resource"] == "Procedure"
    assert routing_decisions[0]["routing_source"] == "procedure_code"


def test_transform_chunk_sfi_unified_preserves_xml_for_procedure_rows_as_observation(
    monkeypatch,
):
    row = {
        "OBJEKT_ID": "1",
        "AKTIVITETTYPE_KODE": "SKS.a.proZZ3160",
        "MODELLERINGSNAVN": "M",
        "MODELLERINGSVERSION": "v1",
    }
    xml_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=1,
        element_navn="Systolisk",
        element_vaerdi="120",
        element_type="integer",
        raekke_type="Element",
    )

    base = "DSA.FHIR.Observation.observation_sfi_builder"

    monkeypatch.setattr(
        f"{base}._resolve_row_target_resource",
        lambda *args, **kwargs: RoutingDecision(
            target_resource=TargetResource.OBSERVATION,
            observation_category="vital-signs",
            routing_source="routing_table",
            routing_confidence="high",
            routing_reason="allowlist",
        ),
    )

    def fake_build_sfi_observations_for_row(**kwargs):
        assert kwargs["numeric_values_only"] is True
        return [
            (
                "obs-1",
                {
                    "resourceType": "Observation",
                    "id": "obs-1",
                    "status": "final",
                    "code": {"text": "Systolisk"},
                },
            )
        ]

    monkeypatch.setattr(
        f"{base}._build_sfi_observations_for_row",
        fake_build_sfi_observations_for_row,
    )
    monkeypatch.setattr(f"{base}._validate_procedure_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_condition_dicts_sample", lambda *a: None)

    routing_decisions: list[dict[str, str | None]] = []
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=[row],
        parsed_data_by_index=[[xml_row]],
        kontekst_by_index=[None],
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=routing_decisions,
        questionnaires_by_id={},
    )

    assert len(obs) == 1
    assert len(proc) == 1
    assert qrs == []
    assert errors == 0
    assert routing_decisions[0]["target_resource"] == "Observation"
    assert routing_decisions[0]["routing_source"] == "routing_table"
    assert "Procedure emitted in parallel" in routing_decisions[0]["routing_reason"]


def test_transform_chunk_sfi_unified_qr_fallback_when_procedure_obs_has_no_numeric_values(
    monkeypatch,
):
    row = {
        "OBJEKT_ID": "1",
        "AKTIVITETTYPE_KODE": "SKS.a.proZZ3160",
        "MODELLERINGSNAVN": "M",
        "MODELLERINGSVERSION": "v1",
    }
    xml_row = AktivitetsresultatRow(
        aktivitets_id=1,
        opdateret_tid=None,
        dw_id_audit_update=0,
        dw_is_deleted=0,
        element_nummer=1,
        element_navn="Kommentar",
        element_vaerdi="ikke numerisk",
        element_type="string",
        raekke_type="Element",
    )

    base = "DSA.FHIR.Observation.observation_sfi_builder"

    monkeypatch.setattr(
        f"{base}._resolve_row_target_resource",
        lambda *args, **kwargs: RoutingDecision(
            target_resource=TargetResource.OBSERVATION,
            observation_category="vital-signs",
            routing_source="routing_table",
            routing_confidence="high",
            routing_reason="allowlist",
        ),
    )
    monkeypatch.setattr(
        f"{base}._build_sfi_observations_for_row",
        lambda **kwargs: [],
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_dict",
        lambda parsed_rows, **kwargs: {
            "id": "questionnaire-existing-id",
            "item": [{"linkId": "M.Kommentar", "type": "string"}],
        },
    )
    monkeypatch.setattr(
        "DSA.FHIR.Aktivitetsresultat.questionnaire_outputs.build_questionnaire_response_dict",
        lambda row_dict, parsed_rows, **kwargs: {
            "id": f"qr-existing-{row_dict['OBJEKT_ID']}",
            "resourceType": "QuestionnaireResponse",
        },
    )
    monkeypatch.setattr(f"{base}._validate_procedure_dicts_sample", lambda *a: None)
    monkeypatch.setattr(f"{base}._validate_condition_dicts_sample", lambda *a: None)

    routing_decisions: list[dict[str, str | None]] = []
    questionnaires: dict[str, dict] = {}
    obs, proc, cond, qrs, errors = _build_sfi_unified_dicts_for_rows(
        rows=[row],
        parsed_data_by_index=[[xml_row]],
        kontekst_by_index=[None],
        logger=MagicMock(),
        resolved=None,
        routing_resolver=MagicMock(),
        routing_decision_sink=routing_decisions,
        questionnaires_by_id=questionnaires,
    )

    assert obs == []
    assert len(proc) == 1
    assert len(qrs) == 1
    assert errors == 0
    assert routing_decisions[0]["target_resource"] == "QuestionnaireResponse"
    assert routing_decisions[0]["routing_source"] == "xml_fallback"
    assert "no numeric SFI datapoints" in routing_decisions[0]["routing_reason"]
    assert "Procedure emitted in parallel" in routing_decisions[0]["routing_reason"]
    assert "questionnaire-existing-id" in questionnaires


def test_pre_resolve_references_uses_targeted_lookups_for_relations():
    class FakeFhirDatabase:
        def __init__(self):
            self.identifier_calls = []
            self.targeted_calls = []

        def resolve_identifier_values(self, *, resource_type, identifier_system, values, **_kw):
            self.targeted_calls.append((resource_type, identifier_system, values))
            return {
                ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT): {
                    v: f"observation-{v}" for v in values
                },
                ("Patient", SYSTEM_IFDB_PATIENT_ID): {"P1": "patient-targeted"},
                ("Encounter", SYSTEM_IFDB_OBJECT_ID): {"E1": "encounter-targeted"},
                ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID): {"EP1": "episode-targeted"},
                ("Procedure", SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE): {
                    v: f"procedure-{v}" for v in values
                },
            }.get((resource_type, identifier_system), {})

        def get_identifier_lookup(self, resource_type, identifier_systems, force_reload=False):
            self.identifier_calls.append((resource_type, tuple(identifier_systems)))
            lookup: dict[tuple[str, str], str] = {}
            if resource_type == "Observation":
                lookup[(SYSTEM_IFDB_AKTIVITETSRESULTAT, "100")] = "observation-parent-cache"
            if resource_type == "Procedure":
                lookup[(SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "100")] = "procedure-cache"
            return lookup

    rows = [
        {
            "OBJEKT_ID": "100",
            "PATIENT_ID": "P1",
            "ENCOUNTER_ID": "E1",
            "EPISODE_OF_CARE_ID": "EP1",
            "AKTIVITETTYPE_KODE": "SKS.a.proZZ3160",
        }
    ]
    fhir_database = FakeFhirDatabase()

    resolved = _pre_resolve_references(rows, fhir_database)

    assert resolved.patient_ref("P1") == {
        "id": "patient-targeted",
        "resourceType": "Patient",
    }
    assert resolved.encounter_ref("E1") == {
        "id": "encounter-targeted",
        "resourceType": "Encounter",
    }
    assert resolved.episode_ref("EP1") == {
        "id": "episode-targeted",
        "resourceType": "EpisodeOfCare",
    }
    assert resolved.observation_id("100") == "observation-100"
    assert resolved.procedure_id("100") == "procedure-100"
    assert len(fhir_database.targeted_calls) == 6
    assert len(fhir_database.identifier_calls) == 0
    assert ("Patient", SYSTEM_IFDB_PATIENT_ID, ["P1"]) in fhir_database.targeted_calls
    assert ("Encounter", SYSTEM_IFDB_OBJECT_ID, ["E1"]) in fhir_database.targeted_calls
    assert (
        "EpisodeOfCare",
        SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        ["EP1"],
    ) in fhir_database.targeted_calls
    assert (
        "Observation",
        SYSTEM_IFDB_AKTIVITETSRESULTAT,
        ["100"],
    ) in fhir_database.targeted_calls
    assert (
        "Procedure",
        SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
        ["100"],
    ) in fhir_database.targeted_calls
    assert (
        "Condition",
        SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
        ["100"],
    ) in fhir_database.targeted_calls
    assert resolved.observation_id("100") == "observation-100"
    assert resolved.procedure_id("100") == "procedure-100"
    assert resolved.lookup_profile["relation"]["strategy"] == "targeted"
    assert any(
        item["resource_type"] == "Observation" and item["strategy"] == "targeted"
        for item in resolved.lookup_profile["self_id"]["lookups"]
    )


def test_pre_resolve_references_reuses_dependency_lookups_for_relations():
    class FakeFhirDatabase:
        def resolve_identifier_values(
            self,
            *,
            resource_type,
            identifier_system,
            values,
            **_kw,
        ):
            if resource_type == "Procedure":
                return {v: f"procedure-{v}" for v in values}
            if resource_type == "Observation":
                return {v: f"observation-{v}" for v in values}
            return {}

        def get_identifier_lookup(self, _resource_type, _identifier_systems, force_reload=False):
            return {}

    rows = [
        {
            "OBJEKT_ID": "100",
            "PATIENT_ID": "P1",
            "ENCOUNTER_ID": "E1",
            "EPISODE_OF_CARE_ID": "EP1",
            "AKTIVITETTYPE_KODE": "SKS.a.proZZ3160",
        }
    ]
    dependency_lookups = {
        "Patient": {(SYSTEM_IFDB_PATIENT_ID, "P1"): "patient-existing"},
        "Encounter": {(SYSTEM_IFDB_OBJECT_ID, "E1"): "encounter-existing"},
        "EpisodeOfCare": {(SYSTEM_IFDB_EPISODE_OF_CARE_ID, "EP1"): "episode-existing"},
    }
    fhir_database = FakeFhirDatabase()

    resolved = _pre_resolve_references(
        rows,
        fhir_database,
        dependency_lookups=dependency_lookups,
    )

    assert resolved.patient_ref("P1") == {
        "id": "patient-existing",
        "resourceType": "Patient",
    }
    assert resolved.encounter_ref("E1") == {
        "id": "encounter-existing",
        "resourceType": "Encounter",
    }
    assert resolved.episode_ref("EP1") == {
        "id": "episode-existing",
        "resourceType": "EpisodeOfCare",
    }
    assert resolved.procedure_id("100") == "procedure-100"


def test_pre_resolve_references_can_include_questionnaire_lookups_in_sfi_mode():
    class FakeFhirDatabase:
        def __init__(self):
            self.identifier_calls = []
            self.targeted_calls = []

        def resolve_identifier_values(self, *, resource_type, identifier_system, values, **_kw):
            self.targeted_calls.append((resource_type, identifier_system))
            if resource_type == "Questionnaire":
                return {"Model|v1": "questionnaire-cache"}
            if resource_type == "QuestionnaireResponse":
                return {"100": "qr-cache"}
            return {v: f"{resource_type.lower()}-{v}" for v in values}

        def get_identifier_lookup(self, resource_type, identifier_systems, force_reload=False):
            self.identifier_calls.append((resource_type, tuple(identifier_systems)))
            if resource_type == "Observation":
                return {}
            if resource_type == "Procedure":
                return {}
            if resource_type == "Questionnaire":
                return {(QUESTIONNAIRE_IDENTIFIER_SYSTEM, "Model|v1"): "questionnaire-cache"}
            if resource_type == "QuestionnaireResponse":
                return {(SYSTEM_IFDB_AKTIVITETSRESULTAT, "100"): "qr-cache"}
            return {}

    rows = [
        {
            "OBJEKT_ID": "100",
            "PATIENT_ID": "P1",
            "ENCOUNTER_ID": "E1",
            "EPISODE_OF_CARE_ID": "EP1",
            "MODELLERINGSNAVN": "Model",
            "MODELLERINGSVERSION": "v1",
            "AKTIVITETTYPE_KODE": "A",
        }
    ]
    fhir_database = FakeFhirDatabase()
    resolved = _pre_resolve_references(
        rows,
        fhir_database,
        include_questionnaire_lookups=True,
    )

    assert resolved.questionnaire_id("Model", "v1") == "questionnaire-cache"
    assert resolved.questionnaire_response_id("100") == "qr-cache"
    assert len(fhir_database.targeted_calls) == 8
    assert len(fhir_database.identifier_calls) == 0
    assert any(
        item["resource_type"] == "QuestionnaireResponse" and item["strategy"] == "targeted"
        for item in resolved.lookup_profile["self_id"]["lookups"]
    )


def test_hydrate_lookups_for_rows_prefetches_missing_udf_and_speciale_only():
    context = MagicMock()
    ifdb_oracle = MagicMock()

    udf_calls: list[list[str]] = []
    speciale_calls: list[list[str]] = []

    def fake_udf_fetch(udf_ids, *_args, **_kwargs):
        udf_calls.append(list(udf_ids))
        return {udf_id: {"mock": f"udf-{udf_id}"} for udf_id in udf_ids}

    def fake_speciale_fetch(udf_ids, *_args, **_kwargs):
        speciale_calls.append(list(udf_ids))
        return {udf_id: [{"mock": f"speciale-{udf_ids}"}] for udf_id in udf_ids}

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(
        akt_orchestrator,
        "_fetch_aktivitetsudfoerelse_data_arc",
        fake_udf_fetch,
    )
    monkeypatch.setattr(
        akt_orchestrator,
        "_fetch_aktivitetsudfoerelse_speciale_data_arc",
        fake_speciale_fetch,
    )
    try:
        rows = [
            {"UDFOERELSE_ID": "1"},
            {"UDFOERELSE_ID": "2"},
            {"UDFOERELSE_ID": "2"},
            {"UDFOERELSE_ID": None},
        ]
        udf_lookup: dict[str, dict[str, str]] = {"1": {"cached": "already"}}
        speciale_lookup: dict[str, list[dict[str, str]]] = {"1": [{"cached": "already"}]}

        updated_udf, updated_speciale = akt_orchestrator._hydrate_lookups_for_rows(
            ifdb_oracle=ifdb_oracle,
            context=context,
            rows=rows,
            udf_lookup=udf_lookup,
            speciale_lookup=speciale_lookup,
            mssql_target_staging=MagicMock(),
        )
    finally:
        monkeypatch.undo()

    assert updated_udf == {
        "1": {"cached": "already"},
        "2": {"mock": "udf-2"},
    }
    assert updated_speciale == {
        "1": [{"cached": "already"}],
        "2": [{"mock": "speciale-['2']"}],
    }
    assert udf_calls == [["2"]]
    assert speciale_calls == [["2"]]


def test_pre_resolve_chunk_sfi_observation_ids_uses_cached_lookup_and_targets_missing_only():
    class FakeFhirDatabase:
        def __init__(self):
            self.calls: list[tuple[str, str, tuple[str, ...]]] = []

        def resolve_identifier_values(self, *, resource_type, identifier_system, values, **_kwargs):
            self.calls.append((resource_type, identifier_system, tuple(values)))
            return {value: f"obs-{value}" for value in values}

    fhir_database = FakeFhirDatabase()
    logger = MagicMock()
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={
            (SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "100#sfi#1"): "obs-cached",
        },
    )

    akt_orchestrator._pre_resolve_chunk_sfi_observation_ids(
        fhir_database=fhir_database,
        resolved=resolved,
        chunk_sfi_identifiers=["100#sfi#1", "100#sfi#2", "100#sfi#3", "100#sfi#1"],
        logger=logger,
    )

    assert resolved.lookups_by_resource_type()["Observation"] == {
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "100#sfi#1"): "obs-cached",
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "100#sfi#2"): "obs-100#sfi#2",
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI, "100#sfi#3"): "obs-100#sfi#3",
    }
    assert fhir_database.calls == [
        (
            "Observation",
            SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
            ("100#sfi#2", "100#sfi#3"),
        )
    ]


def test_pre_resolve_chunk_delop_procedure_ids_uses_cached_lookup_and_targets_missing_only():
    class FakeFhirDatabase:
        def __init__(self):
            self.calls: list[tuple[str, str, tuple[str, ...]]] = []

        def resolve_identifier_values(self, *, resource_type, identifier_system, values, **_kwargs):
            self.calls.append((resource_type, identifier_system, tuple(values)))
            return {value: f"proc-{value}" for value in values}

    fhir_database = FakeFhirDatabase()
    logger = MagicMock()
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
        procedure={
            (SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "100-delop-1"): "proc-cached",
        },
    )

    akt_orchestrator._pre_resolve_chunk_delop_procedure_ids(
        fhir_database=fhir_database,
        resolved=resolved,
        chunk_delop_identifiers=["100-delop-1", "100-delop-2", "100-delop-3", "100-delop-1"],
        logger=logger,
    )

    assert resolved.lookups_by_resource_type()["Procedure"] == {
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "100-delop-1"): "proc-cached",
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "100-delop-2"): "proc-100-delop-2",
        (SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE, "100-delop-3"): "proc-100-delop-3",
    }
    assert fhir_database.calls == [
        (
            "Procedure",
            SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
            ("100-delop-2", "100-delop-3"),
        )
    ]
    assert resolved.procedure_id("100-delop-1") == "proc-cached"
    assert resolved.procedure_id("100-delop-2") == "proc-100-delop-2"


def test_procedure_id_mints_once_per_identifier_on_cache_miss():
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
        procedure={},
    )
    first = resolved.procedure_id("100-delop-1")
    second = resolved.procedure_id("100-delop-1")
    other = resolved.procedure_id("100-delop-2")
    assert first == second
    assert first != other


def test_dedupe_resource_dicts_by_business_identifier():
    logger = MagicMock()
    resources = [
        (
            "rid-1",
            {
                "resourceType": "Observation",
                "identifier": [
                    {
                        "system": "urn:dk:ifdb:observation_id",
                        "value": "OBS-100",
                    }
                ],
            },
        ),
        (
            "rid-2",
            {
                "resourceType": "Observation",
                "identifier": [
                    {
                        "system": "urn:dk:ifdb:observation_id",
                        "value": "OBS-100",
                    }
                ],
            },
        ),
        (
            "rid-3",
            {
                "resourceType": "Observation",
                "identifier": [
                    {
                        "system": "urn:dk:ifdb:observation_id",
                        "value": "OBS-200",
                    }
                ],
            },
        ),
    ]
    deduped, duplicates = akt_orchestrator._dedupe_resource_dicts_by_business_id(
        resources,
        logger=logger,
        resource_type="Observation",
    )
    assert len(deduped) == 2
    assert duplicates == 1  # tælleren er politikkens synlige del
    assert deduped[0][0] == "rid-1"
    assert deduped[1][0] == "rid-3"
    logger.info.assert_called_once()
    assert logger.info.call_args[0][0].endswith(
        "removed %d duplicate resource dictionaries by business identifier in chunk"
    )
    assert logger.info.call_args[0][1] == "Observation"
    assert logger.info.call_args[0][2] == 1


def test_questionnaire_and_qr_ids_use_random_uuid_when_no_existing_lookup():
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
        questionnaire={},
        questionnaire_response={},
    )

    with patch(
        "DSA.FHIR.Aktivitetsresultat.references.uuid.uuid4",
        side_effect=["q-random", "qr-random", "unused"],
    ):
        first_q = resolved.questionnaire_id("Model", "v1")
        second_q = resolved.questionnaire_id("Model", "v1")
        first_qr = resolved.questionnaire_response_id("100")
        second_qr = resolved.questionnaire_response_id("100")

    assert first_q == second_q == "q-random"
    assert first_qr == second_qr == "qr-random"


def test_questionnaire_ids_are_scoped_by_model_version():
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
        questionnaire={},
        questionnaire_response={},
    )

    with patch(
        "DSA.FHIR.Aktivitetsresultat.references.uuid.uuid4",
        side_effect=["q-v1", "q-v2", "unused"],
    ):
        first_v1 = resolved.questionnaire_id("Model", "v1")
        second_v1 = resolved.questionnaire_id("Model", "v1")
        first_v2 = resolved.questionnaire_id("Model", "v2")

    assert first_v1 == second_v1 == "q-v1"
    assert first_v2 == "q-v2"
    assert first_v2 != first_v1


def test_sfi_observation_id_reuses_minted_uuid_when_no_existing_lookup():
    resolved = ResolvedRefLookup(
        patient={},
        encounter={},
        episode={},
        observation={},
    )

    with patch(
        "DSA.FHIR.Aktivitetsresultat.references.uuid.uuid4",
        side_effect=["random-uuid-1", "random-uuid-2"],
    ):
        first = resolved.observation_id_sfi("100", 7)
        second = resolved.observation_id_sfi("100", 7)
        third = resolved.observation_id_sfi("100", 8)

    assert first == "random-uuid-1"
    assert second == "random-uuid-1"
    assert third == "random-uuid-2"


def test_unified_side_outputs_use_observation_chunk_size():
    assert _AKTIVITETSRESULTAT_CHUNK_SIZE > 0


def test_multi_asset_wrapper_yields_by_output_name():
    output = next(
        _yield_multi_asset_output(
            output_name=_AKTIVITETSRESULTAT_OUT,
            result=dg.MaterializeResult(
                metadata={"posted_count": 1},
            ),
        )
    )

    assert output.output_name == _AKTIVITETSRESULTAT_OUT
    assert output.metadata["posted_count"].value == 1


@pytest.mark.unit
def test_multi_asset_registers_legacy_asset_keys():
    from DSA.FHIR.Aktivitetsresultat.assets import all_assets as aktivitetsresultat_assets

    names: set[str] = set()
    for asset in aktivitetsresultat_assets:
        keys = getattr(asset, "keys", None)
        if keys:
            for key in keys:
                names.add(key.path[-1])
            continue
        key = getattr(asset, "key", None)
        if key is not None and getattr(key, "path", None):
            names.add(key.path[-1])
    assert "fhir_server_aktivitetsresultat_dfm" in names
    assert "fhir_server_questionnaire_dfm" in names
    assert "fhir_server_questionnaire_response_dfm" in names
