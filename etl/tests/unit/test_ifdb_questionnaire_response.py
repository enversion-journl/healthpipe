# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the IFDB AKTIVITETSRESULTAT QuestionnaireResponse builder."""

from __future__ import annotations

from pathlib import Path

import pytest
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import build_questionnaire_from_xml
from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB import (
    build_qr_identifier_value,
    build_questionnaire_response_dict,
    build_questionnaire_response_from_xml,
)
from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB_assets import (
    merge_flat_questionnaire_items,
)

pytestmark = pytest.mark.unit

SFI_SYSTEM = "urn:dk:ifdb:sfi"

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "ifdb_sfi" / "xml"


def _read_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


def _row_for(xml: str, **overrides) -> dict:
    base = {
        "OBJEKT_ID": "12345",
        "PATIENT_ID": "P-1",
        "ENCOUNTER_ID": "E-1",
        "MODELLERINGSNAVN": "RM_Generelle",
        "MODELLERINGSVERSION": "2026-05-01",
        "OVERSKRIFT": "Aktuelt",
        "PERIOD_START": "2026-05-01T16:22:00",
        "KLADDE": 0,
        "SLETTET": 0,
        "XML": xml,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Header / status / version-pin
# ---------------------------------------------------------------------------


def test_qr_header_version_pinned_to_questionnaire():
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    qr = build_questionnaire_response_from_xml(_row_for(xml))
    assert qr["resourceType"] == "QuestionnaireResponse"
    assert qr["questionnaire"] == ("urn:dk:ifdb:questionnaire:RM_Generelle|2026-05-01")
    assert qr["status"] == "completed"
    assert qr["id"]
    assert not qr["id"].startswith("ifdb-qr-")
    assert qr["identifier"][0]["system"] == "urn:source:ifdb:aktivitetsresultat"
    assert qr["identifier"][0]["value"] == "12345"
    assert qr["subject"]["identifier"]["value"] == "P-1"
    assert qr["encounter"]["identifier"]["value"] == "E-1"
    assert qr["authored"].startswith("2026-05-01T16:22")


def test_qr_status_maps_kladde_and_slettet():
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    draft = build_questionnaire_response_from_xml(_row_for(xml, KLADDE=1))
    assert draft["status"] == "in-progress"

    deleted = build_questionnaire_response_from_xml(_row_for(xml, SLETTET=1))
    assert deleted["status"] == "entered-in-error"


def test_qr_baerer_sletmarkeringen_som_udvidelse():
    # De flade tabellers sletteflag-kolonner laeser udvidelsen urn:fhir:isDeleted.
    # Skemasvar var den eneste af de fire ressourcetyper, der ikke skrev den, saa
    # en kolonne paa skemasvar-tabellen ville staa tom for evigt.
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")

    def _flag(qr: dict) -> int | None:
        for ext in qr.get("extension", []):
            if ext.get("url") == "urn:fhir:isDeleted":
                return ext.get("valueInteger")
        return None

    assert _flag(build_questionnaire_response_from_xml(_row_for(xml))) == 0
    slettet = build_questionnaire_response_from_xml(_row_for(xml, SLETTET=1))
    assert _flag(slettet) == 1
    # Flag og status maa aldrig modsige hinanden.
    assert slettet["status"] == "entered-in-error"
    # En kladde er ikke slettet.
    assert _flag(build_questionnaire_response_from_xml(_row_for(xml, KLADDE=1))) == 0


def test_qr_missing_model_version_raises():
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    row = _row_for(xml, MODELLERINGSVERSION=None)
    with pytest.raises(ValueError):
        build_questionnaire_response_from_xml(row)


def test_qr_identifier_value_is_source_objekt_id():
    assert build_qr_identifier_value("12345") == "12345"
    assert build_qr_identifier_value(99) == "99"
    assert build_qr_identifier_value(None) is None


# ---------------------------------------------------------------------------
# Item linking (QR.item.linkId -> Q.item.linkId)
# ---------------------------------------------------------------------------


def test_qr_linkids_match_questionnaire_linkids():
    """The QR/Q item pairing contract."""
    xml = _read_fixture("case_05_qr_gcs_total.xml")
    row = _row_for(
        xml,
        OBJEKT_ID="55555",
        MODELLERINGSNAVN="RM_Generelle",
        MODELLERINGSVERSION="2026-05-01",
    )
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=55555,
        model_name="RM_Generelle",
        model_version="2026-05-01",
    )
    qr = build_questionnaire_response_from_xml(row)

    q_link_ids = {it["linkId"] for it in q["item"]}
    qr_link_ids = {it["linkId"] for it in qr["item"]}
    q_item_keys = {(it["linkId"], it["id"]) for it in q["item"]}
    qr_item_keys = {(it["linkId"], it["id"]) for it in qr["item"]}

    # Every linkId used in QR must resolve back to a Q.item linkId.
    assert qr_link_ids.issubset(q_link_ids), (q_link_ids, qr_link_ids)
    # For repeated SFI codes, the native item id is the occurrence-level join key.
    assert qr_item_keys.issubset(q_item_keys), (q_item_keys, qr_item_keys)


def test_merge_flat_questionnaire_items_preserves_distinct_composite_keys():
    existing = [{"id": "e-1", "linkId": "Model.A", "type": "string"}]
    incoming = [
        {"id": "e-1", "linkId": "Model.A", "type": "string"},
        {"id": "e-2", "linkId": "Model.A", "type": "string"},
        {"id": "e-1", "linkId": "Model.B", "type": "integer"},
    ]

    merge_flat_questionnaire_items(existing, incoming)

    assert [(item["linkId"], item["id"]) for item in existing] == [
        ("Model.A", "e-1"),
        ("Model.A", "e-2"),
        ("Model.B", "e-1"),
    ]


def test_qr_value_typing_for_integer_total_score():
    xml = _read_fixture("case_05_qr_gcs_total.xml")
    qr = build_questionnaire_response_from_xml(_row_for(xml, OBJEKT_ID="55555"))

    total = [it for it in qr["item"] if it["linkId"] == "RM_Generelle.RM_Generelle:edGCSTotalScore"]
    assert len(total) == 1
    answers = total[0]["answer"]
    assert len(answers) == 1
    assert "valueInteger" in answers[0]
    assert answers[0]["valueInteger"] == 15


def test_qr_value_typing_for_klassificering_uses_coding():
    xml = _read_fixture("case_05_qr_gcs_total.xml")
    qr = build_questionnaire_response_from_xml(_row_for(xml, OBJEKT_ID="55555"))

    eyes = [
        it for it in qr["item"] if it["linkId"] == "RM_Generelle.RM_Generelle:edGCSPatientensOejne"
    ]
    assert len(eyes) == 1
    answer = eyes[0]["answer"][0]
    assert "valueCoding" in answer
    coding = answer["valueCoding"]
    assert coding["code"] == "GCSOejne0010"
    assert coding["system"] == SFI_SYSTEM
    # display strips the leading "4: " prefix.
    assert coding["display"] == "Spontant"


def test_qr_item_uses_native_id_for_occurrence_key_without_extensions():
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    qr = build_questionnaire_response_from_xml(_row_for(xml))
    item = qr["item"][0]
    assert item["id"].startswith("e-")
    assert item["linkId"] == "RM_Generelle.RM_Generelle:edBeskrivelse"
    for ext in item.get("extension", []):
        assert ext["url"].endswith(
            "sfiInstanceMeta"
        ), f"Only sfiInstanceMeta extensions expected on QR items, got: {ext['url']}"


# ---------------------------------------------------------------------------
# Repetition and klynge preservation
# ---------------------------------------------------------------------------


def test_qr_emits_one_item_per_repetition_for_same_sfi():
    """QR.item is NOT deduped: each Element row produces one QR.item."""
    xml = """<?xml version="1.0" ?>
    <Afdelingsydelsesresultat>
      <Repetition>
        <Element type="long">
          <Navn>BT_SYS</Navn>
          <Vaerdi>140</Vaerdi>
          <ElementId>Mal:BT_SYS</ElementId>
        </Element>
      </Repetition>
      <Repetition>
        <Element type="long">
          <Navn>BT_SYS</Navn>
          <Vaerdi>148</Vaerdi>
          <ElementId>Mal:BT_SYS</ElementId>
        </Element>
      </Repetition>
    </Afdelingsydelsesresultat>"""

    qr = build_questionnaire_response_from_xml(
        _row_for(xml, MODELLERINGSNAVN="Maalinger", MODELLERINGSVERSION="v1"),
    )

    bt_items = [it for it in qr["item"] if it["linkId"] == "Maalinger.Mal:BT_SYS"]
    assert len(bt_items) == 2
    assert len({it["id"] for it in bt_items}) == 2
    values = sorted(it["answer"][0]["valueInteger"] for it in bt_items)
    assert values == [140, 148]


def test_qr_klynge_element_is_flat_item():
    """Klynge Element children appear as flat top-level QR.items.

    Klynge/hierarchy context lives in the SFI CodeSystem properties, not in
    nested QR.item groups. Each Element is a separate flat QR.item.
    """
    xml = """<?xml version="1.0" ?>
    <Afdelingsydelsesresultat>
      <Klynge type="sekvens">
        <Navn>SO16ErnaerAppetitSekv</Navn>
        <Ledetekst>Appetit</Ledetekst>
        <Element type="string">
          <Navn>SO16ErnaerAppetit</Navn>
          <Ledetekst>Supplerende</Ledetekst>
          <Vaerdi>Spiser dårligt</Vaerdi>
          <ElementId>RM_Generelle2:edSO16ErnaerAppetit</ElementId>
        </Element>
      </Klynge>
    </Afdelingsydelsesresultat>"""

    qr = build_questionnaire_response_from_xml(
        _row_for(
            xml,
            MODELLERINGSNAVN="RM_Generelle2",
            MODELLERINGSVERSION="2026-05-01",
        )
    )

    leaf_link = "RM_Generelle2.RM_Generelle2:edSO16ErnaerAppetit"
    root_link_ids = {it["linkId"] for it in qr["item"]}
    assert leaf_link in root_link_ids, "Leaf must be a flat top-level item"

    leaf = [it for it in qr["item"] if it["linkId"] == leaf_link]
    assert len(leaf) == 1
    assert leaf[0]["id"].startswith("e-")
    assert "item" not in leaf[0], "Flat items must not have nested children"
    for ext in leaf[0].get("extension", []):
        assert ext["url"].endswith(
            "sfiInstanceMeta"
        ), f"Only sfiInstanceMeta extensions expected, got: {ext['url']}"
    assert leaf[0]["answer"][0]["valueString"] == "Spiser dårligt"


def test_qr_selektion_and_children_are_flat_items():
    """Selektion and its child Elements are all flat top-level QR.items."""
    xml = """<?xml version="1.0" ?>
    <Afdelingsydelsesresultat>
      <Klynge type="selektion">
        <Navn>DiagnoseValg</Navn>
        <Ledetekst>Vælg diagnose</Ledetekst>
        <ValgLedetekst>Valgt: Hypertension</ValgLedetekst>
        <Element type="string">
          <Navn>DiagnoseKode</Navn>
          <Ledetekst>ICD-10 kode</Ledetekst>
          <Vaerdi>I10</Vaerdi>
          <ElementId>E020</ElementId>
          <Kode>I10</Kode>
        </Element>
      </Klynge>
    </Afdelingsydelsesresultat>"""

    qr = build_questionnaire_response_from_xml(
        _row_for(
            xml,
            MODELLERINGSNAVN="DiagnoseModel",
            MODELLERINGSVERSION="v1",
        )
    )

    by_link = {it["linkId"]: it for it in qr["item"]}
    assert len(by_link) == 2

    selection = by_link["DiagnoseModel._fb_DiagnoseValg"]
    assert selection["id"].startswith("g-")
    assert selection["answer"][0]["valueCoding"]["code"] == "DiagnoseValg"
    assert selection["answer"][0]["valueCoding"]["display"] == "Hypertension"
    assert "item" not in selection, "Flat items must not have nested children"
    for ext in selection.get("extension", []):
        assert ext["url"].endswith(
            "sfiInstanceMeta"
        ), f"Only sfiInstanceMeta extensions expected, got: {ext['url']}"

    child = by_link["DiagnoseModel.E020"]
    assert child["id"].startswith("e-")
    assert child["answer"][0]["valueString"] == "I10"
    assert "item" not in child


# ---------------------------------------------------------------------------
# Determinism
# ---------------------------------------------------------------------------


def test_qr_build_preserves_stable_content_except_resource_id():
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    a = build_questionnaire_response_from_xml(_row_for(xml))
    b = build_questionnaire_response_from_xml(_row_for(xml))
    assert a.pop("id") != b.pop("id")
    assert a == b


# ---------------------------------------------------------------------------
# build_questionnaire_response_dict with pre-parsed rows
# ---------------------------------------------------------------------------


def test_qr_dict_accepts_pre_parsed_rows_for_perf_reuse():
    """XML should be parsed once and shared across builders."""
    from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
        parse_aktivitetsresultat_xml,
    )

    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    rows, kontekst = parse_aktivitetsresultat_xml(xml_string=xml, aktivitets_id=999)
    row_dict = _row_for(xml, OBJEKT_ID="999")
    qr = build_questionnaire_response_dict(
        row_dict,
        rows,
        kontekst=kontekst,
        model_name="RM_Generelle",
        model_version="2026-05-01",
        resource_id="existing-aidbox-qr-id",
    )
    assert qr["id"] == "existing-aidbox-qr-id"
    assert qr["item"][0]["linkId"] == "RM_Generelle.RM_Generelle:edBeskrivelse"
