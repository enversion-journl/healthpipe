# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the IFDB AKTIVITETSRESULTAT Questionnaire builder.

These tests do not depend on a live database; they exercise the pure builder
API exposed by ``DSA.FHIR.Questionnaire``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import (
    QUESTIONNAIRE_CANONICAL_PREFIX,
    QUESTIONNAIRE_IDENTIFIER_SYSTEM,
    build_questionnaire_canonical,
    build_questionnaire_dict,
    build_questionnaire_from_xml,
    build_questionnaire_identifier_value,
    build_questionnaire_version_pinned_reference,
)

pytestmark = pytest.mark.unit

SFI_SYSTEM = "urn:dk:ifdb:sfi"

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "ifdb_sfi" / "xml"


def _read_fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text()


# ---------------------------------------------------------------------------
# Canonical / id helpers
# ---------------------------------------------------------------------------


def test_canonical_url_uses_safe_token():
    assert build_questionnaire_canonical("RM_Generelle") == (
        f"{QUESTIONNAIRE_CANONICAL_PREFIX}:RM_Generelle"
    )
    # Whitespace and disallowed characters are normalised so the URL stays safe.
    assert build_questionnaire_canonical("RM Generelle / v2") == (
        f"{QUESTIONNAIRE_CANONICAL_PREFIX}:RM-Generelle-v2"
    )


def test_canonical_url_for_missing_model_name_uses_placeholder():
    assert build_questionnaire_canonical(None) == (f"{QUESTIONNAIRE_CANONICAL_PREFIX}:unspecified")


def test_identifier_value_stable():
    assert (
        build_questionnaire_identifier_value("RM_Generelle", "2026-05-01")
        == "RM_Generelle|2026-05-01"
    )


def test_version_pinned_reference_requires_version():
    assert (
        build_questionnaire_version_pinned_reference("RM_Generelle", "2026-05-01")
        == "urn:dk:ifdb:questionnaire:RM_Generelle|2026-05-01"
    )
    with pytest.raises(ValueError):
        build_questionnaire_version_pinned_reference("RM_Generelle", None)
    with pytest.raises(ValueError):
        build_questionnaire_version_pinned_reference("RM_Generelle", "")


# ---------------------------------------------------------------------------
# build_questionnaire_dict
# ---------------------------------------------------------------------------


def test_build_questionnaire_requires_model_version():
    with pytest.raises(ValueError):
        build_questionnaire_dict([], model_name="RM_Generelle", model_version=None)


def test_build_questionnaire_simple_repetition_string():
    """case_03: one Repetition wrapping one string Element -> one Q.item."""
    xml = _read_fixture("case_03_qr_simpel_aktuelt.xml")
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=3001,
        model_name="RM_Generelle",
        model_version="2026-05-01",
        overskrift="Aktuelt",
    )

    assert q["resourceType"] == "Questionnaire"
    assert q["id"]
    assert not q["id"].startswith("ifdb-q-")
    assert q["url"] == "urn:dk:ifdb:questionnaire:RM_Generelle"
    assert q["version"] == "2026-05-01"
    assert q["status"] == "active"
    assert q["identifier"][0]["system"] == QUESTIONNAIRE_IDENTIFIER_SYSTEM
    assert q["identifier"][0]["value"] == "RM_Generelle|2026-05-01"
    assert q["title"] == "Aktuelt"

    items = q["item"]
    assert len(items) == 1
    item = items[0]
    assert item["id"].startswith("e-")
    # SFI codes are qualified by MODELLERINGSNAVN.
    assert item["linkId"] == "RM_Generelle.RM_Generelle:edBeskrivelse"
    assert item["type"] == "string"
    assert item["text"] == "Beskrivelse"
    coding = item["code"][0]
    assert coding["system"] == SFI_SYSTEM
    assert coding["code"] == "RM_Generelle.RM_Generelle:edBeskrivelse"
    assert item.get("repeats") is True  # nested in <Repetition>


def test_questionnaire_klassificering_maps_to_choice_type():
    """case_05: klassificering elements become choice items."""
    xml = _read_fixture("case_05_qr_gcs_total.xml")
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=5005,
        model_name="RM_Generelle",
        model_version="2026-05-01",
    )

    by_link = {it["linkId"]: it for it in q["item"]}

    # Three klassificering elements + one integer total score = 4 unique SFI items
    # (codes qualified by MODELLERINGSNAVN).
    assert set(by_link) == {
        "RM_Generelle.RM_Generelle:edGCSPatientensOejne",
        "RM_Generelle.RM_Generelle:edGCSPatientensVerbaleRespons",
        "RM_Generelle.RM_Generelle:edGCSPatientensMotoriskeRespons",
        "RM_Generelle.RM_Generelle:edGCSTotalScore",
    }

    eyes = by_link["RM_Generelle.RM_Generelle:edGCSPatientensOejne"]
    assert eyes["type"] == "choice"
    assert eyes["code"][0]["code"] == "RM_Generelle.RM_Generelle:edGCSPatientensOejne"

    # type="long" maps to integer per the SFI datatype mapping.
    total = by_link["RM_Generelle.RM_Generelle:edGCSTotalScore"]
    assert total["type"] == "integer"


def test_questionnaire_klynge_element_is_flat_item():
    """Klynge Element children appear as flat top-level Q.items.

    Klynge/hierarchy context lives in the SFI CodeSystem properties, not in
    nested Questionnaire.item groups. The Element is the answerable datapoint.
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
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=4004,
        model_name="RM_Generelle2",
        model_version="2026-05-01",
    )

    root_items = q["item"]
    assert len(root_items) == 1
    item = root_items[0]
    assert item["linkId"] == "RM_Generelle2.RM_Generelle2:edSO16ErnaerAppetit"
    assert item["type"] == "string"
    assert item["text"] == "Supplerende"
    assert "item" not in item, "Flat items must not have nested children"
    assert item["code"][0]["system"] == SFI_SYSTEM


def test_questionnaire_selektion_is_answerable_choice_item():
    """Selektion becomes a flat choice item. Child Elements are also flat items."""
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
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=21,
        model_name="DiagnoseModel",
        model_version="v1",
    )

    by_link = {it["linkId"]: it for it in q["item"]}
    assert len(by_link) == 2

    selection = by_link["DiagnoseModel._fb_DiagnoseValg"]
    assert selection["type"] == "choice"
    assert selection["code"][0]["system"] == SFI_SYSTEM
    assert selection["code"][0]["code"] == "DiagnoseModel._fb_DiagnoseValg"
    assert "item" not in selection, "Flat items must not have nested children"

    child = by_link["DiagnoseModel.E020"]
    assert child["type"] == "string"
    assert "item" not in child


def test_questionnaire_preserves_repeated_sfi_codes_with_distinct_item_ids():
    """Two repetitions of the same SFI code remain separate Q.items."""
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

    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=42,
        model_name="Maalinger",
        model_version="v1",
    )
    link_ids = [it["linkId"] for it in q["item"]]
    repeated = [it for it in q["item"] if it["linkId"] == "Maalinger.Mal:BT_SYS"]
    assert link_ids.count("Maalinger.Mal:BT_SYS") == 2
    assert len({it["id"] for it in repeated}) == 2


def test_questionnaire_uses_native_structure_without_extensions():
    xml = _read_fixture("case_05_qr_gcs_total.xml")
    q = build_questionnaire_from_xml(
        xml_string=xml,
        aktivitets_id=5005,
        model_name="RM_Generelle",
        model_version="2026-05-01",
    )
    for item in q["item"]:
        assert "extension" not in item


def test_questionnaire_accepts_existing_resource_id_from_lookup():
    q = build_questionnaire_dict(
        [],
        model_name="RM_Generelle",
        model_version="2026-05-01",
        resource_id="existing-aidbox-id",
    )
    assert q["id"] == "existing-aidbox-id"
