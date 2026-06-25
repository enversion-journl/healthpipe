# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for AKTIVITETSRESULTAT -> Condition extraction.

Verifies:
- is_sks_diagnosis_code() classification logic
- _build_aktivitetsresultat_condition_dict() produces valid Condition dicts
"""

import pytest
from DSA.FHIR.Condition.condition_aktivitetsresultat_builder import (
    _append_xml_diagnosis_condition_dicts,
    _build_aktivitetsresultat_condition_dict,
    _build_xml_diagnosis_condition_dict,
)
from DSA.FHIR.utils.code_normalization import (
    is_sks_diagnosis_code,
    load_ifdb_diagnosis_code_rules,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


class TestIsSKSDiagnosisCode:
    """Test SKS diagnosis code classification."""

    @pytest.mark.parametrize(
        "code",
        [
            "SKS.a.diaDA01",
            "SKS.a.diaI10",
        ],
    )
    def test_diagnosis_codes(self, code: str) -> None:
        assert is_sks_diagnosis_code(code) is True

    @pytest.mark.parametrize(
        "code",
        [
            "SKS.a.oprKABC10",
            "SKS.a.proZXY01",
            "SKS.a.undZZ3170",
            "KPYdelser.Afdelingsydelser.X",
            "BomKS.a.BomYdelsesTyper.Ambulant",
            "DA01",
            "",
            None,
        ],
    )
    def test_non_diagnosis_codes(self, code: str | None) -> None:
        assert is_sks_diagnosis_code(code) is False

    def test_diagnosis_rules_can_add_exact_bare_codes(self, tmp_path) -> None:
        rules_path = tmp_path / "ifdb_diagnosis_code_rules.yml"
        rules_path.write_text(
            """
diagnosis_code_rules:
  exact:
    - "DA01"
  prefixes:
    - "SKS.a.dia"
  bare_regex: []
  exclude_exact: []
  exclude_prefixes:
    - "KPYdelser."
    - "BomKS."
""",
            encoding="utf-8",
        )

        load_ifdb_diagnosis_code_rules.cache_clear()
        try:
            assert is_sks_diagnosis_code("DA01", rules_path=rules_path) is True
            assert is_sks_diagnosis_code("KPYdelser.X", rules_path=rules_path) is False
        finally:
            load_ifdb_diagnosis_code_rules.cache_clear()


def _make_row(**overrides: object) -> dict:
    row = {
        "OBJEKT_ID": "12345",
        "PATIENT_ID": "PAT-001",
        "ENCOUNTER_ID": "ENC-001",
        "EPISODE_OF_CARE_ID": "EOC-001",
        "AKTIVITETTYPE_KODE": "SKS.a.diaDA01",
        "AKTIVITETTYPE_KODETEKST": "Hypertension",
        "PERIOD_START": "2024-06-15T10:00:00",
        "PERIOD_END": None,
        "SLETTET": None,
        "MODTIDSPUNKT": "2024-06-15T12:00:00",
        "UDFOERT_BRUGERROLLE": "Læge",
        "UDFOERT_BRUGERROLLE_NAME": "Overlæge",
        "UDFOERT_ADMENHED_KODE": "4202011",
        "UDFOERT_ADMENHED_NAVN": "Medicinsk Afd.",
        "UDFOERT_ADMENHED_ORGKODE": "ORG-001",
        "UDFOERT_ADMENHED_SORCODE": "SOR-001",
    }
    row.update(overrides)
    return row


class TestBuildConditionDict:
    """Test Condition dict builder."""

    def test_basic_structure(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert cond["resourceType"] == "Condition"
        assert cond["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION
        assert cond["identifier"][0]["value"] == "12345"
        assert cond["clinicalStatus"]["coding"][0]["code"] == "active"
        assert cond["verificationStatus"]["coding"][0]["code"] == "confirmed"

    def test_code_mapping(self) -> None:
        # SKS.a.diaI10 -> bare ICD-10 "I10" -> canonical SKS-D "DI10".
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.diaI10")
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert cond["code"]["coding"][0]["system"] == SYSTEM_SKS
        assert cond["code"]["coding"][0]["code"] == "DI10"
        assert cond["code"]["coding"][0]["display"] == "Hypertension"
        assert cond["code"]["text"] == "Hypertension"

    def test_subject_reference(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert cond["subject"]["identifier"]["system"] == SYSTEM_IFDB_PATIENT_ID
        assert cond["subject"]["identifier"]["value"] == "PAT-001"

    def test_encounter_reference(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert cond["encounter"]["identifier"]["system"] == SYSTEM_IFDB_OBJECT_ID
        assert cond["encounter"]["identifier"]["value"] == "ENC-001"

    def test_onset_datetime(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert "onsetDateTime" in cond
        assert "abatementDateTime" not in cond

    def test_resolved_status_when_period_end(self) -> None:
        row = _make_row(PERIOD_END="2024-06-20T10:00:00")
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert cond["clinicalStatus"]["coding"][0]["code"] == "resolved"
        assert cond["verificationStatus"]["coding"][0]["code"] == "confirmed"
        assert "abatementDateTime" in cond

    def test_deleted_status(self) -> None:
        row = _make_row(SLETTET="1")
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        assert "clinicalStatus" not in cond
        assert cond["verificationStatus"]["coding"][0]["code"] == "entered-in-error"
        is_deleted = [
            e for e in cond.get("extension", []) if e.get("url") == EXTENSION_FHIR_IS_DELETED
        ]
        assert len(is_deleted) == 1
        assert is_deleted[0].get("valueInteger") == 1

    def test_source_meta_extension(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        source_ext = [e for e in cond["extension"] if "ConditionSourceMeta" in e["url"]]
        assert len(source_ext) == 1
        sub_urls = {s["url"] for s in source_ext[0]["extension"]}
        assert "udfoertBrugerrolle" in sub_urls
        assert "modtidspunkt" in sub_urls

    def test_episode_of_care_extension(self) -> None:
        row = _make_row()
        _, cond = _build_aktivitetsresultat_condition_dict(row)

        eoc_ext = [e for e in cond["extension"] if "episodeOfCare" in e["url"]]
        assert len(eoc_ext) == 1


def _make_xml_row(**overrides: object) -> AktivitetsresultatRow:
    base = {
        "aktivitets_id": 42,
        "opdateret_tid": None,
        "dw_id_audit_update": 0,
        "dw_is_deleted": 0,
        "element_nummer": 2,
        "element_navn": "DiagnoseKode",
        "element_ledetekst": "ICD-10 kode",
        "element_vaerdi": "I10",
        "element_kode": "I10",
        "element_element_id": "E020",
        "raekke_type": "Element",
        "xml_status": 0,
    }
    base.update(overrides)
    return AktivitetsresultatRow(**base)


class TestXmlDiagnosisCondition:
    def test_xml_condition_builder(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="A01")
        xml_row = _make_xml_row()
        _, cond = _build_xml_diagnosis_condition_dict(row, xml_row, "I10")

        assert cond["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION
        assert cond["identifier"][0]["value"] == "12345/2"
        # Bare ICD-10 "I10" is canonicalised to SKS-D "DI10".
        assert cond["code"]["coding"][0]["code"] == "DI10"
        assert cond["code"]["coding"][0]["system"] == SYSTEM_SKS

    def test_append_xml_diagnosis_skips_error_rows(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="A01")
        xml_rows = [
            _make_xml_row(
                xml_status=-1,
                raekke_type=None,
                element_kode="I10",
                element_vaerdi="I10",
            ),
            _make_xml_row(element_nummer=3, element_kode="J44", element_vaerdi="J44"),
        ]
        sink: list[tuple[str, dict]] = []
        _append_xml_diagnosis_condition_dicts(row, xml_rows, None, sink)

        assert len(sink) == 1
        assert sink[0][1]["code"]["coding"][0]["code"] == "DJ44"

    def test_append_xml_diagnosis_skips_schema_index_codes(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="A01")
        xml_rows = [
            _make_xml_row(element_kode="D", element_vaerdi="Fødselslæsion"),
            _make_xml_row(element_nummer=3, element_kode="I10", element_vaerdi="I10"),
        ]
        sink: list[tuple[str, dict]] = []
        _append_xml_diagnosis_condition_dicts(row, xml_rows, None, sink)

        assert len(sink) == 1
        assert sink[0][1]["code"]["coding"][0]["code"] == "DI10"


_XML_WITH_DIAGNOSIS = """<?xml version="1.0"?>
<Afdelingsydelsesresultat>
  <Overskrift>Diagnose</Overskrift>
  <Klynge type="selektion">
    <Navn>DiagnoseValg</Navn>
    <Ledetekst>Vælg diagnose</Ledetekst>
    <Element type="coded">
      <Navn>DiagnoseKode</Navn>
      <Ledetekst>ICD-10</Ledetekst>
      <Vaerdi>I10</Vaerdi>
      <Kode>I10</Kode>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>"""

_XML_WITHOUT_DIAGNOSIS = """<?xml version="1.0"?>
<Afdelingsydelsesresultat>
  <Overskrift>Notat</Overskrift>
  <Klynge type="sekvens">
    <Navn>NotatGruppe</Navn>
    <Ledetekst>Notat</Ledetekst>
    <Element type="string">
      <Navn>Fritekst</Navn>
      <Ledetekst>Notat</Ledetekst>
      <Vaerdi>Ingen diagnose her</Vaerdi>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>"""


class TestCodeLevelSuppressionWhenXmlDiagnosis:
    """One diagnosis must never become two Condition resources.

    When a dia*-row also carries structured DiagnoseKode XML elements, the
    XML-level Conditions are the clinical source of truth and the coarser
    code-level Condition for the same OBJEKT_ID is suppressed.
    """

    def _transform(self, row: dict) -> list[tuple[str, dict]]:
        from DSA.FHIR.Aktivitetsresultat import orchestrator as akt_orchestrator

        _obs, _procs, conds = akt_orchestrator._transform_aktivitetsresultat_to_fhir_dicts(
            [row], context=None
        )
        return conds

    def test_code_level_suppressed_when_xml_diagnosis_present(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.diaDA01")
        row["XML"] = _XML_WITH_DIAGNOSIS
        row["KLADDE"] = 0

        conds = self._transform(row)

        assert len(conds) == 1
        assert (
            conds[0][1]["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION
        )
        assert conds[0][1]["code"]["coding"][0]["code"] == "DI10"

    def test_code_level_emitted_when_no_xml_diagnosis(self) -> None:
        row = _make_row(AKTIVITETTYPE_KODE="SKS.a.diaDA01")
        row["XML"] = _XML_WITHOUT_DIAGNOSIS
        row["KLADDE"] = 0

        conds = self._transform(row)

        assert len(conds) == 1
        assert conds[0][1]["identifier"][0]["system"] == SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION
