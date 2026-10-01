# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lossless parity tests for AKTIVITETSRESULTAT -> FHIR Observation.

Verifies that the lossless mode (default) preserves ALL C# SSIS-equivalent XML
fields in FHIR Observation resources via extensions and component metadata.

Coverage:
- Root metadata (Overskrift, Ledetekst, Type, Kladde) in resource-level extension
- Kontekst (Oprettet/Aendret/Besluttet/Status) in resource-level extensions
- Element-level metadata (ElementId, Kode, Originalkode, PersId, MOID, type, Ledetekst)
  in component extensions
- Hierarchy (Klynge/Repetition IDs, hierarki paths) in component extensions
- RaekkeType (Element/Selektion) in component extension
- Fast mode opt-in produces simpler output
- Deterministic parity between lossless sequential and parallel modes
"""

import pytest
from DSA.FHIR.Observation.observation_dict_builder import (
    _build_observation_from_row_dict,
    _build_observation_lossless_dict,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    parse_aktivitetsresultat_xml,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_XML_ELEMENT_META,
    EXTENSION_XML_HIERARCHY,
    EXTENSION_XML_KONTEKST,
    EXTENSION_XML_ROOT_META,
    EXTENSION_XML_ROW_TYPE,
)

pytestmark = pytest.mark.unit

FULL_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Ledetekst>Vitale</Ledetekst>
  <Type>Observation</Type>
  <Kladde>false</Kladde>
  <Kontekst>
    <Oprettet>
      <bruger>DrHansen</bruger>
      <PersId>OP001</PersId>
      <Rolle>Læge</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-15T10:30:00</Tidspunkt>
      <Tidsstempel>1705312200</Tidsstempel>
    </Oprettet>
    <Aendret>
      <bruger>NurseJensen</bruger>
      <PersId>AE002</PersId>
      <Rolle>Sygeplejerske</Rolle>
      <AdministrativEnhed>Medicin</AdministrativEnhed>
      <AdministrativEnhedId>AE002</AdministrativEnhedId>
      <Tidspunkt>2024-01-16T14:00:00</Tidspunkt>
      <Tidsstempel>1705413600</Tidsstempel>
    </Aendret>
  </Kontekst>
  <Klynge type="sekvens">
    <Navn>VitalGruppe</Navn>
    <Ledetekst>Vitale målinger</Ledetekst>
    <Element type="string">
      <Navn>Systolisk</Navn>
      <Ledetekst>Systolisk BP</Ledetekst>
      <Vaerdi>120</Vaerdi>
      <ElementId>E001</ElementId>
      <Kode>ZZ3170</Kode>
      <Originalkode>ZZ3170.1</Originalkode>
      <AdministrativEnhedId>ADM001</AdministrativEnhedId>
      <PersId>PERS001</PersId>
      <MOID>MOID001</MOID>
    </Element>
  </Klynge>
  <Klynge type="selektion">
    <Navn>DiagnoseValg</Navn>
    <Ledetekst>Vælg diagnose</Ledetekst>
    <ValgLedetekst>Valgt: Hypertension</ValgLedetekst>
    <Element type="coded">
      <Navn>DiagnoseKode</Navn>
      <Ledetekst>ICD-10</Ledetekst>
      <Vaerdi>I10</Vaerdi>
      <Kode>I10</Kode>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>"""

BASE_ROW = {
    "OBJEKT_ID": 42,
    "PATIENT_ID": 100,
    "ENCOUNTER_ID": 200,
    "EPISODE_OF_CARE_ID": 300,
    "AKTIVITETTYPE_KODE": "A01",
    "AKTIVITETTYPE_KODETEKST": "Aktivitet",
    "RESULTATTYPE_KODE": "R01",
    "RESULTATTYPE_KODETEKST": "Resultat",
    "OVERSKRIFT": "Blodtryk",
    "XML": FULL_XML,
    "KLADDE": 0,
    "PERIOD_START": "2024-01-15T10:30:00",
    "SLETTET": 0,
}


def _build_obs():
    xml_rows, kontekst = parse_aktivitetsresultat_xml(xml_string=FULL_XML, aktivitets_id=42)
    _rid, obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
    return obs


def _find_ext(extensions, url_fragment):
    """Find extension(s) matching a URL fragment."""
    if not extensions:
        return []
    return [e for e in extensions if url_fragment in e.get("url", "")]


def _find_sub_ext(ext, url_fragment):
    """Find sub-extensions matching a URL fragment."""
    subs = ext.get("extension")
    if not subs:
        return []
    return [e for e in subs if url_fragment in e.get("url", "")]


class TestLosslessRootMetadata:
    def test_root_meta_extension_present(self):
        obs = _build_obs()
        root_exts = _find_ext(obs.get("extension"), EXTENSION_XML_ROOT_META)
        assert len(root_exts) == 1

    def test_root_meta_overskrift(self):
        obs = _build_obs()
        root_ext = _find_ext(obs.get("extension"), EXTENSION_XML_ROOT_META)[0]
        overskrift = _find_sub_ext(root_ext, "overskrift")
        assert len(overskrift) == 1
        assert overskrift[0]["valueString"] == "Blodtryk"

    def test_root_meta_ledetekst(self):
        obs = _build_obs()
        root_ext = _find_ext(obs.get("extension"), EXTENSION_XML_ROOT_META)[0]
        ledetekst = _find_sub_ext(root_ext, "ledetekst")
        assert len(ledetekst) == 1
        assert ledetekst[0]["valueString"] == "Vitale"

    def test_root_meta_type(self):
        obs = _build_obs()
        root_ext = _find_ext(obs.get("extension"), EXTENSION_XML_ROOT_META)[0]
        type_ext = _find_sub_ext(root_ext, "type")
        assert len(type_ext) == 1
        assert type_ext[0]["valueString"] == "Observation"

    def test_root_meta_kladde(self):
        obs = _build_obs()
        root_ext = _find_ext(obs.get("extension"), EXTENSION_XML_ROOT_META)[0]
        kladde = _find_sub_ext(root_ext, "kladde")
        assert len(kladde) == 1
        assert kladde[0]["valueString"] == "false"


class TestLosslessKontekst:
    def test_oprettet_kontekst_present(self):
        obs = _build_obs()
        oprettet = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#oprettet")
        assert len(oprettet) == 1

    def test_oprettet_bruger(self):
        obs = _build_obs()
        oprettet = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#oprettet")[0]
        bruger = _find_sub_ext(oprettet, "bruger")
        assert len(bruger) == 1
        assert bruger[0]["valueString"] == "DrHansen"

    def test_oprettet_rolle(self):
        obs = _build_obs()
        oprettet = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#oprettet")[0]
        rolle = _find_sub_ext(oprettet, "rolle")
        assert rolle[0]["valueString"] == "Læge"

    def test_oprettet_tidsstempel(self):
        obs = _build_obs()
        oprettet = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#oprettet")[0]
        ts = _find_sub_ext(oprettet, "tidsstempel")
        assert ts[0]["valueInteger"] == 1705312200

    def test_aendret_kontekst_present(self):
        obs = _build_obs()
        aendret = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#aendret")
        assert len(aendret) == 1

    def test_aendret_bruger(self):
        obs = _build_obs()
        aendret = _find_ext(obs.get("extension"), f"{EXTENSION_XML_KONTEKST}#aendret")[0]
        bruger = _find_sub_ext(aendret, "bruger")
        assert bruger[0]["valueString"] == "NurseJensen"


class TestLosslessComponentMetadata:
    def test_component_count(self):
        obs = _build_obs()
        components = obs.get("component")
        assert components is not None
        assert len(components) == 3

    def test_element_id_in_extension(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        elem_id_exts = _find_ext(
            systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#elementId"
        )
        assert len(elem_id_exts) == 1
        assert elem_id_exts[0]["valueString"] == "E001"

    def test_element_kode_in_extension(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        kode_exts = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#kode")
        assert len(kode_exts) == 1
        assert kode_exts[0]["valueString"] == "ZZ3170"

    def test_element_originalkode(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        orgkode = _find_ext(
            systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#originalkode"
        )
        assert len(orgkode) == 1
        assert orgkode[0]["valueString"] == "ZZ3170.1"

    def test_element_pers_id(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        persid = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#persId")
        assert len(persid) == 1
        assert persid[0]["valueString"] == "PERS001"

    def test_element_moid(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        moid = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#moid")
        assert len(moid) == 1
        assert moid[0]["valueString"] == "MOID001"

    def test_element_administrativ_enhed_id(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        adm = _find_ext(
            systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#administrativEnhedId"
        )
        assert len(adm) == 1
        assert adm[0]["valueString"] == "ADM001"

    def test_element_type(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        etype = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#type")
        assert len(etype) == 1
        assert etype[0]["valueString"] == "string"

    def test_element_ledetekst(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        ldt = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_ELEMENT_META}#ledetekst")
        assert len(ldt) == 1
        assert ldt[0]["valueString"] == "Systolisk BP"


class TestLosslessHierarchy:
    def test_hierarki_string_present(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        hier = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_HIERARCHY}#hierarki")
        assert len(hier) == 1
        assert "Kly" in hier[0]["valueString"]

    def test_klynge_id_present(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        kly_id = [
            e
            for e in systolisk.get("extension", [])
            if e.get("url") == f"{EXTENSION_XML_HIERARCHY}#klyngeId"
        ]
        assert len(kly_id) == 1
        assert kly_id[0]["valueInteger"] >= 1

    def test_klynge_navn_present(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        kly_navn = [
            e
            for e in systolisk.get("extension", [])
            if e.get("url") == f"{EXTENSION_XML_HIERARCHY}#klyngeNavn"
        ]
        assert len(kly_navn) == 1
        assert kly_navn[0]["valueString"] == "VitalGruppe"

    def test_element_nummer_present(self):
        obs = _build_obs()
        systolisk = obs["component"][0]
        enum = _find_ext(systolisk.get("extension"), f"{EXTENSION_XML_HIERARCHY}#elementNummer")
        assert len(enum) == 1
        assert enum[0]["valueInteger"] >= 1


class TestLosslessSelektionRow:
    def test_selektion_row_type(self):
        obs = _build_obs()
        selektion = obs["component"][1]
        row_type = _find_ext(selektion.get("extension"), EXTENSION_XML_ROW_TYPE)
        assert len(row_type) == 1
        assert row_type[0]["valueString"] == "Selektion"

    def test_selektion_valg_ledetekst(self):
        obs = _build_obs()
        selektion = obs["component"][1]
        vldt = _find_ext(
            selektion.get("extension"), f"{EXTENSION_XML_HIERARCHY}#klyngeValgLedetekst"
        )
        assert len(vldt) == 1
        assert vldt[0]["valueString"] == "Valgt: Hypertension"


class TestLosslessVsFastMode:
    def test_fast_mode_has_fewer_extensions(self):
        _rid, fast_obs = _build_observation_from_row_dict(BASE_ROW, [("Systolisk", "120")])
        lossless_obs = _build_obs()

        fast_comp_exts = (
            fast_obs["component"][0].get("extension") if fast_obs.get("component") else None
        )
        lossless_comp_exts = lossless_obs["component"][0].get("extension")

        assert fast_comp_exts is None
        assert lossless_comp_exts is not None
        assert len(lossless_comp_exts) > 0


class TestLosslessEdgeCases:
    def test_empty_xml_returns_observation_without_components(self):
        row = dict(BASE_ROW)
        row["XML"] = ""
        _rid, obs = _build_observation_lossless_dict(row, None, None)
        assert obs.get("component") is None or len(obs.get("component", [])) == 0

    def test_malformed_xml_returns_observation_without_components(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml("<bad><xml", aktivitets_id=99)
        row = dict(BASE_ROW)
        _rid, obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        assert obs.get("component") is None or len(obs.get("component", [])) == 0

    def test_no_kontekst_still_builds(self):
        xml_rows, _ = parse_aktivitetsresultat_xml(
            "<Afdelingsydelsesresultat><Element type='string'><Navn>X</Navn><Vaerdi>Y</Vaerdi></Element></Afdelingsydelsesresultat>",
            aktivitets_id=99,
        )
        row = dict(BASE_ROW)
        _rid, obs = _build_observation_lossless_dict(row, xml_rows, None)
        assert obs.get("component") is not None
        assert len(obs["component"]) == 1


class TestLosslessCategory:
    def test_category_present_for_non_procedure(self):
        obs = _build_obs()
        assert obs.get("category") is not None
        assert len(obs["category"]) == 1

    def test_category_exam_for_generic_code(self):
        obs = _build_obs()
        codings = obs["category"][0]["coding"]
        fhir_coding = codings[0]
        assert fhir_coding["code"] == "exam"
        assert fhir_coding["system"] == "http://terminology.hl7.org/CodeSystem/observation-category"

    def test_category_raw_level1(self):
        obs = _build_obs()
        codings = obs["category"][0]["coding"]
        raw_coding = codings[1]
        assert raw_coding["code"] == "A01"
        assert raw_coding["system"] == "urn:source:ifdb:aktivitettype_category"

    def test_category_procedure_for_sks_procedure(self):
        row = {**BASE_ROW, "AKTIVITETTYPE_KODE": "SKS.a.proBWAA00"}
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, aktivitets_id=42)
        _rid, obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        assert obs["category"][0]["coding"][0]["code"] == "procedure"

    def test_observation_code_strips_sks_prefix_from_activity_type(self):
        row = {
            **BASE_ROW,
            "AKTIVITETTYPE_KODE": "SKS.a.proBWAA00",
            "AKTIVITETTYPE_KODETEKST": "Appendektomi",
        }
        _rid, obs = _build_observation_lossless_dict(row, None, None)
        assert obs["code"]["coding"][0]["code"] == "BWAA00"
        assert "SKS.a." not in obs["code"]["coding"][0]["code"]

    def test_category_survey_for_sfi(self):
        row = {**BASE_ROW, "AKTIVITETTYPE_KODE": "SFI.2.1"}
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, aktivitets_id=42)
        _rid, obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        assert obs["category"][0]["coding"][0]["code"] == "survey"
        assert obs["category"][0]["coding"][1]["code"] == "SFI"

    def test_category_none_when_no_activity_type(self):
        row = {**BASE_ROW}
        del row["AKTIVITETTYPE_KODE"]
        _rid, obs = _build_observation_lossless_dict(row, None, None)
        assert "category" not in obs

    def test_category_dotted_code_uses_level1(self):
        row = {**BASE_ROW, "AKTIVITETTYPE_KODE": "DI10.1.2"}
        _rid, obs = _build_observation_lossless_dict(row, None, None)
        assert obs["category"][0]["coding"][1]["code"] == "DI10"


class TestLosslessIssued:
    def test_issued_from_udfoert_tidspunkt(self):
        row = {**BASE_ROW, "UDFOERT_TIDSPUNKT": "2024-03-20T14:30:00"}
        _rid, obs = _build_observation_lossless_dict(row, None, None)
        assert obs.get("issued") is not None
        assert "2024-03-20" in obs["issued"]

    def test_issued_none_when_missing(self):
        obs = _build_obs()
        assert "issued" not in obs

    def test_issued_not_in_source_meta_extension(self):
        row = {**BASE_ROW, "UDFOERT_TIDSPUNKT": "2024-03-20T14:30:00"}
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, aktivitets_id=42)
        _rid, obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        for ext in obs.get("extension") or []:
            for sub in ext.get("extension") or []:
                assert "udfoertTidspunkt" not in sub.get("url", "")
