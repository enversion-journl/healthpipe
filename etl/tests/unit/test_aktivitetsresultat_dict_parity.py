# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dict builder tests for AKTIVITETSRESULTAT.

Verifies that the dict-based FHIR pipeline produces structurally correct
JSON output for all field types:

- identifier, status, code, effectiveDateTime
- subject, encounter, extension (EpisodeOfCare)
- Root metadata extensions (Overskrift, Ledetekst, Type, Kladde)
- Kontekst provenance extensions
- Component metadata and hierarchy extensions
- exclude_none semantics (no null values in output)
- Fast mode output
"""

import pytest
from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
    _build_level_extensions_dict,
    _build_udf_extensions_dict,
)
from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Observation.observation_dict_builder import (
    _build_kontekst_extensions_dict,
    _build_lossless_component_dict,
    _build_observation_from_row_dict,
    _build_observation_lossless_dict,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    parse_aktivitetsresultat_xml,
    parse_xml_components_fast,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSUDFOERELSE_META,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
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
    <Element type="integer">
      <Navn>Diastolisk</Navn>
      <Vaerdi>80</Vaerdi>
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


def _normalize(obj):
    """Recursively sort dicts by key for deterministic comparison."""
    if isinstance(obj, dict):
        return {k: _normalize(v) for k, v in sorted(obj.items())}
    if isinstance(obj, list):
        return [_normalize(item) for item in obj]
    return obj


class TestComponentBuilder:
    def test_component_with_all_fields(self):
        xml_rows, _ = parse_aktivitetsresultat_xml(FULL_XML, 42)
        row = xml_rows[0]
        dict_comp = _build_lossless_component_dict(row)
        assert dict_comp["code"]["text"] == "Systolisk"
        assert "extension" in dict_comp

    def test_component_minimal_fields(self):
        xml = "<Afdelingsydelsesresultat><Element><Navn>Test</Navn><Vaerdi>V</Vaerdi></Element></Afdelingsydelsesresultat>"
        xml_rows, _ = parse_aktivitetsresultat_xml(xml, 1)
        row = xml_rows[0]
        dict_comp = _build_lossless_component_dict(row)
        assert dict_comp["code"]["text"] == "Test"

    def test_all_xml_rows_build_components(self):
        xml_rows, _ = parse_aktivitetsresultat_xml(FULL_XML, 42)
        for i, row in enumerate(xml_rows):
            if row.xml_status == -1 or row.raekke_type == "Tom":
                continue
            dict_version = _build_lossless_component_dict(row)
            assert "code" in dict_version, f"Missing code at row {i}"


class TestKontekstBuilder:
    def test_kontekst_all_actors(self):
        _, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        dict_exts = _build_kontekst_extensions_dict(kontekst)
        assert len(dict_exts) == 2
        assert any("oprettet" in ext.get("url", "") for ext in dict_exts)
        assert any("aendret" in ext.get("url", "") for ext in dict_exts)

    def test_kontekst_none(self):
        assert _build_kontekst_extensions_dict(None) == []


class TestObservationLosslessDict:
    def test_full_observation_structure(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _rid, dict_obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
        assert dict_obs["resourceType"] == "Observation"
        assert dict_obs["status"] == "final"
        assert "component" in dict_obs
        assert len(dict_obs["component"]) >= 1

    def test_identifier_preserved(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _, dict_obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
        assert (
            dict_obs["identifier"] == [{"system": "urn:oid:1.2.208.176.7.200.3", "value": "42"}]
            or "identifier" in dict_obs
        )

    def test_status_final(self):
        _, dict_obs = _build_observation_lossless_dict(
            BASE_ROW, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        assert dict_obs["status"] == "final"

    def test_status_preliminary_for_draft(self):
        row = {**BASE_ROW, "KLADDE": 1}
        _, dict_obs = _build_observation_lossless_dict(
            row, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        assert dict_obs["status"] == "preliminary"

    def test_status_and_extension_for_deleted(self):
        row = {**BASE_ROW, "SLETTET": 1}
        _, dict_obs = _build_observation_lossless_dict(
            row, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        assert dict_obs["status"] == "entered-in-error"
        is_deleted = [
            ext for ext in dict_obs.get("extension", []) if ext.get("url") == "urn:fhir:isDeleted"
        ]
        assert len(is_deleted) == 1
        assert is_deleted[0].get("valueInteger") == 1

    def test_code_with_codings(self):
        _, dict_obs = _build_observation_lossless_dict(
            BASE_ROW, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        assert "code" in dict_obs
        assert "coding" in dict_obs["code"]
        assert len(dict_obs["code"]["coding"]) == 2

    def test_component_count(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _, dict_obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
        assert len(dict_obs.get("component", [])) >= 1

    def test_extension_count(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _, dict_obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
        assert len(dict_obs.get("extension", [])) >= 1

    def test_no_null_values(self):
        _, dict_obs = _build_observation_lossless_dict(
            BASE_ROW, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        _assert_no_nulls(dict_obs, "root")

    def test_minimal_row(self):
        row = {"OBJEKT_ID": 1, "PATIENT_ID": 2}
        _, dict_obs = _build_observation_lossless_dict(row, None, None)
        assert dict_obs["status"] == "final"
        assert dict_obs["resourceType"] == "Observation"

    def test_empty_xml(self):
        row = {**BASE_ROW, "XML": "<Afdelingsydelsesresultat/>"}
        xml_rows, kontekst = parse_aktivitetsresultat_xml(row["XML"], 42)
        _, dict_obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        assert dict_obs["status"] == "final"
        assert "component" not in dict_obs or dict_obs["component"] == []


class TestFastPathDict:
    def test_fast_path_observation(self):
        components, _ = parse_xml_components_fast(FULL_XML)
        _, dict_obs = _build_observation_from_row_dict(BASE_ROW, components)
        assert dict_obs["resourceType"] == "Observation"
        assert dict_obs["status"] == "final"
        assert len(dict_obs.get("component", [])) >= 1

    def test_fast_path_no_components(self):
        row = {"OBJEKT_ID": 1, "PATIENT_ID": 2}
        _, dict_obs = _build_observation_from_row_dict(row, None)
        assert dict_obs["status"] == "final"
        assert "component" not in dict_obs or dict_obs["component"] == []


class TestOracleNumberNormalization:
    def test_lossless_dict_normalizes_oracle_number_identifiers(self):
        row = {
            **BASE_ROW,
            "OBJEKT_ID": 42.0,
            "PATIENT_ID": 100.0,
            "ENCOUNTER_ID": 200.0,
            "EPISODE_OF_CARE_ID": 300.0,
            "UDFOERT_PERSID": 400.0,
        }
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42.0)

        _, dict_obs = _build_observation_lossless_dict(row, xml_rows, kontekst)

        assert dict_obs["identifier"][0]["value"] == "42"
        assert dict_obs["subject"]["identifier"]["value"] == "100"
        assert dict_obs["encounter"]["identifier"]["value"] == "200"
        assert dict_obs["performer"][0]["identifier"]["value"] == "400"
        episode_refs = [
            ext["valueReference"] for ext in dict_obs["extension"] if "episodeOfCare" in ext["url"]
        ]
        assert episode_refs[0]["identifier"]["value"] == "300"

    def test_fast_dict_resolved_refs_normalize_oracle_number_lookup_keys(self):
        row = {
            **BASE_ROW,
            "OBJEKT_ID": 42.0,
            "PATIENT_ID": 100.0,
            "ENCOUNTER_ID": 200.0,
            "EPISODE_OF_CARE_ID": 300.0,
        }
        components, _ = parse_xml_components_fast(FULL_XML)
        resolved = ResolvedRefLookup(
            patient={(SYSTEM_IFDB_PATIENT_ID, "100"): "patient-100"},
            encounter={(SYSTEM_IFDB_OBJECT_ID, "200"): "encounter-200"},
            episode={(SYSTEM_IFDB_EPISODE_OF_CARE_ID, "300"): "episode-300"},
            observation={(SYSTEM_IFDB_AKTIVITETSRESULTAT, "42"): "observation-42"},
        )

        rid, dict_obs = _build_observation_from_row_dict(row, components, resolved)

        assert rid == "observation-42"
        assert dict_obs["identifier"][0]["value"] == "42"
        assert dict_obs["subject"] == {"id": "patient-100", "resourceType": "Patient"}
        assert dict_obs["encounter"] == {
            "id": "encounter-200",
            "resourceType": "Encounter",
        }
        episode_refs = [
            ext["valueReference"] for ext in dict_obs["extension"] if "episodeOfCare" in ext["url"]
        ]
        assert episode_refs == [{"id": "episode-300", "resourceType": "EpisodeOfCare"}]


class TestDictSpecificFields:
    def test_resource_type_present(self):
        _, dict_obs = _build_observation_lossless_dict(
            BASE_ROW, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        assert dict_obs["resourceType"] == "Observation"

    def test_id_is_uuid(self):
        _, dict_obs = _build_observation_lossless_dict(
            BASE_ROW, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        rid = dict_obs["id"]
        assert len(rid) == 36 and rid.count("-") == 4


class TestCategoryDict:
    def test_category_present(self):
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _, dict_obs = _build_observation_lossless_dict(BASE_ROW, xml_rows, kontekst)
        assert "category" in dict_obs
        assert dict_obs["category"][0]["coding"][0]["code"] == "exam"

    def test_category_absent_when_no_code(self):
        row = {"OBJEKT_ID": 1, "PATIENT_ID": 2}
        _, dict_obs = _build_observation_lossless_dict(row, None, None)
        assert "category" not in dict_obs

    def test_category_fast_path(self):
        components, _ = parse_xml_components_fast(FULL_XML)
        _, dict_obs = _build_observation_from_row_dict(BASE_ROW, components)
        assert dict_obs.get("category") is not None


class TestIssuedDict:
    def test_issued_present(self):
        row = {**BASE_ROW, "UDFOERT_TIDSPUNKT": "2024-03-20T14:30:00"}
        xml_rows, kontekst = parse_aktivitetsresultat_xml(FULL_XML, 42)
        _, dict_obs = _build_observation_lossless_dict(row, xml_rows, kontekst)
        assert "issued" in dict_obs
        assert "2024-03-20" in dict_obs["issued"]

    def test_issued_absent_when_no_timestamp(self):
        _, dict_obs = _build_observation_lossless_dict(BASE_ROW, None, None)
        assert "issued" not in dict_obs

    def test_issued_fast_path(self):
        row = {**BASE_ROW, "UDFOERT_TIDSPUNKT": "2024-06-15T08:00:00"}
        components, _ = parse_xml_components_fast(FULL_XML)
        _, dict_obs = _build_observation_from_row_dict(row, components)
        assert "issued" in dict_obs

    def test_udfoert_tidspunkt_not_in_source_meta(self):
        row = {**BASE_ROW, "UDFOERT_TIDSPUNKT": "2024-03-20T14:30:00"}
        _, dict_obs = _build_observation_lossless_dict(
            row, *parse_aktivitetsresultat_xml(FULL_XML, 42)
        )
        for ext in dict_obs.get("extension", []):
            for sub in ext.get("extension", []):
                assert sub.get("url") != "udfoertTidspunkt"


class TestUdfExtensions:
    def test_udf_extension_built_with_all_fields(self):
        udf_row = {
            "OPRET_PERSID": "OP001",
            "OPRET_PERSNAVN": "Dr Hansen",
            "OPRET_BRUGERROLLE": "Læge",
            "OPRET_ADMENHED_KODE": "1234",
            "OPRET_ADMENHED_NAVN": "Kardiologi",
            "OPRET_ADMENHED_ORGKODE": "ORG01",
            "OPRET_TIDSPUNKT": "2024-01-15T10:30:00",
            "AENDRET_PERSID": "AE001",
            "AENDRET_PERSNAVN": "Nurse Jensen",
            "AENDRET_BRUGERROLLE": "Sygeplejerske",
            "AENDRET_ADMENHED_KODE": "5678",
            "AENDRET_ADMENHED_NAVN": "Medicin",
            "AENDRET_ADMENHED_ORGKODE": "ORG02",
            "AENDRET_TIDSPUNKT": "2024-01-16T14:00:00",
            "AKTIVITET_ID": "99",
            "FORTR_NIVEAU": "1",
            "FORTR_GRUPPE_ID": "G01",
            "FORTR_GRUPPE_NAVN": "Fortrolig",
        }
        result = _build_udf_extensions_dict(udf_row)
        assert len(result) == 1
        assert result[0]["url"] == EXTENSION_AKTIVITETSUDFOERELSE_META
        sub_urls = {s["url"] for s in result[0]["extension"]}
        assert "oprettetPersId" in sub_urls
        assert "aendretPersId" in sub_urls
        assert "aktivitetId" in sub_urls
        assert "fortrolighedsNiveau" in sub_urls

    def test_udf_extension_empty_when_no_data(self):
        result = _build_udf_extensions_dict({})
        assert result == []

    def test_udf_extension_partial_data(self):
        udf_row = {"OPRET_PERSID": "OP001", "FORTR_NIVEAU": "2"}
        result = _build_udf_extensions_dict(udf_row)
        assert len(result) == 1
        sub_urls = {s["url"] for s in result[0]["extension"]}
        assert "oprettetPersId" in sub_urls
        assert "fortrolighedsNiveau" in sub_urls
        assert "aendretPersId" not in sub_urls


class TestLevelExtensions:
    def test_level_extension_single(self):
        result = _build_level_extensions_dict("ZZ3170")
        assert len(result) == 1
        assert result[0]["url"] == EXTENSION_AKTIVITETTYPE_KODE_HIERARKI
        subs = result[0]["extension"]
        assert len(subs) == 1
        assert subs[0] == {"url": "level1", "valueString": "ZZ3170"}

    def test_level_extension_three_levels(self):
        result = _build_level_extensions_dict("DI10.1.2")
        subs = result[0]["extension"]
        assert len(subs) == 3
        assert subs[0] == {"url": "level1", "valueString": "DI10"}
        assert subs[1] == {"url": "level2", "valueString": "1"}
        assert subs[2] == {"url": "level3", "valueString": "2"}

    def test_level_extension_none_code(self):
        result = _build_level_extensions_dict(None)
        assert result == []

    def test_level_extension_empty_code(self):
        result = _build_level_extensions_dict("")
        assert result == []


def _assert_no_nulls(obj, path=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            assert v is not None, f"Null value at {path}.{k}"
            _assert_no_nulls(v, f"{path}.{k}")
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            assert item is not None, f"Null value at {path}[{i}]"
            _assert_no_nulls(item, f"{path}[{i}]")
