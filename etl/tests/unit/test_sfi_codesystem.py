# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for SFI CodeSystem: code generation, element extraction, and codesystem build.

Tests the parser contract that downstream bots (D, E, F) depend on.
"""

import pytest
from DSA.CodeSystem.sfi.sfi import (
    SFI_SYSTEM,
    SfiElement,
    build_sfi_codesystem,
    extract_sfi_elements,
    generate_sfi_code,
    summarize_sfi_conflicts,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    parse_aktivitetsresultat_xml,
)

# ---------------------------------------------------------------------------
# Fixtures: XML strings for known clinical cases
# ---------------------------------------------------------------------------

BLODTRYK_XML = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Ledetekst>Blodtryk</Ledetekst>
  <Type>Maalinger.Blodtryk</Type>
  <Kontekst>
    <Oprettet>
      <bruger>TestBruger</bruger>
      <Tidspunkt>2026-01-15T10:00:00</Tidspunkt>
    </Oprettet>
  </Kontekst>
  <Element>
    <Navn>BT_SYS</Navn>
    <Vaerdi>142</Vaerdi>
    <Ledetekst>Systolisk blodtryk</Ledetekst>
    <ElementId>BT_SYS</ElementId>
    <Type>heltal</Type>
  </Element>
  <Element>
    <Navn>BT_DIA</Navn>
    <Vaerdi>88</Vaerdi>
    <Ledetekst>Diastolisk blodtryk</Ledetekst>
    <ElementId>BT_DIA</ElementId>
    <Type>heltal</Type>
  </Element>
</Afdelingsydelsesresultat>"""

GCS_XML = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Overskrift>Glasgow Coma Scale</Overskrift>
  <Ledetekst>GCS</Ledetekst>
  <Type>RM_Generelle.GCS</Type>
  <Kontekst>
    <Oprettet>
      <bruger>TestBruger</bruger>
      <Tidspunkt>2026-01-15T10:00:00</Tidspunkt>
    </Oprettet>
  </Kontekst>
  <Element>
    <Navn>edGCSTotalScore</Navn>
    <Vaerdi>15</Vaerdi>
    <Ledetekst>Total score</Ledetekst>
    <ElementId>edGCSTotalScore</ElementId>
    <Type>heltal</Type>
  </Element>
  <Element>
    <Navn>edGCSMotorisk</Navn>
    <Vaerdi>6</Vaerdi>
    <Ledetekst>Motorisk</Ledetekst>
    <ElementId>edGCSMotorisk</ElementId>
    <Type>heltal</Type>
  </Element>
  <Element>
    <Navn>edGCSVerbal</Navn>
    <Vaerdi>5</Vaerdi>
    <Ledetekst>Verbal</Ledetekst>
    <ElementId>edGCSVerbal</ElementId>
    <Type>heltal</Type>
  </Element>
  <Element>
    <Navn>edGCSOejne</Navn>
    <Vaerdi>4</Vaerdi>
    <Ledetekst>Øjne</Ledetekst>
    <ElementId>edGCSOejne</ElementId>
    <Type>heltal</Type>
  </Element>
</Afdelingsydelsesresultat>"""

KLYNGE_SELEKTION_XML = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Overskrift>Ernæring</Overskrift>
  <Ledetekst>Ernæringsvurdering</Ledetekst>
  <Type>Ernaering.Vurdering</Type>
  <Klynge type="selektion">
    <Navn>ValgtErnaeringPlan</Navn>
    <Ledetekst>Ernæringsplan valg</Ledetekst>
    <ValgLedetekst>Standardkost</ValgLedetekst>
    <Element>
      <Navn>Kosttype</Navn>
      <Vaerdi>Standard</Vaerdi>
      <Ledetekst>Valgt kosttype</Ledetekst>
      <ElementId>edKosttype</ElementId>
      <Type>tekst</Type>
    </Element>
  </Klynge>
  <Klynge type="sekvens">
    <Navn>DetaljerKlynge</Navn>
    <Ledetekst>Detaljer</Ledetekst>
    <Element>
      <Navn>Vaegt</Navn>
      <Vaerdi>72.5</Vaerdi>
      <Ledetekst>Vægt</Ledetekst>
      <ElementId>edVaegt</ElementId>
      <Type>decimal</Type>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>"""

NO_ELEMENT_ID_XML = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Overskrift>Test uden ElementId</Overskrift>
  <Type>TestType</Type>
  <Element>
    <Navn>FritFelt</Navn>
    <Vaerdi>noget tekst</Vaerdi>
    <Ledetekst>Frit felt</Ledetekst>
    <Type>fritekst</Type>
  </Element>
</Afdelingsydelsesresultat>"""

EMPTY_XML = ""

TOM_STATUS_XML = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Overskrift>Tom</Overskrift>
  <Type>Tom</Type>
</Afdelingsydelsesresultat>"""


# ===========================================================================
# Test: generate_sfi_code
# ===========================================================================


class TestGenerateSfiCode:
    @pytest.mark.unit
    def test_qualified_code_from_element_id_and_model(self) -> None:
        # Code is qualified by MODELLERINGSNAVN.
        code, status = generate_sfi_code("BT_SYS", "Systolisk blodtryk", "Blodtryk")
        assert code == "Blodtryk.BT_SYS"
        assert status == "active"

    @pytest.mark.unit
    def test_bare_code_without_model_flagged_review(self) -> None:
        # No model context → bare element key, status downgraded to review.
        code, status = generate_sfi_code("  BT_DIA  ", "Diastolisk", None)
        assert code == "BT_DIA"
        assert status == "review"

    @pytest.mark.unit
    def test_element_id_stripped_inside_qualified_code(self) -> None:
        code, status = generate_sfi_code("  BT_DIA  ", "Diastolisk", "Blodtryk")
        assert code == "Blodtryk.BT_DIA"
        assert status == "active"

    @pytest.mark.unit
    def test_fallback_with_model_name(self) -> None:
        # The model_name itself contains a dot → must be sanitised to underscore.
        code, status = generate_sfi_code(None, "FritFelt", "TestModel.V1")
        assert code == "TestModel_V1._fb_FritFelt"
        assert status == "review"

    @pytest.mark.unit
    def test_fallback_without_model_name(self) -> None:
        code, status = generate_sfi_code(None, "FritFelt", None)
        assert code == "_fb_FritFelt"
        assert status == "review"

    @pytest.mark.unit
    def test_fallback_unknown_when_both_missing(self) -> None:
        code, status = generate_sfi_code(None, None, None)
        assert code == "_fb_unknown"
        assert status == "review"

    @pytest.mark.unit
    def test_empty_element_id_triggers_fallback(self) -> None:
        code, status = generate_sfi_code("", "Felt", "Model")
        assert status == "review"
        assert "Felt" in code
        assert code.startswith("Model.")

    @pytest.mark.unit
    def test_whitespace_element_id_triggers_fallback(self) -> None:
        code, status = generate_sfi_code("   ", "Felt", None)
        assert status == "review"


# ===========================================================================
# Test: extract_sfi_elements from parsed XML
# ===========================================================================


class TestExtractSfiElements:
    def _parse_and_extract(
        self,
        xml: str,
        model_name: str | None = None,
        model_version: str | None = None,
    ) -> list[SfiElement]:
        rows, _ = parse_aktivitetsresultat_xml(xml, aktivitets_id=1)
        return extract_sfi_elements(rows, model_name=model_name, model_version=model_version)

    @pytest.mark.unit
    def test_blodtryk_gives_two_elements(self) -> None:
        elements = self._parse_and_extract(BLODTRYK_XML, "Maalinger.Blodtryk", "1.0")
        codes = {e.code for e in elements}
        # "Maalinger.Blodtryk" → "Maalinger_Blodtryk." after dot sanitisation.
        assert "Maalinger_Blodtryk.BT_SYS" in codes
        assert "Maalinger_Blodtryk.BT_DIA" in codes
        assert len(elements) == 2

    @pytest.mark.unit
    def test_blodtryk_elements_have_active_status(self) -> None:
        elements = self._parse_and_extract(BLODTRYK_XML, model_name="Maalinger.Blodtryk")
        for e in elements:
            assert e.status == "active"

    @pytest.mark.unit
    def test_blodtryk_elements_have_integer_datatype(self) -> None:
        elements = self._parse_and_extract(BLODTRYK_XML)
        for e in elements:
            assert e.datatype == "integer"

    @pytest.mark.unit
    def test_blodtryk_elements_have_mmhg_unit(self) -> None:
        elements = self._parse_and_extract(BLODTRYK_XML)
        for e in elements:
            assert e.unit == "mmHg"

    @pytest.mark.unit
    def test_blodtryk_elements_have_model_metadata(self) -> None:
        elements = self._parse_and_extract(BLODTRYK_XML, "Maalinger.Blodtryk", "2.1")
        for e in elements:
            assert e.source_model_name == "Maalinger.Blodtryk"
            assert e.source_model_version == "2.1"

    @pytest.mark.unit
    def test_gcs_gives_four_elements(self) -> None:
        elements = self._parse_and_extract(GCS_XML, "RM_Generelle.GCS", "1.0")
        codes = {e.code for e in elements}
        # Dots in model name → underscores.
        assert "RM_Generelle_GCS.edGCSTotalScore" in codes
        assert "RM_Generelle_GCS.edGCSMotorisk" in codes
        assert "RM_Generelle_GCS.edGCSVerbal" in codes
        assert "RM_Generelle_GCS.edGCSOejne" in codes
        assert len(elements) == 4

    @pytest.mark.unit
    def test_gcs_total_score_properties(self) -> None:
        elements = self._parse_and_extract(GCS_XML, model_name="RM_Generelle.GCS")
        total = next(e for e in elements if e.code == "RM_Generelle_GCS.edGCSTotalScore")
        assert total.display == "Total score"
        assert total.datatype == "integer"
        # source_element_id remains the bare XML ElementId — lineage stays clean.
        assert total.source_element_id == "edGCSTotalScore"

    @pytest.mark.unit
    def test_klynge_selektion_extracted(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        codes = {e.code for e in elements}
        assert "Ernaering.edKosttype" in codes
        assert "Ernaering.edVaegt" in codes

    @pytest.mark.unit
    def test_selektion_has_coding_datatype(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        selektion = [e for e in elements if e.datatype == "coding"]
        assert len(selektion) >= 1

    @pytest.mark.unit
    def test_vaegt_has_kg_unit(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        vaegt = next(e for e in elements if e.code == "Ernaering.edVaegt")
        assert vaegt.unit == "kg"

    @pytest.mark.unit
    def test_vaegt_has_decimal_datatype(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        vaegt = next(e for e in elements if e.code == "Ernaering.edVaegt")
        assert vaegt.datatype == "decimal"

    @pytest.mark.unit
    def test_missing_element_id_gives_review_status(self) -> None:
        elements = self._parse_and_extract(NO_ELEMENT_ID_XML, model_name="TestType")
        assert len(elements) == 1
        assert elements[0].status == "review"
        # Qualified code = "<model>.<_fb_…>" — verify both prefix layers.
        assert elements[0].code.startswith("TestType.")
        assert "_fb_" in elements[0].code

    @pytest.mark.unit
    def test_empty_xml_gives_no_elements(self) -> None:
        elements = self._parse_and_extract(EMPTY_XML)
        assert elements == []

    @pytest.mark.unit
    def test_tom_xml_gives_no_elements(self) -> None:
        elements = self._parse_and_extract(TOM_STATUS_XML)
        assert elements == []

    @pytest.mark.unit
    def test_hierarchy_element_under_klynge(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        vaegt = next(e for e in elements if e.code == "Ernaering.edVaegt")
        assert vaegt.parent_code != ""

    @pytest.mark.unit
    def test_sfi_path_contains_hierarchy(self) -> None:
        elements = self._parse_and_extract(KLYNGE_SELEKTION_XML, model_name="Ernaering")
        vaegt = next(e for e in elements if e.code == "Ernaering.edVaegt")
        assert vaegt.sfi_path != ""
        assert "Vaegt" in vaegt.sfi_path

    @pytest.mark.unit
    def test_dedup_across_repeated_elements(self) -> None:
        xml = """<?xml version="1.0" encoding="utf-8"?>
<Afdelingsydelsesresultat>
  <Element>
    <Navn>Felt1</Navn>
    <Vaerdi>a</Vaerdi>
    <ElementId>id1</ElementId>
  </Element>
  <Element>
    <Navn>Felt1</Navn>
    <Vaerdi>b</Vaerdi>
    <ElementId>id1</ElementId>
  </Element>
</Afdelingsydelsesresultat>"""
        elements = self._parse_and_extract(xml, model_name="TestModel")
        assert len(elements) == 1
        assert elements[0].code == "TestModel.id1"


# ===========================================================================
# Test: build_sfi_codesystem
# ===========================================================================


class TestBuildSfiCodesystem:
    @pytest.mark.unit
    def test_concepts_shape(self) -> None:
        elems = [
            SfiElement(
                code="BT_SYS",
                display="Systolisk blodtryk",
                datatype="integer",
                unit="mmHg",
                sfi_path="BT_SYS",
                source_element_id="BT_SYS",
                source_element_name="BT_SYS",
                source_model_name="Maalinger.Blodtryk",
                source_model_version="1.0",
                status="active",
                parent_code="",
            )
        ]
        concepts, properties, hierarchy = build_sfi_codesystem(elems)
        assert len(concepts) == 1
        assert concepts[0]["CODE"] == "BT_SYS"
        assert concepts[0]["DISPLAY"] == "Systolisk blodtryk"

    @pytest.mark.unit
    def test_all_properties_present(self) -> None:
        elems = [
            SfiElement(
                code="Maalinger_Blodtryk.BT_SYS",
                display="Systolisk blodtryk",
                datatype="integer",
                unit="mmHg",
                sfi_path="BT_SYS",
                source_element_id="BT_SYS",
                source_element_name="BT_SYS",
                source_model_name="Maalinger.Blodtryk",
                source_model_version="1.0",
                status="active",
                parent_code="",
            )
        ]
        _, properties, _ = build_sfi_codesystem(elems)
        prop_keys = {p["KEY"] for p in properties}
        # source_model_version is now expressed via the version aggregates,
        # not as a single value.
        expected_keys = {
            "datatype",
            "unit",
            "sfi_path",
            "source_element_id",
            "source_element_name",
            "source_model_name",
            "first_seen_in_version",
            "last_seen_in_version",
            "seen_in_versions",
            "status",
        }
        assert expected_keys == prop_keys

    @pytest.mark.unit
    def test_properties_match_merge_shape(self) -> None:
        elems = [
            SfiElement(
                code="X",
                display="X",
                datatype="string",
                unit="",
                sfi_path="X",
                source_element_id="X",
                source_element_name="X",
                source_model_name="M",
                source_model_version="1",
                status="active",
                parent_code="",
            )
        ]
        _, properties, _ = build_sfi_codesystem(elems)
        for p in properties:
            assert "CODE" in p
            assert "KEY" in p
            assert "VALUE" in p
            assert "TYPE" in p

    @pytest.mark.unit
    def test_empty_properties_omitted(self) -> None:
        elems = [
            SfiElement(
                code="X",
                display="X",
                datatype="string",
                unit="",
                sfi_path="root",
                source_element_id="",
                source_element_name="X",
                source_model_name="",
                source_model_version="",
                status="active",
                parent_code="",
            )
        ]
        _, properties, _ = build_sfi_codesystem(elems)
        prop_keys = {p["KEY"] for p in properties}
        assert "unit" not in prop_keys
        assert "source_element_id" not in prop_keys
        assert "source_model_name" not in prop_keys
        # Version aggregates are skipped when there is no version data at all.
        assert "first_seen_in_version" not in prop_keys
        assert "last_seen_in_version" not in prop_keys
        assert "seen_in_versions" not in prop_keys

    @pytest.mark.unit
    def test_hierarchy_from_parent_code(self) -> None:
        elems = [
            SfiElement(
                code="child",
                display="Child",
                datatype="string",
                unit="",
                sfi_path="p/child",
                source_element_id="child",
                source_element_name="Child",
                source_model_name="M",
                source_model_version="1",
                status="active",
                parent_code="parent",
            ),
        ]
        _, _, hierarchy = build_sfi_codesystem(elems)
        assert len(hierarchy) == 1
        assert hierarchy[0]["CODE"] == "child"
        assert hierarchy[0]["PARENT_CODE"] == "parent"

    @pytest.mark.unit
    def test_no_hierarchy_when_no_parent(self) -> None:
        elems = [
            SfiElement(
                code="root_elem",
                display="Root",
                datatype="integer",
                unit="",
                sfi_path="root_elem",
                source_element_id="root_elem",
                source_element_name="Root",
                source_model_name="M",
                source_model_version="1",
                status="active",
                parent_code="",
            ),
        ]
        _, _, hierarchy = build_sfi_codesystem(elems)
        assert len(hierarchy) == 0

    @pytest.mark.unit
    def test_dedup_in_build(self) -> None:
        elem = SfiElement(
            code="dup",
            display="Dup",
            datatype="string",
            unit="",
            sfi_path="dup",
            source_element_id="dup",
            source_element_name="Dup",
            source_model_name="M",
            source_model_version="1",
            status="active",
            parent_code="",
        )
        concepts, _, _ = build_sfi_codesystem([elem, elem, elem])
        assert len(concepts) == 1


# ===========================================================================
# Test: SFI_SYSTEM constant
# ===========================================================================


class TestSfiSystem:
    @pytest.mark.unit
    def test_sfi_system_value(self) -> None:
        assert SFI_SYSTEM == "urn:dk:ifdb:sfi"


# ===========================================================================
# Test: Full pipeline from XML -> SFI elements -> codesystem triple
# ===========================================================================


class TestEndToEndPipeline:
    @pytest.mark.unit
    def test_blodtryk_full_pipeline(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(BLODTRYK_XML, aktivitets_id=100)
        elements = extract_sfi_elements(rows, "Maalinger.Blodtryk", "1.0")
        concepts, properties, hierarchy = build_sfi_codesystem(elements)

        assert len(concepts) == 2
        concept_codes = {c["CODE"] for c in concepts}
        # Codes are qualified by sanitised model name.
        assert concept_codes == {"Maalinger_Blodtryk.BT_SYS", "Maalinger_Blodtryk.BT_DIA"}

        sys_code = "Maalinger_Blodtryk.BT_SYS"
        bt_sys_props = [p for p in properties if p["CODE"] == sys_code]
        datatype_prop = next(p for p in bt_sys_props if p["KEY"] == "datatype")
        assert datatype_prop["VALUE"] == "integer"

        unit_prop = next(p for p in bt_sys_props if p["KEY"] == "unit")
        assert unit_prop["VALUE"] == "mmHg"

        model_prop = next(p for p in bt_sys_props if p["KEY"] == "source_model_name")
        assert model_prop["VALUE"] == "Maalinger.Blodtryk"

        status_prop = next(p for p in bt_sys_props if p["KEY"] == "status")
        assert status_prop["VALUE"] == "active"

    @pytest.mark.unit
    def test_gcs_full_pipeline(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(GCS_XML, aktivitets_id=200)
        elements = extract_sfi_elements(rows, "RM_Generelle.GCS", "1.0")
        concepts, properties, _ = build_sfi_codesystem(elements)

        assert len(concepts) == 4
        concept_codes = {c["CODE"] for c in concepts}
        total_code = "RM_Generelle_GCS.edGCSTotalScore"
        assert total_code in concept_codes

        total_props = [p for p in properties if p["CODE"] == total_code]
        datatype_prop = next(p for p in total_props if p["KEY"] == "datatype")
        assert datatype_prop["VALUE"] == "integer"

    @pytest.mark.unit
    def test_klynge_selektion_full_pipeline(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SELEKTION_XML, aktivitets_id=300)
        elements = extract_sfi_elements(rows, "Ernaering.Vurdering", "1.0")
        concepts, properties, hierarchy = build_sfi_codesystem(elements)

        concept_codes = {c["CODE"] for c in concepts}
        assert "Ernaering_Vurdering.edKosttype" in concept_codes
        assert "Ernaering_Vurdering.edVaegt" in concept_codes

        vaegt_props = [p for p in properties if p["CODE"] == "Ernaering_Vurdering.edVaegt"]
        datatype_prop = next(p for p in vaegt_props if p["KEY"] == "datatype")
        assert datatype_prop["VALUE"] == "decimal"

        unit_prop = next(p for p in vaegt_props if p["KEY"] == "unit")
        assert unit_prop["VALUE"] == "kg"

        assert len(hierarchy) > 0

    @pytest.mark.unit
    def test_fallback_code_full_pipeline(self) -> None:
        rows, _ = parse_aktivitetsresultat_xml(NO_ELEMENT_ID_XML, aktivitets_id=400)
        elements = extract_sfi_elements(rows, "TestType", "1.0")
        concepts, properties, _ = build_sfi_codesystem(elements)

        assert len(concepts) == 1
        # Fallback code is still qualified: "TestType._fb_<name>"
        assert concepts[0]["CODE"].startswith("TestType.")
        assert "_fb_" in concepts[0]["CODE"]

        status_prop = next(
            p for p in properties if p["CODE"] == concepts[0]["CODE"] and p["KEY"] == "status"
        )
        assert status_prop["VALUE"] == "review"


# ---------------------------------------------------------------------------
# Test: summarize_sfi_conflicts (F6 — conflicts observable)
# ---------------------------------------------------------------------------


def _elem(version: str, datatype: str = "integer", unit: str = "mmHg") -> SfiElement:
    return SfiElement(
        code="BT_SYS",
        display="Systolisk blodtryk",
        datatype=datatype,
        unit=unit,
        sfi_path="BT_SYS",
        source_element_id="BT_SYS",
        source_element_name="BT_SYS",
        source_model_name="Maalinger.Blodtryk",
        source_model_version=version,
        status="active",
        parent_code="",
    )


class TestSummarizeSfiConflicts:
    @pytest.mark.unit
    def test_no_conflict_reports_zero(self) -> None:
        _, properties, _ = build_sfi_codesystem([_elem("1.0"), _elem("2.0")])
        summary = summarize_sfi_conflicts(properties)
        assert summary["conflict_total"] == 0
        assert summary["conflict_fields_tally"] == {}
        assert summary["conflict_sample_codes"] == []

    @pytest.mark.unit
    def test_datatype_conflict_is_counted(self) -> None:
        # Same code, two versions with different datatype → conflict.
        _, properties, _ = build_sfi_codesystem(
            [_elem("1.0", datatype="integer"), _elem("2.0", datatype="decimal")]
        )
        summary = summarize_sfi_conflicts(properties)
        assert summary["conflict_total"] == 1
        assert summary["conflict_fields_tally"].get("datatype") == 1
        assert summary["conflict_sample_codes"] == ["BT_SYS"]

    @pytest.mark.unit
    def test_sample_codes_capped_and_sorted(self) -> None:
        properties = [
            {"CODE": code, "KEY": "conflict_fields", "VALUE": "unit:a,b", "TYPE": "string"}
            for code in ["E", "D", "C", "B", "A", "F", "G"]
        ]
        summary = summarize_sfi_conflicts(properties, sample_limit=3)
        assert summary["conflict_total"] == 7
        assert summary["conflict_sample_codes"] == ["A", "B", "C"]
        assert summary["conflict_fields_tally"] == {"unit": 7}
