# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Comprehensive unit tests for the Python XML parser (xml_aktivitetsresultat.py).

Tests cover all edge cases from the original C# SSIS ScriptMain:
- Simple elements at root level
- Nested Klynge (sekvens / selektion)
- Repetition nodes
- Deep nesting (Klynge > Repetition > Klynge > Element)
- PersonaleRolle special handling
- Kontekst parsing (Oprettet, Aendret, Besluttet, Status)
- Error handling (null XML, malformed XML, empty XML)
- Empty result sets (no elements)
- trimReplace behavior
"""

from DSA.FHIR.utils.code_normalization import split_aktivitettype_kode
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    _trim_xml,
    parse_aktivitetsresultat_xml,
    parse_xml_components_fast,
)

# =============================================================================
# Test XML Fixtures
# =============================================================================

SIMPLE_ELEMENT_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Ledetekst>Vitale værdier</Ledetekst>
  <Type>Observation</Type>
  <Kladde>false</Kladde>
  <Element type="string">
    <Navn>Systolisk</Navn>
    <Ledetekst>Systolisk blodtryk</Ledetekst>
    <Vaerdi>120</Vaerdi>
    <ElementId>E001</ElementId>
    <Kode>ZZ3170</Kode>
  </Element>
  <Element type="string">
    <Navn>Diastolisk</Navn>
    <Ledetekst>Diastolisk blodtryk</Ledetekst>
    <Vaerdi>80</Vaerdi>
    <ElementId>E002</ElementId>
    <Kode>ZZ3172</Kode>
  </Element>
</Afdelingsydelsesresultat>"""

KLYNGE_SEKVENS_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Undersøgelse</Overskrift>
  <Klynge type="sekvens">
    <Navn>VitalerGruppe</Navn>
    <Ledetekst>Vitale målinger</Ledetekst>
    <Element type="number">
      <Navn>Temperatur</Navn>
      <Ledetekst>Kropstemperatur</Ledetekst>
      <Vaerdi>37.2</Vaerdi>
      <ElementId>E010</ElementId>
    </Element>
    <Element type="number">
      <Navn>Puls</Navn>
      <Ledetekst>Hjertefrekvens</Ledetekst>
      <Vaerdi>72</Vaerdi>
      <ElementId>E011</ElementId>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>"""

KLYNGE_SELEKTION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Diagnose</Overskrift>
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

REPETITION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Medicin</Overskrift>
  <Repetition>
    <Element type="string">
      <Navn>Lægemiddel</Navn>
      <Ledetekst>Medicinnavn</Ledetekst>
      <Vaerdi>Metoprolol</Vaerdi>
    </Element>
    <Element type="string">
      <Navn>Dosis</Navn>
      <Ledetekst>Dosis</Ledetekst>
      <Vaerdi>50mg</Vaerdi>
    </Element>
  </Repetition>
  <Repetition>
    <Element type="string">
      <Navn>Lægemiddel</Navn>
      <Ledetekst>Medicinnavn</Ledetekst>
      <Vaerdi>Amlodipine</Vaerdi>
    </Element>
    <Element type="string">
      <Navn>Dosis</Navn>
      <Ledetekst>Dosis</Ledetekst>
      <Vaerdi>5mg</Vaerdi>
    </Element>
  </Repetition>
</Afdelingsydelsesresultat>"""

DEEP_NESTING_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Komplex</Overskrift>
  <Klynge type="sekvens">
    <Navn>Niveau1</Navn>
    <Ledetekst>Første niveau</Ledetekst>
    <Klynge type="sekvens">
      <Navn>Niveau2</Navn>
      <Ledetekst>Andet niveau</Ledetekst>
      <Repetition>
        <Element type="string">
          <Navn>DybtElement</Navn>
          <Ledetekst>Dyb værdi</Ledetekst>
          <Vaerdi>NestedValue</Vaerdi>
          <ElementId>DEEP001</ElementId>
        </Element>
      </Repetition>
    </Klynge>
  </Klynge>
</Afdelingsydelsesresultat>"""

PERSONALE_ROLLE_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Personale</Overskrift>
  <Element type="hidden">
    <Navn>PersonaleRolle</Navn>
    <Ledetekst>Rolle</Ledetekst>
    <Vaerdi>Sygeplejerske</Vaerdi>
  </Element>
  <Element type="string">
    <Navn>Personale</Navn>
    <Ledetekst>Udførende</Ledetekst>
    <Vaerdi>Marie Hansen</Vaerdi>
    <PersId>P12345</PersId>
  </Element>
</Afdelingsydelsesresultat>"""

FULL_KONTEKST_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Fuld kontekst</Overskrift>
  <Kontekst>
    <Oprettet>
      <Bruger>OpretBruger</Bruger>
      <PersId>OP001</PersId>
      <Rolle>Læge</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-15T10:30:00</Tidspunkt>
      <Tidsstempel>1705312200</Tidsstempel>
    </Oprettet>
    <Aendret>
      <Bruger>AendretBruger</Bruger>
      <PersId>AE001</PersId>
      <Rolle>Sygeplejerske</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-16T14:00:00</Tidspunkt>
      <Tidsstempel>1705413600</Tidsstempel>
    </Aendret>
    <Besluttet>
      <Bruger>BesluttetBruger</Bruger>
      <PersId>BE001</PersId>
      <Rolle>Overlæge</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-17T09:00:00</Tidspunkt>
      <Tidsstempel>1705482000</Tidsstempel>
    </Besluttet>
  </Kontekst>
  <Element type="string">
    <Navn>Status</Navn>
    <Ledetekst>Status</Ledetekst>
    <Vaerdi>Afsluttet</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""

EMPTY_ELEMENTS_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Tom</Overskrift>
  <Kontekst>
    <Oprettet>
      <Bruger>TestBruger</Bruger>
    </Oprettet>
  </Kontekst>
</Afdelingsydelsesresultat>"""

WHITESPACE_XML = """<Afdelingsydelsesresultat>
  <Overskrift>  Mellemrum  </Overskrift>
  <Element type="string">
    <Navn>  SpacedNavn  </Navn>
    <Ledetekst>  Spaced Label  </Ledetekst>
    <Vaerdi>  Spaced Value  </Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""

NEWLINE_VALUES_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Newline
Test</Overskrift>
  <Element type="text">
    <Navn>Notat</Navn>
    <Ledetekst>Fritekst</Ledetekst>
    <Vaerdi>Linje1
Linje2
Linje3</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""

MIXED_KLYNGE_AND_ELEMENT_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Blandet</Overskrift>
  <Element type="string">
    <Navn>TopElement</Navn>
    <Ledetekst>Top</Ledetekst>
    <Vaerdi>TopValue</Vaerdi>
  </Element>
  <Klynge type="sekvens">
    <Navn>Gruppe1</Navn>
    <Ledetekst>Første gruppe</Ledetekst>
    <Element type="string">
      <Navn>GruppeElement</Navn>
      <Ledetekst>I gruppe</Ledetekst>
      <Vaerdi>GruppeValue</Vaerdi>
    </Element>
  </Klynge>
  <Element type="string">
    <Navn>BundElement</Navn>
    <Ledetekst>Bund</Ledetekst>
    <Vaerdi>BundValue</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>"""

ELEMENT_WITH_CODES_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Koder</Overskrift>
  <Element type="string">
    <Navn>Procedure</Navn>
    <Ledetekst>Proceduretype</Ledetekst>
    <Vaerdi>Blodprøve</Vaerdi>
    <ElementId>PROC001</ElementId>
    <Kode>DNKA00</Kode>
    <Originalkode>DNKA00.1</Originalkode>
    <AdministrativEnhedId>ADM001</AdministrativEnhedId>
    <PersId>PERS001</PersId>
    <MOID>MOID001</MOID>
  </Element>
</Afdelingsydelsesresultat>"""


# =============================================================================
# Unit Tests: Error Handling
# =============================================================================


class TestXmlParserErrorHandling:
    def test_null_xml(self):
        rows, ctx = parse_aktivitetsresultat_xml(None, aktivitets_id=1)
        assert len(rows) == 1
        assert rows[0].xml_status == -1
        assert rows[0].xml_status_text == "XML Fejl"
        assert rows[0].aktivitets_id == 1

    def test_empty_string_xml(self):
        rows, ctx = parse_aktivitetsresultat_xml("", aktivitets_id=2)
        assert len(rows) == 1
        assert rows[0].xml_status == -1

    def test_whitespace_only_xml(self):
        rows, ctx = parse_aktivitetsresultat_xml("   \n\t  ", aktivitets_id=3)
        assert len(rows) == 1
        assert rows[0].xml_status == -1

    def test_malformed_xml(self):
        rows, ctx = parse_aktivitetsresultat_xml("<bad><xml", aktivitets_id=4)
        assert len(rows) == 1
        assert rows[0].xml_status == -1
        assert rows[0].xml_status_text == "XML Fejl"

    def test_kontekst_returned_on_error(self):
        _rows, ctx = parse_aktivitetsresultat_xml(None, aktivitets_id=5)
        assert isinstance(ctx, AktivitetskontekstRow)
        assert ctx.aktivitets_id == 5


# =============================================================================
# Unit Tests: Simple Elements
# =============================================================================


class TestXmlParserSimpleElements:
    def test_two_root_elements(self):
        rows, ctx = parse_aktivitetsresultat_xml(SIMPLE_ELEMENT_XML, aktivitets_id=10)
        assert len(rows) == 2
        assert all(r.xml_status == 1 for r in rows)
        assert all(r.raekke_type == "Element" for r in rows)

    def test_element_values(self):
        rows, _ = parse_aktivitetsresultat_xml(SIMPLE_ELEMENT_XML, aktivitets_id=10)
        assert rows[0].element_navn == "Systolisk"
        assert rows[0].element_ledetekst == "Systolisk blodtryk"
        assert rows[0].element_vaerdi == "120"
        assert rows[0].element_element_id == "E001"
        assert rows[0].element_kode == "ZZ3170"
        assert rows[0].element_type == "string"

    def test_element_counters(self):
        rows, _ = parse_aktivitetsresultat_xml(SIMPLE_ELEMENT_XML, aktivitets_id=10)
        assert rows[0].element_nummer == 1
        assert rows[1].element_nummer == 2

    def test_overskrift_propagated(self):
        rows, _ = parse_aktivitetsresultat_xml(SIMPLE_ELEMENT_XML, aktivitets_id=10)
        assert rows[0].overskrift == "Blodtryk"
        assert rows[1].overskrift == "Blodtryk"

    def test_no_klynge_at_root(self):
        rows, _ = parse_aktivitetsresultat_xml(SIMPLE_ELEMENT_XML, aktivitets_id=10)
        assert rows[0].klynge_id is None
        assert rows[0].klynge_niveau == 0
        assert rows[0].klynge_id_hierarki is None

    def test_element_all_codes(self):
        rows, _ = parse_aktivitetsresultat_xml(ELEMENT_WITH_CODES_XML, aktivitets_id=50)
        r = rows[0]
        assert r.element_element_id == "PROC001"
        assert r.element_kode == "DNKA00"
        assert r.element_original_kode == "DNKA00.1"
        assert r.element_administrativ_enhed_id == "ADM001"
        assert r.element_pers_id == "PERS001"
        assert r.element_moid == "MOID001"


# =============================================================================
# Unit Tests: Klynge (Sekvens)
# =============================================================================


class TestXmlParserKlyngeSekvens:
    def test_elements_inside_sekvens(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SEKVENS_XML, aktivitets_id=20)
        assert len(rows) == 2
        assert rows[0].element_navn == "Temperatur"
        assert rows[1].element_navn == "Puls"

    def test_klynge_metadata(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SEKVENS_XML, aktivitets_id=20)
        assert rows[0].klynge_id == 1
        assert rows[0].klynge_niveau == 1
        assert rows[0].klynge_navn == "VitalerGruppe"
        assert rows[0].klynge_ledetekst == "Vitale målinger"
        assert rows[0].klynge_type == "sekvens"

    def test_klynge_hierarki(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SEKVENS_XML, aktivitets_id=20)
        assert rows[0].klynge_id_hierarki == "1"
        assert rows[0].hierarki == "Kly1"


# =============================================================================
# Unit Tests: Klynge (Selektion)
# =============================================================================


class TestXmlParserKlyngeSelektion:
    def test_selektion_produces_selektion_row(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SELEKTION_XML, aktivitets_id=21)
        selektion_rows = [r for r in rows if r.raekke_type == "Selektion"]
        assert len(selektion_rows) == 1

    def test_selektion_metadata(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SELEKTION_XML, aktivitets_id=21)
        sel = [r for r in rows if r.raekke_type == "Selektion"][0]
        assert sel.klynge_navn == "DiagnoseValg"
        assert sel.klynge_ledetekst == "Vælg diagnose"
        assert sel.klynge_valg_ledetekst == "Valgt: Hypertension"
        assert sel.klynge_type == "selektion"

    def test_selektion_plus_child_element(self):
        rows, _ = parse_aktivitetsresultat_xml(KLYNGE_SELEKTION_XML, aktivitets_id=21)
        assert len(rows) == 2  # 1 Selektion + 1 Element
        assert rows[0].raekke_type == "Selektion"
        assert rows[1].raekke_type == "Element"
        assert rows[1].element_navn == "DiagnoseKode"


# =============================================================================
# Unit Tests: Repetition
# =============================================================================


class TestXmlParserRepetition:
    def test_repetition_elements(self):
        rows, _ = parse_aktivitetsresultat_xml(REPETITION_XML, aktivitets_id=30)
        assert len(rows) == 4

    def test_repetition_id_increments(self):
        rows, _ = parse_aktivitetsresultat_xml(REPETITION_XML, aktivitets_id=30)
        rep1_rows = [r for r in rows if r.repetition_id == 1]
        rep2_rows = [r for r in rows if r.repetition_id == 2]
        assert len(rep1_rows) == 2
        assert len(rep2_rows) == 2

    def test_repetition_hierarki(self):
        rows, _ = parse_aktivitetsresultat_xml(REPETITION_XML, aktivitets_id=30)
        assert rows[0].hierarki == "Rep1"
        assert rows[0].repetition_niveau == 1
        assert rows[2].hierarki == "Rep2"


# =============================================================================
# Unit Tests: Deep Nesting
# =============================================================================


class TestXmlParserDeepNesting:
    def test_deep_nested_element(self):
        rows, _ = parse_aktivitetsresultat_xml(DEEP_NESTING_XML, aktivitets_id=40)
        assert len(rows) == 1
        r = rows[0]
        assert r.element_navn == "DybtElement"
        assert r.element_vaerdi == "NestedValue"

    def test_deep_nesting_levels(self):
        rows, _ = parse_aktivitetsresultat_xml(DEEP_NESTING_XML, aktivitets_id=40)
        r = rows[0]
        assert r.klynge_niveau == 2
        assert r.repetition_niveau == 1

    def test_deep_nesting_hierarki(self):
        rows, _ = parse_aktivitetsresultat_xml(DEEP_NESTING_XML, aktivitets_id=40)
        r = rows[0]
        assert "Kly1" in r.hierarki
        assert "Kly2" in r.hierarki
        assert "Rep1" in r.hierarki

    def test_deep_nesting_klynge_parent(self):
        rows, _ = parse_aktivitetsresultat_xml(DEEP_NESTING_XML, aktivitets_id=40)
        r = rows[0]
        assert r.klynge_parent_id == 1
        assert r.klynge_id == 2


# =============================================================================
# Unit Tests: PersonaleRolle Special Handling
# =============================================================================


PERSONALE_ROLLE_IN_REPETITION_XML = """<Afdelingsydelsesresultat>
  <Overskrift>Personale</Overskrift>
  <Repetition>
    <Element type="hidden">
      <Navn>PersonaleRolle</Navn>
      <Ledetekst>Rolle</Ledetekst>
      <Vaerdi>Sygeplejerske</Vaerdi>
    </Element>
    <Element type="string">
      <Navn>Personale</Navn>
      <Ledetekst>Udførende</Ledetekst>
      <Vaerdi>Marie Hansen</Vaerdi>
      <PersId>P12345</PersId>
    </Element>
  </Repetition>
</Afdelingsydelsesresultat>"""


class TestXmlParserPersonaleRolle:
    def test_personale_rolle_consumed_at_top_level(self):
        """PersonaleRolle at top level: consumed but reset before next sibling."""
        rows, _ = parse_aktivitetsresultat_xml(PERSONALE_ROLLE_XML, aktivitets_id=60)
        assert len(rows) == 1
        assert rows[0].element_navn == "Personale"
        assert rows[0].element_ledetekst == "Udførende"

    def test_personale_rolle_in_repetition(self):
        """PersonaleRolle inside Repetition: ledetekst override works."""
        rows, _ = parse_aktivitetsresultat_xml(PERSONALE_ROLLE_IN_REPETITION_XML, aktivitets_id=61)
        assert len(rows) == 1
        assert rows[0].element_navn == "Personale"
        assert rows[0].element_ledetekst == "Sygeplejerske"

    def test_personale_pers_id(self):
        rows, _ = parse_aktivitetsresultat_xml(PERSONALE_ROLLE_XML, aktivitets_id=60)
        assert rows[0].element_pers_id == "P12345"


# =============================================================================
# Unit Tests: Kontekst Parsing
# =============================================================================


class TestXmlParserKontekst:
    def test_full_kontekst(self):
        _, ctx = parse_aktivitetsresultat_xml(FULL_KONTEKST_XML, aktivitets_id=70)
        assert ctx.oprettet_bruger == "OpretBruger"
        assert ctx.oprettet_pers_id == "OP001"
        assert ctx.oprettet_rolle == "Læge"
        assert ctx.oprettet_adm_enhed == "Kardiologi"
        assert ctx.oprettet_adm_enhed_id == "AE001"
        assert ctx.oprettet_tidsstempel == 1705312200

    def test_aendret_kontekst(self):
        _, ctx = parse_aktivitetsresultat_xml(FULL_KONTEKST_XML, aktivitets_id=70)
        assert ctx.aendret_bruger == "AendretBruger"
        assert ctx.aendret_pers_id == "AE001"
        assert ctx.aendret_rolle == "Sygeplejerske"
        assert ctx.aendret_tidsstempel == 1705413600

    def test_besluttet_kontekst(self):
        _, ctx = parse_aktivitetsresultat_xml(FULL_KONTEKST_XML, aktivitets_id=70)
        assert ctx.besluttet_bruger == "BesluttetBruger"
        assert ctx.besluttet_pers_id == "BE001"
        assert ctx.besluttet_rolle == "Overlæge"
        assert ctx.besluttet_tidsstempel == 1705482000


# =============================================================================
# Unit Tests: Empty Results
# =============================================================================


class TestXmlParserEmptyResults:
    def test_no_elements_produces_tom_row(self):
        rows, _ = parse_aktivitetsresultat_xml(EMPTY_ELEMENTS_XML, aktivitets_id=80)
        assert len(rows) == 1
        assert rows[0].raekke_type == "Tom"
        assert rows[0].xml_status == 0
        assert rows[0].xml_status_text == "Ingen Elementer"

    def test_empty_has_overskrift(self):
        rows, _ = parse_aktivitetsresultat_xml(EMPTY_ELEMENTS_XML, aktivitets_id=80)
        assert rows[0].overskrift == "Tom"


# =============================================================================
# Unit Tests: trimReplace Behavior
# =============================================================================


class TestXmlParsertrimReplace:
    def test_newlines_in_overskrift_removed(self):
        rows, _ = parse_aktivitetsresultat_xml(NEWLINE_VALUES_XML, aktivitets_id=90)
        assert rows[0].overskrift == "NewlineTest"

    def test_value_not_trimmed(self):
        """Element values are NOT trimReplaced in C# (stored raw)."""
        rows, _ = parse_aktivitetsresultat_xml(NEWLINE_VALUES_XML, aktivitets_id=90)
        assert "Linje1" in rows[0].element_vaerdi
        assert "Linje2" in rows[0].element_vaerdi


# =============================================================================
# Unit Tests: Mixed Structure
# =============================================================================


class TestXmlParserMixedStructure:
    def test_mixed_count(self):
        rows, _ = parse_aktivitetsresultat_xml(MIXED_KLYNGE_AND_ELEMENT_XML, aktivitets_id=100)
        assert len(rows) == 3

    def test_top_level_element_no_klynge(self):
        rows, _ = parse_aktivitetsresultat_xml(MIXED_KLYNGE_AND_ELEMENT_XML, aktivitets_id=100)
        top = rows[0]
        assert top.element_navn == "TopElement"
        assert top.klynge_id is None

    def test_grouped_element_has_klynge(self):
        rows, _ = parse_aktivitetsresultat_xml(MIXED_KLYNGE_AND_ELEMENT_XML, aktivitets_id=100)
        grouped = rows[1]
        assert grouped.element_navn == "GruppeElement"
        assert grouped.klynge_id == 1

    def test_bottom_element_after_klynge(self):
        rows, _ = parse_aktivitetsresultat_xml(MIXED_KLYNGE_AND_ELEMENT_XML, aktivitets_id=100)
        bottom = rows[2]
        assert bottom.element_navn == "BundElement"
        assert bottom.klynge_id is None


# =============================================================================
# Unit Tests: Whitespace Handling
# =============================================================================


class TestXmlParserWhitespace:
    def test_trimmed_navn(self):
        rows, _ = parse_aktivitetsresultat_xml(WHITESPACE_XML, aktivitets_id=110)
        assert rows[0].element_navn == "SpacedNavn"

    def test_trimmed_ledetekst(self):
        rows, _ = parse_aktivitetsresultat_xml(WHITESPACE_XML, aktivitets_id=110)
        assert rows[0].element_ledetekst == "Spaced Label"

    def test_trimmed_overskrift(self):
        rows, _ = parse_aktivitetsresultat_xml(WHITESPACE_XML, aktivitets_id=110)
        assert rows[0].overskrift == "Mellemrum"


# =============================================================================
# Unit Tests: _trim_xml Defensive Trimming
# =============================================================================


class TestTrimXml:
    def test_clean_xml_unchanged(self):
        xml = "<Afdelingsydelsesresultat><Element/></Afdelingsydelsesresultat>"
        assert _trim_xml(xml) == xml

    def test_trailing_whitespace_trimmed(self):
        xml = "<Afdelingsydelsesresultat><Element/></Afdelingsydelsesresultat>   \n\t  "
        result = _trim_xml(xml)
        assert result.endswith("</Afdelingsydelsesresultat>")
        assert "\n" not in result.split("</Afdelingsydelsesresultat>")[-1]

    def test_trailing_garbage_trimmed(self):
        xml = "<Afdelingsydelsesresultat><Element/></Afdelingsydelsesresultat>GARBAGE DATA"
        result = _trim_xml(xml)
        assert result == "<Afdelingsydelsesresultat><Element/></Afdelingsydelsesresultat>"

    def test_no_end_tag_returns_original(self):
        xml = "<SomeOtherRoot/>"
        assert _trim_xml(xml) == xml

    def test_parser_succeeds_with_trailing_garbage(self):
        xml = (
            "<Afdelingsydelsesresultat>"
            "<Element type='string'><Navn>Test</Navn><Vaerdi>V</Vaerdi></Element>"
            "</Afdelingsydelsesresultat>"
            "    trailing noise here"
        )
        rows, _ = parse_aktivitetsresultat_xml(xml, aktivitets_id=999)
        assert len(rows) == 1
        assert rows[0].xml_status == 1
        assert rows[0].element_navn == "Test"

    def test_fast_parser_succeeds_with_trailing_garbage(self):
        xml = (
            "<Afdelingsydelsesresultat>"
            "<Element type='string'><Navn>Test</Navn><Vaerdi>V</Vaerdi></Element>"
            "</Afdelingsydelsesresultat>"
            "EXTRA BYTES"
        )
        results, errors = parse_xml_components_fast(xml)
        assert errors == 0
        assert len(results) == 1
        assert results[0] == ("Test", "V")


# =============================================================================
# Unit Tests: split_aktivitettype_kode
# =============================================================================


class TestSplitAktivitettypeKode:
    def test_none_returns_8_nones(self):
        result = split_aktivitettype_kode(None)
        assert result == [None] * 8

    def test_empty_string_returns_8_nones(self):
        result = split_aktivitettype_kode("")
        assert result == [None] * 8

    def test_single_level(self):
        result = split_aktivitettype_kode("ZZ3170")
        assert result[0] == "ZZ3170"
        assert all(v is None for v in result[1:])

    def test_three_levels(self):
        result = split_aktivitettype_kode("DI10.1.2")
        assert result[0] == "DI10"
        assert result[1] == "1"
        assert result[2] == "2"
        assert all(v is None for v in result[3:])

    def test_eight_levels(self):
        result = split_aktivitettype_kode("A.B.C.D.E.F.G.H")
        assert result == ["A", "B", "C", "D", "E", "F", "G", "H"]

    def test_more_than_eight_levels_truncated(self):
        result = split_aktivitettype_kode("1.2.3.4.5.6.7.8.9.10")
        assert len(result) == 8
        assert result[7] == "8"

    def test_sfi_code(self):
        result = split_aktivitettype_kode("SFI.2.1")
        assert result[0] == "SFI"
        assert result[1] == "2"
        assert result[2] == "1"

    def test_integer_input(self):
        result = split_aktivitettype_kode(12345)
        assert result[0] == "12345"
        assert all(v is None for v in result[1:])


# =============================================================================
# Unit Tests: Sekvens/Selektion wrapper passthrough (IFDB production structure)
#
# Real IFDB XML wraps Elements/Repetitions inside <Sekvens> or <Selektion>
# container tags that live between <Klynge> and the leaves, e.g.
#
#   <Klynge type="sekvens">
#     <Sekvens>
#       <Repetition>
#         <Element>...</Element>
#       </Repetition>
#     </Sekvens>
#   </Klynge>
#
# The older fixtures in this file omitted those wrappers, which masked a bug
# where the parser silently dropped everything inside <Sekvens>/<Selektion>.
# =============================================================================


KLYNGE_SEKVENS_WITH_SEKVENS_WRAPPER_XML = """<Afdelingsydelsesresultat>
  <Overskrift>SO16Ernaer</Overskrift>
  <Repetition>
    <Klynge type="sekvens">
      <Navn>SO16ErnaerAppetitSekv</Navn>
      <Ledetekst>Appetit</Ledetekst>
      <Sekvens>
        <Repetition>
          <Element type="string">
            <Navn>SO16ErnaerAppetit</Navn>
            <Ledetekst>Supplerende</Ledetekst>
            <Vaerdi>Spiser dårligt, har kvalme.</Vaerdi>
            <ElementId>RM_Generelle2:edSO16ErnaerAppetit</ElementId>
          </Element>
        </Repetition>
      </Sekvens>
    </Klynge>
  </Repetition>
</Afdelingsydelsesresultat>"""


SO16_FUNK_FAERDIGHED_REAL_XML = """<Afdelingsydelsesresultat>
  <Overskrift>SO16FunkFaerdighed</Overskrift>
  <Type>SO16FunkFaerdighed</Type>
  <Repetition>
    <Klynge type="sekvens">
      <Navn>SO16FunkFaerdMobiliSekv</Navn>
      <Ledetekst>Evne til at bevæge sig omkring:</Ledetekst>
      <Sekvens>
        <Repetition>
          <Element type="klassificering">
            <Navn>SO16FunkFaerdBevaegKlassif</Navn>
            <Ledetekst>Evne til at bevæge sig omkring</Ledetekst>
            <Vaerdi>Fuldt mobil</Vaerdi>
            <ElementId>RM_Generelle2:edSO16FunkFaerdBevaegKlassif</ElementId>
            <Kode>SO16FunkFaerdBevaeg0100</Kode>
          </Element>
        </Repetition>
        <Repetition>
          <Element type="string">
            <Navn>SO16FunkFaerdMobiliTidUdAfSengTXT</Navn>
            <Ledetekst>Tid ude af sengen</Ledetekst>
            <Vaerdi>2 timer</Vaerdi>
            <ElementId>RM_Generelle2:edSO16FunkFaerdMobiliTidUdAfSengTXT</ElementId>
          </Element>
        </Repetition>
      </Sekvens>
    </Klynge>
  </Repetition>
</Afdelingsydelsesresultat>"""


KLYNGE_SELEKTION_WITH_SELEKTION_WRAPPER_XML = """<Afdelingsydelsesresultat>
  <Overskrift>SelektionWrapper</Overskrift>
  <Selektion>
    <Klynge type="selektion">
      <Navn>VandlTrang</Navn>
      <Ledetekst>Vandladningstrang</Ledetekst>
      <ValgLedetekst>Hyppig trang</ValgLedetekst>
      <Element type="string">
        <Navn>VandlTrangBeskrivelse</Navn>
        <Ledetekst>Beskrivelse</Ledetekst>
        <Vaerdi>Hyppig</Vaerdi>
        <ElementId>RM_Generelle2:edVandlTrangBeskrivelse</ElementId>
      </Element>
    </Klynge>
  </Selektion>
</Afdelingsydelsesresultat>"""


class TestXmlParserSekvensWrapper:
    """Production IFDB XML wraps content in <Sekvens>; parser must recurse into it."""

    def test_element_inside_sekvens_wrapper_is_found(self):
        rows, _ = parse_aktivitetsresultat_xml(
            KLYNGE_SEKVENS_WITH_SEKVENS_WRAPPER_XML, aktivitets_id=200
        )
        element_rows = [r for r in rows if r.raekke_type == "Element"]
        assert len(element_rows) == 1
        assert element_rows[0].element_navn == "SO16ErnaerAppetit"
        assert element_rows[0].element_vaerdi == "Spiser dårligt, har kvalme."

    def test_element_inside_sekvens_keeps_klynge_context(self):
        rows, _ = parse_aktivitetsresultat_xml(
            KLYNGE_SEKVENS_WITH_SEKVENS_WRAPPER_XML, aktivitets_id=200
        )
        element_rows = [r for r in rows if r.raekke_type == "Element"]
        assert element_rows[0].klynge_navn == "SO16ErnaerAppetitSekv"
        assert element_rows[0].klynge_ledetekst == "Appetit"
        assert element_rows[0].klynge_type == "sekvens"
        assert element_rows[0].klynge_niveau == 1

    def test_multiple_elements_under_one_sekvens_all_found(self):
        rows, _ = parse_aktivitetsresultat_xml(SO16_FUNK_FAERDIGHED_REAL_XML, aktivitets_id=201)
        element_rows = [r for r in rows if r.raekke_type == "Element"]
        assert len(element_rows) == 2
        navne = {r.element_navn for r in element_rows}
        assert navne == {
            "SO16FunkFaerdBevaegKlassif",
            "SO16FunkFaerdMobiliTidUdAfSengTXT",
        }


class TestXmlParserSelektionWrapper:
    """<Selektion> container wraps a <Klynge> in some IFDB models."""

    def test_klynge_inside_selektion_wrapper_emits_selektion_row(self):
        rows, _ = parse_aktivitetsresultat_xml(
            KLYNGE_SELEKTION_WITH_SELEKTION_WRAPPER_XML, aktivitets_id=202
        )
        selektion_rows = [r for r in rows if r.raekke_type == "Selektion"]
        assert len(selektion_rows) == 1
        assert selektion_rows[0].klynge_navn == "VandlTrang"

    def test_element_inside_selektion_wrapper_is_found(self):
        rows, _ = parse_aktivitetsresultat_xml(
            KLYNGE_SELEKTION_WITH_SELEKTION_WRAPPER_XML, aktivitets_id=202
        )
        element_rows = [r for r in rows if r.raekke_type == "Element"]
        assert len(element_rows) == 1
        assert element_rows[0].element_navn == "VandlTrangBeskrivelse"
