# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
End-to-end validation: runs both C# and Python XML parsers on identical
test data and asserts that outputs match field-by-field.

Requires .NET 8 SDK installed at /home/vscode/.dotnet.
The C# project is at tests/validation/csharp_parser/XmlParserValidator/.

Test data covers all structural patterns from the C# SSIS ScriptMain:
- Simple elements
- Klynge sekvens / selektion
- Repetition
- Deep nesting
- PersonaleRolle special handling
- Error cases
- Mixed structures
- Real-world-like clinical XML
"""

import json
import os
import subprocess
from pathlib import Path

import pytest
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    parse_aktivitetsresultat_xml,
)

DOTNET_PATH = "/home/vscode/.dotnet/dotnet"
CSHARP_PROJECT = str(Path(__file__).parent / "csharp_parser" / "XmlParserValidator")


def csharp_available() -> bool:
    return os.path.exists(DOTNET_PATH) and os.path.exists(
        os.path.join(CSHARP_PROJECT, "XmlParserValidator.csproj")
    )


pytestmark = [
    pytest.mark.skipif(
        not csharp_available(),
        reason="C# .NET SDK or project not available",
    ),
]

# =============================================================================
# Test Cases: Each dict has test_id, xml, aktivitets_id
# =============================================================================

TEST_CASES = [
    {
        "test_id": "simple_two_elements",
        "aktivitets_id": 1001,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Blodtryk</Overskrift>
  <Ledetekst>Vitale</Ledetekst>
  <Type>Observation</Type>
  <Kladde>false</Kladde>
  <Element type="string">
    <Navn>Systolisk</Navn>
    <Ledetekst>Systolisk BP</Ledetekst>
    <Vaerdi>120</Vaerdi>
    <ElementId>E001</ElementId>
    <Kode>ZZ3170</Kode>
  </Element>
  <Element type="string">
    <Navn>Diastolisk</Navn>
    <Ledetekst>Diastolisk BP</Ledetekst>
    <Vaerdi>80</Vaerdi>
    <ElementId>E002</ElementId>
  </Element>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "klynge_sekvens",
        "aktivitets_id": 1002,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Vitaler</Overskrift>
  <Klynge type="sekvens">
    <Navn>VitalGruppe</Navn>
    <Ledetekst>Vitale maalinger</Ledetekst>
    <Element type="number">
      <Navn>Temp</Navn>
      <Ledetekst>Temperatur</Ledetekst>
      <Vaerdi>37.2</Vaerdi>
    </Element>
    <Element type="number">
      <Navn>Puls</Navn>
      <Ledetekst>Puls</Ledetekst>
      <Vaerdi>72</Vaerdi>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "klynge_selektion",
        "aktivitets_id": 1003,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Diagnose</Overskrift>
  <Klynge type="selektion">
    <Navn>DiagnoseValg</Navn>
    <Ledetekst>Vaelg diagnose</Ledetekst>
    <ValgLedetekst>Valgt: Hypertension</ValgLedetekst>
    <Element type="string">
      <Navn>Kode</Navn>
      <Ledetekst>ICD-10</Ledetekst>
      <Vaerdi>I10</Vaerdi>
      <Kode>I10</Kode>
    </Element>
  </Klynge>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "repetition",
        "aktivitets_id": 1004,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Medicin</Overskrift>
  <Repetition>
    <Element type="string">
      <Navn>Middel</Navn>
      <Ledetekst>Laeg</Ledetekst>
      <Vaerdi>Metoprolol</Vaerdi>
    </Element>
  </Repetition>
  <Repetition>
    <Element type="string">
      <Navn>Middel</Navn>
      <Ledetekst>Laeg</Ledetekst>
      <Vaerdi>Amlodipine</Vaerdi>
    </Element>
  </Repetition>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "deep_nesting",
        "aktivitets_id": 1005,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Deep</Overskrift>
  <Klynge type="sekvens">
    <Navn>L1</Navn>
    <Ledetekst>Level1</Ledetekst>
    <Klynge type="sekvens">
      <Navn>L2</Navn>
      <Ledetekst>Level2</Ledetekst>
      <Repetition>
        <Element type="string">
          <Navn>DeepElem</Navn>
          <Ledetekst>Deep</Ledetekst>
          <Vaerdi>DeepVal</Vaerdi>
        </Element>
      </Repetition>
    </Klynge>
  </Klynge>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "personale_rolle",
        "aktivitets_id": 1006,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Personale</Overskrift>
  <Element type="hidden">
    <Navn>PersonaleRolle</Navn>
    <Vaerdi>Sygeplejerske</Vaerdi>
  </Element>
  <Element type="string">
    <Navn>Personale</Navn>
    <Ledetekst>Udfoerendes rolle</Ledetekst>
    <Vaerdi>Marie Hansen</Vaerdi>
    <PersId>P123</PersId>
  </Element>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "kontekst_full",
        "aktivitets_id": 1007,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>KontekstTest</Overskrift>
  <Kontekst>
    <Oprettet>
      <bruger>OpretBruger</bruger>
      <PersId>OP001</PersId>
      <Rolle>Laege</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-15T10:30:00</Tidspunkt>
      <Tidsstempel>1705312200</Tidsstempel>
    </Oprettet>
    <Aendret>
      <bruger>AendretBruger</bruger>
      <PersId>AE002</PersId>
      <Rolle>Sygeplejerske</Rolle>
      <AdministrativEnhed>Medicin</AdministrativEnhed>
      <AdministrativEnhedId>AE002</AdministrativEnhedId>
      <Tidspunkt>2024-01-16T14:00:00</Tidspunkt>
      <Tidsstempel>1705413600</Tidsstempel>
    </Aendret>
    <Besluttet>
      <bruger>BeslutBruger</bruger>
      <PersId>BE003</PersId>
      <Rolle>Overlaege</Rolle>
      <AdministrativEnhed>Kardiologi</AdministrativEnhed>
      <AdministrativEnhedId>AE001</AdministrativEnhedId>
      <Tidspunkt>2024-01-17T09:00:00</Tidspunkt>
      <Tidsstempel>1705482000</Tidsstempel>
    </Besluttet>
  </Kontekst>
  <Element type="string">
    <Navn>Note</Navn>
    <Ledetekst>Notat</Ledetekst>
    <Vaerdi>Test notat</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "empty_no_elements",
        "aktivitets_id": 1008,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Tom formular</Overskrift>
  <Kontekst>
    <Oprettet><bruger>Test</bruger></Oprettet>
  </Kontekst>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "null_xml",
        "aktivitets_id": 1009,
        "xml": "",
    },
    {
        "test_id": "malformed_xml",
        "aktivitets_id": 1010,
        "xml": "<broken><xml",
    },
    {
        "test_id": "mixed_top_and_klynge",
        "aktivitets_id": 1011,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Blandet</Overskrift>
  <Element type="string">
    <Navn>Top</Navn>
    <Ledetekst>TopLabel</Ledetekst>
    <Vaerdi>TopVal</Vaerdi>
  </Element>
  <Klynge type="sekvens">
    <Navn>Grp</Navn>
    <Ledetekst>Gruppe</Ledetekst>
    <Element type="string">
      <Navn>GrpElem</Navn>
      <Ledetekst>I gruppe</Ledetekst>
      <Vaerdi>GrpVal</Vaerdi>
    </Element>
  </Klynge>
  <Element type="string">
    <Navn>Bund</Navn>
    <Ledetekst>BundLabel</Ledetekst>
    <Vaerdi>BundVal</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "all_element_fields",
        "aktivitets_id": 1012,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>AllFields</Overskrift>
  <Element type="coded">
    <Navn>FullElement</Navn>
    <Ledetekst>Full ledetekst</Ledetekst>
    <Vaerdi>FullValue</Vaerdi>
    <ElementId>FULL001</ElementId>
    <Kode>ABC123</Kode>
    <Originalkode>ABC123.1</Originalkode>
    <AdministrativEnhedId>ADM001</AdministrativEnhedId>
    <PersId>PERS001</PersId>
    <MOID>MOID001</MOID>
  </Element>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "nested_selektion_in_klynge",
        "aktivitets_id": 1013,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>NestedSel</Overskrift>
  <Klynge type="sekvens">
    <Navn>Outer</Navn>
    <Ledetekst>OuterLabel</Ledetekst>
    <Klynge type="selektion">
      <Navn>InnerSel</Navn>
      <Ledetekst>InnerSelLabel</Ledetekst>
      <ValgLedetekst>Valgt: Option A</ValgLedetekst>
      <Element type="string">
        <Navn>SelElem</Navn>
        <Ledetekst>SelLabel</Ledetekst>
        <Vaerdi>SelVal</Vaerdi>
      </Element>
    </Klynge>
  </Klynge>
</Afdelingsydelsesresultat>""",
    },
    {
        "test_id": "clinical_realistic",
        "aktivitets_id": 1014,
        "xml": """<Afdelingsydelsesresultat>
  <Overskrift>Sygeplejefaglig vurdering</Overskrift>
  <Ledetekst>Sygeplejeanamnese</Ledetekst>
  <Type>Klinisk notat</Type>
  <Kladde>false</Kladde>
  <Kontekst>
    <Oprettet>
      <bruger>SSP Hansen</bruger>
      <PersId>SSP0042</PersId>
      <Rolle>Sygeplejerske</Rolle>
      <AdministrativEnhed>Medicinsk afdeling</AdministrativEnhed>
      <AdministrativEnhedId>MED01</AdministrativEnhedId>
      <Tidspunkt>2024-03-15T08:30:00</Tidspunkt>
      <Tidsstempel>1710491400</Tidsstempel>
    </Oprettet>
    <Besluttet>
      <bruger>OL Petersen</bruger>
      <PersId>OL0001</PersId>
      <Rolle>Overlaege</Rolle>
      <AdministrativEnhed>Medicinsk afdeling</AdministrativEnhed>
      <AdministrativEnhedId>MED01</AdministrativEnhedId>
      <Tidspunkt>2024-03-15T09:15:00</Tidspunkt>
      <Tidsstempel>1710494100</Tidsstempel>
    </Besluttet>
  </Kontekst>
  <Klynge type="sekvens">
    <Navn>Funktionsniveau</Navn>
    <Ledetekst>Funktionsniveau vurdering</Ledetekst>
    <Klynge type="selektion">
      <Navn>Mobilitet</Navn>
      <Ledetekst>Mobilitets vurdering</Ledetekst>
      <ValgLedetekst>Nedsat mobilitet</ValgLedetekst>
      <Element type="string">
        <Navn>MobilitetScore</Navn>
        <Ledetekst>Score</Ledetekst>
        <Vaerdi>3</Vaerdi>
        <Kode>MOB03</Kode>
      </Element>
    </Klynge>
    <Element type="text">
      <Navn>Bemærkning</Navn>
      <Ledetekst>Supplerende bemærkning</Ledetekst>
      <Vaerdi>Patient har brug for rollator</Vaerdi>
    </Element>
  </Klynge>
  <Repetition>
    <Element type="hidden">
      <Navn>PersonaleRolle</Navn>
      <Vaerdi>Sygeplejerske</Vaerdi>
    </Element>
    <Element type="string">
      <Navn>Personale</Navn>
      <Ledetekst>Kontaktperson</Ledetekst>
      <Vaerdi>SSP Hansen</Vaerdi>
      <PersId>SSP0042</PersId>
    </Element>
  </Repetition>
  <Element type="datetime">
    <Navn>VurderingsDato</Navn>
    <Ledetekst>Dato for vurdering</Ledetekst>
    <Vaerdi>2024-03-15T08:30:00</Vaerdi>
  </Element>
</Afdelingsydelsesresultat>""",
    },
]


# =============================================================================
# Helper: Run C# parser on test cases
# =============================================================================


def run_csharp_parser(test_cases: list[dict]) -> list[dict]:
    """Run the C# parser and return parsed JSON results."""
    input_json = json.dumps(test_cases)
    env = os.environ.copy()
    env["PATH"] = f"/home/vscode/.dotnet:{env.get('PATH', '')}"
    env["DOTNET_CLI_HOME"] = "/home/vscode/.dotnet"
    env["DOTNET_SKIP_FIRST_TIME_EXPERIENCE"] = "1"
    env["DOTNET_CLI_TELEMETRY_OPTOUT"] = "1"
    env["DOTNET_NOLOGO"] = "1"

    dll_path = Path(CSHARP_PROJECT) / "bin" / "Release" / "net8.0" / "XmlParserValidator.dll"
    if not dll_path.exists():
        build = subprocess.run(
            ["dotnet", "build", CSHARP_PROJECT, "-c", "Release"],
            capture_output=True,
            text=True,
            env=env,
            timeout=60,
        )
        if build.returncode != 0:
            raise RuntimeError(f"C# build failed: {build.stderr}")

    result = subprocess.run(
        ["dotnet", str(dll_path)],
        input=input_json,
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(f"C# parser failed: {result.stderr}")
    return json.loads(result.stdout)


# =============================================================================
# Helper: Run Python parser on test cases
# =============================================================================


def run_python_parser(test_cases: list[dict]) -> list[dict]:
    """Run the Python parser and return results as dicts matching C# output format."""
    all_results = []
    for tc in test_cases:
        xml = tc["xml"]
        aid = tc["aktivitets_id"]

        if not xml:
            xml = None

        rows, kontekst = parse_aktivitetsresultat_xml(xml_string=xml, aktivitets_id=aid)

        result_rows = []
        for r in rows:
            result_rows.append(
                {
                    "ElementNummer": r.element_nummer,
                    "KlyngeId": r.klynge_id,
                    "KlyngeIdHierarki": r.klynge_id_hierarki,
                    "KlyngeNiveau": r.klynge_niveau,
                    "KlyngeNavne": r.klynge_navne,
                    "KlyngeLedetekster": r.klynge_ledetekster,
                    "KlyngeParentId": r.klynge_parent_id,
                    "RepetitionId": r.repetition_id,
                    "RepetitionIdHierarki": r.repetition_id_hierarki,
                    "RepetitionNiveau": r.repetition_niveau,
                    "Hierarki": r.hierarki,
                    "Overskrift": r.overskrift,
                    "Ledetekst": r.ledetekst,
                    "Type": r.type,
                    "Kladde": r.kladde,
                    "KlyngeNavn": r.klynge_navn,
                    "KlyngeLedetekst": r.klynge_ledetekst,
                    "KlyngeValgLedetekst": r.klynge_valg_ledetekst,
                    "KlyngeType": r.klynge_type,
                    "ElementNavn": r.element_navn,
                    "ElementLedetekst": r.element_ledetekst,
                    "ElementVaerdi": r.element_vaerdi,
                    "ElementElementId": r.element_element_id,
                    "ElementKode": r.element_kode,
                    "ElementOriginalKode": r.element_original_kode,
                    "ElementAdministrativEnhedId": r.element_administrativ_enhed_id,
                    "ElementPersId": r.element_pers_id,
                    "ElementType": r.element_type,
                    "ElementMoid": r.element_moid,
                    "RaekkeType": r.raekke_type,
                    "XmlStatus": r.xml_status,
                    "XmlStatusText": r.xml_status_text,
                }
            )

        kontekst_dict = {
            "AktivitetsId": kontekst.aktivitets_id,
            "OprettetBruger": kontekst.oprettet_bruger,
            "OprettetPersId": kontekst.oprettet_pers_id,
            "OprettetRolle": kontekst.oprettet_rolle,
            "OprettetAdmEnhed": kontekst.oprettet_adm_enhed,
            "OprettetAdmEnhedId": kontekst.oprettet_adm_enhed_id,
            "OprettetTidspunkt": (
                kontekst.oprettet_tidspunkt.isoformat() if kontekst.oprettet_tidspunkt else None
            ),
            "OprettetTidsstempel": kontekst.oprettet_tidsstempel,
            "AendretBruger": kontekst.aendret_bruger,
            "AendretPersId": kontekst.aendret_pers_id,
            "AendretRolle": kontekst.aendret_rolle,
            "AendretAdmEnhed": kontekst.aendret_adm_enhed,
            "AendretAdmEnhedId": kontekst.aendret_adm_enhed_id,
            "AendretTidspunkt": (
                kontekst.aendret_tidspunkt.isoformat() if kontekst.aendret_tidspunkt else None
            ),
            "AendretTidsstempel": kontekst.aendret_tidsstempel,
            "BesluttetBruger": kontekst.besluttet_bruger,
            "BesluttetPersId": kontekst.besluttet_pers_id,
            "BesluttetRolle": kontekst.besluttet_rolle,
            "BesluttetAdmEnhed": kontekst.besluttet_adm_enhed,
            "BesluttetAdmEnhedId": kontekst.besluttet_adm_enhed_id,
            "BesluttetTidspunkt": (
                kontekst.besluttet_tidspunkt.isoformat() if kontekst.besluttet_tidspunkt else None
            ),
            "BesluttetTidsstempel": kontekst.besluttet_tidsstempel,
            "Status": kontekst.status,
        }

        all_results.append(
            {
                "test_id": tc["test_id"],
                "aktivitets_id": aid,
                "element_count": len(result_rows),
                "results": result_rows,
                "kontekst": kontekst_dict,
            }
        )

    return all_results


# =============================================================================
# Comparison Helpers
# =============================================================================

RESULT_FIELDS = [
    "ElementNummer",
    "KlyngeId",
    "KlyngeIdHierarki",
    "KlyngeNiveau",
    "KlyngeNavne",
    "KlyngeLedetekster",
    "KlyngeParentId",
    "RepetitionId",
    "RepetitionIdHierarki",
    "RepetitionNiveau",
    "Hierarki",
    "Overskrift",
    "Ledetekst",
    "Type",
    "Kladde",
    "KlyngeNavn",
    "KlyngeLedetekst",
    "KlyngeValgLedetekst",
    "KlyngeType",
    "ElementNavn",
    "ElementLedetekst",
    "ElementVaerdi",
    "ElementElementId",
    "ElementKode",
    "ElementOriginalKode",
    "ElementAdministrativEnhedId",
    "ElementPersId",
    "ElementType",
    "ElementMoid",
    "RaekkeType",
    "XmlStatus",
    "XmlStatusText",
]

KONTEKST_FIELDS = [
    "AktivitetsId",
    "OprettetBruger",
    "OprettetPersId",
    "OprettetRolle",
    "OprettetAdmEnhed",
    "OprettetAdmEnhedId",
    "OprettetTidsstempel",
    "AendretBruger",
    "AendretPersId",
    "AendretRolle",
    "AendretAdmEnhed",
    "AendretAdmEnhedId",
    "AendretTidsstempel",
    "BesluttetBruger",
    "BesluttetPersId",
    "BesluttetRolle",
    "BesluttetAdmEnhed",
    "BesluttetAdmEnhedId",
    "BesluttetTidsstempel",
    "Status",
]


def compare_results(test_id: str, py_output: dict, cs_output: dict) -> list[str]:
    """Compare Python vs C# output field-by-field. Returns list of differences."""
    diffs = []

    if py_output["element_count"] != cs_output["element_count"]:
        diffs.append(
            f"[{test_id}] element_count: Python={py_output['element_count']} C#={cs_output['element_count']}"
        )
        return diffs

    for i, (py_row, cs_row) in enumerate(
        zip(py_output["results"], cs_output["results"], strict=False)
    ):
        for field in RESULT_FIELDS:
            py_val = py_row.get(field)
            cs_val = cs_row.get(field)
            if py_val != cs_val:
                diffs.append(f"[{test_id}] row[{i}].{field}: Python={py_val!r} C#={cs_val!r}")

    py_ctx = py_output.get("kontekst", {})
    cs_ctx = cs_output.get("kontekst", {})
    for field in KONTEKST_FIELDS:
        py_val = py_ctx.get(field)
        cs_val = cs_ctx.get(field)
        if py_val != cs_val:
            diffs.append(f"[{test_id}] kontekst.{field}: Python={py_val!r} C#={cs_val!r}")

    return diffs


# =============================================================================
# E2E Test: Run All Test Cases Through Both Parsers
# =============================================================================


class TestXmlParserE2EValidation:
    """Compare Python XML parser output against C# reference implementation."""

    @pytest.fixture(scope="class")
    def csharp_results(self) -> list[dict]:
        return run_csharp_parser(TEST_CASES)

    @pytest.fixture(scope="class")
    def python_results(self) -> list[dict]:
        return run_python_parser(TEST_CASES)

    def test_same_number_of_results(self, csharp_results, python_results):
        assert len(csharp_results) == len(python_results) == len(TEST_CASES)

    @pytest.mark.parametrize(
        "idx", range(len(TEST_CASES)), ids=[tc["test_id"] for tc in TEST_CASES]
    )
    def test_case_matches(self, idx, csharp_results, python_results):
        py = python_results[idx]
        cs = csharp_results[idx]
        test_id = TEST_CASES[idx]["test_id"]

        diffs = compare_results(test_id, py, cs)
        if diffs:
            diff_msg = "\n".join(diffs)
            pytest.fail(f"Mismatch between Python and C# for '{test_id}':\n{diff_msg}")

    def test_element_counts_match(self, csharp_results, python_results):
        for py, cs, tc in zip(python_results, csharp_results, TEST_CASES, strict=False):
            assert py["element_count"] == cs["element_count"], (
                f"Element count mismatch for {tc['test_id']}: "
                f"Python={py['element_count']} C#={cs['element_count']}"
            )

    def test_kontekst_matches(self, csharp_results, python_results):
        for py, cs, tc in zip(python_results, csharp_results, TEST_CASES, strict=False):
            py_ctx = py.get("kontekst", {})
            cs_ctx = cs.get("kontekst", {})
            for field in KONTEKST_FIELDS:
                assert py_ctx.get(field) == cs_ctx.get(field), (
                    f"Kontekst mismatch for {tc['test_id']}.{field}: "
                    f"Python={py_ctx.get(field)!r} C#={cs_ctx.get(field)!r}"
                )
