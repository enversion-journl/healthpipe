# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Massive differential validation between Python and C# XML parsers.

This test generates many deterministic synthetic XML documents with varying:
- depth and branching
- combinations of Element/Klynge/Repetition
- selektion clusters
- optional Kontekst blocks
- optional PersonaleRolle patterns
- malformed/empty/null inputs

Goal: stress the parser and detect regressions by comparing Python output
against the C# reference implementation field-by-field.
"""

from __future__ import annotations

import random
from xml.etree import ElementTree as ET

import pytest
from tests.validation.test_xml_parser_e2e import (
    compare_results,
    csharp_available,
    run_csharp_parser,
    run_python_parser,
)

pytestmark = [
    pytest.mark.slow,
    pytest.mark.skipif(
        not csharp_available(),
        reason="C# .NET SDK or project not available",
    ),
]


def _txt(prefix: str, i: int, j: int = 0) -> str:
    return f"{prefix}_{i}_{j}"


def _add_element(parent: ET.Element, i: int, j: int, include_codes: bool) -> None:
    el = ET.SubElement(parent, "Element", {"type": random.choice(["string", "number", "text"])})
    ET.SubElement(el, "Navn").text = _txt("Navn", i, j)
    ET.SubElement(el, "Ledetekst").text = _txt("Ledetekst", i, j)
    ET.SubElement(el, "Vaerdi").text = _txt("Vaerdi", i, j)
    if include_codes:
        ET.SubElement(el, "ElementId").text = _txt("EID", i, j)
        ET.SubElement(el, "Kode").text = _txt("KODE", i, j)
        ET.SubElement(el, "Originalkode").text = _txt("ORGKODE", i, j)
        ET.SubElement(el, "AdministrativEnhedId").text = _txt("ADM", i, j)
        ET.SubElement(el, "PersId").text = _txt("PERS", i, j)
        ET.SubElement(el, "MOID").text = _txt("MOID", i, j)


def _add_kontekst(root: ET.Element, i: int) -> None:
    kontekst = ET.SubElement(root, "Kontekst")
    for tag in ["Oprettet", "Aendret", "Besluttet"]:
        part = ET.SubElement(kontekst, tag)
        ET.SubElement(part, "bruger").text = f"{tag}_Bruger_{i}"
        ET.SubElement(part, "PersId").text = f"{tag}_Pers_{i}"
        ET.SubElement(part, "Rolle").text = f"{tag}_Rolle_{i}"
        ET.SubElement(part, "AdministrativEnhed").text = f"{tag}_AE_{i}"
        ET.SubElement(part, "AdministrativEnhedId").text = f"{tag}_AEID_{i}"
        ET.SubElement(part, "Tidspunkt").text = "2024-01-15T10:30:00"
        ET.SubElement(part, "Tidsstempel").text = str(1705312200 + i)


def _build_random_xml(case_idx: int, rng: random.Random) -> str:
    root = ET.Element("Afdelingsydelsesresultat")
    ET.SubElement(root, "Overskrift").text = f"Overskrift_{case_idx}"
    ET.SubElement(root, "Ledetekst").text = f"Ledetekst_{case_idx}"
    ET.SubElement(root, "Type").text = random.choice(["Observation", "Klinisk notat", "Status"])
    ET.SubElement(root, "Kladde").text = random.choice(["true", "false"])

    if rng.random() < 0.6:
        _add_kontekst(root, case_idx)

    # Optional top-level PersonaleRolle + Personale
    if rng.random() < 0.2:
        role = ET.SubElement(root, "Element", {"type": "hidden"})
        ET.SubElement(role, "Navn").text = "PersonaleRolle"
        ET.SubElement(role, "Vaerdi").text = random.choice(["Sygeplejerske", "Læge", "SSA"])

        pers = ET.SubElement(root, "Element", {"type": "string"})
        ET.SubElement(pers, "Navn").text = "Personale"
        ET.SubElement(pers, "Ledetekst").text = "Udførende"
        ET.SubElement(pers, "Vaerdi").text = f"Person_{case_idx}"
        ET.SubElement(pers, "PersId").text = f"P_{case_idx}"

    top_nodes = rng.randint(1, 5)
    for n in range(top_nodes):
        choice = rng.choice(["element", "klynge", "repetition"])

        if choice == "element":
            _add_element(root, case_idx, n, include_codes=rng.random() < 0.5)
            continue

        if choice == "repetition":
            rep = ET.SubElement(root, "Repetition")
            count = rng.randint(1, 4)
            for j in range(count):
                # Some repetitions include PersonaleRolle pattern
                if rng.random() < 0.15:
                    role = ET.SubElement(rep, "Element", {"type": "hidden"})
                    ET.SubElement(role, "Navn").text = "PersonaleRolle"
                    ET.SubElement(role, "Vaerdi").text = random.choice(
                        ["Sygeplejerske", "Læge", "SSA"]
                    )
                    pers = ET.SubElement(rep, "Element", {"type": "string"})
                    ET.SubElement(pers, "Navn").text = "Personale"
                    ET.SubElement(pers, "Ledetekst").text = "Kontaktperson"
                    ET.SubElement(pers, "Vaerdi").text = f"PersRep_{case_idx}_{j}"
                    ET.SubElement(pers, "PersId").text = f"PR_{case_idx}_{j}"
                else:
                    _add_element(rep, case_idx, j, include_codes=rng.random() < 0.4)
            continue

        # Klynge
        k_type = rng.choice(["sekvens", "selektion"])
        klynge = ET.SubElement(root, "Klynge", {"type": k_type})
        ET.SubElement(klynge, "Navn").text = f"Klynge_{case_idx}_{n}"
        ET.SubElement(klynge, "Ledetekst").text = f"KlyngeLedetekst_{case_idx}_{n}"
        if k_type == "selektion":
            ET.SubElement(klynge, "ValgLedetekst").text = f"Valg_{case_idx}_{n}"

        child_count = rng.randint(1, 4)
        for j in range(child_count):
            sub_choice = rng.choice(["element", "nested_klynge", "repetition"])
            if sub_choice == "element":
                _add_element(klynge, case_idx, j, include_codes=rng.random() < 0.4)
            elif sub_choice == "repetition":
                rep = ET.SubElement(klynge, "Repetition")
                for k in range(rng.randint(1, 2)):
                    _add_element(rep, case_idx, j * 10 + k, include_codes=rng.random() < 0.3)
            else:
                nested = ET.SubElement(
                    klynge,
                    "Klynge",
                    {"type": rng.choice(["sekvens", "selektion"])},
                )
                ET.SubElement(nested, "Navn").text = f"Nested_{case_idx}_{j}"
                ET.SubElement(nested, "Ledetekst").text = f"NestedL_{case_idx}_{j}"
                if nested.attrib.get("type") == "selektion":
                    ET.SubElement(nested, "ValgLedetekst").text = f"NestedValg_{case_idx}_{j}"
                _add_element(nested, case_idx, j, include_codes=rng.random() < 0.3)

    return ET.tostring(root, encoding="unicode")


def _build_massive_cases(num_cases: int = 350) -> list[dict]:
    rng = random.Random(20260311)
    cases: list[dict] = []

    for i in range(num_cases):
        xml = _build_random_xml(i, rng)
        cases.append(
            {
                "test_id": f"rnd_{i:04d}",
                "aktivitets_id": 50000 + i,
                "xml": xml,
            }
        )

    # Add explicit malformed/empty/null at the end
    cases.extend(
        [
            {"test_id": "edge_empty", "aktivitets_id": 99001, "xml": ""},
            {"test_id": "edge_null_like", "aktivitets_id": 99002, "xml": ""},
            {"test_id": "edge_malformed_1", "aktivitets_id": 99003, "xml": "<broken><xml"},
            {"test_id": "edge_malformed_2", "aktivitets_id": 99004, "xml": "<x><y></x>"},
        ]
    )

    return cases


def _run_csharp_batched(test_cases: list[dict], batch_size: int = 80) -> list[dict]:
    all_results: list[dict] = []
    for i in range(0, len(test_cases), batch_size):
        batch = test_cases[i : i + batch_size]
        all_results.extend(run_csharp_parser(batch))
    return all_results


class TestXmlParserMassiveDifferential:
    @pytest.fixture(scope="class")
    def massive_cases(self) -> list[dict]:
        return _build_massive_cases()

    @pytest.fixture(scope="class")
    def csharp_results(self, massive_cases) -> list[dict]:
        return _run_csharp_batched(massive_cases)

    @pytest.fixture(scope="class")
    def python_results(self, massive_cases) -> list[dict]:
        return run_python_parser(massive_cases)

    def test_counts_match(self, massive_cases, csharp_results, python_results):
        assert len(csharp_results) == len(python_results) == len(massive_cases)

    def test_all_massive_cases_match(self, massive_cases, csharp_results, python_results):
        mismatches: list[str] = []
        for tc, cs, py in zip(massive_cases, csharp_results, python_results, strict=False):
            diffs = compare_results(tc["test_id"], py, cs)
            if diffs:
                mismatches.extend(diffs[:10])  # cap per case to keep output manageable
                if len(mismatches) > 80:
                    break

        assert not mismatches, "Massive differential mismatches found:\n" + "\n".join(mismatches)
