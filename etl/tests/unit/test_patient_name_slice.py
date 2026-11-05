# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The name carries the slice use=official, and patient_view filters on the same value."""

import ast
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from DSA.FHIR.Patient.patient_ifdb_contribution import transform_fhir_patient_ifdb
from DSA.FHIR.Patient.patient_ownership import USE_OFFICIAL

pytestmark = pytest.mark.unit

ROD = Path(__file__).resolve().parents[2]
FHIR_ROD = ROD / "DSA" / "FHIR"
PROJEKTION = FHIR_ROD / "SqlOnFhir" / "view_definitions" / "patient_view.json"

SLICE = USE_OFFICIAL

# Practitioner is projected through name.text without a slice and is outside the coupling.
UDEN_FOR = {"Practitioner"}


def _navnekolonner() -> dict[str, str]:
    view = json.loads(PROJEKTION.read_text(encoding="utf-8"))
    return {
        k["name"]: k["path"]
        for sel in view["select"]
        for k in sel["column"]
        if k["name"].startswith("name_")
    }


def _humanname_kald() -> list[tuple[str, int, set[str]]]:
    """(file, line, argument names) for each HumanName(...) under DSA/FHIR."""
    fundne = []
    for sti in sorted(FHIR_ROD.rglob("*.py")):
        if "__pycache__" in sti.parts or UDEN_FOR & set(sti.relative_to(FHIR_ROD).parts):
            continue
        traeet = ast.parse(sti.read_text(encoding="utf-8"))
        for knude in ast.walk(traeet):
            if isinstance(knude, ast.Call):
                navn = knude.func.id if isinstance(knude.func, ast.Name) else None
                if navn == "HumanName":
                    argumenter = {k.arg for k in knude.keywords if k.arg}
                    fundne.append((str(sti.relative_to(FHIR_ROD)), knude.lineno, argumenter))
    return fundne


def test_there_are_name_constructions_to_check():
    """Guard against the scanner silently ceasing to find anything."""
    assert _humanname_kald(), "no HumanName calls found — is the scanner broken?"


def test_every_name_carries_a_slice():
    """A name without use is never read by the projection."""
    uden = [f"{fil}:{linje}" for fil, linje, args in _humanname_kald() if "use" not in args]
    assert not uden, (
        "HumanName without use — the projection's name.where(use='official') will never match "
        "them, and the name columns become NULL without anything failing:\n  " + "\n  ".join(uden)
    )


def test_the_projection_filters_on_the_slice_we_write():
    """Writer and reader must agree. Otherwise the names are gone again."""
    kolonner = _navnekolonner()
    assert kolonner, "patient_view has no name_* columns"
    for kolonne, sti in kolonner.items():
        assert f"use='{SLICE}'" in sti, f"{kolonne} does not filter on use='{SLICE}': {sti}"


def test_the_epj_transform_sets_the_slice():
    """The path the vast majority of citizens arrive by."""
    patienter = transform_fhir_patient_ifdb(
        context=MagicMock(),
        patient_rows=[
            {
                "patient_id": "999001",
                "identifier_cpr": "0000000001",
                "name_given": "Test Person",
                "name_family": "Testesen",
            }
        ],
    )

    assert len(patienter) == 1
    navn = patienter[0].name[0]
    assert navn.use == SLICE
    assert navn.family == "Testesen"
    assert navn.given == ["Test", "Person"]


def test_name_is_still_omitted_when_the_source_has_nothing():
    """The slice must not make us write an empty name."""
    patienter = transform_fhir_patient_ifdb(
        context=MagicMock(),
        patient_rows=[{"patient_id": "999002", "identifier_cpr": "0000000002"}],
    )

    assert len(patienter) == 1
    assert patienter[0].name is None
