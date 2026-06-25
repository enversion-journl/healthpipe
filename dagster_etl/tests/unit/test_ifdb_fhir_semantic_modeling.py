# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Semantic modeling tests for new IFDB -> FHIR resources.

Focus:
- Correct FHIR semantics for source deletion flags
- Use of standard FHIR encounter field for Observation/Procedure
- Backward-compatible EpisodeOfCare extension handling
"""

import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose import _transform_diagnose_to_fhir
from DSA.FHIR.Observation.observation_dict_builder import _build_observation_from_row_dict
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import _transform_adm_procedure_to_fhir
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import _transform_ris_ydelse_to_fhir
from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


@pytest.mark.unit
def test_diagnose_deleted_sets_verification_entered_in_error() -> None:
    rows = [
        {
            "OBJEKT_ID": "diag-1",
            "PATIENT_ID": "pat-1",
            "DIAGNOSE_KODE": "DX1",
            "DIAGNOSE_KODETEKST": "Test diagnose",
            "SLETTET": 1,
            "BESLUT_TIDSPUNKT": "2026-01-01T10:00:00",
            "OPRET_TIDSPUNKT": "2026-01-01T09:00:00",
            "AFSLUT_TIDSPUNKT": None,
        }
    ]
    conditions = _transform_diagnose_to_fhir(rows, context=None)
    assert len(conditions) == 1
    cond = conditions[0]
    assert cond.verificationStatus is not None
    coding = cond.verificationStatus.coding[0]
    assert coding.code == "entered-in-error"
    assert coding.system == "http://terminology.hl7.org/CodeSystem/condition-ver-status"
    assert cond.clinicalStatus is None
    assert cond.extension is not None
    is_deleted = [ext for ext in cond.extension if ext.url == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].valueInteger == 1


@pytest.mark.unit
def test_observation_uses_standard_encounter_field() -> None:
    row = {
        "OBJEKT_ID": "akt-1",
        "PATIENT_ID": "pat-1",
        "ENCOUNTER_ID": "enc-1",
        "EPISODE_OF_CARE_ID": "ep-1",
        "AKTIVITETTYPE_KODE": "A1",
        "AKTIVITETTYPE_KODETEKST": "Aktivitet",
        "RESULTATTYPE_KODE": "R1",
        "RESULTATTYPE_KODETEKST": "Resultat",
        "OVERSKRIFT": "Titel",
        "KLADDE": 0,
        "PERIOD_START": "2026-01-01T09:00:00",
        "SLETTET": 0,
    }
    _rid, obs = _build_observation_from_row_dict(row, [("Navn", "Vaerdi")])
    assert obs.get("encounter") is not None
    assert obs["encounter"].get("identifier") is not None
    assert str(obs["encounter"]["identifier"]["value"]) == "enc-1"
    assert obs.get("extension") is not None
    assert any(ext.get("url") == EXTENSION_EPISODE_OF_CARE for ext in obs["extension"])


@pytest.mark.unit
def test_administrative_procedure_uses_standard_encounter_field() -> None:
    rows = [
        {
            "OBJECT_ID": "proc-1",
            "PATIENT_ID": "pat-1",
            "ENCOUNTER_ID": "enc-1",
            "EPISODE_OF_CARE_ID": "ep-1",
            "PROCEDURE_CODE": "P1",
            "PROCEDURE_TEXT": "Procedure",
            "PERIOD_START": "2026-01-01T11:00:00",
            "DELETED": 0,
        }
    ]
    procedures = _transform_adm_procedure_to_fhir(rows, context=None)
    assert len(procedures) == 1
    proc = procedures[0]
    assert proc.encounter is not None
    assert proc.encounter.identifier is not None
    assert str(proc.encounter.identifier.value) == "enc-1"
    assert proc.extension is not None
    assert any(ext.url == EXTENSION_EPISODE_OF_CARE for ext in proc.extension)


class _MockRisContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


@pytest.mark.unit
def test_ris_ydelse_uses_standard_encounter_field() -> None:
    rows = [
        {
            "OBJEKT_ID": "ris-1",
            "PATIENT_ID": "pat-1",
            "ENCOUNTER_ID": "enc-1",
            "EPISODE_OF_CARE_ID": "ep-1",
            "TYPE": "RIS_SERVICE",
            "STATE": "FINAL_REPORT",
            "XML": "<rekvisition><service><kode>UXRC00</kode></service></rekvisition>",
            "UDFOERT_TIDSPUNKT": "2026-01-01T11:00:00",
            "SLETTET": 0,
        }
    ]
    procedures = _transform_ris_ydelse_to_fhir(rows, context=_MockRisContext())
    assert len(procedures) == 1
    proc = procedures[0]
    assert proc.encounter is not None
    assert proc.encounter.identifier is not None
    assert str(proc.encounter.identifier.value) == "enc-1"
