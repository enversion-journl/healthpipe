# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Robustness tests for unexpected source data changes and flag variations.
"""

import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose import _transform_diagnose_to_fhir
from DSA.FHIR.Observation.observation_dict_builder import _build_observation_from_row_dict
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import _transform_adm_procedure_to_fhir
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import _transform_ris_ydelse_to_fhir
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


@pytest.mark.unit
@pytest.mark.parametrize("flag_value", [1, "1", True, "true", "TRUE", "yes", "Y"])
def test_observation_deleted_flag_variants_map_to_is_deleted_extension(flag_value) -> None:
    row = {
        "OBJEKT_ID": "obs-1",
        "PATIENT_ID": "pat-1",
        "ENCOUNTER_ID": "enc-1",
        "EPISODE_OF_CARE_ID": "ep-1",
        "AKTIVITETTYPE_KODE": "A1",
        "AKTIVITETTYPE_KODETEKST": "Aktivitet",
        "RESULTATTYPE_KODE": "R1",
        "RESULTATTYPE_KODETEKST": "Resultat",
        "OVERSKRIFT": "Titel",
        "KLADDE": 0,
        "PERIOD_START": "2026-01-01T10:00:00",
        "SLETTET": flag_value,
    }
    _rid, obs = _build_observation_from_row_dict(row, [("Kode", "Vaerdi")])
    assert obs["status"] == "entered-in-error"
    assert obs.get("extension") is not None
    is_deleted = [ext for ext in obs["extension"] if ext.get("url") == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].get("valueInteger") == 1


@pytest.mark.unit
def test_observation_deleted_with_draft_kladde_sets_entered_in_error_and_extension() -> None:
    row = {
        "OBJEKT_ID": "obs-2",
        "PATIENT_ID": "pat-1",
        "ENCOUNTER_ID": "enc-1",
        "EPISODE_OF_CARE_ID": "ep-1",
        "AKTIVITETTYPE_KODE": "A1",
        "AKTIVITETTYPE_KODETEKST": "Aktivitet",
        "RESULTATTYPE_KODE": "R1",
        "RESULTATTYPE_KODETEKST": "Resultat",
        "OVERSKRIFT": "Titel",
        "KLADDE": "true",
        "PERIOD_START": "2026-01-01T10:00:00",
        "SLETTET": "true",
    }
    _rid, obs = _build_observation_from_row_dict(row, [("Kode", "Vaerdi")])
    assert obs["status"] == "entered-in-error"
    assert obs.get("extension") is not None
    is_deleted = [ext for ext in obs["extension"] if ext.get("url") == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].get("valueInteger") == 1


@pytest.mark.unit
@pytest.mark.parametrize("flag_value", [True, "true", "yes"])
def test_diagnose_deleted_flag_variants_map_to_entered_in_error(flag_value) -> None:
    rows = [
        {
            "OBJEKT_ID": "diag-1",
            "PATIENT_ID": "pat-1",
            "DIAGNOSE_KODE": "DX1",
            "DIAGNOSE_KODETEKST": "Diagnose",
            "SLETTET": flag_value,
            "BESLUT_TIDSPUNKT": "2026-01-01T10:00:00",
            "OPRET_TIDSPUNKT": "2026-01-01T09:00:00",
            "AFSLUT_TIDSPUNKT": None,
        }
    ]
    cond = _transform_diagnose_to_fhir(rows, context=None)[0]
    assert cond.verificationStatus.coding[0].code == "entered-in-error"
    assert cond.extension is not None
    is_deleted = [ext for ext in cond.extension if ext.url == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].valueInteger == 1


@pytest.mark.unit
@pytest.mark.parametrize("flag_value", [True, "true", "yes"])
def test_administrative_procedure_deleted_flag_variants(flag_value) -> None:
    rows = [
        {
            "OBJECT_ID": "proc-1",
            "PATIENT_ID": "pat-1",
            "ENCOUNTER_ID": "enc-1",
            "EPISODE_OF_CARE_ID": "ep-1",
            "PROCEDURE_CODE": "P1",
            "PROCEDURE_TEXT": "Procedure",
            "PERIOD_START": "2026-01-01T11:00:00",
            "DELETED": flag_value,
        }
    ]
    proc = _transform_adm_procedure_to_fhir(rows, context=None)[0]
    assert proc.status == "entered-in-error"
    assert proc.extension is not None
    is_deleted = [ext for ext in proc.extension if ext.url == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].valueInteger == 1


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


_RIS_XML = "<rekvisition><service><kode>UXRC00</kode></service></rekvisition>"


@pytest.mark.unit
@pytest.mark.parametrize("flag_value", [True, "true", "yes"])
def test_ris_ydelse_deleted_flag_variants(flag_value) -> None:
    rows = [
        {
            "OBJEKT_ID": "ris-1",
            "PATIENT_ID": "pat-1",
            "ENCOUNTER_ID": "enc-1",
            "EPISODE_OF_CARE_ID": "ep-1",
            "TYPE": "RIS_SERVICE",
            "STATE": "FINAL_REPORT",
            "XML": _RIS_XML,
            "UDFOERT_TIDSPUNKT": "2026-01-01T11:00:00",
            "SLETTET": flag_value,
        }
    ]
    proc = _transform_ris_ydelse_to_fhir(rows, context=_MockContext())[0]
    assert proc.status == "entered-in-error"
    assert proc.extension is not None
    is_deleted = [ext for ext in proc.extension if ext.url == EXTENSION_FHIR_IS_DELETED]
    assert len(is_deleted) == 1
    assert is_deleted[0].valueInteger == 1
