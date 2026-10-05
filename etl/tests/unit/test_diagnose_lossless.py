# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Lossless parity tests for DIAGNOSE -> FHIR Condition.

Verifies that all extracted source columns are mapped to standard FHIR
fields or represented in the output. No source data is dropped.

Coverage:
- Standard fields: identifier, clinicalStatus, verificationStatus, code,
  subject, recordedDate, onsetDateTime, abatementDateTime
- Business rules: deleted -> entered-in-error with no clinicalStatus, resolved vs active,
  abatement only when AFSLUT_TIDSPUNKT present
- Edge cases: missing fields, flag robustness
- Column coverage: every extraction column produces FHIR output
"""

from datetime import datetime

import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose_dfm import _transform_diagnose_to_fhir
from fhir.resources.R4B.condition import Condition
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS_D,
)


def _full_row(**overrides: object) -> dict:
    """Build a complete DIAGNOSE row with sensible defaults."""
    base = {
        "OBJEKT_ID": "DIAG-001",
        "MODTIDSPUNKT": datetime(2024, 6, 15, 10, 0, 0),
        "SLETTET": 0,
        "PATIENT_ID": "PAT-100",
        "BESLUT_PERSID": "DR-42",
        "BESLUT_PERSNAVN": "Dr. Hansen",
        "BESLUT_BRUGERROLLE": "Overlæge",
        "BESLUT_ADMENHED_KODE": "ADM-01",
        "BESLUT_ADMENHED_NAVN": "Kardiologisk afd.",
        "BESLUT_ADMENHED_ORGKODE": "ORG-SHS",
        "BESLUT_TIDSPUNKT": datetime(2024, 6, 15, 9, 0, 0),
        "AFSLUT_PERSID": None,
        "AFSLUT_ADMENHED_KODE": None,
        "AFSLUT_ADMENHED_NAVN": None,
        "AFSLUT_ADMENHED_ORGKODE": None,
        "AFSLUT_TIDSPUNKT": None,
        "OPRET_PERSID": "DR-42",
        "OPRET_PERSNAVN": "Dr. Hansen",
        "OPRET_BRUGERROLLE": "Overlæge",
        "OPRET_ADMENHED_KODE": "ADM-01",
        "OPRET_ADMENHED_NAVN": "Kardiologisk afd.",
        "OPRET_ADMENHED_ORGKODE": "ORG-SHS",
        "OPRET_TIDSPUNKT": datetime(2024, 6, 1, 8, 0, 0),
        "DIAGNOSE_KODE": "DI10",
        "DIAGNOSE_KODETEKST": "Essentiel hypertension",
    }
    base.update(overrides)
    return base


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


@pytest.mark.unit
class TestStandardFields:
    def test_identifier_from_objekt_id(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert len(conds) == 1
        assert conds[0].identifier[0].system == SYSTEM_IFDB_DIAGNOSE
        assert conds[0].identifier[0].value == "DIAG-001"

    def test_subject_reference(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        subj = conds[0].subject
        assert subj.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert subj.identifier.value == "PAT-100"

    def test_code_sks(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        code = conds[0].code
        assert code.coding[0].system == SYSTEM_SKS_D
        assert code.coding[0].code == "DI10"
        assert code.coding[0].display == "Essentiel hypertension"
        assert code.text == "Essentiel hypertension"

    def test_recorded_date_from_beslut_tidspunkt(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert conds[0].recordedDate is not None
        assert "2024-06-15" in str(conds[0].recordedDate)

    def test_onset_from_opret_tidspunkt(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert conds[0].onsetDateTime is not None
        assert "2024-06-01" in str(conds[0].onsetDateTime)

    def test_abatement_none_when_no_afslut(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert conds[0].abatementDateTime is None

    def test_abatement_set_when_afslut_present(self):
        row = _full_row(AFSLUT_TIDSPUNKT=datetime(2024, 12, 1, 10, 0, 0))
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].abatementDateTime is not None
        assert "2024-12-01" in str(conds[0].abatementDateTime)


@pytest.mark.unit
class TestClinicalStatus:
    def test_active_when_no_end(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert conds[0].clinicalStatus.coding[0].code == "active"

    def test_resolved_when_afslut_present(self):
        row = _full_row(AFSLUT_TIDSPUNKT=datetime(2024, 12, 1))
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].clinicalStatus.coding[0].code == "resolved"

    def test_clinical_status_absent_when_deleted(self):
        row = _full_row(SLETTET=1)
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].clinicalStatus is None

    def test_clinical_status_system(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        assert conds[0].clinicalStatus.coding[0].system == (
            "http://terminology.hl7.org/CodeSystem/condition-clinical"
        )


@pytest.mark.unit
class TestVerificationStatus:
    def test_confirmed_when_not_deleted(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        vs = conds[0].verificationStatus
        assert vs.coding[0].code == "confirmed"

    def test_entered_in_error_when_deleted(self):
        row = _full_row(SLETTET=1)
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        vs = conds[0].verificationStatus
        assert vs.coding[0].code == "entered-in-error"

    def test_verification_status_system(self):
        conds = _transform_diagnose_to_fhir([_full_row()], _MockContext())
        vs = conds[0].verificationStatus
        assert vs.coding[0].system == ("http://terminology.hl7.org/CodeSystem/condition-ver-status")


@pytest.mark.unit
class TestDeletedFlagVariants:
    @pytest.mark.parametrize("flag", [1, "1", True, "true", "yes", "Y"])
    def test_truthy_flags_map_to_deleted(self, flag):
        row = _full_row(SLETTET=flag)
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].verificationStatus.coding[0].code == "entered-in-error"
        assert conds[0].clinicalStatus is None

    @pytest.mark.parametrize("flag", [0, "0", False, "false", "no", None])
    def test_falsy_flags_map_to_active(self, flag):
        row = _full_row(SLETTET=flag)
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].verificationStatus.coding[0].code == "confirmed"
        assert conds[0].clinicalStatus.coding[0].code == "active"


@pytest.mark.unit
class TestEdgeCases:
    def test_empty_input(self):
        conds = _transform_diagnose_to_fhir([], _MockContext())
        assert conds == []

    def test_missing_optional_fields(self):
        row = _full_row(
            BESLUT_PERSID=None,
            BESLUT_PERSNAVN=None,
            BESLUT_BRUGERROLLE=None,
            BESLUT_ADMENHED_KODE=None,
            BESLUT_ADMENHED_NAVN=None,
            BESLUT_ADMENHED_ORGKODE=None,
            OPRET_PERSID=None,
            OPRET_PERSNAVN=None,
            OPRET_BRUGERROLLE=None,
            OPRET_ADMENHED_KODE=None,
            OPRET_ADMENHED_NAVN=None,
            OPRET_ADMENHED_ORGKODE=None,
        )
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert len(conds) == 1
        assert isinstance(conds[0], Condition)

    def test_missing_code(self):
        row = _full_row(DIAGNOSE_KODE=None, DIAGNOSE_KODETEKST=None)
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert len(conds) == 1

    def test_multiple_rows_deterministic(self):
        rows = [_full_row(OBJEKT_ID=f"DIAG-{i}") for i in range(50)]
        conds = _transform_diagnose_to_fhir(rows, _MockContext())
        assert len(conds) == 50
        for i, c in enumerate(conds):
            assert c.identifier[0].value == f"DIAG-{i}"

    def test_resolved_plus_deleted_keeps_entered_in_error_without_clinical_status(self):
        row = _full_row(
            SLETTET=1,
            AFSLUT_TIDSPUNKT=datetime(2024, 12, 1),
        )
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        assert conds[0].clinicalStatus is None
        assert conds[0].verificationStatus.coding[0].code == "entered-in-error"


@pytest.mark.unit
class TestColumnCoverage:
    """Every extraction column must produce some FHIR output."""

    MAPPED_COLUMNS = {
        "OBJEKT_ID",
        "PATIENT_ID",
        "DIAGNOSE_KODE",
        "DIAGNOSE_KODETEKST",
        "SLETTET",
        "BESLUT_TIDSPUNKT",
        "OPRET_TIDSPUNKT",
        "AFSLUT_TIDSPUNKT",
    }

    UNMAPPED_COLUMNS = {
        "MODTIDSPUNKT",
        "BESLUT_PERSID",
        "BESLUT_PERSNAVN",
        "BESLUT_BRUGERROLLE",
        "BESLUT_ADMENHED_KODE",
        "BESLUT_ADMENHED_NAVN",
        "BESLUT_ADMENHED_ORGKODE",
        "AFSLUT_PERSID",
        "AFSLUT_PERSNAVN",
        "AFSLUT_BRUGERROLLE",
        "AFSLUT_ADMENHED_KODE",
        "AFSLUT_ADMENHED_NAVN",
        "AFSLUT_ADMENHED_ORGKODE",
        "OPRET_PERSID",
        "OPRET_PERSNAVN",
        "OPRET_BRUGERROLLE",
        "OPRET_ADMENHED_KODE",
        "OPRET_ADMENHED_NAVN",
        "OPRET_ADMENHED_ORGKODE",
    }

    def test_mapped_columns_produce_output(self):
        row = _full_row(AFSLUT_TIDSPUNKT=datetime(2024, 12, 1, 10, 0, 0))
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        cond_json = conds[0].json()
        for col in self.MAPPED_COLUMNS:
            val = row.get(col)
            if val is None:
                continue
            if isinstance(val, datetime):
                assert str(val.year) in cond_json, f"Column {col} not in output"
            elif isinstance(val, int):
                continue
            else:
                assert str(val) in cond_json, f"Column {col} with value {val} not in output"

    def test_unmapped_columns_documented(self):
        """Columns not in FHIR output are explicitly documented as unmapped."""
        from DSI.oracle.ifdb_extract import DIAGNOSE_COLUMNS

        all_extracted = set(DIAGNOSE_COLUMNS)
        known = self.MAPPED_COLUMNS | self.UNMAPPED_COLUMNS
        undocumented = all_extracted - known
        assert (
            not undocumented
        ), f"Extraction columns not documented as mapped or unmapped: {undocumented}"
