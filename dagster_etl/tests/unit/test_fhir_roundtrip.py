# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Round-trip validation: source row -> FHIR resource -> verify key fields preserved.

Tests the complete transform chain for each domain and validates that critical
source data survives the FHIR transformation without loss or corruption.
"""

from datetime import datetime

import pytest
from DSA.FHIR.Condition.Condition_IFDB_Diagnose import _transform_diagnose_to_fhir
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import _transform_adm_procedure_to_fhir
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import _transform_ris_ydelse_to_fhir
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


_RIS_XML = (
    "<services><serviceGroup><service>"
    "<kode>UXRC40</kode><tillaegskode>ZZ3542</tillaegskode>"
    "</service></serviceGroup></services>"
)


@pytest.mark.unit
class TestDiagnoseRoundtrip:
    """Source DIAGNOSE row -> FHIR Condition -> verify critical fields."""

    def test_identity_preserved(self):
        row = {
            "OBJEKT_ID": "DIAG-RT-001",
            "PATIENT_ID": "PAT-RT-100",
            "DIAGNOSE_KODE": "DE112",
            "DIAGNOSE_KODETEKST": "Type 2-diabetes uden komplikationer",
            "SLETTET": 0,
            "BESLUT_TIDSPUNKT": datetime(2024, 3, 15, 14, 30),
            "OPRET_TIDSPUNKT": datetime(2024, 3, 10, 8, 0),
            "AFSLUT_TIDSPUNKT": None,
            "MODTIDSPUNKT": datetime(2024, 3, 15, 15, 0),
            "BESLUT_PERSID": "DR-1",
            "BESLUT_PERSNAVN": "Hansen",
            "BESLUT_BRUGERROLLE": "Overlæge",
            "BESLUT_ADMENHED_KODE": "K1",
            "BESLUT_ADMENHED_NAVN": "Kard",
            "BESLUT_ADMENHED_ORGKODE": "ORG1",
            "AFSLUT_PERSID": None,
            "AFSLUT_ADMENHED_KODE": None,
            "AFSLUT_ADMENHED_NAVN": None,
            "AFSLUT_ADMENHED_ORGKODE": None,
            "OPRET_PERSID": "DR-1",
            "OPRET_PERSNAVN": "Hansen",
            "OPRET_BRUGERROLLE": "Overlæge",
            "OPRET_ADMENHED_KODE": "K1",
            "OPRET_ADMENHED_NAVN": "Kard",
            "OPRET_ADMENHED_ORGKODE": "ORG1",
        }
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        c = conds[0]

        assert c.identifier[0].value == "DIAG-RT-001"
        assert c.subject.identifier.value == "PAT-RT-100"
        assert c.code.coding[0].code == "DE112"
        assert c.code.coding[0].system == SYSTEM_SKS
        assert c.clinicalStatus.coding[0].code == "active"
        assert c.verificationStatus.coding[0].code == "confirmed"
        assert "2024-03-15" in str(c.recordedDate)
        assert "2024-03-10" in str(c.onsetDateTime)
        assert c.abatementDateTime is None

    def test_deleted_roundtrip(self):
        row = {
            "OBJEKT_ID": "DIAG-RT-DEL",
            "PATIENT_ID": "PAT-RT-200",
            "DIAGNOSE_KODE": "DJ459",
            "DIAGNOSE_KODETEKST": "Astma",
            "SLETTET": 1,
            "BESLUT_TIDSPUNKT": datetime(2024, 1, 1),
            "OPRET_TIDSPUNKT": datetime(2024, 1, 1),
            "AFSLUT_TIDSPUNKT": datetime(2024, 6, 1),
            "MODTIDSPUNKT": datetime(2024, 6, 2),
            "BESLUT_PERSID": None,
            "BESLUT_PERSNAVN": None,
            "BESLUT_BRUGERROLLE": None,
            "BESLUT_ADMENHED_KODE": None,
            "BESLUT_ADMENHED_NAVN": None,
            "BESLUT_ADMENHED_ORGKODE": None,
            "AFSLUT_PERSID": None,
            "AFSLUT_ADMENHED_KODE": None,
            "AFSLUT_ADMENHED_NAVN": None,
            "AFSLUT_ADMENHED_ORGKODE": None,
            "OPRET_PERSID": None,
            "OPRET_PERSNAVN": None,
            "OPRET_BRUGERROLLE": None,
            "OPRET_ADMENHED_KODE": None,
            "OPRET_ADMENHED_NAVN": None,
            "OPRET_ADMENHED_ORGKODE": None,
        }
        conds = _transform_diagnose_to_fhir([row], _MockContext())
        c = conds[0]
        assert c.clinicalStatus is None
        assert c.verificationStatus.coding[0].code == "entered-in-error"
        assert c.extension is not None
        is_deleted = [ext for ext in c.extension if ext.url == EXTENSION_FHIR_IS_DELETED]
        assert len(is_deleted) == 1
        assert is_deleted[0].valueInteger == 1
        assert c.abatementDateTime is not None


@pytest.mark.unit
class TestAdministrativeProcedureRoundtrip:
    """Source ADMINISTRATIVE_PROCEDURE row -> FHIR Procedure -> verify."""

    def test_identity_and_code_preserved(self):
        row = {
            "OBJECT_ID": "PROC-RT-001",
            "PATIENT_ID": "PAT-RT-300",
            "ENCOUNTER_ID": "ENC-RT-400",
            "EPISODE_OF_CARE_ID": "EOC-RT-500",
            "PERIOD_START": "2024-08-01T08:00:00",
            "PERIOD_END": "2024-08-01T10:00:00",
            "PROCEDURE_CODE": "KXBA10",
            "PROCEDURE_TEXT": "Knæprotese",
            "STATUS_CODE": "A",
            "STATUS_TEXT": "Aktiv",
            "INDICATION_OF_SIDE_CODE": "TUL1",
            "INDICATION_OF_SIDE_TEXT": "Venstre",
            "USED_CONTRAST_CODE": "0",
            "USED_CONTRAST_TEXT": "Ingen kontrast",
            "STAFF_CATEGORY_CODE": "7170",
            "STAFF_CATEGORY_TEXT": "Ortopædkirurg",
            "LOCATION_CODE": "LOC-01",
            "LOCATION_NAME": "Operationsgang 3",
            "LOCATION_ORGCODE": "ORG-SHS",
            "LOCATION_SORCODE": "SOR-1234",
            "EPJI_CREATED_DATETIME": "2024-08-01T07:55:00",
            "EPJI_MODIFICATION_DATETIME": "2024-08-01T10:05:00",
            "IFDB_MODIFICATION_DATETIME": "2024-08-01T10:06:00",
            "DELETED": 0,
            "USER_ID": "USR-42",
            "USER_NAME": "DrHansen",
            "USER_ROLE": "Overlæge",
            "LPR3_IGNORED": 0,
            "USER_ROLE_NAME": "Ortopæd-overlæge",
        }
        procs = _transform_adm_procedure_to_fhir([row], _MockContext())
        p = procs[0]

        assert p.identifier[0].value == "PROC-RT-001"
        assert p.identifier[0].system == SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE
        assert p.subject.identifier.value == "PAT-RT-300"
        assert p.code.coding[0].code == "KXBA10"
        assert p.code.coding[0].system == SYSTEM_SKS
        assert p.status == "completed"
        assert p.performedPeriod is not None
        assert "2024-08-01" in str(p.performedPeriod.start)
        assert p.bodySite is not None
        assert p.bodySite[0].coding[0].code == "TUL1"

    def test_deleted_procedure(self):
        row = {
            "OBJECT_ID": "PROC-RT-DEL",
            "PATIENT_ID": "PAT-RT-300",
            "ENCOUNTER_ID": None,
            "EPISODE_OF_CARE_ID": None,
            "PERIOD_START": "2024-08-01T08:00:00",
            "PERIOD_END": None,
            "PROCEDURE_CODE": "KXBA10",
            "PROCEDURE_TEXT": "Knæprotese",
            "STATUS_CODE": None,
            "STATUS_TEXT": None,
            "INDICATION_OF_SIDE_CODE": None,
            "INDICATION_OF_SIDE_TEXT": None,
            "USED_CONTRAST_CODE": None,
            "USED_CONTRAST_TEXT": None,
            "STAFF_CATEGORY_CODE": None,
            "STAFF_CATEGORY_TEXT": None,
            "LOCATION_CODE": None,
            "LOCATION_NAME": None,
            "LOCATION_ORGCODE": None,
            "LOCATION_SORCODE": None,
            "EPJI_CREATED_DATETIME": None,
            "EPJI_MODIFICATION_DATETIME": None,
            "IFDB_MODIFICATION_DATETIME": None,
            "DELETED": 1,
            "USER_ID": None,
            "USER_NAME": None,
            "USER_ROLE": None,
            "LPR3_IGNORED": None,
            "USER_ROLE_NAME": None,
        }
        procs = _transform_adm_procedure_to_fhir([row], _MockContext())
        assert procs[0].status == "entered-in-error"
        assert procs[0].extension is not None
        is_deleted = [ext for ext in procs[0].extension if ext.url == EXTENSION_FHIR_IS_DELETED]
        assert len(is_deleted) == 1
        assert is_deleted[0].valueInteger == 1


@pytest.mark.unit
class TestRisYdelseRoundtrip:
    """Source RIS_YDELSE row -> FHIR Procedure(s) -> verify."""

    def test_single_service_roundtrip(self):
        row = {
            "OBJEKT_ID": "RIS-RT-001",
            "PATIENT_ID": "PAT-RT-600",
            "ENCOUNTER_ID": "ENC-RT-700",
            "EPISODE_OF_CARE_ID": "EOC-RT-800",
            "TYPE": "UXRC40",
            "STATE": "FINAL_REPORT",
            "UDFOERT_TIDSPUNKT": datetime(2024, 9, 1, 14, 0),
            "PERIOD_START": datetime(2024, 9, 1, 13, 30),
            "PERIOD_END": datetime(2024, 9, 1, 14, 30),
            "MODTIDSPUNKT": datetime(2024, 9, 1, 15, 0),
            "SLETTET": 0,
            "RTEE_KODE": "RT-01",
            "RTEE_KODETEKST": "Røntgen",
            "STAMAFD_KODE": "AFD-01",
            "STAMAFD_NAVN": "Radiologisk afd.",
            "STAMAFD_ORGKODE": "ORG-RAD",
            "STAMAFD_SORCODE": "SOR-RAD",
            "UDFOERT_ADMENHED_KODE": "ADM-RAD",
            "UDFOERT_ADMENHED_NAVN": "Radiologisk enhed",
            "UDFOERT_ADMENHED_ORGKODE": "ORG-RAD-2",
            "UDFOERT_ADMENHED_SORCODE": "SOR-RAD-2",
            "LPR3_IGNORED": 0,
            "BESKED": "Standard undersøgelse",
            "OPDATERET_TID": datetime(2024, 9, 1, 16, 0),
            "XML": _RIS_XML,
        }
        procs = _transform_ris_ydelse_to_fhir([row], _MockContext())
        assert len(procs) >= 1
        p = procs[0]

        assert p.subject.identifier.value == "PAT-RT-600"
        assert p.status == "completed"
        assert p.code is not None
        assert p.code.coding[0].code == "UXRC40"
        assert p.performedPeriod is not None
        assert "2024-09-01" in str(p.performedPeriod.start)

    def test_state_filter_excludes_non_final(self):
        row = {
            "OBJEKT_ID": "RIS-RT-SKIP",
            "PATIENT_ID": "PAT-RT-600",
            "ENCOUNTER_ID": None,
            "EPISODE_OF_CARE_ID": None,
            "TYPE": "UXRC40",
            "STATE": "PRELIMINARY_REPORT",
            "UDFOERT_TIDSPUNKT": datetime(2024, 9, 1),
            "PERIOD_START": None,
            "PERIOD_END": None,
            "MODTIDSPUNKT": datetime(2024, 9, 1),
            "SLETTET": 0,
            "RTEE_KODE": None,
            "RTEE_KODETEKST": None,
            "STAMAFD_KODE": None,
            "STAMAFD_NAVN": None,
            "STAMAFD_ORGKODE": None,
            "STAMAFD_SORCODE": None,
            "UDFOERT_ADMENHED_KODE": None,
            "UDFOERT_ADMENHED_NAVN": None,
            "UDFOERT_ADMENHED_ORGKODE": None,
            "UDFOERT_ADMENHED_SORCODE": None,
            "LPR3_IGNORED": None,
            "BESKED": None,
            "OPDATERET_TID": None,
            "XML": _RIS_XML,
        }
        procs = _transform_ris_ydelse_to_fhir([row], _MockContext())
        assert procs == []

    def test_multiple_services_in_xml(self):
        multi_xml = (
            "<services>"
            "<serviceGroup>"
            "<service><kode>UXRC40</kode></service>"
            "<service><kode>UXRC41</kode></service>"
            "</serviceGroup>"
            "</services>"
        )
        row = {
            "OBJEKT_ID": "RIS-RT-MULTI",
            "PATIENT_ID": "PAT-RT-600",
            "ENCOUNTER_ID": "ENC-RT-700",
            "EPISODE_OF_CARE_ID": None,
            "TYPE": "UXRC40",
            "STATE": "FINAL_REPORT",
            "UDFOERT_TIDSPUNKT": datetime(2024, 9, 1, 14, 0),
            "PERIOD_START": None,
            "PERIOD_END": None,
            "MODTIDSPUNKT": datetime(2024, 9, 1, 15, 0),
            "SLETTET": 0,
            "RTEE_KODE": None,
            "RTEE_KODETEKST": None,
            "STAMAFD_KODE": None,
            "STAMAFD_NAVN": None,
            "STAMAFD_ORGKODE": None,
            "STAMAFD_SORCODE": None,
            "UDFOERT_ADMENHED_KODE": None,
            "UDFOERT_ADMENHED_NAVN": None,
            "UDFOERT_ADMENHED_ORGKODE": None,
            "UDFOERT_ADMENHED_SORCODE": None,
            "LPR3_IGNORED": None,
            "BESKED": None,
            "OPDATERET_TID": None,
            "XML": multi_xml,
        }
        procs = _transform_ris_ydelse_to_fhir([row], _MockContext())
        assert len(procs) == 2
        codes = {p.code.coding[0].code for p in procs}
        assert codes == {"UXRC40", "UXRC41"}


@pytest.mark.unit
class TestBatchRoundtrip:
    """Validate batch transform produces correct counts and ordering."""

    def test_large_batch_diagnose(self):
        rows = [
            {
                "OBJEKT_ID": f"DIAG-BATCH-{i}",
                "PATIENT_ID": f"PAT-{i % 10}",
                "DIAGNOSE_KODE": f"D{i:04d}",
                "DIAGNOSE_KODETEKST": f"Diagnose {i}",
                "SLETTET": 1 if i % 7 == 0 else 0,
                "BESLUT_TIDSPUNKT": datetime(2024, 1, 1),
                "OPRET_TIDSPUNKT": datetime(2024, 1, 1),
                "AFSLUT_TIDSPUNKT": datetime(2024, 6, 1) if i % 3 == 0 else None,
                "MODTIDSPUNKT": datetime(2024, 1, 2),
                "BESLUT_PERSID": None,
                "BESLUT_PERSNAVN": None,
                "BESLUT_BRUGERROLLE": None,
                "BESLUT_ADMENHED_KODE": None,
                "BESLUT_ADMENHED_NAVN": None,
                "BESLUT_ADMENHED_ORGKODE": None,
                "AFSLUT_PERSID": None,
                "AFSLUT_ADMENHED_KODE": None,
                "AFSLUT_ADMENHED_NAVN": None,
                "AFSLUT_ADMENHED_ORGKODE": None,
                "OPRET_PERSID": None,
                "OPRET_PERSNAVN": None,
                "OPRET_BRUGERROLLE": None,
                "OPRET_ADMENHED_KODE": None,
                "OPRET_ADMENHED_NAVN": None,
                "OPRET_ADMENHED_ORGKODE": None,
            }
            for i in range(200)
        ]
        conds = _transform_diagnose_to_fhir(rows, _MockContext())
        assert len(conds) == 200

        deleted_count = sum(
            1 for c in conds if c.verificationStatus.coding[0].code == "entered-in-error"
        )
        expected_deleted = sum(1 for i in range(200) if i % 7 == 0)
        assert deleted_count == expected_deleted

        resolved_count = sum(
            1
            for c in conds
            if c.clinicalStatus is not None and c.clinicalStatus.coding[0].code == "resolved"
        )
        expected_resolved = sum(1 for i in range(200) if i % 3 == 0 and i % 7 != 0)
        assert resolved_count == expected_resolved

    def test_large_batch_administrative_procedure(self):
        rows = [
            {
                "OBJECT_ID": f"PROC-BATCH-{i}",
                "PATIENT_ID": f"PAT-{i % 10}",
                "ENCOUNTER_ID": f"ENC-{i}",
                "EPISODE_OF_CARE_ID": None,
                "PERIOD_START": "2024-08-01T08:00:00",
                "PERIOD_END": "2024-08-01T10:00:00",
                "PROCEDURE_CODE": f"K{i:04d}",
                "PROCEDURE_TEXT": f"Procedure {i}",
                "STATUS_CODE": None,
                "STATUS_TEXT": None,
                "INDICATION_OF_SIDE_CODE": None,
                "INDICATION_OF_SIDE_TEXT": None,
                "USED_CONTRAST_CODE": None,
                "USED_CONTRAST_TEXT": None,
                "STAFF_CATEGORY_CODE": None,
                "STAFF_CATEGORY_TEXT": None,
                "LOCATION_CODE": None,
                "LOCATION_NAME": None,
                "LOCATION_ORGCODE": None,
                "LOCATION_SORCODE": None,
                "EPJI_CREATED_DATETIME": None,
                "EPJI_MODIFICATION_DATETIME": None,
                "IFDB_MODIFICATION_DATETIME": None,
                "DELETED": 1 if i % 5 == 0 else 0,
                "USER_ID": None,
                "USER_NAME": None,
                "USER_ROLE": None,
                "LPR3_IGNORED": None,
                "USER_ROLE_NAME": None,
            }
            for i in range(200)
        ]
        procs = _transform_adm_procedure_to_fhir(rows, _MockContext())
        assert len(procs) == 200

        deleted_count = sum(
            1
            for p in procs
            if p.extension
            and any(
                ext.url == EXTENSION_FHIR_IS_DELETED and ext.valueInteger == 1
                for ext in p.extension
            )
        )
        expected_deleted = sum(1 for i in range(200) if i % 5 == 0)
        assert deleted_count == expected_deleted
