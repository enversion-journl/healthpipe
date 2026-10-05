# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Determinism tests for Procedure transforms.

Verifies that running the same input through the transform functions multiple
times always produces identical output, and that ordering is preserved.
"""

import json

import pytest
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm import (
    _transform_adm_procedure_to_fhir,
)
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm import (
    _transform_ris_ydelse_to_fhir,
)

pytestmark = pytest.mark.unit


class _MockContext:
    log = None

    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    def __init__(self):
        self.log = self._Log()


def _adm_row(idx: int) -> dict:
    return {
        "OBJECT_ID": f"ADM-{idx:04d}",
        "PATIENT_ID": f"PAT-{idx % 50}",
        "ENCOUNTER_ID": f"ENC-{idx % 30}",
        "EPISODE_OF_CARE_ID": f"EOC-{idx % 20}",
        "PERIOD_START": f"2024-06-{(idx % 28) + 1:02d}T08:00:00",
        "PERIOD_END": f"2024-06-{(idx % 28) + 1:02d}T09:00:00",
        "PROCEDURE_CODE": f"KXBA{idx % 10}",
        "PROCEDURE_TEXT": f"Procedure-{idx}",
        "STATUS_CODE": "A",
        "STATUS_TEXT": "aktiv",
        "INDICATION_OF_SIDE_CODE": "TUL1" if idx % 3 == 0 else None,
        "INDICATION_OF_SIDE_TEXT": "Venstre" if idx % 3 == 0 else None,
        "USED_CONTRAST_CODE": "1" if idx % 5 == 0 else None,
        "USED_CONTRAST_TEXT": "Kontrast" if idx % 5 == 0 else None,
        "STAFF_CATEGORY_CODE": "7170",
        "STAFF_CATEGORY_TEXT": "Læge",
        "LOCATION_CODE": f"LOC-{idx % 10}",
        "LOCATION_NAME": f"OP-{idx % 10}",
        "LOCATION_ORGCODE": "ORG-1",
        "LOCATION_SORCODE": "SOR-1",
        "EPJI_CREATED_DATETIME": "2024-01-01T08:00:00",
        "EPJI_MODIFICATION_DATETIME": "2024-01-01T09:00:00",
        "IFDB_MODIFICATION_DATETIME": "2024-01-01T09:01:00",
        "DELETED": 1 if idx % 7 == 0 else 0,
        "USER_ID": f"U-{idx % 5}",
        "USER_NAME": f"Dr-{idx % 5}",
        "USER_ROLE": "Overlæge",
        "LPR3_IGNORED": 0,
        "USER_ROLE_NAME": "Ortopæd",
    }


def _ris_row(idx: int) -> dict:
    return {
        "OBJEKT_ID": f"RIS-{idx:04d}",
        "MODTIDSPUNKT": "2024-07-15T10:00:00",
        "SLETTET": 1 if idx % 8 == 0 else 0,
        "PATIENT_ID": f"PAT-{idx % 40}",
        "ENCOUNTER_ID": f"ENC-{idx % 25}",
        "EPISODE_OF_CARE_ID": f"EOC-{idx % 15}",
        "TILSTEDEVAERELSE_ID": f"T-{idx}",
        "XML": (
            f"<rekvisition><service><kode>UX{idx:04d}</kode></service></rekvisition>"
            if idx % 3 == 0
            else "<rekvisition><service><kode>UX0000</kode></service></rekvisition>"
        ),
        "STAMAFD_KODE": "STA-01",
        "STAMAFD_NAVN": "Røntgen",
        "STAMAFD_ORGKODE": "O1",
        "UDFOERT_ADMENHED_KODE": "UAE-01",
        "UDFOERT_ADMENHED_NAVN": "CT-2",
        "UDFOERT_ADMENHED_ORGKODE": "OC-1",
        "UDFOERT_TIDSPUNKT": f"2024-07-{(idx % 28) + 1:02d}T11:00:00",
        "OPDATERET_TID": "2024-07-15T12:00:00",
        "BESKED_ID": f"MSG-{idx}",
        "FILNAVN": f"f-{idx}.xml",
        "STATE": "FINAL_REPORT",
        "TYPE": f"UXRC{idx % 10:02d}",
        "RTEE": f"R-{idx}",
        "IGNORED": 0,
        "PERIOD_START": "2024-07-15T10:00:00",
        "PERIOD_END": "2024-07-15T12:00:00",
        "UDFOERT_ADMENHED_SORCODE": "SOR-1",
        "STAMAFD_SORCODE": "SOR-2",
        "LPR3_IGNORED": 0,
    }


class TestAdministrativeProcedureDeterminism:
    def test_identical_runs(self):
        rows = [_adm_row(i) for i in range(50)]
        ctx = _MockContext()
        run1 = _transform_adm_procedure_to_fhir(rows, ctx)
        run2 = _transform_adm_procedure_to_fhir(rows, ctx)

        assert len(run1) == len(run2) == 50
        for p1, p2 in zip(run1, run2, strict=False):
            assert p1.json() == p2.json()

    def test_ordering_preserved(self):
        rows = [_adm_row(i) for i in range(30)]
        procs = _transform_adm_procedure_to_fhir(rows, _MockContext())
        for i, proc in enumerate(procs):
            assert proc.identifier[0].value == f"ADM-{i:04d}"

    def test_extension_count_stable(self):
        rows = [_adm_row(i) for i in range(20)]
        procs1 = _transform_adm_procedure_to_fhir(rows, _MockContext())
        procs2 = _transform_adm_procedure_to_fhir(rows, _MockContext())
        for p1, p2 in zip(procs1, procs2, strict=False):
            ext_count1 = len(p1.extension) if p1.extension else 0
            ext_count2 = len(p2.extension) if p2.extension else 0
            assert ext_count1 == ext_count2


class TestRisYdelseDeterminism:
    def test_identical_runs(self):
        rows = [_ris_row(i) for i in range(50)]
        ctx = _MockContext()
        run1 = _transform_ris_ydelse_to_fhir(rows, ctx)
        run2 = _transform_ris_ydelse_to_fhir(rows, ctx)

        assert len(run1) == len(run2) == 50
        for p1, p2 in zip(run1, run2, strict=False):
            assert p1.json() == p2.json()

    def test_ordering_preserved(self):
        rows = [_ris_row(i) for i in range(30)]
        procs = _transform_ris_ydelse_to_fhir(rows, _MockContext())
        for i, proc in enumerate(procs):
            assert proc.identifier[0].value == f"RIS-{i:04d}:1"

    def test_xml_preservation_stable(self):
        rows = [_ris_row(i) for i in range(20)]
        procs1 = _transform_ris_ydelse_to_fhir(rows, _MockContext())
        procs2 = _transform_ris_ydelse_to_fhir(rows, _MockContext())
        for p1, p2 in zip(procs1, procs2, strict=False):
            j1 = json.loads(p1.json())
            j2 = json.loads(p2.json())
            xml_exts_1 = [
                e for e in (j1.get("extension") or []) if "risYdelseXml" in e.get("url", "")
            ]
            xml_exts_2 = [
                e for e in (j2.get("extension") or []) if "risYdelseXml" in e.get("url", "")
            ]
            assert xml_exts_1 == xml_exts_2
