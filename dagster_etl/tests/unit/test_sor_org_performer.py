# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Tests for SOR Organization performer on Procedure resources.

WI 27260: Organization modelled as Procedure.performer.actor with
Reference.identifier.system = SYSTEM_SOR when SOR code is present.

Coverage:
- administrative_procedure: staff-only, org-only, staff+org, missing SOR
- ris_ydelse: org-performer, missing SOR, fallback STAMAFD
- aktivitetsresultat: Procedure dict org-performer
"""

from DSA.FHIR.Procedure.procedure_aktivitetsresultat_builder import (
    _build_aktivitetsresultat_procedure_dict,
)
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc import (
    _transform_adm_procedure_to_fhir,
)
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import (
    _transform_ris_ydelse_to_fhir,
)
from utils.fhir.identifier_systems import SYSTEM_IFDB_STAFF, SYSTEM_SOR


class _MockContext:
    log = None

    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

        def debug(self, *a, **kw):
            pass

    log = _Log()
    has_partition_key = False
    partition_key = "2024-06-01"


MOCK_CTX = _MockContext()


# ============================================================================
# ADMINISTRATIVE_PROCEDURE
# ============================================================================


def _adm_proc_row(**overrides) -> dict:
    base = {
        "OBJECT_ID": "ADM-001",
        "PATIENT_ID": "PAT-100",
        "ENCOUNTER_ID": "ENC-200",
        "EPISODE_OF_CARE_ID": "EOC-300",
        "PERIOD_START": "2024-06-01T08:00:00",
        "PERIOD_END": "2024-06-01T09:30:00",
        "PROCEDURE_CODE": "KXBA10",
        "PROCEDURE_TEXT": "Procedure text",
        "STATUS_CODE": "A",
        "STATUS_TEXT": "Aktiv",
        "INDICATION_OF_SIDE_CODE": None,
        "INDICATION_OF_SIDE_TEXT": None,
        "USED_CONTRAST_CODE": None,
        "USED_CONTRAST_TEXT": None,
        "STAFF_CATEGORY_CODE": None,
        "STAFF_CATEGORY_TEXT": None,
        "LOCATION_CODE": "LOC-01",
        "LOCATION_NAME": "Operationsgang 3",
        "LOCATION_ORGCODE": "ORG-SHS",
        "LOCATION_SORCODE": "503121000016001",
        "EPJI_CREATED_DATETIME": "2024-06-01T07:55:00",
        "EPJI_MODIFICATION_DATETIME": "2024-06-01T09:35:00",
        "IFDB_MODIFICATION_DATETIME": "2024-06-01T09:36:00",
        "DELETED": 0,
        "USER_ID": "USR-42",
        "USER_NAME": "DrHansen",
        "USER_ROLE": "Overlæge",
        "USER_ROLE_NAME": "Overlæge",
        "LPR3_IGNORED": 0,
    }
    base.update(overrides)
    return base


class TestAdminProcedureOrgPerformer:
    def test_staff_and_org_performer_both_present(self):
        row = _adm_proc_row()
        procs = _transform_adm_procedure_to_fhir([row], MOCK_CTX)
        assert len(procs) == 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 2

        staff = proc.performer[0]
        assert staff.actor.identifier.system == SYSTEM_IFDB_STAFF
        assert staff.actor.identifier.value == "USR-42"

        org = proc.performer[1]
        assert org.actor.identifier.system == SYSTEM_SOR
        assert org.actor.identifier.value == "503121000016001"

    def test_staff_only_no_sorcode(self):
        row = _adm_proc_row(LOCATION_SORCODE=None)
        procs = _transform_adm_procedure_to_fhir([row], MOCK_CTX)
        assert len(procs) == 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 1
        assert proc.performer[0].actor.identifier.system == SYSTEM_IFDB_STAFF

    def test_org_only_no_staff(self):
        row = _adm_proc_row(USER_ID=None, USER_NAME=None, USER_ROLE=None, USER_ROLE_NAME=None)
        procs = _transform_adm_procedure_to_fhir([row], MOCK_CTX)
        assert len(procs) == 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 1
        assert proc.performer[0].actor.identifier.system == SYSTEM_SOR
        assert proc.performer[0].actor.identifier.value == "503121000016001"

    def test_no_staff_no_org(self):
        row = _adm_proc_row(
            USER_ID=None,
            USER_NAME=None,
            USER_ROLE=None,
            USER_ROLE_NAME=None,
            LOCATION_SORCODE=None,
        )
        procs = _transform_adm_procedure_to_fhir([row], MOCK_CTX)
        assert len(procs) == 1
        assert procs[0].performer is None


# ============================================================================
# RIS_YDELSE
# ============================================================================

SAMPLE_XML = """<rekvisition>
  <service>
    <kode>UXRC00</kode>
  </service>
</rekvisition>"""


def _ris_row(**overrides) -> dict:
    base = {
        "OBJEKT_ID": "RIS-001",
        "MODTIDSPUNKT": "2024-07-15T10:00:00",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-200",
        "ENCOUNTER_ID": "ENC-400",
        "EPISODE_OF_CARE_ID": "EOC-500",
        "TILSTEDEVAERELSE_ID": "TILST-600",
        "XML": SAMPLE_XML,
        "STAMAFD_KODE": "STA-01",
        "STAMAFD_NAVN": "Røntgenafdeling",
        "STAMAFD_ORGKODE": "ORG-RIS",
        "UDFOERT_ADMENHED_KODE": "UAE-01",
        "UDFOERT_ADMENHED_NAVN": "CT-scanner 2",
        "UDFOERT_ADMENHED_ORGKODE": "ORG-CT",
        "UDFOERT_TIDSPUNKT": "2024-07-15T11:30:00",
        "OPDATERET_TID": "2024-07-15T12:00:00",
        "BESKED_ID": "MSG-789",
        "FILNAVN": "ris_20240715.xml",
        "STATE": "FINAL_REPORT",
        "TYPE": "RIS_SERVICE",
        "RTEE": "RTEE-001",
        "IGNORED": 0,
        "PERIOD_START": "2024-07-15T10:00:00",
        "PERIOD_END": "2024-07-15T12:00:00",
        "UDFOERT_ADMENHED_SORCODE": "SOR-CT-01",
        "STAMAFD_SORCODE": "SOR-STA-01",
        "LPR3_IGNORED": 0,
    }
    base.update(overrides)
    return base


class TestRisYdelseOrgPerformer:
    def test_org_performer_from_udfoert_sorcode(self):
        row = _ris_row()
        procs = _transform_ris_ydelse_to_fhir([row], MOCK_CTX)
        assert len(procs) >= 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 1
        assert proc.performer[0].actor.identifier.system == SYSTEM_SOR
        assert proc.performer[0].actor.identifier.value == "SOR-CT-01"

    def test_org_performer_fallback_to_stamafd(self):
        row = _ris_row(UDFOERT_ADMENHED_SORCODE=None)
        procs = _transform_ris_ydelse_to_fhir([row], MOCK_CTX)
        assert len(procs) >= 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 1
        assert proc.performer[0].actor.identifier.system == SYSTEM_SOR
        assert proc.performer[0].actor.identifier.value == "SOR-STA-01"

    def test_no_performer_when_no_sorcode(self):
        row = _ris_row(UDFOERT_ADMENHED_SORCODE=None, STAMAFD_SORCODE=None)
        procs = _transform_ris_ydelse_to_fhir([row], MOCK_CTX)
        assert len(procs) >= 1
        assert procs[0].performer is None


# ============================================================================
# AKTIVITETSRESULTAT (Procedure dict-builder)
# ============================================================================


def _aktres_proc_row(**overrides) -> dict:
    base = {
        "OBJEKT_ID": "AKT-001",
        "PATIENT_ID": "PAT-300",
        "ENCOUNTER_ID": "ENC-600",
        "EPISODE_OF_CARE_ID": "EOC-700",
        "AKTIVITETTYPE_KODE": "KXBA10",
        "AKTIVITETTYPE_KODETEKST": "Knæprotese",
        "PERIOD_START": "2024-08-01T08:00:00",
        "PERIOD_END": "2024-08-01T10:00:00",
        "SLETTET": 0,
        "UDFOERT_PERSID": "USR-99",
        "UDFOERT_PERSNAVN": "DrSørensen",
        "UDFOERT_BRUGERROLLE": "Kirurg",
        "UDFOERT_BRUGERROLLE_NAME": "Kirurg",
        "UDFOERT_ADMENHED_KODE": "ADM-01",
        "UDFOERT_ADMENHED_NAVN": "Ortopædi",
        "UDFOERT_ADMENHED_ORGKODE": "ORG-01",
        "UDFOERT_ADMENHED_SORCODE": "581841000016000",
        "MODTIDSPUNKT": "2024-08-01T10:05:00",
    }
    base.update(overrides)
    return base


class TestAktivitetsresultatProcedureOrgPerformer:
    def test_org_performer_in_procedure_dict(self):
        row = _aktres_proc_row()
        _rid, proc = _build_aktivitetsresultat_procedure_dict(row)
        assert "performer" in proc
        performers = proc["performer"]

        sor_entries = [
            p
            for p in performers
            if p.get("actor", {}).get("identifier", {}).get("system") == SYSTEM_SOR
        ]
        assert len(sor_entries) >= 1
        assert any(
            entry["actor"]["identifier"]["value"] == "581841000016000" for entry in sor_entries
        )

    def test_no_org_performer_when_no_sorcode(self):
        row = _aktres_proc_row(UDFOERT_ADMENHED_SORCODE=None)
        _rid, proc = _build_aktivitetsresultat_procedure_dict(row)
        if "performer" in proc:
            sor_entries = [
                p
                for p in proc["performer"]
                if p.get("actor", {}).get("identifier", {}).get("system") == SYSTEM_SOR
            ]
            assert len(sor_entries) == 0

    def test_staff_and_org_both_present(self):
        row = _aktres_proc_row()
        _rid, proc = _build_aktivitetsresultat_procedure_dict(row)
        performers = proc.get("performer", [])
        assert len(performers) >= 2

        staff_entries = [
            p
            for p in performers
            if p.get("actor", {}).get("identifier", {}).get("system") != SYSTEM_SOR
        ]
        sor_entries = [
            p
            for p in performers
            if p.get("actor", {}).get("identifier", {}).get("system") == SYSTEM_SOR
        ]
        assert len(staff_entries) >= 1
        assert len(sor_entries) >= 1
