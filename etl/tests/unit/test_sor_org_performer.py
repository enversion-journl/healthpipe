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

import pytest
from DSA.FHIR.Procedure.procedure_aktivitetsresultat_builder import (
    _build_aktivitetsresultat_procedure_dict,
)
from DSA.FHIR.Procedure.Procedure_IFDB_AdminProc_dfm import (
    PROCEDURE_NESTED_REFERENCE_CONFIGS,
    _transform_adm_procedure_to_fhir,
)
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm import (
    RIS_YDELSE_NESTED_REFERENCE_CONFIGS,
    _transform_ris_ydelse_to_fhir,
)
from DSA.FHIR.utils.reference_resolver import (
    apply_nested_reference_fhir_ids,
    collect_nested_reference_identifiers,
)
from utils.fhir.identifier_systems import (
    PERFORMER_FUNCTION_STAMAFDELING,
    PERFORMER_FUNCTION_UDFOERENDE,
    SYSTEM_IFDB_STAFF,
    SYSTEM_PERFORMER_FUNCTION,
    SYSTEM_SOR,
)

pytestmark = pytest.mark.unit


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
        "STATUS_TEXT": "aktiv",
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
        assert staff.actor.type == "Practitioner"
        assert staff.actor.identifier.system == SYSTEM_IFDB_STAFF
        assert staff.actor.identifier.value == "USR-42"
        assert staff.actor.display == "DrHansen"
        assert staff.function.coding[0].code == "Overlæge"

        org = proc.performer[1]
        assert org.actor.type == "Organization"
        assert org.actor.identifier.system == SYSTEM_SOR
        assert org.actor.identifier.value == "503121000016001"
        assert org.function.coding[0].system == SYSTEM_PERFORMER_FUNCTION
        assert org.function.coding[0].code == PERFORMER_FUNCTION_UDFOERENDE

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
    def test_udfoerende_og_stamafdeling_er_to_performere_med_funktion(self):
        row = _ris_row()
        procs = _transform_ris_ydelse_to_fhir([row], MOCK_CTX)
        assert len(procs) >= 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 2
        by_function = {p.function.coding[0].code: p for p in proc.performer}
        assert set(by_function) == {PERFORMER_FUNCTION_UDFOERENDE, PERFORMER_FUNCTION_STAMAFDELING}
        udf = by_function[PERFORMER_FUNCTION_UDFOERENDE]
        assert udf.actor.type == "Organization"
        assert udf.actor.identifier.system == SYSTEM_SOR
        assert udf.actor.identifier.value == "SOR-CT-01"
        assert udf.function.coding[0].system == SYSTEM_PERFORMER_FUNCTION
        stam = by_function[PERFORMER_FUNCTION_STAMAFDELING]
        assert stam.actor.identifier.value == "SOR-STA-01"

    def test_manglende_udfoerende_giver_kun_stamafdeling_ikke_fallback(self):
        row = _ris_row(UDFOERT_ADMENHED_SORCODE=None)
        procs = _transform_ris_ydelse_to_fhir([row], MOCK_CTX)
        assert len(procs) >= 1
        proc = procs[0]
        assert proc.performer is not None
        assert len(proc.performer) == 1
        assert proc.performer[0].function.coding[0].code == PERFORMER_FUNCTION_STAMAFDELING
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
        assert len(sor_entries) == 1
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
        assert len(sor_entries) == 1


# ============================================================================
# Opløsning af performer.actor -> Organization (SOR); kliniker bevares
# ============================================================================


def _performer_config(configs):
    (cfg,) = [c for c in configs if c.field_path == "performer.actor"]
    return cfg


class TestPerformerOrganizationResolution:
    def test_adm_proc_kun_organisation_samles_til_opslag(self):
        procs = _transform_adm_procedure_to_fhir([_adm_proc_row()], MOCK_CTX)
        cfg = _performer_config(PROCEDURE_NESTED_REFERENCE_CONFIGS)
        by_type = collect_nested_reference_identifiers(procs, cfg)
        assert by_type == {"Organization": {"503121000016001": ""}}

    def test_adm_proc_organisation_oploeses_og_kliniker_bevares(self):
        procs = _transform_adm_procedure_to_fhir([_adm_proc_row()], MOCK_CTX)
        cfg = _performer_config(PROCEDURE_NESTED_REFERENCE_CONFIGS)
        result = apply_nested_reference_fhir_ids(
            procs, cfg, {"Organization": {"503121000016001": "org-uuid-1"}}
        )
        proc = procs[0]
        staff, org = proc.performer
        assert org.actor.id == "org-uuid-1"
        assert org.actor.type == "Organization"
        assert org.actor.identifier is None
        # Klinikeren er ikke opløst: identifier og navn står stadig på referencen
        assert staff.actor.type == "Practitioner"
        assert staff.actor.identifier.value == "USR-42"
        assert staff.actor.display == "DrHansen"
        assert staff.actor.id is None
        assert result.total_unresolved == 0

    def test_ris_begge_organisationer_oploeses(self):
        procs = _transform_ris_ydelse_to_fhir([_ris_row()], MOCK_CTX)
        cfg = _performer_config(RIS_YDELSE_NESTED_REFERENCE_CONFIGS)
        by_type = collect_nested_reference_identifiers(procs, cfg)
        assert by_type == {"Organization": {"SOR-CT-01": "", "SOR-STA-01": ""}}
        apply_nested_reference_fhir_ids(
            procs, cfg, {"Organization": {"SOR-CT-01": "u1", "SOR-STA-01": "u2"}}
        )
        by_function = {p.function.coding[0].code: p.actor.id for p in procs[0].performer}
        assert by_function == {
            PERFORMER_FUNCTION_UDFOERENDE: "u1",
            PERFORMER_FUNCTION_STAMAFDELING: "u2",
        }


@pytest.mark.parametrize("resolved_id", ["syntetisk-organisation", None])
def test_aktivitetsresultat_active_post_path_resolves_only_organizations(monkeypatch, resolved_id):
    from copy import deepcopy
    from unittest.mock import Mock

    import dagster as dg
    from DSA.FHIR.Aktivitetsresultat.orchestrator import _post_chunk_families

    procedure = _build_aktivitetsresultat_procedure_dict(
        _aktres_proc_row(UDFOERT_PERSNAVN="Syntetisk kliniker")
    )
    original = deepcopy(procedure[1])
    observation = ("syntetisk-observation", {"performer": [{"identifier": {"value": "STAFF1"}}]})
    posted = {}

    def post(**kwargs):
        posted[kwargs["resource_type"]] = kwargs["resource_dicts"]
        return dg.MaterializeResult(
            metadata={"posted_count": len(kwargs["resource_dicts"]), "failed_count": 0}
        )

    monkeypatch.setattr("DSA.FHIR.utils.ifdb.fhir_posting.post_resource_dicts_direct", post)
    database = Mock()
    database.resolve_identifier_values.return_value = (
        {"581841000016000": resolved_id} if resolved_id else {}
    )
    _post_chunk_families(
        context=MOCK_CTX,
        observation_dicts=[observation],
        procedure_dicts=[procedure],
        condition_dicts=[],
        fhir_database=database,
        extracted_count=1,
        stage_id="syntetisk-test",
        thread_name_prefix="test",
        max_workers=1,
        invalidate_cache=False,
    )
    database.resolve_identifier_values.assert_called_once_with(
        resource_type="Organization", identifier_system=SYSTEM_SOR, values=["581841000016000"]
    )
    performers = posted["Procedure"][0][1]["performer"]
    assert len(performers) == 2
    assert performers[1] == original["performer"][1]
    assert posted["Observation"] == [observation]
    assert performers[0]["function"]["coding"][0]["code"] == PERFORMER_FUNCTION_UDFOERENDE
    if resolved_id:
        assert performers[0]["actor"] == {
            "id": resolved_id,
            "resourceType": "Organization",
            "display": "Ortopædi",
        }
    else:
        assert performers[0]["actor"] == original["performer"][0]["actor"]
