# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for IFDB ServiceRequest transforms."""

from DSA.FHIR.ServiceRequest import ServiceRequest_IFDB as sr_mod
from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB import (
    _latest_history_by_henvisning,
    _refinfo_by_episode,
    _transform_henvisning_to_service_requests,
    _transform_ris_ydelse_to_service_requests,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_HENVISNING_SOURCE_META,
    EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_HENVISNING_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

SAMPLE_XML = """<rekvisition>
  <service>
    <kode>SKS.a.proUXRC00</kode>
    <tillaegskode>ZZ1234</tillaegskode>
  </service>
</rekvisition>"""


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    log = _Log()


def _henvisning_row(**overrides: object) -> dict:
    row = {
        "OBJEKT_ID": "HENV-1",
        "MODTIDSPUNKT": "2025-01-01T10:00:00",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-1",
        "EPISODE_OF_CARE_ID": "EOC-1",
        "HENVISNINGSDATO": "2025-01-01T09:00:00",
        "AFSENDELSESDATO": None,
        "SLUTDATO": "2025-01-10T00:00:00",
        "MODTAGET": "2025-01-01T10:30:00",
        "STATUS_KODE": "active",
        "STATUS_KODETEKST": "Active",
        "TYPE_KODE": "TYPE-A",
        "TYPE_KODETEKST": "Referral type",
        "INDKALDELSESPRIORIT_KODE": "akut",
        "INDKALDELSESPRIORIT_KODETEKST": "Akut",
        "HENVISNINGSMAADE_KODE": "HM",
        "HENVISNINGSMAADE_KODETEKST": "Mode",
        "REFERRAL_PURPOSE": "preserved only in source meta",
        "HENVIST_TIL_AFDELING_SORKODE": "SOR-PERF",
        "HENVIST_FRA_AFDELING_SORKODE": "SOR-REQ",
        "AFSENDER_AFDELING_SORKODE": None,
        "BEHANDLER_AFDELING_SORKODE": None,
        "OPDATERET_TID": "2025-01-02T00:00:00",
    }
    row.update(overrides)
    return row


def _history_row(**overrides: object) -> dict:
    row = {
        "HENVISNING_ID": "HENV-1",
        "ORDNING": 2,
        "HANDLING": "VISITERET",
        "BESLUT_TIDSPUNKT": "2025-01-02T12:00:00",
        "FRA_KODE": "OLD",
        "FRA_KODETEKST": "Old",
        "STATUS_KODE": "active",
        "STATUS_KODETEKST": "Active",
        "TIL_AFDELING_SORKODE": "SOR-PERF",
        "MODTAGETDATO": "2025-01-01T10:30:00",
        "VISITATIONSDATO": "2025-01-02T12:00:00",
        "VENTEGRUPPE": "VG",
        "EKSPERTOMRAADE": "EXP",
        "TREATMENTRIGHT": "TR",
        "FAELLESVISITATION": 1,
        "DIAGNOSE_ID": "PAS-1",
    }
    row.update(overrides)
    return row


def _refinfo_row(**overrides: object) -> dict:
    row = {
        "OBJECT_ID": "EOC-1",
        "REFERRAL_ORG_CODE": "ORG",
        "REFERRAL_ORG_NAME": "Referral org",
        "REFERRAL_ORG_ORGCODE": "ORGCODE",
        "REFERRAL_ORG_SORCODE": "SOR-REF",
        "PRACTITIONER_NUMBER": "123",
        "PRACTITIONER_NAME": "Name",
        "REFERRAL_TYPE_CODE": "RT",
        "REFERRAL_TYPE_TEXT": "Referral type",
        "REFERRAL_REASON_CODE": "RR",
        "REFERRAL_REASON_TEXT": "Referral reason",
        "FREECHOICE_CODE": "FC",
        "FREECHOICE_TEXT": "Free choice",
        "REFERRAL_DATE": "2025-01-01T00:00:00",
    }
    row.update(overrides)
    return row


def _ris_row(**overrides: object) -> dict:
    row = {
        "OBJEKT_ID": "RIS-1",
        "MODTIDSPUNKT": "2025-02-01T10:00:00",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-1",
        "ENCOUNTER_ID": "ENC-1",
        "EPISODE_OF_CARE_ID": "EOC-1",
        "TILSTEDEVAERELSE_ID": "TIL-1",
        "XML": SAMPLE_XML,
        "STAMAFD_KODE": "STA",
        "STAMAFD_NAVN": "Radiology",
        "STAMAFD_ORGKODE": "ORG",
        "UDFOERT_ADMENHED_KODE": "UDF",
        "UDFOERT_ADMENHED_NAVN": "CT",
        "UDFOERT_ADMENHED_ORGKODE": "UORG",
        "UDFOERT_TIDSPUNKT": "2025-02-01T11:00:00",
        "OPDATERET_TID": "2025-02-01T12:00:00",
        "BESKED_ID": "MSG",
        "FILNAVN": "ris.xml",
        "STATE": "ORDERED",
        "TYPE": "RIS_SERVICE",
        "RTEE": "RTEE",
        "IGNORED": 0,
        "PERIOD_START": "2025-02-02T08:00:00",
        "PERIOD_END": "2025-02-02T09:00:00",
        "UDFOERT_ADMENHED_SORCODE": "SOR-UDF",
        "STAMAFD_SORCODE": "SOR-STA",
        "LPR3_IGNORED": 0,
    }
    row.update(overrides)
    return row


def test_henvisning_becomes_service_request():
    history = _latest_history_by_henvisning([_history_row()])
    refinfo = _refinfo_by_episode([_refinfo_row()])
    requests = _transform_henvisning_to_service_requests(
        [_henvisning_row()],
        _MockContext(),
        history,
        refinfo,
    )

    assert len(requests) == 1
    req = requests[0]
    assert req.identifier[0].system == SYSTEM_IFDB_HENVISNING_ID
    assert req.identifier[0].value == "HENV-1"
    assert req.status == "active"
    assert req.intent == "proposal"
    assert req.priority == "asap"
    assert req.subject.identifier.system == SYSTEM_IFDB_PATIENT_ID
    assert req.subject.identifier.value == "PAT-1"
    assert req.requester.identifier.value == "SOR-REQ"
    assert req.performer[0].identifier.value == "SOR-PERF"
    assert req.reasonReference[0].identifier.value == "PAS-1"
    assert req.reasonCode is None

    eoc = [e for e in req.extension if e.url == EXTENSION_EPISODE_OF_CARE]
    assert eoc[0].valueReference.identifier.value == "EOC-1"
    source_meta = [e for e in req.extension if e.url == EXTENSION_HENVISNING_SOURCE_META]
    assert source_meta


def test_deleted_henvisning_enters_error_and_sets_is_deleted():
    requests = _transform_henvisning_to_service_requests(
        [_henvisning_row(SLETTET=1)],
        _MockContext(),
        {},
        {},
    )
    assert requests[0].status == "entered-in-error"
    is_deleted = [e for e in requests[0].extension if e.url == EXTENSION_FHIR_IS_DELETED]
    assert is_deleted[0].valueInteger == 1


def test_ris_ordered_becomes_active_service_request():
    requests = _transform_ris_ydelse_to_service_requests([_ris_row()], _MockContext())
    assert len(requests) == 1
    req = requests[0]
    assert req.identifier[0].system == SYSTEM_IFDB_RIS_YDELSE
    assert req.identifier[0].value == "RIS-1:1"
    assert req.status == "active"
    assert req.intent == "order"
    assert req.subject.identifier.value == "PAT-1"
    assert req.encounter.identifier.value == "ENC-1"
    assert req.code.coding[0].system == SYSTEM_SKS
    assert req.code.coding[0].code == "UXRC00"
    assert req.orderDetail[0].coding[0].code == "ZZ1234"
    assert req.occurrencePeriod.start is not None

    source_meta = [e for e in req.extension if e.url == EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META]
    assert source_meta


def test_ris_final_report_becomes_completed_service_request():
    requests = _transform_ris_ydelse_to_service_requests(
        [_ris_row(STATE="FINAL_REPORT")],
        _MockContext(),
    )
    assert requests[0].status == "completed"


def test_ris_invalid_xml_falls_back_without_raw_xml_or_code():
    requests = _transform_ris_ydelse_to_service_requests(
        [_ris_row(XML="<not-valid", STATE="BOOKED")],
        _MockContext(),
    )
    assert len(requests) == 1
    assert requests[0].identifier[0].value == "RIS-1:1"
    assert requests[0].status == "active"
    assert requests[0].code is None
    assert requests[0].orderDetail is None
    assert all("Xml" not in ext.url and "XML" not in ext.url for ext in requests[0].extension)


def test_ris_element_nummer_uses_absolute_service_position():
    # The first <service> has no <kode> and is skipped, but the coded service
    # keeps its absolute position (2) so a code-less sibling cannot shift the
    # identifier — protecting the stable Procedure.basedOn linkage key.
    xml = (
        "<rekvisition>"
        "<service><beskrivelse>no code</beskrivelse></service>"
        "<service><kode>UXRC00</kode></service>"
        "</rekvisition>"
    )
    requests = _transform_ris_ydelse_to_service_requests(
        [_ris_row(XML=xml, STATE="FINAL_REPORT")], _MockContext()
    )
    assert len(requests) == 1
    assert requests[0].identifier[0].value == "RIS-1:2"


def test_fetch_latest_history_picks_newest_per_henvisning(monkeypatch):
    grouped = {
        "H1": [
            {
                "HENVISNING_ID": "H1",
                "BESLUT_TIDSPUNKT": "2025-01-01T00:00:00",
                "ORDNING": 1,
                "DIAGNOSE_ID": "OLD",
            },
            {
                "HENVISNING_ID": "H1",
                "BESLUT_TIDSPUNKT": "2025-03-01T00:00:00",
                "ORDNING": 2,
                "DIAGNOSE_ID": "NEW",
            },
        ]
    }
    monkeypatch.setattr(sr_mod, "chunked_in_fetch_grouped", lambda *a, **k: grouped)
    latest = sr_mod._fetch_latest_history_by_henvisning_ids(["H1"], object(), _MockContext())
    assert latest["H1"]["DIAGNOSE_ID"] == "NEW"


def test_backfill_enrichment_only_fills_missing_keys(monkeypatch):
    henvisning_rows = [
        {"OBJEKT_ID": "H1", "EPISODE_OF_CARE_ID": "E1"},
        {"OBJEKT_ID": "H2", "EPISODE_OF_CARE_ID": "E2"},
    ]
    history_by_id = {"H1": {"HENVISNING_ID": "H1", "DIAGNOSE_ID": "EXISTING"}}
    refinfo_by_episode = {"E1": {"OBJECT_ID": "E1"}}

    monkeypatch.setattr(
        sr_mod,
        "_fetch_latest_history_by_henvisning_ids",
        lambda ids, *a, **k: {hid: {"HENVISNING_ID": hid, "DIAGNOSE_ID": "FETCHED"} for hid in ids},
    )
    monkeypatch.setattr(
        sr_mod,
        "_fetch_latest_refinfo_by_episode_ids",
        lambda ids, *a, **k: {eid: {"OBJECT_ID": eid} for eid in ids},
    )

    sr_mod._backfill_enrichment_from_oracle(
        henvisning_rows, history_by_id, refinfo_by_episode, object(), _MockContext()
    )

    # Present keys are left untouched; only the windowed-out keys are fetched.
    assert history_by_id["H1"]["DIAGNOSE_ID"] == "EXISTING"
    assert history_by_id["H2"]["DIAGNOSE_ID"] == "FETCHED"
    assert set(refinfo_by_episode) == {"E1", "E2"}
