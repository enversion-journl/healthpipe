# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for IFDB ServiceRequest transforms."""

import pytest
from DSA.FHIR.ServiceRequest import ServiceRequest_IFDB_dfm as sr_mod
from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_dfm import (
    _latest_history_by_henvisning,
    _refinfo_by_episode,
    _transform_henvisning_to_service_requests,
    _transform_ris_ydelse_to_service_requests,
)
from DSA.FHIR.utils.code_normalization import shak_identifier_value
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import extract_ris_row_fields
from utils.fhir.condition_categories import (
    CODE_REFERRAL_INDICATION,
    SYSTEM_CONDITION_CATEGORY_RSD,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_HENVISNING_SOURCE_META,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META,
    EXTENSION_SOURCE_TABLE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_HENVISNING_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
    SYSTEM_IFDB_STAFF,
    SYSTEM_SHAK,
    SYSTEM_SKS,
    SYSTEM_SKS_D,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED

pytestmark = pytest.mark.unit

# Mirrors the real RIS_YDELSE.XML shape: <kontakt>/<diagnose>/<rekvisition> as
# siblings, with the ordered service code under <rekvisition><service>.
SAMPLE_XML = """<ns2:local xmlns="http://systematic.dk/epj/PFPAS/xml/RISYdelse/Ydelse" xmlns:ns2="uri">
  <kontakt>
    <starttidspunkt>202605111423</starttidspunkt>
    <type>A</type>
    <henvisningsmaade>A</henvisningsmaade>
    <behandlingsansvarlig><institution>4202</institution><afdeling>04.1</afdeling><type>AFDELING</type></behandlingsansvarlig>
  </kontakt>
  <diagnose><type>A</type><kode>DZ016</kode></diagnose>
  <rekvisition>
    <start>202605111423</start>
    <prioritet>SPACE</prioritet>
    <rekvirent><institution>4202</institution><afdeling>04.1</afdeling><type>AFDELING</type></rekvirent>
    <producent><institution>6008</institution><afdeling>05.3</afdeling><type>AFDELING</type></producent>
    <service>
      <kode>SKS.a.proUXRC00</kode>
      <tillaegskode>ZZ1234</tillaegskode>
    </service>
  </rekvisition>
</ns2:local>"""


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    log = _Log()


def _ris_transform(rows):
    return _transform_ris_ydelse_to_service_requests(rows, _MockContext())


def _ris_requests(rows):
    requests, _ = _ris_transform(rows)
    return requests


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
        "STATUS_KODETEKST": "active",
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
        "STATUS_KODETEKST": "active",
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
        "PRACTITIONER_NAME": "name",
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
    requests, referral_conditions = _ris_transform([_ris_row()])
    assert len(requests) == 1
    assert len(referral_conditions) == 1
    req = requests[0]
    cond = referral_conditions[0]
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
    source_meta_urls = {e.url for e in source_meta[0].extension or []}
    assert {
        "beskedId",
        "filnavn",
        "ignored",
        "lpr3Ignored",
        "opdateretTid",
        "primaryCode",
        "rtee",
        "tillaegskode",
        "tilstedevaerelseId",
        "type",
        "udfoertTidspunkt",
    }.isdisjoint(source_meta_urls)
    assert req.category[0].coding[0].code == "radiology"
    assert len(req.category[0].coding) == 1

    # Clinical fields recovered from the XML via referral Condition + reasonReference.
    assert req.reasonReference[0].identifier.system == SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE
    assert req.reasonReference[0].identifier.value == "RIS-1"
    assert cond.code.coding[0].system == SYSTEM_SKS_D
    assert cond.code.coding[0].code == "DZ016"
    assert cond.verificationStatus.coding[0].code == "unconfirmed"
    assert cond.category[0].coding[0].system == SYSTEM_CONDITION_CATEGORY_RSD
    assert cond.category[0].coding[0].code == CODE_REFERRAL_INDICATION
    assert req.authoredOn is not None
    # prioritet was the SPACE sentinel -> no FHIR priority.
    assert req.priority is None
    # <kontakt> has no contact id, so it is kept as descriptive source metadata.
    assert "kontakt" in source_meta_urls
    assert "diagnoseType" in source_meta_urls


def test_ris_final_report_becomes_completed_service_request():
    requests = _ris_requests([_ris_row(STATE="FINAL_REPORT")])
    assert requests[0].status == "completed"


def test_ris_invalid_xml_falls_back_without_raw_xml_or_code():
    requests = _ris_requests([_ris_row(XML="<not-valid", STATE="BOOKED")])
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
    requests = _ris_requests([_ris_row(XML=xml, STATE="FINAL_REPORT")])
    assert len(requests) == 1
    assert requests[0].identifier[0].value == "RIS-1:2"


def test_extract_ris_row_fields_reads_clinical_xml():
    fields = extract_ris_row_fields(SAMPLE_XML, objekt_id="RIS-1")
    assert fields["diagnose_kode"] == "DZ016"
    assert fields["diagnose_type"] == "A"
    assert fields["rekvisition_start"] == "202605111423"
    # <prioritet>SPACE</prioritet> is the empty sentinel.
    assert "prioritet" not in fields
    # <rekvirent>/<producent> org blocks are extracted (institution+afdeling ->
    # SHAK carried as typed identifiers on the ServiceRequest).
    assert fields["rekvirent"] == {"institution": "4202", "afdeling": "04.1", "type": "AFDELING"}
    assert fields["producent"] == {"institution": "6008", "afdeling": "05.3", "type": "AFDELING"}
    assert fields["kontakt"]["starttidspunkt"] == "202605111423"
    assert fields["kontakt"]["behandlingsansvarlig_afdeling"] == "04.1"


def test_extract_ris_row_fields_warns_on_multiple_diagnose_blocks():
    # Lossless guard: first-wins must never be silent when extra diagnosis
    # codes exist in the XML.
    from unittest.mock import MagicMock

    ctx = MagicMock()
    xml = (
        "<local>"
        "<diagnose><kode>DZ016</kode></diagnose>"
        "<diagnose><kode>DA01</kode></diagnose>"
        "<rekvisition><service><kode>SKS.a.proUXRC00</kode></service></rekvisition>"
        "</local>"
    )
    fields = extract_ris_row_fields(xml, objekt_id="RIS-1", context=ctx)
    assert fields["diagnose_kode"] == "DZ016"
    ctx.log.warning.assert_called_once()


def test_extract_ris_row_fields_no_warning_for_single_diagnose_block():
    from unittest.mock import MagicMock

    ctx = MagicMock()
    fields = extract_ris_row_fields(SAMPLE_XML, objekt_id="RIS-1", context=ctx)
    assert fields["diagnose_kode"] == "DZ016"
    ctx.log.warning.assert_not_called()


def test_extract_ris_row_fields_empty_for_blank_or_invalid_xml():
    assert extract_ris_row_fields(None, objekt_id="RIS-1") == {}
    assert extract_ris_row_fields("<not-valid", objekt_id="RIS-1") == {}


def test_ris_service_fallback_never_picks_diagnose_code():
    # A code-less <service> plus a <diagnose><kode> must NOT let the diagnosis
    # code masquerade as the ordered service code.
    xml = (
        "<ns2:local xmlns:ns2='uri'>"
        "<diagnose><kode>DZ016</kode></diagnose>"
        "<rekvisition><service><beskrivelse>no code</beskrivelse></service></rekvisition>"
        "</ns2:local>"
    )
    requests, referral_conditions = _ris_transform([_ris_row(XML=xml, STATE="BOOKED")])
    assert len(requests) == 1
    assert requests[0].code is None
    assert len(referral_conditions) == 1
    assert referral_conditions[0].code.coding[0].code == "DZ016"
    assert requests[0].reasonReference[0].identifier.value == "RIS-1"


def test_ris_priority_maps_known_value():
    xml = SAMPLE_XML.replace("<prioritet>SPACE</prioritet>", "<prioritet>akut</prioritet>")
    requests = _ris_requests([_ris_row(XML=xml)])
    assert requests[0].priority == "asap"
    # The raw value is also preserved in source metadata.
    parts = _source_meta_parts(requests[0], EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META)
    assert parts["prioritet"].valueString == "akut"


def test_ris_unmapped_priority_and_unrecognised_diagnosis_kept_in_meta():
    # Values outside the mapped/normalisable sets must not vanish: no FHIR
    # priority/reasonCode, but the raw values survive in source metadata.
    xml = SAMPLE_XML.replace("<prioritet>SPACE</prioritet>", "<prioritet>P3</prioritet>").replace(
        "<kode>DZ016</kode>", "<kode>XYZ123</kode>"
    )
    requests, referral_conditions = _ris_transform([_ris_row(XML=xml)])
    req = requests[0]
    assert req.priority is None
    assert len(referral_conditions) == 1
    assert referral_conditions[0].code.coding is None
    assert referral_conditions[0].code.text == "XYZ123"
    assert req.reasonReference[0].identifier.value == "RIS-1"
    parts = _source_meta_parts(req, EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META)
    assert parts["prioritet"].valueString == "P3"
    assert parts["diagnoseKode"].valueString == "XYZ123"


def test_source_table_emitted_as_first_class_extension():
    ris = _ris_requests([_ris_row()])[0]
    henv = _transform_henvisning_to_service_requests([_henvisning_row()], _MockContext(), {}, {})[0]
    ris_by_url = {e.url: e for e in ris.extension}
    henv_by_url = {e.url: e for e in henv.extension}
    assert ris_by_url[EXTENSION_SOURCE_TABLE].valueString == "RIS_YDELSE"
    assert henv_by_url[EXTENSION_SOURCE_TABLE].valueString == "HENVISNING"


def test_ris_rows_carry_modtidspunkt_extension():
    # HENVISNING and RIS rows must stay symmetric on the receive-time extension.
    requests = _ris_requests([_ris_row()])
    urls = {e.url for e in requests[0].extension}
    assert EXTENSION_MODTIDSPUNKT in urls


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
    latest = sr_mod._fetch_latest_history_for_batch(["H1"], object())
    assert latest["H1"]["DIAGNOSE_ID"] == "NEW"


def test_build_enrichment_for_batch_keys_on_henvisning_and_episode(monkeypatch):
    """The archive lookup is keyed, so every row in the batch is covered.

    The legacy pipeline only fetched the keys its co-partition window had
    missed; reading arc.* there is no window, so both lookups are built from
    the batch's own ids.
    """
    henvisning_rows = [
        {"OBJEKT_ID": "H1", "EPISODE_OF_CARE_ID": "E1"},
        {"OBJEKT_ID": "H2", "EPISODE_OF_CARE_ID": "E2"},
        {"OBJEKT_ID": "H3", "EPISODE_OF_CARE_ID": None},
    ]
    seen: dict[str, list[str]] = {}

    def fake_history(ids, _mssql):
        seen["history"] = ids
        return {hid: {"HENVISNING_ID": hid, "DIAGNOSE_ID": "FETCHED"} for hid in ids}

    def fake_refinfo(ids, _mssql):
        seen["refinfo"] = ids
        return {eid: {"OBJECT_ID": eid} for eid in ids}

    monkeypatch.setattr(sr_mod, "_fetch_latest_history_for_batch", fake_history)
    monkeypatch.setattr(sr_mod, "_fetch_latest_refinfo_for_batch", fake_refinfo)

    history_by_id, refinfo_by_episode = sr_mod._build_enrichment_for_batch(
        henvisning_rows, object()
    )

    assert seen["history"] == ["H1", "H2", "H3"]
    # NULL episode ids are dropped, not looked up.
    assert seen["refinfo"] == ["E1", "E2"]
    assert set(history_by_id) == {"H1", "H2", "H3"}
    assert set(refinfo_by_episode) == {"E1", "E2"}


def _source_meta_parts(req, meta_url):
    """Return {url: Extension} for the sub-parts of a source-meta extension."""
    meta = [e for e in req.extension if e.url == meta_url]
    assert meta, f"missing source-meta extension {meta_url}"
    return {p.url: p for p in (meta[0].extension or [])}


def test_ris_source_meta_carries_rekvirent_producent_shak():
    requests = _ris_requests([_ris_row()])
    parts = _source_meta_parts(requests[0], EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META)

    # rekvirent 4202 + 04.1 -> 4202041 ; producent 6008 + 05.3 -> 6008053
    assert parts["rekvirentShak"].valueIdentifier.system == SYSTEM_SHAK
    assert parts["rekvirentShak"].valueIdentifier.value == "4202041"
    assert parts["producentShak"].valueIdentifier.system == SYSTEM_SHAK
    assert parts["producentShak"].valueIdentifier.value == "6008053"


def test_ris_source_meta_shak_absent_when_no_org_block():
    # No <rekvirent>/<producent> in XML -> no SHAK identifiers emitted.
    xml = (
        "<ns2:local xmlns:ns2='uri'>"
        "<rekvisition><service><kode>SKS.a.proUXRC00</kode></service></rekvisition>"
        "</ns2:local>"
    )
    requests = _ris_requests([_ris_row(XML=xml, STATE="BOOKED")])
    parts = _source_meta_parts(requests[0], EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META)
    assert "rekvirentShak" not in parts
    assert "producentShak" not in parts


def test_henvisning_referral_info_carries_shak_and_practitioner():
    history = _latest_history_by_henvisning([_history_row()])
    refinfo = _refinfo_by_episode(
        [_refinfo_row(REFERRAL_ORG_ORGCODE="SKS.a.afd4202041", PRACTITIONER_NUMBER="123")]
    )
    requests = _transform_henvisning_to_service_requests(
        [_henvisning_row()], _MockContext(), history, refinfo
    )
    parts = _source_meta_parts(requests[0], EXTENSION_HENVISNING_SOURCE_META)
    ref_parts = {p.url: p for p in (parts["referralInformation"].extension or [])}

    assert ref_parts["referralOrgShak"].valueIdentifier.system == SYSTEM_SHAK
    assert ref_parts["referralOrgShak"].valueIdentifier.value == "4202041"
    assert ref_parts["practitioner"].valueIdentifier.system == SYSTEM_IFDB_STAFF
    assert ref_parts["practitioner"].valueIdentifier.value == "123"


def test_shak_identifier_value_cases():
    # Prefix stripping (incl. SKS.a.00 numeric prefix), separator/dot removal,
    # uppercasing, alphabetic afsnit suffix preserved, composition, empty -> None.
    assert shak_identifier_value("SKS.a.afd5000108") == "5000108"
    assert shak_identifier_value("SKS.a.00500810") == "500810"
    assert shak_identifier_value("SKS.a.sgh6013") == "6013"
    assert shak_identifier_value("SKS.a.afd420243f") == "420243F"
    assert shak_identifier_value("4202" + "04.1") == "4202041"
    assert shak_identifier_value("6008" + "05.3") == "6008053"
    assert shak_identifier_value("") is None
    assert shak_identifier_value(None) is None


def test_ris_one_referral_condition_per_row_with_multiple_services():
    xml = (
        "<local>"
        "<diagnose><kode>DZ016</kode><type>A</type></diagnose>"
        "<rekvisition>"
        "<service><kode>SKS.a.proUXRC00</kode></service>"
        "<service><kode>SKS.a.proUXRC01</kode></service>"
        "</rekvisition>"
        "</local>"
    )
    requests, referral_conditions = _ris_transform([_ris_row(XML=xml)])
    assert len(requests) == 2
    assert len(referral_conditions) == 1
    assert all(req.reasonReference[0].identifier.value == "RIS-1" for req in requests)


def test_ris_duplicate_tillaegskoder_produce_single_order_detail():
    xml = SAMPLE_XML.replace(
        "<tillaegskode>ZZ1234</tillaegskode>",
        "<tillaegskode>ZZ1234</tillaegskode><tillaegskode>ZZ1234</tillaegskode>",
    )
    requests = _ris_requests([_ris_row(XML=xml)])
    order_details = requests[0].orderDetail
    assert len(order_details) == 1
    assert order_details[0].coding[0].code == "ZZ1234"


def test_ris_row_without_diagnose_has_no_referral_condition():
    xml = "<rekvisition><service><kode>SKS.a.proUXRC00</kode></service></rekvisition>"
    requests, referral_conditions = _ris_transform([_ris_row(XML=xml)])
    assert len(requests) == 1
    assert referral_conditions == []
    assert requests[0].reasonReference is None


def test_ris_deleted_row_referral_condition_keeps_unconfirmed_verification():
    requests, referral_conditions = _ris_transform([_ris_row(SLETTET=1)])
    assert len(referral_conditions) == 1
    assert referral_conditions[0].verificationStatus.coding[0].code == "unconfirmed"
    is_deleted = [e for e in referral_conditions[0].extension if e.url == EXTENSION_FHIR_IS_DELETED]
    assert is_deleted[0].valueInteger == 1


def test_ris_posting_posts_referral_conditions_before_service_requests():
    """Referral Conditions prepare/post before ServiceRequests when present."""
    from unittest.mock import MagicMock, patch

    import dagster as dg
    from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_dfm import _post_service_requests_to_fhir
    from fhir.resources.R4B.condition import Condition
    from fhir.resources.R4B.servicerequest import ServiceRequest

    call_order: list[tuple[str, str]] = []
    post_result = dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(1),
            "failed_count": dg.MetadataValue.int(0),
        }
    )

    def prepare_side_effect(*, resource_type: str, resources: list, **kwargs: object) -> list:
        call_order.append(("prepare", resource_type))
        return resources

    def post_side_effect(*, resource_type: str, **kwargs: object) -> dg.MaterializeResult:
        call_order.append(("post", resource_type))
        return post_result

    context = MagicMock()
    context.log = MagicMock()
    fhir_database = MagicMock()
    service_requests = [
        ServiceRequest.construct(
            status="active", intent="order", subject=None, encounter=None, extension=None
        )
    ]
    referral_conditions = [Condition.construct()]

    with (
        patch(
            "DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_dfm.prepare_resources_for_upsert",
            side_effect=prepare_side_effect,
        ) as mock_prepare,
        patch(
            "DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_dfm.post_resources_direct",
            side_effect=post_side_effect,
        ),
    ):
        _post_service_requests_to_fhir(
            context=context,
            service_requests=service_requests,
            fhir_database=fhir_database,
            extracted_count=1,
            referral_conditions=referral_conditions,
            resolve_missing_dependencies=False,
        )

    assert call_order == [
        ("prepare", "Condition"),
        ("post", "Condition"),
        ("prepare", "ServiceRequest"),
        ("post", "ServiceRequest"),
    ]
    cond_prepare = mock_prepare.call_args_list[0].kwargs
    assert cond_prepare["resource_type"] == "Condition"
    assert cond_prepare["preferred_id_systems"] == [SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE]
