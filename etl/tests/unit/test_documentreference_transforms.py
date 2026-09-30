# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DocumentReference transform tests — deep, source-differentiated coverage.

Mirrors ``test_condition_transforms.py``: DocumentReference is fed by two IFDB
sources (EPIKRISE + CARE_DISCHARGE_REPORT) into one FHIR resource type, so each
source gets its own test class with its own row builder and transform function.

Field-level identity/deletion roundtrips also live (thin) in
``test_fhir_roundtrip.py`` alongside the other domains; the rich domain-specific
behaviour (docStatus mapping, priority absence, signature Mønster B, recipient
Mønster A fan-out, relatesTo, organization/cc/notification) is covered here.
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import dagster as dg
import pytest
from DSA.FHIR.DocumentReference.DocumentReference_IFDB_CareDischargeReport_dfm import (
    CARE_DISCHARGE_REPORT_REFERENCE_CONFIGS,
    _post_care_discharge_reports_to_fhir,
    _transform_care_discharge_reports_to_fhir,
)
from DSA.FHIR.DocumentReference.DocumentReference_IFDB_Epikrise_dfm import (
    EPIKRISE_NESTED_REFERENCE_CONFIGS,
    EPIKRISE_REFERENCE_CONFIGS,
    _post_epikrise_to_fhir,
    _transform_epikrise_to_fhir,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_DOCUMENT_REFERENCE_PRIORITY,
    EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS,
    EXTENSION_DOCUMENT_REFERENCE_SIGNATURE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_CARE_DISCHARGE_REPORT,
    SYSTEM_IFDB_EPIKRISE,
    SYSTEM_IFDB_NOTIFICATION_IDENTIFIER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_SOURCE_TABLE,
    SYSTEM_SOR,
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


def _find_ext(resource, url):
    """First top-level extension whose url == url, else None."""
    for ext in resource.extension or []:
        if ext.url == url:
            return ext
    return None


def _recipient_wrappers(resource):
    """The list of per-recipient wrapper extensions under the recipients parent."""
    parent = _find_ext(resource, EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS)
    return list(parent.extension) if parent else []


def _sub(wrapper, url):
    """First sub-field extension of a recipient/signature wrapper by url, else None."""
    for ext in wrapper.extension or []:
        if ext.url == url:
            return ext
    return None


def _epikrise_row(**overrides):
    """Minimal-but-complete arc.ifdb_epikrise row for the transform."""
    row = {
        "OBJEKT_ID": "EPI-RT-001",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-RT-100",
        "AFSENDER_ADMENHED_SORKODE": "123456789012345",
        "ENCOUNTER_ID": "ENC-RT-1",
        "EPISODE_OF_CARE_ID": None,
        "KORREKTION_AF_ID": None,
        "FOERSTE_AFSENDELSESTIDSPUNKT": datetime(2024, 5, 1, 10, 30),
        "EPIKRISESTATUS_KODETEKST": "Sendt",
        "EPIKRISETYPE_KODE": "PASEpikrise_ks.ur.Epikrisetype.Udskrivning",
        "EPIKRISETYPE_KODETEKST": "Udskrivning",
        "PRIORITET": "PASEpikrise_ks.ur.ActionPriority.WA",
        # UNDERSKRIVER (mode=legal) — always populated
        "UNDERSKRIVER_PERSID": "SIGN-1",
        "UNDERSKRIVER_PERSNAVN": "Dr. Underskriver",
        "UNDERSKRIVER_BRUGERROLLE": "role-legal",
        "UNDERSKRIVER_BRUGERROLLE_NAME": "Læge",
        "UNDERSKRIVER_ADMENHED_KODE": "afd-1",
        "UNDERSKRIVER_ADMENHED_NAVN": "Afdeling 1",
        "UNDERSKRIVER_ADMENHED_SORKODE": "111111111111111",
        "UNDERSKRIVER_TIDSPUNKT": datetime(2024, 5, 1, 10, 0),
        # REGISTRANT — always populated
        "REGISTRANT_PERSID": "REG-1",
        "REGISTRANT_PERSNAVN": "Reg Istrant",
        "REGISTRANT_BRUGERROLLE": None,
        "REGISTRANT_BRUGERROLLE_NAME": None,
        "REGISTRANT_ADMENHED_KODE": None,
        "REGISTRANT_ADMENHED_NAVN": None,
        "REGISTRANT_ADMENHED_SORKODE": None,
        "REGISTRANT_TIDSPUNKT": datetime(2024, 5, 1, 9, 0),
        # GODKENDER left absent by default (nullable ~81% populated)
    }
    row.update(overrides)
    return row


@pytest.mark.unit
class TestEpikriseDocumentReferenceTransform:
    """Source EPIKRISE row -> FHIR DocumentReference -> verify critical fields."""

    def test_identity_and_core_fields(self):
        row = _epikrise_row()
        modtagere = {
            "EPI-RT-001": [
                {
                    "OBJEKT_ID": "MOD-1",
                    "MODTAGERTYPE": "LAEGE",
                    "NAVN": "Egen læge",
                    "ADRESSE": "Lægevej 1",
                }
            ]
        }
        docs = _transform_epikrise_to_fhir([row], modtagere, _MockContext())
        d = docs[0]

        # Only identifier is set, not id (resolved at upsert).
        assert d.id is None
        assert d.identifier[0].system == SYSTEM_IFDB_EPIKRISE
        assert d.identifier[0].value == "EPI-RT-001"
        # subject reference by identifier (no id).
        assert d.subject.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert d.subject.identifier.value == "PAT-RT-100"
        # author (sender org via SOR).
        assert d.author[0].identifier.system == SYSTEM_SOR
        assert d.author[0].identifier.value == "123456789012345"
        # status current + docStatus final + type.
        assert d.status == "current"
        assert d.docStatus == "final"
        assert d.type.coding[0].code == "PASEpikrise_ks.ur.Epikrisetype.Udskrivning"
        assert d.type.coding[0].display == "Udskrivning"
        # source_table meta.tag discriminator.
        tag = d.meta.tag[0]
        assert tag.system == SYSTEM_IFDB_SOURCE_TABLE
        assert tag.code == "epikrise"
        # metadata-only content (no url), satisfies R4B content 1..*.
        assert len(d.content) == 1
        assert d.content[0].attachment.url is None
        assert d.content[0].attachment.contentType == "text/plain"

    def test_context_encounter_reference(self):
        d = _transform_epikrise_to_fhir([_epikrise_row()], {}, _MockContext())[0]
        enc_refs = d.context.encounter
        assert any(r.identifier.value == "ENC-RT-1" for r in enc_refs)

    def test_relates_to_correction(self):
        row = _epikrise_row(KORREKTION_AF_ID="EPI-RT-OLD")
        d = _transform_epikrise_to_fhir([row], {}, _MockContext())[0]
        assert d.relatesTo[0].code == "replaces"
        assert d.relatesTo[0].target.identifier.system == SYSTEM_IFDB_EPIKRISE
        assert d.relatesTo[0].target.identifier.value == "EPI-RT-OLD"

    def test_deleted_row_is_tombstoned(self):
        row = _epikrise_row(SLETTET=1)
        d = _transform_epikrise_to_fhir([row], {}, _MockContext())[0]
        assert d.status == "entered-in-error"
        is_deleted = _find_ext(d, EXTENSION_FHIR_IS_DELETED)
        assert is_deleted is not None
        assert is_deleted.valueInteger == 1

    def test_unknown_docstatus_is_null(self):
        # An unmapped EPIKRISESTATUS → docStatus NULL (+ warning, not asserted here).
        row = _epikrise_row(EPIKRISESTATUS_KODETEKST="Kladde")
        d = _transform_epikrise_to_fhir([row], {}, _MockContext())[0]
        assert d.docStatus is None

    def test_priority_empty_and_null_are_absent(self):
        for priority in ("", "   ", None):
            row = _epikrise_row(PRIORITET=priority)
            d = _transform_epikrise_to_fhir([row], {}, _MockContext())[0]
            assert _find_ext(d, EXTENSION_DOCUMENT_REFERENCE_PRIORITY) is None
        # A real value produces the extension.
        row = _epikrise_row(PRIORITET="PASEpikrise_ks.ur.ActionPriority.AC")
        d = _transform_epikrise_to_fhir([row], {}, _MockContext())[0]
        prio = _find_ext(d, EXTENSION_DOCUMENT_REFERENCE_PRIORITY)
        assert prio is not None
        assert prio.valueString == "PASEpikrise_ks.ur.ActionPriority.AC"

    def test_signature_roles_moenster_b(self):
        d = _transform_epikrise_to_fhir([_epikrise_row()], {}, _MockContext())[0]
        legal_url = f"{EXTENSION_DOCUMENT_REFERENCE_SIGNATURE}#underskriver"
        registrant_url = f"{EXTENSION_DOCUMENT_REFERENCE_SIGNATURE}#registrant"
        godkender_url = f"{EXTENSION_DOCUMENT_REFERENCE_SIGNATURE}#godkender"
        legal = _find_ext(d, legal_url)
        assert legal is not None
        assert _sub(legal, "persId").valueString == "SIGN-1"
        assert _sub(legal, "persName").valueString == "Dr. Underskriver"
        assert _find_ext(d, registrant_url) is not None
        # GODKENDER absent → its role extension is omitted entirely.
        assert _find_ext(d, godkender_url) is None

    def test_recipients_moenster_a_fanout(self):
        modtagere = {
            "EPI-RT-001": [
                {"OBJEKT_ID": "MOD-1", "MODTAGERTYPE": "LAEGE", "NAVN": "A", "ADRESSE": "Vej 1"},
                {"OBJEKT_ID": "MOD-2", "MODTAGERTYPE": "INTERN", "NAVN": "B", "ADRESSE": "Vej 2"},
            ]
        }
        d = _transform_epikrise_to_fhir([_epikrise_row()], modtagere, _MockContext())[0]
        wrappers = _recipient_wrappers(d)
        assert len(wrappers) == 2
        keys = {_sub(w, "recipientKey").valueIdentifier.value for w in wrappers}
        assert keys == {"MOD-1", "MOD-2"}
        first = wrappers[0]
        assert _sub(first, "type").valueString == "LAEGE"
        assert _sub(first, "name").valueString == "A"
        assert _sub(first, "address").valueString == "Vej 1"

    def test_no_recipients_omits_extension(self):
        d = _transform_epikrise_to_fhir([_epikrise_row()], {}, _MockContext())[0]
        assert _find_ext(d, EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS) is None


def _care_discharge_row(**overrides):
    """Minimal arc.ifdb_care_discharge_report row for the transform."""
    row = {
        "OBJECT_ID": "CDR-RT-001",
        "DELETED": 0,
        "PATIENT_ID": "PAT-RT-200",
        "NOTIFICATION_IDENTIFIER": "MSG-42",
        "SENDER_AFDELING_SORKODE": "999888777666555",
        "MODIFICATION_TIME": datetime(2024, 6, 2, 11, 15),
    }
    row.update(overrides)
    return row


def _receiver_row(**overrides):
    row = {
        "RECEIVER_AFDELING_KODE": "hbdks.ur.Kommune.630",
        "RECEIVER_AFDELING_SORKODE": "330651000016004",
        "CCRECEIVER": 0,
        "STATE_KODE": "MessageStatusCS.a.MessageStatus.Sent",
        "STATE_KODETEXT": "Sent",
        "TRANSMISSION_TIME": datetime(2024, 6, 2, 11, 20),
    }
    row.update(overrides)
    return row


@pytest.mark.unit
class TestCareDischargeDocumentReferenceTransform:
    """Source CARE_DISCHARGE_REPORT row -> FHIR DocumentReference -> verify fields."""

    def test_identity_and_core_fields(self):
        receivers = {"CDR-RT-001": [_receiver_row()]}
        docs = _transform_care_discharge_reports_to_fhir([_care_discharge_row()], receivers)
        d = docs[0]

        assert d.id is None
        assert d.identifier[0].system == SYSTEM_IFDB_CARE_DISCHARGE_REPORT
        assert d.identifier[0].value == "CDR-RT-001"
        # Secondary identifier: MedCom notification id.
        notif = [i for i in d.identifier if i.system == SYSTEM_IFDB_NOTIFICATION_IDENTIFIER]
        assert len(notif) == 1
        assert notif[0].value == "MSG-42"
        assert d.subject.identifier.value == "PAT-RT-200"
        assert d.author[0].identifier.system == SYSTEM_SOR
        assert d.status == "current"
        # care_discharge_report has no docStatus (source-specific NULL column).
        assert d.docStatus is None
        # source_table discriminator.
        assert d.meta.tag[0].code == "care_discharge_report"
        assert d.content[0].attachment.url is None

    def test_deleted_row_is_tombstoned(self):
        d = _transform_care_discharge_reports_to_fhir([_care_discharge_row(DELETED=1)], {})[0]
        assert d.status == "entered-in-error"
        assert _find_ext(d, EXTENSION_FHIR_IS_DELETED).valueInteger == 1

    def test_recipient_org_cc_and_status(self):
        receivers = {
            "CDR-RT-001": [
                _receiver_row(CCRECEIVER=1),
            ]
        }
        d = _transform_care_discharge_reports_to_fhir([_care_discharge_row()], receivers)[0]
        wrappers = _recipient_wrappers(d)
        assert len(wrappers) == 1
        w = wrappers[0]
        # recipientKey = RECEIVER_AFDELING_KODE|cc-flag.
        assert _sub(w, "recipientKey").valueIdentifier.value == "hbdks.ur.Kommune.630|1"
        # organization carried as SOR-identifier Reference (not resolved).
        org = _sub(w, "organization")
        assert org.valueReference.identifier.system == SYSTEM_SOR
        assert org.valueReference.identifier.value == "330651000016004"
        assert _sub(w, "ccFlag").valueBoolean is True
        assert _sub(w, "statusCode").valueString == "MessageStatusCS.a.MessageStatus.Sent"
        assert _sub(w, "statusText").valueString == "Sent"

    def test_notification_identifier_optional(self):
        d = _transform_care_discharge_reports_to_fhir(
            [_care_discharge_row(NOTIFICATION_IDENTIFIER=None)], {}
        )[0]
        assert all(i.system != SYSTEM_IFDB_NOTIFICATION_IDENTIFIER for i in d.identifier)

    def test_large_batch(self):
        rows = [
            _care_discharge_row(OBJECT_ID=f"CDR-{i}", DELETED=1 if i % 4 == 0 else 0)
            for i in range(150)
        ]
        docs = _transform_care_discharge_reports_to_fhir(rows, {})
        assert len(docs) == 150
        deleted = sum(1 for d in docs if d.status == "entered-in-error")
        assert deleted == sum(1 for i in range(150) if i % 4 == 0)


_EPI_MOD = "DSA.FHIR.DocumentReference.DocumentReference_IFDB_Epikrise_dfm"
_CDR_MOD = "DSA.FHIR.DocumentReference.DocumentReference_IFDB_CareDischargeReport_dfm"


@pytest.mark.unit
class TestEpikrisePosting:
    """Posting path for epikrise: reference resolution config + 2-pass relatesTo.

    Unlike Condition (which resolves missing patients/encounters up front),
    DocumentReference threads a patient-id -> CPR ``reference_fallback_fn`` through
    ``prepare_resources_for_upsert`` (work item 28596) and resolves relatesTo
    self-references in a second pass.
    """

    def test_empty_documents_short_circuits(self):
        with patch(f"{_EPI_MOD}.prepare_resources_for_upsert") as mock_prepare:
            result = _post_epikrise_to_fhir(
                context=_MockContext(),
                documents=[],
                fhir_database=MagicMock(),
                extracted_count=0,
            )
        assert result == 0
        mock_prepare.assert_not_called()

    @patch(f"{_EPI_MOD}.prepare_resources_for_upsert")
    def test_prepare_uses_epikrise_config_and_threads_fallback(self, mock_prepare):
        docs = _transform_epikrise_to_fhir([_epikrise_row()], {}, _MockContext())
        mock_prepare.return_value = docs
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = (1, 0, [])
        sentinel_fallback = object()

        result = _post_epikrise_to_fhir(
            context=_MockContext(),
            documents=docs,
            fhir_database=fhir_database,
            extracted_count=1,
            reference_fallback_fn=sentinel_fallback,
        )

        assert result == 1
        kwargs = mock_prepare.call_args.kwargs
        assert kwargs["resource_type"] == "DocumentReference"
        assert kwargs["preferred_id_systems"] == [SYSTEM_IFDB_EPIKRISE]
        assert kwargs["reference_configs"] is EPIKRISE_REFERENCE_CONFIGS
        assert kwargs["nested_reference_configs"] is EPIKRISE_NESTED_REFERENCE_CONFIGS
        assert kwargs["reference_fallback_fn"] is sentinel_fallback
        # No relatesTo on a plain row → single upsert pass.
        assert fhir_database.batch_upsert_resources.call_count == 1

    @patch(f"{_EPI_MOD}.prepare_resources_for_upsert")
    def test_pass1_failure_raises_and_blocks_checkpoint(self, mock_prepare):
        docs = _transform_epikrise_to_fhir([_epikrise_row()], {}, _MockContext())
        mock_prepare.return_value = docs
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = (0, 1, ["EPI-RT-001"])

        with pytest.raises(dg.Failure):
            _post_epikrise_to_fhir(
                context=_MockContext(),
                documents=docs,
                fhir_database=fhir_database,
                extracted_count=1,
            )

    @patch(f"{_EPI_MOD}.batch_resolve_references")
    @patch(f"{_EPI_MOD}.prepare_resources_for_upsert")
    def test_relatesto_resolved_in_second_pass(self, mock_prepare, mock_resolve):
        docs = _transform_epikrise_to_fhir(
            [_epikrise_row(KORREKTION_AF_ID="EPI-RT-OLD")], {}, _MockContext()
        )
        mock_prepare.return_value = docs
        mock_resolve.return_value = {"EPI-RT-OLD": "fhir-old-id"}
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = (1, 0, [])

        _post_epikrise_to_fhir(
            context=_MockContext(),
            documents=docs,
            fhir_database=fhir_database,
            extracted_count=1,
        )

        # Pass 1 (all) + pass 2 (docs with resolved relatesTo) = 2 upserts.
        assert fhir_database.batch_upsert_resources.call_count == 2
        target = docs[0].relatesTo[0].target
        assert target.id == "fhir-old-id"
        assert target.type == "DocumentReference"
        assert target.identifier is None

    @patch(f"{_EPI_MOD}.batch_resolve_references")
    @patch(f"{_EPI_MOD}.prepare_resources_for_upsert")
    def test_unresolved_relatesto_is_dropped(self, mock_prepare, mock_resolve):
        docs = _transform_epikrise_to_fhir(
            [_epikrise_row(KORREKTION_AF_ID="EPI-RT-MISSING")], {}, _MockContext()
        )
        mock_prepare.return_value = docs
        mock_resolve.return_value = {}  # target not found in the loaded window
        fhir_database = MagicMock()
        fhir_database.batch_upsert_resources.return_value = (1, 0, [])

        _post_epikrise_to_fhir(
            context=_MockContext(),
            documents=docs,
            fhir_database=fhir_database,
            extracted_count=1,
        )

        # Unresolved relatesTo entry dropped (is_required=False) → no second upsert.
        assert docs[0].relatesTo is None
        assert fhir_database.batch_upsert_resources.call_count == 1


@pytest.mark.unit
class TestCareDischargePosting:
    """Posting path for udskrivningsrapport: single-pass, no self-references."""

    @patch(f"{_CDR_MOD}.post_resources_direct")
    @patch(f"{_CDR_MOD}.prepare_resources_for_upsert")
    def test_prepare_uses_care_discharge_config_and_threads_fallback(self, mock_prepare, mock_post):
        docs = _transform_care_discharge_reports_to_fhir([_care_discharge_row()], {})
        mock_prepare.return_value = docs
        mock_post.return_value = "posted-result"
        sentinel_fallback = object()

        result = _post_care_discharge_reports_to_fhir(
            context=_MockContext(),
            documents=docs,
            fhir_database=MagicMock(),
            extracted_count=1,
            reference_fallback_fn=sentinel_fallback,
        )

        assert result == "posted-result"
        kwargs = mock_prepare.call_args.kwargs
        assert kwargs["resource_type"] == "DocumentReference"
        assert kwargs["preferred_id_systems"] == [SYSTEM_IFDB_CARE_DISCHARGE_REPORT]
        assert kwargs["reference_configs"] is CARE_DISCHARGE_REPORT_REFERENCE_CONFIGS
        assert kwargs["reference_fallback_fn"] is sentinel_fallback
        # No nested references for care_discharge (no context.encounter).
        assert "nested_reference_configs" not in kwargs
        mock_post.assert_called_once()
