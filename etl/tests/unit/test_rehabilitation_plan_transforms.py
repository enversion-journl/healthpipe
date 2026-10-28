# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Rehabilitation plan (genoptræningsplan, G-GOP) transform tests — deep coverage.

REHABILITATION_PLAN (+ _RECEIVER) → FHIR ServiceRequest. The rich
domain behaviour lives here: status/intent, the receiver fan-out (Mønster A),
receiverKey derivation from modtager_noegle, receiverType derivation,
source-meta, and the upsert identifier-system isolation that keeps this DFM source
from touching the legacy HENVISNING/RIS ServiceRequests.

Each test names the bug it would catch (per testing rules).
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB_RehabilitationPlan_dfm import (
    SOURCE_TABLE_REHABILITATION_PLAN,
    _transform_rehabilitation_plans_to_fhir,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_REHABILITATION_PLAN_RECEIVERS,
    EXTENSION_REHABILITATION_PLAN_SOURCE_META,
    EXTENSION_SOURCE_TABLE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_NOTIFICATION_IDENTIFIER,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_REHABILITATION_PLAN,
    SYSTEM_IFDB_REHABILITATION_PLAN_RECEIVER,
    SYSTEM_IFDB_REHABILITATION_PLAN_TYPE,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import EXTENSION_FHIR_IS_DELETED


def _find_ext(resource, url):
    for ext in resource.extension or []:
        if ext.url == url:
            return ext
    return None


def _receiver_wrappers(resource):
    parent = _find_ext(resource, EXTENSION_REHABILITATION_PLAN_RECEIVERS)
    return list(parent.extension) if parent else []


def _sub(wrapper, url):
    for ext in wrapper.extension or []:
        if ext.url == url:
            return ext
    return None


def _plan_row(**overrides):
    """Minimal-but-complete arc.ifdb_rehabilitation_plan row for the transform."""
    row = {
        "OBJECT_ID": "RP-001",
        "PATIENT_ID": "PAT-100",
        "TYPE_KODETEKST": "Almen genoptræning",
        "PLAN_VERSION": "G-GOP",
        "NOTIFICATION_IDENTIFIER": "NID-1",
        "SENDER_YDER_NAVN": None,
        "SENDER_YDER_NUMMER": None,
        "SENDER_AFDELING_KODE": "AFD01",
        "SENDER_AFDELING_KODETEKST": "Afdeling X",
        "SENDER_AFDELING_SORKODE": "123456789012345",
        "DELETED": "0",
        "MODIFICATION_TIME": datetime(2024, 5, 1, 10, 30),
        "UPDATE_TIME": datetime(2024, 5, 1, 10, 35),
    }
    row.update(overrides)
    return row


def _yder_receiver(**overrides):
    """A yder (practitioner) receiver row — afdeling columns are NULL.

    TRANSMISSION_TIME is a ``str`` (not a ``datetime``): arc's column is
    NVARCHAR(100), and ``fetch_child_rows_for_batch`` delivers strings in
    production.
    """
    row = {
        "OBJECT_ID": "RP-001",
        "modtager_noegle": "55",
        "RECEIVER_YDER_NUMMER": "55",
        "RECEIVER_YDER_NAVN": "Fys Klinik",
        "RECEIVER_AFDELING_KODE": None,
        "RECEIVER_AFDELING_SORKODE": None,
        "RECEIVER_AFDELING_KODETEKST": None,
        "STATE_KODE": "PASRehabilitation_ks.ur.SentState.Sent",
        "STATE_KODETEXT": "Sent",
        "TRANSMISSION_TIME": "2024-05-01 10:40:00",
    }
    row.update(overrides)
    return row


def _afdeling_receiver(**overrides):
    """An afdeling/kommune receiver row — yder columns are NULL."""
    row = {
        "OBJECT_ID": "RP-001",
        "modtager_noegle": "KOM01",
        "RECEIVER_YDER_NUMMER": None,
        "RECEIVER_YDER_NAVN": None,
        "RECEIVER_AFDELING_KODE": "KOM01",
        "RECEIVER_AFDELING_SORKODE": "999888777666555",
        "RECEIVER_AFDELING_KODETEKST": "Kommunal genoptræning",
        "STATE_KODE": "PASRehabilitation_ks.ur.SentState.Sent",
        "STATE_KODETEXT": "Sent",
        "TRANSMISSION_TIME": "2024-05-01 10:41:00",
    }
    row.update(overrides)
    return row


def _sor_receiver(**overrides):
    """A receiver row with only a SOR code — yder and afdeling code columns are NULL."""
    row = _afdeling_receiver(
        modtager_noegle="999888777666555",
        RECEIVER_AFDELING_KODE=None,
        RECEIVER_AFDELING_KODETEKST=None,
    )
    row.update(overrides)
    return row


@pytest.mark.unit
class TestRehabilitationPlanTransform:
    """arc.ifdb_rehabilitation_plan row -> FHIR ServiceRequest -> verify fields."""

    def test_identity_status_intent_and_code(self):
        # Bug caught: wrong identifier system, status inversion, intent regression,
        # or a fabricated code system/value for the plan type.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {})[0]

        assert d.id is None  # id resolved at upsert, only identifier set
        assert d.identifier[0].system == SYSTEM_IFDB_REHABILITATION_PLAN
        assert d.identifier[0].value == "RP-001"
        assert d.status == "active"
        assert d.intent == "order"
        assert d.subject.identifier.system == SYSTEM_IFDB_PATIENT_ID
        assert d.subject.identifier.value == "PAT-100"
        assert d.requester.identifier.system == SYSTEM_SOR
        assert d.requester.identifier.value == "123456789012345"
        # code = raw kodetekst as both code + display (+ text); no invented codes.
        assert d.code.coding[0].system == SYSTEM_IFDB_REHABILITATION_PLAN_TYPE
        assert d.code.coding[0].code == "Almen genoptræning"
        assert d.code.coding[0].display == "Almen genoptræning"
        assert d.code.text == "Almen genoptræning"
        assert d.authoredOn is not None

    def test_notification_identifier_present_only_when_set(self):
        # Bug caught: always emitting identifier[1] (would emit empty/NULL values).
        with_nid = _transform_rehabilitation_plans_to_fhir([_plan_row()], {})[0]
        assert any(
            i.system == SYSTEM_IFDB_NOTIFICATION_IDENTIFIER and i.value == "NID-1"
            for i in with_nid.identifier
        )
        without = _transform_rehabilitation_plans_to_fhir(
            [_plan_row(NOTIFICATION_IDENTIFIER=None)], {}
        )[0]
        assert all(i.system != SYSTEM_IFDB_NOTIFICATION_IDENTIFIER for i in without.identifier)
        assert len(without.identifier) == 1

    def test_requester_omitted_when_sorkode_null(self):
        # Bug caught: emitting a requester Reference with a NULL/empty SOR identifier.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row(SENDER_AFDELING_SORKODE=None)], {})[
            0
        ]
        assert d.requester is None

    def test_code_absent_when_type_kodetekst_empty(self):
        # Bug caught: building an empty coding when the plan type is missing.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row(TYPE_KODETEKST="")], {})[0]
        assert d.code is None

    def test_deleted_row_is_tombstoned(self):
        # Bug caught: soft-deleted plans not mapped to entered-in-error / missing is_deleted ext.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row(DELETED="1")], {})[0]
        assert d.status == "entered-in-error"
        assert _find_ext(d, EXTENSION_FHIR_IS_DELETED) is not None

    def test_source_table_is_first_class_extension(self):
        # Bug caught: sourceTable left as a nested source-meta sub-field, which the SOF
        # view no longer projects — [fhir].[ServiceRequest].source_table goes silently NULL.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {})[0]
        source_table = _find_ext(d, EXTENSION_SOURCE_TABLE)
        assert source_table is not None
        assert source_table.valueString == SOURCE_TABLE_REHABILITATION_PLAN

        meta = _find_ext(d, EXTENSION_REHABILITATION_PLAN_SOURCE_META)
        assert (
            _sub(meta, "sourceTable") is None
        ), "sourceTable must not be duplicated in source-meta"

    def test_source_meta_carries_sender_fields(self):
        # Bug caught: dropped lossless sender fields.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {})[0]
        meta = _find_ext(d, EXTENSION_REHABILITATION_PLAN_SOURCE_META)
        assert meta is not None
        assert _sub(meta, "senderAfdelingKode").valueString == "AFD01"
        assert _sub(meta, "senderAfdelingKodetekst").valueString == "Afdeling X"
        assert _sub(meta, "planVersion").valueString == "G-GOP"


@pytest.mark.unit
class TestRehabilitationPlanReceivers:
    """Receiver fan-out (Mønster A) + receiverKey/receiverType behaviour."""

    def test_fan_out_one_two_three(self):
        # Bug caught: receivers not grouped per plan / wrong wrapper count.
        for n in (0, 1, 2, 3):
            receivers = [
                _yder_receiver(
                    modtager_noegle=str(i),
                    RECEIVER_YDER_NUMMER=str(i),
                    TRANSMISSION_TIME=f"2024-05-01 10:{i:02d}:00",
                )
                for i in range(n)
            ]
            d = _transform_rehabilitation_plans_to_fhir(
                [_plan_row()], {"RP-001": receivers} if n else {}
            )[0]
            assert len(_receiver_wrappers(d)) == n

    def test_yder_receiver_subfields_and_type(self):
        # Bug caught: NULL emitted as a value; wrong receiverType; missing yderNavn.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {"RP-001": [_yder_receiver()]})[
            0
        ]
        w = _receiver_wrappers(d)[0]
        assert _sub(w, "receiverType").valueString == "yder"
        assert _sub(w, "yderNummer").valueString == "55"
        assert _sub(w, "yderNavn").valueString == "Fys Klinik"
        # NULL afdeling must NOT be emitted as a subfield.
        assert _sub(w, "afdelingKode") is None
        assert _sub(w, "statusCode").valueString.endswith("Sent")
        assert _sub(w, "timestamp") is not None

    def test_afdeling_receiver_type_and_org_reference(self):
        # Bug caught: afdeling receiver misclassified; SOR org not carried as identifier ref.
        d = _transform_rehabilitation_plans_to_fhir(
            [_plan_row()], {"RP-001": [_afdeling_receiver()]}
        )[0]
        w = _receiver_wrappers(d)[0]
        assert _sub(w, "receiverType").valueString == "afdeling"
        assert _sub(w, "afdelingKode").valueString == "KOM01"
        assert _sub(w, "yderNummer") is None
        org = _sub(w, "organization")
        assert org.valueReference.identifier.system == SYSTEM_SOR
        assert org.valueReference.identifier.value == "999888777666555"

    def test_receiver_key_is_modtager_and_system(self):
        # Bug caught: receiverKey built from anything but arc's modtager_noegle (e.g. with the
        # transmission time) → keys churn on a resend and every resend soft-deletes and
        # re-inserts the receiver's child row.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {"RP-001": [_yder_receiver()]})[
            0
        ]
        key = _sub(_receiver_wrappers(d)[0], "receiverKey").valueIdentifier
        assert key.system == SYSTEM_IFDB_REHABILITATION_PLAN_RECEIVER
        assert key.value == "55"

    def test_sor_only_receiver_keyed_on_sor_code(self):
        # Bug caught: a receiver with only a SOR code gets no key, or a type other than ukendt.
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {"RP-001": [_sor_receiver()]})[0]
        w = _receiver_wrappers(d)[0]
        assert _sub(w, "receiverKey").valueIdentifier.value == "999888777666555"
        assert _sub(w, "receiverType").valueString == "ukendt"
        assert _sub(w, "organization").valueReference.identifier.value == "999888777666555"

    def test_two_sor_only_receivers_on_one_plan_are_both_kept(self):
        # Bug caught: receivers with only a SOR code, sent at the same time, collapse into one.
        receivers = [
            _sor_receiver(),
            _sor_receiver(
                modtager_noegle="111222333444555",
                RECEIVER_AFDELING_SORKODE="111222333444555",
            ),
        ]
        d = _transform_rehabilitation_plans_to_fhir([_plan_row()], {"RP-001": receivers})[0]
        keys = {_sub(w, "receiverKey").valueIdentifier.value for w in _receiver_wrappers(d)}
        assert keys == {"999888777666555", "111222333444555"}

    def test_duplicate_receiver_key_raises(self):
        # Bug caught: a receiverKey that repeats within a plan breaks the arc PK guarantee;
        # dropping the extra receiver silently would hide it.
        dup = _yder_receiver()
        with pytest.raises(ValueError, match="RP-001"):
            _transform_rehabilitation_plans_to_fhir(
                [_plan_row()], {"RP-001": [dict(dup), dict(dup)]}
            )


@pytest.mark.unit
class TestRehabilitationPlanPosting:
    """Upsert isolation — the property that keeps this DFM source from clobbering
    the legacy HENVISNING/RIS ServiceRequests."""

    def test_upsert_keyed_only_on_rehabilitation_plan_system(self):
        # Bug caught: passing the wrong preferred_id_systems would let the upsert
        # match/overwrite HENVISNING or RIS ServiceRequests (cross-source corruption).
        from DSA.FHIR.ServiceRequest import ServiceRequest_IFDB_RehabilitationPlan_dfm as mod

        requests = _transform_rehabilitation_plans_to_fhir([_plan_row()], {})
        with (
            patch.object(mod, "prepare_resources_for_upsert", return_value=requests) as prep,
            patch.object(mod, "post_resources_direct", return_value=MagicMock()),
        ):
            mod._post_rehabilitation_plans_to_fhir(MagicMock(), requests, MagicMock(), 1)

        assert prep.call_args.kwargs["preferred_id_systems"] == [SYSTEM_IFDB_REHABILITATION_PLAN]
        assert prep.call_args.kwargs["resource_type"] == "ServiceRequest"
