# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pin test: RIS_YDELSE dependency resolution must not see skipped rows.

The contract being pinned (from the chunked transform+post refactor plan):
RIS_YDELSE rows whose STATE is non-final (BOOKED, ORDERED, REJECTED, ...) or
whose XML cannot be parsed are silently skipped during transform. The post
function's dependency resolver iterates over the transformed Procedure list,
NOT over the raw rows, so a skipped row's PATIENT_ID/ENCOUNTER_ID/
EPISODE_OF_CARE_ID must not be passed to resolve_missing_*().

This guarantee must hold both before and after the chunked transform+post
refactor. If this test ever turns red after the refactor, it means the
chunked wrapper started leaking raw rows into dependency resolution — that is
a correctness bug, not a refactor.

The test uses a real (transform → post) round-trip and patches the dependency
resolver entry points so it can assert exactly which IDs the post function
asked the resolver for.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm import (
    _post_ris_ydelse_to_fhir,
    _transform_ris_ydelse_to_fhir,
)

pytestmark = pytest.mark.unit

SAMPLE_XML = """<rekvisition>
  <service>
    <kode>UXRC00</kode>
  </service>
</rekvisition>"""


def _row(**overrides: Any) -> dict:
    base = {
        "OBJEKT_ID": "RIS-final",
        "PATIENT_ID": "PAT-keep",
        "ENCOUNTER_ID": "ENC-keep",
        "EPISODE_OF_CARE_ID": "EOC-keep",
        "MODTIDSPUNKT": "2024-07-15T10:00:00",
        "UDFOERT_TIDSPUNKT": "2024-07-15T11:00:00",
        "PERIOD_START": "2024-07-15T10:00:00",
        "PERIOD_END": "2024-07-15T12:00:00",
        "STATE": "FINAL_REPORT",
        "TYPE": "RIS_SERVICE",
        "RTEE": None,
        "BESKED_ID": "MSG-1",
        "FILNAVN": "f.xml",
        "OPDATERET_TID": None,
        "STAMAFD_KODE": "S",
        "STAMAFD_NAVN": "S",
        "STAMAFD_ORGKODE": "S",
        "STAMAFD_SORCODE": "S",
        "UDFOERT_ADMENHED_KODE": "U",
        "UDFOERT_ADMENHED_NAVN": "U",
        "UDFOERT_ADMENHED_ORGKODE": "U",
        "UDFOERT_ADMENHED_SORCODE": "U",
        "TILSTEDEVAERELSE_ID": "T",
        "IGNORED": 0,
        "LPR3_IGNORED": 0,
        "SLETTET": 0,
        "XML": SAMPLE_XML,
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
        self.has_partition_key = False


@pytest.fixture
def fake_database() -> MagicMock:
    db = MagicMock()
    db.batch_upsert_resources.return_value = (1, 0, [])
    db.safe_clear_identifier_caches_for_resources.return_value = True
    return db


def test_skipped_rows_do_not_leak_into_dependency_resolution(
    fake_database: MagicMock,
) -> None:
    """A row with STATE=BOOKED is skipped by transform. Its PATIENT_ID must
    therefore never be passed to resolve_missing_patients — the post helper
    only sees the IDs from kept Procedures.
    """
    rows = [
        _row(
            OBJEKT_ID="RIS-skipped",
            PATIENT_ID="PAT-SKIP-must-not-leak",
            ENCOUNTER_ID="ENC-SKIP-must-not-leak",
            EPISODE_OF_CARE_ID="EOC-SKIP-must-not-leak",
            STATE="BOOKED",
        ),
        _row(
            OBJEKT_ID="RIS-kept",
            PATIENT_ID="PAT-keep",
            ENCOUNTER_ID="ENC-keep",
            EPISODE_OF_CARE_ID="EOC-keep",
            STATE="FINAL_REPORT",
        ),
    ]

    procedures = _transform_ris_ydelse_to_fhir(
        rows,
        _MockContext(),
        strict_source_validation=False,
    )
    # Sanity: only the FINAL_REPORT row produced a Procedure.
    assert len(procedures) == 1, "transform did not skip BOOKED row"
    assert procedures[0].subject.identifier.value == "PAT-keep"

    mssql = MagicMock()

    # Patch the three resolve_missing_* entry points where they are imported
    # inside _post_ris_ydelse_to_fhir, so we can capture the exact IDs.
    with (
        patch("DSA.FHIR.Patient.patient_service.ensure_patients") as mock_pat,
        patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters") as mock_enc,
        patch(
            "DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_episodes_of_care"
        ) as mock_eoc,
        patch(
            "DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.prepare_resources_for_upsert",
            side_effect=lambda resources, **kw: resources,
        ),
        patch(
            "DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.post_resources_direct",
            return_value=MagicMock(),
        ),
    ):
        _post_ris_ydelse_to_fhir(
            context=_MockContext(),
            procedures=procedures,
            fhir_database=fake_database,
            extracted_count=len(rows),
            mssql=mssql,
        )

    # The kept Procedure's IDs should appear; the skipped row's IDs must not.
    pat_call = mock_pat.call_args
    enc_call = mock_enc.call_args
    eoc_call = mock_eoc.call_args

    assert pat_call is not None, "resolve_missing_patients was not called"
    assert pat_call.kwargs["ifdb_patient_ids"] == {"PAT-keep"}
    assert "PAT-SKIP-must-not-leak" not in pat_call.kwargs["ifdb_patient_ids"]

    assert enc_call is not None, "resolve_missing_encounters was not called"
    assert enc_call.kwargs["encounter_ids"] == {"ENC-keep"}
    assert "ENC-SKIP-must-not-leak" not in enc_call.kwargs["encounter_ids"]

    assert eoc_call is not None, "resolve_missing_episodes_of_care not called"
    assert eoc_call.kwargs["eoc_ids"] == {"EOC-keep"}
    assert "EOC-SKIP-must-not-leak" not in eoc_call.kwargs["eoc_ids"]


def test_all_rows_skipped_means_no_dependency_resolution_at_all(
    fake_database: MagicMock,
) -> None:
    """If the transform produces zero Procedures, the post helper must not
    invoke any resolve_missing_*() — patient_ids_needed is empty.
    """
    rows = [
        _row(STATE="BOOKED", PATIENT_ID="PAT-A"),
        _row(STATE="ORDERED", PATIENT_ID="PAT-B"),
    ]
    procedures = _transform_ris_ydelse_to_fhir(
        rows,
        _MockContext(),
        strict_source_validation=False,
    )
    assert procedures == []

    mssql = MagicMock()

    with (
        patch("DSA.FHIR.Patient.patient_service.ensure_patients") as mock_pat,
        patch("DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_encounters") as mock_enc,
        patch(
            "DSA.FHIR.utils.ifdb.dependency_resolver.resolve_missing_episodes_of_care"
        ) as mock_eoc,
        patch(
            "DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.post_resources_direct",
            return_value=MagicMock(),
        ),
        patch(
            "DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse_dfm.prepare_resources_for_upsert",
            side_effect=lambda resources, **kw: resources,
        ),
    ):
        _post_ris_ydelse_to_fhir(
            context=_MockContext(),
            procedures=procedures,
            fhir_database=fake_database,
            extracted_count=len(rows),
            mssql=mssql,
        )

    assert mock_pat.call_count == 0
    assert mock_enc.call_count == 0
    assert mock_eoc.call_count == 0
