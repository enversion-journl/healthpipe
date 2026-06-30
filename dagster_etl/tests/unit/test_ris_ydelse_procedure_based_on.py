# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for RIS_YDELSE Procedure -> ServiceRequest linkage."""

from DSA.FHIR.Procedure.Procedure_IFDB_RisYdelse import _transform_ris_ydelse_to_fhir
from DSA.FHIR.ServiceRequest.ServiceRequest_IFDB import (
    _transform_ris_ydelse_to_service_requests,
)
from utils.fhir.identifier_systems import SYSTEM_IFDB_RIS_YDELSE

SAMPLE_XML = """<rekvisition>
  <service>
    <kode>UXRC00</kode>
  </service>
</rekvisition>"""


class _MockContext:
    class _Log:
        def info(self, *a, **kw):
            pass

        def warning(self, *a, **kw):
            pass

    log = _Log()


def _row(objekt_id: object) -> dict:
    return {
        "OBJEKT_ID": objekt_id,
        "MODTIDSPUNKT": "2025-02-01T10:00:00",
        "SLETTET": 0,
        "PATIENT_ID": "PAT-1",
        "ENCOUNTER_ID": "ENC-1",
        "EPISODE_OF_CARE_ID": "EOC-1",
        "XML": SAMPLE_XML,
        "UDFOERT_TIDSPUNKT": "2025-02-01T11:00:00",
        "OPDATERET_TID": "2025-02-01T12:00:00",
        "STATE": "FINAL_REPORT",
        "TYPE": "RIS_SERVICE",
    }


def test_ris_procedure_based_on_matches_service_request_identifier_numeric():
    """Oracle NUMBER OBJEKT_ID arrives as a float. Both pipelines must normalize
    to the same value-space, otherwise Procedure.basedOn never resolves the
    ServiceRequest. This cross-checks the two transforms on the *numeric* path
    (the real production case), which a string OBJEKT_ID would mask."""
    procedures = _transform_ris_ydelse_to_fhir([_row(12345.0)], _MockContext())
    service_requests = _transform_ris_ydelse_to_service_requests([_row(12345.0)], _MockContext())

    assert len(procedures) == 1
    assert len(service_requests) == 1
    proc = procedures[0]
    sr = service_requests[0]

    # The basedOn key the Procedure emits must equal the SR's business identifier.
    assert proc.basedOn[0].type == "ServiceRequest"
    assert proc.basedOn[0].identifier.system == SYSTEM_IFDB_RIS_YDELSE
    assert sr.identifier[0].system == SYSTEM_IFDB_RIS_YDELSE
    assert proc.basedOn[0].identifier.value == sr.identifier[0].value == "12345:1"
    # The Procedure's own identifier shares that normalized value-space too.
    assert proc.identifier[0].value == "12345:1"


def test_ris_procedure_based_on_string_objekt_id():
    """Non-numeric OBJEKT_IDs pass through normalize_oracle_id unchanged."""
    procedures = _transform_ris_ydelse_to_fhir([_row("RIS-1")], _MockContext())
    service_requests = _transform_ris_ydelse_to_service_requests([_row("RIS-1")], _MockContext())

    proc = procedures[0]
    sr = service_requests[0]
    assert proc.identifier[0].value == "RIS-1:1"
    assert proc.basedOn[0].identifier.value == sr.identifier[0].value == "RIS-1:1"
