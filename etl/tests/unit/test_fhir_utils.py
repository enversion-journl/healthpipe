# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils.fhir_utils import find_identifier
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient

pytestmark = pytest.mark.unit


def test_find_identifier():
    patient = Patient(
        identifier=[
            Identifier(system="urn:sys1", value="123"),
            Identifier(system="urn:sys2", value="456"),
        ]
    )
    assert find_identifier(patient, "urn:sys1") == "123"
    assert find_identifier(patient, "urn:missing") is None
