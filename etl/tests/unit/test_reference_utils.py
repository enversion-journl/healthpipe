# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils.reference_utils import (
    create_identifier_reference,
    verify_reference_exists,
)

pytestmark = pytest.mark.unit


def test_verify_reference_exists_found_and_missing():
    resolved = {"abc": "123"}
    assert verify_reference_exists("abc", resolved, "Patient") == "123"

    with pytest.raises(ValueError):
        verify_reference_exists("missing", resolved, "Patient", raise_on_missing=True)

    assert verify_reference_exists("missing", resolved, "Patient", raise_on_missing=False) is None


def test_create_identifier_reference():
    ref = create_identifier_reference("urn:sys", "val")
    assert ref.identifier.system == "urn:sys"
    assert ref.identifier.value == "val"
