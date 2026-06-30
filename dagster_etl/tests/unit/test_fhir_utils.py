# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from datetime import UTC, datetime

from DSA.FHIR.utils.fhir_utils import (
    SOURCE_TAG_SYSTEM,
    decorate_with_source,
    find_identifier,
)
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient


def test_decorate_with_source_adds_tag_and_extension():
    patient = Patient.construct()  # type: ignore
    updated = decorate_with_source(
        patient, "my-source", updated_at=datetime(2024, 1, 1, tzinfo=UTC)
    )

    # Meta tag added
    tag = updated.meta.tag[0]
    assert tag.system == SOURCE_TAG_SYSTEM
    assert tag.code == "my-source"

    # Extension added
    ext = next((e for e in updated.extension if e.url == "urn:enversion:source"), None)
    assert ext is not None
    codes = {
        e.url: getattr(e, "valueString", None) or getattr(e, "valueDateTime", None)
        for e in ext.extension
    }
    assert codes["source"] == "my-source"
    assert "updated" in codes


def test_find_identifier():
    patient = Patient(
        identifier=[
            Identifier(system="urn:sys1", value="123"),
            Identifier(system="urn:sys2", value="456"),
        ]
    )
    assert find_identifier(patient, "urn:sys1") == "123"
    assert find_identifier(patient, "urn:missing") is None
