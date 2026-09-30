# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test skeleton: extension cleanup contract.

The canonical allowed-extension list is captured in the proposed
contract in fixtures/ifdb_sfi/expected/allowed_extensions.csv. The integration
tests here assert that resources built by the Observation and QuestionnaireResponse paths do NOT carry disallowed
extensions and DO carry the allowed lineage extensions only when needed.
"""

from __future__ import annotations

import importlib.util

import pytest

pytestmark = [pytest.mark.integration]


def _has_module(dotted: str) -> bool:
    return importlib.util.find_spec(dotted) is not None


def test_allowed_extensions_table_has_required_columns(sfi_allowed_extensions):
    required = {"extension_url", "resource_type", "status", "rationale"}
    assert sfi_allowed_extensions, "allowed_extensions.csv must not be empty"
    for row in sfi_allowed_extensions:
        missing = required - set(row.keys())
        assert not missing, f"allowed_extensions row missing columns: {missing}"
        assert row["status"] in {
            "allowed",
            "disallowed",
        }, f"Unexpected status for {row['extension_url']}: {row['status']}"


def test_disallowed_extensions_cover_known_redundancies(sfi_allowed_extensions):
    """Known redundant extensions must be represented in the cleanup contract."""
    disallowed = {
        row["extension_url"] for row in sfi_allowed_extensions if row["status"] == "disallowed"
    }
    must_be_disallowed = {
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRootMeta",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatSourceMeta",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatHierarki",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatElementMeta",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRowType",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsudfoerelseMetadata",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsudfoerelseSpeciale",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitettypeKodeHierarki",
        "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatKontekst",
    }
    missing = must_be_disallowed - disallowed
    assert (
        not missing
    ), f"Extension cleanup contract must disallow {missing}; update allowed_extensions.csv."


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
def test_new_observations_carry_no_disallowed_extensions(sfi_xml_fixtures, sfi_allowed_extensions):
    pytest.skip(
        "Requires resource-backed coverage: build Observations for case_01/02 and assert no Extension.url "
        "is in the disallowed set."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB"),
    reason="QuestionnaireResponse builder import is unavailable.",
)
def test_new_qrs_carry_no_disallowed_extensions(sfi_xml_fixtures, sfi_allowed_extensions):
    pytest.skip(
        "Requires resource-backed coverage: build QR for case_03/04/05 and assert no Extension.url is in "
        "the disallowed set."
    )


@pytest.mark.real_resources
@pytest.mark.skipif(
    not _has_module("DSA.FHIR.Aktivitetsresultat"),
    reason="AKTIVITETSRESULTAT builder not yet refactored.",
)
def test_lineage_extensions_only_present_when_justified(sfi_xml_fixtures):
    """Allowed lineage extensions (episode-of-care, aktivitetstype-kode) may appear
    only when the corresponding source field exists. They must not appear empty.
    """
    pytest.skip(
        "Requires resource-backed coverage: assert that any allowed Extension on the new resources has a "
        "non-empty value and is justified by the source row."
    )
