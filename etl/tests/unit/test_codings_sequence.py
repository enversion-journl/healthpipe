# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tillægskodernes rækkefølge (IDX) skal overleve som codingSequence-extension."""

from DSA.FHIR.utils.codings import build_unique_ordered_codings
from utils.fhir.fhir_extensions import EXTENSION_CODING_SEQUENCE
from utils.fhir.identifier_systems import SYSTEM_SKS


def _sequence(coding) -> int | None:
    for ext in coding.extension or []:
        if ext.url == EXTENSION_CODING_SEQUENCE:
            return ext.valueInteger
    return None


def test_supplementary_codings_carry_source_sequence():
    codings = build_unique_ordered_codings(
        system=SYSTEM_SKS,
        primary_code="KNGA10",
        primary_display="Primær",
        supplementary_rows=[
            {"SUPPL_CODE": "TUL1", "SUPPL_TEXT": "højre", "IDX": "2"},
            {"SUPPL_CODE": "ZZZ", "SUPPL_TEXT": "andet", "IDX": 1},
        ],
        supplement_code_key="SUPPL_CODE",
        supplement_display_key="SUPPL_TEXT",
        supplement_sequence_key="IDX",
    )
    primary, *supplementary = codings
    assert primary.userSelected is True and _sequence(primary) is None
    assert [_sequence(c) for c in supplementary] == [2, 1]


def test_sequence_is_omitted_when_no_key_is_given():
    codings = build_unique_ordered_codings(
        system=SYSTEM_SKS,
        primary_code="KNGA10",
        supplementary_rows=[{"KODE": "TUL1", "TEKST": "højre"}],
        supplement_code_key="KODE",
        supplement_display_key="TEKST",
    )
    assert all(_sequence(c) is None for c in codings)
