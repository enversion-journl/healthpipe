# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for build_unique_ordered_codings SKS-D split behaviour."""

import pytest
from DSA.FHIR.utils.codings import build_unique_ordered_codings
from utils.fhir.identifier_systems import SYSTEM_SKS, SYSTEM_SKS_D

pytestmark = pytest.mark.unit


def test_supplement_repeating_primary_code_dropped_across_sks_d_split():
    codings = build_unique_ordered_codings(
        system=SYSTEM_SKS,
        primary_system=SYSTEM_SKS_D,
        primary_code="DM142",
        supplementary_rows=[{"TILLAEGSKODE_KODE": "DM142", "TILLAEGSKODE_KODETEKST": "Dup"}],
        supplement_code_key="TILLAEGSKODE_KODE",
        supplement_display_key="TILLAEGSKODE_KODETEKST",
    )
    assert len(codings) == 1
    assert codings[0].system == SYSTEM_SKS_D
    assert codings[0].code == "DM142"
