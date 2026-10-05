# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import pytest
from DSA.FHIR.utils import code_normalization as cn

pytestmark = pytest.mark.unit


def test_strip_sks_prefix_and_no_prefix():
    assert cn.strip_sks_prefix("SKS.a.afd4202011") == "4202011"
    assert cn.strip_sks_prefix("SKS.a.0ABC") == "ABC"
    assert cn.strip_sks_prefix("SKS.a.proKABC10") == "KABC10"
    assert cn.strip_sks_prefix("SKS.a.oprKFCA20") == "KFCA20"
    assert cn.strip_sks_prefix("4202011") == "4202011"
    assert cn.strip_sks_prefix(None) is None


def test_normalize_sks_coding_code_strips_source_prefix():
    assert cn.normalize_sks_coding_code("SKS.a.proKABC10") == "KABC10"
    assert cn.normalize_sks_coding_code("SKS.a.oprKFCA20") == "KFCA20"
    assert cn.normalize_sks_coding_code("SKS.a.diaDA01") == "DA01"
    assert cn.normalize_sks_coding_code("SKS.a.diaI10") == "I10"
    assert cn.normalize_sks_coding_code("KABC10") == "KABC10"
    assert cn.normalize_sks_coding_code(None) is None


def test_normalize_shak_and_sor():
    assert cn.normalize_shak_code("  SKS.a.sgh1234  ") == "1234"
    assert cn.normalize_shak_code("") is None

    assert cn.normalize_sor_code(" 215811000016008 ") == "215811000016008"
    assert cn.normalize_sor_code("") is None


def test_extract_shak_parent_from_suffix():
    assert cn.extract_shak_parent_from_suffix("600835S") == "600835"
    assert cn.extract_shak_parent_from_suffix("1234A") == "1234"
    assert cn.extract_shak_parent_from_suffix("4202011") is None
    assert cn.extract_shak_parent_from_suffix(None) is None


def test_is_sks_diagnosis_code():
    assert cn.is_sks_diagnosis_code("SKS.a.diaDA01") is True
    assert cn.is_sks_diagnosis_code("SKS.a.oprKABC10") is False
    assert cn.is_sks_procedure_code("SKS.a.diaDA01") is False


def test_to_sks_diagnosis_code_canonicalises_to_sks_d_form():
    # Bare WHO ICD-10 gets a leading D (the Danish SKS-D form).
    assert cn.to_sks_diagnosis_code("J44") == "DJ44"
    assert cn.to_sks_diagnosis_code("I10") == "DI10"
    # ICD-10 codes that themselves start with D are still prefixed: D50 -> DD50.
    assert cn.to_sks_diagnosis_code("D50") == "DD50"
    # Already SKS-D form is kept unchanged (idempotent).
    assert cn.to_sks_diagnosis_code("DA01") == "DA01"
    assert cn.to_sks_diagnosis_code("DJ44") == "DJ44"
    assert cn.to_sks_diagnosis_code("J44") == cn.to_sks_diagnosis_code("DJ44")
    # Any SKS prefix is stripped before canonicalisation.
    assert cn.to_sks_diagnosis_code("SKS.a.diaI10") == "DI10"
    assert cn.to_sks_diagnosis_code("SKS.a.diaDA01") == "DA01"
    # Schema indices / junk / empty are not SKS diagnosis codes -> None (no Condition).
    assert cn.to_sks_diagnosis_code("D") is None
    assert cn.to_sks_diagnosis_code("9") is None
    assert cn.to_sks_diagnosis_code("Z") is None
    assert cn.to_sks_diagnosis_code(None) is None


def test_extract_xml_clinical_diagnosis_code():
    # XML DiagnoseKode codes are SKS-gated and returned in canonical SKS-D form.
    assert (
        cn.extract_xml_clinical_diagnosis_code(
            element_navn="DiagnoseKode",
            element_kode="I10",
            element_vaerdi="I10",
        )
        == "DI10"
    )
    assert (
        cn.extract_xml_clinical_diagnosis_code(
            element_navn="Diagnosekode",
            element_kode="D",
            element_vaerdi="Fødselslæsion",
        )
        is None
    )


def test_build_unique_ordered_codings_strips_sks_prefix():
    from DSA.FHIR.utils.codings import build_unique_ordered_codings
    from utils.fhir.identifier_systems import SYSTEM_SKS

    codings = build_unique_ordered_codings(
        system=SYSTEM_SKS,
        primary_code="SKS.a.diaDA01",
        primary_display="Hypertension",
        supplementary_rows=[
            {"TILLAEGSKODE_KODE": "SKS.a.tilKZ12", "TILLAEGSKODE_KODETEKST": "Tillæg"}
        ],
        supplement_code_key="TILLAEGSKODE_KODE",
        supplement_display_key="TILLAEGSKODE_KODETEKST",
    )
    assert [coding.code for coding in codings] == ["DA01", "KZ12"]
