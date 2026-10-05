# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for AKTIVITETSUDFOERELSE_SPECIALE extension builder."""

import pytest
from DSA.FHIR.Aktivitetsresultat.activity_metadata import (
    _build_speciale_extensions_dict,
)
from utils.fhir.fhir_extensions import EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE

pytestmark = pytest.mark.unit


class TestSpecialeExtension:
    def test_empty_rows_returns_empty(self):
        assert _build_speciale_extensions_dict([]) == []

    def test_single_speciale_row(self):
        rows = [
            {
                "SPECIALE_KODE": "SP01",
                "SPECIALE_KODETEKST": "Kardiologi",
                "SPECIALE_HOSPITAL_KODE": "H001",
                "SPECIALE_HOSPITAL_NAVN": "OUH",
                "SPECIALE_HOSPITAL_ORGKODE": "ORG01",
            }
        ]
        result = _build_speciale_extensions_dict(rows)
        assert len(result) == 1
        ext = result[0]
        assert ext["url"] == EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE
        assert len(ext["extension"]) == 1
        entry = ext["extension"][0]
        assert entry["url"] == "speciale"
        field_urls = {p["url"] for p in entry["extension"]}
        assert "specialeKode" in field_urls
        assert "specialeHospitalNavn" in field_urls

    def test_multiple_speciale_rows(self):
        rows = [
            {
                "SPECIALE_KODE": "SP01",
                "SPECIALE_KODETEKST": "A",
                "SPECIALE_HOSPITAL_KODE": None,
                "SPECIALE_HOSPITAL_NAVN": None,
                "SPECIALE_HOSPITAL_ORGKODE": None,
            },
            {
                "SPECIALE_KODE": "SP02",
                "SPECIALE_KODETEKST": "B",
                "SPECIALE_HOSPITAL_KODE": None,
                "SPECIALE_HOSPITAL_NAVN": None,
                "SPECIALE_HOSPITAL_ORGKODE": None,
            },
        ]
        result = _build_speciale_extensions_dict(rows)
        assert len(result) == 1
        assert len(result[0]["extension"]) == 2

    def test_null_values_skipped(self):
        rows = [
            {
                "SPECIALE_KODE": "SP01",
                "SPECIALE_KODETEKST": None,
                "SPECIALE_HOSPITAL_KODE": None,
                "SPECIALE_HOSPITAL_NAVN": None,
                "SPECIALE_HOSPITAL_ORGKODE": None,
            }
        ]
        result = _build_speciale_extensions_dict(rows)
        entry = result[0]["extension"][0]
        field_urls = {p["url"] for p in entry["extension"]}
        assert "specialeKode" in field_urls
        assert "specialeKodetekst" not in field_urls
