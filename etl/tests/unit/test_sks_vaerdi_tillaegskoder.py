# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for konvertering af talværdi-tillægskoder til beregnede SKS-koder.

Facittabellen følger referencens kravspecifikation (afsnit 2.1.2.2,
38 rækker) og er suppleret med produktionsvaliderede koder (VPH4115/VPH0010 fra
ADM_PROCEDURE_SUPPL_CODES) samt de værdiformer, der er observeret i prod.
Afvigelse: ±999,99 droppes (PDF-facit VPK999K9/VNK999K9).
"""

import pytest
from DSA.FHIR.utils.ifdb.sks_vaerdi_tillaegskoder import (
    convert_value_tillaegskode,
    is_value_tillaegskode_element,
)

pytestmark = pytest.mark.unit


class TestHeltal:
    @pytest.mark.parametrize(
        ("vaerdi", "forventet"),
        [
            ("0", "VPH0000"),
            ("2", "VPH0002"),
            ("22", "VPH0022"),
            ("22228", "VPH2223"),
            ("99999", "VPH9999"),
            ("-222", "VNH0222"),
            ("-2222", "VNH2222"),
            ("-22228", "VNH2223"),
            ("-99999", "VNH9999"),
            ("4115", "VPH4115"),
            ("10", "VPH0010"),
            ("9999,5", "VPH9999"),
            ("-9999,5", "VNH9999"),
        ],
    )
    def test_kravspec_eksempler(self, vaerdi: str, forventet: str) -> None:
        assert convert_value_tillaegskode("TillaegskodeHeltal", vaerdi) == forventet

    def test_optionel_variant(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeHeltalOptionel", "7") == "VPH0007"

    def test_optionel_tom_eller_ugyldig_giver_none(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeHeltalOptionel", "") is None
        assert convert_value_tillaegskode("TillaegskodeHeltalOptionel", None) is None
        assert convert_value_tillaegskode("TillaegskodeHeltalOptionel", "abc") is None


class TestKommatal:
    @pytest.mark.parametrize(
        ("vaerdi", "forventet"),
        [
            ("0", "VPK000K0"),
            ("0,4", "VPK000K4"),
            ("0,04", "VPK00K04"),
            ("0,993", "VPK0K993"),
            ("0,4444", "VPK0K444"),
            ("4", "VPK004K0"),
            ("4,4", "VPK004K4"),
            ("4,44", "VPK04K44"),
            ("4,444", "VPK4K444"),
            ("4,4448", "VPK4K445"),
            ("44", "VPK044K0"),
            ("44,4", "VPK044K4"),
            ("44,44", "VPK44K44"),
            ("444,4", "VPK444K4"),
            ("444,48", "VPK444K5"),
            ("444,98", "VPK445K0"),
            ("-0", "VNK000K0"),
            ("-0,8", "VNK000K8"),
            ("-0,08", "VNK00K08"),
            ("-8", "VNK008K0"),
            ("-8,88", "VNK08K88"),
            ("-8,888", "VNK8K888"),
            ("-8,8888", "VNK8K889"),
            ("-8,8899", "VNK08K89"),
            ("-88", "VNK088K0"),
            ("-888,8", "VNK888K8"),
            ("-888,88", "VNK888K9"),
            ("10,4", "VPK010K4"),
            ("100", "VPK100K0"),
            ("2,552", "VPK2K552"),
            ("102,2", "VPK102K2"),
            ("1,45", "VPK01K45"),
        ],
    )
    def test_kravspec_og_prod_eksempler(self, vaerdi: str, forventet: str) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", vaerdi) == forventet

    @pytest.mark.parametrize("vaerdi", ["999,99", "-999,99"])
    def test_pdf_graenser_999_99_droppes(self, vaerdi: str) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", vaerdi) is None

    def test_punktum_som_decimaltegn_toleres(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", "1.45") == "VPK01K45"

    def test_efterstillede_nuller_normaliseres(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", "0,40") == "VPK000K4"

    def test_decimaler_afrundes_naar_formatet_er_fuldt(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", "444,44") == "VPK444K4"

    def test_heltalsdel_udenfor_udfaldsrum_giver_ingen_kode(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatal", "1234,5") is None

    def test_optionel_tom_eller_ugyldig_giver_none(self) -> None:
        assert convert_value_tillaegskode("TillaegskodeKommatalOptionel", "") is None
        assert convert_value_tillaegskode("TillaegskodeKommatalOptionel", None) is None
        assert convert_value_tillaegskode("TillaegskodeKommatalOptionel", "xyz") is None


class TestAfvisning:
    @pytest.mark.parametrize(
        ("navn", "vaerdi"),
        [
            ("TillaegskodeHeltal", "abc"),
            ("TillaegskodeHeltal", ""),
            ("TillaegskodeHeltal", None),
            ("TillaegskodeKommatal", "1,2,3"),
            ("Tillaegskoder", "2"),
            ("Operation", "2"),
            (None, "2"),
        ],
    )
    def test_ugyldige_input_giver_none(self, navn: str | None, vaerdi: str | None) -> None:
        assert convert_value_tillaegskode(navn, vaerdi) is None


class TestElementGenkendelse:
    @pytest.mark.parametrize(
        ("navn", "forventet"),
        [
            ("TillaegskodeHeltal", True),
            ("TillaegskodeHeltalOptionel", True),
            ("TillaegskodeKommatal", True),
            ("TillaegskodeKommatalOptionel", True),
            ("Tillaegskoder", False),
            ("Operation", False),
            (None, False),
        ],
    )
    def test_genkender_kun_talvaerdi_elementer(self, navn: str | None, forventet: bool) -> None:
        assert is_value_tillaegskode_element(navn) is forventet
