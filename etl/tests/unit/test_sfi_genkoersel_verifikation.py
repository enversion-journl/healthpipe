# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakttests for det midlertidige verifikationsasset."""

from __future__ import annotations

import pytest
from infrastructure.assets.sfi_genkoersel_verifikation import (
    AFSTEMNINGSTABELLER,
    TJEK,
)

pytestmark = pytest.mark.unit


def test_tjeknavne_er_unikke_og_klassificerede():
    navne = [t.navn for t in TJEK]
    assert len(navne) == len(set(navne))
    assert {t.art for t in TJEK} == {"kontrakt", "maaling"}


def test_hvert_tjek_maaler_brud_og_grundlag():
    # SQL-kontrakten: én række med (antal, grundlag), så et PASS på nul rækker
    # aldrig kan forveksles med et bevis.
    for t in TJEK:
        assert t.sql.upper().startswith("SELECT COUNT(*)"), t.navn
        assert "[fhir].[" in t.sql, t.navn
        assert t.hvorfor


def test_afstemningen_daekker_alle_ni_familietabeller():
    assert len(AFSTEMNINGSTABELLER) == 9
    assert "Procedure" in AFSTEMNINGSTABELLER
    assert "QuestionnaireResponse_item" in AFSTEMNINGSTABELLER
