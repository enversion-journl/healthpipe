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


class _Cursor:
    """Nul brud og ens tal på staging og DSO; kun storens korn afviger."""

    def __init__(self):
        self._sidste = ""

    def execute(self, sql, params=None):
        self._sidste = sql

    def fetchone(self):
        return (5,) if "SFI_datapoint_store" in self._sidste else (0, 0)


def _ressource():
    from contextlib import nullcontext
    from types import SimpleNamespace

    conn = SimpleNamespace(cursor=_Cursor)
    return SimpleNamespace(get_connection=lambda: nullcontext(conn))


@pytest.mark.parametrize("pause", [True, False])
def test_storens_korn_er_en_maaling_mens_refreshen_staar_paa_pause(monkeypatch, pause):
    from types import SimpleNamespace

    import dagster as dg
    import infrastructure.assets.sfi_genkoersel_verifikation as verifikation
    from infrastructure.assets.sfi_genkoersel_verifikation import sfi_genkoersel_verifikation

    since = "2026-10-27 09:00:00" if pause else None
    monkeypatch.setattr(
        verifikation,
        "DfmManager",
        lambda *a: SimpleNamespace(get_checkpoint=lambda *a, **k: since),
    )
    tags: dict = {}
    context = SimpleNamespace(
        resources=SimpleNamespace(mssql_target_staging=_ressource(), mssql_target_dso=_ressource()),
        run=SimpleNamespace(tags=tags),
        log=SimpleNamespace(info=lambda *a: None),
    )
    koer = sfi_genkoersel_verifikation.op.compute_fn.decorated_fn

    if pause:
        resultat = koer(context)
        assert resultat.metadata["rapport"].value["afstemning"]["SFI_datapoint_store_diff"] == 5
    else:
        with pytest.raises(dg.Failure, match="sfi_store_korn"):
            koer(context)
