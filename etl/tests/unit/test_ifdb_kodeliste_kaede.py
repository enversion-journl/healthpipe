# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kodeteksten staar i kodesystemet; koden bliver paa raekken og baerer joinet.

Kaeden skal hænge sammen i alle led: byggeren laeser feltet, projektionen
udelader teksten men beholder koden, tabellen har ingen tekstkolonne, og
vw_fhir joiner teksten paa igen under sit oprindelige navn. Ét led alene er
ikke nok — udelades teksten uden joinet, bliver aftagerfladen tavst fattigere.
"""

import json
import re
from pathlib import Path

import pytest
import yaml
from DSA.CodeSystem.ifdb.ifdb import (
    CODESYSTEM_NAVN,
    KODELISTE_FELTER,
    byg_ifdb_kodelister,
)
from DSA.CodeSystem.utils import codesystem_table_name

ETL = Path(__file__).resolve().parents[2]
VIEWS = ETL / "DSA/FHIR/SqlOnFhir/view_definitions"
VW = (ETL / "infrastructure/ddl/views/fhir/01_vw_fhir.sql").read_text()
DDL = (ETL / "infrastructure/ddl/tables/fhir/01_tables.sql").read_text()
MIGRATION = (
    ETL / "infrastructure/ddl/migrations/20260918_fhir_kodetekster_til_kodesystem.sql"
).read_text()

pytestmark = pytest.mark.unit

# (kildefelt, flad tabel, view, kodekolonne der bliver, tekstkolonne der forsvinder)
LOEFTEDE = (
    ("STATUS_KODE", "ServiceRequest", "servicerequest", "status_kode", "status_kodetekst"),
    (
        "HENVISNINGSMAADE_KODE",
        "ServiceRequest",
        "servicerequest",
        "henvisningsmaade_kode",
        "henvisningsmaade_kodetekst",
    ),
    (
        "INDKALDELSESPRIORIT_KODE",
        "ServiceRequest",
        "servicerequest",
        "prioritet_kode",
        "prioritet_kodetekst",
    ),
    (
        "HENVIST_TIL_MUNI_KODE",
        "ServiceRequest",
        "servicerequest",
        "henvist_til_muni_kode",
        "henvist_til_muni_kodetekst",
    ),
    (
        "INDICATION_OF_SIDE_CODE",
        "Procedure",
        "procedure",
        "body_site_code",
        "body_site_display",
    ),
    (
        "STAFF_CATEGORY_CODE",
        "Procedure",
        "procedure",
        "staff_category_code",
        "staff_category_text",
    ),
)


def _view_kolonner(navn: str) -> set[str]:
    view = json.loads((VIEWS / f"{navn}_view.json").read_text())
    return {c["name"] for del_ in view["select"] for c in del_.get("column", [])}


def _tabel_blok(tabel: str) -> str:
    start = DDL.index(f"CREATE TABLE [fhir].[{tabel}]")
    return DDL[start : DDL.index("GO", start)]


def _vw_blok(tabel: str) -> str:
    start = VW.index(f"CREATE OR ALTER VIEW [vw_fhir].[{tabel}]")
    return VW[start : VW.index("\nGO", start)]


@pytest.mark.parametrize("kildefelt,tabel,view,kodekolonne,tekstkolonne", LOEFTEDE)
def test_teksten_er_loeftet_og_koden_bliver(kildefelt, tabel, view, kodekolonne, tekstkolonne):
    kolonner = _view_kolonner(view)
    assert tekstkolonne not in kolonner, "projektionen maa ikke skrive teksten ud igen"
    assert kodekolonne in kolonner, "koden er joinets noegle og skal blive"
    blok = _tabel_blok(tabel)
    assert f"[{tekstkolonne}]" not in blok
    assert f"[{kodekolonne}]" in blok
    assert kildefelt in {kode for _, kode, _ in KODELISTE_FELTER}


@pytest.mark.parametrize("kildefelt,tabel,view,kodekolonne,tekstkolonne", LOEFTEDE)
def test_vw_fhir_joiner_teksten_paa_igen(kildefelt, tabel, view, kodekolonne, tekstkolonne):
    blok = _vw_blok(tabel)
    assert f"AS [{tekstkolonne}]" in blok, "aftageren skal se kolonnen under samme navn"
    alias = re.search(
        rf"\[codesystem\]\.\[IFDBConcept\] AS (\w+)\s*\n\s*ON \1\.\[code\] = \w+\.\[{kodekolonne}\]",
        blok,
    )
    assert alias, f"mangler join paa {kodekolonne}"
    # Teksten skal komme fra netop det join, ikke fra et vilkaarligt andet.
    assert re.search(
        rf"{alias.group(1)}\.\[display\]\s+AS \[{tekstkolonne}\]", blok
    ), f"{tekstkolonne} hentes ikke fra joinet paa {kodekolonne}"


def test_migrationen_fjerner_praecis_de_loeftede_kolonner():
    droppede = set(re.findall(r"DROP COLUMN \[(\w+)\];", MIGRATION))
    assert droppede == {t for *_, t in LOEFTEDE}


def test_lineage_beskriver_ikke_de_fjernede_kolonner():
    for fil, tabel in (
        ("fhir_servicerequest.yml", "ServiceRequest"),
        ("fhir_procedure.yml", "Procedure"),
    ):
        kolonner = yaml.safe_load((ETL / "data_lineage/fhir" / fil).read_text())["dso_lineage"][
            "Columns"
        ]
        for _, t, _, kode, tekst in LOEFTEDE:
            if t != tabel:
                continue
            assert tekst not in kolonner
            assert kode in kolonner


def test_byggeren_udleder_listenavnet_af_koden():
    concepts, properties, flertydige = byg_ifdb_kodelister(
        {
            "arc.ifdb_henvisning.STATUS_KODE": [
                ("PASHenvisning_ks.ur.Henvisningsstatus.Afsluttet", "Afsluttet", 1),
                ("PASHenvisning_ks.ur.Henvisningsstatus.Fejlet", "Fejlet", 1),
            ]
        }
    )
    assert not flertydige
    assert [c["CODE"] for c in concepts] == [
        "PASHenvisning_ks.ur.Henvisningsstatus.Afsluttet",
        "PASHenvisning_ks.ur.Henvisningsstatus.Fejlet",
    ]
    assert {p["VALUE"] for p in properties} == {"PASHenvisning_ks.ur.Henvisningsstatus"}
    assert {p["KEY"] for p in properties} == {"kodeliste"}


def test_byggeren_afviser_en_kode_med_flere_tekster():
    # Er teksten ikke bestemt af koden, er feltet ikke en kodeliste. Den skal blive
    # paa raekken, hvor den historiske ordlyd er bevaret.
    _, _, flertydige = byg_ifdb_kodelister(
        {"arc.ifdb_henvisning.STATUS_KODE": [("a.b.C", "Første ordlyd", 2)]}
    )
    assert flertydige == ["arc.ifdb_henvisning.STATUS_KODE"]


def test_kodesystemets_navn_peger_paa_de_tabeller_ddl_opretter():
    # Navnet slaas op i et register; en manglende nøgle opdages ellers foerst,
    # naar assettet koerer mod databasen.
    ddl = (ETL / "infrastructure/ddl/tables/codesystem/01_codesystem_tables.sql").read_text()
    for kind in ("concept", "properties"):
        tabel = codesystem_table_name(CODESYSTEM_NAVN, kind)
        assert f"[codesystem].[{tabel}]" in ddl, f"DDL opretter ikke {tabel}"
    assert codesystem_table_name(CODESYSTEM_NAVN, "concept") == "IFDBConcept"


def test_kodesystemet_synkroniseres_til_dso():
    # vw_fhir ligger paa DSO og joiner mod tabellen; uden synk er teksten NULL.
    from DSO.load.sync_configs import CODESYSTEM_SYNC_CONFIGS

    for noegle, tabel in (
        ("codesystem_ifdb_concept", "IFDBConcept"),
        ("codesystem_ifdb_properties", "IFDBProperties"),
    ):
        assert noegle in CODESYSTEM_SYNC_CONFIGS
        assert CODESYSTEM_SYNC_CONFIGS[noegle].table == tabel


def test_byggeren_afviser_samme_kode_med_forskellig_tekst_paa_tvaers_af_felter():
    # Koderne er fuldt kvalificerede, saa det burde ikke ske. Sker det alligevel,
    # ville den sidst indlaeste tekst overskrive den foerste uden en lyd, og
    # joinet i vw_fhir ville give et andet svar end kilderaekken.
    _, _, flertydige = byg_ifdb_kodelister(
        {
            "arc.ifdb_henvisning.STATUS_KODE": [("a.b.C", "Første ordlyd", 1)],
            "arc.ifdb_administrative_procedure.STATUS_CODE": [("a.b.C", "Anden ordlyd", 1)],
        }
    )
    assert flertydige == [
        "arc.ifdb_administrative_procedure.STATUS_CODE (koden findes med anden tekst i et andet felt)"
    ]


def test_samme_kode_med_samme_tekst_er_ikke_en_konflikt():
    # To felter maa gerne dele en kode, saa laenge de er enige om teksten.
    concepts, _, flertydige = byg_ifdb_kodelister(
        {
            "arc.ifdb_henvisning.STATUS_KODE": [("a.b.C", "Samme ordlyd", 1)],
            "arc.ifdb_administrative_procedure.STATUS_CODE": [("a.b.C", "Samme ordlyd", 1)],
        }
    )
    assert flertydige == []
    assert concepts == [{"CODE": "a.b.C", "DISPLAY": "Samme ordlyd"}]


def test_byggerens_deps_daekker_de_arkiver_den_laeser():
    """Et asset, der laeser en tabel uden at afhaenge af den, kan koere foer den er fyldt.

    Feltlisten spaender over to arkivtabeller, ikke én. Staar kun den ene i deps,
    kan byggeren naa at koere, foer den anden er indlaest — og kodelisten bliver
    saa bygget paa et halvt grundlag uden at noget fejler.
    """
    from DSA.CodeSystem.ifdb.ifdb import cs_ifdb_kodelister

    deps = {str(k.to_user_string()) for k in cs_ifdb_kodelister.dependency_keys}
    for tabel in {t for t, _, _ in KODELISTE_FELTER}:
        kerne = tabel.split(".", 1)[1].removeprefix("ifdb_")
        assert any(kerne in d for d in deps), f"{tabel} laeses uden en dep der daekker den: {deps}"
