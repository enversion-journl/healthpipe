# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Hver leveret kolonne skal sige, hvor den kommer fra.

En kolonne, der bliver projiceret ned i den flade tabel uden en kildeangivelse,
ser komplet ud for aftageren og er umulig at efterproeve. Det er ikke et tab af
data, men et tab af tillid til modellen — og det opdages foerst, naar nogen
spoerger, hvor et tal stammer fra.

Kolonner uden kilde findes med rette: nøgler og referencer, som resolveren
tildeler ved posting, faste URI'er, og lagerets egne tidsstempler. De staar
opremset her med begrundelse, saa en ny kolonne ikke kan glide ind i gruppen ved
et uheld.
"""

import re
from pathlib import Path

import pytest
import yaml

ETL = Path(__file__).resolve().parents[2]
LINEAGE = ETL / "data_lineage/fhir"

pytestmark = pytest.mark.unit

# Tabellerne, som IFDB-variabellisten daekker.
TABELLER = (
    "fhir_condition",
    "fhir_condition_henvisning",
    "fhir_condition_supplementary_code",
    "fhir_procedure",
    "fhir_procedure_supplementary_code",
    "fhir_servicerequest",
    "fhir_service_request_supplementary_code",
    "fhir_observation_speciale",
)

# Kolonner, der med rette ikke har en kildekolonne, med grunden.
UDEN_KILDE: dict[str, str] = {
    # Nøgler og referencer, resolveren tildeler ved posting. Patient-, kontakt- og
    # forløbsreferencer staar IKKE her: de opløses ud fra en kildekolonne, og den
    # kolonne skal kunne læses af en maskine, ikke kun af prosaen i Description.
    "condition_id": "resolver-tildelt ressource-id",
    "observation_id": "resolver-tildelt ressource-id",
    "procedure_id": "resolver-tildelt ressource-id",
    "service_request_id": "resolver-tildelt ressource-id",
    "requester_id": "resolver-tildelt reference",
    "performer_id": "resolver-tildelt reference",
    "performer_organization_id": "resolver-tildelt reference",
    "stamafdeling_organization_id": "resolver-tildelt reference",
    # Faste URI'er og kategorier, der ikke kommer fra en kildekolonne.
    "code_system": "fast kodesystem-URI",
    "identifier_system": "fast identifier-URI",
    "source_system": "fast identifier-URI, angiver hvilken kilde raekken kom fra",
    "source_table": "fast markering af kilden",
    "category_code": "fast kategori",
    "supplementary_code_system": "fast kodesystem-URI",
    "intent": "fast FHIR-vaerdi",
    # Lagerets egne felter.
    "last_updated": "tidsstempel fra FHIR-serveren",
    "created_at": "tidsstempel fra FHIR-serveren",
    "dw_is_deleted": "lagerstyret sletteflag",
}


def _kolonner(navn: str) -> dict[str, dict]:
    tekst = (LINEAGE / f"{navn}.yml").read_text(encoding="utf-8")
    blok = yaml.safe_load(tekst)["dso_lineage"]
    return blok.get("Columns") or {}


@pytest.mark.parametrize("tabel", TABELLER)
def test_hver_leveret_kolonne_angiver_sin_kilde(tabel):
    mangler = []
    for kolonne, felter in _kolonner(tabel).items():
        felter = felter or {}
        if felter.get("Source") or felter.get("Expr"):
            continue
        if kolonne in UDEN_KILDE:
            continue
        mangler.append(kolonne)
    assert not mangler, (
        f"{tabel}: kolonnerne {sorted(mangler)} leveres uden Source eller Expr. "
        "Skriv kildekolonnen paa, eller optag kolonnen i UDEN_KILDE med en begrundelse."
    )


@pytest.mark.parametrize("tabel", TABELLER)
def test_kildeangivelsen_peger_paa_en_erklaeret_kilde(tabel):
    tekst = (LINEAGE / f"{tabel}.yml").read_text(encoding="utf-8")
    blok = yaml.safe_load(tekst)["dso_lineage"]
    erklaerede = set()
    for post in blok.get("SourceSystems") or []:
        erklaerede.update(post.keys())
    ukendte = set()
    for felter in (blok.get("Columns") or {}).values():
        for kilde in (felter or {}).get("Source") or []:
            praefiks = str(kilde).split(".", 1)[0]
            if praefiks not in erklaerede:
                ukendte.add(str(kilde))
    assert (
        not ukendte
    ), f"{tabel}: {sorted(ukendte)} peger paa et kildesystem, der ikke staar i SourceSystems."


def test_kildens_statuskode_leveres_hele_vejen():
    # Statusteksten blev projiceret, koden blev bygget ind i ressourcen og tabt
    # paa vejen ned i tabellen. FHIR-kolonnen status daekker den ikke: den er en
    # oversaettelse af sletteflaget.
    view = (ETL / "DSA/FHIR/SqlOnFhir/view_definitions/procedure_view.json").read_text()
    ddl = (ETL / "infrastructure/ddl/tables/fhir/01_tables.sql").read_text()
    kolonner = _kolonner("fhir_procedure")

    assert '"name": "status_code"' in view, "ViewDefinition projicerer ikke status_code"
    assert re.search(r"\[status_code\] NVARCHAR", ddl), "fhir.Procedure mangler status_code"
    assert "STATUS_CODE" in str(kolonner["status_code"]["Source"])
    assert "SLETTET" in str(kolonner["status"]["Source"])
    assert "STATUS_CODE" not in str(kolonner["status"].get("Source", ""))


# Børnetabeller og deres forældre. Første led i den sammensatte primærnøgle er
# samtidig fremmednøglen opad; uden annoteringen kan en aftager ikke se, at
# rækkerne hører til en forælder.
BOERN_OG_FORAELDRE = {
    "fhir_condition_henvisning": ("condition_id", "fhir.Condition"),
    "fhir_condition_supplementary_code": ("condition_id", "fhir.Condition"),
    "fhir_procedure_supplementary_code": ("procedure_id", "fhir.Procedure"),
    "fhir_service_request_supplementary_code": ("service_request_id", "fhir.ServiceRequest"),
    "fhir_servicerequest_recipient": ("service_request_id", "fhir.ServiceRequest"),
    "fhir_documentreference_recipient": ("documentreference_id", "fhir.DocumentReference"),
    "fhir_observation_speciale": ("observation_id", "fhir.Observation"),
}


@pytest.mark.parametrize(("tabel", "noegle_og_foraelder"), sorted(BOERN_OG_FORAELDRE.items()))
def test_boernetabellens_noegle_peger_paa_sin_foraelder(tabel, noegle_og_foraelder):
    kolonne, foraelder = noegle_og_foraelder
    felter = _kolonner(tabel)[kolonne]
    assert felter.get("Type") == "PK", f"{tabel}.{kolonne} er ikke en del af primærnøglen"
    assert felter.get("FK-Target") == foraelder, (
        f"{tabel}.{kolonne} mangler FK-Target til {foraelder} — en aftager kan ikke se, "
        "at rækkerne hører til en forælder"
    )
