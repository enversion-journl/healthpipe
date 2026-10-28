# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Faste testbrugere i sec.RolleDomaene og sec.RBOrganisation til test af sikkerhedsviews.

Loaderen lægger rækkerne ind sammen med AD-udtrækket, så de overlever den natlige
genindlæsning. Indlæses kun når ``SECURITY_SEED_ENABLED`` er slået til, og aldrig i et
miljø der ligner prod.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from dataclasses import dataclass

from core.environment import is_prod_environment, prod_environment_reason

SEED_ENABLED_ENV = "SECURITY_SEED_ENABLED"

_TRUE_VALUES = frozenset({"1", "true", "yes", "ja"})
_FALSE_VALUES = frozenset({"", "0", "false", "no", "nej"})

# Domæner fra AD-udtrækket lægges oveni, så et nyt domæne også får testbrugere.
SIKKERHEDSDOMAENER = (
    "DPA_Aktivitet",
    "DPA_Akutdata",
    "DPA_Biokemi",
    "DPA_Booking",
    "DPA_DRG",
    "DPA_ERP",
    "DPA_Forskningsstoette",
    "DPA_Kvalitetsdatabaser",
    "DPA_Medicin",
    "DPA_Microbiologi",
    "DPA_Patientrettigheder",
    "DPA_Patologi",
    "DPA_Roentgen",
    "DPA_Socialdata",
)


@dataclass(frozen=True)
class Testbruger:
    """En testbruger med samme rolle i alle sikkerhedsdomæner."""

    bruger: str
    rolle: str
    populationsansvar: str = ""
    sor_koder: tuple[str, ...] = ()


TESTBRUGERE = (
    Testbruger("testbruger_ingen_adgang", "K"),
    Testbruger("testbruger_org1", "K", sor_koder=("247991000016009",)),
    Testbruger("testbruger_population", "P", populationsansvar="SLB"),
    Testbruger("testbruger_U", "U"),
)


class SeedInProdError(ValueError):
    """Seed-data er slået til i et miljø, der ligner prod."""


class SeedCollisionError(RuntimeError):
    """En testbruger findes også i AD-udtrækket."""


def seed_enabled() -> bool:
    """Om seed-data skal indlæses; fejler ved ukendt værdi og i et prod-lignende miljø."""
    raw = (os.environ.get(SEED_ENABLED_ENV) or "").strip().lower()
    if raw in _FALSE_VALUES:
        return False
    if raw not in _TRUE_VALUES:
        raise ValueError(f"{SEED_ENABLED_ENV} er ugyldig: {raw!r} (brug true eller false)")
    if is_prod_environment():
        raise SeedInProdError(
            f"{SEED_ENABLED_ENV} er slået til, men miljøet ligner prod "
            f"({prod_environment_reason()}) - testbrugere indlæses ikke i prod."
        )
    return True


def assert_no_collision(ad_brugere: Iterable[str]) -> None:
    """Rejser SeedCollisionError, hvis en testbruger også findes i AD-udtrækket."""
    testbrugere = {t.bruger.lower() for t in TESTBRUGERE}
    kollisioner = sorted({b.lower() for b in ad_brugere} & testbrugere)
    if kollisioner:
        raise SeedCollisionError(
            f"Testbrugere findes også i AD-udtrækket: {', '.join(kollisioner)}. "
            "Fjern dem fra AD-grupperne."
        )


def rolle_domaene_rows(ad_domaener: Iterable[str]) -> list[tuple[str, str, str, str]]:
    """(bruger, Sikkerhedsdomaene, Rolle, Populationsansvar) for alle testbrugere og domæner."""
    domaener: dict[str, str] = {d.lower(): d for d in SIKKERHEDSDOMAENER}
    for d in ad_domaener:
        domaener.setdefault(d.lower(), d)
    return [
        (t.bruger, domaene, t.rolle, t.populationsansvar)
        for t in TESTBRUGERE
        for domaene in sorted(domaener.values(), key=str.lower)
    ]


def rb_organisation_rows() -> list[dict]:
    """Testbrugernes SOR-tildelinger i samme form som org-rækkerne fra AD-udtrækket."""
    return [{"bruger": t.bruger, "code_sor": kode} for t in TESTBRUGERE for kode in t.sor_koder]
