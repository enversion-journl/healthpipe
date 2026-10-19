# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Et vw_fhir-view dækker hele sin tabel, medmindre det er et erklæret udvalg.

En fhir-tabel får kun et vw_fhir-view, når der er en grund til det (se
docs/basismodel/04_DSO_sync_til_DSO.md, afsnit 5), så laget er ikke komplet.

Laget må rumme *udvalgsviews*, der gør en kendt skelnen let at bruge, fx kun
kontaktdiagnoser ud af en Condition-tabel med tre kilder. De står i ``_UDVALGSVIEWS``
med den tabel, de udvælger fra, så et view aldrig kan filtrere i det skjulte: er navnet
ikke på listen, skal viewet dække hele sin tabel.
Testen sammenholder ``CREATE TABLE [fhir].[X]`` i tabel-DDL'en med
``CREATE OR ALTER VIEW [vw_fhir].[X]`` i view-DDL'en. Interne arbejdstabeller står i
``_INTERNE_TABELLER`` og må aldrig eksponeres — udvid listen bevidst, ikke i forbifarten.
"""

from __future__ import annotations

import re
from pathlib import Path

ETL = Path(__file__).resolve().parents[2]
TABLE_DDL = [
    ETL / "infrastructure" / "ddl" / "tables" / "fhir" / "01_tables.sql",
    ETL / "infrastructure" / "ddl" / "tables" / "fhir" / "02_sfi_datapoint_store.sql",
]
VIEW_DDL = ETL / "infrastructure" / "ddl" / "views" / "fhir" / "01_vw_fhir.sql"

# Materialiseringer og arbejdstabeller bag fhir.SFI_datapoint — aftagerindgangen er viewet.
_INTERNE_TABELLER = {
    "SFI_datapoint_store",
    "SFI_datapoint_store_backfill",
    "SFI_datapoint_store_efterslaeb",
}

# Udvalgsview -> tabellen det udvælger fra. Et udvalg skal være bevidst og begrundet.
_UDVALGSVIEWS = {
    "Condition_pas_diagnose": "Condition",
}

_TABLE_RE = re.compile(r"CREATE TABLE \[fhir\]\.\[([A-Za-z_]+)\]")
_FHIR_VIEW_RE = re.compile(r"CREATE OR ALTER VIEW \[fhir\]\.\[([A-Za-z_]+)\]")
_VIEW_RE = re.compile(r"CREATE OR ALTER VIEW \[vw_fhir\]\.\[([A-Za-z_]+)\]")


def _fhir_tabeller() -> set[str]:
    return {
        name for path in TABLE_DDL for name in _TABLE_RE.findall(path.read_text(encoding="utf-8"))
    }


def _fhir_objekter() -> set[str]:
    """Tabeller plus de views, fhir-skemaet selv definerer (fx SFI_datapoint)."""
    return _fhir_tabeller() | set(_FHIR_VIEW_RE.findall(VIEW_DDL.read_text(encoding="utf-8")))


def _vw_fhir_views() -> set[str]:
    return set(_VIEW_RE.findall(VIEW_DDL.read_text(encoding="utf-8")))


def test_view_uden_egen_tabel_er_et_erklaeret_udvalg() -> None:
    uerklaerede = sorted(_vw_fhir_views() - _fhir_objekter() - _UDVALGSVIEWS.keys())
    assert not uerklaerede, (
        "vw_fhir-views uden tilsvarende tabel og uden erklæring i _UDVALGSVIEWS: " f"{uerklaerede}"
    )


def test_udvalgsview_filtrerer_paa_sin_egen_tabel() -> None:
    sql = VIEW_DDL.read_text(encoding="utf-8")
    for view, tabel in _UDVALGSVIEWS.items():
        assert tabel in _fhir_objekter(), f"{view} udvælger fra et objekt der ikke findes"
        start = sql.index(f"CREATE OR ALTER VIEW [vw_fhir].[{view}]")
        blok = sql[start : sql.index("\nGO", start)]
        assert f"FROM [fhir].[{tabel}]" in blok
        assert "WHERE" in blok, f"{view} er erklæret som udvalg, men filtrerer ikke"


def test_interne_tabeller_har_ikke_vw_fhir_view() -> None:
    eksponeret = sorted(_INTERNE_TABELLER & _vw_fhir_views())
    assert not eksponeret, f"interne tabeller eksponeret i vw_fhir: {eksponeret}"


def test_allowlisten_peger_paa_eksisterende_tabeller() -> None:
    forsvundet = sorted(_INTERNE_TABELLER - _fhir_tabeller())
    assert not forsvundet, f"allowlist nævner tabeller der ikke findes i DDL: {forsvundet}"
