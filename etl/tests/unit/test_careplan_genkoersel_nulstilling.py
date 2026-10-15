# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Spærrer på den destruktive CarePlan-nulstilling.

Assetet tømmer alle CarePlan-ressourcer i Aidbox plus de flade kopier i staging
og DSO. Det kan ikke fortrydes, så bekræftelsesstrengen, dry_run-defaulten,
server-pinden og produktionsspærren er kontrakten der er værd at pinne — plus
at DSO-syncens checkpoints rammes dér hvor de faktisk bor efter
DSO-unificeringen.
"""

import json
from typing import NamedTuple
from unittest.mock import MagicMock

import dagster as dg
import pytest
from infrastructure.assets.careplan_genkoersel_nulstilling import (
    ALLE_MARKOERER_FRAGMENT,
    BEKRAEFTELSE,
    CHECKPOINTS,
    FLADE_TABELLER,
    CarePlanGenkoerselConfig,
    careplan_genkoersel_nulstilling,
)

pytestmark = pytest.mark.unit

_STAGING = ("SQLDEV01", "healthpipe_staging")
_DSO = ("SQLDEV01", "healthpipe_dso")


class _MssqlCursor:
    """Svarer efter SQL'ens indhold, så én cursor dækker alle opslagene."""

    def __init__(self, identitet, lag="mssql", haendelser=None):
        self.identitet = identitet
        self.sql: list[str] = []
        self._lag = lag
        self._haendelser = haendelser if haendelser is not None else []
        self._last = ""
        self.rowcount = 3

    def execute(self, sql, *args):
        self.sql.append(str(sql))
        self._haendelser.append((self._lag, str(sql)))
        self._last = str(sql)

    def fetchone(self):
        if "@@SERVERNAME" in self._last:
            return self.identitet
        return (11,)  # COUNT(*)

    def fetchall(self):
        if "sys.tables" in self._last:
            return [(navn,) for navn in FLADE_TABELLER]
        return []

    def close(self):
        pass


def _mssql(identitet, lag="mssql", haendelser=None):
    cursor = _MssqlCursor(identitet, lag, haendelser)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    resource = MagicMock()
    resource.get_connection.return_value = cm
    return resource, cursor


class _PgCursor:
    """Svarer efter SQL'ens indhold: identitet, tællinger og indeksopslag."""

    def __init__(self, index_rows, haendelser=None):
        self._index_rows = index_rows
        self._haendelser = haendelser if haendelser is not None else []
        self._last = ""
        self.sql: list[str] = []
        self.parametre: list[tuple] = []

    def execute(self, sql, params=None):
        self._last = str(sql)
        self.sql.append(str(sql))
        self._haendelser.append(("aidbox", str(sql)))
        self.parametre.append(params or ())

    def fetchone(self):
        if "current_database()" in self._last:
            return ("aidbox", "10.0.0.5", 5432)
        return (1_270_000,)  # COUNT(*)

    def fetchall(self):
        return self._index_rows


def _fhir_database(index_rows=None, haendelser=None):
    cursor = _PgCursor(
        (
            index_rows
            if index_rows is not None
            else [("idx_careplan_urn_dk_sundhed_alle_markoerer_careplan_uniq", True, True)]
        ),
        haendelser,
    )
    cursor_cm = MagicMock()
    cursor_cm.__enter__ = MagicMock(return_value=cursor)
    cursor_cm.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor_cm
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=conn)
    cm.__exit__ = MagicMock(return_value=False)
    resource = MagicMock()
    resource.get_connection.return_value = cm
    return resource, cursor


class _Koersel(NamedTuple):
    """Resultatet plus de tre cursorer og en fælles, ordnet hændelseslog."""

    result: object
    staging: "_MssqlCursor"
    dso: "_MssqlCursor"
    aidbox: "_PgCursor"
    haendelser: list[tuple[str, str]]


def _instans_med_aktive_koersler(jobnavne):
    """Ephemeral instans hvis kørselsliste er skiftet ud med de navngivne jobs."""
    instans = dg.DagsterInstance.ephemeral()
    records = []
    for navn in jobnavne:
        record = MagicMock()
        record.dagster_run.job_name = navn
        record.dagster_run.run_id = f"run-{navn}"
        records.append(record)
    instans.get_run_records = lambda *args, **kwargs: records  # type: ignore[method-assign]
    return instans


def _run(config, staging_id=_STAGING, dso_id=_DSO, index_rows=None, aktive_jobs=()):
    haendelser: list[tuple[str, str]] = []
    staging, staging_cur = _mssql(staging_id, "staging", haendelser)
    dso, dso_cur = _mssql(dso_id, "dso", haendelser)
    fhir_db, fhir_cur = _fhir_database(index_rows, haendelser)
    context = dg.build_asset_context(
        resources={
            "mssql_target_staging": staging,
            "mssql_target_dso": dso,
            "fhir_database": fhir_db,
        },
        instance=_instans_med_aktive_koersler(aktive_jobs),
    )
    result = careplan_genkoersel_nulstilling(context, config)
    return _Koersel(result, staging_cur, dso_cur, fhir_cur, haendelser)


def _ok(**overrides):
    base = {
        "bekraeft": BEKRAEFTELSE,
        "dry_run": False,
        "bekraeft_staging_server": _STAGING[0],
        "bekraeft_dso_server": _DSO[0],
    }
    base.update(overrides)
    return CarePlanGenkoerselConfig(**base)


def test_dry_run_er_default():
    """En materialisering uden config må ikke ødelægge noget."""
    assert CarePlanGenkoerselConfig().dry_run is True


def test_dry_run_toemmer_intet():
    koersel = _run(CarePlanGenkoerselConfig(dry_run=True))

    assert koersel.result.metadata["dry_run"] is True
    assert not any("TRUNCATE" in sql.upper() for _, sql in koersel.haendelser)


def test_rigtig_koersel_kraever_praecis_bekraeftelse():
    """Asset-navnet alene må ikke kunne slette 1,27 mio. ressourcer."""
    for forkert in ("", "ja", BEKRAEFTELSE.lower(), BEKRAEFTELSE + " "):
        with pytest.raises(dg.Failure):
            _run(_ok(bekraeft=forkert))


def test_rigtig_koersel_kraever_server_pind():
    """Databasenavnene er ens på tværs af miljøer; kun servernavnet skiller dem."""
    with pytest.raises(dg.Failure):
        _run(_ok(bekraeft_staging_server=""))
    with pytest.raises(dg.Failure):
        _run(_ok(bekraeft_dso_server="EN-HELT-ANDEN-SERVER"))


def test_produktionslignende_navn_afvises_selv_i_dry_run():
    with pytest.raises(dg.Failure):
        _run(CarePlanGenkoerselConfig(dry_run=True), staging_id=("SQLPROD01", "healthpipe_staging"))


def test_bekraeftet_koersel_toemmer_barn_foer_foraelder():
    koersel = _run(_ok())

    for cursor, lag in ((koersel.staging, "staging"), (koersel.dso, "dso")):
        truncates = [s for s in cursor.sql if "TRUNCATE" in s.upper()]
        assert truncates == [
            "TRUNCATE TABLE [fhir].[CarePlan_Activity]",
            "TRUNCATE TABLE [fhir].[CarePlan]",
        ], lag
    assert koersel.result.metadata["dry_run"] is False


def test_checkpoints_nulstilles_via_proceduren_i_staging():
    """meta.usp_dfm_reset frem for en DELETE: atomisk, samme target_type-
    afbildning som DfmManager, og en reset-række i meta.DFMRunLog."""
    koersel = _run(_ok())

    resets = [s for s in koersel.staging.sql if "usp_dfm_reset" in s]
    assert len(resets) == len(CHECKPOINTS)
    assert not any("DELETE" in s.upper() and "DFMState" in s for s in koersel.staging.sql)
    # DSO-siden rører kun rester i det gamle vandmærke-lager.
    assert not any("DFMState" in s for s in koersel.dso.sql)
    assert any("SyncWatermarks" in s for s in koersel.dso.sql)


def test_checkpointlisten_daekker_alle_fem_ejere_inkl_dso_scope():
    """DSO-syncen kører på DFM nu: dens vandmærker bor i stagings meta.DFMState
    med scope 'dso', ikke længere i DSO's meta.SyncWatermarks."""
    assert set(CHECKPOINTS) == {
        ("staging", "fhir_server", "CarePlan"),
        ("staging", "fhir", "CarePlan"),
        ("staging", "fhir", "CarePlan_Activity"),
        ("dso", "fhir", "CarePlan"),
        ("dso", "fhir", "CarePlan_Activity"),
    }


def test_jsonb_parametre_er_serialiseret_json():
    """psycopg adapterer ikke en Python-dict til en %s-plads.

    Mocken herunder sluger hvad som helst, så en dict ville slippe igennem
    testene og først fejle mod den rigtige database. Kontrakten pinnes derfor
    på parametrenes TYPE: alt der sendes til FHIR-cursoren skal være skalarer
    eller strenge — jsonb går ind som json.dumps-tekst.
    """
    assert isinstance(ALLE_MARKOERER_FRAGMENT, str)
    assert json.loads(ALLE_MARKOERER_FRAGMENT) == [
        {"system": "urn:dk:sundhed:alle-markoerer:careplan"}
    ]

    koersel = _run(_ok())

    for parametre in koersel.aidbox.parametre:
        for parameter in parametre:
            assert not isinstance(parameter, (dict, list, tuple)), (
                f"{parameter!r} sendes uadapteret til psycopg — serialisér "
                "jsonb-parametre med json.dumps"
            )


@pytest.mark.parametrize(
    "index_rows",
    [
        [],
        [("idx_careplan_urn_dk_sundhed_alle_markoerer_careplan_uniq", False, True)],
        [("idx_careplan_urn_dk_sundhed_alle_markoerer_careplan_uniq", True, False)],
    ],
    ids=["mangler", "ikke_unik", "ugyldig"],
)
def test_fejler_naar_unikheden_ikke_haandhaeves(index_rows):
    """Indeksoprettelsen er ikke-fatal i fhir_ifdb_database_indexes, så et grønt
    infrastruktur-asset er ikke bevis. Et genindlæs uden indekset ville genåbne
    dublet-tilstanden, ændringen fjerner."""
    with pytest.raises(dg.Failure):
        _run(_ok(), index_rows=index_rows)


def test_checkpoints_nulstilles_foer_foerste_toemning():
    """Rækkefølgen er selve spærren mod en halv kørsel.

    Døde kørslen efter tømningen, men før checkpointerne, ville tabellerne stå
    tomme med intakte checkpoints: spærren i fhir_server_careplan_dfm tæller nul
    gamle rækker, og natten kører videre som en almindelig delta, der kun
    genopbygger de patienter, der tilfældigvis har ændringer. Nulstillet først
    læser næste kørsel hele arkivet og møder spærren, så længe der er gamle
    rækker tilbage.
    """
    haendelser = _run(_ok()).haendelser
    ordner = [
        i
        for i, (_, sql) in enumerate(haendelser)
        if "usp_dfm_reset" in sql or "SyncWatermarks" in sql
    ]
    toemninger = [i for i, (_, sql) in enumerate(haendelser) if "TRUNCATE" in sql.upper()]

    assert ordner and toemninger
    assert max(ordner) < min(toemninger)


def test_unikhedsindekset_verificeres_foer_foerste_toemning():
    """TRUNCATE rører ikke indekser, så tjekket siger det samme før som efter —
    og lagt først kan det nå at forhindre tømningen."""
    haendelser = _run(_ok()).haendelser
    indeks = [i for i, (_, sql) in enumerate(haendelser) if "pg_index" in sql]
    toemninger = [i for i, (_, sql) in enumerate(haendelser) if "TRUNCATE" in sql.upper()]

    assert indeks and toemninger
    assert max(indeks) < min(toemninger)


def test_dry_run_rapporterer_manglende_unikhedsindeks():
    """Dry-run er rapporten, operatøren planlægger efter: den skal vise et
    manglende indeks i stedet for først at afsløre det midt i den rigtige
    kørsel."""
    koersel = _run(CarePlanGenkoerselConfig(dry_run=True), index_rows=[])

    assert koersel.result.metadata["unikhedsindeks"] == "MANGLER"


def test_aktiv_relevant_koersel_blokerer_den_rigtige_koersel():
    """En samtidig kørsel skriver til de lag, der tømmes — eller læser dem
    halvtomme. Dry-run nøjes med at logge, som i sfi_genkoersel_nulstilling."""
    with pytest.raises(dg.Failure):
        _run(_ok(), aktive_jobs=["ifdb_dfm_daily_job"])

    koersel = _run(CarePlanGenkoerselConfig(dry_run=True), aktive_jobs=["ifdb_dfm_daily_job"])
    assert koersel.result.metadata["aktive_relevante_koersler"].data == ["ifdb_dfm_daily_job"]


def test_urelateret_koersel_blokerer_ikke():
    """Spærren må ikke gøre nulstillingen ukørbar, fordi ERP-jobbet kører."""
    koersel = _run(_ok(), aktive_jobs=["erp_daily_dfm_job"])
    assert koersel.result.metadata["dry_run"] is False
