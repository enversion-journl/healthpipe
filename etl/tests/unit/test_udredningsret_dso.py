# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for udredningsrets DSO-lag (US-6): sync-configs, assets,
praesentationsviews, publish-sensoren og DQA-subjects.

De fejl, testene her vogter, er alle tavse:

* en tabel med raa CPR (``arc.udredningsret_mont``) eller en IDENTITY-mintende
  map-tabel, der glider med i synkroniseringen til DSO;
* en sync, der koeres foer de tabeller, dens FK'er peger paa, findes paa DSO —
  RI-fejl i koerselstid, ikke i test;
* et factview, der holder op med at filtrere ``dw_is_deleted`` (og dermed
  over-taeller mere for hver leverance), eller et view, der faar logik ud over
  det ene praedikat — et JOIN i praesentationslaget er en forretningsregel,
  ingen aftager kan se;
* en sensor, der enten fyrer hver nat (og re-stager 8,2 M arkivraekker for at
  opdage ingenting) eller aldrig kommer ud af et plateau igen;
* et DQA-check eller en DSO-kolonne, der eksponerer CPR-digesten — hvad enten den
  hedder ``clean_code_Helbredsforloeb`` eller baeres som delstreng i
  ``clean_code_Udredningsforloeb``.

Ingen database: DDL asserteres som tekst, sensoren mod en fake-cursor, og
asset-grafen mod Dagsters egen oploesning.

Spec: docs/source_systems/udredningsret/udredningsret_model.md (afsnit 1, 8 og
9; beslutning U5, U9, U10, U13, U15, U16, U18).
"""

import re
from pathlib import Path

import dagster as dg
import pytest
from dqa.config import ALL_SUBJECTS
from dqa.config.udredningsret_subjects import UDREDNINGSRET_SUBJECTS
from dqa.model import Cadence, Severity, Track
from dqa.sensors import DQA_AFTER_ETL_SENSOR_SPECS
from DSO.load.sync_configs import ALL_SYNC_CONFIGS, UDREDNINGSRET_SYNC_CONFIGS
from DSO.load.sync_udredningsret import all_udredningsret_dso_sync_assets
from sensors.udredningsret_publish_sensor import (
    ARC_SOURCE_TABLES,
    DELIVERY_SIGNATURE_TAG,
    FORLOEB_FACT_TABLE,
    UNRESOLVED_TAG,
    evaluate,
    read_delivery_signature,
    read_unresolved_forloeb,
)

pytestmark = pytest.mark.unit

_DDL = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
_TABLES_DDL = [
    _DDL / "tables" / "udredningsret" / "03_dims.sql",
    _DDL / "tables" / "udredningsret" / "04_facts.sql",
]

PUBLISH_JOB = "udredningsret_publish_job"
SYNC_GRUPPE = "udredningsret_dso_sync"

# De fem CPR-frie tabeller, der udgoer den forespoergbare udredningsretsmodel paa
# DSO. Navngivet eksplicit (ikke udledt af configen), saa en tabel der falder ud
# — eller en map/arc-tabel der glider ind — fejler her.
FORVENTEDE_TABELLER = {
    ("dim", "UdredningsretEgenskaber"),
    ("dim", "UdredningsretKlassifikation"),
    ("dim", "Forloebsmarkoer"),
    ("fct", "Udredningsforloeb"),
    ("fct", "UdredningsretMarkoer"),
}

# Facterne, der faar indekserne slaaet fra under bulk-loadet, og antallet af
# nonclustered indekser DDL'en giver dem (inkl. det unikke clean_code-indeks).
BULK_FACTER = {"fct_Udredningsforloeb": 8, "fct_UdredningsretMarkoer": 7}


# ===========================================================================
# 1. Sync-configs
# ===========================================================================


def test_de_fem_cpr_frie_tabeller_synkroniseres_og_intet_andet():
    """map minter IDENTITY-noegler og er staging-internt; arc.udredningsret_mont
    holder den raa CPR; ext/meta er pipeline-interne. Kun dim/fct maa forlade
    staging, og de synkroniserer den tabel, de selv hedder — ikke en anden."""
    assert {(c.schema, c.table) for c in UDREDNINGSRET_SYNC_CONFIGS.values()} == FORVENTEDE_TABELLER

    for noegle, c in UDREDNINGSRET_SYNC_CONFIGS.items():
        assert c.schema in {"dim", "fct"}, f"{c.table}: skemaet {c.schema} maa ikke synkronisere"
        assert (
            c.source_schema is None and c.source_table is None
        ), f"{c.table}: skal synkronisere den DSA-tabel, den hedder, ikke laese en anden"
        # Config-noeglen ER DSA-asset-navnet (deps i sync-modulet bruger den).
        assert noegle == f"{c.schema}_{c.table}", noegle


def test_primaernoeglerne_er_tabellernes_egne_klyngede_noegler():
    """Egenskabsdimensionen deler PK med forloebsfacten (som DRG's egenskabsdim
    goer med fct.DRGKontakt) — en forkert PK her ville faa MERGE'n til at
    opdatere den forkerte raekke paa DSO."""
    assert {n: c.primary_key for n, c in UDREDNINGSRET_SYNC_CONFIGS.items()} == {
        "dim_UdredningsretEgenskaber": "dw_ek_Udredningsforloeb",
        "dim_UdredningsretKlassifikation": "dw_ek_UdredningsretKlassifikation",
        "dim_Forloebsmarkoer": "dw_ek_Forloebsmarkoer",
        "fct_Udredningsforloeb": "dw_ek_Udredningsforloeb",
        "fct_UdredningsretMarkoer": "dw_ek_UdredningsretMarkoer",
    }


def test_alle_fem_er_watermark_paa_opdateringstidsstemplet():
    """DSA-laget vedligeholder alle fem inkrementelt (fact-MERGE + soft-delete,
    Type 1-upsert paa dimensionerne), saa dw_timestamp_update rykker praecis paa
    de raekker, der faktisk aendrede sig. truncate_insert ville flytte 8,2 M
    raekker hver nat for at opdage det samme."""
    for c in UDREDNINGSRET_SYNC_CONFIGS.values():
        assert c.mode == "watermark", f"{c.table}: mode {c.mode}"
        assert c.watermark_column == "dw_timestamp_update", c.table


def test_kun_facterne_slaar_indekser_fra_og_kun_paa_bulk_stien():
    """Flaget er dyrt at tage fejl af begge veje: uden det bygger foerste load
    ~57 M tilfaeldige indeksskrivninger paa markoerfacten; med det paa den
    inkrementelle sti ville hver lille delta koste en fuld indeksgenopbygning.

    Grunden til at det ER sikkert er, at begge facter har en enkelt streng-PK og
    derfor rammer den batched sti, hvor flaget kun fyrer paa bulk-loadet
    (``_sync_via_linked_server_batched``: ``if ... and is_bulk_load``). En
    sammensat PK ville flytte dem til den composite sti, der slet ikke kender
    flaget."""
    for noegle, c in UDREDNINGSRET_SYNC_CONFIGS.items():
        forventet_bulk = noegle in BULK_FACTER
        assert c.disable_target_indexes_during_sync is forventet_bulk, noegle
        if forventet_bulk:
            assert isinstance(c.primary_key, str), f"{noegle}: sammensat PK goer flaget inert"
            assert c.chunk_size == 20_000, noegle


def test_flaget_er_begrundet_i_ddl_ens_faktiske_indeksantal():
    """Kommentaren i configen naevner otte og syv nonclustered indekser. Bliver
    et indeks fjernet fra DDL'en, er begrundelsen — og loggens forventede
    'Disabled N indexes' — ikke laengere sand."""
    facts_sql = (_DDL / "tables" / "udredningsret" / "04_facts.sql").read_text(encoding="utf-8")
    for noegle, antal in BULK_FACTER.items():
        tabel = UDREDNINGSRET_SYNC_CONFIGS[noegle].table
        fundet = re.findall(
            rf"CREATE (?:UNIQUE )?NONCLUSTERED INDEX \[[^\]]+\] ON \[fct\]\.\[{tabel}\]",
            facts_sql,
        )
        assert len(fundet) == antal, f"{tabel}: {len(fundet)} nonclustered indekser, ikke {antal}"


def _tabel_blok(schema: str, table: str) -> str:
    for f in _TABLES_DDL:
        sql = f.read_text(encoding="utf-8")
        m = re.search(
            rf"CREATE TABLE \[{schema}\]\.\[{re.escape(table)}\]\s*\((.*?)CONSTRAINT \[PK_",
            sql,
            re.DOTALL,
        )
        if m:
            return m.group(1)
    raise AssertionError(f"ingen CREATE TABLE-blok for {schema}.{table}")


def test_ingen_synkroniseret_tabel_erklaerer_en_cpr_kolonne():
    """Hele GDPR-pointen med laget: dim/fct baerer dw_ek_Borger, aldrig raa CPR,
    og dimensionerne har slet ingen borgernoegle. Her tjekkes kun for raa CPR;
    den hashede form er testen nedenfor. Asserteret mod den DDL, hver
    synkroniseret tabel faktisk oprettes af."""
    for schema, table in FORVENTEDE_TABELLER:
        kolonner = _tabel_blok(schema, table)
        assert "cpr" not in kolonner.lower(), f"{schema}.{table} erklaerer en CPR-lignende kolonne"

    # Kontrolproeve: arkivet, der IKKE synkroniserer, har kolonnen.
    arkiv = (_DDL / "tables" / "udredningsret" / "00_archive.sql").read_text(encoding="utf-8")
    assert "[CPR]" in arkiv, "arkivets CPR-kolonne er vaek — er testen ovenfor stadig meningsfuld?"


def test_forloebets_clean_code_forlader_ikke_dsa():
    """clean_code_Udredningsforloeb er 'UDREDFORL_' + helbredsforloeb_noegle +
    startdato + loebenummer, og helbredsforloeb_noegle ER arkivets SHA2_256 over
    CPR. De 64 hex-tegn staar altsaa ordret i kolonnen: et genberegneligt, stabilt
    pseudonym over CPR — praecis den kategori, §9 holder ude af dqa.Results.
    U18 holder den paa DSA med exclude_columns; falder flaget ud, rejser digesten
    til rapporteringsdatabasen uden at noget andet fejler."""
    for noegle in ("dim_UdredningsretEgenskaber", "fct_Udredningsforloeb"):
        c = UDREDNINGSRET_SYNC_CONFIGS[noegle]
        assert "clean_code_Udredningsforloeb" in c.exclude_columns, noegle
        # Kolonnen SKAL stadig staa i DDL'en: skemaerne valideres som identiske,
        # og paa DSA er den forretningsnoeglen.
        assert "[clean_code_Udredningsforloeb]" in _tabel_blok(c.schema, c.table), noegle

    # Markoerens clean_code er IKKE beroert: markoer_noegle er en ny hash AF
    # helbredsforloebsnoeglen, ikke noeglen selv — den maa gerne synkronisere.
    assert UDREDNINGSRET_SYNC_CONFIGS["fct_UdredningsretMarkoer"].exclude_columns == []
    for noegle in ("dim_UdredningsretKlassifikation", "dim_Forloebsmarkoer"):
        assert UDREDNINGSRET_SYNC_CONFIGS[noegle].exclude_columns == [], noegle


def test_configene_indgaar_i_all_sync_configs_saa_dso_afstemningen_udledes():
    """dqa/dso.py udleder ét dso_reconciliation-subject pr. post i
    ALL_SYNC_CONFIGS. Uden dem ville DSO-laget staa uafstemt uden at noget
    fejlede."""
    for noegle, config in UDREDNINGSRET_SYNC_CONFIGS.items():
        assert ALL_SYNC_CONFIGS[noegle] is config, noegle


def test_de_fem_dso_afstemninger_udledes_med_domaenet_udredningsret():
    from dqa.dso import _dso_subjects

    subjects = {s.name: (s, d) for s, d in _dso_subjects()}
    for noegle, c in UDREDNINGSRET_SYNC_CONFIGS.items():
        navn = f"dso_{noegle}"
        assert navn in subjects, f"{navn} udledes ikke"
        subject, domaene = subjects[navn]
        assert domaene == "udredningsret"
        assert subject.track is Track.DWH
        (spec,) = subject.checks
        assert spec.check_type == "dso_reconciliation"
        assert spec.params["pk"] == [c.primary_key]


# ===========================================================================
# 2. Sync-assets: registrering, gruppe og RI-raekkefoelge
# ===========================================================================


def _deps(asset) -> set[str]:
    noegle = next(iter(asset.keys))
    return {k.to_user_string() for k in asset.asset_deps[noegle]}


def _efter_navn() -> dict[str, set[str]]:
    return {a.key.to_user_string(): _deps(a) for a in all_udredningsret_dso_sync_assets}


def test_de_fem_assets_hedder_og_grupperes_som_forventet():
    """Asset-navnet er dso_sync_<config-noegle i smaa bogstaver>, saa en omdoebt
    config-noegle flytter asset'et — og dermed ud af publish-jobbets selektion,
    der vaelger paa gruppenavn."""
    assert set(_efter_navn()) == {
        "dso_sync_dim_udredningsretegenskaber",
        "dso_sync_dim_udredningsretklassifikation",
        "dso_sync_dim_forloebsmarkoer",
        "dso_sync_fct_udredningsforloeb",
        "dso_sync_fct_udredningsretmarkoer",
    }
    for asset in all_udredningsret_dso_sync_assets:
        noegle = asset.key
        assert asset.group_names_by_key[noegle] == SYNC_GRUPPE, noegle.to_user_string()
        tags = asset.tags_by_key[noegle]
        assert tags["domain"] == "udredningsret" and tags["source"] == "udredningsret"
        assert tags["layer"] == "dso_sync"
        assert asset.op.tags.get("dagster/compute_kind") == "mssql"


def test_hver_sync_venter_paa_dso_ddl_en_og_paa_sin_dsa_upstream():
    """En sync ind i en tabel, der ikke findes paa DSO, fejler i koerselstid; en
    sync foer DSA-genopbygningen har fyldt den, publicerer forrige leverance."""
    for asset in all_udredningsret_dso_sync_assets:
        deps = _deps(asset)
        navn = asset.key.to_user_string()
        assert "dso_infra_udredningsret" in deps, navn
        dsa = navn[len("dso_sync_") :]
        assert any(d.lower() == dsa for d in deps), navn


def test_forloebsfacten_synkroniserer_efter_alle_sine_fk_maal():
    """fct.Udredningsforloeb FK'er klassifikationsdimensionen, deler PK med
    egenskabsdimensionen og peger paa de delte borger-/organisationsdimensioner.
    Alle skal staa paa DSO foerst, ellers bryder linked-server-upserten RI."""
    forloeb = _efter_navn()["dso_sync_fct_udredningsforloeb"]
    for maal in (
        "dso_sync_dim_udredningsretklassifikation",
        "dso_sync_dim_udredningsretegenskaber",
        "dso_sync_dim_borger",
        "dso_sync_dim_organisation",
    ):
        assert maal in forloeb, maal


def test_markoerfacten_synkroniserer_sidst_efter_forloebsfacten():
    """Markoerfactens dw_ek_Udredningsforloeb er en FK ind i foraeldrefacten —
    den er derfor den eneste fact i modellen, der venter paa en anden fact."""
    markoer = _efter_navn()["dso_sync_fct_udredningsretmarkoer"]
    for maal in (
        "dso_sync_fct_udredningsforloeb",
        "dso_sync_dim_forloebsmarkoer",
        "dso_sync_dim_borger",
        "dso_sync_dim_organisation",
    ):
        assert maal in markoer, maal


def test_dimensionerne_venter_ikke_paa_hinanden_eller_paa_facterne():
    """De tre dimensioner har ingen indbyrdes FK'er, saa en kunstig kaede ville
    kun serialisere det, der kan koere parallelt — og en dimension, der ventede
    paa en fact, ville vende RI-raekkefoelgen paa hovedet."""
    efter = _efter_navn()
    dimensioner = {n for n in efter if n.startswith("dso_sync_dim_")}
    assert len(dimensioner) == 3
    for navn in dimensioner:
        andre_syncs = {d for d in efter[navn] if d in efter} - {navn}
        assert not andre_syncs, f"{navn} venter unoedigt paa {andre_syncs}"


def test_assets_er_registreret_i_det_samlede_dso_sync_lag():
    from DSO.load import UDREDNINGSRET_SYNC_CONFIGS as eksporteret
    from DSO.load import all_dso_sync_assets
    from DSO.load import all_udredningsret_dso_sync_assets as eksporterede

    assert eksporteret is UDREDNINGSRET_SYNC_CONFIGS
    for asset in all_udredningsret_dso_sync_assets:
        assert asset in all_dso_sync_assets
    assert eksporterede is all_udredningsret_dso_sync_assets


# ===========================================================================
# 3. Infrastruktur-assets (tabeller + views paa DSO)
# ===========================================================================


def test_infra_asset_koerer_kun_dim_og_fct_ddl_en_paa_dso(monkeypatch):
    """05_seeds seeder map-tabellerne (som ikke findes paa DSO), 02_maps minter
    IDENTITY-noegler, 00_archive holder den raa CPR og 01_meta er
    leveranceregistret. Faar nogen af dem foejet sig til listen, staar der
    CPR-baerende data paa rapporteringsdatabasen. Asset'et koeres mod en fake, saa
    det er de faktisk udfoerte filer — ikke en beskrivelse — der asserteres."""
    from infrastructure.assets import dso_infrastructure

    udfoerte: list[str] = []
    monkeypatch.setattr(
        dso_infrastructure,
        "execute_sql_file",
        lambda resource, sti, ctx=None, **kw: udfoerte.append(sti),
    )
    fn = dso_infrastructure.dso_infra_udredningsret.op.compute_fn.decorated_fn
    resultat = fn(dg.build_asset_context(), object())

    assert udfoerte == [
        "tables/udredningsret/03_dims.sql",
        "tables/udredningsret/04_facts.sql",
    ]
    assert resultat.metadata["files_executed"] == udfoerte
    # Filerne findes — de springes bevidst over, de er ikke bare uskrevne.
    ddl = _DDL / "tables" / "udredningsret"
    for forbudt in ("00_archive.sql", "01_meta.sql", "02_maps.sql", "05_seeds.sql"):
        assert (ddl / forbudt).exists(), forbudt
        assert not any(forbudt in f for f in udfoerte), f"{forbudt} maa ikke oprettes paa DSO"


def test_infra_asset_er_registreret_i_dso_laget():
    """Dette repo bygger KUN tabellerne paa DSO. Praesentationsviews over
    udredningsretten ejes af security-views-repoet, saa her findes hverken et
    view-asset eller en views/udredningsret-mappe (beslutning U19)."""
    from infrastructure import all_dso_assets, dso_infra_udredningsret

    assert dso_infra_udredningsret in all_dso_assets
    assert not hasattr(__import__("infrastructure"), "dso_infra_views_udredningsret")
    assert not (_DDL / "views" / "udredningsret").exists()


# ===========================================================================
# 5. Publish-jobbet
# ===========================================================================


def test_dso_sync_gruppen_ligger_i_publish_jobbet_og_ikke_i_ingest():
    """Synkroniseringen hoerer til publish, ikke ingest: ingest opretter
    stub-patienter for ukendte CPR, og intet maa publiceres mellem den oprettelse
    og nattens minting — ellers staar forloebene paa DSO med dw_ek_Borger = -1."""
    from jobs.udredningsret_jobs import udredningsret_ingest_job, udredningsret_publish_job

    assert SYNC_GRUPPE in str(udredningsret_publish_job.selection)
    assert SYNC_GRUPPE not in str(udredningsret_ingest_job.selection)


def test_publish_jobbet_opfrisker_ikke_de_delte_dimensioner_paa_dso():
    """dso_sync_dim_borger og dso_sync_dim_organisation ligger i gruppen
    shared_dimensions_dso_sync og er derfor UDEN FOR selektionen — praecis som i
    DRG. Det er sikkert (Dagster haandhaever kun raekkefoelge inden for den valgte
    delmaengde), men det betyder ogsaa, at en publish-koersel alene ikke opfrisker
    dim.Borger paa DSO; det goer nattekaeden, hvilket er netop det, sensoren
    venter paa."""
    from definitions import defs
    from jobs.udredningsret_jobs import udredningsret_publish_job

    graph = defs.resolve_asset_graph()
    valgt = {k.to_user_string() for k in udredningsret_publish_job.selection.resolve(graph)}

    assert "dso_sync_fct_udredningsforloeb" in valgt
    for delt in ("dso_sync_dim_borger", "dso_sync_dim_organisation"):
        assert delt not in valgt, delt
        assert delt in _efter_navn()["dso_sync_fct_udredningsforloeb"], delt


# ===========================================================================
# 6. Publish-sensoren (ren logik mod fakes — som HAIBA-sensorens tests)
# ===========================================================================


class _FakeCursor:
    """Svarer paa de tre forespoergsler, sensoren stiller.

    ``leverancer`` mapper kildetabel -> registreret leverancetidsstempel (en
    tabel, der mangler i dictet, har arkivet aldrig koert for). ``uafklarede`` er
    antallet af levende forloeb paa dw_ek_Borger = -1, eller None for "facten er
    ikke oprettet endnu"."""

    def __init__(self, leverancer, uafklarede, meta_findes=True):
        self.leverancer = leverancer
        self.uafklarede = uafklarede
        self.meta_findes = meta_findes
        self._resultat = None
        self.set_sql: list[str] = []

    def execute(self, sql, params=None):
        self.set_sql.append(sql)
        if "sys.tables" in sql:
            schema, table = params
            findes = self.meta_findes if schema == "meta" else self.uafklarede is not None
            self._resultat = [(int(findes),)]
        elif "UdredningsretLeverance" in sql:
            assert set(params) == set(ARC_SOURCE_TABLES)
            self._resultat = [(k, v) for k, v in self.leverancer.items()]
        elif "dw_ek_Borger" in sql:
            # Kun LEVENDE raekker paa forloebsfacten: en retireret raekke paa -1
            # bliver aldrig repareret (U9 re-stager den ikke), saa den ville
            # laase gaten over nul for evigt.
            assert "COUNT(*)" in sql
            assert "[dw_ek_Borger] = -1" in sql and "[dw_is_deleted] = 0" in sql
            assert FORLOEB_FACT_TABLE in sql and "UdredningsretMarkoer" not in sql
            self._resultat = [(self.uafklarede,)]
        else:
            raise AssertionError(f"uventet forespoergsel: {sql}")

    def fetchone(self):
        return self._resultat[0]

    def fetchall(self):
        return self._resultat

    def close(self):
        pass


class _FakeMSSQL:
    def __init__(self, leverancer, uafklarede, meta_findes=True, rejser=None):
        self.leverancer = leverancer
        self.uafklarede = uafklarede
        self.meta_findes = meta_findes
        self.rejser = rejser

    def get_connection(self):
        if self.rejser:
            raise self.rejser
        cursor = _FakeCursor(self.leverancer, self.uafklarede, self.meta_findes)

        class _Conn:
            def __enter__(self_indre):
                return self_indre

            def __exit__(self_indre, *exc):
                return False

            def cursor(self_indre):
                return cursor

        return _Conn()


class _FakeInstance:
    """KOErselshistorik for publish-jobbet. ``koersler`` er nyeste foerst."""

    def __init__(self, koersler=()):
        self.koersler = list(koersler)

    def get_run_records(self, filters=None, limit=None):
        oensket = dict(getattr(filters, "tags", None) or {})
        traef = [r for r in self.koersler if all(r["tags"].get(k) == v for k, v in oensket.items())]
        return [
            type(
                "Rec",
                (),
                {
                    "dagster_run": type(
                        "Run",
                        (),
                        {"tags": r["tags"], "status": r.get("status", dg.DagsterRunStatus.SUCCESS)},
                    )()
                },
            )()
            for r in traef[: limit or len(traef)]
        ]


class _FakeContext:
    def __init__(self, instance, run_id="run-abc"):
        self.instance = instance
        self.log = dg.get_dagster_logger()
        self.dagster_run = type("Run", (), {"run_id": run_id, "job_name": "ifdb_dfm_daily_job"})()


def _leverancer(stempel="2026-09-07 03:00:00"):
    return {navn: stempel for navn in ARC_SOURCE_TABLES}


def _signatur(stempel="2026-09-07 03:00:00"):
    return ";".join(f"{navn}={stempel}" for navn in ARC_SOURCE_TABLES)


def _publiceret(signatur, uafklarede_tag=None):
    tags = {DELIVERY_SIGNATURE_TAG: signatur}
    if uafklarede_tag is not None:
        tags[UNRESOLVED_TAG] = str(uafklarede_tag)
    return {"tags": tags}


def _koer(mssql, instance, run_id="run-abc"):
    return evaluate(_FakeContext(instance, run_id), publish_job_name=PUBLISH_JOB, mssql=mssql)


# --- laesningen af lageret --------------------------------------------------


def test_signaturen_daekker_hver_arkiveret_kildetabel():
    """Signaturen ER leveranceidentiteten. To forskellige leverancer maa aldrig
    sammenlignes ens — saa ville den nye blive publiceret."""
    signatur = read_delivery_signature(_FakeCursor(_leverancer(), 0))

    for navn in ARC_SOURCE_TABLES:
        assert navn in signatur
    assert signatur != read_delivery_signature(_FakeCursor(_leverancer("2026-08-31 03:00:00"), 0))


def test_tomt_eller_manglende_leveranceregister_giver_ingen_signatur():
    """En delvis signatur, der tilfaeldigvis sammenlignede ens med en publiceret,
    ville tavst undertrykke en publicering — den maa derfor slet ikke dannes."""
    assert read_delivery_signature(_FakeCursor({}, 0)) is None
    assert read_delivery_signature(_FakeCursor(_leverancer(), 0, meta_findes=False)) is None


def test_uafklarede_taelles_paa_forloebsfacten_og_kun_levende():
    """Markoerfacten baerer ogsaa dw_ek_Borger, men taelles bevidst ikke: U9's
    genopretning vaelger helbredsforloeb fra levende raekker i FORLOEBSfacten, saa
    et helbredsforloeb uden F_ID har ingen forloebsraekke og re-stages aldrig.
    Praedikaterne selv haandhaeves af _FakeCursor."""
    assert read_unresolved_forloeb(_FakeCursor(_leverancer(), 0)) == 0
    assert read_unresolved_forloeb(_FakeCursor(_leverancer(), 412)) == 412
    # "facten findes ikke endnu" er ikke det samme som "nul uafklarede".
    assert read_unresolved_forloeb(_FakeCursor(_leverancer(), None)) is None


# --- gate 1: publicér én gang pr. leverance ---------------------------------


def test_fyrer_for_en_upubliceret_leverance():
    resultat = _koer(_FakeMSSQL(_leverancer(), 0), _FakeInstance())

    assert isinstance(resultat, dg.SensorResult)
    (request,) = resultat.run_requests
    assert request.job_name == PUBLISH_JOB
    assert request.tags[DELIVERY_SIGNATURE_TAG] == _signatur()
    # Foerste publicering baerer ingen taelling — det fravaer er netop det, der
    # giver foerste genopretningsforsoeg et ubetinget forsoeg.
    assert UNRESOLVED_TAG not in request.tags


def test_republicerer_ikke_en_afklaret_leverance():
    """SDS leverer ugentligt, mens den overvaagede kaede koerer hver nat. Uden
    denne gate re-stages 8,2 M arkivraekker og begge facter publiceres til DSO
    hver nat for at opdage, at intet aendrede sig."""
    instance = _FakeInstance([_publiceret(_signatur())])

    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 0), instance), dg.SkipReason)


def test_en_ny_leverance_aabner_gaten_igen():
    instance = _FakeInstance([_publiceret(_signatur("2026-08-31 03:00:00"))])

    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 0), instance), dg.SensorResult)


def test_en_fejlet_publicering_forsoeges_igen_ved_naeste_nattekoersel():
    """Gaten spoerger efter en SUCCESFULD publicering, ikke en anmodet — ellers
    ville run-key-deduplikering strande leverancen til naeste uge. Her betyder det
    mere end for HAIBA: baade kontroltotalen (U13) og omgrupperingsdetektoren
    (U15) fejler publiceringen udad, saa sensoren SKAL kunne gaa ud af den
    tilstand selv. run_key baerer den udloesende koersels id, saa hver nattesucces
    giver hoejst ét forsoeg."""
    mssql, instance = _FakeMSSQL(_leverancer(), 0), _FakeInstance()

    foerste = _koer(mssql, instance, run_id="nat-1")
    anden = _koer(mssql, instance, run_id="nat-2")

    assert isinstance(foerste, dg.SensorResult) and isinstance(anden, dg.SensorResult)
    assert foerste.run_requests[0].run_key != anden.run_requests[0].run_key


# --- gate 2: genopretning mens borgerkaeden stadig afklarer ------------------


def test_genopretter_forloeb_uden_borger_efter_foerste_publicering():
    instance = _FakeInstance([_publiceret(_signatur())])

    resultat = _koer(_FakeMSSQL(_leverancer(), 412), instance)

    assert isinstance(resultat, dg.SensorResult)
    # Taellingen, der udloeste forsoeget, bliver baseline for det naeste.
    assert resultat.run_requests[0].tags[UNRESOLVED_TAG] == "412"


def test_bliver_ved_saa_laenge_taellingen_falder():
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=412)])

    resultat = _koer(_FakeMSSQL(_leverancer(), 37), instance)

    assert isinstance(resultat, dg.SensorResult)
    assert resultat.run_requests[0].tags[UNRESOLVED_TAG] == "37"


def test_stopper_naar_taellingen_flader_ud():
    """Nogle forloeb afklares aldrig: en CPR, FHIR-serveren kun kender som
    previous_ssn, er "kendt" for stub-oprettelsen, saa ingen stub laves, men den
    naar aldrig ref.PatientToBorger. Uden plateau-detektion ville sensoren jage
    et tal, der ikke kan naa nul, hver eneste nat."""
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=37)])

    resultat = _koer(_FakeMSSQL(_leverancer(), 37), instance)

    assert isinstance(resultat, dg.SkipReason)
    assert "stopped resolving" in str(resultat.skip_message)


def test_en_stigende_taelling_er_ogsaa_et_plateau():
    """Flere uafklarede end sidst er ikke fremgang. Uden >= ville en stigning
    (nye forloeb fra en delvist publiceret leverance) starte koerslerne igen."""
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=37)])

    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 40), instance), dg.SkipReason)


def test_et_plateau_blokerer_ikke_den_naeste_leverance():
    instance = _FakeInstance([_publiceret(_signatur("2026-08-31 03:00:00"), uafklarede_tag=37)])

    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 37), instance), dg.SensorResult)


def test_genopretningen_stopper_naar_alt_er_afklaret():
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=37)])

    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 0), instance), dg.SkipReason)


def test_en_databasefejl_skipper_i_stedet_for_at_fejle_ticket():
    mssql = _FakeMSSQL(_leverancer(), 0, rejser=RuntimeError("login timeout"))

    resultat = _koer(mssql, _FakeInstance())

    assert isinstance(resultat, dg.SkipReason)
    assert "login timeout" in str(resultat.skip_message)


# --- wiring -----------------------------------------------------------------


def test_sensoren_er_registreret_og_anmoder_publish_jobbet():
    from definitions import postfixed_sensors, udredningsret_publish_sensor

    assert udredningsret_publish_sensor in postfixed_sensors
    assert [t.job_name for t in udredningsret_publish_sensor.targets] == [PUBLISH_JOB]
    assert udredningsret_publish_sensor.required_resource_keys == frozenset(
        {"mssql_target_staging"}
    )


def test_sensoren_venter_paa_borgerkaeden_ikke_paa_dfm_base_jobbet():
    """Den skal udloeses af det job, der koerer sof_patient_dfm ->
    ref_patient_borger -> dim_Borger -> dso_sync_dim_borger. Haenges den paa
    base-jobbet i stedet, publiceres der, foer borgernoeglerne findes."""
    from definitions import udredningsret_publish_sensor

    assert "ifdb_dfm_daily_job SUCCESS" in udredningsret_publish_sensor.description


# ===========================================================================
# 7. DQA-subjects (spec afsnit 9)
# ===========================================================================


FORVENTEDE_SUBJECTS = {
    "udredningsret_dobbeltregistrerede_markoerer",
    "udredningsret_markoernoegle_dubletter",
    "udredningsret_flere_endeligt_udredt",
    "udredningsret_afslutning_og_afbrydelse",
    "udredningsret_lang_udredningstid",
    "udredningsret_aendret_indikatorvaerdi",
    "udredningsret_soft_slettede_forloeb",
    "udredningsret_sor_uden_opslag",
    "udredningsret_skiftet_foerste_markoer",
    "udredningsret_forloeb_pr_helbredsforloeb",
}


def _subject_efter_navn():
    return {s.name: s for s in UDREDNINGSRET_SUBJECTS}


def test_alle_ti_subjects_findes_og_er_praefikset():
    """Spec afsnit 9 fastlaegger ni subjects plus afstemningen pr.
    helbredsforloeb (nr. 10). Et glemt subject er en maaling, ingen savner."""
    assert {s.name for s in UDREDNINGSRET_SUBJECTS} == FORVENTEDE_SUBJECTS
    assert len(UDREDNINGSRET_SUBJECTS) == 10
    assert all(s.name.startswith("udredningsret_") for s in UDREDNINGSRET_SUBJECTS)


def test_subjects_er_registreret_i_all_subjects():
    """ALL_SUBJECTS er det eneste sted, DQA-jobbene bygges fra — et uregistreret
    subject koerer aldrig."""
    registrerede = {s.name: s for s in ALL_SUBJECTS}
    for s in UDREDNINGSRET_SUBJECTS:
        assert registrerede.get(s.name) is s, s.name


def test_subjects_ligger_paa_dwh_sporet_uden_source_split():
    """source=None holder dem i dqa_dwh_daily_job sammen med de to
    lineage-udledte fact-subjects. Et source-split ville sprede checkene paa
    fct.Udredningsforloeb over to job uden gevinst."""
    for s in UDREDNINGSRET_SUBJECTS:
        assert s.track is Track.DWH and s.source is None, s.name
        assert s.resource_key == "mssql_target_staging", s.name
        assert s.deleted_column == "dw_is_deleted", s.name


def test_hvert_check_er_custom_sql_og_kun_taellinger():
    """Kun tael-SQL og naevner: ingen examples_sql, ingen anden parameter. En
    examples_sql ville laegge raekkevaerdier i dqa.Results — og domaenets egen
    forretningsnoegle er et stabilt pseudonym over CPR."""
    for s in UDREDNINGSRET_SUBJECTS:
        assert s.checks, s.name
        for spec in s.checks:
            etiket = f"{s.name}/{spec.name_suffix}"
            assert spec.check_type == "custom_sql", etiket
            assert spec.fn is None, etiket
            assert set(spec.params) == {"sql", "checked_sql"}, etiket
            assert spec.name_suffix, etiket


def test_intet_check_kan_eksponere_cpr_eller_det_pseudonyme_helbredsforloeb():
    """clean_code_Helbredsforloeb = 'UDREDHF_' + SHA2_256 over CPR: et stabilt
    pseudonym. Afstemningen (nr. 10) BEREGNER den som join-noegle — det er
    lovligt — men ingen check maa vaelge den ud som resultatkolonne, og ingen maa
    naevne arkivets CPR-kolonne overhovedet."""
    for s in UDREDNINGSRET_SUBJECTS:
        for spec in s.checks:
            etiket = f"{s.name}/{spec.name_suffix}"
            assert "examples_sql" not in spec.params, etiket
            for navn, sql in spec.params.items():
                mrk = f"{etiket}/{navn}"
                assert not re.search(r"\[CPR\]|\bcprnr\b", sql, re.IGNORECASE), mrk
                # Den YDERSTE projektion (den eneste SELECT i kolonne 0; CTE'er
                # og subqueries er indrykkede) skal vaere en skalar taelling, saa
                # der ikke kan komme en raekkevaerdi ud af checket.
                ydre = re.findall(r"(?m)^SELECT\s+(.*)", sql)
                assert len(ydre) == 1, f"{mrk}: {len(ydre)} projektioner i kolonne 0"
                assert (
                    ydre[0].upper().startswith(("COUNT(", "ISNULL(SUM("))
                ), f"{mrk}: {ydre[0][:60]}"


def test_hvert_check_koerer_dagligt():
    """dqa_dwh_weekly_job har ingen cron og anmodes kun loerdag af
    after-publish-sensorerne — et ugentligt udredningsretscheck ville kun koere,
    naar en leverances publicering tilfaeldigvis faldt paa en loerdag."""
    for s in UDREDNINGSRET_SUBJECTS:
        for spec in s.checks:
            assert spec.cadence is Cadence.DAILY, f"{s.name}/{spec.name_suffix}"
            assert spec.enabled, f"{s.name}/{spec.name_suffix}"


def test_severity_tierne_afspejler_hvad_tallet_betyder():
    """De ti tal betyder ikke det samme: otte er egenskaber ved SDS' data uden
    aftalt baseline (INFO passerer altid og trendes), en uopslagbar SOR-kode er en
    opslagsregression (WARN), og en afvigelse pr. helbredsforloeb er en modelfejl
    (ERROR). Ingen af dem er BLOCKING — DQA retter aldrig data og gater aldrig
    ETL'en."""
    efter_navn = _subject_efter_navn()
    severiteter = {navn: {spec.severity for spec in s.checks} for navn, s in efter_navn.items()}
    assert severiteter["udredningsret_sor_uden_opslag"] == {Severity.WARN}
    assert severiteter["udredningsret_forloeb_pr_helbredsforloeb"] == {Severity.ERROR}
    for navn, sev in severiteter.items():
        if navn not in (
            "udredningsret_sor_uden_opslag",
            "udredningsret_forloeb_pr_helbredsforloeb",
        ):
            assert sev == {Severity.INFO}, navn
        assert Severity.BLOCKING not in sev, navn


def test_sor_checket_taeller_kun_koder_der_faktisk_er_leveret():
    """En blank SOR-kode opslaas lovligt til -1. Talte checket dem med, ville det
    maale hyppigheden af blanke felter i stedet for opslagsregressioner — og de
    to roller holdes adskilt, fordi de fejler af forskellige grunde."""
    s = _subject_efter_navn()["udredningsret_sor_uden_opslag"]
    assert {spec.name_suffix for spec in s.checks} == {"forloeb_enhed_sor", "kontakt_enhed_sor"}
    for spec in s.checks:
        sql = spec.params["sql"]
        assert "NULLIF(LTRIM(RTRIM(" in sql and "IS NOT NULL" in sql
        assert "= -1" in sql
        # Naevneren er de leverede koder, ikke alle forloeb.
        assert "NULLIF(LTRIM(RTRIM(" in spec.params["checked_sql"]


def test_afstemningen_pr_helbredsforloeb_overlever_udligning():
    """Kontroltotalen (U13) sammenligner to skalarer over hele populationen, saa
    en splitning og en sammenlaegning i samme vindue udligner hinanden og
    koerslen gaar groen med begge noegler forkerte. Denne afstemning er PR noegle
    — FULL OUTER JOIN, saa en noegle, der kun findes paa den ene side, ogsaa
    taeller — og kan derfor ikke udlignes."""
    (spec,) = _subject_efter_navn()["udredningsret_forloeb_pr_helbredsforloeb"].checks
    sql = spec.params["sql"]
    assert "FULL OUTER JOIN" in sql
    assert re.search(r"ISNULL\(\s*a\.\[antal\],\s*0\)\s*<>\s*ISNULL\(\s*f\.\[antal\],\s*0\)", sql)
    # Blank F_ID hoerer til intet forloeb (U5) og har ingen fact-raekke.
    assert "LTRIM(RTRIM(a.[F_ID])) <> ''" in sql
    # Arkivsiden skal taelle DISTINKTE F_ID — ellers ville dubletraekker paa
    # markoernoeglen (subject 2) se ud som ekstra forloeb.
    assert "COUNT(DISTINCT LTRIM(RTRIM(a.[F_ID])))" in sql


def test_forloebsspecifikke_checks_udelader_ukendt_sentinellen():
    """1,17 M markoerer uden F_ID staar lovligt paa dw_ek_Udredningsforloeb = -1
    (U5). Grupperes de med, samles de i ét fantomforloeb, der altid har baade
    flere afslutninger og en afbrydelse."""
    for navn in ("udredningsret_flere_endeligt_udredt", "udredningsret_afslutning_og_afbrydelse"):
        (spec,) = _subject_efter_navn()[navn].checks
        assert "[dw_ek_Udredningsforloeb] <> -1" in spec.params["sql"], navn


def test_markoergrupperne_i_checkene_findes_i_stagingens_egne_grupper():
    """Grupperne skrives af markoer_gruppe_sql fra MARKOER_GRUPPER. Omdoebes en
    gruppe der, ville checket ellers tavst taelle nul for evigt. _gruppe() faelder
    allerede en ukendt gruppe ved import; det, der kan slippe forbi den, er en
    literal skrevet direkte ind i SQL'en — saa der laeses praecis de literaler,
    der staar i et [gruppe]-praedikat (baade '=' og IN), ikke enhver N'...' i
    SQL'en (en fremtidig tekstvaerdi ville ellers faelde testen uden grund)."""
    from DSA.SQL.staging.udredningsret_kolonner import MARKOER_GRUPPE_TEKSTER

    brugte = set()
    for navn in ("udredningsret_flere_endeligt_udredt", "udredningsret_afslutning_og_afbrydelse"):
        (spec,) = _subject_efter_navn()[navn].checks
        praedikater = re.findall(r"\[gruppe\]\s*(?:=\s*N'\w+'|IN\s*\([^)]*\))", spec.params["sql"])
        assert praedikater, f"{navn}: intet [gruppe]-praedikat — er SQL'ens form aendret?"
        for praedikat in praedikater:
            brugte |= set(re.findall(r"N'(\w+)'", praedikat))
    assert brugte, "ingen gruppenavne fundet — er SQL'ens form aendret?"
    for gruppe in brugte:
        assert gruppe in MARKOER_GRUPPE_TEKSTER, gruppe


def test_aendret_indikatorvaerdi_er_bundet_til_leveranceperioden():
    """Uden den nedre graense bliver checket "enhver raekke nogensinde opdateret"
    og rapporterer et meningsloest femcifret tal som nyt. ISNULL-faldbacken til
    9999-12-31 tommer vinduet, hvis registret mangler — bevidst den sikre vej."""
    (spec,) = _subject_efter_navn()["udredningsret_aendret_indikatorvaerdi"].checks
    sql = spec.params["sql"]
    assert "[dw_timestamp_update] > [dw_timestamp_insert]" in sql
    assert "MAX([leverance_tidsstempel])" in sql
    assert "9999-12-31" in sql


def test_retirerede_forloeb_maales_mod_hele_facten():
    """Naevneren er ALLE raekker, levende og retirerede. Var den kun de levende,
    ville andelen vokse, naar populationen skrumper — altsaa netop naar tallet
    skulle vaere mest laeseligt."""
    (spec,) = _subject_efter_navn()["udredningsret_soft_slettede_forloeb"].checks
    assert "[dw_is_deleted] = 1" in spec.params["sql"]
    assert "dw_is_deleted" not in spec.params["checked_sql"]


def test_dqa_sensoren_er_kaedet_paa_publish_jobbet():
    """De ti subjects og de to lineage-udledte fact-subjects ligger alle i
    dqa_dwh_daily_job; den ugentlige tier anmodes for facternes
    referential_integrity."""
    spec = next(
        s for s in DQA_AFTER_ETL_SENSOR_SPECS if s["name"] == "dqa_udredningsret_after_publish"
    )
    assert spec["monitored"] == [PUBLISH_JOB]
    assert spec["daily_job"] == "dqa_dwh_daily_job"
    assert spec["weekly_jobs"] == ["dqa_dwh_weekly_job"]
    assert spec["default_status"] is dg.DefaultSensorStatus.STOPPED


@pytest.mark.parametrize("status", [dg.DagsterRunStatus.FAILURE, dg.DagsterRunStatus.CANCELED])
@pytest.mark.parametrize("unresolved", [0, 37])
def test_genforsoeger_seneste_fejlede_genopretning_foer_borger_gaten(status, unresolved):
    """DSA kan vaere committet, selv om DSO stadig har den gamle publicering."""
    failed = _publiceret(_signatur(), uafklarede_tag=37)
    failed["status"] = status
    instance = _FakeInstance([failed, _publiceret(_signatur(), uafklarede_tag=37)])
    result = _koer(_FakeMSSQL(_leverancer(), unresolved), instance)
    assert isinstance(result, dg.SensorResult)
    assert result.run_requests[0].tags[UNRESOLVED_TAG] == "37"


@pytest.mark.parametrize("status", [dg.DagsterRunStatus.QUEUED, dg.DagsterRunStatus.STARTED])
def test_starter_ikke_en_ekstra_publicering_mens_seneste_koerer(status):
    pending = _publiceret(_signatur())
    pending["status"] = status
    result = _koer(_FakeMSSQL(_leverancer(), 37), _FakeInstance([pending]))
    assert isinstance(result, dg.SkipReason)
    assert "pending" in result.skip_message


def test_fejl_foer_seneste_succes_udloeser_ikke_en_ny_publicering():
    failed = _publiceret(_signatur(), uafklarede_tag=37)
    failed["status"] = dg.DagsterRunStatus.FAILURE
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=37), failed])
    assert isinstance(_koer(_FakeMSSQL(_leverancer(), 0), instance), dg.SkipReason)


@pytest.mark.parametrize("unresolved_tag", [None, 37])
def test_manglende_fact_er_ukendt_daekning_ikke_nul_uafklarede(unresolved_tag):
    instance = _FakeInstance([_publiceret(_signatur(), uafklarede_tag=unresolved_tag)])
    result = _koer(_FakeMSSQL(_leverancer(), None), instance)
    assert isinstance(result, dg.SkipReason)
    assert "fct.Udredningsforloeb is missing" in result.skip_message
    assert "mode=full_load" in result.skip_message
    assert "every live forloeb resolved" not in result.skip_message


def test_publish_genforsoeg_koerer_ikke_arkivets_merge():
    """The archive merge belongs to the ingest job; a publish retry does not run it."""
    from definitions import defs
    from jobs.udredningsret_jobs import udredningsret_ingest_job, udredningsret_publish_job

    graph = defs.resolve_asset_graph()
    ingest = {k.to_user_string() for k in udredningsret_ingest_job.selection.resolve(graph)}
    publish = {k.to_user_string() for k in udredningsret_publish_job.selection.resolve(graph)}
    assert "arc_udredningsret_mont" in ingest
    assert "arc_udredningsret_mont" not in publish
    assert not ingest.intersection(publish)
