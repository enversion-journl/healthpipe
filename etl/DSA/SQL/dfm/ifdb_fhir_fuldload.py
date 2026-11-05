# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Meta-asset: fuldindlæsning af IFDB -> FHIR-kæden, ét vindue ad gangen.

Ét grønt gennemløb betyder, at ALT er kørt for vinduet: Oracle -> ext -> arc ->
Aidbox -> staging (SOF) -> DSO -> fhir.SFI_datapoint_store. Du skal ikke angive
noget. Asset'et finder selv det ældste vindue, der mangler, kører kæden og rykker
sin markør ÉN gang til sidst -- kun hvis alle fatale trin var grønne. Næste kørsel
tager den næste måned, og sådan fortsætter det, indtil historikken er inde. Så
melder den færdig og gør ingenting.

Stop er derfor gratis. Afbrydes en kørsel, står markøren, hvor den stod, og
vinduet tages forfra næste gang. Der efterlades ingen halv tilstand, fordi hvert
trin er idempotent: arkivet skriver de samme rækker igen i periodetilstand,
posten er en upsert mod Aidbox, SOF og DSO er MERGE, og storen sletter og
indsætter pr. ressource. Prisen ved en afbrydelse er spildt arbejde, ikke data.

Markøren er en egen række i ``meta.DFMState`` under scope ``backfill``, adskilt
fra de checkpoints, assets'ene selv ejer. Uden den ville en daglig kørsel, der
flytter arkivets checkpoints frem til nu, få fuldindlæsningen til at tro, at den
var færdig, mens historikken stadig manglede.

Asset'et skriver aldrig selv de checkpoints, den daglige kæde ejer; et vindue
skriver kun sin egen markør. Skrev vinduet sin slutdato ind i kædens
checkpoints, ville næste natkørsel spole historikken igennem. Postens byggere
kører i deres egen delta-tilstand og rykker derfor deres egne checkpoints, som
hver morgen — til læsestarten, ikke til vinduets slutdato.

Det, asset'et ved, og som man ellers skal huske:
  * Arkivet afgrænser på kildens delta-kolonne, posten på lagerets
    dw_timestamp_update. Posten kører derfor i delta og læser alt, byggerne ikke
    har postet endnu — også det, vinduet lige har arkiveret.
  * Før posten hentes og arkiveres nye patienter i delta, og foreningen af
    identiteter bygges. Posten læser også rækker, andre kørsler har arkiveret, og
    en Encounter uden sin patient i arkivet stopper ellers hele vinduet.
  * Posten og sof_dso begynder ikke, mens en daglig kørsel er i gang: de to må
    ikke læse og skrive det samme samtidig. Natten stopper da, og vinduet tages
    forfra næste nat.
  * Er tidsbudgettet brugt midt i et vindue, afbryder kørslen sig selv. Den daglige
    kæde venter ikke på fuldindlæsningen, så et vindue, der løber over, ville dele
    arkiv, lås og DSO med den. Vinduet tages forfra næste nat.
  * Posten er fatal. Et vindue, der ikke er postet, må ikke rykke markøren, for
    så arver næste vindues post det.
  * Store-refreshen kan først køre, når DSO-synken er færdig; den ligger derfor i
    sof_dso-jobbet og ikke som et løst trin.
  * Verifikationsporten fejler ved design, når kendte kontraktbrud findes, så den
    er ikke fatal. Dens tal ender i metadataen, hvor de kan følges over tid.
  * SOF-projektionernes tidsbudget er slået fra i vinduets kørsler. Budgettet er
    lavet til den daglige kæde; med det ville vinduet slutte grønt og lade resten
    af månedens spørgeskemasvar vente til næste morgens kæde.
  * SOF læser vinduet i én skive, og dens backfill-port ser kun på antal rækker,
    ikke på andelen af måltabellen. Med skiver på en million ressourcer og kravet
    om en tiendedel af tabellen åbner porten aldrig, når tabellen er vokset, og
    MERGE'n i QR-tabellerne kører med alle indeks på.
  * Kørslens egne dso_sync/*- og sfi_datapoint/*-tags gives videre til trinnene, så
    indeksporten og storens pause kan styres for én fuldindlæsning uden at ændre miljøet.
"""

# Ingen ``from __future__ import annotations``: dg.Config-klasser kan ikke opløses
# fra strengannoteringer (PEP 563), og Dagster fejler ved definitionstid.

import _thread
import calendar
import threading
import time
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any, NamedTuple
from zoneinfo import ZoneInfo

import dagster as dg
from core.branch_postfix import get_postfixed_name
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster._core.definitions.reconstruct import ReconstructableJob, ReconstructableRepository
from dagster._core.execution.api import execute_run
from DSA.FHIR.utils.sql_on_fhir.sof_utils import (
    SOF_BACKFILL_MIN_RATIO_TAG,
    SOF_SLICE_MAX_RESOURCES_TAG,
    SOF_SLICE_TIME_BUDGET_MINUTES_TAG,
)
from DSA.SQL.dfm.period_orchestrator import DatePartition, build_period_run_config

# Markøren: hvor langt fuldindlæsningen er nået. Scope 'backfill' holder den fri af
# de checkpoints, den daglige kæde flytter. Skemaet skal være kendt af DfmManager.
MARKOER_SCOPE = "backfill"
MARKOER_SCHEMA = "arc"
MARKOER_TABEL = "ifdb_fuldload"

# Historikkens start: migreringsbulken på kildens opdateringsakse. Alt før er tomt.
HISTORIK_START = date(2021, 10, 1)

_VERIFIKATION_ASSET = "sfi_genkoersel_verifikation"
_VIDERESENDTE_TAG_PRAEFIKSER = ("dso_sync/", "sfi_datapoint/")
_WINDOW_RECONCILIATION = "ifdb_backfill_window_reconciliation"

# Anden halvdel af kæden: SOF med sin infrastruktur, dw-nøglerne, DSO-synken,
# kvalitetstjekkene, codesystem-synken og borger-kæden — og dermed også store-refreshen,
# der venter på DSO. Udpeget ved gruppe og asset, ALDRIG ved jobnavn: et job kan blive slået
# sammen eller omdøbt uden at give merge-konflikt, og så fejler hvert vindue.
_SOF_DSO_GRUPPER = (
    "fhir_sof_dfm",
    "fhir_dw_keys",
    "fhir_dso_sync_dfm",
    "fhir_quality_dfm",
    "codesystem_dso_sync",
)
_SOF_DSO_ASSETS = (
    "staging_infra_fhir",
    "fhir_sof_infrastructure",
    "cs_sfi",
    "ref_patient_borger",
    "ref_patient_address_borger",
    "dim_Borger",
    "dim_BorgerDemografi",
    "dso_sync_dim_borger",
    "dso_sync_dim_borgerdemografi",
)


class Vindue(NamedTuple):
    """Et lukket datointerval, begge ender inklusive."""

    fra: str
    til: str

    def __str__(self) -> str:
        return f"{self.fra} -> {self.til}"


@dataclass(frozen=True)
class Trin:
    """Ét trin i kæden, udpeget ved asset-grupper og asset-navne.

    ``mode`` tom betyder, at trinnets config ikke røres, så assets'ene kører i
    deres egen standardtilstand (delta). ``fatal`` falsk betyder, at en fejl
    noteres, men hverken stopper vinduet eller holder markøren tilbage.
    """

    navn: str
    grupper: tuple[str, ...] = ()
    assets: tuple[str, ...] = ()
    undtag: tuple[str, ...] = ()
    mode: str = ""
    fatal: bool = True


_PATIENT_ASSETS = ("ext_ifdb_patient", "arc_patient_ifdb", "ref_patient_identity")

# Rækkefølgen ER kontrakten for, hvad "kørt" betyder.
TRIN: tuple[Trin, ...] = (
    Trin(navn="arkiv", grupper=("ifdb_archive",), mode="period"),
    # Fatal: a month not proven complete in the archive must not move the marker.
    Trin(navn="window_reconciliation", assets=(_WINDOW_RECONCILIATION,), mode="period"),
    # Delta: the post reads every archive row not yet posted, also rows other runs
    # archived, so their patients must be in the archive and the union first.
    Trin(navn="patienter", assets=_PATIENT_ASSETS),
    # Delta, not period: builders select on dw_timestamp_update. Fatal: a month
    # not posted must not move the marker, or the next month's post inherits it.
    Trin(navn="post", grupper=("fhir_ifdb_dfm",)),
    Trin(navn="sof_dso", grupper=_SOF_DSO_GRUPPER, assets=_SOF_DSO_ASSETS),
    Trin(navn="verifikation", assets=(_VERIFIKATION_ASSET,), fatal=False),
)


class FuldloadConfig(dg.Config):
    """Alt er valgfrit. Tomt felt betyder, at asset'et selv finder ud af det."""

    # Overstyring til reparation af ét bestemt vindue. Markøren spørges ikke, og
    # den rykkes kun, hvis vinduet ligger i forlængelse af den.
    vindue_fra: str = ""
    vindue_til: str = ""
    # Hvor mange vinduer én kørsel må tage. Standard er ét; skemaet tager resten.
    max_vinduer: int = 1
    # "HH:MM" lokal tid. Sat, begynder den kun et nyt vindue, hvis det når at blive
    # færdigt inden da, regnet på nattens længste vindue.
    koer_indtil: str = ""
    # Verifikationsporten fejler ved design på kendte brud; sæt til sand, hvis en
    # rød port skal stoppe kørslen.
    stop_ved_roed_verifikation: bool = False
    # Tørkørsel: kør vinduet, men lad markøren stå.
    ryk_markoer: bool = True


# ---------------------------------------------------------------------------
# Vinduer og markør
# ---------------------------------------------------------------------------


def _parse_dato(vaerdi: str) -> date:
    return datetime.strptime(vaerdi.strip()[:10], "%Y-%m-%d").date()


def _foerste_i_maaneden(dag: date) -> date:
    return dag.replace(day=1)


def _sidste_i_maaneden(dag: date) -> date:
    return date(dag.year, dag.month, calendar.monthrange(dag.year, dag.month)[1])


def naeste_vindue(markoer: str | None, i_dag: date) -> Vindue | None:
    """Månedsvinduet efter markøren. ``None`` betyder, at historikken er inde.

    Markøren holder sidste grønne vindues slutdato. Uden markør begynder vi ved
    historikkens start. Indeværende måned overlades til den daglige kæde, så
    fuldindlæsningen melder færdig, når den har indhentet den.
    """
    if not markoer:
        start = HISTORIK_START
    else:
        start = _foerste_i_maaneden(_sidste_i_maaneden(_parse_dato(markoer)) + timedelta(days=1))
    start = _foerste_i_maaneden(start)
    if start >= _foerste_i_maaneden(i_dag):
        return None
    return Vindue(start.isoformat(), _sidste_i_maaneden(start).isoformat())


def vindue_fra_config(config: FuldloadConfig) -> Vindue | None:
    """Eksplicit vindue fra config, hvis begge ender er sat."""
    if config.vindue_fra and config.vindue_til:
        return Vindue(
            _parse_dato(config.vindue_fra).isoformat(), _parse_dato(config.vindue_til).isoformat()
        )
    if config.vindue_fra or config.vindue_til:
        raise dg.Failure(description="vindue_fra og vindue_til skal sættes sammen eller slet ikke.")
    return None


def beregn_deadline(koer_indtil: str, start: datetime) -> datetime | None:
    """Tidsbudgettet som ét fast klokkeslæt, beregnet én gang ved kørslens start."""
    if not koer_indtil.strip():
        return None
    klokken = datetime.strptime(koer_indtil.strip(), "%H:%M").time()
    graense = datetime.combine(start.date(), klokken)
    if graense <= start:
        graense += timedelta(days=1)
    return graense


_KOEBENHAVN = ZoneInfo("Europe/Copenhagen")


def _dansk_tid() -> datetime:
    """Klokken i København uden tidszone; koer_indtil er dansk tid uanset containerens zone."""
    return datetime.now(_KOEBENHAVN).replace(tzinfo=None)


def deadline_vagt(deadline: datetime | None, nu: datetime, stop: Any) -> threading.Timer | None:
    """En timer, der kalder ``stop``, når deadline passeres; ``None`` uden deadline."""
    if deadline is None:
        return None
    vagt = threading.Timer(max(0.0, (deadline - nu).total_seconds()), stop)
    vagt.daemon = True
    return vagt


def naeste_vindue_naas(nu: datetime, laengste_vindue_sekunder: float, deadline: datetime) -> bool:
    """Om et vindue, der varer som det længste i nat, når at blive færdigt før deadline."""
    return nu + timedelta(seconds=laengste_vindue_sekunder) <= deadline


# ---------------------------------------------------------------------------
# Opslag af mål
# ---------------------------------------------------------------------------


def _navnevarianter(navn: str) -> frozenset[str]:
    """Asset-navnet som det står, og som feature-locations postfixer det."""
    return frozenset({navn, get_postfixed_name(navn)})


def udvaelg_noegler(
    grupper_pr_noegle: dict[dg.AssetKey, str | None],
    *,
    grupper: tuple[str, ...] = (),
    assets: tuple[str, ...] = (),
    undtag: tuple[str, ...] = (),
) -> frozenset[dg.AssetKey]:
    """Asset-nøgler for et trin, matchet på gruppe eller navn.

    Navne matches præcist, både som de står og med denne deployments branch-
    postfix. Et præfiksmatch ville lade ``dim_Borger`` hive ``dim_Borger_historik``
    med. Gruppenavne postfixes ikke.
    """
    valgte: set[dg.AssetKey] = set()
    for noegle, gruppe in grupper_pr_noegle.items():
        navn = noegle.path[-1]
        if any(navn in _navnevarianter(u) for u in undtag):
            continue
        if gruppe in grupper or any(navn in _navnevarianter(a) for a in assets):
            valgte.add(noegle)
    return frozenset(valgte)


def _grupper_pr_noegle(repo_def: dg.RepositoryDefinition) -> dict[dg.AssetKey, str | None]:
    return {
        noegle: assets_def.group_names_by_key.get(noegle)
        for noegle, assets_def in repo_def.assets_defs_by_key.items()
    }


def _reconstructable_job(context: dg.AssetExecutionContext, job_navn: str) -> ReconstructableJob:
    """Reconstructable reference til måljobbet, som ``execute_run`` kræver.

    Samme greb som period-orkestratoren bruger: vi låner den kørende proces'
    repository-reference og beder den om måljobbet.
    """
    step_ctx = context.get_step_execution_context()
    current_job = step_ctx.plan_data.job
    if isinstance(current_job, ReconstructableJob):
        recon_repo: ReconstructableRepository = current_job.get_reconstructable_repository()
        return recon_repo.get_reconstructable_job(job_navn)
    raise dg.Failure(
        description=(
            "Kan ikke danne en reconstructable jobreference. Fuldindlæsningen skal køre i et "
            "Dagster-miljø, der understøtter proces-overdragelse (dagster dev eller produktion)."
        )
    )


# ---------------------------------------------------------------------------
# Kørsel af ét trin og ét vindue
# ---------------------------------------------------------------------------


class TrinResultat(NamedTuple):
    navn: str
    ok: bool
    run_id: str
    sekunder: int
    fejl: str


def _videresendte_tags(context: Any) -> dict[str, str]:
    """Kørslens tags, som trinnene skal arve: indeksportens og storens indstillinger."""
    tags = getattr(getattr(context, "run", None), "tags", None) or {}
    return {k: v for k, v in tags.items() if k.startswith(_VIDERESENDTE_TAG_PRAEFIKSER)}


def _koer_trin(
    context: dg.AssetExecutionContext, trin: Trin, vindue: Vindue, nr: int
) -> TrinResultat:
    repo_def = context.repository_def
    noegler = udvaelg_noegler(
        _grupper_pr_noegle(repo_def),
        grupper=trin.grupper,
        assets=trin.assets,
        undtag=trin.undtag,
    )
    if not noegler:
        raise dg.Failure(
            description=(
                f"Trin {trin.navn!r} matchede ingen assets "
                f"(grupper={trin.grupper}, assets={trin.assets}). Er navnene ændret?"
            )
        )
    job_def_eller_none = repo_def.get_implicit_job_def_for_assets(noegler)
    if job_def_eller_none is None:
        raise dg.Failure(
            description=f"Trin {trin.navn!r}: kunne ikke danne et job for {len(noegler)} assets."
        )
    job_def = job_def_eller_none
    asset_selection = noegler
    maal = f"{len(noegler)} assets"

    run_config: dict[str, Any] = {}
    if trin.mode:
        run_config = build_period_run_config(
            job_def,
            DatePartition(vindue.fra, vindue.til),
            # ALDRIG sandt. Et vindue skriver vinduets slutdato ind i de
            # checkpoints, den daglige kæde ejer. Står de på nu, og ligger
            # vinduet i 2023, rulles hele miljøet tre år tilbage, og næste
            # natkørsel spoler historikken igennem. Fuldindlæsningen skriver
            # kun sin egen markør; startpositionen på et tømt miljø er
            # seed-scriptets opgave.
            advance_checkpoint=False,
            mode=trin.mode,
        )

    context.log.info(
        "[%d/%d] %s: kører %s for %s (mode=%s)",
        nr,
        len(TRIN),
        trin.navn,
        maal,
        vindue,
        trin.mode or "standard",
    )

    dagster_run = context.instance.create_run_for_job(
        job_def=job_def,
        run_config=run_config,
        asset_selection=asset_selection,
        tags={
            **_videresendte_tags(context),
            "dagster/orchestrator": "ifdb_fhir_fuldload",
            "fuldload/trin": trin.navn,
            "fuldload/vindue": str(vindue),
            "dagster/parent_run_id": context.run_id,
            SOF_SLICE_TIME_BUDGET_MINUTES_TAG: "0",
            SOF_SLICE_MAX_RESOURCES_TAG: "0",
            SOF_BACKFILL_MIN_RATIO_TAG: "0",
        },
    )
    startet = time.monotonic()
    resultat = execute_run(
        job=_reconstructable_job(context, job_def.name),
        dagster_run=dagster_run,
        instance=context.instance,
    )
    sekunder = int(time.monotonic() - startet)
    ok = bool(resultat.success)
    fejl = "" if ok else _foerste_fejl(context, dagster_run.run_id)

    if ok:
        context.log.info(
            "[%d/%d] %s grøn på %d s (run %s).",
            nr,
            len(TRIN),
            trin.navn,
            sekunder,
            dagster_run.run_id,
        )
    else:
        context.log.error(
            "[%d/%d] %s FEJLEDE efter %d s (run %s): %s",
            nr,
            len(TRIN),
            trin.navn,
            sekunder,
            dagster_run.run_id,
            fejl or "ukendt årsag",
        )
    return TrinResultat(trin.navn, ok, dagster_run.run_id, sekunder, fejl)


def _foerste_fejl(context: dg.AssetExecutionContext, run_id: str) -> str:
    """Første fejltekst fra barnekørslen, til forælderens log og metadata.

    Uden den står der kun "ukendt årsag", og man skal selv finde barnekørslen for
    at se, hvad der gik galt. Årsagen (``cause``) er den interessante: det er dér,
    værnets egen besked står, ikke i Dagsters indpakning.
    """
    try:
        for post in context.instance.all_logs(run_id):
            haendelse = post.dagster_event
            if haendelse is None:
                continue
            if haendelse.is_step_failure and haendelse.step_failure_data:
                fejl = haendelse.step_failure_data.error
            elif haendelse.is_run_failure and haendelse.job_failure_data:
                fejl = haendelse.job_failure_data.error
            else:
                continue
            if fejl is None:
                continue
            besked = (fejl.cause.message if fejl.cause else fejl.message) or ""
            besked = " ".join(besked.split())
            if besked:
                return f"{haendelse.step_key or 'kørslen'}: {besked}"[:500]
    except Exception:  # noqa: BLE001 -- fejlteksten er pynt, ikke kontrakt
        return ""
    return ""


_DAILY_JOB_PREFIX = "ifdb_dfm_daily_job"
_ACTIVE_STATUSES = (
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
)


def _daily_run_in_progress(context: Any) -> str | None:
    """Run id of an active daily IFDB run in any code location, else None.

    Child runs are started with execute_run, so the run queue cannot keep the
    post and the daily chain apart. They write and read the same archive.
    """
    instance = getattr(context, "instance", None)
    if instance is None:
        return None
    # A daily run does not last a day; an older active one is a dead worker's
    # run left in STARTED, and must not stop every night.
    recent = datetime.now(UTC) - timedelta(hours=24)
    runs = instance.get_runs(
        filters=dg.RunsFilter(statuses=list(_ACTIVE_STATUSES), created_after=recent)
    )
    for run in runs:
        if run.job_name.startswith(_DAILY_JOB_PREFIX):
            return run.run_id
    return None


def active_fuldload_run(instance: dg.DagsterInstance, job_name: str) -> str | None:
    """Run id of an active full load run of ``job_name`` from the last day, else None."""
    # A full load stops at koer_indtil, so it never lasts a day; an older active run is a
    # dead worker's run left in STARTED and must not block every start.
    records = instance.get_run_records(
        filters=dg.RunsFilter(
            job_name=job_name,
            statuses=list(_ACTIVE_STATUSES),
            created_after=datetime.now(UTC) - timedelta(hours=24),
        ),
        limit=1,
    )
    return records[0].dagster_run.run_id if records else None


# Steps that read or write what the daily chain reads and writes.
_STEPS_EXCLUSIVE_WITH_DAILY = ("patienter", "post", "sof_dso")
_DAILY_STOP = "stopped: daily run "


def stopped_for_daily(resultater: list[TrinResultat]) -> bool:
    """True when the window stopped because the daily chain was running."""
    return bool(resultater) and resultater[-1].fejl.startswith(_DAILY_STOP)


def koer_vindue(
    context: dg.AssetExecutionContext, vindue: Vindue, config: FuldloadConfig
) -> list[TrinResultat]:
    """Kør kædens trin for ét vindue. Stopper ved første fatale fejl."""
    resultater: list[TrinResultat] = []
    for nr, trin in enumerate(TRIN, start=1):
        daily = (
            _daily_run_in_progress(context) if trin.navn in _STEPS_EXCLUSIVE_WITH_DAILY else None
        )
        if daily:
            context.log.warning(
                "%s not started: daily run %s is in progress. The window is redone next night.",
                trin.navn,
                daily,
            )
            resultater.append(TrinResultat(trin.navn, False, "", 0, f"{_DAILY_STOP}{daily}"))
            break
        resultat = _koer_trin(context, trin, vindue, nr)
        resultater.append(resultat)
        if resultat.ok:
            continue
        if _er_fatal(trin.navn, config):
            break
        context.log.warning(
            "%s var rød, men er ikke fatal. Vinduet regnes som kørt; se tallene i kørslen.",
            trin.navn,
        )
    return resultater


def _er_fatal(navn: str, config: FuldloadConfig) -> bool:
    trin = next(t for t in TRIN if t.navn == navn)
    return trin.fatal or (trin.navn == "verifikation" and config.stop_ved_roed_verifikation)


def vinduet_er_groent(resultater: list[TrinResultat], config: FuldloadConfig) -> bool:
    """Grønt = hvert fatalt trin kørte. Verifikationsporten tæller kun, hvis den skal."""
    efter_navn = {r.navn: r for r in resultater}
    for trin in TRIN:
        resultat = efter_navn.get(trin.navn)
        if resultat is None:
            return False
        if resultat.ok:
            continue
        if _er_fatal(trin.navn, config):
            return False
    return True


# ---------------------------------------------------------------------------
# Asset
# ---------------------------------------------------------------------------


def _metadata(
    vinduer: list[tuple[Vindue, list[TrinResultat]]],
    markoer_foer: str | None,
    markoer_efter: str | None,
    status: str,
) -> dict[str, Any]:
    linjer = [
        "| Vindue | Trin | Status | Minutter | Kørsel |",
        "|---|---|---|---|---|",
    ]
    for vindue, resultater in vinduer:
        for r in resultater:
            linjer.append(
                f"| {vindue} | {r.navn} | {'grøn' if r.ok else 'FEJL'} | "
                f"{round(r.sekunder / 60, 1)} | {r.run_id[:8]} |"
            )
    return {
        "status": dg.MetadataValue.text(status),
        "vinduer_kørt": dg.MetadataValue.int(len(vinduer)),
        "vinduer": dg.MetadataValue.text(", ".join(str(v) for v, _ in vinduer) or "-"),
        "markør_før": dg.MetadataValue.text(markoer_foer or "(ingen)"),
        "markør_efter": dg.MetadataValue.text(markoer_efter or "(ingen)"),
        "minutter_i_alt": dg.MetadataValue.float(
            round(sum(r.sekunder for _, rs in vinduer for r in rs) / 60, 1)
        ),
        "forløb": dg.MetadataValue.md("\n".join(linjer) if vinduer else "_intet vindue kørt_"),
    }


@dg.asset(
    name="ifdb_fhir_fuldload",
    group_name="dfm_operations",
    compute_kind="python",
    description=(
        "Meta-asset: kører hele IFDB -> FHIR-kæden for ét vindue ad gangen (arkiv, RIS, "
        "window_reconciliation, post, SOF, DSO, store, verifikation) og rykker sin egen "
        "markør, når vinduet er "
        "grønt. Uden config tager den selv den ældste måned, der mangler, og melder "
        "færdig, når historikken er inde."
    ),
)
def ifdb_fhir_fuldload(
    context: dg.AssetExecutionContext,
    config: FuldloadConfig,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    markoer_foer = dfm.get_checkpoint(MARKOER_SCHEMA, MARKOER_TABEL, scope=MARKOER_SCOPE)
    eksplicit = vindue_fra_config(config)
    deadline = beregn_deadline(config.koer_indtil, _dansk_tid())

    def _afbryd() -> None:
        context.log.error(
            "Tidsbudgettet %s er brugt midt i et vindue. Kørslen afbrydes, så den daglige kæde "
            "ikke deler arkiv, lås og DSO med den. Markøren er ikke rykket.",
            config.koer_indtil,
        )
        _thread.interrupt_main()

    vagt = deadline_vagt(deadline, _dansk_tid(), _afbryd)
    if vagt is not None:
        vagt.start()
    try:
        return _koer_natten(context, config, dfm, markoer_foer, eksplicit, deadline)
    finally:
        if vagt is not None:
            vagt.cancel()


def _koer_natten(
    context: dg.AssetExecutionContext,
    config: FuldloadConfig,
    dfm: DfmManager,
    markoer_foer: str | None,
    eksplicit: Vindue | None,
    deadline: datetime | None,
) -> dg.MaterializeResult:
    markoer = markoer_foer
    kørte: list[tuple[Vindue, list[TrinResultat]]] = []
    laengste_vindue = 0.0

    while True:
        vindue = eksplicit or naeste_vindue(markoer, date.today())
        if vindue is None:
            context.log.info(
                "Historikken er inde til og med %s. Den daglige kæde tager over herfra.", markoer
            )
            return dg.MaterializeResult(metadata=_metadata(kørte, markoer_foer, markoer, "færdig"))

        context.log.info(
            "Fuldindlæsning: vindue %s (markør står på %s).", vindue, markoer or "ingen"
        )
        vindue_start = time.monotonic()
        resultater = koer_vindue(context, vindue, config)
        laengste_vindue = max(laengste_vindue, time.monotonic() - vindue_start)
        kørte.append((vindue, resultater))

        if stopped_for_daily(resultater):
            if len(kørte) == 1:
                raise dg.Failure(
                    description=(
                        f"Vinduet {vindue} blev stoppet af en daglig kørsel, før nattens første "
                        "vindue var færdigt. Fuldindlæsningen starter normalt først, når den "
                        "daglige kæde er færdig, så tjek, om den hænger."
                    ),
                    metadata=_metadata(
                        kørte, markoer_foer, markoer, "stoppet for den daglige kæde"
                    ),
                )
            return dg.MaterializeResult(
                metadata=_metadata(kørte, markoer_foer, markoer, "stoppet for den daglige kæde")
            )
        if not vinduet_er_groent(resultater, config):
            fejlede = next((r for r in resultater if not r.ok and _er_fatal(r.navn, config)), None)
            raise dg.Failure(
                description=(
                    f"Vinduet {vindue} fejlede i trin {fejlede.navn if fejlede else '?'}. "
                    "Markøren er ikke rykket, så vinduet tages forfra ved næste kørsel."
                ),
                metadata=_metadata(kørte, markoer_foer, markoer, "fejlet"),
            )

        if config.ryk_markoer:
            dfm.set_checkpoint(
                MARKOER_SCHEMA,
                MARKOER_TABEL,
                value=vindue.til,
                mode="period",
                scope=MARKOER_SCOPE,
            )
            markoer = vindue.til
            context.log.info("Vinduet %s er grønt. Markøren står nu på %s.", vindue, markoer)
        else:
            context.log.warning("ryk_markoer er slået fra: markøren står stadig på %s.", markoer)

        if eksplicit or len(kørte) >= max(1, config.max_vinduer):
            break
        if deadline is not None and not naeste_vindue_naas(_dansk_tid(), laengste_vindue, deadline):
            context.log.info(
                "Næste vindue når ikke at blive færdigt før %s (det længste tog %d min). "
                "Stopper efter %d vindue(r).",
                config.koer_indtil,
                laengste_vindue // 60,
                len(kørte),
            )
            break

    return dg.MaterializeResult(metadata=_metadata(kørte, markoer_foer, markoer, "vindue kørt"))


ifdb_fhir_fuldload_job = dg.define_asset_job(
    name="ifdb_fhir_fuldload_job",
    selection=dg.AssetSelection.assets("ifdb_fhir_fuldload"),
    description=(
        "Fuldindlæsning af IFDB -> FHIR-kæden, ét vindue pr. kørsel. Sæt den på et natligt "
        "skema: den tager selv den ældste måned, der mangler, og melder færdig, når "
        "historikken er inde."
    ),
)
