# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Operatørudløst nulstilling af SFI/procedure/diagnose-sporet før genkørsel.

Tømmer de AFLEDTE lag for grenens ressourcefamilier og sletter deres
DFM-checkpoints, så næste kørsel genopbygger alt fra arkivet:

    1. Aidbox (Postgres):  condition/observation/procedure/questionnaire/
                           questionnaireresponse + deres _history-tvillinger.
    2. Staging (MSSQL):    de flade fhir.*-familietabeller + SFI-lageret.
    3. DSO (MSSQL):        samme fhir.*-kopier + SFI-lageret.
    4. Checkpoints:        meta.DFMState-rækkerne (staging) for de seks
                           fhir_server-poster, de ni SOF-loads, DSO-syncens
                           ni rækker (scope 'dso') og SFI-lagerets række
                           (scope 'sfi_store') SLETTES ("aldrig kørt").
                           Kapacitets-checkpointet slettes altid, fordi
                           observation-tabellen tømmes med, se nedenfor.

Arkivet (ext/arc) er kilden til genopbygningen og RØRES IKKE — derfor heller
ingen seed: et slettet checkpoint får næste kørsel til at læse hele arkivet,
selvbegrænset af arkivets indhold. meta.DFMRunLog bevares som revisionsspor.

Rækkefølgen Aidbox -> staging -> DSO er afgørende: tømmes kun MSSQL, fylder
næste SOF-kørsel de flade tabeller op igen fra de gamle ressourcer.

Sikkerhedsspærrer: bekræftelsesstreng, dry_run som default, produktions-
navnespærre, server-pind (en rigtig kørsel skal navngive præcis de servere,
dry-run'en viste — databasenavnene er ens på tværs af miljøer, kun
servernavnet skiller dev fra QA), aktive-kørsler-tjek, og identifier-
system-tjekket der FEJLER, hvis en Aidbox-tabel indeholder ressourcer fra
en pipeline uden for allow-listen — så antagelsen "tabellen er vores"
bevises, ikke antages. Alle identiteter verificeres FØR første sletning.

Efter en rigtig kørsel: kør fhir_sof_infrastructure (genbekræfter
ViewDefinitions efter TRUNCATE), derefter de seks fhir_server-jobs med
mode=full_load — inkl. ServiceRequest, der ejer henvisningsdiagnoserne i
condition-tabellen — så SOF-loads, DSO-sync og DQA. Se genkørselsdrejebogen.
"""

import dagster as dg
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import QUESTIONNAIRE_IDENTIFIER_SYSTEM
from pydantic import Field
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
)

BEKRAEFTELSE = "NULSTIL-SFI-DEV"

# Aidbox-grundtabeller for grenens familier. Patient/Encounter/EpisodeOfCare/
# Location/Organization/ServiceRequest/DocumentReference/Appointment/CarePlan
# er andre spors data og må ALDRIG optræde her.
AIDBOX_RESSOURCETABELLER = (
    "condition",
    "observation",
    "procedure",
    "questionnaire",
    "questionnaireresponse",
)

# De flade familietabeller. Navnene er samtidig SOF-checkpointernes og
# DSO-syncens table_name i meta.DFMState (schema_name='fhir').
FLADE_FAMILIETABELLER = (
    "Condition",
    "Condition_SupplementaryCode",
    "Observation",
    "Procedure",
    "Procedure_SupplementaryCode",
    "Questionnaire",
    "Questionnaire_item",
    "QuestionnaireResponse",
    "QuestionnaireResponse_item",
)

# Afledt forbrugerlager — materialiseret, med eget DFM-checkpoint (scope
# 'sfi_store'). Tømmes hvor tabellen findes (DDL'en er rullet ud i både staging
# og DSO).
SFI_STORE_TABEL = "SFI_datapoint_store"

# fhir_server-DFM-posterne (arc -> Aidbox), præcis som de hedder i
# meta.DFMState (schema_name='fhir_server'). ServiceRequest er med, fordi det
# spor ejer henvisningsdiagnoserne fra billeddiagnostik — den femte diagnosevej
# i fhir.Condition: tømmes condition-tabellen, skal ServiceRequest-jobbet
# genposte dem, og det slettede checkpoint sikrer det, selv hvis full_load-
# kørslen glemmes. Sporets egne flader (fhir.ServiceRequest*) røres ikke.
# De øvrige ti poster (Patient, Encounter, ...) er andre spor og røres ikke.
FHIR_SERVER_CHECKPOINTS = (
    "Aktivitetsresultat_SFI",
    "Condition_Diagnose",
    "Condition_PasDiagnose",
    "Procedure_AdminProc",
    "Procedure_RisYdelse",
    "ServiceRequest",
)

# fhir_server-poster for spor, hvis ressourcer ryger med i tømningen, fordi
# de deler Aidbox-tabel med os, men som ikke er vores spor og derfor aldrig
# seedes: checkpointet SLETTES altid, så sporets næste delta-kørsel læser hele
# sit arkiv igen. Kapacitetsobservationerne bygges af senge/kapacitet-sporet.
FHIR_SERVER_CHECKPOINTS_UDEN_SEED = ("Observation_WardUnitCapacity",)

# Identifier-systemer pr. Aidbox-tabel. Preflight aggregerer ALLE systemer i
# tabellen og fejler ved et ukendt — dukker et femte diagnosespor eller en
# fremmed pipeline op, skal listen udvides bevidst, ikke overskrives stille.
IDENTIFIER_ALLOWLIST = {
    # De fem diagnoseveje fra beslutningsloggen — vej 5 (henvisningsdiagnoser,
    # ris_ydelse:diagnose) bygges af ServiceRequest-sporet, se checkpoint-listen.
    "condition": frozenset(
        {
            SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
            SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
            SYSTEM_IFDB_DIAGNOSE,
            SYSTEM_IFDB_PAS_DIAGNOSE,
            SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
        }
    ),
    "observation": frozenset(
        {
            SYSTEM_IFDB_AKTIVITETSRESULTAT,
            SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
            # Senge/kapacitet-sporets data fra dets egen feature-gren
            # (konstanten findes ikke på denne gren). Besluttet at de ryger
            # med i tømningen: vores genkørsel genopbygger dem IKKE — sporets
            # eget checkpoint slettes (FHIR_SERVER_CHECKPOINTS_UDEN_SEED), så
            # dets næste kørsel genindlæser dem fra arkivet.
            "urn:source:ifdb:sengeenhed_kapacitet",
        }
    ),
    "procedure": frozenset(
        {
            SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
            SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
            SYSTEM_IFDB_RIS_YDELSE,
        }
    ),
    "questionnaire": frozenset({QUESTIONNAIRE_IDENTIFIER_SYSTEM}),
    "questionnaireresponse": frozenset({SYSTEM_IFDB_AKTIVITETSRESULTAT}),
}

# Kørende jobs med et af disse ord i navnet blokerer en rigtig kørsel:
# de læser eller skriver de lag, der er ved at blive tømt.
RELEVANTE_JOBORD = (
    "aktivitetsresultat",
    "sfi",
    "ris_ydelse",
    "administrative",
    "adminproc",
    "diagnose",
    "condition",
    "procedure",
    "observation",
    "questionnaire",
    "sof",
    "dso",
    "dqa",
)


def aidbox_arbejdsliste(grundtabeller=AIDBOX_RESSOURCETABELLER):
    """Grundtabeller + deres _history-tvillinger, i truncate-rækkefølge."""
    tabeller = []
    for t in grundtabeller:
        tabeller.append(t)
        tabeller.append(f"{t}_history")
    return tabeller


def pg_truncate_statement(tabeller):
    """Én TRUNCATE for alle tabeller: én transaktion, ingen indbyrdes FK-blokering.

    CASCADE er bevidst udeladt — peger en tabel uden for arbejdslisten på en af
    dem, skal det fejle højlydt, ikke rive ukendte tabeller med. Citaterne gør
    navne som "procedure" og "condition" ufarlige som nøgleord.
    """
    citeret = ", ".join(f'"{t}"' for t in tabeller)
    return f"TRUNCATE TABLE {citeret}"


def pg_identifier_system_sql(tabel):
    """Aggregér identifier-systemer i én Aidbox-tabel (fuld skanning, med vilje)."""
    return (
        "SELECT ident->>'system' AS system, COUNT(*) AS antal "
        f"FROM \"{tabel}\" t, LATERAL jsonb_array_elements(t.resource->'identifier') AS ident "
        "GROUP BY 1 ORDER BY 2 DESC"
    )


def pg_uden_identifier_sql(tabel):
    """Ressourcer helt uden identifier er usynlige for system-tjekket — tæl dem."""
    return f"SELECT COUNT(*) FROM \"{tabel}\" WHERE resource->'identifier' IS NULL"


def mssql_navneliste(navne):
    """Navne som citeret IN-liste. Navnene er modulkonstanter, aldrig input."""
    return ", ".join(f"N'{n}'" for n in navne)


def dfmstate_where(kun_sof=False):
    """WHERE-klausul for de checkpoint-rækker i meta.DFMState, der SLETTES.

    Altid med: de ni SOF-rækker (scope 'staging'), DSO-syncens ni rækker for de
    samme tabeller (scope 'dso' — vandmærkerne bor i stagings meta.DFMState
    siden DSO-unificeringen), SFI-lagerets række (scope 'sfi_store') og de
    fhir_server-poster, der aldrig seedes.

    ``kun_sof`` bruges ved seeding: de seedbare fhir_server-rækker slettes så
    ikke, men MERGE'S til seed-værdien, så der aldrig findes et vindue, hvor et
    manglende checkpoint kan udløse en fuld arkivlæsning.
    """
    familie = mssql_navneliste(FLADE_FAMILIETABELLER)
    sof = f"([scope] = N'staging' AND [schema_name] = N'fhir' AND [table_name] IN ({familie}))"
    dso = f"([scope] = N'dso' AND [schema_name] = N'fhir' AND [table_name] IN ({familie}))"
    lager = (
        f"([scope] = N'sfi_store' AND [schema_name] = N'fhir' "
        f"AND [table_name] = N'{SFI_STORE_TABEL}')"
    )
    uden_seed = (
        "([scope] = N'staging' AND [schema_name] = N'fhir_server' AND [table_name] IN "
        f"({mssql_navneliste(FHIR_SERVER_CHECKPOINTS_UDEN_SEED)}))"
    )
    if kun_sof:
        return f"WHERE {sof} OR {dso} OR {lager} OR {uden_seed}"
    fhir_server = (
        "([scope] = N'staging' AND [schema_name] = N'fhir_server' AND [table_name] IN "
        f"({mssql_navneliste(FHIR_SERVER_CHECKPOINTS)}))"
    )
    return f"WHERE {fhir_server} OR {sof} OR {dso} OR {lager} OR {uden_seed}"


def _mssql_identitet(cursor):
    cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
    server, database = cursor.fetchone()
    return str(server), str(database)


def _kraev_server_pind(logger, pinde, identiteter, dry_run):
    """Bind en rigtig kørsel til navngivne servere — dry-run lærer dig navnene.

    Dev og QA skelnes ikke af databasenavnene (healthpipe_staging hedder det samme
    begge steder), kun af serverens navn. Pinden står i den git-ekskluderede
    launch-fil, aldrig i koden. En sat pind håndhæves også i dry-run, så en
    forkert pind opdages billigt.
    """
    for lag, pind in pinde.items():
        server = identiteter[lag][0]
        if not pind:
            if dry_run:
                logger.warning(
                    f"Server-pind mangler for {lag}: sæt bekraeft_{lag}_server = "
                    f"'{server}' i launch-configen, før der køres rigtigt."
                )
                continue
            raise dg.Failure(
                description=(
                    f"bekraeft_{lag}_server er ikke sat. En rigtig kørsel skal "
                    f"navngive serveren fra dry-run'en (forbindelsen rammer nu "
                    f"'{server}'), så dev og QA aldrig kan forveksles."
                )
            )
        if pind.strip().lower() != server.strip().lower():
            raise dg.Failure(
                description=(
                    f"Server-pinden for {lag} siger '{pind}', men forbindelsen "
                    f"rammer '{server}'. Forkert miljø — eller gendeployet "
                    "database-container; verificér, og ret pinden bevidst."
                )
            )


def _afvis_produktionslignende(server, database):
    for navn in (server, database):
        n = navn.lower()
        if "prod" in n or n.endswith("_p"):
            raise dg.Failure(
                description=(
                    f"Server- eller databasenavnet [{server}]/[{database}] ligner "
                    "produktion. Nulstillingen er kun til udviklingsmiljøet."
                )
            )


def _eksisterende_fhir_tabeller(cursor, kandidater):
    cursor.execute(
        "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "
        f"WHERE s.name = N'fhir' AND t.name IN ({mssql_navneliste(kandidater)})"
    )
    fundne = {row[0] for row in cursor.fetchall()}
    return [t for t in kandidater if t in fundne]


def _mssql_taelling(cursor, tabeller):
    tal = {}
    for t in tabeller:
        cursor.execute(f"SELECT COUNT_BIG(*) FROM [fhir].[{t}]")
        tal[t] = int(cursor.fetchone()[0] or 0)
    return tal


def _mssql_toem(context, cursor, tabeller, dry_run, etiket):
    """FK-bevidst TRUNCATE/DELETE pr. tabel — SQL Server blokerer TRUNCATE på
    en tabel, der er mål for en fremmednøgle, også en deaktiveret."""
    for t in tabeller:
        cursor.execute(
            "SELECT COUNT(*) FROM sys.foreign_keys WHERE referenced_object_id = OBJECT_ID(?)",
            (f"[fhir].[{t}]",),
        )
        har_fk = int(cursor.fetchone()[0] or 0) > 0
        metode = "DELETE FROM" if har_fk else "TRUNCATE TABLE"
        if dry_run:
            context.log.info(f"  [dry run] {metode} [fhir].[{t}]  ({etiket})")
        else:
            cursor.execute(f"{metode} [fhir].[{t}]")
            context.log.info(f"  {metode} [fhir].[{t}]  ({etiket})")


class SfiGenkoerselConfig(dg.Config):
    bekraeft: str = Field(
        default="",
        description=f"Skal være præcis '{BEKRAEFTELSE}'. Spærren mod refleks-launches.",
    )
    dry_run: bool = Field(
        default=True,
        description="Tæl, verificér og log alt — men slet intet. Kør altid denne først.",
    )
    bekraeft_staging_server: str = Field(
        default="",
        description="@@SERVERNAME for staging, som dry-run viste. Krævet ved dry_run=false: "
        "databasenavnene er ens på tværs af miljøer, kun servernavnet skiller dev fra QA.",
    )
    bekraeft_dso_server: str = Field(
        default="",
        description="@@SERVERNAME for DSO, som dry-run viste. Krævet ved dry_run=false.",
    )
    seed_fhir_server_dage: int = Field(
        default=0,
        description="0 (default): fhir_server-checkpoints SLETTES, og næste kørsel læser hele "
        "arkivet — den fulde genkørsel. >0: checkpoints seedes i stedet til nu minus N døgn "
        "(MSSQL-lokaltid via usp_dfm_reset), så næste delta henter præcis det vindue — og en "
        "natlig kørsel aldrig kan udløse en utilsigtet fuld arkivlæsning. SOF-checkpoints "
        "slettes altid; de er selvbegrænsende af Aidbox' indhold.",
    )


@dg.asset(
    name="sfi_genkoersel_nulstilling",
    group_name="fhir_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging", "mssql_target_dso", "fhir_database"},
    description=(
        "Operatørudløst: tøm Aidbox/staging/DSO for SFI-, procedure- og diagnose-"
        "familierne og slet deres DFM-checkpoints, så næste kørsel genopbygger alt "
        "fra arkivet. Kør dry_run=true først, og pause de daglige schedules i vinduet."
    ),
)
def sfi_genkoersel_nulstilling(
    context: dg.AssetExecutionContext,
    config: SfiGenkoerselConfig,
) -> dg.MaterializeResult:
    if config.bekraeft != BEKRAEFTELSE:
        raise dg.Failure(
            description=(
                f"bekraeft skal være præcis '{BEKRAEFTELSE}' — fik {config.bekraeft!r}. "
                "Spærren findes, fordi jobbet tømmer tre lag på én gang."
            )
        )
    dry_run = config.dry_run
    tilstand = "DRY RUN (intet slettes)" if dry_run else "*** SLETTER DATA ***"
    context.log.info(f"sfi_genkoersel_nulstilling: {tilstand}")
    res = context.resources
    rapport: dict = {"dry_run": dry_run}

    # ---- Preflight A: aktive kørsler --------------------------------------
    # En samtidig pipeline-kørsel kan poste til de tabeller, der tømmes, eller
    # læse halvtomme lag. Dry-run nøjes med at logge.
    statusser = [
        dg.DagsterRunStatus.QUEUED,
        dg.DagsterRunStatus.NOT_STARTED,
        dg.DagsterRunStatus.STARTING,
        dg.DagsterRunStatus.STARTED,
        dg.DagsterRunStatus.CANCELING,
    ]
    aktive = [
        record.dagster_run
        for record in context.instance.get_run_records(dg.RunsFilter(statuses=statusser))
        if record.dagster_run.run_id != context.run_id
    ]
    relevante = [
        run.job_name
        for run in aktive
        if any(ord in run.job_name.lower() for ord in RELEVANTE_JOBORD)
    ]
    for run in aktive:
        context.log.info(f"Aktiv kørsel: {run.job_name} ({run.run_id})")
    if relevante and not dry_run:
        raise dg.Failure(
            description=(
                f"{len(relevante)} aktive kørsler rører de berørte lag: "
                f"{sorted(set(relevante))}. Pause schedules og vent, før nulstillingen køres."
            )
        )
    rapport["aktive_relevante_koersler"] = sorted(set(relevante))

    # ---- Preflight B: server-identiteter — FØR noget som helst tømmes ------
    # Scriptet bærer ingen forbindelser selv; ressourcerne arver miljøet fra
    # code-locationen. Navnespærren afviser produktion, men QA hedder ikke
    # «prod» — derfor pinden. Verificeres her, før Aidbox-tømningen, så en
    # rigtig kørsel aldrig når at slette noget mod et uventet miljø.
    identiteter = {}
    for lag, resource in (("staging", res.mssql_target_staging), ("dso", res.mssql_target_dso)):
        with resource.get_connection() as conn:
            server, database = _mssql_identitet(conn.cursor())
        _afvis_produktionslignende(server, database)
        identiteter[lag] = (server, database)
        context.log.info(f"{lag}: {server}/{database}")
        rapport[f"{lag}_identitet"] = f"{server}/{database}"
    _kraev_server_pind(
        context.log,
        {"staging": config.bekraeft_staging_server, "dso": config.bekraeft_dso_server},
        identiteter,
        dry_run,
    )

    # ---- Preflight D: seed-forudsætning — FØR noget tømmes ------------------
    # usp_dfm_reset bruges først i fase 2, men mangler proceduren, skal det
    # opdages her og ikke efter, at Aidbox og staging er tømt.
    if config.seed_fhir_server_dage > 0:
        with res.mssql_target_staging.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("SELECT OBJECT_ID(N'[meta].[usp_dfm_reset]', N'P')")
            if cur.fetchone()[0] is None:
                raise dg.Failure(
                    description="[meta].[usp_dfm_reset] findes ikke — kan ikke seede "
                    "checkpoints. Rul DFM-metaobjekterne ud, eller kør uden seed."
                )

    # ---- Preflight C + fase 1: Aidbox -------------------------------------
    with res.fhir_database.get_connection() as pg:
        with pg.cursor() as cur:
            # Preflighten skanner millioner af rækker; ingen kunstig frist.
            cur.execute("SET statement_timeout = 0")
            cur.execute(
                "SELECT current_database(), COALESCE(inet_server_addr()::text, '(socket)'), "
                "inet_server_port()"
            )
            db, host, port = cur.fetchone()
            # Aidbox hedder 'aidbox' i alle miljøer — navnet beviser intet, så
            # identiteten logges til operatørens egne øjne i stedet for en spærre.
            context.log.info(f"Aidbox: database={db} host={host} port={port}")
            rapport["aidbox_identitet"] = f"{db}@{host}:{port}"

            kandidater = aidbox_arbejdsliste()
            cur.execute(
                "SELECT c.relname, c.reltuples::bigint FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname = ANY(%s)",
                (list(kandidater),),
            )
            fundet = {navn: est for navn, est in cur.fetchall()}
            aidbox_tabeller = [t for t in kandidater if t in fundet]
            rapport["aidbox_estimater"] = {t: int(fundet[t]) for t in aidbox_tabeller}
            for t in aidbox_tabeller:
                context.log.info(f"Aidbox {t}: ~{int(fundet[t])} rækker (estimat)")

            # Identifier-spærren: tabellerne SKAL udelukkende rumme vores familier.
            # Fund SAMLES på tværs af alle tabeller, så én dry-run giver det
            # komplette beslutningsgrundlag i stedet for ét fund pr. kørsel.
            systemtal: dict = {}
            identifier_fund: list[str] = []
            for t in AIDBOX_RESSOURCETABELLER:
                if t not in fundet:
                    continue
                cur.execute(pg_identifier_system_sql(t))
                fordeling = {row[0]: int(row[1]) for row in cur.fetchall()}
                cur.execute(pg_uden_identifier_sql(t))
                uden = int(cur.fetchone()[0] or 0)
                systemtal[t] = {"systemer": fordeling, "uden_identifier": uden}
                for system, antal in fordeling.items():
                    context.log.info(f"Aidbox {t}: {antal} rækker med system {system}")
                ukendte = set(fordeling) - IDENTIFIER_ALLOWLIST[t]
                if ukendte:
                    identifier_fund.append(
                        f"'{t}' rummer systemer uden for allow-listen: {sorted(ukendte)}"
                    )
                if uden:
                    identifier_fund.append(f"'{t}' rummer {uden} ressourcer helt uden identifier")
            rapport["aidbox_identifier_systemer"] = systemtal
            rapport["identifier_fund"] = identifier_fund
            for fund in identifier_fund:
                context.log.error(f"Identifier-spærre: {fund}")
            if identifier_fund and not dry_run:
                # Rigtig kørsel: stop FØR første sletning. En anden pipeline
                # ejer data i tabellerne — udvid allow-listen bevidst, eller
                # snævr tømningen ind, men aldrig stiltiende.
                raise dg.Failure(
                    description="Identifier-spærren stoppede nulstillingen: "
                    + "; ".join(identifier_fund)
                )

            if aidbox_tabeller:
                statement = pg_truncate_statement(aidbox_tabeller)
                if dry_run:
                    context.log.info(f"[dry run] {statement}")
                else:
                    context.log.info(statement)
                    cur.execute(statement)
                    # Uden ANALYZE tror planlæggeren, tabellerne stadig er store,
                    # og genopbygningens første kørsler får dårlige planer.
                    for t in aidbox_tabeller:
                        cur.execute(f'ANALYZE "{t}"')
        if not dry_run:
            pg.commit()

    # ---- Fase 2: staging --------------------------------------------------
    with res.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        staging_tabeller = _eksisterende_fhir_tabeller(
            cur, FLADE_FAMILIETABELLER + (SFI_STORE_TABEL,)
        )
        rapport["staging_raekker_foer"] = _mssql_taelling(cur, staging_tabeller)
        for t, n in rapport["staging_raekker_foer"].items():
            context.log.info(f"staging fhir.{t}: {n} rækker")
        _mssql_toem(context, cur, staging_tabeller, dry_run, "staging")

        # Checkpoints: SELECT-kvittering før ændring; DFMRunLog bevares.
        cur.execute(
            "SELECT [schema_name], [table_name], [checkpoint_value], [last_mode] "
            f"FROM [meta].[DFMState] {dfmstate_where()} ORDER BY [schema_name], [table_name]"
        )
        checkpoints = [f"{r[0]}.{r[1]} = {r[2]} ({r[3]})" for r in cur.fetchall()]
        for linje in checkpoints:
            context.log.info(f"DFMState: {linje}")
        rapport["dfm_checkpoints_foer"] = checkpoints

        seed_dage = config.seed_fhir_server_dage
        where = dfmstate_where(kun_sof=seed_dage > 0)
        if seed_dage > 0:
            # Seed i stedet for sletning på fhir_server-laget: MERGE via
            # usp_dfm_reset (revisionsspor i DFMRunLog, mode='reset') — der
            # findes aldrig et vindue uden checkpoint, så hverken vores eller
            # nattens kørsler kan udløse en fuld arkivlæsning ved et uheld.
            # Værdien beregnes på SQL-serveren: samme ur som dw_timestamp_update.
            # Procedurens eksistens er verificeret i preflight D.
            cur.execute(
                f"SELECT CONVERT(NVARCHAR(23), DATEADD(day, -{int(seed_dage)}, SYSDATETIME()), 121)"
            )
            seed_vaerdi = cur.fetchone()[0]
            rapport["seed_vaerdi"] = seed_vaerdi
            for navn in FHIR_SERVER_CHECKPOINTS:
                if dry_run:
                    context.log.info(f"[dry run] ville seede fhir_server.{navn} -> {seed_vaerdi}")
                else:
                    cur.execute(
                        "EXEC [meta].[usp_dfm_reset] @schema_name=N'fhir_server', "
                        "@table_name=?, @new_value=?, @reason=?, @scope=N'staging'",
                        (navn, seed_vaerdi, f"Genkørsel: seed til {seed_dage} døgns vindue"),
                    )
                    context.log.info(f"Seedet fhir_server.{navn} -> {seed_vaerdi}")
        if dry_run:
            context.log.info(f"[dry run] DELETE FROM [meta].[DFMState] {where}")
        else:
            cur.execute(f"DELETE FROM [meta].[DFMState] {where}")
            context.log.info(f"DFMState: {cur.rowcount} checkpoint-rækker slettet")
            conn.commit()

    # ---- Fase 3: DSO ------------------------------------------------------
    with res.mssql_target_dso.get_connection() as conn:
        cur = conn.cursor()
        dso_tabeller = _eksisterende_fhir_tabeller(cur, FLADE_FAMILIETABELLER + (SFI_STORE_TABEL,))
        rapport["dso_raekker_foer"] = _mssql_taelling(cur, dso_tabeller)
        for t, n in rapport["dso_raekker_foer"].items():
            context.log.info(f"DSO fhir.{t}: {n} rækker")
        _mssql_toem(context, cur, dso_tabeller, dry_run, "DSO")

    context.log.info(
        "Nulstilling %s. Næste skridt: fhir_sof_infrastructure, derefter de seks "
        "fhir_server-jobs med mode=full_load, SOF-loads, DSO-sync og DQA.",
        "gennemtænkt (dry run)" if dry_run else "gennemført",
    )
    if identifier_fund:
        # Dry-run gennemfører alle tællinger først (tallene står i loggen),
        # men ender som FAILURE, så et fund aldrig kan forveksles med et go.
        raise dg.Failure(
            description="Identifier-spærren fandt fremmede data (alle tal er "
            "logget ovenfor): " + "; ".join(identifier_fund)
        )
    return dg.MaterializeResult(
        metadata={
            "status": dg.MetadataValue.text("dry_run" if dry_run else "udført"),
            "rapport": dg.MetadataValue.json(rapport),
        }
    )


sfi_genkoersel_nulstilling_job = dg.define_asset_job(
    name="sfi_genkoersel_nulstilling_job",
    selection=dg.AssetSelection.assets("sfi_genkoersel_nulstilling"),
    description=(
        "Tøm SFI/procedure/diagnose-familiernes afledte lag og slet deres "
        "DFM-checkpoints før genkørsel (operatørudløst, dry_run som default)."
    ),
    tags={"domain": "fhir", "kind": "operations"},
)
