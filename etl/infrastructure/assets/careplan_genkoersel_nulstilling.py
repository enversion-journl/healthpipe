# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Operatørudløst nulstilling af CarePlan-sporet før genkørsel.

Tømmer de AFLEDTE lag for Alle Markoerer-CarePlanen og sletter dens
DFM-checkpoints, så næste kørsel genopbygger alt fra arkivet:

    1. Checkpoints:        meta.DFMState-rækkerne (staging) for fhir_server.
                           CarePlan, de to SOF-loads og DSO-syncens to rækker
                           (scope 'dso') nulstilles til NULL ("aldrig kørt")
                           via meta.usp_dfm_reset.
    2. Aidbox (Postgres):  careplan + careplan_history.
    3. Staging (MSSQL):    fhir.CarePlan_Activity og fhir.CarePlan.
    4. DSO (MSSQL):        de samme to kopier.

Arkivet (arc.ifdb_eventmarker) er kilden til genopbygningen og RØRES IKKE —
derfor heller ingen seed: et nulstillet checkpoint får næste kørsel til at læse
hele arkivet, selvbegrænset af arkivets indhold.

Nulstillingen går gennem meta.usp_dfm_reset frem for en DELETE, fordi
proceduren findes til præcis det: den er atomisk, den udleder target_type af
schema_name med samme afbildning som DfmManager, og den skriver en
reset-række i meta.DFMRunLog, så checkpointets spring kan spores bagefter som
ethvert andet. DfmManager læser en manglende række og en NULL-værdi ens
(get_checkpoint returnerer None i begge tilfælde), så det koster intet at
beholde revisionssporet.

Rækkefølgen Aidbox -> staging -> DSO er afgørende: tømmes kun MSSQL, fylder
næste SOF-kørsel de flade tabeller op igen fra de gamle ressourcer.

Checkpointerne nulstilles FØRST, så enhver halv kørsel fejler højt. Gik
tømningen forud, ville en kørsel, der døde midtvejs, efterlade tomme tabeller
med intakte checkpoints: spærren i ``fhir_server_careplan_dfm`` tæller da nul
gamle rækker og lader natten køre som en almindelig delta, der kun genopbygger
de patienter, der tilfældigvis har ændringer. Med checkpointerne nulstillet
først læser næste kørsel hele arkivet og møder spærren, så længe der er gamle
rækker tilbage i ``fhir.CarePlan``.

Hvorfor
-------
Alle Markoerer-CarePlanen var nøglet på ``EVENTMARKER.PATIENT_ID``. IFDB
pensionerer et PATIENT_ID og udsteder et nyt for samme CPR, så personen fik en
CarePlan mere, mens den første blev uopnåelig for pipelinen — to levende
ressourcer, hvis aktivitetsrækker kolliderer i SOF-barnets MERGE ved ethvert
bredt vindue. Nøglen er nu FHIR-patient-id'et, som dependency-resolverens
CPR-bro gør ens for begge IFDB-id'er.

Hver eksisterende ressource bærer derfor en nøgle, pipelinen aldrig slår op
igen. De ryddes og genopbygges i stedet for at blive migreret på plads. Intet i
repoet producerer andre CarePlans end ``alle_markoerer`` — kræftpakke,
udredningsret, behandlingsret og maksimale-ventetider er konstanter uden
producent — så et full load genskaber det komplette sæt, og levnene fra den
pensionerede logik ryger med. SQL-oprydningen af dem
(``20260410_cleanup_kraeftpakker_legacy.sql``) rørte aldrig FHIR-ressourcerne.

Hvorfor et asset og ikke en migrationsfil
-----------------------------------------
``ddl/migrations/`` afvikles automatisk af BÅDE ``staging_migrations`` og
``dso_migrations`` mod hver sin MSSQL-database, i filnavnsorden og fail-fast. En
Postgres-fil dér køres mod MSSQL og tager infrastruktur-DAG'en ned. Og selv
MSSQL-halvdelen hører ikke hjemme der: den ville affyres når infrastruktur næste
gang materialiseres, muligvis før FHIR-serveren er ryddet — hvorefter
natkørslen genudfylder de flade tabeller fra de gamle ressourcer, og
tracker-rækken i ``meta.MigrationHistory`` sørger for at filen aldrig kører
igen.

Køres i HVERT miljø. Identifieren ændrer sig for alle eksisterende ressourcer,
så de kan ikke bæres over — Dev og QA skal begge tømmes og full-loades, i samme
rækkefølge og med samme værktøj. Der er ingen migration på plads at falde tilbage
på: pipelinen slår kun op på den nye nøgle.

Sikkerhedsspærrer: bekræftelsesstreng, dry_run som default, produktions-
navnespærre og server-pind (databasenavnene er ens på tværs af miljøer, kun
servernavnet skiller dev fra QA). Alle spærrer er preflights og afvikles FØR
første sletning, så en rigtig kørsel aldrig når at røre noget mod et uventet
miljø. Det gælder også verifikationen af, at det unikke partielle indeks på
identifieren står og håndhæver — uden det ville genindlæsningen genåbne netop
den dublet-tilstand, ændringen fjerner. TRUNCATE rører ikke indekser, så
tjekket siger det samme før som efter, og lagt først kan dry-run også vise det.

Pause ``ifdb_dfm_daily_schedule`` i vinduet. Natkørslen indeholder hele
IFDB-kæden, og en genkørsel der overlapper den, læser fra tabeller den anden er
i gang med at fylde. ``fhir_server_careplan_dfm`` nægter ganske vist at poste
mod gamle ressourcer, men den spærre er mod at GLEMME nulstillingen, ikke mod at
køre oven i den. En rigtig kørsel afviser derfor selv at starte, mens et af de
berørte jobs kører — dry-run nøjes med at logge dem.

Efter en rigtig kørsel: ``fhir_server_careplan_dfm`` -> ``sof_careplan_dfm`` ->
``sof_careplan_activity_dfm`` -> DSO-syncen for de to tabeller, alle med
mode=full_load. Rækkefølgen betyder noget: barnets udtræk joiner på careplan_id,
og dets child-grain-reconcile markerer rækker for vinduets forældre, før MERGE'n
genopliver de overlevende.
"""

import json

import dagster as dg
from pydantic import Field
from utils.fhir.identifier_systems import SYSTEM_CAREPLAN_ALLE_MARKOERER

# Miljøneutral med vilje: omnøglingen rammer hvert miljø, så nulstillingen skal
# køres både på Dev og QA. Det er server-pinden, ikke strengen, der binder en
# kørsel til ét bestemt miljø — en miljønavngivet streng ville bare invitere til
# at redigere den, når turen kom til QA.
BEKRAEFTELSE = "NULSTIL-CAREPLAN"

# Containment-fragmentet til @>. psycopg adapterer ikke en Python-dict til en
# %s-plads, så jsonb-parametre serialiseres med json.dumps — som overalt ellers
# i repoet (child_resource_reconcile, fhir_grain_checks, index-DDL'en).
ALLE_MARKOERER_FRAGMENT = json.dumps([{"system": SYSTEM_CAREPLAN_ALLE_MARKOERER}])

# Aidbox-grundtabellen for sporet. Alle andre ressourcetabeller er andre spors
# data og må ALDRIG optræde her.
AIDBOX_RESSOURCETABELLER = ("careplan",)

# De flade tabeller. Navnene er samtidig SOF-checkpointernes table_name i
# meta.DFMState (schema_name='fhir') og DSO-syncens rækker samme sted
# (scope='dso'). Barnet først: dets rækker bærer careplan_id.
FLADE_TABELLER = ("CarePlan_Activity", "CarePlan")

# Sporets checkpoint-ejere som (scope, schema_name, table_name). Alle fem bor i
# STAGINGS meta.DFMState — også DSO-syncens, siden DSO-unificeringen flyttede
# vandmærkerne fra DSO's meta.SyncWatermarks til scope 'dso' her.
CHECKPOINTS = (
    ("staging", "fhir_server", "CarePlan"),  # fhir_server_careplan_dfm
    ("staging", "fhir", "CarePlan_Activity"),  # sof_careplan_activity_dfm
    ("staging", "fhir", "CarePlan"),  # sof_careplan_dfm
    ("dso", "fhir", "CarePlan_Activity"),  # DSO-syncen
    ("dso", "fhir", "CarePlan"),
)

NULSTILLINGSGRUND = "CarePlan-genkørsel: identifieren er omnøglet til FHIR-patient-id'et"

# Kørende jobs med et af disse ord i navnet blokerer en rigtig kørsel: de læser
# eller skriver de lag, der er ved at blive tømt (Aidbox, fhir.* i staging, DSO)
# eller checkpointerne. Dækker ifdb_dfm_daily_job/ifdb_dfm_full_job,
# fhir_ifdb_dfm_job, fhir_sof_dfm_job og hele dqa_fhir-familien.
RELEVANTE_JOBORD = (
    "ifdb",
    "fhir",
    "careplan",
    "sof",
    "dso",
    "dqa",
)


def mssql_navneliste(navne):
    """N'...'-liste til en IN-klausul."""
    return ", ".join(f"N'{navn}'" for navn in navne)


def vandmaerke_where():
    """WHERE-klausul for DSO's meta.SyncWatermarks.

    De nuværende DSO-vandmærker for fhir-tabellerne ligger i stagings
    meta.DFMState (scope 'dso') og rammes af dfmstate_where(); det her fanger
    eventuelle rester fra før flytningen.
    """
    return f"WHERE [schema_name] = N'fhir' AND [table_name] IN ({mssql_navneliste(FLADE_TABELLER)})"


# Spærrerne herunder er bevidst kopieret fra sfi_genkoersel_nulstilling frem for
# importeret: begge assets er engangsværktøjer, der fjernes igen efter deres
# genkørsel, og en fælles modul-afhængighed mellem to ting med hver sin
# levetid koster mere end de tredive linjer.
def _mssql_identitet(cursor):
    cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
    server, database = cursor.fetchone()
    return str(server), str(database)


def _afvis_produktionslignende(server, database):
    for navn in (server, database):
        n = navn.lower()
        if "prod" in n or n.endswith("_p"):
            raise dg.Failure(
                description=(
                    f"Server- eller databasenavnet [{server}]/[{database}] ligner "
                    "produktion. Nulstillingen køres kun mod Dev og QA."
                )
            )


def _kraev_server_pind(logger, pinde, identiteter, dry_run):
    """Bind en rigtig kørsel til navngivne servere — dry-run lærer dig navnene."""
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


def _eksisterende_flade_tabeller(cursor):
    cursor.execute(
        "SELECT t.name FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "
        f"WHERE s.name = 'fhir' AND t.name IN ({mssql_navneliste(FLADE_TABELLER)})"
    )
    fundne = {str(row[0]) for row in cursor.fetchall()}
    # Bevar den definerede rækkefølge (barn før forælder).
    return [navn for navn in FLADE_TABELLER if navn in fundne]


def _haandhaevende_unikhedsindeks(conn):
    """Navnet på det UNIQUE VALID indeks på alle_markoerer-identifieren, ellers None.

    Indekset er det, der gør en anden levende CarePlan pr. person uskrivelig.
    Oprettelsen er ikke-fatal i ``fhir_ifdb_database_indexes``, så et grønt
    asset er ikke bevis — og et genindlæs uden indekset ville stille genåbne
    fejltilstanden.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT c.relname, i.indisunique, i.indisvalid "
            "FROM pg_index i "
            "JOIN pg_class c ON c.oid = i.indexrelid "
            "JOIN pg_class t ON t.oid = i.indrelid "
            "WHERE t.relname = 'careplan' AND c.relname LIKE %s",
            ("%alle_markoerer%",),
        )
        raekker = cur.fetchall()
    haandhaever = [navn for navn, unik, gyldig in raekker if unik and gyldig]
    return (haandhaever[0] if haandhaever else None), raekker


def _taelling(cursor, tabeller):
    counts = {}
    for tabel in tabeller:
        cursor.execute(f"SELECT COUNT(*) FROM [fhir].[{tabel}]")
        counts[tabel] = cursor.fetchone()[0]
    return counts


class CarePlanGenkoerselConfig(dg.Config):
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


@dg.asset(
    name="careplan_genkoersel_nulstilling",
    group_name="fhir_operations",
    compute_kind="mssql",
    required_resource_keys={"mssql_target_staging", "mssql_target_dso", "fhir_database"},
    description=(
        "Operatørudløst: tøm Aidbox/staging/DSO for CarePlan og nulstil sporets "
        "DFM-checkpoints, så næste kørsel genopbygger alt fra arkivet under den "
        "FHIR-patient-nøglede identifier. Kør dry_run=true først, og pause "
        "ifdb_dfm_daily_schedule i vinduet."
    ),
)
def careplan_genkoersel_nulstilling(
    context: dg.AssetExecutionContext,
    config: CarePlanGenkoerselConfig,
) -> dg.MaterializeResult:
    logger = context.log
    res = context.resources
    dry_run = config.dry_run

    # --- Preflight A: aktive kørsler ----------------------------------------
    # En samtidig pipeline-kørsel kan poste til de tabeller, der tømmes, eller
    # læse halvtomme lag. Dry-run nøjes med at logge.
    aktive = [
        record.dagster_run
        for record in context.instance.get_run_records(
            dg.RunsFilter(
                statuses=[
                    dg.DagsterRunStatus.QUEUED,
                    dg.DagsterRunStatus.NOT_STARTED,
                    dg.DagsterRunStatus.STARTING,
                    dg.DagsterRunStatus.STARTED,
                    dg.DagsterRunStatus.CANCELING,
                ]
            )
        )
        if record.dagster_run.run_id != context.run_id
    ]
    for run in aktive:
        logger.info(f"Aktiv kørsel: {run.job_name} ({run.run_id})")
    relevante = sorted(
        {
            run.job_name
            for run in aktive
            if any(ord_ in run.job_name.lower() for ord_ in RELEVANTE_JOBORD)
        }
    )
    if relevante and not dry_run:
        raise dg.Failure(
            description=(
                f"{len(relevante)} aktive kørsler rører de berørte lag: {relevante}. "
                "Pause ifdb_dfm_daily_schedule og vent, før nulstillingen køres."
            )
        )

    # --- Preflight B: Aidbox — optæl, identificér, verificér indekset -------
    with res.fhir_database.get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            # Aidbox hedder 'aidbox' i alle miljøer — navnet beviser intet, så
            # identiteten logges til operatørens egne øjne i stedet for en
            # spærre, som i sfi_genkoersel_nulstilling.
            cur.execute(
                "SELECT current_database(), "
                "COALESCE(inet_server_addr()::text, '(socket)'), inet_server_port()"
            )
            aidbox_db, aidbox_host, aidbox_port = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM careplan")
            aidbox_ressourcer = cur.fetchone()[0]
            cur.execute(
                "SELECT COUNT(*) FROM careplan WHERE resource -> 'identifier' @> %s::jsonb",
                (ALLE_MARKOERER_FRAGMENT,),
            )
            alle_markoerer = cur.fetchone()[0]
        unikhedsindeks, indeksraekker = _haandhaevende_unikhedsindeks(conn)
    aidbox_identitet = f"{aidbox_db}@{aidbox_host}:{aidbox_port}"
    logger.info(f"Aidbox: {aidbox_identitet}")

    if unikhedsindeks:
        logger.info(f"Identifier-unikhed håndhæves af {unikhedsindeks}")
    else:
        mangler = (
            "Der findes intet UNIQUE VALID indeks på alle_markoerer-identifieren "
            "på careplan — en anden levende CarePlan pr. person ville kunne "
            "skrives igen efter genindlæsningen. Materialisér "
            f"fhir_ifdb_database_indexes FØR nulstillingen. Fundet: {indeksraekker!r}"
        )
        if dry_run:
            logger.warning(mangler)
        else:
            raise dg.Failure(description=mangler)

    # --- Preflight C: MSSQL-identiteter, navnespærre og optælling ----------
    identiteter = {}
    tabeller = {}
    taellinger = {}
    for lag, resource in (
        ("staging", res.mssql_target_staging),
        ("dso", res.mssql_target_dso),
    ):
        with resource.get_connection() as conn:
            cur = conn.cursor()
            try:
                identiteter[lag] = _mssql_identitet(cur)
                _afvis_produktionslignende(*identiteter[lag])
                tabeller[lag] = _eksisterende_flade_tabeller(cur)
                taellinger[lag] = _taelling(cur, tabeller[lag])
            finally:
                cur.close()
        logger.info(
            f"[{lag}] server={identiteter[lag][0]} database={identiteter[lag][1]} "
            f"rækker={taellinger[lag]}"
        )

    _kraev_server_pind(
        logger,
        {
            "staging": config.bekraeft_staging_server,
            "dso": config.bekraeft_dso_server,
        },
        identiteter,
        dry_run,
    )

    logger.info(
        f"Aidbox: {aidbox_ressourcer:,} CarePlan-ressourcer "
        f"({alle_markoerer:,} med alle_markoerer-identifier, "
        f"{aidbox_ressourcer - alle_markoerer:,} fra den pensionerede "
        "patientrettighedslogik)"
    )

    if dry_run:
        logger.info("dry_run=true — intet er ændret.")
        return dg.MaterializeResult(
            metadata={
                "dry_run": True,
                "aidbox_identitet": aidbox_identitet,
                "aidbox_ressourcer": aidbox_ressourcer,
                "alle_markoerer_ressourcer": alle_markoerer,
                "staging_raekker": dg.MetadataValue.json(taellinger["staging"]),
                "dso_raekker": dg.MetadataValue.json(taellinger["dso"]),
                "unikhedsindeks": unikhedsindeks or "MANGLER",
                "aktive_relevante_koersler": dg.MetadataValue.json(relevante),
            }
        )

    if config.bekraeft != BEKRAEFTELSE:
        raise dg.Failure(
            description=(
                "Nulstillingen sletter alle CarePlan-ressourcer og kan ikke fortrydes. "
                f"Sæt bekraeft til præcis '{BEKRAEFTELSE}', eller lad dry_run stå på true."
            )
        )

    # --- 1. Checkpoints -----------------------------------------------------
    # Først: så længe checkpointerne står, læser næste kørsel kun deltaen, og en
    # kørsel der dør efter tømningen ville efterlade tomme tabeller, som spærren
    # i fhir_server_careplan_dfm accepterer. Nulstillet først fejler enhver halv
    # kørsel højt i stedet.
    with res.mssql_target_staging.get_connection() as conn:
        cur = conn.cursor()
        try:
            for scope, schema_name, table_name in CHECKPOINTS:
                cur.execute(
                    "EXEC [meta].[usp_dfm_reset] "
                    "@schema_name = ?, @table_name = ?, "
                    "@new_value = ?, @reason = ?, @scope = ?",
                    (schema_name, table_name, None, NULSTILLINGSGRUND, scope),
                )
                logger.info(f"[staging] nulstillede [{scope}] {schema_name}.{table_name}")
            conn.commit()
        finally:
            cur.close()

    with res.mssql_target_dso.get_connection() as conn:
        cur = conn.cursor()
        try:
            cur.execute(f"DELETE FROM [meta].[SyncWatermarks] {vandmaerke_where()}")
            vandmaerker_slettet = cur.rowcount
            conn.commit()
        finally:
            cur.close()
    logger.info(f"[dso] slettede {vandmaerker_slettet} rest-vandmærke(r) i meta.SyncWatermarks")

    # --- 2. Aidbox ----------------------------------------------------------
    # Før de flade kopier, så en samtidig SOF-kørsel ikke kan nå at re-projicere
    # de gamle ressourcer ned i tabeller, vi er ved at rydde. TRUNCATE frem for
    # DELETE: tabellen er multi-GB, der er ingen fremmednøgler mellem Aidbox'
    # ressourcetabeller, og versionshistorikken har ingen værdi, når
    # ressourcerne genopbygges fra arkivet.
    with res.fhir_database.get_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for tabel in AIDBOX_RESSOURCETABELLER:
                cur.execute(f"TRUNCATE TABLE {tabel}_history")
                cur.execute(f"TRUNCATE TABLE {tabel}")
                logger.info(f"[aidbox] tømte {tabel} og {tabel}_history")

    # --- 3. + 4. Flade kopier ----------------------------------------------
    for lag, resource in (
        ("staging", res.mssql_target_staging),
        ("dso", res.mssql_target_dso),
    ):
        with resource.get_connection() as conn:
            cur = conn.cursor()
            try:
                for tabel in tabeller[lag]:
                    cur.execute(f"TRUNCATE TABLE [fhir].[{tabel}]")
                    logger.info(f"[{lag}] tømte fhir.{tabel}")
                conn.commit()
            finally:
                cur.close()

    return dg.MaterializeResult(
        metadata={
            "dry_run": False,
            "aidbox_ressourcer_slettet": aidbox_ressourcer,
            "staging_raekker_slettet": dg.MetadataValue.json(taellinger["staging"]),
            "dso_raekker_slettet": dg.MetadataValue.json(taellinger["dso"]),
            "checkpoints_nulstillet": [f"[{s}] {sc}.{t}" for s, sc, t in CHECKPOINTS],
            "rest_vandmaerker_slettet": vandmaerker_slettet,
            "aidbox_identitet": aidbox_identitet,
            "unikhedsindeks": unikhedsindeks,
        }
    )


careplan_genkoersel_nulstilling_job = dg.define_asset_job(
    name="careplan_genkoersel_nulstilling_job",
    selection=dg.AssetSelection.assets("careplan_genkoersel_nulstilling"),
    description=(
        "Tøm CarePlan i Aidbox/staging/DSO og slet sporets DFM-checkpoints, så næste "
        "kørsel genopbygger fra arkivet (operatørudløst)."
    ),
)
