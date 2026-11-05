<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Navngivningsstandard for basismodellen

Her er alle navngivningsregler for basismodellen samlet ét sted — fra branch-navne
og filer i repoet til Dagster-assets, tabeller, views, kolonner, indekser og
FHIR-URI'er.

Standarden gælder repoet `healthpipe-basismodel`. Reglerne for
datamart-repositoriet står i [navngivningsstandard.md](navngivningsstandard.md). De to
standarder bygger på de samme principper, men de to repositories har forskellig
struktur, så enkelte regler er forskellige.

Nogle af reglerne er skrevet ind i unit-tests, så de bliver tjekket ved hvert push.
Kolonnen **Tjekkes af** viser, hvor det er tilfældet. Står der ingenting, er reglen
en konvention, som reviewet holder øje med.

Begreber som CleanCode, DFM og SQL on FHIR (SoF) er forklaret i ordlisten i
[00 — Overblik](../basismodel/00_overblik.md#ordliste).

## Indhold

- [Casetyper](#casetyper)
- [Generelle regler](#generelle-regler)
- [Git og GitLab](#git-og-gitlab)
- [Filer og mapper i repoet](#filer-og-mapper-i-repoet)
- [Dagster: assets, groups, job og schedules](#dagster-assets-groups-job-og-schedules)
- [Databaser: skemaer, tabeller og views](#databaser-skemaer-tabeller-og-views)
- [Kolonner](#kolonner)
- [Indekser og constraints](#indekser-og-constraints)
- [FHIR: URI'er og konstanter](#fhir-urier-og-konstanter)
- [Python-kode](#python-kode)
- [Ældre navne, der ikke omdøbes](#ældre-navne-der-ikke-omdøbes)

## Casetyper

| Case | Forklaring | Eksempel |
| :--- | :--- | :--- |
| **lowercase** | Små bogstaver hele vejen, ingen adskillelse | `arc` |
| **UPPERCASE** | Store bogstaver hele vejen, ingen adskillelse | `IFDB` |
| **snake_case** | Små bogstaver, ord adskilt af `_` | `dw_timestamp_update` |
| **UPPER_SNAKE_CASE** | Store bogstaver, ord adskilt af `_` | `SYSTEM_IFDB_PATIENT_ID` |
| **kebab-case** | Små bogstaver, ord adskilt af `-` | `healthpipe-basismodel` |
| **PascalCase** | Stort begyndelsesbogstav i hvert ord, ingen adskillelse | `LagerbeholdningEgenskaber` |

Tommelfingerreglen er: **tabelnavne i PascalCase, alt andet i snake_case** — men
den del af et navn, der henviser til en tabel, beholder tabellens PascalCase. Derfor
hedder dimensionen `dim.Aftale`, dens asset `dim_Aftale` og dens entity key
`dw_ek_Aftale`.

## Generelle regler

| Regel | Eksempel |
| :--- | :--- |
| Ingen æ, ø og å i de navne, vi selv giver: skriv `ae`, `oe` og `aa`. Kildens egne navne i `ext` og `arc` bevares, som kilden stavede dem (se [Tabeller](#tabeller)) | `dw_haendelsestidspunkt`, `Leverandoervare` |
| Danske ord til forretningsbegreber, engelske ord til tekniske begreber | `dim.Kreditor`, `dw_timestamp_insert` |
| Akronymer skrives med versaler, også midt i et PascalCase-navn | `dim.ILSVare`, `dim.SHAK`, `SKSConcept` — ikke `IlsVare` |
| Ingen forkortelser, der ikke er almindeligt kendte — skriv ordet ud | `Organisation`, ikke `Org` |
| Navngiv efter, hvad noget *tilhører*, ikke hvilken type det har | `id_filter_column` og `id_filter_values`, ikke `id_column` og `id_values` |
| Kommentarer og tekst må gerne have æ, ø og å — reglen gælder navne, ikke prosa | |

## Git og GitLab

| Element | Regel | Eksempel | Tjekkes af |
| :--- | :--- | :--- | :--- |
| **Branch** | `feature/` for nyt arbejde og `bugfix/` for fejlrettelser, efterfulgt af et kort, beskrivende navn i kebab-case. | `feature/dim-ilsvare-kolonner` | GitLab afviser andre præfikser end `feature/`, `bugfix/`, `hotfix/` og `release/` |
| **Commit-besked** | Dansk. Starter med en conventional commit-type og evt. et scope. Hører ændringen til et work item i Azure DevOps, står `AB#<nummer>` først | `AB#<nummer> fix(security): registrér sec.*-DDL i security_daily_job` | |
| **Merge request-titel** | Indeholder `AB#<nummer>`, når der merges til `development`, så pipelinen kan koble MR'en til work item'et i Azure Boards. Styres opgaven i ServiceNow, kan du skrive ServiceNow-id'et i titlen på samme måde | `AB#<nummer> docs: ret DSO-kapitlet` | |

`AB#` er Azure Boards' syntaks: pipelinen kobler automatisk MR'en til work item'et i
Azure DevOps. Mellem ServiceNow og GitLab er der ingen automatisk kobling, men et
ServiceNow-id i titlen gør det stadig muligt at finde frem til MR'en fra opgaven og
omvendt.

Work item- og opgavenumre hører til i commit-beskeden og MR-titlen — aldrig i
kodekommentarer og docstrings.

## Filer og mapper i repoet

### Mapper

| Element | Regel | Eksempel |
| :--- | :--- | :--- |
| **Lag** | UPPERCASE | `DSI/`, `DSA/`, `DSO/` |
| **Kildetype og teknologi i DSI** | snake_case | `DSI/oracle/`, `DSI/mssql/`, `DSI/csv_files/` |
| **FHIR-ressource** | PascalCase, præcis som ressourcetypen hedder i FHIR | `DSA/FHIR/Encounter/`, `DSA/FHIR/CarePlan/` |
| **SQL-lag i DSA** | snake_case, én mappe per artefakttype | `DSA/SQL/staging/`, `DSA/SQL/map/`, `DSA/SQL/dim/`, `DSA/SQL/fct/`, `DSA/SQL/dm/` |
| **DDL per domæne** | snake_case | `infrastructure/ddl/tables/erp/`, `infrastructure/ddl/views/erp/` |
| **Arkiv-DDL per kilde** | Kildenavnet i snake_case | `infrastructure/ddl/sources/ifdb/`, `infrastructure/ddl/sources/kmd_uddata/` |
| **Lineage per skema** | Skemanavnet | `data_lineage/dim/`, `data_lineage/fhir/` |

### Filer

| Element | Regel | Eksempel | Tjekkes af |
| :--- | :--- | :--- | :--- |
| **Python-moduler generelt** | snake_case | `sync_configs.py`, `identifier_systems.py` | |
| **DSA SQL-moduler** | `<lag>_<Tabel>[_<kilde>].py`: laget i snake_case, tabellen i PascalCase, kilden i lowercase | `pre_stg_Budget_sydax.py`, `map_Aftale.py`, `dim_Aftale.py`, `fct_Budget.py` | |
| **FHIR-transformation** | `<Ressource>_<KILDE>_dfm.py`, eller `<Ressource>_<KILDE>_<Kildetabel>_dfm.py`, når én ressource bygges fra flere kildetabeller | `Encounter_IFDB_dfm.py`, `DocumentReference_IFDB_Epikrise_dfm.py` | |
| **SQL on FHIR-modul** | `<Ressource>[_<Del>]_dfm.py` | `Encounter_dfm.py`, `Encounter_Location_dfm.py` | |
| **ViewDefinition** | `<ressource>[_<del>]_view.json` i snake_case. Filnavnet og feltet `name` i filen er det samme som nøglen i `SOF_RESOURCE_CONFIGS` | `encounter_view.json`, `encounter_location_view.json` | `test_each_view_definition_has_resource_config` (feltet `name`) |
| **Tabel-DDL** | `NN_<beskrivelse>.sql` i `infrastructure/ddl/tables/<domæne>/`, hvor `NN` er to cifre, der styrer rækkefølgen, filerne køres i. Flade FHIR-tabeller står i `tables/fhir/01_tables.sql` | `02_dims.sql`, `03_facts.sql` | `test_sof_ddl_crosscheck` (FHIR-tabellerne) |
| **Arkiv-DDL** | `<tabel>.sql` i `infrastructure/ddl/sources/<kilde>/`: én fil per `arc`-tabel eller leverancelog. Filnavnet er tabelnavnet uden skema | `sydax_kreditor.sql`, `LPR3Leverance.sql` | `test_ddl_folder_conventions` |
| **View-DDL** | `NN_vw_<skema>.sql` i `infrastructure/ddl/views/<domæne>/` | `01_vw_dim.sql`, `02_vw_fct.sql`, `01_vw_fhir.sql` | |
| **Migration** | Starter med datoen: `ÅÅÅÅMMDD[bogstav]_<beskrivelse>.sql` i `infrastructure/ddl/migrations/`. Datoen styrer rækkefølgen; et bogstav efter datoen ordner flere filer fra samme dag. Beskrivelsen er snake_case og må gerne starte med work item-nummeret | `20260820c_drop_redundant_dw_ek_indexes.sql`, `20260903_02_reset_vareudlevering_checkpoints.sql` | |
| **Lineage-fil** | `<skema>_<tabel>.yml` i snake_case. Filnavnet er kun til at finde filen — det er feltet `TableName` i filen, der gælder | `fct_afdeling_lagerbeholdning.yml`, `fhir_encounter.yml` | `test_lineage_matches_ddl` (indholdet) |
| **Job og schedules** | `jobs/<domæne>_jobs.py` og `schedules/<domæne>_schedules.py` | `jobs/fhir_ifdb_jobs.py`, `schedules/security_schedules.py` | |
| **README** | UPPERCASE | `README.md` | |
| **YAML** | `.yml`, ikke `.yaml` | | |

## Dagster: assets, groups, job og schedules

### Assets

Et asset hedder det, det laver. Den del af navnet, der er en tabel, beholder
tabellens PascalCase; resten er snake_case.

| Lag | Regel | Eksempel | Tjekkes af |
| :--- | :--- | :--- | :--- |
| **Extract** | `ext_<target_table>` — bygges af factory'en | `ext_ifdb_encounter`, `ext_ils_vare` | |
| **Arkiv** | `arc_<target_table>` | `arc_ils_vare` | |
| **Pre-staging og staging** | `pre_stg_<Tabel>_<kilde>`, `stg_<lag>_<Tabel>[_<kilde>]` | `pre_stg_Budget_sydax`, `stg_dim_Lager`, `stg_dim_EAN` | |
| **Map, dimension, fact** | `map_<Tabel>`, `dim_<Tabel>`, `fct_<Tabel>` — samme navn som modulet og funktionen | `map_Aftale`, `dim_BudgetEgenskaber`, `fct_Budget` | |
| **FHIR: transformation og post** | `fhir_server_<ressource>_dfm`. Skriv ressourcen ud | `fhir_server_encounter_dfm`, `fhir_server_patient_dfm` | `test_fhir_conventions` |
| **FHIR: SQL on FHIR** | `sof_<view>_dfm` | `sof_encounter_dfm` | `test_fhir_conventions` |
| **DSO-sync** | `dso_sync_<skema>_<Tabel>` i group `<domæne>_dso_sync`, med tabellens PascalCase ligesom `dim_<Tabel>`. FHIR-tabeller på DFM-sporet: `dso_sync_fhir_<navn>_dfm`, hvor `<navn>` er SoF-viewets kortnavn, bygget af factory'en i `DSO/load/sync_fhir.py` | `dso_sync_dim_UNSPSC`, `dso_sync_fhir_encounter_dfm` | |
| **Tabel-DDL** | `staging_infra_<domæne>` og `dso_infra_<domæne>` | `staging_infra_erp`, `dso_infra_fhir` | |
| **Arkiv-DDL** | `staging_infra_source_<kilde>` — kører `sources/<kilde>/` på staging | `staging_infra_source_ifdb` | |
| **View-DDL** | `dso_infra_views_<domæne>` — kører view-DDL'en for ét domæne på DSO | `dso_infra_views_haiba`, `dso_infra_views_erp` | |

Alle assets under `DSA/FHIR` skal ligge i en godkendt group og hedde noget, der
passer til den groups mønster. Groups og mønstre står i
`SANCTIONED_GROUP_KEY_PATTERNS` øverst i `etl/tests/unit/test_fhir_conventions.py`,
og det er den liste, der gælder. Vælg group efter, hvilket job assettet skal køre
i — en ressource fra SilkeborgData hører fx til i en `fhir_silkeborgdata_*`-group,
ikke i `fhir_ifdb_dfm`.

### Groups

| Regel | Eksempel |
| :--- | :--- |
| `<domæne>_<lag>` i snake_case. Lagene hedder `archive`, `pre_staging`, `staging`, `ref`, `map`, `dimension`, `fact` og `dso_sync` | `ils_archive`, `erp_staging`, `erp_dimension`, `erp_dso_sync` |
| FHIR-groups: se `SANCTIONED_GROUP_KEY_PATTERNS` ovenfor | `fhir_ifdb_dfm`, `fhir_sof_dfm` |
| DDL til MSSQL-databaserne ligger i to fælles groups, `staging_infrastructure` og `dso_infrastructure` — aldrig i en domænespecifik infrastruktur-group. DDL til FHIR-serveren ligger i `fhir_infrastructure` | `dso_infra_views_haiba` i `dso_infrastructure` |

### Job og schedules

| Element | Regel | Eksempel |
| :--- | :--- | :--- |
| **Job** | snake_case, slutter på `_job` | `data_quality_job`, `dim_dato_daily_job` |
| **Schedule** | snake_case, slutter på `_schedule` | `db_space_daily_schedule` |

Kører en branch som feature-deployment på Dev, sætter Dagster branch-navnet bag på
alle asset-, job- og schedulenavne. Hold derfor branch-navnet kort. Det sker
automatisk — skriv det aldrig selv ind i et navn. Se
[feature_branch_deployments.md](feature_branch_deployments.md).

## Databaser: skemaer, tabeller og views

### Skemaer

Skemanavne er lowercase og korte.

| Skema | Indhold |
| :--- | :--- |
| `ext` | Landing zone: slettes og oprettes igen ved hver kørsel |
| `arc` | Det permanente arkiv |
| `stg` | Pre-staging- og staging-tabeller |
| `map` | Mapping-tabeller fra CleanCode til entity key |
| `dim` / `fct` | Dimensioner og facts |
| `bridge` | Bridge-tabeller mellem facts og dimensioner |
| `dm` | Færdigberegnede leverancer: fakta + `Egenskaber` |
| `ref` | Referencedata og hjælpetabeller |
| `fhir` | Flade FHIR-tabeller fra SQL on FHIR |
| `codesystem` / `conceptmap` | CodeSystems og ConceptMaps |
| `meta` / `dqa` / `sec` | DFM- og migrationsstyring, datakvalitet, sikkerhedstabeller |
| `vw_<skema>` | Views på DSO oven på tabellerne i `<skema>`: `vw_dim`, `vw_fct`, `vw_bridge`, `vw_dm`, `vw_fhir` |

### Tabeller

| Lag | Regel | Eksempel |
| :--- | :--- | :--- |
| **`ext` og `arc`** | `<kilde>_<kildetabel>` i snake_case, med kilden først. Kildens egne navne bevares, som kilden stavede dem — de oversættes ikke til `ae`/`oe`/`aa` | `arc.ils_vare`, `arc.ifdb_encounter`. SDS' `DRGSGHFORLOB` bliver `arc.drg_sghforlob` (kildens `FORLOB`), og SDS' `Psyk_forloeb` bliver `drgpsyk_forloeb` (kildens `forloeb`) |
| **`stg`** | Staging: `<lag>_<Tabel>[_<kilde>]` — målets lag, tabellen i PascalCase og evt. kilden. Pre-staging: `pre_<Tabel>_<kilde>` | `stg.dim_Aftale_ils`, `stg.bridge_AftaleLager`, `stg.pre_Budget_sydax` |
| **`map`, `dim`, `fct`, `bridge`, `ref`** | PascalCase og **ental** — også facts | `dim.Kreditor`, `fct.Postering`, `bridge.AftaleLager` |
| **`dm`** | `<KILDE>_<Tabel>` i PascalCase. Til hver faktatabel hører en `Egenskaber`-tabel med de beskrivende kolonner for faktaets rækker (en transaktionsdimension, se [03c](../basismodel/03c_DSA_arkiv_til_dmskema.md)) | `dm.HAIBA_CasesInfektion`, `dm.HAIBA_CasesInfektionEgenskaber` |
| **`fhir`** | Ressourcetypen i PascalCase, som den hedder i FHIR. En undertabel får del-navnet efter en underscore | `fhir.Encounter`, `fhir.EpisodeOfCare`, `fhir.Encounter_Location` |

`Egenskaber` er i flertal. Det er en vedtaget undtagelse fra reglen om ental.

### Views

| Regel | Eksempel |
| :--- | :--- |
| `vw_<skema>.<Tabel>` — viewet hedder det samme som tabellen, det ligger oven på, i skemaet `vw_` plus tabellens skema | `vw_dim.Organisation`, `vw_fct.Postering`, `vw_dm.HAIBA_CasesInfektion` |

Et view laves kun, når der er en grund til det. Hvornår det er tilfældet, står i
[04 — DSO](../basismodel/04_DSO_sync_til_DSO.md#5-viewet--kun-når-der-er-en-grund).

## Kolonner

### Hvor kolonnenavnet kommer fra

| Lag | Regel |
| :--- | :--- |
| **`ext` og `arc`** | Kildens kolonner 1:1, med kildens egen stavning og casing (`DIAGNOSE` bliver ikke til `Diagnose`) |
| **Kolonner, vi selv tilføjer** | snake_case og `ae`/`oe`/`aa` — også i `arc`. Det gælder audit-kolonnerne (`dw_*`) og afledte nøgler (`*_uuid`, `*_noegle`) |
| **Fra `stg` og frem** | snake_case og `ae`/`oe`/`aa`. Den del, der henviser til en tabel, beholder tabellens PascalCase |
| **`fhir`-tabeller** | snake_case, med samme navn som i ViewDefinition'en. Referencer til andre ressourcer hedder `<ressource>_id` |

### Nøgler og faste kolonner

| Kolonne | Regel | Eksempel | Tjekkes af |
| :--- | :--- | :--- | :--- |
| **Business key** | `bk_<navn>` i snake_case, typisk efter kildens kolonnenavn | `bk_aftale_nummer`, `bk_faktura_header` | |
| **CleanCode** | `clean_code_<Tabel>[_<rolle>]` | `clean_code_Kreditor` | |
| **Entity key** | `dw_ek_<Tabel>[_<rolle>]` — rollen i snake_case | `dw_ek_Kreditor`, `dw_ek_Organisation_henvisende` | `test_lineage_matches_ddl` (FK-annotation) |
| **Surrogatnøgle** (type 2) | `dw_sk_<Tabel>` | `dw_sk_Aftale` | |
| **Datonøgle** | `dw_ek_Dato_<rolle>` — altid med et `dato_tid_<rolle>` ved siden af, der gemmer det rå tidspunkt | `dw_ek_Dato_bogfoering` + `dato_tid_bogfoering` | |
| **Audit** | `dw_timestamp_insert`, `dw_timestamp_update` | | |
| **Type 2 og slettemarkering** | `dw_valid_from`, `dw_valid_to`, `dw_is_deleted` | | |
| **Hændelsestidspunkt** (facts) | `dw_haendelsestidspunkt` | | |
| **FHIR-id'er** | `<ressource>_id` for referencer, `identifier_value` for kildens forretningsnøgle | `patient_id`, `encounter_id` | `test_lineage_matches_ddl` (FK-annotation) |

Alle kolonner i en `map`-tabel, der ikke er systemkolonner, skal begynde med
`dw_`, `clean_code_` eller `bk_`.

## Indekser og constraints

**Giv altid et indeks eller en constraint et navn.** Uden et navn finder SQL Server
selv på et, fx `DF__ils_vare__dw_ti__1A2B3C4D`, og det er forskelligt fra database
til database. Så kan en senere migration ikke droppe eller ændre det ved navn.
Skriv derfor navnet i DDL'en — også for defaults og primærnøgler, der står inde i
`CREATE TABLE`:

```sql
-- map.AftaleVarePris i tables/erp/01_maps.sql
[dw_timestamp_insert] DATETIME2 NOT NULL CONSTRAINT [DF_AftaleVarePris_dw_timestamp_insert] DEFAULT SYSDATETIME(),
CONSTRAINT [PK_Map_AftaleVarePris] PRIMARY KEY CLUSTERED ([clean_code_AftaleVarePris] ASC)
```

Ikke sådan — her får primærnøglen et navn, SQL Server selv finder på:

```sql
[dw_ek_Rekvirent] [int] PRIMARY KEY,
```

Navnet siger, hvad slags objekt det er, hvilken tabel det hører til, og hvad det
dækker.

| Objekt | Regel | Eksempel |
| :--- | :--- | :--- |
| **Primærnøgle** | `PK_<Lag>_<Tabel>` | `PK_Map_AftaleVarePris`, `PK_FHIR_DocumentReference` |
| **Indeks** | `IX_<Lag>_<Tabel>_<kolonne eller formål>` | `IX_arc_ifdb_encounter_dw_timestamp_update`, `IX_Fct_AfdelingLagerbeholdning_Dato_Snapshot`, `IX_Dim_Borger_Temporal` |
| **Unik constraint** | `UQ_<Lag>_<Tabel>_<kolonne eller formål>` | `UQ_Ref_PatientToBorger_PatientId` |
| **Unikt indeks** (fx filtreret) | `UX_<Lag>_<Tabel>_<formål>` | `UX_Dim_Borger_CurrentEntity` |
| **Check constraint** | `CK_<Lag>_<Tabel>_<regel>` | `CK_Dim_Borger_CurrentNotDeleted` |
| **Default constraint** | `DF_<Tabel>_<kolonne>` — uden skema | `DF_Rekvirent_dw_timestamp_insert` |
| **Fremmednøgle** | Erklæres som hovedregel ikke i DDL. Relationerne står i lineage-filen og tjekkes af DQA | |

`<Lag>` er tabellens skema eller lag: `arc`, `FHIR`, `Map`, `Dim`, `Fct`,
`Bridge`, `dm`. Hvordan det staves, er ikke ensartet i dag — både `IX_Dim_…` og
`IX_dim_…` forekommer — og det samme gælder, om sidste del er en kolonne eller et
formål. Det vigtige er, at objektet har et navn; følg stavningen i den DDL-fil, du
arbejder i.

Opret kun et indeks, når det gør en bestemt forespørgsel hurtigere, og skriv i en
kommentar i DDL'en, hvilken. For en arkivtabel er det typisk to: merge-nøglen til
MERGE'en ind i arkivet og `dw_timestamp_update` til delta-udtrækket længere fremme.

## FHIR: URI'er og konstanter

| Element | Regel | Eksempel | Tjekkes af |
| :--- | :--- | :--- | :--- |
| **Egne identifier-systemer og kodesystemer** | URI: `urn:source:<kilde>:<felt>`. Konstant: `SYSTEM_<KILDE>_<FELT>` i `utils/fhir/identifier_systems.py` — ikke i filerne under `DSA/FHIR` | `SYSTEM_IFDB_PATIENT_ID = "urn:source:ifdb:patient_id"` | `test_no_stray_uri_literals_under_dsa_fhir` |
| **Egne extensions** | Konstant: `EXTENSION_<NAVN>` i `utils/fhir/fhir_extensions.py` | `EXTENSION_IFDB_HEALTHISSUE_CODE` | `test_no_stray_uri_literals_under_dsa_fhir` |
| **HL7's egne kodesystemer** | Må stå som konstant øverst i den fil, der bruger dem | `_V3_ACT_CODE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v3-ActCode"` | `test_no_stray_uri_literals_under_dsa_fhir` |
| **SoF-view** | `<ressource>[_<del>]_view` — nøglen i `SOF_RESOURCE_CONFIGS`, feltet `name` i ViewDefinition'en og viewets navn i Aidbox (`sof.<navn>`) | `encounter_view`, `encounter_location_view` | `test_each_view_definition_has_resource_config` |
| **CodeSystem-tabeller** | `<KILDE><Type>` i PascalCase med akronymet i versaler | `SKSConcept`, `SORConcept`, `SHAKAlikeHierarchy` | `test_codesystem_naming` |

URI'er, vi selv finder på, må kun stå ét sted. Står de to steder, bliver de før
eller siden forskellige, og så finder opslagene ikke hinanden.

## Python-kode

| Element | Regel | Eksempel |
| :--- | :--- | :--- |
| **Funktioner og variabler** | snake_case | `build_fhir_archive_query` |
| **Asset-funktioner i DSA SQL** | Samme navn som assettet | `def dim_BudgetEgenskaber(...)` |
| **Klasser** | PascalCase | `SyncTableConfig`, `ArchiveMergeConfig` |
| **Konstanter** | UPPER_SNAKE_CASE | `ENCOUNTER_COLUMNS` |
| **Kun til brug i modulet** | Underscore foran | `_V3_ACT_CODE_SYSTEM` |

## Ældre navne, der ikke omdøbes

En del navne i repoet er fra før navngivningsoprydningen. De bliver stående,
indtil de omdøbes samlet i en særskilt opgave. **Brug dem aldrig i ny kode, og omdøb
dem heller ikke i forbifarten** — en halv omdøbning bryder kæden fra `pre_stg` til
`map`/`dim`/`fct` med fejlen `Invalid column name`.

| Ældre navn | Nyt navn |
| :--- | :--- |
| `DW_EK_…`, `DW_SK_…` | `dw_ek_…`, `dw_sk_…` |
| `DW_Timestamp_Insert`, `DW_ValidFrom` … | `dw_timestamp_insert`, `dw_valid_from` … |
| `CleanCode_…`, `…_CleanCode` | `clean_code_…` |
| `arc_<tabel>_ifdb` (IFDB-arkivassets i `ifdb_archive.py`, fx `arc_encounter_ifdb`) | `arc_<target_table>` |
| `ext.<tabel>_ils` (ILS' gamle udtræk) | `ext.<kilde>_<tabel>` |
| `dso_sync_<skema>_<tabel>` med tabellen i små bogstaver (fx `dso_sync_dim_aftale`) | `dso_sync_<skema>_<Tabel>` |
| Lineage-filer med tabellens PascalCase (`dim_ILSVare.yml`, `dim_UNSPSC.yml`) | `<skema>_<tabel>.yml` i snake_case |
| Forkortede ressourcenavne i FHIR-assets (`fhir_server_eoc_dfm`) | Ressourcen skrevet ud |
| `_dfm` på FHIR-filer og -assets | Uden `_dfm`. Suffikset fjernes i én samlet omdøbning af alle FHIR-filer og -assets |
