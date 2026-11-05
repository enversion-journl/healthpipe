<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DPA Sikkerhedsmodel

## Konceptoversigt

> **Vigtigt:** Der er kun én måde at blive tildelt sikkerhed på, og det er
> igennem AD. De to DPA-tabeller (`sec.RolleDomaene`, `sec.RBOrganisation`)
> populeres udelukkende fra HealthPipe's AD-grupper via det natlige Dagster-job
> (se [Drift (Dagster)](#drift-dagster)). Eneste undtagelse er de faste
> testbrugere på DEV og QA (se [Testbrugere (seed-data)](#testbrugere-seed-data)).

Sikkerhedsmodellen implementerer adgangskontrol på DPA-views på datamarterne.
Viewene kalder sikkerhedsfunktionerne i DSO-databasen.
Den baserer sig på tre sikkerhedsniveauer der tilsammen bestemmer hvad en bruger
kan se:

### Roller

| Rolle | Navn | beskrivelse |
|-------|------|-------------|
| `U`   | Ubegrænset | Fuld adgang til alle rækker og kolonner i domænet |
| `K`   | Kolonnebegrænset | Personhenførbare kolonner maskeres medmindre bruger har RB-match |
| `P`   | Kolonnebegrænset med populationsansvar | Personhenførbare kolonner maskeres medmindre bruger har RB-match eller populationsansvar for borgeren |

### Sikkerhedsdomæner

Et sikkerhedsdomæne svarer til et DPA-fagområde, f.eks.:
- `DPA_Medicin`
- `DPA_Aktivitet`
- `DPA_Patientrettigheder`
- `DPA_HR`

Hver bruger tildeles præcis én rolle per domæne via `sec.RolleDomaene`. En
bruger kan have forskellige roller på forskellige domæner, fx `K` på
`DPA_Medicin`, `U` på `DPA_ERP` og `P` (SHS) på `DPA_Booking`.

### Rækkebaseret sikkerhed (RB-Organisation)

Brugere med `K`- eller `P`-rolle kan yderligere tildeles adgang til specifikke
organisationer via `sec.RBOrganisation`. Er en borger knyttet til en
organisation, der matcher brugerens RB-tildelinger, gives adgang til
persondata om borgeren.

### Populationsansvar

Rollen `P` giver "populationsansvar" for ét sygehus (`SHS`, `EGS`, `SLB`
eller `OUH`, fx `SLB` = Sygehus Lillebælt) i ét sikkerhedsdomæne.
Populationsansvaret gælder kun det domæne, gruppen gælder for. Brugeren får
adgang til persondata om en borger, når borgerens `Populationsansvar` i
`dim.Borger` er lig brugerens, uanset organisations-match. En bruger har højst ét populationsansvar pr. domæne (se
dublet-reglerne under [Drift (Dagster)](#drift-dagster)).

### Adgangsreglen

DimBorger-viewet på datamarten afgør adgangen (se
[Sikkerhed på datamarterne](#sikkerhed-på-datamarterne)). DPA_ERP-viewene i
`views/security/01_vw_dim_security.sql` har deres egen regel (kun `U`):

| Rolle | Adgang til persondata om en borger |
|-------|------------------------------------|
| `U`   | Altid |
| `K`   | Ved RB-match |
| `P`   | Ved RB-match, eller når brugerens populationsansvar er udfyldt og lig borgerens |
| Ingen rolle i domænet | Aldrig (brugeren ser ingen rækker) |

---

## Tabelstruktur

### sec.RolleDomaene

Mapper bruger til sikkerhedsdomæne med rolle og populationsansvar.
Populeres fra AD-grupperne `LSSAPP_HP_Domaenerolle_<domæne>_<rolle>`
(rolle `U` eller `K`) og `LSSAPP_HP_Domaenerolle_<domæne>_<sygehus>_P`
(rolle `P` med sygehuset som populationsansvar).

| Kolonne | Type | beskrivelse |
|---------|------|-------------|
| bruger | nvarchar(50) | sAMAccountName uden domænepræfiks (fx `vah5ep`) |
| Sikkerhedsdomaene | nvarchar(50) | DPA-domæne (fx `DPA_Medicin`) |
| Rolle | char(1) | `U`, `K` eller `P` (CHECK-constraint) |
| Populationsansvar | nvarchar(50) | Sygehuskode for `P`, ellers tom streng |
| dw_timestamp_update | datetime2 | Tidsstempel for seneste opdatering |
| dw_is_deleted | bit | Soft-delete flag (0=aktiv, 1=slettet) |

**PK (clustered):** `(bruger, Sikkerhedsdomaene)` — ingen IDENTITY-surrogat

### sec.RBOrganisation

Mapper bruger til tilladte organisationer (rækkebaseret sikkerhed).
Populeres fra AD-grupperne `LSSAPP/GSCAPP_HP_Org_<SOR-kode>`;
SOR-koden udpakkes først til sig selv + alle underliggende SOR-koder via
`codesystem.SORHierarchy`, hvorefter hver kode opløses til organisationsnøgler
via join til `dim.Organisation`. Der indsættes altså én række pr. underliggende
organisation, ikke kun for den tildelte kode.

| Kolonne | Type | beskrivelse |
|---------|------|-------------|
| bruger | nvarchar(50) | sAMAccountName uden domænepræfiks (fx `vah5ep`) |
| DW_EK_RBOrganisation | int | Surrogatnøgle til organisation (DW) |
| SOR_kode | varchar(50) | SOR-identifikator (rækkens egen organisation) |
| dw_timestamp_update | datetime2 | Tidsstempel for seneste load |

**PK (clustered):** `(bruger, DW_EK_RBOrganisation)` — ingen IDENTITY-surrogat.
Clusteret på DPA-adgangsstien: `sec.HentRBOrganisation` prober præcis dette
par pr. faktarække, så opslaget er ét clustered seek uden key lookups, og en
brugers samlede rettigheder ligger fysisk samlet. Sekundært index på
`DW_EK_RBOrganisation` til omvendte admin/audit-opslag. Ingen `dw_is_deleted`
— indholdet overskrives fuldt hver kørsel.

**Swap-load:** Indholdet roteres gennem de strukturelt identiske tabeller
`sec.RBOrganisation_New` (fyldes) og `sec.RBOrganisation_Old` (forrige
generation) via metadata-only `ALTER TABLE ... SWITCH` i én transaktion, så
læsere **på staging** aldrig ser en tom/halvfyldt tabel. Hver load logger rækkeantal og
distinkte brugere for gammel og ny generation til `sec.RBOrganisationLoadLog`
(også når størrelses-guarden blokerer) — grundlag for en senere manuel
acceptgate på de daglige kørsler.

> **Scope — the guarantee holds on staging only.** The DPA views read on DSO,
> and `dso_sync_sec_rborganisation` is still `truncate_insert`
> (`sync_configs.py`). TRUNCATE+INSERT runs in one transaction, so READ
> COMMITTED readers block on the Sch-M lock for the duration of the insert —
> and `NOLOCK` readers can see an empty table. The hierarchy expansion makes
> the insert larger and the window longer. The gap is pre-existing and lives in
> the shared sync framework, not in the swap load; an atomic/swap mode for
> `sec_*` in that framework is tracked separately.

### sec.RolleDomaeneHistorik og sec.SORTildelingHistorik

Historikken for brugernes roller og SOR-tilknytning, så man kan slå op, hvilke
roller og hvilken SOR-enhed en bruger havde på et givet tidspunkt.
`sec.RolleDomaene` og `sec.RBOrganisation` er stadig fulde genindlæsninger, og
DPA-funktionerne læser kun dem. Efter et fejlet medlemsopslag ændres ingen af
tabellerne (se Load nedenfor).

| Tabel | Nøgle | Historiserede værdier |
|-------|-------|-----------------------|
| `sec.RolleDomaeneHistorik` | bruger, Sikkerhedsdomaene | Rolle, Populationsansvar |
| `sec.SORTildelingHistorik` | bruger, SOR_kode | Ingen; en tildeling åbnes og lukkes |

`sec.SORTildelingHistorik` gemmer tildelingen, som den står i AD, før
hierarki-udpakningen. Én tildeling giver altså én række, ikke én række pr.
underliggende enhed. Ifølge sikkerhedskonceptet giver en tildeling altid adgang
til alle underliggende enheder.

Begge tabeller har ud over nøglen og værdierne disse kolonner:

| Kolonne | Type | beskrivelse |
|---------|------|-------------|
| dw_valid_from | datetime2(0) | Start på gyldighedsperioden (inklusiv) |
| dw_valid_to | datetime2(0) | Slut på gyldighedsperioden (eksklusiv); `9999-12-31 23:59:59` for den åbne række |
| dw_run_id_from | nvarchar(64) | Kørslen, der indsatte rækken: Dagster-run-id, eller `cli-<uuid>` ved en CLI-indlæsning |
| dw_run_id_to | nvarchar(64) | Kørslen, der lukkede rækken; NULL for den åbne række |
| dw_timestamp_insert | datetime2(0) | Tidspunktet, hvor rækken blev indsat |
| dw_timestamp_update | datetime2(0) | Tidspunktet, hvor rækken sidst blev ændret |

**PK (clustered):** nøglen plus `dw_valid_from`. Et filtreret unikt index
sikrer højst én åben række pr. nøgle.

**Load:** Hver kørsel lukker den åbne række for en nøgle, hvis en værdi er
ændret (også ændringer i store/små bogstaver), eller hvis nøglen ikke længere
er i udtrækket. Fejlede et medlemsopslag i AD under udtrækket, fejler
`populate_security` (CLI: exit 5), før en tabel ændres: alle fire sec-tabeller
beholder deres nuværende indhold, og DSO-synken springes over. Et ufuldstændigt
udtræk kan nemlig både skjule en bruger og ændre en værdi, fx en rolle fra K
til U, når opslaget i K-gruppen fejlede. En reel ændring den dag kommer med i
næste fejlfri kørsel.
Et fejlet opslag er en LDAP-fejl; et slettet objekt (`noSuchObject`) og en
henvisning til et andet domæne (`referral`) tæller ikke. Eksporten skriver de
fejlede DN'er til `healthpipe_fejlede_opslag.jsonl`, så CLI-indlæsningen følger
samme regel som Dagster. CLI-indlæsningen må kun bruge filer fra en ny eksport,
fordi ældre filer skriver gamle værdier ind i historikken. Derefter indsættes en ny åben række for hver nøgle
uden en åben række. Lukning og indsættelse bruger samme tidsstempel, så
perioderne ligger kant i kant. Et tomt udtræk ændrer ingenting. Historikken
ligger kun på staging. Den synkroniseres ikke til DSO,
fordi intet i DSO læser den.

Opslag på et tidspunkt `@t`:

```sql
SELECT Sikkerhedsdomaene, Rolle, Populationsansvar
FROM sec.RolleDomaeneHistorik
WHERE bruger = @bruger
  AND dw_valid_from <= @t
  AND @t < dw_valid_to;
```

Historikken starter ved første load. Et opslag før det tidspunkt giver ingen række.

---

## Funktionsreference

DPA-funktionerne ligger i `infrastructure/ddl/tables/security/05_dpa_functions.sql`.
Funktionerne genskabes med `CREATE OR ALTER`, hver gang `staging_infra_security`
og `dso_infra_security` kører.

### sec.HentDPASikkerhed

Konstruerer en pipe-delimiteret audit-streng der viser hvilken sikkerhedssti
der blev anvendt.

```sql
SELECT * FROM sec.HentDPASikkerhed('K', 'DPA_Medicin', '', '4')
-- Returnerer: DPASikkerhed = 'K|DPA_Medicin||4'
```

| Parameter | Type | beskrivelse |
|-----------|------|-------------|
| @Rolle | nvarchar(100) | Brugerens rolle |
| @Sikkerhedsdomaene | nvarchar(100) | Aktivt domæne |
| @Populationsansvar | nvarchar(100) | Brugerens populationsansvar |
| @Organisation | nvarchar(200) | Matchet organisations-ID |

**Returnerer:** `DPASikkerhed` (nvarchar)

---

### sec.HentBrugerRolleDomaene

Slår brugerens rolle og domæne-tilknytning op. Bruges via `CROSS JOIN` i
sikkerhedsviews til at hente den aktuelle brugers kontekst.

```sql
SELECT * FROM sec.HentBrugerRolleDomaene(SUSER_SNAME(), 'DPA_Medicin')
-- Returnerer: bruger, Rolle, Sikkerhedsdomaene, Populationsansvar
```

| Parameter | Type | beskrivelse |
|-----------|------|-------------|
| @bruger | nvarchar(128) | Brugerlogin (typisk `SUSER_SNAME()`) |
| @Sikkerhedsdomaene | nvarchar(100) | Domæne at slå op |

**Returnerer:** `bruger`, `Rolle`, `Sikkerhedsdomaene`, `Populationsansvar`

---

### sec.HentRBOrganisation

Kontrollerer om en bruger har rækkebaseret adgang til en specifik organisation.
Returnerer organisationsrækken hvis match, tomt resultatsæt ellers.
Domænet strippes fra `@bruger` (typisk `SUSER_SNAME()`) inden opslaget —
tabellen gemmer bart sAMAccountName.

```sql
SELECT * FROM sec.HentRBOrganisation('DPA_Medicin', SUSER_SNAME(), 4)
-- Returnerer rækken hvis bruger har adgang til org 4, ellers tomt
```

| Parameter | Type | beskrivelse |
|-----------|------|-------------|
| @Sikkerhedsdomaene | nvarchar(50) | Domæne |
| @bruger | nvarchar(50) | Brugerlogin |
| @dw_ek_Organisation | bigint | Organisations-surrogatnøgle |

**Returnerer:** `DW_EK_RBOrganisation`, `bruger`, `SOR_kode`

---

## Sikkerhed på datamarterne

Sikkerheden på datamarterne bygges ud fra to skabeloner i datamart-repositoriet.
Kopiér skabelonen, og udfyld pladsholderne. Skabelonerne indeholder også
testvejledningen med testbrugere for rollerne `U`, `K` og `P`.

| Skabelon | Laver | Kalder |
|----------|-------|--------|
| `etl/domains/_template/_faelles/sql/dbo_sec_DimBorger.sql` | `<domæne>.dim.Borger_Organisation` og viewet `dbo.DimBorger_<domæne>` | `sec.HentBrugerRolleDomaene`, `sec.HentRBOrganisation`, `sec.HentDPASikkerhed` |
| `etl/domains/_template/datamart/sql/dbo_sec_Datamart.sql` | Datamart-viewet | `sec.HentBrugerRolleDomaene`, `sec.HentRBOrganisation`, `sec.HentDPASikkerhed` |

Skabelonerne kalder funktionerne på tværs af databaser som
`DSO_Basismodel.sec.<funktion>`.

**DimBorger-viewet** afgrænser `dim.Borger` til de borgere, brugeren må se, efter
[adgangsreglen](#adgangsreglen). Organisationsadgangen slås op i
`<domæne>.dim.Borger_Organisation`, som holder parrene af borger og
organisation. Tabellen skal indeholde alle datamarter i domænet med
persondata. Mangler en datamart, giver organisationsadgangen ikke adgang via
de par af borger og organisation, som kun findes i den datamart.

**Datamart-viewet** joiner med `LEFT JOIN` til `dbo.DimBorger_<domæne>`. Findes
borgeren ikke i DimBorger-viewet, bliver `DW_SK_Borger` til `-99`, og de
personhenførbare kolonner maskeres med
`CASE WHEN DB.DW_SK_Borger IS NULL THEN 'Maskeret'`. Rettighedsvurderingen sker
derfor ét sted, i DimBorger-viewet.

**Sikkerhedsdomænet** er `DB_NAME()`. Begge skabeloner sender databasenavnet som
sikkerhedsdomæne til `sec.HentBrugerRolleDomaene`. Datamartens database skal
derfor have samme navn som domænet i `sec.RolleDomaene` (fx `DPA_Medicin`).
Ellers ser ingen brugere rækker i viewene.

---

## Dagster Asset Graph

Assets eksekveres via Dagster i følgende afhængighedsrækkefølge:

```
dso_infra_schemas
       ↓
dso_infra_security          (DDL: tabeller + funktioner — tables/security/*.sql)
       ↓
dso_infra_views_security    (Views: views/security/*.sql — vw_dim.Borger, vw_dim.Leverandoervare m.fl.)
```

Materialisér hele grafen i Dagster-UI'et: vælg `dso_infra_schemas`,
`dso_infra_security` og `dso_infra_views_security` i asset-grafen, og kør dem.

---

## Filstruktur

Alle sikkerhedsrelaterede DDL-filer ligger centraliseret under `infrastructure/ddl/`:

```
infrastructure/ddl/
├── tables/security/
│   ├── 01_tables.sql          # sec.RBOrganisation
│   ├── 02_functions.sql       # sec.BeskytDimLeverandoervare_v1 (fritekst-maskering, @Rolle)
│   ├── 04_dpa_tables.sql      # sec.RolleDomaene (kanonisk)
│   └── 05_dpa_functions.sql   # sec.HentDPASikkerhed, HentBrugerRolleDomaene,
│                              #   HentRBOrganisation
├── tables/security_historik/
│   └── 01_historik.sql        # sec.RolleDomaeneHistorik, sec.SORTildelingHistorik (kun staging)
└── views/security/
    └── 01_vw_dim_security.sql # vw_dim.Borger, Organisation, Okosted, Leverandoervare, Fakturalinje
```

---

## Tilføj nyt DPA-domæne

For at tilføje sikkerhed på et nyt domæne (fx `DPA_HR`):

1. Opret AD-grupperne for domænet
   (`LSSAPP_HP_Domaenerolle_DPA_HR_U` / `..._K` / `..._<sygehus>_P`) — rolle-tildelingerne
   indlæses derefter automatisk af det natlige AD-load
2. Opret datamart-databasen med domænets navn (`DPA_HR`)
3. Byg DimBorger-viewet og datamart-viewene i datamart-repositoriet ud fra
   skabelonerne (se [Sikkerhed på datamarterne](#sikkerhed-på-datamarterne))

---

## Drift (Dagster)

Sikkerhedsdata vedligeholdes udelukkende via AD. Det natlige job
`security_daily_job` (kl. 03:00 Europe/Copenhagen,
`schedules/security_schedules.py` / `jobs/security_jobs.py`) kører kæden:

```
staging_infra_security ┐
dso_infra_security     ┴→ extract_security → populate_security → dso_sync_security
```

- **staging_infra_security / dso_infra_security**
  (`infrastructure/ddl/tables/security/`): deploys the `sec.*` tables and DPA
  functions on both databases. In the job's selection because a `deps` edge
  only guarantees ordering — not that the tables exist. The DDL is idempotent.
  `{staging,dso}_migrations` is excluded; migrations run at deploy time.
  `staging_infra_security` also runs `tables/security_historik/`, so the
  history tables exist on staging only.
- **extract_security** (`infrastructure/security/export_healthpipe_ad_groups.py`):
  live LDAPS-udtræk af HealthPipe-grupperne (org / domæneroller) og de
  fejlede medlemsopslag — kræver
  netværksadgang til AD.
- **populate_security** (`infrastructure/security/load_healthpipe_ad_to_mssql.py`):
  fuld genindlæsning på STAGING (sandhedskilden): `sec.RolleDomaene` via
  TRUNCATE+INSERT; `sec.RBOrganisation` via **swap-load** — `_New` fyldes,
  gammel/ny-metrikker (rækker + distinkte brugere) logges til
  `sec.RBOrganisationLoadLog`, og indholdet roteres metadata-only i én
  transaktion (`ALTER TABLE ... SWITCH`; forrige generation står i `_Old`).
  Den levende tabel trunkeres aldrig. Hver tildelt SOR-kode udpakkes til sig
  selv + alle underliggende koder via `codesystem.SORHierarchy` (staging;
  genopfriskes kl. 01:00 af organisation-jobbet, to timer før dette job) inden
  joinet til `dim.Organisation`. Dublet-regler pr. `(bruger, Sikkerhedsdomaene)`:
  `K` vinder over `P`, og `P` vinder over `U`; ved `P` for flere
  sygehuse vælges det alfabetisk første, så en bruger højst har ét
  populationsansvar pr. domæne. Begge dubletter er fejl i AD-opsætningen og
  logges som advarsel i Dagster-loggen med bruger, domæne og de fundne
  værdier. RBOrganisation dedupliceres efter udpakning og join.
  Bemærk: `--dry-run` viser kun de tildelte SOR-koder — udpakningen kræver
  DB-opslag og vises ikke. Derefter historiseres `sec.RolleDomaeneHistorik` og
  `sec.SORTildelingHistorik` (se
  [historiktabellerne](#secrolledomaenehistorik-og-secsortildelinghistorik)).
- **dso_sync_security** (`DSO/load/sync_security.py`): truncate/insert-sync af
  begge tabeller fra staging til DSO.

**Størrelses-guard:** `AD_LOAD_MIN_COUNT_RATIO` (default `0.8`) — udtrækket
skal udgøre mindst denne andel af de nuværende rækker i `sec.RolleDomaene` og
`sec.RBOrganisation`. Historiktabellerne har ingen størrelses-guard. Guarden
evalueres FØR indholdet udskiftes, så et tomt eller ufuldstændigt udtræk aldrig tømmer en
fyldt tabel. Brud giver exit 3 (CLI) hhv. et fejlet Dagster-run.
For `sec.RBOrganisation` gælder yderligere to grænser, som fanger et udtræk med
samme antal rækker, men andre RB-tildelinger: `AD_LOAD_MAX_REMOVED_GRANTS_RATIO`
(default `0.2`) er den største andel af de nuværende RB-tildelinger (bruger,
organisation), der må forsvinde efter hierarki-udpakning, og
`AD_LOAD_MAX_LOST_USERS_RATIO` (default `0.1`) er den største andel af de
nuværende brugere, der må miste alle RB-tildelinger; én bruger må altid
forsvinde. Den undtagelse gælder ikke grænsen for RB-tildelinger: en enkelt
bruger med en bred tildeling kan alene bryde den. En
omstrukturering i `codesystem.SORHierarchy` kan også bryde grænsen for
RB-tildelinger. Brud giver også exit 3, og loadet logges i
`sec.RBOrganisationLoadLog` uden swap. Et brud stopper også de to
historiktabeller og DSO-synkroniseringen af `sec.RolleDomaene` og
`sec.RBOrganisation`. En varig ændring blokerer hver nat, indtil nogen griber
ind; efter et midlertidigt ufuldstændigt udtræk går næste nats load igennem.
Fejlbeskeden nævner hver overskredet grænse og dens miljøvariabel. Er
udskiftningen forventet (fx en SOR-omstrukturering), så sæt hver nævnt
`*_MAX_*_RATIO` til `1` og `AD_LOAD_MIN_COUNT_RATIO` lavt, hvis den er nævnt,
for én kørsel. Alle tre værdier skal ligge over 0 og højst være 1.
Bemærk: rulles hierarki-udpakningen tilbage, vil den første ikke-udpakkede
load ligne massiv shrinkage og fejle med exit 3 — sæt da bevidst de grænser,
fejlbeskeden nævner, for én kørsel.

**Verifikation af hierarki-udpakning (dev/QA):** Så længe AD-grupperne har få
medlemmer, kan udpakningen verificeres end-to-end med hardcodede
dummy-brugere (`zzdummy1`/`zzdummy2`) via:

```bash
python -m infrastructure.security.verify_sor_expansion --yes
```

Værktøjet indlæser seneste reelle AD-udtræk + dummy-tildelinger gennem den
rigtige loader, krydstjekker mod en uafhængig rekursiv CTE over
`codesystem.SORHierarchy` (udpakning, overlap-dedup, flag=false), og
genindlæser til sidst AD-sandheden. Afbryder hvis miljøet ligner prod.
Med `--keep` bliver dummy-brugerne i tabellen, og næste load kan derfor blokere
på en af grænserne; kør værktøjet igen uden `--keep`.

**Manuel kørsel (fallback):**

```bash
cd etl
python infrastructure/security/export_healthpipe_ad_groups.py
python infrastructure/security/load_healthpipe_ad_to_mssql.py --dry-run   # verificér først
python infrastructure/security/load_healthpipe_ad_to_mssql.py
```

Se modulernes docstrings for detaljer (miljøvariabler, dublet-regler, exit codes).

---

## Testbrugere (seed-data)

Fire faste testbrugere gør det muligt at teste sikkerhedsviews uden at oprette
nogen i AD-grupperne. De er defineret i
`infrastructure/security/security_seed.py` og har samme rolle i alle
DPA-sikkerhedsdomæner:

| bruger | Rolle | Populationsansvar | sec.RBOrganisation |
|--------|-------|-------------------|--------------------|
| `testbruger_ingen_adgang` | `K` | `''` | Ingen |
| `testbruger_org1` | `K` | `''` | SOR `247991000016009` inkl. underliggende |
| `testbruger_population` | `P` | `SLB` | Ingen |
| `testbruger_U` | `U` | `''` | Ingen |

Domænerne er listen `SIKKERHEDSDOMAENER` i modulet plus de domæner, der står i
AD-udtrækket, så et nyt domæne automatisk får testbrugere.

**Indlæsning:** Når `SECURITY_SEED_ENABLED=true`, lægger
`populate_security` testbrugerne ind i `sec.RolleDomaene` og
`sec.RBOrganisation` i samme transaktion som AD-rækkerne, før
størrelses-guard, TRUNCATE og swap. De overlever derfor den natlige
genindlæsning og synkroniseres til DSO med de øvrige rækker. SOR-tildelingen
udpakkes via `codesystem.SORHierarchy` som en AD-tildeling. Testbrugerne
historiseres ikke i `sec.RolleDomaeneHistorik` og `sec.SORTildelingHistorik`.

- Variablen er slået fra som standard og sættes kun på DEV og QA.
- Loaderen afviser seed-data, hvis miljøet ligner prod (`core.environment`),
  og ved en ukendt værdi i variablen.
- Findes en testbruger også i AD-udtrækket, fejler indlæsningen.
- Er AD-udtrækket for en tabel tomt, springes tabellen over som før. Seed-rækkerne
  fra forrige indlæsning står så urørt; på en helt tom tabel kommer de først med
  ved første AD-indlæsning med rækker.

**Test:** Testbrugerne findes kun i sikkerhedstabellerne og ikke som logins i
AD, så man kan ikke logge ind eller bruge `EXECUTE AS` som dem. Sæt i stedet
navnet i `AktuelBruger` i DimBorger-viewet (se testvejledningen i skabelonerne
under [Sikkerhed på datamarterne](#sikkerhed-på-datamarterne)), og kør
testforespørgslen pr. testbruger. Funktionerne kan også kaldes direkte:

```sql
SELECT * FROM [sec].[HentBrugerRolleDomaene](N'testbruger_population', N'DPA_Medicin');
SELECT COUNT(*) FROM [sec].[RBOrganisation] WHERE [bruger] = N'testbruger_org1';
```

`zzdummy1`/`zzdummy2` fra `verify_sor_expansion` er noget andet: de er
midlertidige og fjernes igen efter verifikationen, mens testbrugerne er
permanente.

---

## Åbne punkter

- Fritekst-maskeringen på `vw_dim.Leverandoervare` og `vw_dim.Fakturalinje` er nu RolleDomaene-baseret
  (reconciled ind i denne branch): viewsene `CROSS JOIN`'er
  `sec.HentBrugerRolleDomaene(SUSER_SNAME(), 'DPA_ERP')` og viser fritekst-kolonner
  kun for `Rolle = 'U'` (ellers `[MASKERET]`). Den gamle `sec.BrugerRoller.Fritekst` /
  `sec.HentFritekstAdgang_v1`-vej er fjernet. En bruger uden en `DPA_ERP`-rolle i
  `sec.RolleDomaene` ser ingen rækker i de to views (CROSS JOIN).
- `bruger` gemmes som bart sAMAccountName (fx `vah5ep`, uden domænepræfiks).
  `sec.HentBrugerRolleDomaene`/`sec.HentRBOrganisation` stripper selv domænet
  fra `@bruger` (typisk `SUSER_SNAME()` = `DOMAIN\sam`), så opslaget virker
  uanset domæneformatet.
