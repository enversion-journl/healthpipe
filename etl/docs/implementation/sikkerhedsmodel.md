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
> (se [Drift (Dagster)](#drift-dagster)).

Sikkerhedsmodellen implementerer adgangskontrol på DPA-views i DSO-databasen.
Den baserer sig på tre sikkerhedsniveauer der tilsammen bestemmer hvad en bruger
kan se:

### Roller

| Rolle | Navn | beskrivelse |
|-------|------|-------------|
| `U`   | Ubegrænset | Fuld adgang til alle rækker og kolonner i domænet |
| `K`   | Kolonnebegrænset | Personhenførbare kolonner maskeres medmindre bruger har RB-match eller populationsansvar |

### Sikkerhedsdomæner

Et sikkerhedsdomæne svarer til et DPA-fagområde, f.eks.:
- `DPA_Medicin`
- `DPA_Aktivitet`
- `DPA_Patientrettigheder`
- `DPA_HR`

Hver bruger tildeles præcis én rolle per domæne via `sec.RolleDomaene`.

### Rækkebaseret sikkerhed (RB-Organisation)

Brugere med `K`-rolle kan yderligere tildeles adgang til specifikke
organisationer via `sec.RBOrganisation`. Hvis en faktarække har en
organisationsreference der matcher brugerens RB-tildelinger, gives adgang
til persondata på den række.

### Populationsansvar

En bruger kan have "populationsansvar" (fx `SLB` = Sygehus Lillebælt).
Dette giver adgang til borgere der hører under samme population,
uanset organisations-match.

---

## Tabelstruktur

### sec.RolleDomaene

Mapper bruger til sikkerhedsdomæne med rolle og populationsansvar.
Populeres fra AD-grupperne `LSSAPP_HP_Domaenerolle_<domæne>_<rolle>`
(rolle) og `LSSAPP_HP_Bopael_<sygehus>` (populationsansvar).

| Kolonne | Type | beskrivelse |
|---------|------|-------------|
| bruger | nvarchar(50) | sAMAccountName uden domænepræfiks (fx `vah5ep`) |
| Sikkerhedsdomaene | nvarchar(50) | DPA-domæne (fx `DPA_Medicin`) |
| Rolle | char(1) | `U` eller `K` (CHECK-constraint) |
| Populationsansvar | nvarchar(50) | Populationskode eller tom streng |
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

---

## Funktionsreference

DPA-funktionerne ligger i `infrastructure/ddl/tables/security/05_dpa_functions.sql`.

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
| @Populationsansvar | nvarchar(100) | Populationskode |
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
| @dw_ek_Organisation | int | Organisations-surrogatnøgle |

**Returnerer:** `DW_EK_RBOrganisation`, `bruger`, `SOR_kode`

---

### sec.BeskytPersonoplysning

Generisk maskeringsfunktion for en enkelt tekstkolonne. Afslører værdien
hvis brugeren har tilstrækkelig adgang (`U`-rolle eller RB-match), ellers
returneres maskeret værdi.

```sql
CROSS APPLY sec.BeskytPersonoplysning(
    R.Rolle,                    -- Fra HentBrugerRolleDomaene
    p.Populationsansvar,
    R.Sikkerhedsdomaene,
    COALESCE(RB1.DW_EK_RBOrganisation, RB2.DW_EK_RBOrganisation),
    p.Notat,                    -- Værdi der skal beskyttes
    N'Maskeret'                 -- Maskeret værdi
) AS SecNotat
```

| Parameter | Type | beskrivelse |
|-----------|------|-------------|
| @Rolle | nvarchar(100) | Brugerrolle |
| @Populationsansvar | nvarchar(100) | Populationsansvar |
| @Sikkerhedsdomaene | nvarchar(100) | Domæne |
| @DW_EK_RBOrganisation | int | RB-match (NULL = ingen) |
| @Personoplysning | nvarchar(250) | Original værdi |
| @PersonoplysningMaskeret | nvarchar(250) | Maskeret værdi |

**Returnerer:** `Personoplysning`, `DPASikkerhed`

> **NB:** Den nuværende implementering returnerer literalen `'Maskeret'` ved
> manglende adgang — parameteren `@PersonoplysningMaskeret` anvendes (endnu)
> ikke i funktionskroppen.

---

### sec.BeskytDimBorger

Specialiseret maskeringsfunktion for borger-dimensionen.
Maskerer CPR, Fornavne og Efternavn.

```sql
CROSS APPLY sec.BeskytDimBorger(
    R.Rolle,
    R.Populationsansvar,
    R.Sikkerhedsdomaene,
    DimBorger.CPR,
    DimBorger.dw_ek_Borger,
    DimBorger.fornavne,
    DimBorger.efternavn
) AS secCPR
```

**Returnerer:** `CPRNummer`, `fornavne`, `efternavn`, `DPASikkerhed`

---

## View-mønstre

### Blød sikkerhed (Soft security)

Alle rækker vises, men personhenførbare kolonner maskeres for brugere
uden adgang. Bruges typisk på fakta-tabeller.

**Mønster:**
```sql
CREATE OR ALTER VIEW [vw_fct].[MitDomaeneFaktaView] AS
SELECT
    p.NøgleKolonner,
    CASE
        WHEN R.Rolle = 'U'
             OR (R.Populationsansvar != '' AND R.Populationsansvar = p.Populationsansvar)
            THEN p.dw_sk_Borger
        WHEN RB.DW_EK_RBOrganisation IS NOT NULL
            THEN p.dw_sk_Borger
        ELSE -1
    END AS dw_sk_Borger,
    SecFelt.Personoplysning AS SensitivtFelt,
    SecFelt.DPASikkerhed
FROM [fct].[MitDomaeneFakta] AS p
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), 'DPA_MitDomaene') AS R
OUTER APPLY [sec].[HentRBOrganisation]('DPA_MitDomaene', SUSER_SNAME(), p.dw_ek_Organisation) AS RB
CROSS APPLY [sec].[BeskytPersonoplysning](
    R.Rolle, p.Populationsansvar, R.Sikkerhedsdomaene,
    COALESCE(RB.DW_EK_RBOrganisation, NULL),
    p.SensitivtFelt, N'Maskeret'
) AS SecFelt;
```

### Hård sikkerhed (Hard security)

Rækker filtreres helt bort for brugere uden adgang.
Bruges typisk på dimensions-tabeller (borger).

**Mønster:**
```sql
CREATE OR ALTER VIEW [vw_dim].[Borger_MitDomaene] AS
SELECT DimBorger.*
FROM [dim].[Borger] AS DimBorger
CROSS JOIN [sec].[HentBrugerRolleDomaene](SUSER_SNAME(), 'DPA_MitDomaene') AS R
WHERE EXISTS (
    SELECT 1 FROM [dbo].[TilladteBorgere_MitDomaene] AS tb
    WHERE tb.dw_ek_Borger = DimBorger.dw_ek_Borger
)
OR R.Rolle = 'U'
OR DimBorger.dw_ek_Borger IN (-1, -99);
```

### TilladteBorgere helper-view

Identificerer hvilke borgere brugeren må se baseret på RB-matches
i domænets fakta-tabeller:

```sql
CREATE OR ALTER VIEW [dbo].[TilladteBorgere_MitDomaene] AS
WITH Tilladte AS (
    SELECT fct.dw_ek_Borger
    FROM [fct].[MitDomaeneFakta] AS fct
    WHERE EXISTS (
        SELECT 1
        FROM [sec].[HentRBOrganisation]('DPA_MitDomaene', SUSER_SNAME(), fct.dw_ek_Organisation) AS rb
        WHERE rb.DW_EK_RBOrganisation IS NOT NULL
    )
    GROUP BY fct.dw_ek_Borger
)
SELECT dw_ek_Borger FROM Tilladte;
```

---

## Eksempel: DPA_Medicin

> **NB:** `fct.Medicinadministration` og de tilhørende views
> (`dbo.TilladteBorgere_Medicin`, `vw_dim.Borger_Medicin`,
> `vw_fct.Medicinadministration`) er på denne branch en
> **reference-implementering** i
> `infrastructure/ddl/views/security/_reference/01_vw_dpa_medicin.sql` og
> deployes ikke herfra. Den fulde test-opsætning (inkl. seed-data og
> test-fakta-tabel) ligger på branchen `[internal ticket]`.

### Scenarie 1: bruger med U-rolle

```sql
-- Sæt bruger til Ubegrænset
UPDATE [sec].[RolleDomaene]
SET Rolle = 'U', Populationsansvar = ''
WHERE bruger = 'ext-yme7na'
  AND Sikkerhedsdomaene = 'DPA_Medicin';

-- Resultat: Alle rækker synlige, ingen maskering
SELECT * FROM [vw_fct].[Medicinadministration];
SELECT * FROM [vw_dim].[Borger_Medicin];
```

### Scenarie 2: bruger med K-rolle + RB-organisation

```sql
-- Sæt bruger til Kolonnebegrænset
UPDATE [sec].[RolleDomaene]
SET Rolle = 'K', Populationsansvar = ''
WHERE bruger = 'ext-yme7na'
  AND Sikkerhedsdomaene = 'DPA_Medicin';

-- bruger har adgang til org 4 via sec.RBOrganisation
-- Resultat: Rækker med Organisation_adm=4 eller Organisation_ord=4
--           viser borgernøgler og notat. Andre rækker maskeret.
SELECT * FROM [vw_fct].[Medicinadministration];
```

### Scenarie 3: bruger med K-rolle + Populationsansvar

```sql
-- Sæt bruger til Kolonnebegrænset med populationsansvar
UPDATE [sec].[RolleDomaene]
SET Rolle = 'K', Populationsansvar = 'SLB'
WHERE bruger = 'ext-yme7na'
  AND Sikkerhedsdomaene = 'DPA_Medicin';

-- Resultat: Borgere med Populationsansvar='SLB' er synlige
SELECT * FROM [vw_dim].[Borger_Medicin];
```

---

## Testprocedure

> **NB:** Proceduren kan ikke køres på denne branch som den står — den kræver
> at reference-SQL'en (`_reference/01_vw_dpa_medicin.sql`) og test-opsætningen
> (test-fakta-tabel + seeds) fra `[internal ticket]` først deployes
> manuelt.

> **NB:** Manuelle `UPDATE`s på `sec.RolleDomaene` er *kun* til test og
> overskrives ved næste natlige AD-load (TRUNCATE+INSERT) — sikkerhed tildeles
> udelukkende via AD-grupper.

### Forudsætninger
1. Kør Dagster-assets: `dso_infra_schemas` → `dso_infra_security` → `dso_infra_views_security`
2. Verificér at din bruger findes i `sec.RolleDomaene` (kommer fra AD-gruppemedlemskab)

### Trin-for-trin

```sql
-- 1. Tjek dine rettigheder
SELECT * FROM [sec].[RolleDomaene]
WHERE bruger = SUSER_SNAME() AND Sikkerhedsdomaene = 'DPA_Medicin';

SELECT * FROM [sec].[RBOrganisation]
WHERE bruger = SUSER_SNAME();

-- 2. Se rådata
SELECT * FROM [fct].[Medicinadministration];
SELECT * FROM [dim].[Borger];

-- 3. Test via sikkerhedsviews
SELECT * FROM [vw_fct].[Medicinadministration];
SELECT * FROM [vw_dim].[Borger_Medicin];

-- 4. Skift rolle og test igen
UPDATE [sec].[RolleDomaene]
SET Rolle = 'K', Populationsansvar = ''
WHERE bruger = SUSER_SNAME() AND Sikkerhedsdomaene = 'DPA_Medicin';

-- Gentag trin 3 og observer forskellen
```

### Forventet adfærd

| Rolle | Populationsansvar | RB-match | Borger synlig? | Notat synlig? |
|-------|-------------------|----------|----------------|---------------|
| U     | (ligegyldigt)     | (ligegyldigt) | Ja | Ja |
| K     | ''                | Ja       | Ja | Ja |
| K     | ''                | Nej      | Nej (maskeret/-1) | Nej (Maskeret) |
| K     | 'SLB'            | Nej      | Ja (hvis borger har SLB) | Ja (via populationsansvar) |
| K     | 'SLB'            | Ja       | Ja | Ja |

---

## Dagster Asset Graph

Assets eksekveres via Dagster i følgende afhængighedsrækkefølge:

```
dso_infra_schemas
       ↓
dso_infra_security          (DDL: tabeller + funktioner — tables/security/*.sql)
       ↓
dso_infra_views_security    (Views: views/security/*.sql — vw_dim.Borger, vw_dim.Leverandoervare m.fl.;
                             _reference/ eksekveres bevidst ikke)
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
│   ├── 04_dpa_tables.sql      # sec.RolleDomaene (kanonisk, fra [internal ticket])
│   └── 05_dpa_functions.sql   # sec.HentDPASikkerhed, HentBrugerRolleDomaene,
│                              #   HentRBOrganisation, BeskytPersonoplysning, BeskytDimBorger
└── views/security/
    ├── 01_vw_dim_security.sql # vw_dim.Borger, Organisation, Okosted, Leverandoervare, Fakturalinje
    └── _reference/
        └── 01_vw_dpa_medicin.sql  # REFERENCE ONLY — DPA_Medicin-mønstret (ikke deployeret)
```

> **NB:** Seed-filerne og test-fakta-tabellen (`01_seed_rolle_domaene.sql`,
> `02_seed_rb_organisation.sql`, `03_seed_test_fcttable.sql`,
> `03_test_fct_table.sql`) findes kun på branchen `[internal ticket]`
> og deployes ikke fra dette repo — tabellerne populeres i stedet fra AD
> (se [Drift (Dagster)](#drift-dagster)).

---

## Tilføj nyt DPA-domæne

For at tilføje sikkerhed på et nyt domæne (fx `DPA_HR`):

1. Opret AD-grupperne for domænet
   (`LSSAPP_HP_Domaenerolle_DPA_HR_U` / `..._K`) — rolle-tildelingerne
   indlæses derefter automatisk af det natlige AD-load
2. Tilføj domænets fakta-tabel via domænets egen pipeline
3. Tilføj sikkerhedsviews i `infrastructure/ddl/views/security/` (ny SQL-fil med
   `dbo.TilladteBorgere_HR`, `vw_dim.Borger_HR`, `vw_fct.*`) — brug
   `_reference/01_vw_dpa_medicin.sql` som skabelon
4. Opdater metadata i `dso_infra_security` og `dso_infra_views_security` assets
   i `infrastructure/assets/dso_infrastructure.py`

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
- **extract_security** (`infrastructure/security/export_healthpipe_ad_groups.py`):
  live LDAPS-udtræk af HealthPipe-grupperne (org / domæneroller / bopæl) — kræver
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
  joinet til `dim.Organisation`. Dublet-regler: `K` vinder over `U` pr.
  `(bruger, Sikkerhedsdomaene)`; ved flere bopæle vælges det alfabetisk
  første populationsansvar; RBOrganisation dedupliceres efter udpakning og join.
  Bemærk: `--dry-run` viser kun de tildelte SOR-koder — udpakningen kræver
  DB-opslag og vises ikke.
- **dso_sync_security** (`DSO/load/sync_security.py`): truncate/insert-sync af
  begge tabeller fra staging til DSO.

**Størrelses-guard:** `AD_LOAD_MIN_COUNT_RATIO` (default `0.8`) — udtrækket
skal udgøre mindst denne andel af måltabellens nuværende rækker. Guarden
evalueres FØR indholdet udskiftes, så et tomt eller ufuldstændigt udtræk aldrig tømmer en
fyldt tabel. Brud giver exit 3 (CLI) hhv. et fejlet Dagster-run.
Bemærk: rulles hierarki-udpakningen tilbage, vil den første ikke-udpakkede
load ligne massiv shrinkage og fejle med exit 3 — sæt da bevidst
`AD_LOAD_MIN_COUNT_RATIO` lavt for én kørsel.

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

**Manuel kørsel (fallback):**

```bash
cd etl
python infrastructure/security/export_healthpipe_ad_groups.py
python infrastructure/security/load_healthpipe_ad_to_mssql.py --dry-run   # verificér først
python infrastructure/security/load_healthpipe_ad_to_mssql.py
```

Se modulernes docstrings for detaljer (miljøvariabler, dublet-regler, exit codes).

---

## Åbne punkter

- Fritekst-maskeringen på `vw_dim.Leverandoervare` og `vw_dim.Fakturalinje` er nu RolleDomaene-baseret
  ([internal ticket], reconciled ind i denne branch): viewsene `CROSS JOIN`'er
  `sec.HentBrugerRolleDomaene(SUSER_SNAME(), 'DPA_ERP')` og viser fritekst-kolonner
  kun for `Rolle = 'U'` (ellers `[MASKERET]`). Den gamle `sec.BrugerRoller.Fritekst` /
  `sec.HentFritekstAdgang_v1`-vej er fjernet. En bruger uden en `DPA_ERP`-rolle i
  `sec.RolleDomaene` ser ingen rækker i de to views (CROSS JOIN).
- `bruger` gemmes som bart sAMAccountName (fx `vah5ep`, uden domænepræfiks).
  `sec.HentBrugerRolleDomaene`/`sec.HentRBOrganisation` stripper selv domænet
  fra `@bruger` (typisk `SUSER_SNAME()` = `DOMAIN\sam`), så opslaget virker
  uanset domæneformatet.
