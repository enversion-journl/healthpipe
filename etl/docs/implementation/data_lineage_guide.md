<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DSO YAML Data Lineage Guide

Lineage-filerne dokumenterer **hvor data kommer fra** med sporbarhed helt tilbage til **kildesystem og kildetabel**. De bruges også til at generere star schema-dokumentation.

## Mappestruktur

```text
etl/data_lineage/
├── bridge/       # Bro-tabeller (mange-til-mange)
├── dim/          # Dimensionstabeller
├── dm/           # Datamart-tabeller (fakta + _Egenskaber-transaktionsdimensioner, fx HAIBA)
├── fct/          # Fact tabeller
└── fhir/         # FHIR-flade tabeller
```

Mappenavnet er SQL-schemanavnet: validatoren (`tools/lineage_ddl_check/config.py`,
`TARGET_SCHEMAS`) globber `data_lineage/<schema>/*.yml` og matcher mod
`CREATE TABLE [<schema>]` i base-DDL'en — en ny mappe kræver derfor at schemaet
tilføjes i `TARGET_SCHEMAS`.

## Navngivningskonvention

- Filnavn: `{type}_{entitet}.yml` (typisk snake_case af tabelnavnet, men filnavnet er kun til organisering — det er `TableName`-feltet der er kilden til sandhed)
- `type`-præfikser: `dim_`, `dm_`, `fct_`, `fhir_`
- Root-nøgle er altid `dso_lineage`
- Tabelnavnet angives altid eksplicit via det påkrævede `TableName`-felt (fx `TableName: dim.Fakturalinje`) — det udledes ikke fra filnavnet

---

## Filformat

Alle lineage-filer deler ét format under `dso_lineage`-nøglen.

### Tabelniveau-felter

| Felt | Påkrævet | beskrivelse | Effekt i star schema |
|------|----------|-------------|----------------------|
| `TableName` | Ja | Fuldt kvalificeret tabelnavn, fx `dim.Fakturalinje` | Bruges som tabel-ID i hele visualiseringen |
| `Type` | Ja | `dimension`, `fact`, eller `fhir` | Bestemmer om tabellen vises i bus matrix (fact), som dimension-kolonne (dimension), eller i FHIR-fanen (fhir) |
| `Domain` | Ja | Liste af domæner, fx `[ERP]`, `[FHIR]`, `[Shared]` | Grupperer facts i bus matrix efter domæne. Tilgængelig som filter |
| `Description` | Ja | beskrivelse af tabellen (på dansk) | Vises i detalje-panelet |
| `Grain` | Ja | Hvad én række repræsenterer (på dansk) | Vises i detalje-panelet |
| `SourceSystems` | Nej | Liste af `{system: [tabeller]}` — udelad for seed/genererede tabeller | Vises i entity graph tabel oversigt |
| `Columns` | Ja | Kolonne-lineage (se nedenfor) | Kolonneliste, FK-relationer, og measures i detalje-panelet |
| `SCD` | Nej | `type2` for slowly changing dimensions | Vises i detalje-panelet |
| `T2Scope` | Nej | Liste af kolonner der bærer T2-versionering (kun `SCD: type2`) — ændringer udenfor listen overskriver eksisterende række | Dokumentation |
| `Keys` | Nej | Mapping `nøglekolonne: beskrivelse` — dokumenterer hvor entitets-/versionsnøgler (`dw_ek_*`/`dw_sk_*`) tildeles (map-/ref-lag) | Dokumentation |
| `SourcePriority` | Nej | Ordnet liste af systemer fra `SourceSystems` — ved overlap vinder det først nævnte (fx `[datafordeler, ifdb]`) | Dokumentation |
| `FhirResource` | Nej | Kanonisk FHIR-ressourcenavn (fx `Patient`, `Encounter`) | Vises som badge i FHIR-fanen |
| `Profile` | Nej | FHIR-profil-URL tabellen er compliant med (fx dk-core-patient) | Dokumentation |
| `Extensions` | Nej | Mapping `kolonne/alias: extension-URL` — FHIR-extensions som kolonnernes `FhirPath` refererer | Dokumentation |
| `Temporal` | Nej | `period` når tabellens grain bærer historik via `period.start` (FHIR sidecars) | Dokumentation |
| `NonProjectedFhirFields` | Nej | Liste af FHIR-felter på ressourcen som bevidst ikke projiceres til flat-tabellen | Dokumentation |
| `ConsumerView` | Nej | Aftagerviewet (`vw_fhir.<Navn>`), når det ikke hedder det samme som tabellen (fx `fhir.SFI_datapoint_store` → `vw_fhir.SFI_datapoint`) | Dokumentation; kontrakttesten kræver, at viewet findes |
| `ViewColumns` | Nej | Mapping `kolonne: {...}` for kolonner, som `vw_fhir`-aftagerviewet lægger til ved opslag (fx kodetekster fra `codesystem.IFDBConcept`), og som derfor ikke står i den flade tabel. Samme kolonnefelter som `Columns` | Dokumentation; ignoreres af lineage↔DDL-verifieren. En test holder listen lig viewets tilføjede kolonner |
| `Privacy` | Nej | Sikkerheds-/adgangsnotat for tabellen | Vises i detalje-panelet (gul boks) |
| `Notes` | Nej | Liste af frie noter (designvalg, gotchas) | Vises som punktliste i detalje-panelet |

### Kolonnefelter

Hver nøgle under `Columns` er et kolonnenavn. Værdien er en dict med valgfri felter:

| Felt | beskrivelse | Effekt i star schema |
|------|-------------|----------------------|
| `Type` | `PK`, `FK`, `Measure`, eller `Audit` | **PK**: gul markering. **FK**: blå markering + relationer. **Measure**: orange markering. **Audit**: grå markering (typisk `DW_Timestamp_*`) |
| `DataType` | SQL-datatype (fx `INT`, `DECIMAL(18,4)`) | Vises i detalje-panelet |
| `Description` | beskrivelse af kolonnen (på dansk) | Dokumentation |
| `Source` | Liste af kildereferencer (`system.schema.tabel.kolonne`) — **skal pege på kildesystemer, aldrig DWH-beregnede tabeller** (se nedenfor) | Dokumentation |
| `Expr` | Udtryk eller afledningslogik | Dokumentation |
| `FK-Target` | Eksplicit FK-mål når navngivningskonventionen ikke passer | Overskriver den automatiske FK-målresolution (se nedenfor) |
| `FK-Key` | Kolonnen i måltabellen FK'en peger på — kun nødvendig når mål-PK ikke hedder `{parent}_id` | Bruges af DQA `referential_integrity`-checket |
| `FK-Required` | `true` når NULL/sentinel i FK-kolonnen er en violation (referencen SKAL kunne opløses) | Skærper DQA `referential_integrity`-checket |
| `FK-Ignore` | Liste af sentinel-værdier der betyder "ingen reference" og ikke skal opløses (fx `[-1]`) | Undtages i DQA `referential_integrity`-checket |
| `FK-None` | `true` når en relationship-formet kolonne (`*_id` / `dw_ek_*`) bevidst IKKE er en flat-tabel-reference (kilde-/forretnings-id) | Opfylder CI's krav om eksplicit FK-intent uden at generere DQA-check |
| `FhirPath` | FHIR-ressource-sti for `fhir`-kolonner | Dokumentation |

Hver relationship-formet kolonne (FHIR `*_id`, DWH non-PK `dw_ek_*`) skal deklarere præcis én intent: `Type: PK`, `Type: FK`/`FK-Target`, eller `FK-None: true` — CI fejler ellers (`data_lineage/tools/lineage_ddl_check`). Et `FK-Target` skal desuden pege på en `TableName` i lineage-filerne — for en FHIR-tabel en `fhir.*`-tabel (store og små bogstaver ignoreres), ellers fejler CI også.

---

## FK-navngivningskonvention (dim/fct)

Kolonnenavne følger [`navngivningsstandard_basismodel.md`](navngivningsstandard_basismodel.md):
snake_case med PascalCase på tabelreferencer. Entitetsnøgler bruger præfikset `dw_ek_`
(lowercase); versionsnøgler i type 2-dimensioner bruger `dw_sk_`.

FK-kolonner følger mønstret `dw_ek_{DimensionNavn}`, hvor `DimensionNavn` matcher måltabellens navn **uden underscores**.

For role-playing keys (samme dimension refereret flere gange), tilføj et rolle-suffiks i
lowercase snake_case: `dw_ek_{DimensionNavn}_{rolle}`.

Eksempler:
- `dw_ek_Kreditor` → `dim.Kreditor`
- `dw_ek_Dato_faktura` → `dim.Dato` med rolle `faktura`
- `dw_ek_Dato_ordre` → `dim.Dato` med rolle `ordre`

**Legacy:** eksisterende DDL kan stadig have uppercase `DW_EK_`/`DW_SK_`. Nye kolonner og
eksempler i denne guide følger `dw_ek_`/`dw_sk_` jf. navngivningsstandarden.

**Begrænsning:** Dimensionsnavne må ikke indeholde underscores. Det sikrer at FK-kolonnenavnet kan parses entydigt.

### FK-Target override

Når navngivningskonventionen ikke matcher det tiltænkte mål, brug `FK-Target`:

```yaml
dw_ek_Kontoplan:    { Type: FK, FK-Target: fct.Postering }
dw_ek_Lager_parent: { Type: FK, FK-Target: dim.Lager }
```

Nødvendigt når:
- FK-navnet ikke matcher måltabellen (fx `dw_ek_Kontoplan` → `fct.Postering`)
- Selvreferencerende FK (fx `dw_ek_Lager_parent` → `dim.Lager`)
- FK peger på en tabel i et andet schema (fx `fct.Postering` i stedet for `dim.Kontoplan`)

## Transaktionsdimension vs. egenskabsdimension

ERP-facts kan have en tilhørende `dim.*`-tabel på samme grain. Der er to mønstre — vælg det rigtige **før** navngivning og lineage:

| | Transaktionsdimension | Egenskabsdimension |
|---|---|---|
| **Grain** | Samme som fact | Grovere end fact (flere fact-rækker kan dele én egenskabs-række) |
| **Navn** | `dim.{Entitet}` = `fct.{Entitet}` (samme entitetsnavn) | `dim.{Entitet}Egenskaber` (suffiks `Egenskaber`) |
| **Map** | Delt `map.{Entitet}` mellem fct og dim | Egen `map.{Entitet}Egenskaber`, separat fra fact-grain |
| **Fact PK** | `dw_ek_{Entitet}` fra delt map — **ikke** IDENTITY | Egen PK fra fact-grain (`dw_ek_Budget`) |
| **Dim PK** | Samme kolonne og værdi som fact-PK | Egen PK fra egenskabs-map (`dw_ek_{Entitet}Egenskaber`) |
| **Fact → dim** | Fact-PK **er** FK til dim (samme kolonnenavn) | Separat `dw_ek_{Entitet}Egenskaber`-kolonne jf. navngivningskonventionen |

**Transaktionsdimension** — alle tre betingelser skal være opfyldt:
1. `fct.X` og `dim.X` deler `map.X` (samme `CleanCode_X` / grain)
2. Tabellerne har samme entitetsnavn (`Fakturalinje`, `Vareudlevering`, `Faktura`, …)
3. Fact-PK = dim-PK = FK (`dw_ek_Fakturalinje` på både `fct.Fakturalinje` og `dim.Fakturalinje`)

```yaml
# Transaktionsdim — fct.Fakturalinje / dim.Fakturalinje (delt map.Fakturalinje)
# fct/fct_fakturalinje.yml:
dw_ek_Fakturalinje: { Type: PK, DataType: INT, FK-Target: dim.Fakturalinje }

# dim/dim_fakturalinje.yml:
dw_ek_Fakturalinje: { Type: PK, DataType: INT }
```

```yaml
# Transaktionsdim — fct.Vareudlevering / dim.Vareudlevering (delt map.Vareudlevering)
# fct/fct_vareudlevering.yml:
dw_ek_Vareudlevering: { Type: PK, DataType: INT, FK-Target: dim.Vareudlevering }

# dim/dim_vareudlevering.yml:
dw_ek_Vareudlevering: { Type: PK, DataType: INT }
```

**Egenskabsdimension** — fact har egen grain-PK og en **separat** FK til egenskabstabellen:

```yaml
# Egenskabsdim — fct.Budget / dim.BudgetEgenskaber (egen map.BudgetEgenskaber)
# fct/fct_budget.yml:
dw_ek_Budget:           { Type: PK, DataType: INT }
dw_ek_BudgetEgenskaber: { Type: FK, DataType: INT }   # → dim.BudgetEgenskaber

# dim/dim_budget_egenskaber.yml:
dw_ek_BudgetEgenskaber: { Type: PK, DataType: INT }
```

**Lineage:** transaktionsdim-PK'er (`dw_ek_*`) har ingen `Source` — de er entitetsnøgler tildelt i delt `map.*`. Egenskabsdim-FK'er dokumenterer kildesystem-kolonnerne der driver `map.{Entitet}Egenskaber`.

### CI-håndhævelse

Reglen håndhæves automatisk — ikke kun via denne dokumentation:

- **Unit tests** (`tests/unit/test_transaktionsdim_grain.py`): verificerer at `Fakturalinje` og `Vareudlevering` resolver PK fra delt `map.*`, at DSO-sync bruger shared PK uden IDENTITY, og at `validate_erp_grain` kalder parity-tjekket.
- **Grain validation** (`scripts/validate_erp_grain.py`, check `fct_dim_parity`): ved kørsel mod staging-DB verificeres 1:1 PK-paritet mellem `fct.*` og `dim.*` for alle transaction-dim facts i `FCT_SPECS` med `map_table`/`map_grain_column` (pt. `Fakturalinje`, `Vareudlevering`).

Kør grain validation manuelt:

```bash
cd etl
python -m scripts.validate_erp_grain --partition-key 2025-01-15
```

## FHIR-referencekonvention

FHIR-tabeller bruger ikke `dw_ek_`-mønstret. Et eksplicit `FK-Target` vinder; ellers udledes relationen automatisk fra `{ressource}_id`-navngivningen:

- `patient_id` → `fhir.Patient`
- `encounter_id` → `fhir.Encounter`
- `episode_of_care_id` → `fhir.EpisodeOfCare`
- `managing_organization_id` → `fhir.Organization` (præfikset `managing_` fjernes)

Selvreferencerende kolonner (fx `patient_id` i `fhir.Patient`) og kildesystem-ID'er (`ifdb_*`) springes over. Kolonner der ikke matcher en eksisterende FHIR-tabel ignoreres; et `FK-Target` uden en matchende FHIR-tabel logger en advarsel.

FHIR-kolonner behøver ikke `Type: FK` — relationen udledes automatisk.

## Identifier-system URIs in FhirPath (RETIRED `%name` convention)

**The `%name` placeholder convention is RETIRED.** All lineage YAML files must use literal
URIs in FhirPath expressions. The canonical URI for each identifier system is defined in
[`etl/utils/fhir/identifier_systems.py`](../../utils/fhir/identifier_systems.py).

```yaml
# ✅ Correct — literal URI:
patient_ssn: { FhirPath: "identifier.where(system='urn:dk:cpr').value" }

# ❌ RETIRED — do NOT use %name placeholders:
# patient_ssn: { FhirPath: "identifier.where(system=%cpr).value" }
```

A CI test (`tests/unit/test_lineage_identifier_systems_consistency.py`) enforces:
1. No `%[a-z]` patterns remain in any `data_lineage/**/*.yml` file.
2. Every `system='...'` URI in lineage YAML exists as a constant value in `identifier_systems.py`.

## Kildereferenceformat

Brug fuld `system.schema.tabel.kolonne`-notation i `Source`-lister:

```
sydax.Kreditor.Kreditor.juridisk_enhed       ← godt
mysupply.dbo.statistic_invoice_line.linje_beloeb  ← godt
juridisk_enhed                                ← dårligt
```

### `Source` skal pege på kildesystemer — aldrig DWH-beregnede tabeller

`Source` dokumenterer, **hvor data oprindeligt kommer fra**. Referencen skal derfor
altid pege på et **kildesystem** (fx `sydax`, `ils`, `mysupply`, `nemhandel`, `sor`),
og **aldrig** på en tabel der er beregnet internt i data warehouset:

- ❌ `arc.*` (archive/råkopi af kilden)
- ❌ `stg.*` / `pre_stg.*` (staging)
- ❌ `map.*` (entitetsnøgle-/business-key-mapping)
- ❌ `dim.*` / `fct.*` (endelige DWH-tabeller)

Disse lag er *transformationstrin*, ikke kilder. Skal en kolonne spores, så følg
transformationslogikken (DSA/DSO SQL) hele vejen tilbage til kildesystem-kolonnen og
angiv **den** i `Source`. Brug evt. `Expr` til at beskrive selve afledningen.

```yaml
# ❌ Forkert — peger på en DWH-beregnet map-tabel:
cvr: { Source: [map.Leverandoervare.bk_kreditor] }

# ✅ Korrekt — peger på kildesystem-kolonnerne, afledning i Expr:
cvr: { Source: [mysupply.dbo.metadata.LeverandorCVR, ils.dbo.lev.se_nr], Expr: "'CVR_' + cvr (DK-præfiks trimmet)" }
```

**Undtagelse — entitetsnøgler:** `dw_ek_*`-nøgler genereres i map-laget og har ingen
kildesystem-oprindelse. De skal derfor stå **helt uden** `Source` (ikke med `map.*` som
kilde). `dw_sk_*` er versionsnøgler i type 2-dimensioner — samme regel.

### CI-håndhævelse af Source-referencer

Checkeren (`data_lineage/tools/lineage_ddl_check`) validerer hver `Source`-reference:

- Referencen skal opløses **case-sensitivt** til `system.schema.tabel.kolonne`, hvor
  `system` og `schema.tabel` er deklareret i filens `SourceSystems`, og kolonnedelen er
  udfyldt. Casing-drift mellem deklaration og reference fejler derfor.
- Referencer med DWH-lag som første segment afvises: `arc.`, `stg.`, `pre_stg.`, `map.`,
  `dim.`, `fct.`, `codesystem.`, `conceptmap.`, `ref.`, `sec.`.
- Legacy `coalesce:`-nøglen afvises — brug `Source:` (fallback-rækkefølgen beskrives i `Expr`).

Dokumenterede undtagelser:

- **Skemaløse registre:** kilder uden schema-begreb deklareres med bart tabelnavn
  (fx `sor: [SOR_registry]`) og giver 3-delte referencer (`sor.SOR_registry.CodeSOR`).
- **Hierarki-shorthand:** niveau-kolonner kan angives som interval i kolonnedelen,
  fx `sydax.Finans.OmkostningsstedHierarki.nummer1..nummer20`, i stedet for 20 enkeltreferencer.
- **Codesystem-afledte ILS-dimensioner:** `dim.Lager`/`dim.Rekvirent` bygges fra
  codesystem-tabeller der selv er ILS-afledte; de refereres som
  `ils.codesystem.<Tabel>.<kolonne>` med tabellen deklareret under `ils`.

---

## Eksempler

### Dimension

```yaml
dso_lineage:
  TableName: dim.Lager
  Type: dimension
  Domain: [ERP]
  Description: "Lagerdimension fra ILS med SOR-organisationskobling"
  Grain: "Én række pr. lager-/oplagringssted"

  SourceSystems:
    - ils: [dbo.sgh, dbo.lgr]
    - sor: [SOR_registry]

  Columns:
    dw_ek_Lager:        { Type: PK, DataType: INT }   # entitetsnøgle fra map-laget → ingen Source
    code:               { DataType: VARCHAR(100), Source: [ils.dbo.sgh.Code] }
    dw_ek_Organisation: { Type: FK, DataType: INT }
    dw_ek_Lager_parent: { Type: FK, DataType: INT, FK-Target: dim.Lager }
```

### Faktum

```yaml
dso_lineage:
  TableName: fct.Fakturalinje
  Type: fact
  Domain: [ERP]
  Description: "Fakturalinje-detalje-facttabel — én række pr. fakturalinje fra MySupply"
  Grain: "Én række pr. fakturalinje, identificeret ved metadata + header + linje sammensat nøgle"

  SourceSystems:
    - mysupply: [dbo.statistic_invoice_header, dbo.statistic_invoice_line, dbo.metadata]

  Columns:
    dw_ek_Fakturalinje: { Type: PK, DataType: INT, FK-Target: dim.Fakturalinje }
    dw_ek_Kreditor:     { Type: FK, DataType: INT }
    dw_ek_Faktura:      { Type: FK, DataType: INT, FK-Target: dim.Faktura }
    dw_ek_Dato_faktura: { Type: FK, DataType: INT, Source: [mysupply.dbo.statistic_invoice_header.FakturaDato] }
    linje_beloeb:        { Type: Measure, DataType: DECIMAL(18,4), Source: [mysupply.dbo.statistic_invoice_line.LinjeBeloeb] }
```

Se [Transaktionsdimension vs. egenskabsdimension](#transaktionsdimension-vs-egenskabsdimension): `dw_ek_Fakturalinje` er transaktionsdim-PK (delt med `dim.Fakturalinje` via `map.Fakturalinje`); `dw_ek_Faktura` er en separat FK til header-transaktionsdim `dim.Faktura`.

### FHIR

```yaml
dso_lineage:
  TableName: fhir.Encounter
  Type: fhir
  Domain: [FHIR]
  Description: "FHIR Encounter ressource — kliniske kontakter fra IFDB"
  Grain: "Én række pr. klinisk kontakt (besøg)"
  FhirResource: Encounter

  SourceSystems:
    - ifdb: [IFDB.ENCOUNTER]

  Columns:
    encounter_id: { Type: PK, FhirPath: "id", Source: [ifdb.IFDB.ENCOUNTER.OBJECT_ID] }
    patient_id:   { FhirPath: "subject.id", Source: [ifdb.IFDB.ENCOUNTER.PATIENT_ID] }
    status:       { FhirPath: "status", Source: [ifdb.IFDB.ENCOUNTER.STATUS_TEXT] }
    period_start: { FhirPath: "period.start", Source: [ifdb.IFDB.ENCOUNTER.PERIOD_START] }
```

### Dimension med underscores i entitetsnavnet

`TableName` er altid påkrævet (se ovenfor), men er særligt vigtigt her: uden det ville et navn som `AfdelingLagerbeholdningEgenskaber` ikke kunne udledes entydigt fra filnavnet alene.

```yaml
dso_lineage:
  TableName: dim.AfdelingLagerbeholdningEgenskaber
  Type: dimension
  Domain: [ERP]
  Description: "Afdelingslager-egenskabsdimension fra ILS"
  Grain: "Én række pr. afdelingslagerpost"
  ...
```

---

## Star Schema-generering

Lineage-filerne driver en interaktiv HTML-visualisering af data warehouse-stjerneschemat.

### Generer

```bash
python etl/data_lineage/tools/star_schema/generate_star_schema.py
open etl/data_lineage/tools/star_schema/star_schema.html
```

Kræver Python 3.10+ og `pyyaml`.

### CLI-optioner

| Option | Standard | beskrivelse |
|--------|----------|-------------|
| `--lineage-dir` | Auto-detekteret | Sti til `data_lineage/`-mappen |
| `--template` | `star_schema_template.html` | Sti til HTML-skabelonen |
| `--output` | `star_schema.html` | Sti til den genererede HTML |

### Sådan fungerer det

1. **Scan** — Finder alle `.yml`-filer i lineage-mappen
2. **Parse** — Uddrager tabelmetadata fra `dso_lineage`-sektionen
3. **Klassificer relationer** — Udleder relationer fra FK-kolonner:
   - `Type: FK` med `DW_EK_`/`dw_ek_`-navngivning → bus matrix (fact → dim) eller fact-to-fact/dim-to-dim
   - `Type: PK` + `FK-Target: dim.{Entitet}` → transaktionsdim (delt PK/FK, `dual_key` i visualiseringen)
   - `FK-Target` override → brug eksplicit mål
   - FHIR `{ressource}_id`-kolonner → fhir_ref (fhir → fhir)
   - Selvreference → hierarki
4. **Byg JSON** — Samler tabeller, relationer, domæner og FHIR-tabeller
5. **Injicer i skabelon** — Erstatter `{{DATA}}` i HTML-skabelonen

### Visualiseringen

| View | beskrivelse |
|------|-------------|
| **Bus Matrix** | Facts som rækker, dimensioner som kolonner, grupperet efter domæne. Fyldt grøn prik = FK; ring = delt PK/FK (transaktionsdim). Klik på en fact for at åbne entity graph |
| **Entity Graph** | D3 force-directed layout med den valgte tabel i centrum og dens relationer som omgivende noder. Transaktionsdim-kanter viser kolonnenavn med `(PK↔FK)`. Klik på en node for detaljer |
| **FHIR-fane** | Liste over FHIR-tabeller med ressource-badge og kildesystem. Klik for entity graph |
| **Detalje-panel** | Metadata, transaktionsdim-badge på `dim.*`, kolonner (PK/FK/Measure/PK↔FK-markering), og forbundne tabeller |
| **Søg & filter** | Filtrer efter tabelnavn, kolonnenavn, domæne, eller type |

### Hvad felterne styrer i visualiseringen

| Lineage-felt | Effekt |
|--------------|--------|
| `Type: dimension` | Tabel vises som kolonne i bus matrix |
| `Type: fact` | Tabel vises som række i bus matrix |
| `Type: fhir` | Tabel vises i FHIR-fanen (ikke i bus matrix) |
| `Domain` | Grupperer facts i bus matrix, tilgængelig som dropdown-filter |
| `Description` | Vises i tabel detalje-panelet |
| `Grain` | Vises i detalje-panelet |
| `SCD` | Vises i detalje-panelet |
| `Type: PK` (kolonne) | Gul markering i detalje-panelet |
| `Role: pk_and_fk` (kolonne) | Grøn PK↔FK-markering — fact grain key der også er FK til transaktionsdim |
| `DimModel: transaction` (tabel) | Transaktionsdim-badge i detalje-panelet |
| `TransactionDimFor` (tabel) | Vises som badge på transaktionsdim (back-reference til fact) |
| `Privacy` (tabel) | Vises i detalje-panelet (gul advarselsboks) |
| `Notes` (tabel) | Vises som punktliste i detalje-panelet |
| `Type: FK` (kolonne) | Blå markering, skaber relation i bus matrix/entity graph |
| `Type: Measure` (kolonne) | Orange markering, tælles i entity graph sublabel ("N measures") |
| `Type: Audit` (kolonne) | Grå markering for audit/timestamp-kolonner |
| `FK-Target` | Overskriver automatisk FK-målresolution; på fact-PK markerer transaktionsdim-FK |
| `DataType` | Vises i type-kolonnen i detalje-panelet |

---

## Hvornår skal en lineage-fil oprettes eller opdateres?

- En ny DSO-synkroniseret `dim`, `fct`, eller `fhir`-tabel introduceres
- En kolonnemap ændres i DSA/DSO SQL eller FHIR-flattening-logik
- Kildesystemer, grain, eller FK-mål ændres

Lineage-filer er dokumentationsartefakter og bør udvikle sig med transformationslogikken.
