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
dagster_etl/data_lineage/
├── dim/          # Dimensionstabeller
├── fct/          # Fact tabeller
└── fhir/         # FHIR-flade tabeller
```

## Navngivningskonvention

- Filnavn: `{type}_{entitet}.yml`
- `type`-præfikser: `dim_`, `fct_`, `fhir_`
- Root-nøgle er altid `dso_lineage`
- Tabelnavnet udledes fra filnavnet: split på første `_`, derefter PascalCase resten. Eksempel: `dim_faktura_egenskaber.yml` → `dim.FakturaEgenskaber`
- Når entitetsnavnet indeholder underscores der skal bevares (fx `VareudleveringEgenskaber`), tilføj et eksplicit `table`-felt

---

## Filformat

Alle lineage-filer deler ét format under `dso_lineage`-nøglen.

### Tabelniveau-felter

| Felt | Påkrævet | Beskrivelse | Effekt i star schema |
|------|----------|-------------|----------------------|
| `Type` | Ja | `dimension`, `fact`, eller `fhir` | Bestemmer om tabellen vises i bus matrix (fact), som dimension-kolonne (dimension), eller i FHIR-fanen (fhir) |
| `Domain` | Ja | Liste af domæner, fx `[ERP]`, `[FHIR]`, `[Shared]` | Grupperer facts i bus matrix efter domæne. Tilgængelig som filter |
| `Description` | Ja | Beskrivelse af tabellen (på dansk) | Vises i detalje-panelet |
| `Grain` | Ja | Hvad én række repræsenterer (på dansk) | Vises i detalje-panelet |
| `SourceSystems` | Nej | Liste af `{system: [tabeller]}` — udelad for seed/genererede tabeller | Vises i entity graph tabel oversigt |
| `Columns` | Ja | Kolonne-lineage (se nedenfor) | Kolonneliste, FK-relationer, og measures i detalje-panelet |
| `table` | Nej | Eksplicit tabelnavn når filnavnet ikke kan udlede det korrekt | Bruges som tabel-ID i hele visualiseringen |
| `SCD` | Nej | `type2` for slowly changing dimensions | Vises i detalje-panelet |
| `FhirResource` | Nej | Kanonisk FHIR-ressourcenavn (fx `Patient`, `Encounter`) | Vises som badge i FHIR-fanen |

### Kolonnefelter

Hver nøgle under `Columns` er et kolonnenavn. Værdien er en dict med valgfri felter:

| Felt | Beskrivelse | Effekt i star schema |
|------|-------------|----------------------|
| `Type` | `PK`, `FK`, eller `Measure` | **PK**: gul markering i detalje-panelet. **FK**: blå markering, bruges til at udlede relationer (bus matrix, entity graph). **Measure**: orange markering, tælles i entity graph sublabel |
| `DataType` | SQL-datatype (fx `INT`, `DECIMAL(18,4)`) | Vises i detalje-panelet |
| `Description` | Beskrivelse af kolonnen (på dansk) | Dokumentation |
| `Source` | Liste af kildereferencer (`system.schema.tabel.kolonne`) | Dokumentation |
| `Expr` | Udtryk eller afledningslogik | Dokumentation |
| `FK-Target` | Eksplicit FK-mål når navngivningskonventionen ikke passer | Overskriver den automatiske FK-målresolution (se nedenfor) |
| `FhirPath` | FHIR-ressource-sti for `fhir`-kolonner | Dokumentation |

---

## FK-navngivningskonvention (dim/fct)

FK-kolonner følger mønstret `DW_EK_{DimensionNavn}`, hvor `DimensionNavn` matcher måltabellens navn **uden underscores**.

For role-playing keys (samme dimension refereret flere gange), tilføj et rolle-suffiks: `DW_EK_{DimensionNavn}_{Rolle}`.

Eksempler:
- `DW_EK_Kreditor` → `dim.Kreditor`
- `DW_EK_Dato_Faktura` → `dim.Dato` med rolle `Faktura`
- `DW_EK_Dato_Ordre` → `dim.Dato` med rolle `Ordre`

**Begrænsning:** Dimensionsnavne må ikke indeholde underscores. Det sikrer at FK-kolonnenavnet kan parses entydigt.

### FK-Target override

Når navngivningskonventionen ikke matcher det tiltænkte mål, brug `FK-Target`:

```yaml
DW_EK_Faktura:      { Type: FK, FK-Target: dim.FakturaEgenskaber }
DW_EK_Kontoplan:    { Type: FK, FK-Target: fct.Postering }
DW_EK_Lager_Parent: { Type: FK, FK-Target: dim.Lager }
```

Nødvendigt når:
- FK-navnet ikke matcher måltabellen (fx `DW_EK_Faktura` → `dim.FakturaEgenskaber`)
- Selvreferencerende FK (fx `DW_EK_Lager_Parent` → `dim.Lager`)
- FK peger på en tabel i et andet schema (fx `fct.Postering` i stedet for `dim.Kontoplan`)

## FHIR-referencekonvention

FHIR-tabeller bruger ikke `DW_EK_`-mønstret. I stedet udledes relationer automatisk fra `{ressource}_id`-navngivningen:

- `patient_id` → `fhir.Patient`
- `encounter_id` → `fhir.Encounter`
- `episode_of_care_id` → `fhir.EpisodeOfCare`
- `managing_organization_id` → `fhir.Organization` (præfikset `managing_` fjernes)

Selvreferencerende kolonner (fx `patient_id` i `fhir.Patient`) og kildesystem-ID'er (`ifdb_*`) springes over. Kolonner der ikke matcher en eksisterende FHIR-tabel ignoreres.

FHIR-kolonner behøver ikke `Type: FK` — relationen udledes automatisk.

## Identifier-system URIs in FhirPath (RETIRED `%name` convention)

**The `%name` placeholder convention is RETIRED.** All lineage YAML files must use literal
URIs in FhirPath expressions. The canonical URI for each identifier system is defined in
[`dagster_etl/utils/fhir/identifier_systems.py`](../../utils/fhir/identifier_systems.py).

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
sydax.Kreditor.Kreditor.JuridiskEnhed       ← godt
mysupply.dbo.statistic_invoice_line.LinjeBeloeb  ← godt
JuridiskEnhed                                ← dårligt
```

---

## Eksempler

### Dimension

```yaml
dso_lineage:
  Type: dimension
  Domain: [ERP]
  Description: "Lagerdimension fra ILS med SOR-organisationskobling"
  Grain: "Én række pr. lager-/oplagringssted"

  SourceSystems:
    - ils: [dbo.sgh, dbo.lgr]
    - sor: [SOR_registry]

  Columns:
    DW_EK_Lager:        { Type: PK, DataType: INT, Source: [map.Lager.DW_EK_Lager] }
    Code:               { DataType: VARCHAR(100), Source: [ils.dbo.sgh.Code] }
    DW_EK_Organisation: { Type: FK, DataType: INT }
    DW_EK_Lager_Parent: { Type: FK, DataType: INT, FK-Target: dim.Lager }
```

### Faktum

```yaml
dso_lineage:
  Type: fact
  Domain: [ERP]
  Description: "Fakturalinje-detalje-facttabel — én række pr. fakturalinje fra MySupply"
  Grain: "Én række pr. fakturalinje, identificeret ved metadata + header + linje sammensat nøgle"

  SourceSystems:
    - mysupply: [dbo.statistic_invoice_header, dbo.statistic_invoice_line, dbo.metadata]

  Columns:
    DW_EK_Fakturalinje: { Type: PK, DataType: INT }
    DW_EK_Kreditor:     { Type: FK, DataType: INT }
    DW_EK_Faktura:      { Type: FK, DataType: INT, FK-Target: dim.FakturaEgenskaber }
    DW_EK_Dato_Faktura: { Type: FK, DataType: INT, Source: [mysupply.dbo.statistic_invoice_header.FakturaDato] }
    LinjeBeloeb:        { Type: Measure, DataType: DECIMAL(18,4), Source: [mysupply.dbo.statistic_invoice_line.LinjeBeloeb] }
```

### FHIR

```yaml
dso_lineage:
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

### Dimension med eksplicit tabelnavn (underscores i entitet)

```yaml
dso_lineage:
  table: dim.VareudleveringEgenskaber
  Type: dimension
  Domain: [ERP]
  Description: "Vareudleverings-egenskabsdimension fra ILS"
  Grain: "Én række pr. vareudlevering"
  ...
```

---

## Star Schema-generering

Lineage-filerne driver en interaktiv HTML-visualisering af data warehouse-stjerneschemat.

### Generer

```bash
python dagster_etl/data_lineage/tools/star_schema/generate_star_schema.py
open dagster_etl/data_lineage/tools/star_schema/star_schema.html
```

Kræver Python 3.10+ og `pyyaml`.

### CLI-optioner

| Option | Standard | Beskrivelse |
|--------|----------|-------------|
| `--lineage-dir` | Auto-detekteret | Sti til `data_lineage/`-mappen |
| `--template` | `star_schema_template.html` | Sti til HTML-skabelonen |
| `--output` | `star_schema.html` | Sti til den genererede HTML |

### Sådan fungerer det

1. **Scan** — Finder alle `.yml`-filer i lineage-mappen
2. **Parse** — Uddrager tabelmetadata fra `dso_lineage`-sektionen
3. **Klassificer relationer** — Udleder relationer fra FK-kolonner:
   - `Type: FK` med `DW_EK_`-navngivning → bus matrix (fact → dim) eller fact-to-fact/dim-to-dim
   - `FK-Target` override → brug eksplicit mål
   - FHIR `{ressource}_id`-kolonner → fhir_ref (fhir → fhir)
   - Selvreference → hierarki
4. **Byg JSON** — Samler tabeller, relationer, domæner og FHIR-tabeller
5. **Injicer i skabelon** — Erstatter `{{DATA}}` i HTML-skabelonen

### Visualiseringen

| View | Beskrivelse |
|------|-------------|
| **Bus Matrix** | Facts som rækker, dimensioner som kolonner, grupperet efter domæne. Klik på en fact for at åbne entity graph |
| **Entity Graph** | D3 force-directed layout med den valgte tabel i centrum og dens relationer som omgivende noder. Klik på en node for detaljer |
| **FHIR-fane** | Liste over FHIR-tabeller med ressource-badge og kildesystem. Klik for entity graph |
| **Detalje-panel** | Metadata, kolonner (PK/FK/Measure-markering), og forbundne tabeller |
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
| `Type: FK` (kolonne) | Blå markering, skaber relation i bus matrix/entity graph |
| `Type: Measure` (kolonne) | Orange markering, tælles i entity graph sublabel ("N measures") |
| `FK-Target` | Overskriver automatisk FK-målresolution |
| `DataType` | Vises i type-kolonnen i detalje-panelet |

---

## Hvornår skal en lineage-fil oprettes eller opdateres?

- En ny DSO-synkroniseret `dim`, `fct`, eller `fhir`-tabel introduceres
- En kolonnemap ændres i DSA/DSO SQL eller FHIR-flattening-logik
- Kildesystemer, grain, eller FK-mål ændres

Lineage-filer er dokumentationsartefakter og bør udvikle sig med transformationslogikken.
