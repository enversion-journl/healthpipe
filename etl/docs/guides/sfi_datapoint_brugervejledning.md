<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SFI_datapoint — Brugervejledning

## Hvad er SFI_datapoint?

`fhir.SFI_datapoint` er et view på DSO-databasen, der samler **alle kliniske
datapunkter fra IFDB-skemaer** i én flad tabel — uanset om de teknisk set er
gemt som FHIR Observations eller QuestionnaireResponses.

Det betyder at du kan søge på tværs af alle IFDB-registreringer uden at vide
noget om den underliggende FHIR-ressourcetype. Viewet er designet til
slutbrugere der vil arbejde med kliniske data — ikke FHIR-specialister.

### Hvad ligger der?

Hver række er ét datapunkt fra en IFDB-aktivitetsregistrering:

- En temperaturmåling
- Et svar på "Ryger du?" i et screeningsskema
- En Glasgow Coma Score
- Et fritekst-notat
- En valgt kode fra en dropdown

### Hvor kommer data fra?

Data udtrækkes fra **IFDB.AKTIVITETSRESULTAT** (Oracle), hvor de kliniske
registreringer ligger som XML-strukturer. ETL-pipelinen:

1. Parser XML for hvert registreret skema
2. Udtrækker hvert enkelt dataelement (felt) som et selvstændigt datapunkt
3. Beriger med metadata fra SFI-kataloget (ledetekster, datatyper, enheder)
4. Gør det tilgængeligt i `fhir.SFI_datapoint` viewet

Data opdateres dagligt via den automatiske IFDB-pipeline.


---

## Routing: Hvad bliver hvad?

Hver IFDB-registrering (AKTIVITETSRESULTAT) kan blive til tre forskellige
FHIR-ressourcer afhængigt af indholdet. Routingen sker automatisk:

```
┌─────────────────────────────────────────┐
│       IFDB AKTIVITETSRESULTAT           │
│  (én XML-registrering fra et skema)     │
└──────────────────┬──────────────────────┘
                   │
                   ▼
        ┌──────────────────────┐
        │  Er AKTIVITETTYPE_   │
        │  KODE en SKS         │    SKS-procedurecheck:
        │  procedurekode?      │    SKS.a.opr*, SKS.a.pro*
        │  (f.eks. operationer)│
        └─────┬────────┬──────┘
          JA  │        │  NEJ
              ▼        │
    ┌──────────────┐   │
    │  PROCEDURE   │   │
    │  (altid)     │   │
    └──────┬───────┘   │
           │           │
           ▼           ▼
    ┌─────────────────────────┐
    │  Har rækken XML med     │
    │  udfyldte felter?       │
    └─────┬───────────┬───────┘
      NEJ │           │ JA
          ▼           │
    ┌──────────┐      │
    │  Færdig: │      │
    │  kun     │      ▼
    │  Procedure│  ┌───────────────────────────────────────────┐
    └──────────┘  │  SFI-elementer udtrækkes fra XML           │
                  │  (kode, ledetekst, datatype, enhed, værdi) │
                  └─────┬──────────────────────────────────────┘
                        │
                        ├──────────────────────────────────────┐
                        │                                      ▼
                        │                         ┌─────────────────────┐
                        │                         │  CODESYSTEM         │
                        │                         │  (sideeffekt)       │
                        │                         │                     │
                        │                         │  Alle unikke SFI-   │
                        │                         │  koder samles op    │
                        │                         │  og MERGE'es til    │
                        │                         │  codesystem.sfi_*   │
                        │                         └─────────────────────┘
                        ▼
                  ┌───────────────────────┐
                  │  Routing-tabel:       │
                  │  Er AKTIVITETTYPE_    │
                  │  KODE godkendt til    │
                  │  Observation?         │
                  └─────┬──────────┬──────┘
                    JA  │          │ NEJ / ukendt
                        ▼          ▼
              ┌──────────────┐  ┌──────────────────────┐
              │ OBSERVATION  │  │ QUESTIONNAIRE-       │
              │              │  │ RESPONSE             │
              │ Ét per SFI-  │  │                      │
              │ datapunkt    │  │ Bevarer skema-       │
              │ (måling/svar)│  │ strukturen fra XML   │
              └──────────────┘  └──────────────────────┘
```

### Hvad styrer routingen?

| Regel | beskrivelse |
|-------|-------------|
| **Procedurekode** | `AKTIVITETTYPE_KODE` der matcher SKS-operationskoder (`SKS.a.opr*`, `SKS.a.pro*`). Giver altid en Procedure-ressource — uafhængigt af alt andet. |
| **Routing-tabel** | Godkendelsesliste (`meta.IFDBAktivitetstypeRouting`) der bestemmer hvilke skematyper der må blive Observations. Kun `active` regler med gyldig Dato matcher. |
| **Fallback → QR** | Alt der ikke er eksplicit godkendt til Observation ender som QuestionnaireResponse. Det er det sikre valg — QR bevarer den fulde skemastruktur. |
| **Paralleloutput** | En procedurekodet række med udfyldte felter kan give **både** Procedure + Observation/QR. |

### Hvad ser du i `SFI_datapoint`?

`SFI_datapoint` viewet samler **Observations** og beriger dem med metadata fra
SFI-kataloget. Procedures og QuestionnaireResponses har deres egne views
(`fhir.IFDB_procedure_event` og `fhir.QuestionnaireResponse`).

---

## Sådan samles `fhir.SFI_datapoint` viewet

Viewet er et JOIN af fire tabeller der allerede ligger på DSO-databasen:

```
┌───────────────────────────────────────────────────────────────────┐
│                         DSO Database                              │
│                                                                   │
│   ┌─────────────────────────┐    ┌──────────────────────────┐     │
│   │   fhir.Observation      │    │  codesystem.sfi_concept  │     │
│   │                         │    │                          │     │
│   │  Datapunkterne:         │    │  Ledetekst-katalog:      │     │
│   │  element_code      ─────┼───►│  Code                    │     │
│   │  value_int              │    │  display (ledetekst)     │     │
│   │  value_text             │    └──────────────────────────┘     │
│   │  value_quantity         │                                     │
│   │  value_unit             │    ┌───────────────────────────────┐│
│   │  patient_id             │    │  codesystem.sfi_properties    ││
│   │  encounter_id           │    │                               ││
│   │  effective_date         │    │  Metadata per SFI-kode:       ││
│   │  element_code      ─────┼───►│  Code                         ││
│   │                         │    │  source_model_name            ││
│   └────────┬────────────────┘    │  source_element_label         ││
│            │                     │  sfi_path                     ││
│            │ encounter_id        │  datatype, unit, version      ││
│            │                     └───────────────────────────────┘│
│            ▼                                                      │
│   ┌─────────────────────────┐                                     │
│   │   fhir.Encounter        │                                     │
│   │                         │                                     │
│   │  Fallback for:          │                                     │
│   │  organization_id        │                                     │
│   │  (service_provider_id)  │                                     │
│   └─────────────────────────┘                                     │
│                                                                   │
│            ════════════════════════════════════                    │
│                          ▼                                        │
│   ┌───────────────────────────────────────────────────────────┐   │
│   │                 fhir.SFI_datapoint (VIEW)                 │   │
│   │                                                           │   │
│   │  element_code ──────────────── fra Observation             │   │
│   │  element_display ──────────── Observation ELLER            │   │
│   │                               sfi_concept.display          │   │
│   │  source_model_name ────────── fra sfi_properties           │   │
│   │  source_element_label ─────── fra sfi_properties           │   │
│   │  sfi_path ─────────────────── fra sfi_properties           │   │
│   │  value_int / value_text ───── fra Observation              │   │
│   │  value_quantity / value_unit ─ fra Observation              │   │
│   │  occurred_at ──────────────── fra Observation              │   │
│   │  patient_id ───────────────── fra Observation              │   │
│   │  organization_id ──────────── Observation ELLER Encounter  │   │
│   └───────────────────────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────┘
```

### Hvad bidrager hver kilde?

| Tabel | Hvad den giver viewet |
|-------|----------------------|
| **fhir.Observation** | Selve datapunktet: SFI-kode, værdi(er), patient, encounter, tidspunkt |
| **codesystem.sfi_concept** | Ledetekst (`display`) som fallback hvis Observation ikke har sin egen |
| **codesystem.sfi_properties** | Berigelse: skema-navn, element-label, sfi_path, datatype, enhed, versionssporing |
| **fhir.Encounter** | Organisation (`service_provider_id`) som fallback for `organization_id` |


---

## SFI-kataloget (CodeSystem)

Udover selve datapunkterne bygger pipelinen et **SFI-katalog** — en
opslagstabel der beskriver alle kendte SFI-koder med metadata. Det er
kataloget der gør `fhir.SFI_datapoint` viewet rigt i stedet for bare rådata.

### Hvad er det?

Tre tabeller på `codesystem`-schemaet:

```
┌─────────────────────────────────────────────────────────────────┐
│                    SFI-kataloget (CodeSystem)                    │
│                                                                 │
│  ┌───────────────────────────┐                                  │
│  │  codesystem.sfi_concept   │  Én række per SFI-kode           │
│  │                           │                                  │
│  │  Code    │ display        │  Code = teknisk nøgle            │
│  │  ────────┼────────────    │  display = ledetekst             │
│  │  Skema   │                │                                  │
│  │  .GCS    │ Glasgow Coma   │  Eksempel: koden for GCS         │
│  │  Skema   │                │                                  │
│  │  .Temp   │ Temperatur     │  Eksempel: koden for temperatur  │
│  └───────────────────────────┘                                  │
│                                                                 │
│  ┌───────────────────────────────────────────────┐              │
│  │  codesystem.sfi_properties                    │              │
│  │                                               │              │
│  │  Code     │ PropertyCode       │ Value        │              │
│  │  ─────────┼────────────────────┼──────────    │              │
│  │  Skema.GCS│ datatype           │ integer      │              │
│  │  Skema.GCS│ unit               │              │              │
│  │  Skema.GCS│ source_model_name  │ VFU_Scr      │              │
│  │  Skema.GCS│ source_element_name│ GCS_Score    │              │
│  │  Skema.GCS│ source_element_label│ Glasgow ... │              │
│  │  Skema.Tmp│ datatype           │ decimal      │              │
│  │  Skema.Tmp│ unit               │ Cel          │              │
│  └───────────────────────────────────────────────┘              │
│                                                                 │
│  ┌───────────────────────────────────────┐                      │
│  │  codesystem.sfi_hierarchy             │                      │
│  │                                       │                      │
│  │  ParentCode      │ ChildCode          │  Klynge-/gruppe-     │
│  │  ────────────────┼────────────────    │  hierarki fra XML    │
│  │  Skema._fb_Vitale│ Skema.GCS          │                      │
│  │  Skema._fb_Vitale│ Skema.Tmp          │                      │
│  └───────────────────────────────────────┘                      │
└─────────────────────────────────────────────────────────────────┘
```

### Hvordan bygges det?

Kataloget bygges **automatisk som sideeffekt** af den eksisterende
XML-parsing. Når pipelinen parser AKTIVITETSRESULTAT-rækker, samler den
alle unikke SFI-koder med deres metadata op undervejs og MERGE'er dem
ind i `codesystem.sfi_*` tabellerne bagefter — ingen ekstra
database-forespørgsler.

### Hvad bruger viewet fra kataloget?

`fhir.SFI_datapoint` joiner med kataloget for at berige hver datapunkt-række:

| Viewkolonne | Kilde | Hvad det giver |
|-------------|-------|----------------|
| `element_display` | `sfi_concept.display` (fallback) | Ledetekst fra kataloget hvis Observation ikke har sin egen |
| `source_model_name` | `sfi_properties` `source_model_name` | Skemanavnet (f.eks. `VFU_Screeningsskema`) |
| `source_model_version` | `sfi_properties` `last_seen_in_version` | Seneste version af skemaet |
| `source_element_id` | `sfi_properties` `source_element_id` | Teknisk element-ID fra IFDB |
| `source_element_name` | `sfi_properties` `source_element_name` | Element-navn fra XML |
| `source_element_label` | `sfi_properties` `source_element_label` | Ledetekst fra XML |
| `sfi_path` | `sfi_properties` `sfi_path` | Hierarkisk sti i skemaet |

**Uden kataloget** ville viewet stadig virke, men kolonner som
`source_model_name`, `source_element_label` og `sfi_path` ville være NULL.
Kataloget er den berigelse der gør rå Observations querybare med
klinisk kontekst.


---

## Vigtige kolonner

| Kolonne | beskrivelse |
|---------|-------------|
| `patient_id` | FHIR Patient-id |
| `encounter_id` | FHIR Encounter-id (kontakt/besøg) |
| `organization_id` | Organisationen bag registreringen (SOR-opslag) |
| `practitioner_id` | Udførende person (IFDB personale-ID) |
| `practitioner_display` | Udførende persons navn |
| `occurred_at` | Tidspunkt for registreringen |
| `element_code` | Teknisk SFI-kode (`Skema.FeltId`) — stabil nøgle |
| `element_display` | Ledeteksten fra IFDB-formularen — det kliniske navn |
| `source_model_name` | Navnet på IFDB-skemaet (formularen) |
| `value_type` | Datatypen: `integer`, `decimal`, `quantity`, `string`, `bool`, `coding`, `datetime` |
| `has_value` | `1` hvis datapunktet har en værdi, `0` hvis tomt |
| `value_int` | Heltalsværdi (f.eks. GCS-score) |
| `value_dec` | Decimalværdi |
| `value_quantity` | Numerisk værdi med enhed |
| `value_unit` | Enheden (f.eks. `mmHg`, `kg`, `%`) |
| `value_text` | Fritekst/strengværdi |
| `value_bool` | Sand/falsk |
| `value_coding_code` | Kodeværdi (fra dropdown/valgliste) |
| `value_coding_display` | Visningstekst for kodeværdi |
| `source_resource` | Den underliggende FHIR-type (`Observation`) |

**Tommelfingerregel:** Brug `element_display` til at søge — det er det
kliniske navn du kender fra IFDB-skemaerne. Brug `element_code` når du har
fundet den præcise kode og vil lave stabile, genbrug­bare queries.


## Eksempel på output

Sådan ser data ud når du henter fra `fhir.SFI_datapoint`:

```sql
SELECT patient_id, practitioner_display, source_model_name, element_display,
       value_type, value_int, value_quantity, value_unit, value_text, occurred_at
FROM fhir.SFI_datapoint
WHERE has_value = 1
ORDER BY occurred_at DESC
```

| patient_id | practitioner_display | source_model_name | element_display | value_type | value_int | value_quantity | value_unit | value_text | occurred_at |
|-----------|---------------------|-------------------|----------------|-----------|----------|---------------|-----------|-----------|-------------|
| a3f8-..91 | Jens Hansen | hbdNiHSSMedBeregning | Total Score | integer | 4 | | | | 2026-05-26 09:14 |
| a3f8-..91 | Jens Hansen | hbdNiHSSMedBeregning | Bevidsthedsniveau | coding | | | | | 2026-05-26 09:14 |
| b712-..c4 | Mette Larsen | hbdObjektivUndersoegelseStandard | Systolisk BT | quantity | | 138 | mmHg | | 2026-05-26 08:42 |
| b712-..c4 | Mette Larsen | hbdObjektivUndersoegelseStandard | Diastolisk BT | quantity | | 82 | mmHg | | 2026-05-26 08:42 |
| b712-..c4 | Mette Larsen | hbdObjektivUndersoegelseStandard | Temperatur | quantity | | 37.2 | Cel | | 2026-05-26 08:42 |
| b712-..c4 | Mette Larsen | hbdObjektivUndersoegelseStandard | Vægt | quantity | | 78.5 | kg | | 2026-05-26 08:42 |
| c945-..e7 | Pia Nielsen | hbdScreeningStofDudit | Hvor ofte bruger du cannabis? | string | | | | 2-4 gange om mdr | 2026-05-25 14:30 |
| c945-..e7 | Pia Nielsen | hbdScreeningStofDudit | Total DUDIT score | integer | 12 | | | | 2026-05-25 14:30 |
| d201-..a3 | Lars Sørensen | hbdAlkoholScreeningAudit | AUDIT score | integer | 8 | | | | 2026-05-25 11:05 |
| d201-..a3 | Lars Sørensen | hbdAlkoholScreeningAudit | Hvor ofte drikker du? | coding | | | | | 2026-05-25 11:05 |

*Bemærk: patient_id er anonymiseret i dette eksempel. `value_coding_code`/`value_coding_display` (ikke vist) indeholder kodeværdier for coding-typen.*

---

## Eksempler

### 1. Start her: Find hvilke datapunkter der findes

Brug denne query til at se hvad der er tilgængeligt, før du bygger specifikke
udtræk:

```sql
SELECT
    element_display,
    element_code,
    source_model_name,
    value_type,
    COUNT(*)                    AS antal,
    COUNT(DISTINCT patient_id)  AS patienter,
    SUM(CAST(has_value AS INT)) AS med_vaerdi
FROM fhir.SFI_datapoint
GROUP BY element_display, element_code, source_model_name, value_type
ORDER BY antal DESC
```

### 2. Fritekstsøgning i ledetekster

Kender du ikke den præcise kode, kan du søge bredt i ledeteksterne:

```sql
SELECT DISTINCT
    element_display,
    element_code,
    source_model_name,
    value_type
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%Glasgow%'
   OR element_display LIKE '%GCS%'
ORDER BY element_display
```

### 3. Glasgow Coma Score — alle registreringer

```sql
SELECT
    patient_id,
    element_display,
    value_int,
    value_quantity,
    value_text,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%Glasgow%'
   OR element_display LIKE '%GCS%'
ORDER BY occurred_at DESC
```

### 4. Rygestatus / tobaksforbrug

```sql
SELECT
    patient_id,
    element_display,
    value_text,
    value_coding_display,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%ryge%'
   OR element_display LIKE '%tobak%'
   OR element_display LIKE '%nikotin%'
   OR element_display LIKE '%cigaret%'
ORDER BY occurred_at DESC
```

### 5. Blodtryk (systolisk / diastolisk)

```sql
SELECT
    patient_id,
    element_display,
    value_quantity,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%blodtryk%'
   OR element_display LIKE '%systolisk%'
   OR element_display LIKE '%diastolisk%'
   OR element_display LIKE '%BT_sys%'
   OR element_display LIKE '%BT_dia%'
ORDER BY patient_id, occurred_at DESC
```

### 6. Vægt, højde og BMI

```sql
SELECT
    patient_id,
    element_display,
    value_quantity,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%BMI%'
   OR element_display LIKE '%ægt%'
   OR element_display LIKE '%vaegt%'
   OR element_display LIKE '%øjde%'
   OR element_display LIKE '%hoejde%'
ORDER BY patient_id, occurred_at DESC
```

### 7. Smerteregistreringer (VAS / NRS)

```sql
SELECT
    patient_id,
    element_display,
    value_int,
    value_quantity,
    value_text,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_display LIKE '%smerte%'
   OR element_display LIKE '%VAS%'
   OR element_display LIKE '%NRS%'
ORDER BY occurred_at DESC
```

### 8. Alle datapunkter for én patient

```sql
SELECT
    element_display,
    source_model_name,
    value_type,
    value_text,
    value_int,
    value_quantity,
    value_unit,
    value_coding_display,
    occurred_at
FROM fhir.SFI_datapoint
WHERE patient_id = '<indsæt patient_id>'
ORDER BY occurred_at DESC
```

### 9. Alle registreringer fra ét bestemt skema

```sql
SELECT
    patient_id,
    element_display,
    value_type,
    value_text,
    value_int,
    value_quantity,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE source_model_name = 'VFU_Screeningsskema'
ORDER BY patient_id, occurred_at DESC
```

### 10. Tidsafgrænset udtræk

```sql
SELECT
    patient_id,
    element_display,
    value_text,
    value_quantity,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE occurred_at >= '2025-01-01'
  AND occurred_at <  '2026-01-01'
ORDER BY occurred_at
```

### 11. Præcis filtrering på en bestemt SFI-kode

Når du har fundet den rigtige kode via query 1 eller 2, kan du lave et stabilt
udtræk der ikke er følsomt over for ændringer i ledetekst:

```sql
SELECT
    patient_id,
    encounter_id,
    value_text,
    value_quantity,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE element_code = 'VFU_Screeningsskema.GCS_Score'
ORDER BY occurred_at DESC
```

### 12. Kun datapunkter med værdier (filtrer tomme felter fra)

```sql
SELECT
    patient_id,
    element_display,
    value_type,
    COALESCE(
        CAST(value_int AS NVARCHAR),
        CAST(value_quantity AS NVARCHAR),
        value_text,
        value_coding_display,
        CAST(value_bool AS NVARCHAR),
        CAST(value_datetime AS NVARCHAR)
    ) AS vaerdi,
    value_unit,
    occurred_at
FROM fhir.SFI_datapoint
WHERE has_value = 1
  AND element_display LIKE '%temperatur%'
ORDER BY occurred_at DESC
```


## FAQ

**Hvorfor ser jeg både Observations og QuestionnaireResponses?**
Det gør du ikke — viewet abstraherer det væk. Du ser kun datapunkter.
Kolonnen `source_resource` fortæller dig den underliggende type, men du
behøver ikke forholde dig til den for at lave udtræk.

**Hvad er forskellen på `element_code` og `element_display`?**
`element_code` er en stabil teknisk nøgle (f.eks. `VFU_Screeningsskema.GCS_Score`).
`element_display` er ledeteksten fra formularen (f.eks. "Glasgow Coma Score").
Ledetekster kan ændre sig mellem versioner af et skema — koden er stabil.

**Hvorfor er nogle værdier NULL?**
Et datapunkt kan være tomt (patienten har ikke udfyldt feltet) eller af en
anden type end den kolonne du kigger i. Brug `has_value = 1` til at filtrere
tomme felter fra, og `value_type` til at se hvilken kolonne værdien ligger i.

**Hvad er `source_model_name`?**
Det er navnet på IFDB-skemaet/formularen. F.eks. `VFU_Screeningsskema`,
`Sygeplejestatus`, `Indlæggelsessamtale`. Brug det til at afgrænse til
registreringer fra ét bestemt skema.

**Hvor ofte opdateres data?**
Dagligt via den automatiske IFDB ETL-pipeline.
