<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# SFI Datapoint Store — Data Quality Review

**Dato**: 2026-06-09
**Analyseret af**: Pipeline-team
**Scope**: `fhir.SFI_datapoint_store` på DSO (7.739.382 rækker)

---

## Baggrund

SFI datapoint store materialiserer data fra `fhir.SFI_datapoint_build`-viewet, som er en UNION ALL af:

1. **Observation**-baserede SFI-datapunkter (fra `fhir.Observation` med `element_system = 'urn:dk:ifdb:sfi'`)
2. **QuestionnaireResponse**-baserede SFI-datapunkter (fra `fhir.QuestionnaireResponse_item` JOIN `fhir.Questionnaire_item`)

Begge beriges med CodeSystem-metadata fra `codesystem.sfi_concept` og `codesystem.sfi_properties`.

---

## Fund

### 1. `fhir_resource_id` og `source_resource_id` er altid identiske

**Omfang**: 7.739.382 / 7.739.382 (100%)

**Årsag**: Hardcoded i `fhir.SFI_datapoint_build` view-definitionen:

```sql
-- Observation-side (linje 131-132):
o.[observation_id] AS fhir_resource_id,
o.[observation_id] AS source_resource_id,

-- QR-side (linje 257-258):
qr.[qr_id] AS fhir_resource_id,
qr.[qr_id] AS source_resource_id,
```

Begge kolonner sættes til det samme felt. `source_resource_id` var sandsynligvis tiltænkt at holde den originale IFDB kilde-ID (OBJEKT_ID), men det blev aldrig implementeret.

**Konsekvens**: Kolonnen er komplet redundant. Forbrugere der antager `source_resource_id` er et kilde-system ID (fx IFDB OBJEKT_ID) tager fejl — det er blot en kopi af FHIR resource UUID.

---

### 2. NULL patient_id (238.604 rækker, 3.1%)

**Omfang**: 238.604 rækker mangler patient_id, encounter_id, organization_id og practitioner_id.

**Tidsmæssig fordeling**:

| Periode | Rækker | NULL patient | Andel |
|---------|--------|-------------|-------|
| 2026-05-19 – 2026-05-21 | 238.606 | 238.604 | 99.999% |
| 2026-05-27+ | 7.500.776 | 0 | 0% |

**Root cause**: Verificeret via Q4 — `patient_id` er allerede NULL i `fhir.QuestionnaireResponse`-tabellen. Alle berørte rækker:
- Har `ifdb-qr-*` format ID (legacy, før UUID-generering)
- Er indsat `2026-05-18 23:20:38` (den allerførste bulk extraction)
- Har gyldige `source_aktivitet_id` → IFDB-kilderækken eksisterer

**Fejlmekanisme i pipeline**: `resolve_reference_maps()` i `QuestionnaireResponse_IFDB_assets.py` (linje 114-131) opslår patient_id i FHIR-databasen:

```python
patient_map = fhir_database.resolve_identifier_values(
    resource_type="Patient",
    identifier_system=SYSTEM_IFDB_PATIENT_ID,
    values=patient_ids,
)
```

Ved den første extraction (18. maj) var Patient-resources sandsynligvis IKKE loadet endnu. Opslaget returnerede tom map → `qr["subject"]` blev aldrig sat → `patient_id = NULL` i flat-tabellen.

**Pipeline-svaghed**: Der er ingen guard, retry, eller logging når reference resolution fejler for en stor andel af rækker. En batch med 0% patient-hit bør logge en WARNING og potentielt fejle.

---

### 3. NULL xml_status (678.882 rækker, 8.8%)

**Tidsmæssig fordeling**:

| Periode | Rækker | NULL xml_status | Andel |
|---------|--------|----------------|-------|
| 2026-05-19 – 2026-06-01 | 675.214 | 675.214 | 100% |
| 2026-06-02+ | 7.064.168 | 3.668 | 0.05% |

**Root cause**: `xml_status` stammer fra FHIR extension `sfiInstanceMeta.xmlStatus` på QR items, flattened via SOF view. Hele den første måned har NULL xml_status, hvilket indikerer:
- Enten var `xmlStatus`-extensionen ikke del af QR item-generering i den tidlige kode
- Eller SOF view-definitionen for `questionnaire_response_item_view.json` blev først udvidet til at inkludere xml_status-felterne efter 1. juni

De sporadiske NULL xml_status efter 2. juni (57, 37, 2746, 828 per dag) er sandsynligvis QR items der blev re-processeret uden xml_status extension (edge case i XML parsing).

**Konsekvens**: For de 675K rækker fra maj-batchen ved vi IKKE om XML'en var valid. Rækker der HAR værdier (has_value = true) er sandsynligvis OK, men det kan ikke garanteres.

---

### 4. `ifdb_aktivitet_sfi_id` altid NULL for QuestionnaireResponse

**Omfang**: 100% af QR-rækker (alle ~7.6M)

**Årsag**: Eksplicit hardcoded i build view:

```sql
-- QR-side (linje 269):
CAST(NULL AS NVARCHAR(255)) AS ifdb_aktivitet_sfi_id,
```

Kun Observation-rækker har denne værdi (fra `o.[ifdb_aktivitet_sfi_id]`).

**Konsekvens**: By design. Ikke et dataproblem. Kolonnen er kun relevant for Observation-baserede datapunkter.

---

### 5. `ifdb-qr-*` vs UUID ID-format

**Fordeling**:

| Format | Rækker | NULL patient | NULL xml_status |
|--------|--------|-------------|----------------|
| `ifdb-qr-*` | 397.489 (5.1%) | 2.497 (0.6%) | 4.202 (1.1%) |
| UUID | 7.228.410 (93.4%) | 236.107 (3.3%) | 674.680 (9.3%) |

**Årsag**: `ifdb-qr-*` er legacy format fra tidlig extraction der brugte `f"ifdb-qr-{OBJEKT_ID}"`. Nuværende kode genererer UUIDs (bekræftet via test: `assert not qr["id"].startswith("ifdb-qr-")`).

De 2.497 ifdb-qr-* rækker med NULL patient er fra den allerførste batch (2026-05-18) som beskrevet i fund #2.

**Pipeline-status**: Koden er allerede rettet (UUID-generering). Legacy data skal backfilles.

---

### 6. `value_text = "%"` for passiv rygning

**Årsag**: Direkte kliniker-input fra IFDB. Kæden:

```
IFDB XML → xml_aktivitetsresultat.py (element_vaerdi) → QR item valueString → SOF flattening → value_string → build view value_text
```

Der er ingen validering af fritekst-indhold i pipelinen. Klinikeren har skrevet "%" i feltet for passiv rygning. Andre eksempler inkluderer "nej" (validt) og tomme strings.

**Konsekvens**: Source data kvalitet. Ikke et pipeline-problem, men downstream BI/rapportering bør filtrere/validere string-type svar.

---

## Pipeline-svagheder identificeret

### A. Ingen reference resolution guard

**Placering**: `QuestionnaireResponse_IFDB_assets.py`, `resolve_reference_maps()` + `apply_resolved_refs()`

**Problem**: Hvis `patient_map` returnerer tom (eller næsten tom) fordi Patient-resources ikke er loadet endnu, fortsætter pipelinen gladelig og producerer QRs uden patient-kobling. Der er ingen:
- WARNING log når resolution rate er < X%
- Threshold-baseret fail-safe
- Dependency-check at Patient-assets er materialiseret FØR QR-extraction

**Anbefaling**: Tilføj en preflight-check i QR-asset der verificerer at `fhir_database.resolve_identifier_values` returnerer en non-trivial hit rate (fx > 90%) for patient_ids i batchen. Log WARNING ved lav rate; FAIL ved 0%.

### B. `source_resource_id` aldrig differentieret fra `fhir_resource_id`

**Placering**: `infrastructure/ddl/views/fhir/01_vw_fhir.sql`, linje 131-132 og 257-258

**Problem**: Kolonnen serverer ingen formål. Forbrugere kan fejlagtigt tro det er kilde-system-ID.

**Anbefaling**: Enten:
1. Repurpose `source_resource_id` til at holde IFDB OBJEKT_ID (tilgængeligt via `qr.identifier[0].value` → allerede i flat table som en identifier-kolonne, eller via `source_aktivitet_id`)
2. Eller deprecate/fjern kolonnen fra consumer-viewet

### C. Manglende backfill-mekanisme for reference resolution

**Problem**: Når Patient-resources bliver loaded EFTER QuestionnaireResponse-extraction, er der ingen automatisk re-resolution. De 238K rækker fra den første batch forbliver uden patient-kobling for evigt medmindre et manuelt backfill-job køres.

**Anbefaling**: Implementér et periodisk "reference backfill"-job der:
1. Finder QRs med NULL patient_id men gyldigt source_aktivitet_id
2. Re-resolver patient references via den nu-populerede Patient-tabel
3. Opdaterer `fhir.QuestionnaireResponse.patient_id`
4. Trigger re-refresh af SFI_datapoint_store for de berørte resources

### D. xml_status backfill

**Problem**: 675K rækker fra den første måned har NULL xml_status. Disse QR items eksisterer i FHIR-databasen med (formodentlig) korrekte extensions, men flat-tabellen blev aldrig opdateret.

**Anbefaling**: Re-kør SOF flattening for de berørte QR items (alle med `dw_timestamp_insert` < 2026-06-02). Dette bør ske via den eksisterende monthly SOF-sync mekanisme.

---

## Prioriteret handlingsplan

| # | Handling | Type | Impact | Effort |
|---|----------|------|--------|--------|
| 1 | Tilføj reference resolution guard i QR-assets | Pipeline fix | Forhindrer fremtidig NULL patient | Lav |
| 2 | Backfill patient_id for 238K legacy rækker | Data fix | Retter 3.1% af store | Medium |
| 3 | Backfill xml_status via SOF re-sync | Data fix | Retter 8.8% af store | Lav |
| 4 | Repurpose/fjern `source_resource_id` redundans | Schema cleanup | Klarhed for forbrugere | Medium (breaking) |
| 5 | Dokumentér `ifdb_aktivitet_sfi_id = NULL` for QR | Docs | Fjerner forvirring | Triviel |

---

## Konklusion

Datamodellen og granulariteten er korrekt. De primære problemer stammer fra **temporal ordering i den initielle load** (Patient-resources var ikke klar da QRs blev extractet) og **schema evolution** (xml_status blev tilføjet efter første extraction). Ingen af problemerne indikerer logiske fejl i transform-kæden.

Den vigtigste pipeline-ændring er en **reference resolution guard** der forhindrer at fremtidige bulk loads producerer kontekst-løse datapunkter.
