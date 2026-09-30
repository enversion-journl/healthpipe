<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Overdragelse: Encounter duplicate key fejl + cache refactor — 2026-06-09

## Baggrund

Branch `feature/sfi-datapoint-performance` har i dag modtaget en række commits
der refactorerer cache-invalidation i FHIR posting-pipeline. Under test-deploy
er der opstået fejl der skal analyseres og løses.

---

## FAKTA (verificeret med data og logs)

### 1. Succesfuldt run kl. 11:08
- Run `84d130a0`, partition `2026-06-09`, job `fhir_etl_ifdb_daily_job`
- Alle assets grønne, inkl. `fhir_aktivitetsresultat_sfi_daily_ifdb` og `fhir_ris_ydelse_daily_ifdb`
- Kørte med kode FØR dagens commits (første commit var kl. 12:47)

### 2. Fejlende run kl. 16:43
- Run `d9f849b9`, partition `2026-06-09`, samme job
- Kørte med kode EFTER dagens commits (code-serveren opdateres ved push)
- Fejl i `fhir_aktivitetsresultat_sfi_daily_ifdb` og `fhir_ris_ydelse_daily_ifdb`
- `fhir_pas_diagnose_daily_ifdb` var grøn

### 3. Fejlbeskrivelse
- `duplicate key value violates unique constraint "idx_encounter_urn_source_ifdb_object_id_uniq"`
- `DETAIL: Key (extract_identifier_value(resource, 'urn:source:ifdb:object_id'::text))=(100103944724) already exists`
- Dependency resolver (`resolve_missing_encounters`) finder encounters som "missing" via targeted lookup, prøver at oprette dem, men UNIQUE index siger de allerede eksisterer
- Alle efterfølgende inserts i samme transaktion fejler med `current transaction is aborted`

### 4. Postgres data-verifikation (kørt mod aidbox)
- **Ingen duplikat-identifiers** på Encounter-tabellen: `SELECT count(*) ... WHERE count > 1` → **0**
- Encounteren `100103944724` **eksisterer** i Postgres med korrekt identifier
- FHIR ID er `78175d64-...`, men dependency resolver genererede et nyt UUID `a37e4aeb-...`
- `EXPLAIN ANALYZE` viser **Index Scan** (korrekt) for targeted lookup med 1 value (9ms)
- `EXPLAIN` med 10.000 values viser også **Index Scan** (planlægger dropper ikke indexet)
- Encounter-tabel: **12.825.073 rows**, `count(*)` query tog 94 sekunder

### 5. Job-konfiguration
- `max_concurrent = 4` (linje 32 i `jobs/fhir_ifdb_jobs.py`)
- Assets kører parallelt i `multiprocess_executor`
- Både `fhir_aktivitetsresultat_sfi_daily_ifdb` og `fhir_ris_ydelse_daily_ifdb` kalder `resolve_missing_encounters()` som dependency resolution

### 6. Kode-diff vs development
- `Encounter_IFDB.py` — **nul diff** (identisk med development)
- `direct_upsert.py` — **nul diff**
- `identifier_lookup.py` — **nul diff**
- `transforms.py` (`normalize_oracle_id`) — **nul diff**
- `resolve_missing_encounters()` i `dependency_resolver.py` — kun tilføjet `mark_stale()` + `safe_clear()` EFTER posting (linje 742-744). Selve lookup + insert logik er uændret.

### 7. Dagens commits (kronologisk)
```
63b0d2ae 10:49 feat(aktivitetsresultat): emit Condition side-output
208eefe7 12:32 perf: skip QR preflight
04ca6e83 12:39 perf: pre-pivot sfi_properties
0e0a47a1 12:47 perf: batch incremental SFI_datapoint apply
f9999fa8 14:41 fix: batch durability, SKS-D normalization
0ad1e6e9 14:45 refactor: remove dead code paths
ab6a66d4 14:48 fix: xml_status guard
55b6d641 14:49 docs: watermark-race limitation
35cbd924 15:25 fix: propagate force_reload through reference resolution
cb494398 15:48 refactor: centralize cache invalidation via stale-tracking  ← DENNE
0ad791ab 15:59 fix: invalidate Patient cache on any CPR-bridge match
9824bae3 16:19 fix: CPR-bridge strip+append
```

---

## HVAD STALE-TRACKING REFACTOREN ÆNDREDE (commit cb494398)

### I posting-funktionerne (7 filer: Condition×3, EoC, Procedure×2, Observation):
- Fjernede `DependencyMutationReport` import og instantiering
- Fjernede `mutation_report=mutation_report` fra alle `resolve_missing_*()` kald
- Fjernede `deps_mutated` flag og `force_reload=deps_mutated` fra `prepare_resources_for_upsert()`
- Fjernede `safe_clear_identifier_caches_for_resources()` kald i posting-funktionerne

### I `dependency_resolver.py`:
- Tilføjede `fhir_database.mark_stale()` + `fhir_database.safe_clear_identifier_caches_for_resources()` direkte i resolverne (Patient, Encounter, EpisodeOfCare)
- `mutation_report.mark()` kald er bevaret for bagudkompatibilitet

### I `FHIRDatabaseResource` (resources.py):
- Ny `_stale_resource_types: set[str]` + `mark_stale()` / `is_stale()` metoder
- `resolve_identifier_values()` og `resolve_resource_ids()` auto-OR'er `force_reload` med `is_stale()`

### I `references.py` (Aktivitetsresultat):
- `_ensure_reference_dependencies()` sender IKKE længere `mutation_report` til resolve_missing_* kald

### Konsekvens for Encounter dependency resolution:
- `resolve_missing_encounters()` modtager nu `mutation_report=None` (var tidligere en `DependencyMutationReport()` instans)
- Linje 738-739: `if mutation_report is not None: post_kwargs["dependency_mutation_report"] = mutation_report` → `_post_encounters_to_fhir` modtager IKKE længere `dependency_mutation_report`
- `_post_encounters_to_fhir` sender dermed ikke `mutation_report` videre til sin interne `resolve_missing_patients()` og `resolve_missing_episodes_of_care()`
- **BEMÆRK**: dette ændrer IKKE selve Encounter insert-logikken. (Rettet 2026-06-09 aften:
  den oprindelige formulering "men kan ændre timing" var en hypotese uden kodegrundlag —
  Encounter-eksistensopslaget går via den indekserede direct-path som ALTID læser live
  Postgres (`identifier_lookup.py:190-191`, force_reload er et dokumenteret no-op dér),
  og `_stale_resource_types` er et in-process set der hverken kan forårsage eller
  helbrede en cross-process kollision. Refactoren er inert for Encounter-resolution.)

---

## HYPOTESER (ikke verificeret)

### Hypotese A: Parallel race condition (timing-ændring)
- Stale-tracking refactoren ændrede execution path (færre kald, hurtigere completion)
- To parallelle assets (`aktivitetsresultat` + `ris_ydelse`) kører begge `resolve_missing_encounters` med overlappende encounter IDs
- Begge finder de nye encounters som "missing" (korrekt — de er nye IFDB data fra efter kl. 11)
- Begge prøver at oprette dem → den ene vinder → den anden får `duplicate key`
- Race har altid eksisteret, men timing-ændringen gør at den nu trigger

### Hypotese B: Ny IFDB-data mellem kl. 11 og 16
- Partition `2026-06-09` er *dagens* Dato
- Kl. 11 extractede IFDB data op til det tidspunkt
- Kl. 16 extractede IFDB data med nye encounters oprettet mellem 11 og 16
- Disse nye encounters mangler i FHIR → dependency resolver prøver at oprette dem
- Kl. 11-runnet havde ingen "missing" encounters → ingen race
- Kl. 16-runnet har nye "missing" encounters → to assets rammer dem parallelt

### Hypotese C: Ukendt årsag
- Targeted lookup finder 14.557/17.179 (2.622 missing) — men encounterne eksisterer
- EXPLAIN + direkte SQL viser at index og data er korrekt
- Der kan være en ukendt faktor (Postgres connection pool, statement timeout, memory pressure fra 12.8M row tabel) der forårsager partial results fra targeted lookup
- Men dette er IKKE påvist

---

## SEPARATE PROBLEMER (Patient CPR-bridge)

### Patient 3283517599 (EpisodeOfCare job)
- Patient resolved via CPR-match men ikke fundet via IFDB_PATIENT_ID targeted lookup
- Tre gange i træk med fresh SQL — konsekvent 9400/9401
- `_persist_cpr_matched_identifiers` siger `0/1 added` — identifier "allerede der"
- **Mulig root cause**: Patienten har to `urn:source:ifdb:patient_id` identifiers (gammel + ny), og `extract_identifier_value LIMIT 1` returnerer den gamle
- **IKKE bekræftet** med SQL (vi har kun kørt duplikat-check på Encounter, ikke Patient)
- Commits `0ad791ab` og `9824bae3` adresserer dette med hhv. `cpr_matched` guard og strip+append SQL

---

## ÅBNE HANDLINGER

1. **Undersøg om Patient-tabellen har duplikat-identifiers** (kør samme count-query som for Encounter)
2. **Afgør om Encounter-fejlen er en race condition** — kør jobbet med `max_concurrent=1` og se om det lykkes
3. **Hvis race**: gør `_post_encounters_to_fhir` (eller dependency resolver) robust over for `duplicate key` — f.eks. catch exception og re-resolve i stedet for crash
4. **Overvej rollback** af stale-tracking refactoren (commit `cb494398`) hvis det er nødvendigt at unblokere test-miljøet
5. **Deploy og test Patient CPR-bridge fix** (`9824bae3`) separat

---

## FILER DER ER ÆNDRET I BRANCHEN (relevant for fejlen)

**Rettet 2026-06-09 aften:** tabellen blandede oprindeligt commit `cb494398`s sletninger
sammen med hele branchens diff (references.py/orchestrator.py bærer også de store
SFI-perf-rewrites: +448/+405 linjer vs development). Tallene nedenfor er PR. COMMIT
`cb494398` (+`35cbd924`), ikke hele branchen. `fhir_posting.py` manglede helt.

| Fil | Ændring (cb494398/35cbd924) | Omfang i de to commits |
|---|---|---|
| `core/resources.py` | `_stale_resource_types`, `mark_stale()`, `is_stale()`, auto-stale i resolve | +22 linjer (cb494398) |
| `DSA/FHIR/utils/ifdb/dependency_resolver.py` | `mark_stale()` + `safe_clear` direkte i resolverne | +25/-5 (cb494398) |
| `DSA/FHIR/utils/reference_resolver.py` | `force_reload` parameter propagation | +13 linjer (35cbd924) |
| `DSA/FHIR/utils/ifdb/fhir_posting.py` | `prepare_resources_for_upsert` forwarder nu `force_reload` til `resolve_and_apply_all_references` — **den ene branch-ændring der påvirker Encounter-postingens reference-resolution** (selv om Encounter_IFDB.py er nul-diff) | +1 linje (35cbd924) |
| `DSA/FHIR/Aktivitetsresultat/references.py` | Fjernet `mutation_report` fra `_ensure_reference_dependencies`-kald | -17/+? (cb494398-andelen) |
| `DSA/FHIR/Aktivitetsresultat/orchestrator.py` | Fjernet `DependencyMutationReport` import + brug | -13 (cb494398-andelen) |
| 7 posting-filer (Condition×3, EoC, Procedure×2, Observation) | Fjernet `deps_mutated` + `DependencyMutationReport` boilerplate (Observation havde dog `force_reload=True`, aldrig `deps_mutated`) | ~-85 linjer |
| `Encounter_IFDB.py` | **Nul diff** | Identisk |
| `direct_upsert.py` | **Nul diff** (pr. overdragelsestidspunktet; se efterskrift) | Identisk |
| `identifier_lookup.py` | **Nul diff** | Identisk |

---

## EFTERSKRIFT 2026-06-09 AFTEN — verificeret konklusion + landede fixes

Uafhængig audit af koden (line-level verifikation af alle påstande ovenfor) gav:

### Root cause (verificeret i kode, ikke kun hypotese)

**En præeksisterende cross-process check-then-insert (TOCTOU) race i Encounter-create-stien
— IKKE en regression fra cache-refactoren:**

1. Fire assets (aktivitetsresultat, ris_ydelse, admin_proc, pas_diagnose) kalder
   `resolve_missing_encounters()` parallelt i separate processer (`max_concurrent=4`)
   med overlappende encounter object_ids og ingen cross-process-lås.
2. Eksistensopslaget læser live Postgres (indekseret direct-path) — begge processer
   kan se en *ny* encounter som "missing".
3. Begge minter hver sit UUID og INSERT'er. `ON CONFLICT (id)` dækker kun
   primærnøglen, så taberen rammer det sekundære unique-index
   (`idx_encounter_urn_source_ifdb_object_id_uniq`) → rå duplicate key.
4. Ingen savepoint pr. række → én kollision satte hele transaktionen i
   abort-tilstand → alle efterfølgende inserts fejlede ("current transaction is
   aborted") — det observerede symptom.

"Hvorfor i dag" = Hypotese B (ny IFDB-data mellem 11:08- og 16:43-runnet) der for
første gang sendte encounters gennem den parallelle create-sti under kontention.
Hypotese A's mekanisme er korrekt, men "timing-ændringen fra refactoren" som
trigger er afkræftet (refactoren er inert for Encounter, jf. rettelsen ovenfor).
Hypotese C er afkræftet i kode (`_resolve_identifier_values_direct` rejser på fejl
frem for at returnere partielle resultater).

### Fixes landet på branchen (samme aften)

| Fix | Hvad |
|---|---|
| Savepoint-isolation | `direct_upsert.py`: per-row fallback wrapper hver insert i `conn.transaction()` (SAVEPOINT) — én kollision kan ikke længere vælte batchen. Unique-violations (SQLSTATE 23505) logges distinkt med constraint-navn; resolverens efterfølgende `force_reload`-re-resolve mapper til den eksisterende række (selvhelende). |
| Revert af `9824bae3` | CPR strip+append-committen er rullet tilbage: den var committet med 3 røde tests, root cause var ubekræftet, og replace-semantikken introducerede en flip-flop-risiko for personer med flere IFDB-patient-ids (gammel/ny id ville skiftevis slette hinanden run for run) plus cross-row UNIQUE-kollisionsrisiko. |
| `0ad791ab` beholdt | Over-invalidering af Patient-cache ved CPR-match er harmløs og beholdes. |

### Udestående (separat opgave)

- **Multi-identifier-design for Patient**: datamodellen kan kun opslå én
  identifier pr. system (`extract_identifier_value` LIMIT 1 + unikt
  expression-index). Personer med flere IFDB-patient-ids kan ikke repræsenteres
  korrekt uanset append/replace. Kræver designet fix i opslags-/indekslaget.
- **Billig diagnostik der bekræfter racen empirisk**: kør
  `SELECT ts, txid, status FROM encounter WHERE id = '78175d64-...'`
  og sammenhold ts/txid med 16:43-runnets start. Inde i run-vinduet = race
  bekræftet direkte. (Fixet ovenfor er korrekt uanset udfald.)
- `max_concurrent=1` -reruns er IKKE længere nødvendige som unblock —
  savepoint-fixet neutraliserer kollisionen.
