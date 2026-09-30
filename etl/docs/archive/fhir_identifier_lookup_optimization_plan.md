<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# FHIR Identifier Lookup Optimering Plan

**Status:** Forslag. Ikke implementeret endnu.

**Branch:** Implementeres på ny feature-branch `feature/identifier-lookup-cache-warmup`.

## Formål

Reducere kørselstid for monthly FHIR-pipelines ved at eliminere gentagne identifier-lookups mod Aidbox PostgreSQL inden for samme partition. Konkret observation: `fhir_careplan_monthly` for `2026-04` kører 4-5 timer hvor ~70% af tiden bruges på identifier-opslag der returnerer samme data igen og igen.

## Observation

### Bevis 1 — CarePlan repeterer 12k EoC-opslag pr. chunk

`fhir_careplan_monthly` (partition `2026-04`, 116.803 CarePlans) splittes i 117 chunks á 1.000. Pr. chunk:

```
[IdentifierLookup] Targeted EpisodeOfCare lookup for 12021 values in 2 batches
[IdentifierLookup] Targeted EpisodeOfCare lookup resolved 12021/12021 values in 36.49s
```

Total: 117 × ~36s = ~70 min spildt på samme reference-resolution. Hertil 117 × ~15s for Patient og 117 × ~5s for Organization/Practitioner.

### Bevis 2 — Dependency-fase og upsert-prep dublerer hinanden

I `fhir_aktivitetsresultat_monthly_ifdb` (Encounter-dependency-fase):

| Tidspunkt | Opslag | Tid |
|---|---|---|
| 21.13.06 | Patient 8455 (`[DEPENDENCY] Checking 8455 patients`) | 11.09s |
| 21.13.19 | Patient 8455 (`subject` field reference resolution) | 13.51s |
| 21.12.54 | EpisodeOfCare 8559 (dependency-check) | 1.96s |
| 21.13.36 | EpisodeOfCare 8559 (`episodeOfCare` field) | 16.48s |

Identiske input-værdier, identisk system, ~30s spildt pr. parti. `resolve_missing_patients` ([dependency_resolver.py:44](../../DSA/FHIR/utils/ifdb/dependency_resolver.py#L44)) returnerer allerede et `dict[ifdb_id → fhir_id]`, men callers smider det væk. `prepare_resources_for_upsert` rammer derefter samme SQL igen.

### Bevis 3 — Warning-detektoren bekræfter mønstret

```
[IdentifierLookup] Targeted EpisodeOfCare/urn:source:ifdb:episode_of_care_id lookup
called 3 times in this partition (threshold=2). This is the per-chunk-DB-lookup
pattern that PartitionLookupContext is designed to prevent.
```

Tælleren i [identifier_lookup.py:268](../../core/fhir/identifier_lookup.py#L268) detekterer det. Selve `PartitionLookupContext`-klassen findes ikke i kodebasen — kun warning-detektoren er implementeret.

### Bevis 4 — 98s self-ID lookup på 0 resultater

```
[IdentifierLookup] Targeted Encounter lookup for 9164 values in 1 batches
[IdentifierLookup] Targeted Encounter lookup resolved 0/9164 values in 98.61s
```

Upsert self-ID resolution scanner Aidbox for 9164 nye Encountere; finder 0; bruger 98 sekunder. Korrekt for idempotency men ren spildtid for kendt-nye delta-batches.

## Root cause

**Cache-deling er udokumenteret konvention, ikke kontrakt.**

`fhir_database.resolve_identifier_values()` ([identifier_lookup.py:158](../../core/fhir/identifier_lookup.py#L158)) checker først om en fuld identifier-cache er indlæst i memory:

```python
if not force_reload:
    lookup = self.get_loaded_identifier_cache(...)
    if lookup is not None and _loaded_lookup_is_current(...):
        return _resolve_from_loaded_lookup(...)   # FAST: in-memory hit
return _resolve_identifier_values_direct(...)     # SLOW: targeted SQL
```

Cachen indlæses kun hvis nogen eksplicit kalder `get_identifier_lookups([(resource_type, [systems])])` først. CarePlan gør det aldrig. Aktivitetsresultat gør det i `_pre_resolve_references` ([Observation_IFDB_Aktivitetsresultat.py:2277](../../DSA/FHIR/Observation/Observation_IFDB_Aktivitetsresultat.py#L2277)) — men kun for sine egne refs, ikke for Encounter-dependency-fasen der kører **før** Aktivitetsresultats post.

Resultatet: hver caller-sti der skipper warmup'en falder gennem til targeted SQL pr. opslag. Cache-mekanikken er rigtig; den bruges bare ikke konsekvent.

## Designprincipper

1. **Eksplicit setup-fase pr. partition.** Hver IFDB-asset deklarerer sine reference-resource-typer i én liste. Cache-warmup centraliseres.
2. **Genbrug allerede-resolvede ID'er.** Dependency-resolver returnerer dict; det videresendes til upsert-prep i stedet for at blive smidt væk.
3. **Detektoren skal forblive grøn.** Efter optimering bør warning-tælleren ikke fyre i nogen monthly partition.
4. **Adfærdsparitet på ID-niveau.** Optimering ændrer ikke hvilke FHIR-ID'er der attaches — kun hvor mange gange de slås op.
5. **Ingen ny abstraktion.** `get_identifier_lookups` + `reference_resolve_fn` eksisterer allerede. Planen wires dem sammen, opfinder ikke et `PartitionLookupContext`-klassehierarki.

## Optimeringer

### Optimering 1 — CarePlan setup_fn cache warmup (STORE GEVINST)

**Estimeret gevinst:** 4-5h → 30-60 min for `fhir_careplan_monthly` på `2026-04`-størrelse partitioner.

**Hvorfor virker det:** `run_chunked_transform_post` accepterer en `setup_fn` der køres ÉN gang før chunk-loop'et ([chunked_pipeline.py:158](../../core/parallel/chunked_pipeline.py#L158)). CarePlan kalder den ikke i dag ([CarePlan_IFDB.py:659](../../DSA/FHIR/CarePlan/CarePlan_IFDB.py#L659)). Når caches er pre-loaded, hitter alle 117 chunks in-memory map'en i stedet for SQL.

**Diff (skitse) i [CarePlan_IFDB.py:625-660](../../DSA/FHIR/CarePlan/CarePlan_IFDB.py#L625):**

```python
def _setup_careplan_partition(_rows: list[dict[str, Any]]) -> None:
    fhir_database.get_identifier_lookups([
        ("Patient", [SYSTEM_IFDB_PATIENT_ID]),
        ("EpisodeOfCare", [SYSTEM_IFDB_EPISODE_OF_CARE_ID]),
        ("Organization", [SYSTEM_SOR]),
        ("Practitioner", [SYSTEM_IFDB_STAFF]),
        ("CarePlan", [SYSTEM_CAREPLAN_ALLE_MARKOERER]),
    ], parallel=True)
    return None  # caches lever på fhir_database._identifier_caches

return run_chunked_transform_post(
    CAREPLAN_CHUNK_STAGE,
    rows=careplans_data,
    transform_fn=transform,
    post_fn=post,
    fhir_database=fhir_database,
    context=context,
    setup_fn=_setup_careplan_partition,
)
```

**Sideeffekt:** Fjern `force_reload=True` i [CarePlan_IFDB.py:588](../../DSA/FHIR/CarePlan/CarePlan_IFDB.py#L588). Den invaliderer den lige-warmede cache for self-ID lookup. Ny default: `force_reload=False`.

**Cache-invalidation efter dependency-mutation:** `[SharedCache] Cleared identifier caches for EpisodeOfCare` fyrer kun når nye EoC oprettes. For 2026-04 var det 7 ud af 13.667. Sjælden hændelse; pris er én ekstra cache-rebuild i næste chunk-loop. Acceptabel.

**Risiko:** Cache-warmup loader hele identifier-tabellen for hver type. For CarePlan med 1-2M patienter og ~10M EoC kan det betyde ~256 MB pr. cache (mmap'd). `suspend_identifier_caches()` kaldes allerede pr. chunk ([chunked_pipeline.py:276](../../core/parallel/chunked_pipeline.py#L276)) og frigør mmap-memory mellem chunks — uden at droppe cache-strukturen. Memory-profilen bør verificeres på staging.

### Optimering 2 — Dependency-resolver returnerer ID-dict; upsert-prep genbruger det

**Estimeret gevinst:** ~30s pr. ressource-partition × N-ressourcer (Aktivitetsresultat, Encounter, Condition, Procedure). Sekundær effekt: warning-tælleren falder under threshold for de fleste assets.

**Hvorfor virker det:** `resolve_missing_patients` ([dependency_resolver.py:44](../../DSA/FHIR/utils/ifdb/dependency_resolver.py#L44)) returnerer allerede `dict[str, str]`. Callers ([Encounter_IFDB.py:410](../../DSA/FHIR/Encounter/Encounter_IFDB.py#L410), [Condition_IFDB_PasDiagnose.py:376](../../DSA/FHIR/Condition/Condition_IFDB_PasDiagnose.py#L376), [Condition_IFDB_Diagnose.py:230](../../DSA/FHIR/Condition/Condition_IFDB_Diagnose.py#L230), m.fl.) ignorerer return-værdien. Hvis den i stedet sammenstilles til en `reference_resolve_fn` der lukker over en partition-shared dict, kan `prepare_resources_for_upsert` injecte den via det eksisterende `reference_resolve_fn`-parameter ([fhir_posting.py:376](../../DSA/FHIR/utils/ifdb/fhir_posting.py#L376)).

**Diff (skitse) — pattern for samtlige callers:**

```python
# før
resolve_missing_patients(
    patient_ids=patient_ids_needed,
    ifdb_oracle=ifdb_oracle,
    fhir_database=fhir_database,
    context=context,
    mutation_report=report,
)
# ... senere ...
prepare_resources_for_upsert(resources=..., fhir_database=fhir_database, ...)

# efter
patient_id_map = resolve_missing_patients(...)  # samme kald, brug return
eoc_id_map = resolve_missing_episodes_of_care(...)

partition_resolved_ids = {
    (SYSTEM_IFDB_PATIENT_ID, "Patient"): patient_id_map,
    (SYSTEM_IFDB_EPISODE_OF_CARE_ID, "EpisodeOfCare"): eoc_id_map,
}

def _resolve_with_partition_cache(*, resource_type, identifier_system, values):
    cached = partition_resolved_ids.get((identifier_system, resource_type), {})
    missing = [v for v in values if v not in cached]
    if missing:
        # fall through til DB kun for ikke-allerede-resolvede værdier
        fresh = fhir_database.resolve_identifier_values(
            resource_type=resource_type,
            identifier_system=identifier_system,
            values=missing,
        )
        cached.update(fresh)
    return {v: cached[v] for v in values if v in cached}

prepare_resources_for_upsert(
    resources=...,
    fhir_database=fhir_database,
    reference_resolve_fn=_resolve_with_partition_cache,
    ...
)
```

**Vigtig nuance:** `_resolve_with_partition_cache` er kun nødvendig hvis Optimering 1 ikke fanger samme refs (fx hvis assetet ikke er chunked, eller dependency-fase kører før setup_fn). For chunked assets med korrekt warmup vil dependency-fasen i sig selv hitte den indlæste cache, og dubletteringen forsvinder af sig selv.

**Hvor er det vigtigt:** Encounter-pipelinen der kører som dependency-fase under Aktivitetsresultat — den indlæser ikke caches pr. partition fordi den ikke er chunked på samme måde.

### Optimering 3 — Skip self-ID lookup for kendt-nye delta inserts

**Estimeret gevinst:** ~100s pr. delta-batch hvor self-ID lookup returnerer 0 (fx 9164 nye Encountere på `2026-04`).

**Problem:** [fhir_posting.py:269-307](../../DSA/FHIR/utils/ifdb/fhir_posting.py#L269) (`_resolve_resource_ids_targeted`) kører før hver upsert for at finde eksisterende rows og undgå dublet-insert ved retry. For monthly delta-partitioner er det forventede resultat 0 i 99% af tilfældene.

**Forslag:** Tilføj `skip_self_id_lookup_when_empty: bool = False` på `prepare_resources_for_upsert`. Når `True`, hop over self-ID lookup; lad PostgreSQL's `ON CONFLICT (identifier)` håndtere idempotency direkte i INSERT'et.

**Forudsætning:** Aidbox-tabellerne har et UNIQUE-constraint eller `ON CONFLICT`-target på (resource_type, identifier_system, identifier_value). Hvis ikke, er optimering ikke sikker — verificer via [infrastructure/ddl/migrations/](../../infrastructure/ddl/migrations/).

**Risiko:** Hvis constraint ikke findes, kan retry skabe dubletter. Skal være opt-in pr. asset, ikke default.

### Optimering 4 — Tunable chunk size

**Estimeret gevinst:** 117 chunks → 12-24 chunks reducerer per-chunk-overhead (logging, suspend/resume mmap, dependency-fase) med faktor ~5.

**Hvorfor:** `IFDB_CAREPLAN_PIPELINE_CHUNK_SIZE` env var styrer allerede chunk-størrelsen ([CarePlan_IFDB.py:84](../../DSA/FHIR/CarePlan/CarePlan_IFDB.py#L84)). Default 1.000 er konservativt; med Optimering 1 på plads er per-chunk-overhead lavt nok til at 5.000-10.000 er sikkert.

**Forslag:** Sæt `IFDB_CAREPLAN_PIPELINE_CHUNK_SIZE=5000` i staging-env. Mål memory og chunk-tid. Hvis OK, rul ud til prod.

**Risiko:** Store chunks → større Pydantic-objekt-lister i memory → potentielt OOM. `suspend_identifier_caches()` mellem chunks hjælper ikke når selve resource-listen i `transform_fn → post_fn` overlever.

### Optimering 5 — Aktivitetsresultat: pre-resolve refs FØR dependency-fasen

**Estimeret gevinst:** ~30-60s pr. partition.

**Problem:** [Observation_IFDB_Aktivitetsresultat.py:2535](../../DSA/FHIR/Observation/Observation_IFDB_Aktivitetsresultat.py#L2535) kalder `_pre_resolve_references` **efter** Encounter-dependency-fasen er kørt. Dep-fasen indlæser ingen cache; hits derfor DB.

**Forslag:** Flyt `_pre_resolve_references`-kaldet før dep-fasen, eller udvid `cache_specs` så dep-fasens lookups (Patient + EoC for Encounter-dependencies) også er pre-loaded.

**Risiko:** Lav. Dep-fasen og post-fasen bruger samme `fhir_database._identifier_caches`-instans; en cache loaded først hitter også dep-fasens kald.

## Hvad denne plan IKKE gør

| Forkastet | Begrundelse |
|---|---|
| Indfør `PartitionLookupContext`-klasse | Kun warning-detektor refererer navnet. Eksisterende `get_identifier_lookups` + in-memory `_identifier_caches` dict opfylder samme funktion. Ny klasse flytter samme state ind i ny indpakning. |
| Parallelisér chunks i `run_chunked_transform_post` | Krav: thread-safe write til Aidbox + ny core-engine. Stor refactoring; lille gevinst når Optimering 1 er på plads. Ikke i scope. |
| Refaktorér Practitioner-debt (2.572 unresolvable refs pr. chunk) | Datakvalitetsspørgsmål, ikke caching. Egen task. |
| Sletning af `_resolve_resource_ids_targeted` | Den er korrekt for self-ID upsert; kun skipbar når asset er kendt-delta. Optimering 3 gør den opt-in skipbar, ikke fjernet. |
| Erstat `resolve_identifier_values` cache-check med ny mekanik | Eksisterende mekanik er korrekt. Problemet er at warmup ikke kaldes konsekvent. |

## Implementeringsrækkefølge

Rækkefølge afspejler impact og risiko. Hver optimering kan landes som selvstændig PR.

1. **Optimering 1 (CarePlan setup_fn).** Lille diff (~20 linjer), stor gevinst. Test på staging mod 2026-04-partition; bekræft warning-tæller falder under threshold; bekræft kørselstid <60 min.
2. **Optimering 5 (Aktivitetsresultat pre-resolve order).** Selvstændig diff i én fil. Bekræft ingen ny dep-fase warning.
3. **Optimering 4 (chunk_size bump).** Env-var ændring, ingen kode-diff. Mål memory på staging først.
4. **Optimering 2 (dependency-resolver pass-through).** Større diff (rør ~7 callsites). Anbefales ikke før 1+5 har bevist hovedmønstret virker. Kan delvis være redundant efter 1.
5. **Optimering 3 (skip self-ID for delta).** Kræver verifikation af DB-constraints før det er sikkert. Lavest prioritet.

## Test

### Per-optimering acceptkriterie

| Optimering | Test |
|---|---|
| 1 | `fhir_careplan_monthly` (2026-04) kørselstid < 60 min; ingen `[IdentifierLookup] ... called N times` warning for Patient/EoC/Org/Practitioner; samme antal CarePlans posted som baseline |
| 2 | Warning-tælleren forbliver under threshold for Encounter-dep-fasen i Aktivitetsresultat |
| 3 | Encounter dep-fase i Aktivitetsresultat: ingen 0/N self-ID lookup (eller den er <5s) |
| 4 | Memory under chunk-loop overstiger ikke baseline + 50%; chunk-tid pr. chunk skalerer lineært med chunk_size |
| 5 | `_pre_resolve_references` log-linje ses før første `[DEPENDENCY]` log i Aktivitetsresultats run |

### Eksisterende tests skal forblive grønne

- `tests/unit/test_core_chunked_pipeline.py`
- `tests/unit/test_careplan.py`
- `tests/unit/test_encounter_pipeline.py`
- `tests/integration/test_aktivitetsresultat_chunked_parity.py`
- `tests/integration/test_cache_chunked_pipeline_resilience.py`

### Verifikation

```bash
cd etl
python -m pytest tests/unit/test_core_chunked_pipeline.py tests/unit/test_careplan.py \
  tests/unit/test_encounter_pipeline.py \
  tests/integration/test_aktivitetsresultat_chunked_parity.py
```

Plus end-to-end run mod staging Aidbox med samme partition som baseline.

## Acceptkriterier (Definition of Done)

1. `fhir_careplan_monthly` for typisk månedspartition (~100k CarePlans) kører på <60 min.
2. Warning `[IdentifierLookup] ... called N times in this partition` fyrer ikke for Patient, EpisodeOfCare, Organization eller Practitioner i nogen monthly run.
3. `setup_fn`-mønstret er dokumenteret i `core/parallel/README.md` som kanonisk pattern for IFDB-assets.
4. Eksisterende unit + integration tests grønne.
5. Ingen ændring af FHIR-output på resource-niveau (samme ID'er attaches, samme antal resources posted).

## Rollback

Hver optimering er isolérbar:

1. **Optimering 1:** Fjern `setup_fn=_setup_careplan_partition` linjen. Pipeline falder tilbage til per-chunk targeted lookup (= status quo i dag).
2. **Optimering 2:** Slet `reference_resolve_fn`-parameter i kaldsstedet; revert til at smide dep-resolver returnen væk.
3. **Optimering 3:** Sæt `skip_self_id_lookup_when_empty=False` (default). Self-ID lookup kører igen.
4. **Optimering 4:** Unset env var.
5. **Optimering 5:** Flyt `_pre_resolve_references`-kald tilbage til original position.

Ingen persistence-, cache- eller schema-ændringer involveret. Cache-filer på disk er backwards-compatible.

## Ikke-mål

- Ingen ændring af `core.parallel.chunked_pipeline.run_chunked_transform_post` engine.
- Ingen ændring af FHIR identifier-system konventioner.
- Ingen ny core-abstraktion (`PartitionLookupContext`-klasse, `IdentifierCacheManager` etc.).
- Ingen migrering af Aktivitetsresultat ind i fælles runner — den forbliver bespoke (den refererede `fhir_chunked_pipeline_v2_plan.md` blev aldrig skrevet).
- Ingen håndtering af Practitioner-datakvalitetsdebt (2.572 unresolvable refs pr. chunk) — egen task.
