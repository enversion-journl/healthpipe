<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Data Quality Assurance (DQA) Framework

Config-driven data-quality checks for the HealthPipe platform: one
`DqaSubject` per table, a small registry of reusable check types, and a factory
that emits Dagster anchor assets + bound `@asset_check`s. Results persist to
`dqa.Results`; dangling FHIR references are captured for semi-automatic replay.

This doc describes the framework **as built**; see "Status / not yet done" at the
end for what remains. New to DQA? Start with **Overview** just below.

---

## Overview — what this is (start here)

DQA is a set of **automated data-quality checks** that run after the ETL. Each
check answers one question — *did this data arrive correctly and stay consistent
across the pipeline's layers?* — and runs as a Dagster `@asset_check`, so results
(pass/fail + severity) show up natively in the Dagster UI and are also written to a
`dqa.results` table for dashboards.

**Examples of questions it answers**

- Does every `fhir.Encounter.patient_id` still point at a `fhir.Patient` that exists
  (no orphaned reference after a duplicate patient was deleted)? — `referential_integrity`
- Does every `DW_EK_Kreditor` on `fct.Faktura` resolve to a row in `dim.Kreditor`? — `referential_integrity`
- Did each month's invoice lines flow all the way Source → DSA → DSO without loss? — `completeness`
- Is every record in `arc.encounter_ifdb` present (and delete-consistent) in flat `fhir.Encounter`? — `source_reconciliation`
- Are there flat `fhir.*` rows whose resource was deleted on the FHIR server but never propagated? — `fhir_server_reconciliation`
- After the DSO-sync, does each DSA table match its DSO copy row-for-row? — `dso_reconciliation`
- Any duplicate business keys? — `uniqueness`
- How often does a fact fall back to the `-1` unknown-member key (lookup coverage)? — `sentinel_rate`

**Where the checks sit** — every hop between pipeline layers has a check that guards it:

```
  Source systems:  IFDB (Oracle)  ·  ERP (MSSQL)
        │
        │  archive_reconciliation  — arc.* holds every current source key; deletes propagated
        ▼
  arc.*  (archive, on DSA)
        │
        │  source_reconciliation   — flat fhir.* business keys match arc.*
        ▼
  FHIR server (Aidbox)  ──▶  fhir_server_reconciliation  — flat rows for resources deleted on the server
        │
        ▼
  flat fhir.*  +  fct / dim   (DSA)
        │   uniqueness · referential_integrity · sentinel_rate  — intra-DSA integrity
        │   completeness                                        — Source→DSA→DSO row-count parity
        ▼
  DSO  (reporting)
            dso_reconciliation     — every DSA row present & delete-consistent in DSO
```

**How you set one up** — a check is pure config: add a `DqaSubject` (one table + its
checks) to a `config/*.py` list. The factory turns it into the anchor asset +
`@asset_check`s automatically, and weekly referential checks are derived from the
lineage YAMLs, so you never hand-list FKs:

```python
# dqa/config/fhir_subjects.py — one subject == one table + its checks
DqaSubject(
    name="fhir_encounter",
    track=Track.FHIR,
    table="fhir.Encounter",
    resource_key="mssql_target_staging",
    identity_column="encounter_id",
    deleted_column="is_deleted",  # row-level checks scope to active (non-deleted) rows
    checks=(
        CheckSpecification("uniqueness", Severity.ERROR, Cadence.DAILY, {"grain": "identifier_value"}, name_suffix="business_key"),
        _reconcile_vs_archive("arc.encounter_ifdb", capture_replay=True),  # source_reconciliation vs the archive
    ),
    # weekly referential_integrity checks are added automatically from the FHIR lineage FK-Targets
)
```

See **Adding a subject or check** near the end for the full recipe. The rest of this
document is reference detail; the sections below go layer by layer.

---

## Package layout (`etl/dqa/`)

**Contributor reference** — where each piece of the code lives. Skip this if you just
want to understand what DQA does (see Overview above); it's a map for people editing
the framework.

| Module | Responsibility |
|---|---|
| `model.py` | Core types: `Severity`, `Cadence`, `Track`, `CheckSpecification`, `DqaSubject`, `ReplaySpec`, `CheckOutcome`. |
| `registry.py` | `CHECK_TYPES` registry + `@register_check`. |
| `check_config.py` | `DqaCheckConfig` — optional per-run `max_keys` override for the remediation-capture guard. |
| `factory.py` | `build_all(subjects)` → one anchor `@asset` per (subject, cadence) + the bound `@asset_check`s. |
| `result.py` | `CheckOutcome` → `AssetCheckResult` (+ always-on best-effort INSERT to `dqa.Results`, via `results_resource_key`). |
| `references/lineage.py` | Loads FK edges from `data_lineage/**` (FHIR `FK-Target`, DWH `DW_EK_`); `referential_specs(table)`. |
| `checks/` | The reusable check implementations (one module each) + `helpers.py`. |
| `config/` | The subject configs: `fhir_subjects.py`, `dwh_subjects.py`, `arc_subjects.py` → `ALL_SUBJECTS`. |
| `dso.py` | DSO-vs-DSA reconciliation subsystem: derives subjects from `ALL_SYNC_CONFIGS`, builds per-domain anchors + checks + jobs. |
| `remediation/` | `capture.py` (queue dangling refs + source/oracle-reconciliation gaps) + `replay.py` (semi-auto DFM replay) + `arc_replay.py` (ids-mode arc re-extract) + `fhir_server_cleanup.py` + `patient_delete_propagation.py` + `arc_vanished_cleanup.py` (operator soft-delete assets). |
| `reporting/summary.py` | `dqa_summary` rollup asset over `dqa.Results`. |
| `jobs.py` / `schedules.py` / `sensors.py` | Per-(track, cadence) jobs, weekly schedules, post-ETL daily sensors. |

Wired into `definitions.py`: `dqa_assets`, `dqa_asset_checks`, `dqa_summary`,
`dqa_jobs`, `dqa_schedules`, daily sensors; the DSO subsystem (`dso_assets`,
`dso_asset_checks`, `dso_jobs`); and the operator remediation assets/jobs
(`dqa_remediation_replay`, `dqa_arc_ifdb_replay`, `dqa_arc_ifdb_vanished_cleanup`,
`dqa_fhir_server_cleanup`, `dqa_patient_delete_propagation` + their `_job`s).

---

## Core model

- **`Severity`** — `BLOCKING` (AssetCheckSeverity.ERROR + `blocking=True`), `ERROR`
  (red, non-blocking), `WARN`, `INFO` (always passes; observability only).
- **`Cadence`** — `DAILY` (cheap intra-table, runs after the ETL via sensor),
  `WEEKLY` (expensive cross-table / cross-DB, Saturday cron), and `DSO` (DSO-vs-DSA
  post-sync reconciliation — its own `dqa_dso_<domain>` jobs, see the DSO section).
- **`Track`** — `FHIR` (flat `fhir.*`), `DWH` (ERP facts/dims), `ARCHIVE` (`arc.*`
  vs its source). Each track gets its own Dagster group (`dqa_fhir` / `dqa_dwh` /
  `dqa_arc`) and its own job(s); the DSO subsystem adds a separate `dqa_dso` group.
  A subject may set `source` (e.g. `ifdb`) to split a multi-source track's jobs
  per source — the ARCHIVE track does this (`dqa_arc_ifdb_weekly_job`).
- **`CheckSpecification`** — one configured check: `check_type`, `severity`, `cadence`,
  `params`, optional inline `fn` (custom callable), `requires` (extra resource keys).
- **`DqaSubject`** — one table asserted about: `track`, `table`, `resource_key`,
  `identity_column`, `checks`, optional `replay` (`ReplaySpec`), `deleted_column`
  (soft-delete column → scopes row-level checks to active rows), `results_resource_key`
  (where its `dqa.Results` row is written; defaults to `resource_key`).
- **`CheckOutcome`** — normalized result every check returns (`passed`, `checked`,
  `violations`, `examples`, `extra`); `result.py` converts it once.

The unit is `@asset_check`, one **anchor asset per subject per cadence**
(`dqa_<subject>_<cadence>`), with the checks bound to it — so selecting an anchor
in a job runs exactly that track+cadence's checks, and they appear natively in
Dagster's check UI with real severities.

---

## Check types (12)

The **Scope** column shows which track/layer each check applies to: **FHIR**
(flat `fhir.*`), **DWH** (ERP facts/dims), **Archive** (`arc.*` vs source), **DSO**
(all synced domains), or **Any** (generic, reusable across tracks).

| Type | Scope | Cadence | What it asserts |
|---|---|---|---|
| `not_null` | Any | daily | listed columns are non-null (active rows*) — currently unused: FHIR required refs are DDL-`NOT NULL`, DWH uses `sentinel_rate` |
| `sentinel_rate` | DWH | daily | rate of `-1` unknown-member values per `DW_EK_` FK column (NULL is impossible — DDL `NOT NULL DEFAULT -1`); observability until a cap is set in `dwh_subjects._SENTINEL_MAX_RATES` (per fact, or per column via a dict) |
| `uniqueness` | Any | daily | a grain (single/composite) is unique (active rows*) |
| `referential_integrity` | FHIR + DWH | weekly | every non-null child FK resolves to a parent row, same DB (active child rows*) |
| `source_reconciliation` | FHIR | weekly | flat FHIR business-key set matches its `arc.*` source (incl. soft-delete agreement; a key is "live" if **any** non-deleted flat row exists, so a soft-deleted cleanup straggler sharing a business key doesn't false-flag) |
| `completeness` | DWH | weekly | monthly row-count parity Source → DSA → DSO per fact |
| `archive_reconciliation` | Archive | weekly | an `arc.*` archive matches its **DFM source** (cross-DB; source `dialect` = oracle/mssql + `source_resource` + `flow_start_state` are params — IFDB Oracle today, SydAX/MySupply-ready) |
| `fhir_server_reconciliation` | FHIR | weekly | flat `fhir.*` matches the **FHIR server** (Aidbox/Postgres) — flags rows deleted on the server but not propagated (cross-DB) |
| `dso_reconciliation` | DSO | dso | a **DSO** table matches its **DSA** (staging) source after the DSO-sync — diffs on the full PK + delete status over the `STAGING_LINK` linked server (runs on DSO, results to DSA) |
| `patient_arc_integrity` | FHIR | weekly | dedup-aware Patient `ifdb_patient_id` → `arc` check (orphan / kept-deleted-dup / delete-mismatch) |
| `custom_sql` | Any | any | escape hatch: a configured SQL predicate |
| `chain` | FHIR | weekly | adapter over `core.chain_validation` — **not yet wired** (see Status) |

\* The row-level checks (`not_null`, `uniqueness`, `referential_integrity`) scope
to **active rows** when the subject sets `deleted_column` (e.g. `dw_is_deleted`) — so
a correctly soft-deleted row (e.g. retired by `dqa_fhir_server_cleanup`) doesn't
keep failing. `uniqueness` can opt out per check with an explicit
`deleted_column=None` param (strict, all-rows) — used for the resource-id grains,
where even a soft-deleted duplicate id would be a projection bug. The
reconciliations (`source_`/`archive_reconciliation`) deliberately compare the
**full** key set incl. deleted rows, to verify delete *propagation*.
`fhir_server_reconciliation` treats **0 live server ids** as an anomaly: the diff
is skipped (no false 100%-stale) but the check **fails** at its severity so the
anomaly is visible in `dqa.Results`/`dqa_summary`.

**`not_null` vs. a DDL `NOT NULL`.** Where the schema *can* enforce a required
column, it does — the required FHIR patient references (`FK-Required: true` in
lineage) are `NOT NULL` in the DDL (migration
`20260707_fhir_required_patient_refs_not_null`; the posting path already hard-fails
on an unresolved required reference), so the FHIR subjects carry **no** `not_null`
checks. The check type remains for business-required columns the schema
deliberately leaves nullable — currently none, so no subject carries a `not_null`
check. The DWH `DW_EK_` columns are `NOT NULL DEFAULT -1` in the fact DDL, so their
signal is the `-1` unknown-member *rate* instead — measured daily by
**`sentinel_rate`** (auto-generated per fact from the `DW_EK_` lineage, PK
excluded). It runs as observability (`Severity.INFO`, rates in
`dqa.results`/metadata) until baselines are known; tighten by adding an entry to
`dwh_subjects._SENTINEL_MAX_RATES` — a float caps all of a fact's FK columns, a
`{column: rate}` dict caps per column (unlisted columns stay observability-only),
and an entry automatically flips that fact's check to WARN so a breached cap flags.

Every check's Dagster `@asset_check` description carries a compact "which columns"
hint (e.g. `arc.ifdb_patient.CPR vs fhir.Patient.patient_ssn`, with "(active rows
only)" where it applies) so it's evident what it compares without reading the config.

Add a reusable type by dropping a module in `checks/` that calls `@register_check`
and importing it in `checks/__init__.py`. One-offs: `custom_sql`, or an inline
`fn` on the `CheckSpecification`.

---

## Relationships come from the lineage YAMLs (single source of truth)

No separate FHIR registry (avoids drift). `references/lineage.py` reads:
- **FHIR**: explicit `FK-Target` / `FK-Key` / `FK-Required` / `FK-Ignore` on
  reference columns in `data_lineage/fhir/*.yml`. Flat `*_id` columns hold Aidbox
  **UUIDs**; the business key lives in `identifier_value` / `ifdb_*`.
- **DWH**: `Type: FK` columns via `FK-Target` or the `DW_EK_<Dim>[_role]` naming
  convention (parent key = `DW_EK_<Dim>`, `-1` sentinel ignored).

`referential_specs(table)` turns those edges into `referential_integrity`
CheckSpecifications, so a subject config never hand-lists FKs.

---

## Subjects & tracks

- **FHIR** (`config/fhir_subjects.py`, on `mssql_target_staging`/DSA): Encounter,
  EpisodeOfCare, Patient, Organization, Location, Encounter_Location. Daily
  uniqueness (no not_null — PKs and required patient references are `NOT NULL` in
  the DDL); weekly lineage-driven referential + (where applicable)
  `source_reconciliation` vs `arc.*`. **Patient** is special — it's deduped by CPR
  in the ETL (one row per CPR), so instead of a `PATIENT_ID` reconciliation it has
  a CPR `source_reconciliation` (`vs_arc_cpr`, person-level) plus the dedup-aware
  `patient_arc_integrity` (`vs_arc_patient_id`) so dropped deleted duplicates
  aren't false gaps. **Location** is likewise deduped (the transform posts
  `DISTINCT LOCATION_SORCODE`), so its `vs_arc` keys on the SOR code
  (`LOCATION_SORCODE` ↔ `identifier_value_sor`), key-set only (the deduped SOR
  grain has no per-key arc delete flag), with a `dw_is_deleted = 0` source filter
  to match what the transform posts. The four IFDB→arc→fhir resources all have
  `vs_arc`: Encounter, EpisodeOfCare, Patient (CPR-grain), Location (SOR-grain).
  **Organization** has no `vs_arc` — it is built from the SOR registry, not an
  `arc.*_ifdb` archive — and **Encounter_Location** is a finer-grain projection of
  the Encounter resource (not its own archive), so it relies on Encounter's
  `vs_arc` plus its own FHIR-server reconciliation rather than a separate `arc`
  reconciliation.
- **FHIR, non-DFM resources** (same file, `_NON_DFM_SUBJECTS`): Appointment,
  ServiceRequest, Condition, Observation, Procedure, CarePlan, CarePlan_Activity,
  Patient_Address, Questionnaire, QuestionnaireResponse. Same daily
  uniqueness + weekly lineage referential + FHIR-server reconciliation —
  but these resources are **not migrated to DFM**, so there is **no replay
  remediation**: no `ReplaySpec`, no `capture_replay`, findings are never enqueued
  to `dqa.RemediationQueue` (fix = re-run the legacy daily job / fix the
  transform). No `arc.*` archives either → no source/oracle reconciliation. The
  one automated fix that DOES apply is `dqa_fhir_server_cleanup` (DFM-independent)
  for the tables with `dw_is_deleted`: Appointment and ServiceRequest. Business-key
  uniqueness checks on these start at WARN until the grains are confirmed against
  real data; Observation's weekly server reconciliation is the heaviest check
  (largest flat table).
- **DWH** (`config/dwh_subjects.py`): 6 ERP facts (Fakturalinje, Faktura,
  Postering, Vareudlevering, Finanskladdelinje, Budget) — daily `sentinel_rate`
  (`-1` rate per `DW_EK_` FK, observability), weekly referential + completeness.
- **ARCHIVE** (`config/arc_subjects.py`): `arc.{encounter,episode_of_care,patient,
  location}_ifdb` reconciled weekly vs their IFDB Oracle sources. Subjects are
  `source`-namespaced (`arc_ifdb_*` → anchors `dqa_arc_ifdb_<entity>_weekly`, job
  `dqa_arc_ifdb_weekly_job`) so future `arc.*` sources (KMD/ILS/Sydax/MySupply) get
  their own `dqa_arc_<source>_weekly_job` rather than being lumped together.

### arc ↔ IFDB reconciliation (`archive_reconciliation`)

The upstream half of the lineage `IFDB → arc → fhir`. Because it's cross-DB and
the archives are large (`IFDB.LOCATION` ≈ 14.4M rows at grain), it **stages**
source keys rather than diffing in Python:

1. `SELECT <merge keys> … GROUP BY` from Oracle, streamed in 10k batches and
   `bulkcopy`'d into a global `##dqa_recon_<uuid>` temp table (executemany
   fallback; the global temp is what lets `bulkcopy`'s own connection see it).
2. The diff runs entirely in MSSQL (`NOT EXISTS` / `LEFT JOIN` over the indexed
   composite key).

Key points:
- **Merge key = `ArchiveMergeConfig.merge_keys`** (composite where needed, e.g.
  Location's `(OBJECT_ID, PERIOD_START)`) — sourced from the DFM config so DQA
  can't drift from the archive's grain.
- Id columns are normalized with the ETL's `normalize_oracle_id`; other key parts
  (e.g. the `PERIOD_START` Oracle DATE) are `str()`'d to match the NVARCHAR archive.
- **Flow-start bound** (delta tables only): when `flow_start_bound_column` is set
  (from the extract config's `upper_bound_column`), each key is staged with a
  `within_bound` flag = 1 if its upper-bound column ≤ the last DFM flow-start
  (`meta.DFMState` `dfm/ifdb_flow_start`). `missing_in_arc` counts only
  within-bound keys (so rows newer than the last archive run aren't false gaps);
  `extra/vanished` ignores the flag and compares against the **full** live key set
  (else a key still in IFDB but modified after flow-start would be a false
  "vanished"). Full-load Location has no upper-bound column → unbounded.
- Reports `missing_in_arc` (gap, violation), `extra_in_arc` split into
  `vanished_marked_deleted` (`dw_is_deleted=1`, expected) vs
  `vanished_not_marked_deleted` (stale, violation).
- **Remediation** (delta single-merge-key entities: encounter, episode_of_care,
  patient; see [remediation_runbook.md](remediation_runbook.md)):
  `missing_in_arc` keys are auto-captured (`capture_replay=True`) to
  `dqa.RemediationQueue` under `replay_entity=arc_ifdb_<entity>` (a separate
  namespace — they never appear in the FHIR `dqa.v_replay_*` feeder views) and
  fixed by the operator-triggered **`dqa_arc_ifdb_replay`**: ids-mode
  `ext_*_ifdb` + `arc_*_ifdb` child runs with the keys passed **inline**
  (`id_filter_values`, chunked at Oracle's 1000-literal IN-list cap — the filter
  runs on Oracle, where MSSQL views don't exist) and **`advance_checkpoint=False`**
  so the daily delta window is untouched. `vanished_not_marked_deleted` is fixed
  by **`dqa_arc_ifdb_vanished_cleanup`** (stages the live IFDB keys, sets
  `dw_is_deleted=1`; snapshot-cutoff + 0-staged refusal + `max_rows` guard).
  **Location is excluded from both**: composite merge key, and its full-load
  archive merge both self-heals gaps and soft-deletes vanished rows
  (NOT MATCHED BY SOURCE) on the next daily run — an ids-mode partial ext would
  instead mass-soft-delete everything outside the batch.

---

## Orchestration & scheduling

Every DQA job runs **after the ETL that feeds it**, never on a fixed clock that
could race the sensor-chained 06:00 pipeline. Two mechanisms:

- **Post-ETL sensors** (`sensors.py`) — one `run_status_sensor` per pipeline. Each
  requests its `daily_job` on every qualifying run and its `weekly_jobs` on
  **Saturdays only** (the heavy cross-table / cross-DB checks + the DSO
  reconciliation). A sensor with several monitored jobs is an **AND-join**: it
  requests the DQA jobs only once *all* of them have succeeded for the same
  business day. Business day is anchored to the monitored run's **start time**
  (Europe/Copenhagen, from the run record — a backlog processed after daemon
  downtime still maps to its own day), and `run_key` is day-scoped, so the success
  events (and any daemon retries) collapse to exactly one DQA run per job per day.

  | Sensor | Fires after (AND-join) | Daily | Weekly (Saturday) |
  |---|---|---|---|
  | `dqa_fhir_after_etl` | `fhir_ifdb_dfm_sof_dso_job` **and** `fhir_etl_ifdb_daily_job` | `dqa_fhir_daily_job` | `dqa_fhir_weekly_job`, `dqa_dso_fhir_job` |
  | `dqa_erp_daily_after_dfm` | `erp_daily_dfm_job` | `dqa_dwh_daily_job` | `dqa_dwh_weekly_job` |
  | `dqa_arc_ifdb_after_archive` | `fhir_ifdb_dfm_base_job` | — | `dqa_arc_ifdb_weekly_job` |

  The FHIR daily pipeline fans out from the 06:00 base job into two jobs (SOF→flat
  `fhir.*` + DSO sync, and the remaining daily resources); the checks need both, so
  the FHIR sensor waits for the pair (and the FHIR-domain DSO reconciliation rides
  the same trigger). The arc IFDB reconciliation is weekly-only (no daily) and
  triggers on the base job, because the `arc.*` tables — and the flow-start bound
  `archive_reconciliation` reads — are settled exactly when the base job succeeds
  (a cron could race the archive merge).

- **DSO staggered crons** (`schedules.py`) — the DSO↔DSA reconciliation is
  detection-only and heavy (linked-server diffs over ~80 tables, some very large),
  and its sync sources are scattered across several jobs/days, so each domain
  (except `fhir`, which is sensor-driven above) runs **weekly on Saturday** on its
  own slot, ≥30 min apart to avoid hammering the linked server: `erp` 08:00,
  `shared` 08:30, `security` 09:00, `codesystem` 09:30, `conceptmap` 10:00 (CET).

**Jobs** (`jobs.py`): one per (track, `source`, cadence) with ≥1 enabled check →
`dqa_fhir_daily`/`_weekly`, `dqa_dwh_daily`/`_weekly`, `dqa_arc_ifdb_weekly`
(ARCHIVE is source-split; fhir/dwh have `source=None`). The DSO reconciliation adds
its own per-domain jobs (`dqa_dso_<domain>_job`, built in `dso.py`).

**Enablement**: sensors and schedules **auto-start in development/production**
(`get_sensor_default_status` / `get_schedule_default_status`, gated on
`should_auto_enable_schedules`) and stay STOPPED on feature branches — so DQA runs
automatically after merge, no manual UI toggle. This also means the full weekly +
DSO sweep goes live the first Saturday after a dev/prod merge (watch linked-server
runtime on the large DSO tables).

---

## Persistence & overview

- **`dqa.Results`** (owned by the `staging_infra_dqa` infra asset, DDL
  `infrastructure/ddl/tables/dqa/01_results.sql`): one row per check execution.
  `result.py` writes it on **every** run (best-effort — a missing/unwritable table
  only warns, never fails the check). `DQA_ENVIRONMENT` sets the `environment`
  column (defaults to branch postfix / `local`).
- **`dqa_summary`** (`reporting/summary.py`): rollup of the latest row per check —
  failing counts by severity/track + worst offenders, in the asset metadata.
- DQA-tuning indexes on the flat FHIR business-key + FK columns:
  `ddl/migrations/20260625_fhir_dqa_comparison_indexes.sql` (tracked migration,
  applied to both servers by `staging_migrations` / `dso_migrations`).

---

## Referential remediation (dangling FHIR references)

> **Operating the remediation assets** (how to run `dqa_remediation_replay` and
> `dqa_fhir_server_cleanup`, and after which assets): see
> [remediation_runbook.md](remediation_runbook.md).

When `referential_integrity` finds a dangling **non-null** flat reference, it's
almost always a stale UUID left when a duplicate parent was deleted after the
child was posted (failed resolution writes NULL, never a dangling UUID — see
`DSA/FHIR/utils/reference_resolver.py`). Re-running the child transform
re-resolves the business key to the surviving parent, so **replay is convergent**.
The full capture → `execute_run` replay → recheck/status-flip flow has been
**validated end-to-end in Dev** (`dry_run` then live) against real dangling
references.

- **Capture** (`remediation/capture.py`, always on): on a referential failure for a
  subject with a `ReplaySpec`, the distinct violating **business keys**
  (`identifier_value` = `OBJECT_ID`, the DFM `ids` handle — not the Aidbox UUID)
  are MERGE'd into `dqa.RemediationQueue` (DDL `…/dqa/02_remediation_queue.sql`),
  set-based and idempotent. Guard: `DQA_REMEDIATION_MAX_KEYS` (default 100k) →
  records an `overflow` marker instead of enqueuing a systemic spike (the marker
  is cleared again on the next under-guard capture, so a drained backlog doesn't
  leave it lingering). The guard is overridable **per run** via each check's
  `max_keys` config (`DqaCheckConfig`, resolved through a `ContextVar` in the
  factory) — set it in the Launchpad to drain a known backlog without touching the
  container env (see the runbook). Per-entity `dqa.v_replay_<entity>` views expose
  pending keys under the archive id column.
- **Replay** (`remediation/replay.py`): `dqa_remediation_replay_job` — one
  operator-triggered run (config `entity=ifdb_encounter|…`, `dry_run` first). It
  reads pending keys, launches one in-process child run over
  `fhir_server_<entity>_dfm` (ids mode, `id_filter_table` = the feeder view) +
  `sof_<entity>_dfm` + `dso_sync_fhir_<entity>_dfm` (delta), then re-checks each
  reference and flips resolved keys to `status='resolved'`. Keys still failing
  after `unresolvable_after` (default 3) attempts flip to `status='unresolvable'`
  and leave the replay loop until an operator resets them (see the runbook).
  Mirrors the `period_orchestrator` `execute_run` pattern.

Operator recipe: `docs/implementation/dfm_operations_guide.md` →
"Replay DQA-flagged dangling references".

### Unposted / unpropagated arc rows (also fed to the replay)

`source_reconciliation` can also feed the replay queue (param `capture_replay=True`,
enabled on `fhir_encounter` and `fhir_episode_of_care` — both key on `OBJECT_ID`,
the `ReplaySpec.filter_column`). It captures arc keys that are **`missing_in_target`**
(never posted) or an **arc-deleted `delete_status_mismatch`** (arc `DELETED=1` while
the flat row is still active — a delete that never propagated, e.g. soft-deletes that
weren't extracted). The reverse mismatch (arc active, flat deleted) is **not**
captured — it may be a deliberate FHIR-server delete. Captured under
`reference_column='source_reconciliation'`; replaying re-posts/tomb-stones the keys,
and the recheck resolves them optimistically (next weekly run re-captures if still
broken). **Not** enabled for `fhir_patient` (its reconciliation keys on CPR, not the
`PATIENT_ID` replay filter — the capture no-ops with a warning if mis-wired). A
backlog above `DQA_REMEDIATION_MAX_KEYS` writes an `overflow` marker instead of
enqueuing; see the runbook for draining it.

### FHIR-server deletions not propagated (separate from the above)

A resource hard-deleted in Aidbox lingers in the flat `fhir.*` table because the
delta SoF extraction can't see deletions — surfacing as duplicate
`identifier_value` (the deleted old version alongside the live new one). The
`fhir_server_reconciliation` check (above) detects it (`stale_in_flat`); the fix
is **`dqa_fhir_server_cleanup`** (`remediation/fhir_server_cleanup.py`) — an
operator-triggered asset/job that stages the server's live ids and sets
`dw_is_deleted = 1` on the stale active rows (`dry_run` first, `max_rows` guard).
Soft-delete only (reversible).

**Coverage.** The check runs against every flat FHIR table that mirrors an Aidbox
resource — the DFM-backed six (Encounter, EpisodeOfCare, Patient, Organization,
Location, Encounter_Location) **and** the non-DFM resources (Appointment,
ServiceRequest, Condition, Observation, Procedure, CarePlan (+Activity),
Patient_Address, Questionnaire, QuestionnaireResponse). Finer-grain projections
(Encounter_Location, CarePlan_Activity, Patient_Address) set
`report_missing_in_flat=False` since not every parent resource has a row there.
The cleanup is wired for the tables with an `dw_is_deleted` column —
`entity=encounter` (→ `fhir.Encounter` + `fhir.Encounter_Location`),
`episode_of_care`, `patient` (→ `fhir.Patient` + `fhir.Patient_Address`),
`organization`, `location` (both via migration
`20260702_fhir_org_location_add_is_deleted`), `appointment`, `service_request`.
The remaining non-DFM tables (Condition, Observation, Procedure, CarePlan,
Questionnaire, QuestionnaireResponse + CarePlan_Activity) have no
`dw_is_deleted` column, so their check is **detection-only** (severity WARN,
`deleted_column=None` → every row treated as live) until the column is added.
For Patient this is complementary to the arc-driven delete propagation
(`patient_arc_integrity` → `dqa_patient_delete_propagation`): that handles an arc
`SLETTET` that didn't propagate; this handles a patient resource hard-deleted on
the server.

One Aidbox resource can feed **several** flat tables at different grains — the
`encounter` entity targets both `fhir.Encounter` and `fhir.Encounter_Location`
(keyed by the same `encounter_id`). The cleanup stages the live ids **once** and
soft-deletes the stale rows in all targets in one atomic run. `Encounter_Location`
got an `dw_is_deleted` column for this (migration
`20260629_fhir_encounter_location_add_is_deleted`); its check sets
`report_missing_in_flat=False` since not every encounter has a location row.

---

## DSO ↔ DSA reconciliation (`dso.py`)

After the DSO-sync copies each staging (DSA) table to the DSO/reporting database,
the two should be identical. The `dso_reconciliation` check verifies that, and the
whole subsystem (`dqa/dso.py`) is **derived from `DSO.load.sync_configs.ALL_SYNC_CONFIGS`**
— the single source of truth for each synced table's schema, table, and primary key
— so coverage tracks the sync configs automatically (one check per synced table,
~80 today).

- **Where it runs**: on the **DSO** connection (`mssql_source_dso`), comparing the
  DSO-local table against DSA reached over the `STAGING_LINK` linked server
  (`[STAGING_LINK].[<staging_db>].[schema].[table]`, from `get_linked_server_config()`).
- **What it compares**: the **full primary key** (composite-aware) — `missing_in_dso`
  / `extra_in_dso` — plus `delete_status_mismatch` where a delete column exists
  (`dw_is_deleted`, auto-detected; PKs are unique so it's a straight
  per-row comparison, no MIN/MAX). Tables without a delete column are key-set only.
- **Results**: written to the single `dqa.Results` table on **DSA**
  (`results_resource_key="mssql_target_staging"`), so one unified table covers the
  whole platform even though the check runs on DSO.
- **Jobs**: one per domain — `dqa_dso_fhir_job`, `dqa_dso_erp_job`,
  `dqa_dso_shared_job`, `dqa_dso_security_job`, `dqa_dso_codesystem_job`,
  `dqa_dso_conceptmap_job` (anchors `dqa_dso_<domain>`, group `dqa_dso`, cadence
  `dso`). Kept separate from the `dqa_<track>_<cadence>` jobs; run after the
  DSO-sync.
- **Detection only** — no capture/replay yet (deferred until DSO-sync is migrated
  to DFM, per the remediation note above).

---

## Adding a subject or check

- **Cover a new table (= a new anchor asset)**: add a `DqaSubject` to the relevant
  `config/*.py` list (which feeds `ALL_SUBJECTS`) — pick the `track`, `resource_key`,
  `identity_column`, and list its daily checks; weekly referential checks are derived
  from the lineage YAMLs automatically. That's the only step: the factory emits the
  anchor `@asset` per cadence + the bound `@asset_check`s, and — for an **existing**
  track (fhir/dwh/arc) — the anchor is picked up by that track's per-cadence job and
  its post-ETL sensor / weekly schedule with **no extra orchestration wiring**. Set
  `deleted_column` if the table has a soft-delete flag (scopes the row-level checks to
  active rows), and `replay=ReplaySpec(...)` if the resource has a DFM `ids`-capable
  pipeline. (A brand-new *track* also needs a sensor spec in `sensors.py`; DSO
  subjects are auto-derived from `ALL_SYNC_CONFIGS`, not added here.)
- **New reusable check type**: add a module in `checks/` with `@register_check("name")`
  returning a `CheckOutcome`, and import it in `checks/__init__.py`.
- **One-off check**: use the `custom_sql` check type, or an inline `fn` on the `CheckSpecification`.

No env flags are required — persistence and remediation capture are always on.

---

## Status / not yet done

- **`chain` check unwired**: the adapter exists but `core.chain_validation` lives on
  `feature/chain-validation-clean-dev-sync`. Once merged, add `chain` CheckSpecifications
  (lazy import already guards it).
- **DSO reconciliation anchors carry no hard dep on the DSO-sync assets** — they
  are triggered *after* the sync by time (staggered Saturday crons) / by the FHIR
  sensor, not by an asset dependency, so a badly-delayed sync could in principle be
  reconciled before it finishes. Acceptable given the weekly cadence; revisit if the
  sync ever slips past the cron slots. (Scheduling itself is done — see
  Orchestration & scheduling. Large tables e.g. `fct.Faktura` / `fhir.Observation`
  diff over the linked server, so watch runtime.)
- **DSO remediation deferred**: `dso_reconciliation` is detection-only until the
  DSO-sync is migrated to DFM (then a replay-style fix can be added).
- **Non-DFM FHIR resources have no replay**: the `_NON_DFM_SUBJECTS` (Appointment,
  ServiceRequest, Condition, Observation, Procedure, CarePlan(+Activity),
  Patient_Address, Questionnaire(+Response)) are detection-first — referential /
  business-key findings are fixed by re-running the legacy daily job, not by
  `dqa_remediation_replay`. Only their FHIR-server staleness is auto-fixable
  (`dqa_fhir_server_cleanup`, where `dw_is_deleted` exists). Migrating a resource to
  DFM unlocks the replay: add its `ReplaySpec` + `vs_arc` checks then.
