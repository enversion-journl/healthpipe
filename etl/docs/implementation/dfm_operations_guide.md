<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DFM Operations Guide

Practical recipes for day-to-day DFM operations in the Dagster UI. Each section includes copy-paste YAML config for the Launchpad.

All DFM jobs are launched from **Dagster UI → Jobs → (select job) → Launchpad**. Paste the YAML into the config editor.

---

## Quick Reference

| I want to… | Job | Section |
|-------------|-----|---------|
| Run normal incremental extraction | `ifdb_archive_job` | [1. Default Delta Run](#1-default-delta-run) |
| Reprocess a date range (no checkpoint change) | `ifdb_archive_job` | [2. Ad-Hoc Period Run](#2-ad-hoc-period-run) |
| Reprocess specific IDs | `ifdb_archive_job` | [3. Ad-Hoc IDs Run](#3-ad-hoc-ids-run) |
| Fix DQA-flagged dangling FHIR references | `fhir_ifdb_dfm_base_job` | [Replay DQA-flagged dangling references](#replay-dqa-flagged-dangling-references-referential-remediation) |
| Reload a large historical range in chunks | `period_orchestrator_job` | [4. Period Orchestrator](#4-period-orchestrator) |
| Reset a checkpoint to re-extract from a date | `dfm_reset_job` | [5. Checkpoint Reset](#5-checkpoint-reset) |
| Force a specific IFDB flow-start timestamp | `ifdb_archive_job` | [6. Custom Flow-Start Override](#6-custom-flow-start-override) |
| Run without advancing the checkpoint (dry-run) | Any DFM job | [7. Dry-Run Mode](#7-dry-run-mode) |
| Re-extract with lookback days | `ifdb_archive_job` | [8. Delta with Lookback](#8-delta-with-lookback) |
| Run the full pipeline (extract → archive → FHIR → SOF) | `ifdb_dfm_full_job` | [9. Full Pipeline Run](#9-full-pipeline-run) |

---

## 1. Default Delta Run

Runs incremental extraction using the existing checkpoint. This is the normal operational mode — no config override needed.

**Job:** `ifdb_archive_job`

Launch with an **empty config** (or omit the config entirely):

```yaml
{}
```

All assets default to `mode: delta`, `advance_checkpoint: null` (which resolves to `true` for delta/full_load, `false` for period/ids). The `dfm_ifdb_flow_start` asset queries Oracle `JOB_LOG/JOB_STATUS` for the upper-bound timestamp automatically.

---

## 2. Ad-Hoc Period Run

Reprocess a specific date range without moving the production checkpoint. Useful after a source bugfix or to fill in gaps.

**Job:** `ifdb_archive_job`

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2025-06-30 23:59:59"
  ext_encounter_ifdb:
    config:
      mode: period
      date_from: "2025-06-01"
      date_to: "2025-06-30"
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
  ext_location_ifdb:
    config:
      mode: period
      date_from: "2025-06-01"
      date_to: "2025-06-30"
  arc_location_ifdb:
    config:
      advance_checkpoint: false
```

**Notes:**
- `date_from` is **required** and inclusive. `date_to` is **optional** (omit for open-ended upper bound) and inclusive when set (period covers `>= date_from AND < date_to + 1 day`).
- Set `flow_start_override` to prevent the flow-start asset from querying Oracle (it would set a live upper bound that may not match your period).
- `advance_checkpoint: false` on **archive** assets is now the **default for period mode** — you no longer need to set it explicitly. It is shown in examples for clarity. To opt-in to advancement (e.g. during orchestrator runs), set `advance_checkpoint: true` explicitly.
- Location is a full-load table — `mode: period` has no effect on it.

### Period run for a single entity (Encounter only)

If you only need to reprocess Encounter, use the same job but skip Location config:

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2025-06-30 23:59:59"
  ext_encounter_ifdb:
    config:
      mode: period
      date_from: "2025-06-01"
      date_to: "2025-06-30"
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
```

---

## 3. Ad-Hoc IDs Run

Reprocess specific records by ID. Use for targeted fixes after data corrections.

**Job:** `ifdb_archive_job`

### Small ID set (inline values)

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 12:00:00"
  ext_encounter_ifdb:
    config:
      mode: ids
      id_filter_column: "OBJECT_ID"
      id_filter_values: ["12345", "67890", "11111"]
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
```

### Large ID set (filter table)

For 100+ IDs, populate a temporary table first (e.g. via SSMS), then reference it:

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 12:00:00"
  ext_encounter_ifdb:
    config:
      mode: ids
      id_filter_column: "OBJECT_ID"
      id_filter_table: "tmp.encounter_ids_to_reprocess"
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
```

**Note:** For extract assets (e.g. `ext_encounter_ifdb`), the filter table must exist in the **Oracle source**. For FHIR DFM assets (e.g. `fhir_server_patient_dfm`), the filter table must exist in **MSSQL** since those query the archive tables directly.

**Note:** SOF (SQL-on-FHIR) assets do **not** support `mode: ids`. If `mode: ids` is set on a SOF asset (e.g. `sof_patient_dfm`), it logs a warning and falls back to delta behavior — no ID filtering is applied. To reprocess specific SOF data, use `mode: period` or `mode: full_load` instead.

### FHIR DFM asset — filter table on archive

To reprocess specific patients by CPR using a pre-populated MSSQL table:

```yaml
ops:
  fhir_server_patient_dfm:
    config:
      mode: ids
      id_filter_column: "CPR"
      id_filter_table: "ext.temp_pat_delete"
      advance_checkpoint: false
```

The generated query becomes:
`SELECT * FROM [arc].[ifdb_patient] WHERE 1=1 AND [CPR] IN (SELECT [CPR] FROM ext.temp_pat_delete)`.

The `id_filter_column` can be any column in the archive table (e.g. `CPR`, `PATIENT_ID`), not just the primary key.

### Replay DQA-flagged dangling references (referential remediation)

When a DQA `referential_integrity` check (weekly) finds dangling FHIR references
(e.g. `fhir.Encounter.service_provider_id` pointing at an Organization UUID that
no longer exists — typically after a duplicate cleanup), the offending
**business keys** are queued in `dqa.RemediationQueue` and exposed per entity by
the `dqa.v_replay_<entity>` views.

A dangling *non-null* reference always means the parent was deleted **after** the
child was posted (failed resolution writes NULL, never a dangling UUID), so
replaying the child re-resolves the business key to the surviving parent — the
fix is convergent.

**Semi-automatic (recommended):** run `dqa_remediation_replay_job` with config
`{ops: {dqa_remediation_replay: {config: {entity: "ifdb_encounter"}}}}` — it does
all three steps below (ids replay → delta SOF/DSO reproject → re-check + flip
`status` to `resolved`) in one run. Set `dry_run: true` first to preview the
pending count + the run_config without launching or mutating anything. Config
fields: `entity` (`ifdb_encounter` | `ifdb_episode_of_care` | `ifdb_patient` | `bom_ifdb_aftale`),
`max_keys` (default 100k — aborts above this), `run_sof_dso` (default true),
`mark_resolved` (default true), `dry_run` (default false).

The manual steps below are the equivalent done by hand (useful for partial runs
or debugging):

**Step 1 — replay the children** (re-resolve + re-post to the FHIR server). The
feeder view already exposes the pending keys under the archive's id column, so
point `id_filter_table` straight at it:

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-06-25 12:00:00"
  fhir_server_encounter_dfm:
    config:
      mode: ids
      id_filter_column: "OBJECT_ID"
      id_filter_table: "dqa.v_replay_ifdb_encounter"
      advance_checkpoint: false
```

Launch via `fhir_ifdb_dfm_base_job` (which contains `fhir_server_encounter_dfm`).
Feeder views: `dqa.v_replay_ifdb_encounter` / `dqa.v_replay_ifdb_episode_of_care`
(`OBJECT_ID`), `dqa.v_replay_ifdb_patient` (`PATIENT_ID`).

**Step 2 — reproject downstream.** SOF does **not** support `mode: ids` (it falls
back to delta) — but re-posting bumps each resource's `lastUpdated`, so a normal
**delta** SOF + DSO sync (`fhir_ifdb_dfm_sof_dso_job`) reprojects exactly the
re-posted resources into the flat `fhir.*` tables.

**Step 3 — confirm + close out.** Re-run the DQA referential check; the replayed
keys no longer dangle. Mark them resolved:

```sql
UPDATE [dqa].[RemediationQueue]
SET status = 'resolved', last_attempt_at = SYSUTCDATETIME(), attempts = attempts + 1
WHERE replay_entity = 'ifdb_encounter' AND status = 'pending';
```

**Guard:** capture refuses to enqueue more than `DQA_REMEDIATION_MAX_KEYS`
(default 100k) distinct keys for a single reference, recording an `overflow`
marker instead — a spike that large is systemic (e.g. another mass-dedup) and
should be investigated, not bulk-replayed.

---

## 4. Period Orchestrator

Splits a large date range into smaller partitions (day/week/month/quarter/year) and runs a target job sequentially for each partition. Use for historical reloads that are too large for a single run.

**Job:** `period_orchestrator_job`

### Reload by month (checkpoint unchanged)

```yaml
ops:
  period_orchestrator:
    config:
      job_name: "ifdb_archive_job"
      date_from: "2020-01-01"
      date_to: "2025-12-31"
      granularity: "month"
      advance_checkpoint: false
```

### Reload by quarter (advancing checkpoint after each chunk)

```yaml
ops:
  period_orchestrator:
    config:
      job_name: "ifdb_archive_job"
      date_from: "2020-01-01"
      date_to: "2025-12-31"
      granularity: "quarter"
      advance_checkpoint: true
```

### Reload specific assets (without a named job)

Instead of `job_name`, list specific assets:

```yaml
ops:
  period_orchestrator:
    config:
      asset_names:
        - "ext_encounter_ifdb"
        - "arc_encounter_ifdb"
      date_from: "2024-01-01"
      date_to: "2024-12-31"
      granularity: "month"
      advance_checkpoint: false
```

**Behaviour:**
- Runs partitions **sequentially** — stops on first failure.
- `advance_checkpoint` is omitted (or `false`) — period mode does **not** advance checkpoints by default. Set to `true` if you want each chunk to advance the checkpoint (resumable reloads).
- Upstream dependencies (e.g. `dfm_ifdb_flow_start`) are resolved automatically.
- Each child run is tagged with `dagster/partition_start`, `dagster/partition_end`, and `dagster/parent_run_id`.

**Granularity options:** `day`, `week`, `month`, `quarter`, `year`.

**Limitation — synchronous in-process execution:** Child runs are executed synchronously via `execute_run()` in the same process as the orchestrator. This means the orchestrator asset itself must remain alive for the entire duration of all partitions. For moderate reloads (e.g. 72 monthly partitions over 6 years) this works well. For very large historical reloads (100+ fine-grained partitions), consider using a coarser granularity (quarter/year) to reduce the number of child runs, or splitting the date range across multiple orchestrator invocations. A future improvement could use `instance.submit_run()` + polling to run child runs as independent Dagster runs, which would be more resilient to timeouts and reduce memory pressure.

---

## 5. Checkpoint Reset

Permanently rewinds a checkpoint so the next scheduled delta run starts from a new position. Use when you need to re-extract data from a specific point in time.

**Job:** `dfm_reset_job`

### Reset to a specific timestamp

```yaml
ops:
  dfm_reset:
    config:
      schema_name: "arc"
      table_name: "ifdb_encounter"
      new_value: "2025-01-01 00:00:00"
      reason: "Re-extract after source bugfix in IFDB"
      scope: "staging"
```

### Reset to beginning (full re-extract on next delta)

```yaml
ops:
  dfm_reset:
    config:
      schema_name: "arc"
      table_name: "ifdb_encounter"
      new_value: "0001-01-01 00:00:00"
      reason: "Full reload required after schema migration"
      scope: "staging"
```

### Reset the flow-start checkpoint

```yaml
ops:
  dfm_reset:
    config:
      schema_name: "dfm"
      table_name: "ifdb_flow_start"
      new_value: "2025-06-01 00:00:00"
      reason: "Realign flow-start after manual intervention"
      scope: "staging"
```

### Reset a DSO checkpoint

```yaml
ops:
  dfm_reset:
    config:
      schema_name: "fhir"
      table_name: "ifdb_encounter"
      new_value: "2025-01-01 00:00:00"
      reason: "Resync DSO after target rebuild"
      scope: "dso"
```

### SQL alternative (SSMS)

If you prefer direct SQL:

```sql
EXEC [meta].[usp_dfm_reset]
    @schema_name = 'arc',
    @table_name  = 'ifdb_encounter',
    @new_value   = '2025-01-01 00:00:00',
    @reason      = 'Re-extract after source bugfix',
    @scope       = 'staging';
```

**Important:** `dfm_reset` permanently changes the checkpoint. Unlike `advance_checkpoint: false`, this affects the next scheduled run. All resets are logged in `meta.DFMRunLog` for auditing.

---

## 6. Custom Flow-Start Override

Force a specific upper-bound timestamp instead of reading it from Oracle `JOB_LOG`. Useful when Oracle is unavailable, when running in local dev, or when you need to control the exact extraction window.

**Job:** `ifdb_archive_job`

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 05:00:24"
```

This sets the flow-start checkpoint to the provided value. All downstream delta extracts in the same run will use it as their upper bound ceiling (on `EPJI_MODIFICATION_DATETIME` for Encounter).

**When to use:**
- Local development where Oracle `IFDB.JOB_LOG` is not available.
- Debugging a specific time window — combine with a checkpoint reset to control both bounds.
- Re-running after a failed flow-start query.

---

## 7. Dry-Run Mode

Run any DFM pipeline without advancing checkpoints. The extraction and MERGE execute normally, but the checkpoint stays at its current value. Use for testing, verification, or one-off investigations.

**Job:** Any DFM job (e.g. `ifdb_archive_job`)

```yaml
ops:
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
  arc_location_ifdb:
    config:
      advance_checkpoint: false
```

For delta mode, you must explicitly set `advance_checkpoint: false` on archive (and downstream dim/fct/fhir) assets — extract assets never advance checkpoints. All other fields keep their defaults (`mode: delta`). The pipeline runs a normal incremental delta but does not move the checkpoint forward. (For period/ids mode, checkpoint advancement is blocked by default.)

---

## 8. Delta with Lookback

Re-extract additional days beyond the checkpoint to catch late-arriving data. The archive MERGE handles duplicates safely via upsert.

**Job:** `ifdb_archive_job`

```yaml
ops:
  ext_encounter_ifdb:
    config:
      lookback_days: 7
```

This produces a WHERE clause like:
```sql
IFDB_MODIFICATION_DATETIME > (TO_DATE('2026-05-20 08:00:00', 'YYYY-MM-DD HH24:MI:SS') - INTERVAL '7' DAY)
```

**Note:** `lookback_days` only applies in `delta` mode. It has no effect in `period`, `ids`, or `full_load` modes.

---

## 9. Full Pipeline Run

Run the complete IFDB DFM pipeline: extract from Oracle → archive MERGE → FHIR transform → FHIR server upload → SQL-on-FHIR views.

**Job:** `ifdb_dfm_full_job`

```yaml
{}
```

Or with a custom flow-start:

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 05:00:24"
```

This job covers the `ifdb_archive`, `fhir_ifdb_dfm`, and `fhir_sof_dfm` asset groups.

---

## Common Workflows

### Workflow: "Re-extract everything from scratch"

Run `ifdb_archive_job` with `mode: full_load`. This extracts all rows regardless of the current checkpoint:

```yaml
ops:
  ext_encounter_ifdb:
    config:
      mode: full_load
  arc_encounter_ifdb:
    config:
      mode: full_load
  ext_location_ifdb:
    config:
      mode: full_load
  arc_location_ifdb:
    config:
      mode: full_load
```

`full_load` ignores the checkpoint entirely — no reset is needed. The archive MERGE upserts all rows and enables soft-delete detection (since the source is complete). The checkpoint advances to the current timestamp on success.

**When you DO need a reset:** if you want the next *scheduled delta run* to start from a specific point after the full load, reset the checkpoint afterwards. Otherwise the checkpoint will be set to the flow-start timestamp from the full-load run, which is correct in most cases.

### Workflow: "Controlled re-extract of a time window"

The period orchestrator uses `mode: period` with explicit date bounds, so the checkpoint does not affect what gets extracted — no reset is needed.

**Option A: Reload without changing checkpoint** (safe, production-friendly):

```yaml
ops:
  period_orchestrator:
    config:
      job_name: "ifdb_archive_job"
      date_from: "2025-01-01"
      date_to: "2025-04-01"
      granularity: "month"
      advance_checkpoint: false
```

The archive MERGE upserts the period's rows into `arc.*`. The production checkpoint stays unchanged, so the next scheduled delta picks up where it left off.

**Option B: Reload AND advance checkpoint** (use when reprocessing up to "now"):

```yaml
ops:
  period_orchestrator:
    config:
      job_name: "ifdb_archive_job"
      date_from: "2025-01-01"
      date_to: "2025-04-01"
      granularity: "month"
      advance_checkpoint: true
```

Each chunk advances the checkpoint. After the last chunk, the checkpoint reflects the end of the period. Use this when the reload is meant to replace the checkpoint position (e.g. after a reset to `0001-01-01`).

### Workflow: "Investigate a specific encounter"

1. Extract + archive just that ID (dry-run):

```yaml
ops:
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 12:00:00"
  ext_encounter_ifdb:
    config:
      mode: ids
      id_filter_column: "OBJECT_ID"
      id_filter_values: ["12345"]
  arc_encounter_ifdb:
    config:
      advance_checkpoint: false
```

2. Check the result in `ext.ifdb_encounter` and `arc.ifdb_encounter` via SSMS.

---

## Available DFM Jobs

| Job | Description | Config |
|-----|-------------|--------|
| `ifdb_archive_job` | Extract + archive (all DFM IFDB sources, incl. SFI / Appointment) | `RunModeConfig` + `FlowStartConfig` |
| `fhir_ifdb_dfm_job` | FHIR transform + upload from archive | `RunModeConfig` |
| `ifdb_dfm_full_job` | Full pipeline: extract → archive → FHIR → SOF | `RunModeConfig` + `FlowStartConfig` |
| `fhir_sof_dfm_job` | SQL-on-FHIR view extraction | `RunModeConfig` |
| `fhir_ifdb_dfm_base_job` | Base DFM pipeline: flow-start → extract → archive → FHIR server (all DFM producers; CarePlan still partitioned) | `RunModeConfig` + `FlowStartConfig` |
| `fhir_ifdb_dfm_sof_dso_job` | SOF + DSO for DFM resources, then Borger (`ref_patient_borger` → `ref_patient_address_borger` → `dim_Borger` → `dso_sync_dim_borger`) og SFI_datapoint refresh (waits for Observation/QR DSO + codesystem.sfi_*) | `RunModeConfig` |
| `dfm_reset_job` | Checkpoint reset | `DfmResetConfig` |
| `period_orchestrator_job` | Chunked historical reload | `PartitionedPeriodConfig` |

### Mode Support by Asset Layer

Not all modes are supported by all asset types:

| Layer | `delta` | `full_load` | `period` | `ids` |
|-------|---------|-------------|----------|-------|
| Extract (`ext_*`) | Yes | Yes | Yes | Yes |
| Archive (`arc_*`) | Yes | Yes | Yes | Yes |
| FHIR DFM (`fhir_server_*_dfm`) | Yes | Yes | Yes | Yes |
| SOF DFM (`sof_*_dfm`) | Yes | Yes | Yes | **No** — falls back to delta |

SOF assets query PostgreSQL flat views (Aidbox `sof` schema) using a timestamp-based `WHERE` clause. The `ids` mode is not applicable because SOF views are full-table extractions filtered by modification timestamp, not by individual resource IDs. When `mode: ids` is passed to a SOF asset, it logs a warning and processes as delta (using the existing checkpoint).

---

## Daily Job Orchestration

### Overview

The daily FHIR ETL is split into two independent pipelines:

```
┌──────────────────────────────────┐
│   fhir_ifdb_dfm_base_job        │  ← Scheduled at 06:00 CET
│   (Patient, EoC, Encounter,     │
│    Location, DocumentReference,  │
│    Condition, Procedure,         │
│    ServiceRequest, SFI,          │
│    Appointment, Location_SOR:    │
│    DFM checkpoint-based,         │
│    unpartitioned)                │
└──────────────┬───────────────────┘
               │ SUCCESS
               ▼ (sensor)
┌──────────────────────┐   ┌──────────────────────────┐
│ fhir_etl_ifdb_daily  │   │ fhir_ifdb_dfm_sof_dso    │
│ _job (partitioned,   │   │ _job (SOF + DSO,         │
│ remaining resources: │   │ Borger ref/dim + DSO,    │
│ CarePlan only —      │   │ codesystem.sfi_* +       │
│ CarePlan lands as    │   │ SFI_datapoint refresh)   │
│ DFM)                 │   │                          │
└──────────────────────┘   └──────────────────────────┘
```

### How It Works

1. **`fhir_ifdb_dfm_base_job`** runs daily at 06:00 via `ifdb_dfm_base_schedule`. It extracts data from Oracle (checkpoint-based delta), merges into archive tables, and posts FHIR resources to the FHIR server. It covers **Patient, EpisodeOfCare, Encounter, Location, DocumentReference (Epikrise + CareDischargeReport), Condition (Diagnose + PAS-diagnose), Procedure (AdminProc + RIS-ydelse), ServiceRequest (HENVISNING + RIS-ydelse + RehabilitationPlan), Aktivitetsresultat/SFI (incl. Questionnaire/QR), Appointment and Location_SOR**. All SOF/DSO runs in the DFM SOF/DSO job — the multi-producer exception is fully resolved except Observation, which moved early because the EVENTMARKER assets are unregistered stubs (CarePlan must land as DFM).

2. **`dfm_base_job_success_sensor`** watches for successful completions of the base job. On success, it triggers:
   - `fhir_etl_ifdb_daily_job` — processes the remaining daily resource (CarePlan/EVENTMARKER)
   - `fhir_ifdb_dfm_sof_dso_job` — SQL-on-FHIR extraction and DSO sync for the DFM resources, then the Borger chain (`ref_patient_borger` → `ref_patient_address_borger` → `dim_Borger` → `dso_sync_dim_borger`), `codesystem.sfi_*` sync, and finally `dso_refresh_sfi_datapoint_daily`. The refresh asset lives in `fhir_dso_sync_dfm` so Dagster actually waits for today's Observation/QR DSO sync (a dep on an asset outside the job is treated as external and the edge disappears). The two jobs still run **in parallel** with each other; SFI_datapoint no longer runs in the partitioned daily job.

   The partition key is derived from the **base run's start time** (Europe/Copenhagen), not the sensor's wall-clock time — so a backlog of successes (e.g. after daemon downtime) materialises each day's own partition rather than collapsing them onto "today". Re-executing the base run re-fires this chain (intended recovery for a fixed-and-retried base run; harmless for an already-green one, since downstream is idempotent).

3. **`dfm_base_job_failure_sensor`** alarms when the base job fails: downstream `fhir_etl_ifdb_daily_job` and `fhir_ifdb_dfm_sof_dso_job` are gated on base **success** and will not run that tick, so a base failure would otherwise leave the whole FHIR day un-materialised.
   - **Opt-in recovery:** set env var `DFM_BASE_FAILURE_TRIGGER_DAILY=true` to best-effort trigger `fhir_etl_ifdb_daily_job` (but **not** SOF/DSO) even when the base job fails. Safe because legacy resources resolve their Patient/EpisodeOfCare/Location/Encounter references against already-posted data + on-demand resolution, not against today's base run. Default is **off** (strict gating).

4. **`fhir_etl_ifdb_daily_job`** is the original daily job, but it now **excludes** assets tagged with `"dfm_migrated": "true"`. This means DFM-converted assets are automatically removed from the daily partitioned job without needing to maintain explicit key lists.

### The `dfm_migrated` Tag

Assets tagged `"dfm_migrated": "true"` are excluded from `fhir_etl_ifdb_daily_job` via:

```python
_dfm_migrated = dg.AssetSelection.tag("dfm_migrated", "true")

fhir_etl_ifdb_daily_job = dg.define_asset_job(
    name="fhir_etl_ifdb_daily_job",
    selection=(
        (ifdb_extraction_daily_assets - _dfm_migrated)
        | (fhir_ifdb_daily_assets - _dfm_migrated)
        | (fhir_sof_daily_assets - _dfm_migrated)
        | (fhir_dso_sync_daily - _dfm_migrated)
    ),
    ...
)
```

Tag-based subtraction resolves dynamically (no hard-coded keys), which means it works correctly with branch postfixing for multi-deployment support and will never produce `DagsterInvalidSubsetError` for missing keys.

---

## Migrating an IFDB Daily Asset to DFM

When converting an existing daily-partitioned IFDB resource to the DFM framework, follow this checklist:

### 1. Create DFM Assets

- **Extract**: Add a `DfmExtractConfig` + `ext_<entity>_ifdb` asset in `DSI/oracle/ifdb_extract.py`
- **Archive**: Add an `ArchiveMergeConfig` + `arc_<entity>_ifdb` asset in `DSI/oracle/ifdb_archive.py`; create DDL in `infrastructure/ddl/tables/arc/`
- **FHIR**: Create `DSA/FHIR/<Resource>/<Resource>_IFDB_dfm.py` with a single `fhir_server_<entity>_dfm` asset (batched fetch + transform + post)
- **SOF**: Create `DSA/FHIR/SqlOnFhir/<Resource>_dfm.py` with `sof_<entity>_dfm`
- **DSO Sync**: Add a spec entry to `_DFM_DSO_SYNC_SPECS` in `DSO/load/sync_fhir.py`

### 2. Remove the Old Partitioned Assets

Once the DFM assets are in place, remove the old daily/monthly/full_load
partitioned assets for that entity. If you need a transition period where both
pipelines coexist, tag the old daily assets with `"dfm_migrated": "true"`
instead — that excludes them from `fhir_etl_ifdb_daily_job` without deleting
them — and delete them when the DFM pipeline is verified:

| Layer | File | What to remove |
|-------|------|-------------|
| **Extraction** | `DSI/oracle/ifdb.py` | The `create_extraction_assets(...)` call and asset-list entries (keep the `TableExtractionConfig` if quality checks use it — the dependency resolver reads `arc.*` and no longer needs the legacy configs) |
| **FHIR** | `DSA/FHIR/<Resource>/<Resource>_IFDB.py` | The whole file — move shared `_transform_*`/`_post_*` helpers into `<Resource>_IFDB_dfm.py` first and repoint importers (dependency resolver, quality checks, tests) |
| **SOF** | `DSA/FHIR/SqlOnFhir/<Resource>.py` | The whole file (replaced by `<Resource>_dfm.py`) |
| **DSO Sync** | `DSO/load/sync_fhir.py` | Remove the spec from `_DSO_SYNC_SPECS`; add it to `_DFM_DSO_SYNC_SPECS` |

Also update any downstream assets whose `deps=[...]` referenced the old
partitioned asset names to depend on the new DFM asset instead.

### 3. Job Membership Comes From the Asset Group

Do **not** edit `jobs/ifdb_dfm_jobs.py` — its selections are group-based
(`AssetSelection.groups("ifdb_archive") | groups("fhir_ifdb_dfm")` for the base
job, `groups("fhir_sof_dfm") | groups("fhir_dso_sync_dfm") | ...` for SOF/DSO).
Setting the correct `group_name` on the new assets IS the job registration; a
wrong or missing group silently drops the asset from the nightly run.

### 4. Register Modules

- Export new assets from the relevant `__init__.py` files
- Add new DFM assets to `definitions.py` registration

### 5. Verify

Run a quick import check to confirm the job resolves:

```bash
cd etl && python -c "
import sys; sys.path.insert(0, '.')
from definitions import defs
job = defs.get_job_def('fhir_etl_ifdb_daily_job')
keys = sorted([str(k) for k in job.asset_layer.selected_asset_keys])
# Verify the migrated entity is NOT in the list
assert not any('<entity>' in k for k in keys), 'Entity still in daily job!'
print(f'OK: {len(keys)} assets in daily job, entity excluded.')
"
```

### Notes

- For Patient, EpisodeOfCare, Encounter, Location, Condition, Procedure, ServiceRequest, Aktivitetsresultat/SFI, Appointment and Location_SOR the old daily/monthly/full_load partitioned assets have been **deleted** — historical reloads use the DFM `full_load`/`period` modes (see sections above) instead of partition backfills. Only CarePlan (EVENTMARKER) remains on the partitioned path and MUST land as DFM. The partitioned `ris_ydelse`/`henvisning*`/`referralinformation` extracts were deleted with ServiceRequest, their last consumer.
- The DFM base job is unpartitioned (uses checkpoint-based incremental), while the remaining daily job is still daily-partitioned.

---

## Checkpoint Schema Reference

Checkpoints live in `meta.DFMState`. Key checkpoint entries for IFDB:

| Schema | Table | Scope | What it tracks |
|--------|-------|-------|----------------|
| `dfm` | `ifdb_flow_start` | staging | Upper-bound from `JOB_LOG.SLUTTID` |
| `arc` | `ifdb_patient` / `ifdb_episode_of_care` / `ifdb_encounter` / `ifdb_location` | staging | Last archived timestamp per core IFDB table |
| `arc` | `ifdb_epikrise` (+`_modtager`, `_tekst`) | staging | Last archived Epikrise (+children) |
| `arc` | `ifdb_care_discharge_report` (+`_receiver`) | staging | Last archived Udskrivningsrapport (+receivers) |
| `arc` | `ifdb_diagnose` (+`_tillaegskode`) | staging | Last archived Diagnose (+tillægskoder) |
| `arc` | `ifdb_pas_diagnose` (+`_encounter_ref`, `_tillaegskode`, `_henvisning_ref`) | staging | Last archived PAS-diagnose (+ref tables) |
| `arc` | `ifdb_administrative_procedure` (+`ifdb_adm_procedure_suppl_codes`) | staging | Last archived Administrative procedure (+suppl codes) |
| `arc` | `ifdb_ris_ydelse` | staging | Last archived RIS-ydelse (XML verbatim; shared with ServiceRequest) |
| `arc` | `ifdb_henvisning` (+`_historik`) / `ifdb_referralinformation` | staging | Last archived Henvisning (+history, referral info) |
| `arc` | `ifdb_aktivitetsresultat` (+`ifdb_aktivitetsudfoerelse`, `_speciale`) | staging | Last archived Aktivitetsresultat/SFI (+lookups) |
| `arc` | `bom_ifdb_aftale` (+`bom_ifdb_aftaleressourcedeltager`) | staging | Last archived Appointment (AFTALE flatten + deltager-lookup) |
| `arc` | `bom_ifdb_orgenhed_sor` / `bom_ifdb_patientinfo` | staging | Last archived Location_SOR flatten / PATIENTINFO (CPR fallback) |
| `arc` | `sor_register` | staging | Last archived SOR register snapshot |
| `fhir_server` | `Patient` / `Encounter` / `EpisodeOfCare` / `Location` / `Organization` | staging | Last FHIR-posted timestamp per resource |
| `fhir_server` | `DocumentReference_Epikrise` / `DocumentReference_CareDischargeReport` | staging | Last FHIR-posted DocumentReference per slice |
| `fhir_server` | `Condition_Diagnose` / `Condition_PasDiagnose` | staging | Last FHIR-posted Condition per slice |
| `fhir_server` | `Procedure_AdminProc` / `Procedure_RisYdelse` | staging | Last FHIR-posted Procedure per slice |
| `fhir_server` | `ServiceRequest` / `RehabilitationPlan` | staging | Last FHIR-posted ServiceRequest (henvisning+RIS streams / rehab-plan slice) |
| `fhir_server` | `Aktivitetsresultat_SFI` | staging | Last FHIR-posted Observation/Procedure/Condition (+Q/QR) from SFI |
| `fhir_server` | `Appointment` / `Location_SOR` | staging | Last FHIR-posted Appointment / SOR-Location |
| `fhir` | `Patient` / `Patient_Address` / `Encounter` / `Encounter_Location` / `EpisodeOfCare` / `Location` | staging | Last SOF extract per flat table |
| `fhir` | `DocumentReference` / `DocumentReference_Recipient` | staging | Last SOF extract (DocumentReference tables) |
| `fhir` | `ServiceRequest` / `ServiceRequest_tillaegskode` / `ServiceRequest_Recipient` | staging | Last SOF extract (ServiceRequest tables) |
| `fhir` | `Condition` / `Condition_tillaegskode` | staging | Last SOF extract (Condition tables; SOF is DFM — all four producers migrated) |
| `fhir` | `Procedure` / `Procedure_tillaegskode` | staging | Last SOF extract (Procedure tables; SOF is DFM) |
| `fhir` | `Observation` / `Questionnaire` / `Questionnaire_item` / `QuestionnaireResponse` / `QuestionnaireResponse_item` | staging | Last SOF extract (SFI tables; EVENTMARKER-Observation assets are unregistered stubs — CarePlan must land as DFM) |
| `fhir` | `Appointment` | staging | Last SOF extract (Appointment table) |

View current checkpoints:

```sql
SELECT schema_name, table_name, scope, checkpoint_value, updated_at
FROM [meta].[DFMState]
WHERE schema_name IN ('arc', 'dfm', 'fhir')
ORDER BY schema_name, table_name;
```

View recent run history:

```sql
SELECT TOP 20 *
FROM [meta].[DFMRunLog]
ORDER BY run_started_at DESC;
```
