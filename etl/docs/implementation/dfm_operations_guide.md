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
| Fix DQA-flagged dangling FHIR references | `ifdb_dfm_daily_job` | [Replay DQA-flagged dangling references](#replay-dqa-flagged-dangling-references-referential-remediation) |
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

All assets default to `mode: delta`, `advance_checkpoint: null` (which resolves to `true` for delta/full_load, `false` for period/ids). The `dfm_ifdb_flow_start` asset reads the delta ceilings from Oracle automatically (see *Two ceilings* below). It depends on `dfm_ifdb_cycle_wait`, which waits while an IFDB cycle (05, 09, 13, 17) is running: it polls every minute until every active job type has finished that cycle and no job type has `JOB_STATUS.IGANG = 1`. It accepts a complete cycle only when a second read 15 seconds later agrees, because the first job type of a cycle sets `IGANG` up to a second after the hour. The wait time is in its `cycle_wait_seconds` metadata. It fails after `cycle_wait_timeout_minutes` and names the job types that block it. It fails at once when a job type lags, no job type runs and the newest `SLUTTID` is more than 15 minutes old, because that job type failed in IFDB. `cycle_wait_timeout_minutes` is a run config field of `dfm_ifdb_cycle_wait` (`ops: dfm_ifdb_cycle_wait: config:`), with a default of 180. The value 0 turns the wait off, and the asset then does not read Oracle. The period orchestrator and the IFDB full load set 0 in any mode but delta and when they get a `flow_start_override`, because only a delta run without an override reads the ceiling from Oracle. Set 0 yourself in a manual run in any other mode or with `flow_start_override`. The examples in this guide do this.

**Two ceilings.** `dfm_ifdb_flow_start` writes two rows in `meta.DFMState`, and every IFDB delta extract bounds both ends of its window on its own delta column against one of them. The extract records the ceiling it read in its own `meta.DFMState` row (`dfm/<target table>_flow_start`), and the archive merge sets its checkpoint to that value, so the next run starts where this one stopped, even if a flow start in another run has moved the ceiling meanwhile.

- **Transfer ceiling** (`dfm.ifdb_transfer_ceiling`): Oracle `SYSDATE`, read just before the job status, when every active job type has finished the same cycle and none is running. It bounds the tables whose delta column IFDB stamps when it transfers the row: `IFDB_MODIFICATION_DATETIME`, `OPDATERET_TID` and `UPDATE_TIME`, and their join children. Nothing writes while the cycle is complete, so every row stamped at or before the ceiling is committed when the extract reads it. If the cycle is not complete when the flow start reads it, both ceilings keep their previous values and the asset logs a warning; the run then extracts nothing new. With no previous transfer ceiling it fails.
- **EPJ ceiling** (`dfm.ifdb_flow_start`): `MIN(MAX(JOB_LOG.SLUTTID))` across the active job types, the cut on the EPJ clock up to which IFDB has loaded. It bounds Diagnose, Sengeenhed and Referralinformation, which carry no transfer timestamp, only the EPJ modification time (`MODTIDSPUNKT`, `REFERRAL_DATE`).

Because both ends of a window are on one clock, a manual extract and archive run without a new flow start after a later IFDB cycle extracts an empty delta, and a cycle that starts while the extracts run writes only rows above the ceiling. A row that IFDB transfers late with an old EPJ time is still missed by the three tables on the EPJ ceiling, because their lower bound is on the EPJ clock too.

---

## 2. Ad-Hoc Period Run

Reprocess a specific date range without moving the production checkpoint. Useful after a source bugfix or to fill in gaps.

**Job:** `ifdb_archive_job`

```yaml
ops:
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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

Launch via `ifdb_dfm_daily_job` (which contains `fhir_server_encounter_dfm`).
Feeder views: `dqa.v_replay_ifdb_encounter` / `dqa.v_replay_ifdb_episode_of_care`
(`OBJECT_ID`), `dqa.v_replay_ifdb_patient` (`PATIENT_ID`).

**Step 2 — reproject downstream.** SOF does **not** support `mode: ids` (it falls
back to delta) — but re-posting bumps each resource's `lastUpdated`, so a normal
**delta** SOF + DSO sync (in the same `ifdb_dfm_daily_job` run) reprojects exactly the
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

`ifdb_flow_start` is the EPJ ceiling. Reset the transfer ceiling the same way with `table_name: "ifdb_transfer_ceiling"`.

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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
  dfm_ifdb_flow_start:
    config:
      flow_start_override: "2026-05-27 05:00:24"
```

This sets both ceilings, the transfer ceiling and the EPJ ceiling, to the provided value. All downstream delta extracts in the same run use it as their upper bound, and the archive merges set their checkpoints to it. An override above the time IFDB last finished a cycle can make the next run skip rows that a running cycle had not committed, so keep it at or below the end of a finished cycle.

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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
  dfm_ifdb_cycle_wait:
    config:
      cycle_wait_timeout_minutes: 0
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
| `ifdb_dfm_daily_job` | **The nightly pipeline.** flow-start → extract → archive → FHIR server → SOF → DSO, then Borger (`ref_patient_borger` → `ref_patient_address_borger` → `dim_Borger` → `dso_sync_dim_borger`) and the SFI_datapoint refresh | `RunModeConfig` + `FlowStartConfig` |
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

The whole IFDB → FHIR pipeline is one job:

```
┌────────────────────────────────────────────────────────┐
│  ifdb_dfm_daily_job          ← 06:00 CET               │
│                                                        │
│  flow-start → ext.* → arc.*                            │
│      → FHIR server (Patient, EoC, Encounter, Location, │
│        Location_SOR, DocumentReference, Condition,     │
│        Procedure, ServiceRequest, SFI, Appointment,    │
│        CarePlan)                                       │
│      → SOF (sof.* views → flat fhir.*)                 │
│      → DSO sync                                        │
│      → Borger ref/dim + dso_sync_dim_borger            │
│      → codesystem.sfi_* + SFI_datapoint refresh        │
└────────────────────────────────────────────────────────┘
```

### How It Works

1. **`ifdb_dfm_daily_job`** runs daily at 06:00 via `ifdb_dfm_daily_schedule`. Every
   stage is DFM checkpoint-based and unpartitioned, so the run needs no partition
   key and Dagster resolves the whole graph as one set of ordering edges.

2. **Why one job.** It used to be three: a base job, and two jobs a success sensor
   fanned out to. That split existed because the remaining daily resources were
   *partitioned* — a partitioned job cannot select unpartitioned assets, so the
   sensor had to launch it separately with a partition key derived from the base
   run's start time. When CarePlan (the last partitioned producer) moved to DFM,
   the constraint disappeared, and with it the sensors, the partition-anchoring
   logic, and the `DFM_BASE_FAILURE_TRIGGER_DAILY` recovery escape hatch.

3. **Ordering is by asset dep, not by job boundary.** This is the thing to get right
   when adding an asset: a dep on an asset that is *not selected by the running job*
   is treated by Dagster as an external asset and the ordering edge silently
   disappears. `dso_refresh_sfi_datapoint_daily` is the worked example — it lives in
   `fhir_dso_sync_dfm` precisely so it is selected alongside the Observation/QR DSO
   syncs it must wait for.

4. **Failure.** The job failing is reported by `nightly_failure_sensor` like any
   other scheduled run. Recovery is a Dagster re-execution (optionally
   from-failure), not a separate best-effort job.

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
partitioned assets for that entity:

| Layer | File | What to remove |
|-------|------|-------------|
| **Extraction** | `DSI/oracle/ifdb.py` | Any remaining extract-asset wiring and asset-list entries (keep the `TableExtractionConfig` if quality checks use it — the dependency resolver reads `arc.*` and no longer needs the legacy configs) |
| **DSI package** | `DSI/oracle/__init__.py` | Re-exports of the removed assets, and the asset lists that fed `definitions.py` |
| **FHIR** | `DSA/FHIR/<Resource>/<Resource>_IFDB.py` | The whole file — move shared `_transform_*`/`_post_*` helpers into `<Resource>_IFDB_dfm.py` first and repoint importers (dependency resolver, quality checks, tests) |
| **SOF** | `DSA/FHIR/SqlOnFhir/<Resource>.py` | The whole file (replaced by `<Resource>_dfm.py`) |
| **DSO Sync** | `DSO/load/sync_fhir.py` | Remove the spec from `_DSO_SYNC_SPECS`; add it to `_DFM_DSO_SYNC_SPECS` |

Also update any downstream assets whose `deps=[...]` referenced the old
partitioned asset names to depend on the new DFM asset instead.

### 3. Job Membership Comes From the Asset Group

Do **not** edit `jobs/ifdb_dfm_jobs.py` — `ifdb_dfm_daily_job`'s selection is
group-based (`groups("ifdb_archive") | groups("fhir_ifdb_dfm") |
groups("fhir_sof_dfm") | groups("fhir_dso_sync_dfm") | ...`). Setting the correct
`group_name` on the new assets IS the job registration; a wrong or missing group
silently drops the asset from the nightly run — and any `deps` pointing at it
from inside the job become external, losing the ordering edge too.

### 4. Register Modules

- Export new assets from the relevant `__init__.py` files
- Add new DFM assets to `definitions.py` registration

### 5. Verify

Run a quick import check to confirm the job resolves:

```bash
cd etl && python -c "
import sys; sys.path.insert(0, '.')
from definitions import defs
job = defs.get_job_def('ifdb_dfm_daily_job')
keys = sorted([str(k) for k in job.asset_layer.selected_asset_keys])
# Verify the new DFM assets ARE selected by the nightly job
assert any('<entity>' in k for k in keys), 'Entity missing from the nightly job!'
print(f'OK: {len(keys)} assets in the nightly job.')
"
```

### Notes

- Every IFDB resource is now on the DFM path, so the old daily/monthly/full_load partitioned assets have all been **deleted** — historical reloads use the DFM `full_load`/`period` modes (see sections above) instead of partition backfills. CarePlan (EVENTMARKER) was the last one; the `dfm_migrated` tag that used to subtract migrated assets from the partitioned daily job went with it.
- No job in the deployment carries a `partitions_def` any more.

---

## Checkpoint Schema Reference

Checkpoints live in `meta.DFMState`. Key checkpoint entries for IFDB:

| Schema | Table | Scope | What it tracks |
|--------|-------|-------|----------------|
| `dfm` | `ifdb_transfer_ceiling` | staging | Transfer ceiling: Oracle `SYSDATE` at a complete IFDB cycle |
| `dfm` | `ifdb_flow_start` | staging | EPJ ceiling: `MIN(MAX(JOB_LOG.SLUTTID))` |
| `arc` | `ifdb_patient` / `ifdb_episode_of_care` / `ifdb_encounter` / `ifdb_location` | staging | Last archived timestamp per core IFDB table |
| `arc` | `ifdb_epikrise` (+`_modtager`, `_tekst`) | staging | Last archived Epikrise (+children) |
| `arc` | `ifdb_care_discharge_report` (+`_receiver`) | staging | Last archived Udskrivningsrapport (+receivers) |
| `arc` | `ifdb_diagnose` (+`_tillaegskode`) | staging | Last archived Diagnose (+tillægskoder) |
| `arc` | `ifdb_pas_diagnose` (+`_encounter_ref`, `_tillaegskode`, `_henvisning_ref`) | staging | Last archived PAS-diagnose (+ref tables) |
| `arc` | `ifdb_administrative_procedure` (+`ifdb_adm_procedure_suppl_codes`) | staging | Last archived Administrative procedure (+suppl codes) |
| `arc` | `ifdb_ris_ydelse` | staging | Last archived RIS-ydelse (XML verbatim; shared with ServiceRequest) |
| `arc` | `ifdb_henvisning` (+`_historik`) / `ifdb_referralinformation` | staging | Last archived Henvisning (+history, referral info) |
| `arc` | `ifdb_aktivitetsresultat` (+`ifdb_aktivitetsudfoerelse`, `_speciale`) | staging | Last archived Aktivitetsresultat/SFI (+lookups) |
| `arc` | `ifdb_eventmarker` | staging | Last archived EVENTMARKER (all 17 source columns; `DELETED` -> `dw_is_deleted`) |
| `arc` | `bom_ifdb_aftale` (+`bom_ifdb_aftaleressourcedeltager`) | staging | Last archived Appointment (AFTALE flatten + deltager-lookup) |
| `arc` | `bom_ifdb_orgenhed_sor` / `bom_ifdb_patientinfo` | staging | Last archived Location_SOR flatten / PATIENTINFO (CPR fallback) |
| `arc` | `sor_register` | staging | Last archived SOR register snapshot |
| `arc` | `haiba_agg_infektion` / `haiba_agg_ortopaedi` / `haiba_cases_infektion` / `haiba_cases_ssi` | staging | Last archived HAIBA table (archive merge; the delivery watermark itself lives in `meta.HAIBALeverance`). The HAIBA `ext` and `dm` layers log runs to `DFMRunLog` (mode locked to `full_load`) but own no checkpoint — `ext` borrows the arc owner's, and `dm` (target_type `datamart`) is a full rebuild with no delta position. |
| `fhir_server` | `Patient` / `Encounter` / `EpisodeOfCare` / `Location` / `Organization` | staging | Last FHIR-posted timestamp per resource |
| `fhir_server` | `DocumentReference_Epikrise` / `DocumentReference_CareDischargeReport` | staging | Last FHIR-posted DocumentReference per slice |
| `fhir_server` | `Condition_Diagnose` / `Condition_PasDiagnose` | staging | Last FHIR-posted Condition per slice |
| `fhir_server` | `Procedure_AdminProc` / `Procedure_RisYdelse` | staging | Last FHIR-posted Procedure per slice |
| `fhir_server` | `ServiceRequest` / `RehabilitationPlan` | staging | Last FHIR-posted ServiceRequest (henvisning+RIS streams / rehab-plan slice) |
| `fhir_server` | `Aktivitetsresultat_SFI` | staging | Last FHIR-posted Observation/Procedure/Condition (+Q/QR) from SFI |
| `fhir_server` | `CarePlan` | staging | Last FHIR-posted Alle Markoerer CarePlan (one per patient; scoped from the EVENTMARKER archive delta) |
| `fhir_server` | `Appointment` / `Location_SOR` | staging | Last FHIR-posted Appointment / SOR-Location |
| `fhir` | `Patient` / `Patient_Address` / `Encounter` / `Encounter_Location` / `EpisodeOfCare` / `Location` | staging | Last SOF extract per flat table |
| `fhir` | `DocumentReference` / `DocumentReference_Recipient` | staging | Last SOF extract (DocumentReference tables) |
| `fhir` | `ServiceRequest` / `ServiceRequest_SupplementaryCode` / `ServiceRequest_Recipient` | staging | Last SOF extract (ServiceRequest tables) |
| `fhir` | `Condition` / `Condition_SupplementaryCode` | staging | Last SOF extract (Condition tables; SOF is DFM — all four producers migrated) |
| `fhir` | `Procedure` / `Procedure_SupplementaryCode` | staging | Last SOF extract (Procedure tables; SOF is DFM) |
| `fhir` | `Observation` / `Questionnaire` / `Questionnaire_item` / `QuestionnaireResponse` / `QuestionnaireResponse_item` | staging | Last SOF extract (SFI tables; the EVENTMARKER-Observation assets remain unregistered stubs) |
| `fhir` | `CarePlan` / `CarePlan_Activity` | staging | Last SOF extract (CarePlan tables; the child runs `child_grain_reconcile`) |
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

---

## 10. DSO Sync (`scope='dso'`)

DSO table sync (dim/fct/fhir/…) tracks checkpoints on **staging** in
`meta.DFMState` with `scope='dso'`, separate from staging-side dim/fct builds.

### View DSO sync checkpoints

```sql
SELECT schema_name, table_name, checkpoint_value, last_mode, updated_at
FROM [meta].[DFMState]
WHERE scope = 'dso'
ORDER BY schema_name, table_name;
```

### View DSO sync run log

```sql
SELECT TOP 50 schema_name, table_name, mode, status,
       checkpoint_before, checkpoint_after, rows_affected, run_started_at
FROM [meta].[DFMRunLog]
WHERE scope = 'dso'
ORDER BY run_started_at DESC;
```

### Reset a DSO sync checkpoint (full re-sync on next run)

Via SSMS on **staging**:

```sql
EXEC [meta].[usp_dfm_reset]
    @schema_name = 'dim',
    @table_name  = 'Kreditor',
    @new_value   = '0001-01-01',
    @reason      = 'Re-sync dim.Kreditor to DSO',
    @scope       = 'dso';
```

Or use `dfm_reset_job` with `scope: dso` in the asset config when available.

### Cutover: migrate legacy SyncWatermarks → DFMState

Run **once per environment** (DEV → TEST → PROD) **before** the first watermark
DSO sync on the new code. Skipping this leaves `scope='dso'` empty, so the first
sync does a full MERGE for every `mode='watermark'` table.

**Preferred — Dagster**

1. Ensure `staging_infra_dfm` and `dso_infra_meta` have been materialized (asset deps).
2. Launch job `migrate_dso_sync_watermarks_to_dfm_job`, **or** materialize asset
   `migrate_dso_sync_watermarks_to_dfm`.

**CLI fallback** (same DB env as Dagster resources):

```bash
cd etl
python -m DSO.load.migrate_sync_watermarks_to_dfm
```

| Role | Env prefix | Table |
|------|------------|--------|
| Read | `MSSQL_TARGET_DSO_*` | DSO `[meta].[SyncWatermarks]` |
| Write | `MSSQL_TARGET_STAGING_*` (else `MSSQL_SOURCE_STAGING_*`) | staging `[meta].[DFMState]` |

Passwords (`*_PASSWORD`) are required for the CLI. Skips `sfi_datapoint.*`.
Idempotent: inserts missing rows, fills `checkpoint_value IS NULL` (auto-seeded if a
DSO sync ran first), leaves non-NULL checkpoints untouched. Logs
inserted / updated_null / skipped_existing separately.

Migrated checkpoints are stepped back one day (`DATEADD(DAY, -1, last_sync_timestamp)`)
per table so wall-clock SyncWatermarks values do not skip staging rows under exclusive
`>` on the first post-cutover delta. Steady-state delta still uses source `MAX()` — no
ongoing lookback. Prefer running before the first watermark DSO sync so those runs stay
delta; NULL-seed fill still recovers if ordering slips. Verify afterwards:

```sql
SELECT COUNT(*) FROM [meta].[DFMState] WHERE scope = 'dso';
```

**Note:** SFI datapoint refresh still uses legacy `[meta].[SyncWatermarks]` on DSO.
