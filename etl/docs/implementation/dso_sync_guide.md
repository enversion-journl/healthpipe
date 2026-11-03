<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DSO Sync Framework Guide

Reference documentation for the DSO (Data Staging Out) synchronization framework.
Covers architecture, configuration, delta sync, performance tuning, and troubleshooting.

**Which tables are synced?** The authoritative list lives in `DSO/load/sync_configs.py`. This guide describes the *logic* — modes, watermarks, fallbacks — not the concrete table inventory, which changes as domains evolve.

---

## 1. Architecture Overview

### 1.1 DSA to DSO Data Flow

```
DSA (Staging DB)                              DSO (Reporting DB)
+------------------------+                    +------------------------+
| map.*                  |                    | map.*                  |
| dim.*                  | --STAGING_LINK-->  | dim.*                  |
| fct.*                  |     (MERGE)        | fct.*                  |
+------------------------+                    +------------------------+
                                                        |
                                                        v
                                              +------------------------+
                                              | vw_dim.* / vw_fct.*    |
                                              | (Security views)       |
                                              +------------------------+
```

The sync framework moves data from DSA (staging) to DSO (reporting) in three conceptual layers:

1. **map tables** — clean_code mapping tables (IDENTITY on DW_EK, business key is clean_code)
2. **dim tables** — Dimension tables (DW_EK is PK)
3. **fct tables** — Fact tables (various PKs, may have IDENTITY columns)

Not all domains sync all layers. Some domains sync map → dim → fct; others skip map and sync dim/fct only. See `sync_configs.py` for the per-domain configuration.

Dagster assets enforce the ordering: `map -> dim -> fct` where map exists. Each sync asset depends on its upstream staging asset completing first.

### 1.2 Linked Server vs Python Fallback

`sync_table_via_linked_server()` is the main entry point.  It uses a decision cascade:

```
sync_table_via_linked_server()
  │
  ├── Same-database mode?  ──yes──>  Skip sync (data already in place)
  │
  ├── Staging reachable?   ──no───>  Local (no DAGSTER_CODE_LOCATION_NAME) and DSO has data? ──yes──> local_dev_skip
  │   (probe tried 4 times)                                                                  ──no──> ERROR
  │
  └── Validate linked server
        │
        ├── Available?  ──yes──>  _sync_via_linked_server()   [preferred]
        │
        └── Unavailable + staging reachable?
              ──yes──>  Fallback allowed? (default yes) ──yes──>  _sync_via_python() [fallback, logged as ERROR]
              │         Set DSO_SYNC_ALLOW_PYTHON_FALLBACK=false ──no──>  ERROR (fail run)
              ──no───>  ERROR
```

**Linked server path** (`_sync_via_linked_server`): Executes MERGE entirely on DSO via 4-part naming (`[STAGING_LINK].[DW].[schema].[table]`).  Zero data flows through Python.  Supports all three modes: `merge`, `watermark`, and `truncate_insert`.

**Python fallback** (`_sync_via_python`): Reads from staging via `fetchmany()` in configurable chunks and inserts into DSO via `executemany()`.  Always uses TRUNCATE + INSERT regardless of the configured mode.  Keeps memory constant via streaming.

### 1.3 Sync Modes

| Mode | SQL Pattern | Use Case |
|------|-------------|----------|
| `merge` | Full `MERGE` of all rows | Map tables, small reference tables |
| `watermark` | Delta `MERGE` filtered by watermark column | Dimension and fact tables |
| `truncate_insert` | `TRUNCATE` + `INSERT` | Type 2 SCD dimensions, security tables |

---

## 2. Adding a New Table to DSO Sync

### Step 1: Add configuration in `sync_configs.py`

The complete list of synced tables lives here. Add a new `SyncTableConfig`:

```python
# DSO/load/sync_configs.py

"dim_nytabel": SyncTableConfig(
    schema="dim",
    table="NyTabel",
    primary_key="DW_EK_NyTabel",
    mode="watermark",
    watermark_column="dw_timestamp_update",
),
```

### Step 2: Create the sync asset

```python
# DSO/load/sync_dim.py (or sync_fct.py / sync_fhir.py, depending on layer)

@dg.asset(
    name="dso_sync_dim_nytabel",
    group_name="<domain>_dso_sync",
    compute_kind="mssql",
    deps=["dim_nytabel", "dso_sync_map_nytabel"],  # include map dep if domain syncs map
    tags={"domain": "<domain>", "layer": "dso_sync", "type": "dimension"},
    description="Sync dim.NyTabel from staging to DSO database",
)
def dso_sync_dim_nytabel(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
    mssql_source_staging: MSSQLResource,
) -> dg.MaterializeResult:
    config = <DOMAIN>_DIM_CONFIGS["dim_nytabel"]
    result = sync_table_via_linked_server(
        context, mssql_target_dso, config,
        mssql_source_staging=mssql_source_staging,
        validate_schemas=True,
    )
    counts = compare_row_counts(
        context, mssql_target_dso, config.schema, config.table,
        mssql_source_staging=mssql_source_staging,
    )
    return dg.MaterializeResult(
        metadata={
            "rows_affected": result["rows_affected"],
            "sync_mode": result["mode"],
            "staging_count": counts["staging_count"],
            "dso_count": counts["dso_count"],
            "counts_match": counts["match"],
        }
    )
```

### Step 3: Register the asset

Add it to the appropriate `all_*_dso_sync_assets` list in the same file, and export it from `DSO/load/__init__.py`.

### Step 4: Create the view (optional)

If the table needs a view, add the `CREATE OR ALTER VIEW` DDL to the appropriate SQL file under `infrastructure/ddl/views/erp/` or `infrastructure/ddl/views/security/`.

### SyncTableConfig Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | `str` | *required* | Target schema (`dim`, `fct`, `map`, `sec`) |
| `table` | `str` | *required* | Target table name |
| `primary_key` | `str \| list[str]` | *required* | PK column(s) used in MERGE ON clause |
| `mode` | `Literal` | `"merge"` | Sync mode: `"merge"`, `"watermark"`, or `"truncate_insert"` |
| `exclude_columns` | `list[str]` | `[]` | Columns to exclude from sync (e.g. IDENTITY columns on map tables) |
| `insert_only_columns` | `list[str]` | `["dw_timestamp_insert"]` | Columns set only on INSERT, not UPDATE |
| `identity_insert` | `bool` | `False` | Enable `SET IDENTITY_INSERT ON` (when PK is IDENTITY) |
| `watermark_column` | `str \| None` | `None` | Column for delta filter; required when `mode="watermark"` |
| `chunk_size` | `int` | `5000` | Rows per chunk in Python fallback path |

---

## 3. Delta Sync Configuration

### 3.1 How Watermark Mode Works

When `mode="watermark"`, the sync follows this flow on each run:

```
1. DfmManager.start_dfm_run(..., scope='dso') on staging
       │
       ├── sample max_now = MAX(COALESCE(wm, insert)) on staging  (BEFORE MERGE)
       │
       ├── checkpoint None (first run)
       │     └── MERGE with COALESCE(...) <= max_now (or unbounded if empty)
       │
       └── checkpoint NVARCHAR from meta.DFMState
             └── delta MERGE / OPENQUERY batches
                   WHERE COALESCE([wm], [insert]) > checkpoint
                     AND COALESCE([wm], [insert]) <= max_now

2. Execute MERGE on DSO via linked server

3. On success with rows > 0:
       set_checkpoint(max_now)   -- pre-sampled; never re-query MAX after MERGE
       complete_dfm_run(..., checkpoint_after=max_now)
   On success with 0 rows: complete_dfm_run only (checkpoint unchanged)
```

The delta MERGE SQL adds a subquery with a WHERE filter:

```sql
MERGE [dim].[ExampleTable] AS tgt
USING (
    SELECT * FROM [STAGING_LINK].[DW].[dim].[ExampleTable]
    WHERE COALESCE([watermark_column], [dw_timestamp_insert]) > '2026-02-15 08:00:00'
      AND COALESCE([watermark_column], [dw_timestamp_insert]) <= '2026-02-15 09:00:00'
) AS src
ON tgt.[DW_EK_ExampleTable] = src.[DW_EK_ExampleTable]
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
```

The `COALESCE` ensures that rows where the watermark column is NULL are still captured via `dw_timestamp_insert` as a fallback. The inclusive upper bound is the pre-MERGE `max_now` so mid-sync inserts cannot advance the checkpoint past unread rows.

### 3.2 DfmManager and State Tracking

DSO table sync state lives in `[meta].[DFMState]` on **staging** with `scope='dso'`.
Run history is in `[meta].[DFMRunLog]`. See `dfm_design.md` §8 and `DSO/load/sync_dfm.py`.

The SFI datapoint store keeps its checkpoint in the same table with `scope='sfi_store'`
(`fhir.SFI_datapoint_store`). No sync or refresh reads `[meta].[SyncWatermarks]` on DSO.

DSO stamps its own writes: every sync path sets `dw_timestamp_insert` and
`dw_timestamp_update` to `CAST(SYSUTCDATETIME() AS datetime2(0))` on INSERT and UPDATE
(`WRITE_TIMESTAMP_UTC` in `utils/sql/write_timestamp.py`).

### 3.3 Watermark Rules (Source of Truth)

Authoritative rules for delta sync data correctness and control. Implementation:
`build_delta_merge_statement`, `DfmManager` (`scope='dso'`), `[meta].[DFMState]`.

**Filter predicate:** `COALESCE([watermark_column], [dw_timestamp_insert]) > checkpoint_value`

- **watermark_column:** Specified per table in `SyncTableConfig.watermark_column` (commonly `dw_timestamp_update`).
- **checkpoint_value:** From `[meta].[DFMState]` with `scope='dso'` (NVARCHAR, CONVERT style 121).
- **COALESCE:** Rows with NULL `dw_timestamp_update` are still included when insert timestamp exceeds the checkpoint.

**Boundary: exclusive `>` (DFM Strategy B)**

Checkpoint is `MAX(COALESCE(watermark, insert))` from the staging source after a
successful sync. The next run uses exclusive `>` so boundary rows are not
re-MERGE'd every day (DSO MERGE stamps `dw_timestamp_update = SYSDATETIME()`).

| Choice | Effect | Our decision |
|--------|--------|--------------|
| **> (exclusive)** | Rows with timestamp equal to checkpoint were consumed last run; skip them. Aligns with staging DFM delta semantics. | **Used.** |
| **>= (inclusive)** | With MAX checkpoint, re-processes entire last timestamp second every run. | Not used. |

**Checkpoint source:** Before the MERGE, sample staging
`MAX(COALESCE(watermark_col, dw_timestamp_insert))` as `max_now`. On success with
rows > 0, `set_checkpoint(max_now)` — not a post-MERGE `MAX` and not DSO
`SYSDATETIME()`. Every source read is bounded by
`> checkpoint AND <= max_now`.

**Zero-row guard:** Do not advance checkpoint when `rows_affected == 0`.

**Row-count mismatch recovery (watermark delta only):** After every
`watermark_delta` (including zero-row), compare `COUNT(*)` staging vs DSO.
Full-MERGE recovery runs **only when `dso_count < staging_count`** (missing rows
on DSO). When `dso_count >= staging_count` (DSO-only residue / orphans), log a
warning and skip recovery — MERGE does not delete those. Disable with
`auto_recover_mismatch=False`.

**Implementation checklist:** (1) `build_delta_merge_statement` / OPENQUERY WHERE
must use `COALESCE(...) > checkpoint AND COALESCE(...) <= max_now`. (2)
`set_checkpoint` uses the pre-MERGE `max_now`, never a post-MERGE MAX. (3) First
run: no DFM row → pin `max_now` and bound the source (or unbounded full MERGE if
empty); after success, checkpoint = `max_now`.

### 3.4 When to Use Which Mode

| Mode | Use When | Typical Tables |
|------|----------|----------------|
| `merge` | Small reference/map tables where full scan is cheap | map.* (where domain syncs map), sec.* reference tables |
| `watermark` | Dimension and fact tables that grow over time | dim.*, fct.* |
| `truncate_insert` | Type 2 SCD tables or full-refresh security tables | SCD2 dimensions, sec.* full-refresh tables |

### 3.5 Watermark Column Selection

The watermark column is specified per table in `SyncTableConfig.watermark_column`. Common choices:

- **`dw_timestamp_update`** — Standard for dim/fct tables where rows are updated in place.
- **`dw_timestamp_insert`** — Append-only tables where rows are never updated after creation.

See `sync_configs.py` for the actual column used per table.

---

## 4. Performance Tuning

### 4.1 Chunk Size (Python Fallback)

The `chunk_size` field controls how many rows `fetchmany()` retrieves per iteration in the Python fallback path.  Memory usage stays constant regardless of table size.

```python
SyncTableConfig(
    schema="fct",
    table="ExampleFact",
    primary_key="DW_EK_ExampleFact",
    mode="watermark",
    watermark_column="dw_timestamp_update",
    chunk_size=10000,  # larger chunks for high-throughput tables
)
```

Guidelines:
- Default `5000` works well for most tables
- Increase to `10000-20000` for large fact tables with simple column types
- Decrease to `2000-3000` for tables with wide rows (many NVARCHAR(MAX) columns)
- Progress is logged every 10 chunks

### 4.2 Linked Server Optimisation

The linked server path is always preferred because:
- Zero data flows through Python; the MERGE executes entirely on SQL Server
- SQL Server optimises the join/filter at the engine level
- Network overhead is minimal (server-to-server within the same network)

Ensure `STAGING_LINK` is configured with the correct security context and that the DSO server has network access to the staging server.

### 4.3 Monitoring via Dagster UI

Every sync asset returns `MaterializeResult` metadata visible in the Dagster UI:

| Key | Description |
|-----|-------------|
| `rows_affected` | Rows inserted or updated by the MERGE |
| `sync_mode` | Effective mode used: `merge`, `watermark_delta`, `watermark_full`, `truncate_insert`, `same_database`, `python_*` |
| `sync_engine` | Engine used: `linked_server`, `python_fallback`, `same_database`, `local_dev_skip`. Use to filter runs. |
| `fallback_reason` | Set only when `sync_engine=python_fallback`: `linked_server_not_available`, `linked_server_validation_failed`, `connectivity` |
| `sync_duration_seconds` | Wall-clock time of the sync operation in seconds (all paths) |
| `rows_per_second` | Throughput: `rows_affected / sync_duration_seconds` (0.0 when no rows or no-op) |
| `staging_count` | Row count in staging after sync |
| `dso_count` | Row count in DSO after sync |
| `counts_match` | Whether staging and DSO counts match |

The `sync_mode` value distinguishes between a watermark first-run (`watermark_full`) and a subsequent delta run (`watermark_delta`).  Dagster plots `sync_duration_seconds` and `rows_per_second` as time-series automatically, enabling performance trend monitoring across runs.

Checkpoint state can also be queried directly on staging:

```sql
SELECT schema_name, table_name, checkpoint_value, last_mode, updated_at
FROM [meta].[DFMState] WHERE scope = 'dso' ORDER BY updated_at DESC;
```

---

## 5. Troubleshooting

### Linked Server Errors

**Symptom:** `RuntimeError: Linked server 'STAGING_LINK' does not exist on DSO database.`

**Fix:** Run the `dso_infra_linked_server` infrastructure asset to create the linked server, or verify it exists:

```sql
SELECT * FROM sys.servers WHERE name = 'STAGING_LINK';
EXEC sp_testlinkedserver @servername = N'STAGING_LINK';
```

**Symptom:** Linked server exists but is not accessible.

**Fix:** Check network connectivity, credentials, and firewall rules between DSO and staging servers.  In local dev (Azure SQL Edge), linked servers are unsupported; set `DSO_SYNC_ALLOW_PYTHON_FALLBACK=true` and ensure `mssql_source_staging` is provided to allow Python fallback (see §6).

### Schema Mismatch

**Symptom:** `SchemaValidationError: Column mismatch between staging and DSO for <schema>.<table>`

**Fix:** The DSO layer must never modify DSA schemas.  If staging columns changed:
1. Update the DSO table DDL in `etl/infrastructure/ddl/tables/`
2. Run the corresponding `dso_infra_*` asset to apply DDL
3. Re-run the sync

Schema validation runs automatically when `validate_schemas=True` (the default).

### Row Count Validation Failures

**Symptom:** The sync fails with `ROW_COUNT_MISMATCH` (DSO has fewer rows than staging), or `counts_match: False` in Dagster metadata (DSO has more rows than staging; the run stays green).

**Possible causes:**
- Staging had rows inserted between the MERGE and the count check
- Table has an IDENTITY sentinel row (`DW_EK = -1`) counted differently in staging vs DSO
- Python fallback was used and an error occurred mid-transfer
- DSO has more rows: MERGE does not delete DSO-only rows

**Fix:** Re-run the sync.  If it fails again, run the table's DSO sync asset with `mode: full_load` (see "Checkpoint Reset" below).

### Checkpoint Reset

To force a full sync for a specific table, run its DSO sync asset with `mode: full_load`,
or move its checkpoint on staging with `usp_dfm_reset` (`@scope = 'dso'`); see
`dfm_operations_guide.md` §10.

### Python Fallback Issues

**Symptom:** `OutOfMemoryError` during Python fallback sync.

**Fix:** This should not happen with the chunked `fetchmany()` approach.  If it does, reduce `chunk_size` in the table's `SyncTableConfig`.

**Symptom:** Sync returns `mode: "same_database"` and `rows_affected: 0`.

**Fix:** This is expected in local dev where staging and DSO share the same database.  No actual sync occurs because the data is already in place.  In production, ensure staging and DSO point to separate databases.

---

## 6. Runtime policy: sync engine and fallback

Normal operation must use **linked server** (SQL server-side sync). Python fallback is a **controlled contingency** only.

### 6.1 Metadata: sync_engine and fallback_reason

Every sync asset returns:

| Key | Description |
|-----|-------------|
| `sync_engine` | `linked_server` \| `python_fallback` \| `same_database` \| `local_dev_skip` |
| `fallback_reason` | Present only when `sync_engine=python_fallback`. One of: `linked_server_not_available`, `linked_server_validation_failed`, `connectivity` |

Drift can **filter runs by engine type** (e.g. only runs where `sync_engine != 'linked_server'`) to detect or alarm on fallback use.

### 6.2 Python fallback performance

The Python fallback streams data from staging to DSO in chunks via `fetchmany()`.

**Watermark-delta mode** uses a temp table + set-based MERGE per chunk:

1. `SELECT TOP 0 ... INTO #_sync_<schema>_<table>` (create empty temp table matching target schema)
2. Per chunk: `executemany(INSERT INTO #temp)` → single `MERGE target USING #temp` → `TRUNCATE #temp`
3. Temp table dropped in `finally` block

This replaces the previous row-level `MERGE ... USING (VALUES (?,...))` which compiled a plan per row. The set-based approach lets SQL Server optimise the join and reduces round-trips from N to N/chunk_size.

**Prerequisite:** The Dagster DSO service account needs `CREATE TABLE` permission in `tempdb` to use the temp-table strategy. Most SQL Server logins have this via the `public` role by default. If the permission is missing, the code falls back gracefully to row-level `MERGE ... USING (VALUES)` with a logged warning — no data loss, just slower throughput.

**Full-sync modes** (truncate_insert, merge first-run, watermark first-run) use `TRUNCATE` + `executemany(INSERT)` per chunk — no MERGE overhead.

**Recommended chunk_size:**

| Table type | chunk_size | Memory per chunk | Rationale |
|-----------|-----------|-----------------|-----------|
| Dimensions (broad, NVARCHAR(MAX), ~100k rows) | 5,000 (default) | ~7-8 MB | Wide rows with MAX fields; keeps memory <10 MB per chunk |
| Dimensions (narrow, no MAX fields, up to 1M rows) | 5,000–10,000 | ~5-10 MB | Override to 10k if row width allows and round-trips dominate |
| Facts (narrow, FK+measures, many rows) | 20,000 | ~7-14 MB | ~0.5-0.7 KB/row; large batches amortise MERGE and network cost |
| Map tables (small, merge mode) | 5,000 (default) | <5 MB | Small tables; chunk_size rarely matters |

Memory budget: keep each chunk under ~15 MB to stay safe in workers with 512 MB–1 GB RAM.
Multiple chunks are never held simultaneously (fetchmany + process + discard).

Tune by measuring `rows_per_second` in asset metadata. Set `chunk_size` in `SyncTableConfig` per table.

### 6.3 Fallback is default; require linked server in higher environments

- **Default:** Python fallback is **allowed** when the linked server is unavailable (env unset or `true`).
- **Env var:** `DSO_SYNC_ALLOW_PYTHON_FALLBACK`. Set to `false` or `0` to **forbid** fallback and fail the run if the linked server is unavailable (use this in closed/production-like environments to enforce linked-server-only).
- **Local dev (e.g. Azure SQL Edge):** No change needed; fallback is default.

When fallback is disabled (`DSO_SYNC_ALLOW_PYTHON_FALLBACK=false`) and the linked server is unavailable, the asset fails with a clear error.

### 6.4 Monitoring: fallback always alarms

- When Python fallback is used, the code logs at **ERROR** level with prefix `DSO_SYNC_FALLBACK_ACTIVE` and the `fallback_reason`.
- Asset metadata always includes `sync_engine`; when fallback is used, `fallback_reason` is also set.
- Configure alerting (e.g. on log pattern `DSO_SYNC_FALLBACK_ACTIVE` or on metadata `sync_engine=python_fallback`) so that fallback use is never silent.

---

## 7. Decision Log: Delta Load Strategy

Documented in detail in `etl/docs/architecture/dso_delta_load_strategy.md`.

**Decision:** Watermark-based delta load using `dw_timestamp_update` / `dw_timestamp_insert`.

**Alternatives considered:**
1. **Change Data Capture (CDC):** Rejected due to SQL Server edition requirements, higher operational complexity, and Azure SQL Edge incompatibility.
2. **Hash-based comparison:** Rejected due to full table scan requirement on every run and high compute cost for large fact tables.
3. **Full MERGE every run:** The baseline; replaced for dim/fct tables because evaluating millions of unchanged rows per sync is wasteful.

**Key design decisions:**
- `COALESCE(watermark_col, dw_timestamp_insert)` in the WHERE clause ensures rows with NULL watermark columns are still captured.
- `SYSDATETIME()` is used server-side for watermark timestamps to avoid clock skew.
- First run (no watermark) automatically falls back to full MERGE, so no manual initialisation is needed.
- Map tables (where synced) remain on `mode="merge"` because they are small and do not benefit from delta logic.
- Type 2 SCD dimensions with IDENTITY surrogate key use `truncate_insert`.

---

## 8. File Reference

| File | Purpose |
|------|---------|
| `DSO/load/sync_utils.py` | Core sync logic: `SyncTableConfig`, `build_merge_statement()`, `build_delta_merge_statement()`, `sync_table_via_linked_server()` |
| `DSO/load/sync_configs.py` | **Source of truth** for which tables are synced and their configuration (mode, watermark column, etc.) |
| `DSO/load/sync_dim.py` | Dimension sync Dagster assets (ERP and shared dimensions) |
| `DSO/load/sync_fct.py` | Fact sync Dagster assets |
| `DSO/load/sync_fhir.py` | FHIR flat-table sync Dagster assets |
| `etl/infrastructure/ddl/tables/meta/01_sync_watermarks.sql` | DDL for `[meta].[SyncWatermarks]` table |
| `etl/infrastructure/assets/dso_infrastructure.py` | Infrastructure assets (`dso_infra_meta`, `dso_infra_schemas`, etc.) |
| `etl/docs/architecture/dso_delta_load_strategy.md` | Full decision document for the delta load strategy |
