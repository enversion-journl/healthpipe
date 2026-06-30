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

1. **map tables** — CleanCode mapping tables (IDENTITY on DW_EK, business key is CleanCode)
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
  ├── Staging reachable?   ──no───>  DSO has data? ──yes──> local_dev_skip
  │                                                 ──no──> ERROR
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
    watermark_column="DW_Timestamp_Update",
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
| `insert_only_columns` | `list[str]` | `["DW_Timestamp_Insert"]` | Columns set only on INSERT, not UPDATE |
| `identity_insert` | `bool` | `False` | Enable `SET IDENTITY_INSERT ON` (when PK is IDENTITY) |
| `watermark_column` | `str \| None` | `None` | Column for delta filter; required when `mode="watermark"` |
| `chunk_size` | `int` | `5000` | Rows per chunk in Python fallback path |

---

## 3. Delta Sync Configuration

### 3.1 How Watermark Mode Works

When `mode="watermark"`, the sync follows this flow on each run:

```
1. SyncStateManager.get_last_sync_timestamp(schema, table)
       │
       ├── Returns None (first run)
       │     └── build_merge_statement()          → full MERGE
       │
       └── Returns datetime
             └── build_delta_merge_statement()    → delta MERGE
                   WHERE COALESCE([watermark_col], [DW_Timestamp_Insert]) >= last_ts

2. Execute MERGE on DSO via linked server

3. SyncStateManager.update_sync_state(schema, table, rows, duration_ms)
       └── MERGE into [meta].[sync_watermarks] with SYSDATETIME()
```

The delta MERGE SQL adds a subquery with a WHERE filter:

```sql
MERGE [dim].[ExampleTable] AS tgt
USING (
    SELECT * FROM [STAGING_LINK].[DW].[dim].[ExampleTable]
    WHERE COALESCE([watermark_column], [DW_Timestamp_Insert]) >= '2026-02-15 08:00:00'
) AS src
ON tgt.[DW_EK_ExampleTable] = src.[DW_EK_ExampleTable]
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
```

The `COALESCE` ensures that rows where the watermark column is NULL are still captured via `DW_Timestamp_Insert` as a fallback.

### 3.2 SyncStateManager and State Tracking

State is persisted in `[meta].[sync_watermarks]` on the DSO database:

```sql
CREATE TABLE [meta].[sync_watermarks] (
    schema_name           NVARCHAR(128)   NOT NULL,
    table_name            NVARCHAR(128)   NOT NULL,
    last_sync_timestamp   DATETIME2(0)    NOT NULL,
    last_sync_rows        INT             NULL,
    last_sync_duration_ms INT             NULL,
    last_sync_status      NVARCHAR(50)    NULL,
    created_at            DATETIME2(0)    NOT NULL,
    updated_at            DATETIME2(0)    NOT NULL,
    CONSTRAINT PK_sync_watermarks PRIMARY KEY (schema_name, table_name)
);
```

Deployed via the `dso_infra_meta` Dagster asset (see `dagster_etl/infrastructure/assets/dso_infrastructure.py`).

The timestamp is always set server-side with `CAST(SYSDATETIME() AS datetime2(0))` to avoid clock skew between the Dagster worker and SQL Server.

### 3.3 Watermark Rules (Source of Truth)

Authoritative rules for delta sync data correctness and control. Implementation: `build_delta_merge_statement`, `SyncStateManager`, `[meta].[sync_watermarks]`.

**Filter predicate:** `COALESCE([watermark_column], [DW_Timestamp_Insert]) >= last_sync_timestamp`

- **watermark_column:** Specified per table in `SyncTableConfig.watermark_column` (commonly `DW_Timestamp_Update`).
- **last_sync_timestamp:** From `[meta].[sync_watermarks]` for that table (DSO server time, see below).
- **COALESCE:** Rows with NULL `DW_Timestamp_Update` are still included when `DW_Timestamp_Insert >= last_sync_timestamp`.

**Boundary: inclusive `>=`**

We use **inclusive** `>=`, not `>`.

| Choice | Effect | Our decision |
|--------|--------|--------------|
| **>= (inclusive)** | Rows with timestamp equal to `last_sync_timestamp` are included. No rows lost at boundary. Re-running reprocesses boundary rows; MERGE is idempotent (same PK → UPDATE), so no duplicate rows. | **Used.** |
| **> (exclusive) + safety lag** | Avoids reprocessing the exact boundary second but requires defined lag and more complex semantics. | Not used. |

**Guarantees:** (1) No lost rows at boundary. (2) No duplicates from double processing (MERGE on PK).

**NULL in `DW_Timestamp_Update`:** Rows are included when `COALESCE(DW_Timestamp_Update, DW_Timestamp_Insert) >= last_sync_timestamp`, i.e. new inserts not yet updated are captured.

**Concurrent inserts during sync window:** Rows inserted after `last_sync_timestamp` and before the next watermark are included in the next delta. Inclusive `>=` ensures no row is permanently missed; reprocessing is idempotent.

**Where the boundary comes from / clock skew:** `last_sync_timestamp` is read from `[meta].[sync_watermarks]` per `(schema_name, table_name)`. It is set **only after a successful sync** using **DSO server time** (`SYSDATETIME()` in `SyncStateManager.update_sync_state()`). No client or Dagster worker clock is used, so clock skew does not affect which rows are selected.

**`[meta].[sync_watermarks]` columns:** `schema_name`, `table_name` (PK, one row per table); `last_sync_timestamp` (lower bound for next delta, DSO `SYSDATETIME()`); `last_sync_rows`, `last_sync_duration_ms`, `last_sync_status` (observability); `updated_at` (traceability). Updated only on success. Query `SELECT * FROM [meta].[sync_watermarks] ORDER BY updated_at DESC` for last sync per table.

**Implementation checklist:** (1) `build_delta_merge_statement` WHERE must use `COALESCE(..., ...) >= '<timestamp>'`. (2) `SyncStateManager.update_sync_state` must set `last_sync_timestamp` and `updated_at` via DSO `SYSDATETIME()`. (3) First run: no row in `sync_watermarks` → `get_last_sync_timestamp` returns `None` → full MERGE; after success, one row inserted with current DSO time.

### 3.4 When to Use Which Mode

| Mode | Use When | Typical Tables |
|------|----------|----------------|
| `merge` | Small reference/map tables where full scan is cheap | map.* (where domain syncs map), sec.* reference tables |
| `watermark` | Dimension and fact tables that grow over time | dim.*, fct.* |
| `truncate_insert` | Type 2 SCD tables or full-refresh security tables | SCD2 dimensions, sec.* full-refresh tables |

### 3.5 Watermark Column Selection

The watermark column is specified per table in `SyncTableConfig.watermark_column`. Common choices:

- **`DW_Timestamp_Update`** — Standard for dim/fct tables where rows are updated in place.
- **`DW_Timestamp_Insert`** — Append-only tables where rows are never updated after creation.

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
    watermark_column="DW_Timestamp_Update",
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

Watermark state can also be queried directly:

```sql
SELECT * FROM [meta].[sync_watermarks] ORDER BY updated_at DESC;
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
1. Update the DSO table DDL in `dagster_etl/infrastructure/ddl/tables/`
2. Run the corresponding `dso_infra_*` asset to apply DDL
3. Re-run the sync

Schema validation runs automatically when `validate_schemas=True` (the default).

### Row Count Validation Failures

**Symptom:** `counts_match: False` in Dagster metadata.

**Possible causes:**
- Sync ran in watermark mode but staging had rows inserted between the MERGE and the count check (harmless, resolves on next run)
- Table has an IDENTITY sentinel row (`DW_EK = -1`) counted differently in staging vs DSO
- Python fallback was used and an error occurred mid-transfer

**Fix:** Check whether the count difference is within the expected delta window.  If large, run a manual full sync:

```sql
-- Reset watermark to force full sync on next run
DELETE FROM [meta].[sync_watermarks]
WHERE schema_name = '<schema>' AND table_name = '<table>';
```

### SyncStateManager Reset

To force a full sync for a specific table (bypassing delta), delete its watermark row:

```sql
DELETE FROM [meta].[sync_watermarks]
WHERE schema_name = '<schema>' AND table_name = '<table>';
```

The next sync run will detect `last_sync_timestamp = None` and execute a full MERGE.

To reset all watermarks (force full sync for every table):

```sql
TRUNCATE TABLE [meta].[sync_watermarks];
```

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

Documented in detail in `dagster_etl/docs/architecture/dso_delta_load_strategy.md`.

**Decision:** Watermark-based delta load using `DW_Timestamp_Update` / `DW_Timestamp_Insert`.

**Alternatives considered:**
1. **Change Data Capture (CDC):** Rejected due to SQL Server edition requirements, higher operational complexity, and Azure SQL Edge incompatibility.
2. **Hash-based comparison:** Rejected due to full table scan requirement on every run and high compute cost for large fact tables.
3. **Full MERGE every run:** The baseline; replaced for dim/fct tables because evaluating millions of unchanged rows per sync is wasteful.

**Key design decisions:**
- `COALESCE(watermark_col, DW_Timestamp_Insert)` in the WHERE clause ensures rows with NULL watermark columns are still captured.
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
| `DSO/load/sync_state.py` | `SyncStateManager` class for watermark state tracking |
| `DSO/load/sync_dim.py` | Dimension sync Dagster assets (ERP and shared dimensions) |
| `DSO/load/sync_fct.py` | Fact sync Dagster assets |
| `DSO/load/sync_fhir.py` | FHIR flat-table sync Dagster assets |
| `dagster_etl/infrastructure/ddl/tables/meta/01_sync_watermarks.sql` | DDL for `[meta].[sync_watermarks]` table |
| `dagster_etl/infrastructure/assets/dso_infrastructure.py` | Infrastructure assets (`dso_infra_meta`, `dso_infra_schemas`, etc.) |
| `dagster_etl/docs/architecture/dso_delta_load_strategy.md` | Full decision document for the delta load strategy |
