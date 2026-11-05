<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DSO Sync Framework Guide

Reference documentation for the DSO (Data Staging Out) synchronization framework.
Covers architecture, configuration, delta sync, performance tuning, and troubleshooting.

**Which tables are synced?** The authoritative list lives in `DSO/load/sync_configs.py`. This guide describes the *logic* — modes and watermarks — not the concrete table inventory, which changes as domains evolve.

---

## 1. Architecture Overview

### 1.1 DSA to DSO Data Flow

```
DSA (Staging DB)                              DSO (Reporting DB)
+------------------------+                    +------------------------+
| map.*                  |                    |                        |
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

The sync framework moves data from DSA (staging) to DSO (reporting) in two conceptual layers:

1. **dim tables** — Dimension tables (DW_EK is PK)
2. **fct tables** — Fact tables (various PKs, may have IDENTITY columns)

The `map` tables stay on staging: the DSO database has no `ext`, `arc`, `map` or `stg` schema. See `sync_configs.py` for the per-domain configuration.

Dagster assets enforce the ordering: `dim -> fct`. Each sync asset depends on its upstream staging asset completing first.

### 1.2 Linked Server Sync

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
        ├── Available?    ──yes──>  _sync_via_linked_server()
        │
        └── Unavailable   ──────>  ERROR (names the linked server and the table)
```

**Linked server path** (`_sync_via_linked_server`): Executes MERGE entirely on DSO via 4-part naming (`[STAGING_LINK].[DW].[schema].[table]`).  Zero data flows through Python.  Supports all three modes: `merge`, `watermark`, and `truncate_insert`.

A failed linked server fails the sync. The checkpoint does not move, so the next run after the repair catches up.

### 1.3 Sync Modes

| Mode | SQL Pattern | Use Case |
|------|-------------|----------|
| `merge` | Full `MERGE` of all rows | Small reference tables |
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
    deps=["dim_nytabel"],
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

Add it to the appropriate list in the same file. For a new file, import its list in `DSO/load/__init__.py` and add it to `all_dso_sync_assets`. Lists in `sync_fhir.py` are imported directly in `definitions.py`.

### Step 4: Create the view (optional)

If the table needs a view, add the `CREATE OR ALTER VIEW` DDL to the appropriate SQL file under `infrastructure/ddl/views/erp/` or `infrastructure/ddl/views/security/`.

### SyncTableConfig Field Reference

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `schema` | `str` | *required* | Target schema (`dim`, `fct`, `sec`) |
| `table` | `str` | *required* | Target table name |
| `primary_key` | `str \| list[str]` | *required* | PK column(s) used in MERGE ON clause |
| `mode` | `Literal` | `"merge"` | Sync mode: `"merge"`, `"watermark"`, or `"truncate_insert"` |
| `exclude_columns` | `list[str]` | `[]` | Columns to exclude from sync (e.g. IDENTITY columns) |
| `insert_only_columns` | `list[str]` | `["dw_timestamp_insert"]` | Columns set only on INSERT, not UPDATE |
| `identity_insert` | `bool` | `False` | Enable `SET IDENTITY_INSERT ON` (when PK is IDENTITY) |
| `watermark_column` | `str \| None` | `None` | Column for delta filter; required when `mode="watermark"` |

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
| `merge` | Small reference tables where full scan is cheap | sec.* reference tables |
| `watermark` | Dimension and fact tables that grow over time | dim.*, fct.* |
| `truncate_insert` | Type 2 SCD tables or full-refresh security tables | SCD2 dimensions, sec.* full-refresh tables |

### 3.5 Watermark Column Selection

The watermark column is specified per table in `SyncTableConfig.watermark_column`. Common choices:

- **`dw_timestamp_update`** — Standard for dim/fct tables where rows are updated in place.
- **`dw_timestamp_insert`** — Append-only tables where rows are never updated after creation.

See `sync_configs.py` for the actual column used per table.

---

## 4. Performance Tuning

### 4.1 Linked Server Optimisation

The linked server path is the only write path. It gives:
- Zero data flows through Python; the MERGE executes entirely on SQL Server
- SQL Server optimises the join/filter at the engine level
- Network overhead is minimal (server-to-server within the same network)

Ensure `STAGING_LINK` is configured with the correct security context and that the DSO server has network access to the staging server.

### 4.2 Monitoring via Dagster UI

Every sync asset returns `MaterializeResult` metadata visible in the Dagster UI:

| Key | Description |
|-----|-------------|
| `rows_affected` | Rows inserted or updated by the MERGE |
| `sync_mode` | Effective mode used: `merge`, `watermark_delta`, `watermark_full`, `truncate_insert`, `same_database` |
| `sync_engine` | Engine used: `linked_server`, `same_database`, `local_dev_skip`. Use to filter runs. |
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

**Fix:** Check network connectivity, credentials, and firewall rules between DSO and staging servers.

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
- DSO has more rows: MERGE does not delete DSO-only rows

**Fix:** Re-run the sync.  If it fails again, run the table's DSO sync asset with `mode: full_load` (see "Checkpoint Reset" below).

### Checkpoint Reset

To force a full sync for a specific table, run its DSO sync asset with `mode: full_load`,
or move its checkpoint on staging with `usp_dfm_reset` (`@scope = 'dso'`); see
`dfm_operations_guide.md` §10.

### Same-Database Mode

**Symptom:** Sync returns `mode: "same_database"` and `rows_affected: 0`.

**Fix:** This is expected in local dev where staging and DSO share the same database.  No actual sync occurs because the data is already in place.  In production, ensure staging and DSO point to separate databases.

---

## 6. Runtime policy: sync engine

Every sync uses the **linked server** (SQL server-side sync). When the linked server does not exist or does not answer, the sync fails with an error that names the linked server and the table. An integrity error during the MERGE (SQLSTATE 23000: a unique-key, foreign-key or NOT NULL violation) triggers one TRUNCATE + INSERT recovery over the linked server. Any other error during the transfer fails the sync. The checkpoint does not move, so the next run after the repair catches up.

Every sync asset returns `sync_engine` in its metadata: `linked_server` \| `same_database` \| `local_dev_skip`.

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
