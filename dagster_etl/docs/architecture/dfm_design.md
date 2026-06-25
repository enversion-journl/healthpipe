<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Data Flow Management (DFM) Design

A unified design for checkpoint tracking, archive persistence, and ad-hoc overrides across all pipeline layers (extract, archive, dim/fact, FHIR, DSO sync).

---

## 1. Overview

The DFM system provides:

- **Checkpoint tracking** — each pipeline layer knows "where it left off" via a stored checkpoint value
- **Archive persistence** — a durable `arc.*` layer between volatile `ext.*` and downstream transforms
- **Ad-hoc flexibility** — period or ID-based runs that reuse the same assets without advancing the production checkpoint
- **Delete detection** — soft-delete of rows that disappear from source, gated by extract completeness
- **Manual checkpoint reset** — for triggering full reloads or rewinding the starting point
- **DSO sync tracking** — unified delta-sync state for staging-to-DSO transfers

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                              Pipeline Flow                                       │
│                                                                                  │
│  Source DB ──► ext.* (volatile) ──► arc.* (persistent) ──► pre_stg / stg         │
│                                                                │                 │
│                                              ┌─────────────────┼──────────────┐  │
│                                              │                 ▼              │  │
│                                              │  dim.* / fct.* / map.* / fhir.*│  │
│                                              │                                │  │
│                                              └────────────────────────────────┘  │
│                                                         │                        │
│                                                         ▼                        │
│                                                  DSO (reporting)                 │
│                                                                                  │
│  Checkpoint state:  meta.dfm_state      (one row per logical table)              │
│  Run log:           meta.dfm_run_log    (one row per asset run)                  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Run Mode

Every pipeline run is described by a single `mode` value:

| Mode | Meaning | Checkpoint advanced? | Extract is complete? |
|------|---------|----------------------|----------------------|
| `delta` | Scheduled/daily: pull only new/changed rows since checkpoint | Yes | No (partial) |
| `full_load` | Scheduled/daily: reload the complete source table | Yes | Yes |
| `period` | Ad-hoc: filter by date range | No* | No (partial) |
| `ids` | Ad-hoc: filter by specific ID list or staging table | No* | No (partial) |
| `flow_start` | Internal: used by `dfm_ifdb_flow_start` to capture the IFDB upper-bound timestamp | Yes | N/A |
| `reset` | Internal: used by `dfm_reset_job` / `usp_dfm_reset` to rewind a checkpoint | N/A | N/A |

> \* `advance_checkpoint` defaults to `None` (mode-dependent): for `delta`/`full_load` it resolves to `True` (advance); for `period`/`ids` it resolves to `False` (safe-by-default — does not advance). Set `advance_checkpoint=True` explicitly to opt-in to advancement for `period`/`ids` (e.g. the `period_orchestrator` does this for sequential chunk advancement).

> **Use `ids` and `period` carefully.** The pipeline is not designed for partial data subsets — reference data in related tables may be missing or out of date. The operator is responsible for ensuring all dependent tables are current before running downstream dim/fact/FHIR assets on a subset. Running at the **archive level** (ext + arc) is safe because the MERGE only upserts rows. Running at the **dim/fact level** on a subset may produce incorrect JOINs if the archive is incomplete.

This is the **only** mode concept. It controls:
- **What** is extracted (WHERE clause)
- **Whether** the checkpoint advances after a successful run
- **Whether** delete detection is allowed (only `full_load`)

The mode is set per-run via Dagster config. Scheduled jobs use `delta` or `full_load` (depending on the table); ad-hoc jobs set `period` or `ids`. For large-scale reprocessing, the **partitioned-period orchestrator** (section 12) splits a date range into N `period` runs and can advance the checkpoint per chunk (sequential) or after all succeed (parallel).

---

## 3. Configs

### 3.1 RunModeConfig (run-time, per launch)

Applied to all assets that participate in DFM. Note that extract assets (`ext.*`) do **not** own a checkpoint in `meta.dfm_state` — they read the checkpoint from their downstream archive table's entry (see section 4). Similarly, pre_stg/stg assets read the checkpoint from their downstream dim/fct/fhir target. Only "checkpoint owner" assets (arc, dim, fct, fhir) have their own `dfm_state` entries and advance them on success.

Default values produce delta behaviour (the most common scheduled mode).

```python
class RunModeConfig(dg.Config):
    """Run-time config controlling extraction mode and checkpoint behaviour."""

    mode: str = "delta"                     # "delta" | "full_load" | "period" | "ids" (validated)
    advance_checkpoint: bool | None = None  # None = mode-dependent (True for delta/full_load, False for period/ids)
    delta_column: str | None = None         # override: column to filter on for period/delta modes
    lookback_days: int | None = None        # override: extra lookback days for delta (late-arriving data)
    date_from: str | None = None            # ISO date, inclusive (>= date_from); required for mode="period"
    date_to: str | None = None              # ISO date, optional upper bound (< date_to + 1 day); omit for open-ended
    id_filter_table: str | None = None      # for large ID sets: "[adhoc].[filter_ids]"
    id_filter_column: str | None = None            # column name to filter on; required for mode="ids"
    id_filter_values: list[str] | None = None      # for small ID sets (up to ~100)
```

**Validation rules** (enforced at construction):
- `mode` must be one of `"delta"`, `"full_load"`, `"period"`, `"ids"` — typos raise immediately
- `mode="ids"` requires `id_filter_column` AND either `id_filter_values` or `id_filter_table`
- `mode="period"` requires `date_from`; `date_to` is optional (omit for open-ended upper bound)

**`advance_checkpoint`** controls whether checkpoint owners (arc, dim, fct, fhir, fhir_server) advance their checkpoint on successful completion:
- `None` (default): **mode-dependent** — advances for `delta`/`full_load`, does NOT advance for `period`/`ids`
- `True`: explicitly advance (opt-in for period/ids, e.g. period orchestrator last chunk)
- `False`: explicitly block advancement (dry-run / test)

The resolved value is accessed via `config.should_advance_checkpoint` (a property). This safe-by-default design prevents accidental checkpoint corruption when operators run ad-hoc period/ids runs and forget to set `advance_checkpoint: false`.

Extract assets (`ext.*`) never advance checkpoints regardless of this flag.

**`delta_column`** is an optional **run-time override** for the column used in `delta` and `period` WHERE clauses. This column may hold timestamps, datetimes, or integer audit values — hence the name `delta_column` rather than `date_column`.

In normal operation, `delta_column` is `None` and the extract config defines the column at design time:
- `TableExtractionConfig.partition_column` for Oracle IFDB extracts (e.g. `"IFDB_MODIFICATION_DATETIME"`)
- `TableConfig.partition_column` for MSSQL connector extracts

The WHERE clause for `delta` and `period` modes is constructed by the **extract layer** using the design-time column from the extract config. `RunModeConfig` only supplies the mode and filter values (`date_from`, `date_to`); the extract asset combines them with its own column definition.

Set `delta_column` only when an ad-hoc run needs to filter on a different column than the asset's default. Examples:
- `delta_column = "DW_Timestamp_Update"` (override to use a different timestamp column)
- `delta_column = "GREATEST([DW_Audit_Insert], [DW_Audit_Update], [DW_Audit])"` (audit-MAX expression)
- `delta_column = "posting_date"` (filter on a business date instead of the technical delta column)

**`lookback_days`** is an optional **run-time override** for the number of extra lookback days subtracted from the checkpoint in `delta` mode. Some source systems have late-arriving data — rows that land in the source after their delta column timestamp. A lookback window ensures these rows are re-extracted.

In normal operation, `lookback_days` is `None` and the extract config defines the default at design time:
- `TableExtractionConfig.lookback_days` (e.g. `3` — re-extract the last 3 days beyond the checkpoint)
- `TableConfig.lookback_days`

When both are `None` / `0`, no lookback is applied. When `RunModeConfig.lookback_days` is set, it overrides the design-time default. See section 4.1 for how the WHERE clause incorporates lookback.

**`id_filter_column`** is the actual column name in the table being queried — it is NOT assumed to be a generic `'id'`. Examples:
- `id_filter_column = "DW_EK_SpendAnalysisLine"` (archive table column)
- `id_filter_column = "episode_of_care_id"` (archive table column for FHIR)
- `id_filter_column = "patient_id"` (dim/fact source column)

**Resolution for `mode="ids"`:**
1. If `id_filter_table` is set: `WHERE [{id_filter_column}] IN (SELECT [{id_filter_column}] FROM {id_filter_table})`
2. Elif `id_filter_values` is set: `WHERE [{id_filter_column}] IN ('val1', 'val2', ...)` with proper escaping
3. Both set: `id_filter_table` takes precedence (log a warning)

**Guideline**: use `id_filter_values` for up to ~100 IDs. Use `id_filter_table` for larger sets or when the IDs come from a query.

### 3.2 ArchiveMergeConfig (design-time, per table)

Declarative config that drives the MERGE SQL generator:

```python
@dataclass(frozen=True)
class ArchiveMergeConfig:
    source_schema: str                      # "ext"
    source_table: str                       # "spend_analysis_lines_kmd_uddata"
    target_schema: str                      # "arc"
    target_table: str                       # "spend_analysis_lines_kmd_uddata"
    merge_keys: tuple[str, ...]             # single or composite key(s)
    non_key_columns: tuple[str, ...]        # columns for UPDATE SET + INSERT
    checkpoint_query: str | None = None     # SQL evaluated AFTER the MERGE to compute the new checkpoint
    flow_start_key: tuple[str, str] | None = None  # read new checkpoint from another dfm_state entry
    strategy: str = "upsert"                # "upsert" | "insert_only"
    update_only_on_change: bool = False     # skip UPDATE when all non-key columns are unchanged
    detect_deletes: bool = False            # only effective when extract is complete (see section 5.2)
    soft_delete_column: str = "DW_IsDeleted"
    source_delete_column: str | None = None  # source column indicating row is deleted (see below)
    upstream_is_full_load: bool = False     # True if the upstream extract always pulls all rows
```

**`checkpoint_query`** is the SQL expression evaluated immediately after the MERGE to determine the new checkpoint value. The result is passed to `set_checkpoint()`. Example: `"SELECT CAST(MAX([DW_Timestamp_Update]) AS NVARCHAR(50)) FROM [arc].[spend_analysis_lines]"`. When `None`, no checkpoint is set (e.g. for insert-only tables where checkpoint tracking is handled differently).

**`flow_start_key`** is an alternative to `checkpoint_query` for determining the new checkpoint value. Instead of computing it via SQL, the archive reads the checkpoint from another `dfm_state` entry. This is used for the IFDB pattern where the flow-start timestamp (from `dfm.ifdb_flow_start`) becomes the archive checkpoint. Resolution order: `flow_start_key` > `checkpoint_query` > `SYSDATETIME()` fallback. Currently used only for IFDB archives; may be deprecated if all future sources use `checkpoint_query`.

**`update_only_on_change`** adds a NULL-safe column comparison condition to `WHEN MATCHED`, so rows are only updated when at least one non-key column has actually changed. Use for full-load tables where the source frequently re-sends unchanged rows — avoids unnecessary UPDATE writes, transaction log churn, and `DW_Timestamp_Update` noise. See `archive_merge_options.md` for details.

**`detect_deletes`** enables the `WHEN NOT MATCHED BY SOURCE` clause in the generated MERGE. A safety gate in the archive asset automatically disables it unless the extract is complete, because partial extracts (delta, period, ids) would falsely soft-delete rows that simply weren't in the filter. For tables where even a "full load" does not cover the complete set (e.g. periodic snapshots), set `detect_deletes=False`.

**`upstream_is_full_load`** indicates that the upstream extract always pulls the complete source table, regardless of `RunModeConfig.mode`. When `True`, delete detection remains enabled even if the orchestrator passes `mode="period"` or `mode="delta"` — because the extract is complete regardless. Without this flag, a period orchestrator run would suppress delete detection even though the extract is complete. See `archive_merge_options.md` for the full safety matrix.

**`source_delete_column`** enables row-level soft-delete detection from a source column (as opposed to `detect_deletes`, which works via `NOT MATCHED BY SOURCE`). When set, the MERGE reads the named column from the source (`ext`) row and evaluates `CASE WHEN src.[col] IS NOT NULL AND src.[col] NOT IN ('0', '0.0') THEN 1 ELSE 0 END` to set the `soft_delete_column` on both UPDATE and INSERT. This is used when the source system provides an explicit deletion flag rather than simply removing the row. The two delete mechanisms are independent: `source_delete_column` works per-row during upsert, while `detect_deletes` works per-row via `NOT MATCHED BY SOURCE` on full loads.

A pure function `build_archive_merge_sql(config)` generates the T-SQL MERGE from this config. No hand-written SQL per asset needed.
`build_archive_merge_sql(config)` aims to generate SQL that is as readable and debuggable as hand-written SQL. The generated MERGE should be easy to copy-paste and run manually for investigation.

---

## 4. Extract Layer

The WHERE clause for delta and period filtering is constructed by the **extract config** — not by `RunModeConfig`. Each extract config defines the delta column at design time:

- **MSSQL connector**: `TableConfig.partition_column` (e.g. `"DW_Timestamp_Update"`, or an audit-MAX expression)
- **Oracle IFDB**: `TableExtractionConfig.partition_column` (e.g. `"IFDB_MODIFICATION_DATETIME"`, or `None` for full-load tables)

`RunModeConfig` supplies only the **mode** and **filter values** (`date_from`, `date_to`, `id_filter_values`). The extract asset combines them with its own design-time column. The optional `delta_column` override on `RunModeConfig` is used only when an ad-hoc run needs a different column.

### 4.1 Delta Extract (mode="delta")

The extract asset reads the checkpoint from its downstream archive table's `dfm_state` entry (the extract does **not** have its own checkpoint) and builds a filter using its design-time delta column:

```python
dfm = DfmManager(mssql_target_staging, context)

# start_dfm_run logs this extract run and reads the checkpoint from the arc table
run_ctx = dfm.start_dfm_run(
    "ext", table_name,
    checkpoint_from=("arc", table_name),
    dagster_run_id=context.run_id,
    mode=run_config.mode,
)

col = run_config.delta_column or extract_config.partition_column
lookback = run_config.lookback_days or extract_config.lookback_days or 0

if run_ctx.checkpoint:
    if lookback > 0:
        where = f"{col} > DATEADD(day, -{lookback}, '{run_ctx.checkpoint}')"
    else:
        where = f"{col} > '{run_ctx.checkpoint}'"
else:
    where = None  # first run: pull all
```

The extract itself always does `DROP + CREATE` on `ext.*` — the table is volatile. The checkpoint filters what is pulled from **source**, not what is stored locally.

**Lookback days** protect against late-arriving data in the source system. For example, with `lookback_days=3` and a checkpoint of `2026-05-01`, the extract pulls rows where `delta_column > 2026-04-28` — re-extracting 3 extra days to catch any rows that arrived late. The archive MERGE (upsert) handles the overlap safely since matched rows are simply updated.

**Upper-bound column override** (`DfmExtractConfig.upper_bound_column`): when the flow-start ceiling should filter on a different column than the checkpoint lower bound, set `upper_bound_column` on the extract config. This produces a split-column WHERE clause:

```sql
WHERE IFDB_MODIFICATION_DATETIME > TO_DATE(checkpoint)
  AND EPJI_MODIFICATION_DATETIME <= TO_DATE(flow_start)
```

This is needed for IFDB tables where the checkpoint lower bound tracks `IFDB_MODIFICATION_DATETIME` (when IFDB processed the row) but the flow-start ceiling is based on `EPJI_MODIFICATION_DATETIME` (when the source EPJ-I change occurred). Without this, the flow-start ceiling derived from `JOB_LOG.SLUTTID` would not correctly bound the delta window because it corresponds to `EPJI_MODIFICATION_DATETIME`, not `IFDB_MODIFICATION_DATETIME`.

### 4.2 Full-Load Extract (mode="full_load")

No checkpoint filter at extract time — pull everything. Tables that are always full-load have `partition_column = None` in their extract config:

```python
# TableExtractionConfig with partition_column=None → always full-load
LOCATION_ARCHIVE_CONFIG = TableExtractionConfig(
    table="IFDB.LOCATION",
    columns=[...],
    partition_column=None,
    description="Location master data for archive (full-load).",
)
```

### 4.3 Ad-Hoc Override (mode="period" or mode="ids")

When the mode is `period` or `ids`, the extract asset replaces the normal WHERE clause. The column for `period` comes from the extract config (with optional `delta_column` override):

```python
col = run_config.delta_column or extract_config.partition_column

if run_config.mode == "period":
    where = f"{col} >= '{run_config.date_from}' AND {col} < DATEADD(day, 1, CAST('{run_config.date_to}' AS DATE))"
elif run_config.mode == "ids":
    if run_config.id_filter_table:
        where = (
            f"[{run_config.id_filter_column}] IN "
            f"(SELECT [{run_config.id_filter_column}] FROM {run_config.id_filter_table})"
        )
    else:
        escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in run_config.id_filter_values)
        where = f"[{run_config.id_filter_column}] IN ({escaped})"
```

### 4.4 Source-Side JOINs

For extracts that need data from related tables (e.g. delete flags, lookup values), `DfmExtractConfig` supports `select_from_expr` — a single field that replaces the entire `SELECT ... FROM ...` portion of the query:

```python
DfmExtractConfig(
    ...,
    select_from_expr=(
        "SELECT t.*, CASE WHEN d.order_id IS NOT NULL THEN 1 ELSE 0 END AS is_deleted "
        "FROM [dbo].[orders] t "
        "LEFT JOIN [dbo].[deletion_log] d ON d.order_id = t.order_id"
    ),
)
```

**`select_from_expr`** replaces the auto-generated `SELECT {columns or *} FROM [source_schema].[source_table]` entirely. When set, the extractor uses it verbatim and appends the WHERE clause from checkpoint/RunModeConfig. When `None` (default), the extractor builds the SELECT+FROM from `source_schema`, `source_table`, and `columns`.

This single field replaces the need for separate `additional_joins` and `select_columns` fields — when you need JOINs, you almost always need to change the SELECT clause too (aliases, computed columns), so they are better expressed together as one self-contained SQL fragment.

---

## 5. Archive Layer

> **Design principle**: `build_archive_merge_sql()` aims to generate SQL that is as readable and debuggable as hand-written SQL. The generated MERGE should be easy to copy-paste into SSMS and run manually for investigation. The framework exists to eliminate boilerplate, not to obscure what the SQL does.

### 5.1 MERGE Strategies

The `build_archive_merge_sql()` function generates SQL based on `ArchiveMergeConfig`:

**Upsert (strategy="upsert"):**
```sql
MERGE [arc].[table] AS tgt
USING [ext].[table] AS src
    ON tgt.[key1] = src.[key1] AND tgt.[key2] = src.[key2]
WHEN MATCHED THEN UPDATE SET
    tgt.[col1] = src.[col1], ...,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (...)
    VALUES (..., SYSDATETIME(), SYSDATETIME())
-- Optional: WHEN NOT MATCHED BY SOURCE (see 5.2)
;
```

**Insert-only (strategy="insert_only"):**
```sql
INSERT INTO [arc].[table] (..., DW_Timestamp_Insert, DW_Timestamp_Update)
SELECT ..., CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM [ext].[table]
```

### 5.2 Delete Detection (Soft-Delete)

`WHEN NOT MATCHED BY SOURCE` is only safe when `ext` contains the **complete** set. Any partial extract (delta, period, IDs) would cause false soft-deletes of rows that simply weren't included in the filter.

**Safety gate** — the archive asset enforces:

```python
if (
    merge_config.detect_deletes
    and run_config.mode != "full_load"
    and not merge_config.upstream_is_full_load
):
    context.log.warning(
        f"Delete detection disabled — mode={run_config.mode!r} and "
        f"upstream_is_full_load=False; NOT MATCHED BY SOURCE would incorrectly flag rows."
    )
    effective_config = dataclasses.replace(merge_config, detect_deletes=False)
else:
    effective_config = merge_config
```

When delete detection fires, the generated SQL includes:

```sql
WHEN NOT MATCHED BY SOURCE
THEN UPDATE SET
    tgt.[DW_IsDeleted] = 1,
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
```

**Safety matrix:**

| mode | detect_deletes fires? | Why |
|------|-----------------------|-----|
| `full_load` | Yes | ext contains the complete source set |
| `delta` | No (safety gate) | ext only has new/changed rows |
| `period` | No (safety gate) | ext only has rows in the date range |
| `ids` | No (safety gate) | ext only has the requested IDs |

### 5.3 Composite Merge Keys

`ArchiveMergeConfig.merge_keys` supports tuples of any length:

```python
ArchiveMergeConfig(
    merge_keys=("patient_id", "encounter_id"),  # composite
    ...
)
```

Generates: `ON tgt.[patient_id] = src.[patient_id] AND tgt.[encounter_id] = src.[encounter_id]`

### 5.4 Retired Columns

When a source drops a column but the archive retains it for historical data, simply omit the column from `non_key_columns`. The DDL keeps the column (NULLable); new rows get `NULL` for that column. No special framework feature needed.

---

## 6. Staging / Dim / Fact / Map Layer

The downstream pipeline from archive to reporting follows a consistent flow:

```
arc.* ──► pre_stg / stg ──► dim.* / fct.* / map.*
```

Pre-staging and staging assets read from `arc.*` tables using the checkpoint of their downstream dim/fct target. Then dim/fct/map assets are updated from their upstream `stg.*` table. Only dim and fct assets own a checkpoint — map tables do not (see section 6.3).

### 6.1 Staging Checkpoint Flow

Staging assets (pre_stg, stg) do **not** own a checkpoint. They read the checkpoint from their downstream dim/fct target to filter the archive:

```python
dfm = DfmManager(mssql_target_staging, context)
run_ctx = dfm.start_dfm_run(
    "stg", "kreditor",
    checkpoint_from=("dim", "Kreditor"),
    dagster_run_id=context.run_id,
    mode=config.mode,
)

# run_ctx.checkpoint comes from dim.Kreditor's dfm_state entry
# Filter archive for changed rows since last dim build
rows = query_archive(
    f"SELECT ... FROM [arc].[kreditor_source] WHERE DW_Timestamp_Update > '{run_ctx.checkpoint}'"
)
# ... write to stg.kreditor ...
dfm.complete_dfm_run(run_ctx.log_id, rows_affected=len(rows))
```

### 6.2 Dim/Fact Checkpoint Flow

Each dim or fact asset is a **checkpoint owner** — it has its own entry in `meta.dfm_state` and advances it on success. Dim/fct assets read from their upstream `stg.*` table (not directly from `arc.*`):

```python
dfm = DfmManager(mssql_target_staging, context)
run_ctx = dfm.start_dfm_run("dim", "Kreditor", dagster_run_id=context.run_id, mode=config.mode)
# run_ctx.checkpoint contains the current checkpoint value (or None on first run)
# run_ctx.log_id is the dfm_run_log entry ID

# Dim/fct reads from the upstream stg table (already filtered by the stg asset)
# ... MERGE stg.kreditor into dim.Kreditor ...

dfm.set_checkpoint("dim", "Kreditor", value=new_checkpoint)
dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=n)
```

### 6.3 Map Tables

Map tables are **not** checkpoint owners. A map table is coupled to a dim or fct table — the dim/fct checkpoint controls what is extracted to the stg table, and the stg table updates **both** the map and the dim/fct. Because the map processes the entire stg table (already filtered by the dim/fct checkpoint), it has no checkpoint of its own.

```python
dfm = DfmManager(mssql_target_staging, context)
run_ctx = dfm.start_dfm_run("map", "kreditor_mapping", dagster_run_id=context.run_id, mode=config.mode)
# run_ctx.checkpoint is None — map has no checkpoint entry in dfm_state

# ... update map.kreditor_mapping from stg.kreditor ...
dfm.complete_dfm_run(run_ctx.log_id, rows_affected=n)
```

### 6.4 Ad-Hoc Override at Stg/Dim/Fct

All assets in this layer accept `RunModeConfig`. In ad-hoc modes, the staging asset filters the **archive table** (not the source system) using the override:

- `mode="period"`: `WHERE arc.[DW_Timestamp_Update] > '{date_from}' AND arc.[DW_Timestamp_Update] < DATEADD(day, 1, '{date_to}')`
- `mode="ids"`: `WHERE arc.[{id_filter_column}] IN (...)` — where `id_filter_column` is the actual column name in the archive table (e.g. `episode_of_care_id`, `DW_EK_Kreditor`)

Checkpoint is **not** advanced for `period` or `ids` runs. A single asset handles both daily scheduled runs and ad-hoc reprocessing — no separate "adhoc" assets needed.

**Multi-asset ad-hoc materialization**: you can select multiple assets (e.g. `stg_kreditor`, `map_kreditor_mapping`, `fct_kreditor`) and launch them together with a single `RunModeConfig`. Dagster passes the same config to each asset in the selection. This is the recommended way to reprocess a subset of IDs through the full stg → map + dim/fct pipeline:

1. Select `stg_kreditor`, `map_kreditor_mapping`, `dim_kreditor` in the Dagster UI
2. Set config: `mode = "ids"`, `id_filter_column = "DW_EK_Kreditor"`, `id_filter_values = ["123", "456"]`
3. Launch — all 3 assets run with the same filter
4. Checkpoint is **not** advanced (ad-hoc mode)

### 6.5 Logging

Every asset run logs `mode` to `meta.dfm_run_log`, providing full traceability for which runs were scheduled vs ad-hoc.

### 6.6 Checkpoint Value Strategy: SYSDATETIME() vs. MAX(source_column)

There are two valid strategies for computing the new checkpoint value after a successful run. The correct choice depends on the asset's relationship to its source data and its downstream consumers.

**Strategy A — `SYSDATETIME()` (wall-clock time):**

Used by dim/fct assets where the checkpoint is read by **other assets** (e.g. `pre_stg` reads the dim/fct checkpoint to filter the archive). The checkpoint means "this asset was last successfully built at this time" rather than "I've consumed source rows up to this value."

Why SYSDATETIME() is correct here — **the replay scenario**:

1. Archive has rows with `DW_Timestamp_Update = time1`
2. `pre_stg` reads archive with `WHERE DW_Timestamp_Update > dim_checkpoint`, dim/fct processes, checkpoint → `SYSDATETIME() = time1_run`
3. Operator discovers a logic bug in `pre_stg` (archive data is fine), fixes the logic, resets dim/fct checkpoint, re-runs `pre_stg` → dim/fct
4. dim/fct MERGE updates `DW_Timestamp_Update = SYSDATETIME() = time2` on touched rows, checkpoint → `SYSDATETIME() = time2`
5. Downstream consumers (DSO sync, etc.) see the re-processed rows because `DW_Timestamp_Update = time2` > their checkpoint

If `MAX(DW_Timestamp_Update)` from the archive were used instead, the checkpoint would return to `time1` (archive unchanged) — making the logic-fix replay **invisible** to downstream delta consumers. This is the critical failure mode that `SYSDATETIME()` prevents.

**Strategy B — `MAX(source_column)` (source data watermark):**

Used by assets that **directly query their own source** using their own checkpoint (e.g. `ref_patient_borger` reads `fhir.Patient WHERE last_updated > checkpoint`). The checkpoint means "I've consumed all source rows up to and including this `last_updated` value."

Why MAX(source_column) is correct here:

1. `fhir.Patient` has rows with `last_updated = time1`
2. `ref_patient_borger` processes, checkpoint → `MAX(last_updated) = time1`
3. Operator discovers a logic bug in `ref_patient_borger`, fixes logic, resets checkpoint, re-runs
4. MERGE re-processes all rows → `ref.Patient_Borger.DW_Timestamp_Update = SYSDATETIME() = time2`
5. Downstream `dim_borger` sees changes via `ref.Patient_Borger.DW_Timestamp_Update = time2 > dim_checkpoint` ✓
6. `ref_patient_borger` checkpoint → `time1` (correct — source data hasn't changed, no need to re-consume)

The replay is visible downstream because the MERGE always sets `DW_Timestamp_Update = SYSDATETIME()` on affected rows, regardless of what the checkpoint value is. The checkpoint only controls the *next run's* source filter — and returning to the same source watermark is correct because the source data hasn't changed.

**Decision rule:**

| Situation | Strategy | Rationale |
|-----------|----------|-----------|
| Asset's checkpoint is read by **other assets** to filter a shared source (e.g. dim/fct → pre_stg reads from archive) | `SYSDATETIME()` | Replays must be visible to downstream checkpoint consumers |
| Asset **directly queries its own source** by its checkpoint (e.g. ref reads from fhir.*) | `MAX(source_column)` | Checkpoint semantically tracks source consumption; replays are visible via `DW_Timestamp_Update` |
| Archive layer (with `flow_start_key`) | `flow_start` from upstream | Ensures gap-free coverage of the source system's delta window |

**Delta mode comparison: always use `>` (strictly greater than), never `>=`.**

The checkpoint value represents the last successfully processed watermark. Using `>=` would re-process rows whose delta column exactly equals the checkpoint on every subsequent run. This is especially harmful for `MAX(source_column)` checkpoints — e.g. if `ref_patient_borger` sets checkpoint to `MAX(last_updated) = T`, then `WHERE last_updated >= T` re-processes the boundary rows every run, causing unnecessary MERGE activity and potentially bumping `DW_Timestamp_Update` on unchanged rows.

All extract builders (`build_mssql_where`, `build_oracle_where`, `build_fhir_archive_query`) consistently use `>` for delta mode. Manual WHERE clauses in staging, ref, and dim assets must follow the same convention.

**Period mode comparison: use `>=` (inclusive start) for `date_from`.**

Unlike delta mode, period mode uses operator-supplied date bounds — not checkpoint values. The `date_from` parameter (required) represents the first day the operator wants included, so `>=` is the correct inclusive-start semantic. Using `>` would skip the entire first day, which is unintuitive when an operator specifies `date_from: "2025-01-01"` and expects January 1st to be included. The `date_to` parameter is **optional**: when set, the upper bound uses `< date_to + 1 day` to include the full last day; when omitted, no upper boundary is applied (open-ended — useful for "everything from this date forward"). Together, when both are set, this gives `col >= date_from AND col < date_to + 1 day` — covering the full inclusive date range.

Summary of comparison operators by mode:

| Mode | Lower bound | Upper bound | Rationale |
|------|-------------|-------------|-----------|
| `delta` | `col > checkpoint` (exclusive) | `col <= upper_bound` (inclusive, when set) | Checkpoint = last processed value; `>` avoids reprocessing boundary rows |
| `period` | `col >= date_from` (inclusive) | `col < date_to + 1 day` (exclusive, when set) | Operator-supplied bounds; `>=` includes the start day; `date_to` optional for open-ended |
| `ids` | N/A (uses IN filter) | N/A | Row-level filter, no date range |
| `full_load` | N/A (no WHERE) | N/A | Extracts everything |

---

## 7. FHIR Layer

FHIR resource assets follow the same pattern as dim/fact but produce FHIR JSON bundles instead of relational tables.

### 7.1 Schema Convention

FHIR has two distinct target types, distinguished by `schema_name`:

| `schema_name` | `target_type` | Description |
|---|---|---|
| `fhir` | `fhir_table` | SQL-on-FHIR flattened tables in MSSQL |
| `fhir_server` | `fhir_resource` | FHIR resources pushed to the FHIR server (Aidbox) |

This allows both systems to be tracked in the same `meta.dfm_state` table without collision:

```python
# SQL-on-FHIR table checkpoint
dfm.get_checkpoint("fhir", "Encounter")

# FHIR server resource checkpoint
dfm.get_checkpoint("fhir_server", "Patient")
```

### 7.2 Single-Asset Batched Pattern (Primary)

FHIR pipelines use a **single-asset batched** approach: one asset reads from the archive, transforms to FHIR, and posts — all in batched streaming to keep memory bounded:

```
arc.* ──► fhir_server_<resource>_dfm (fetch batches → transform → post)
```

The asset is the **checkpoint owner** for `fhir_server.<Resource>`:

```python
dfm = DfmManager(mssql_target_staging, context)
run_ctx = dfm.start_dfm_run(
    "fhir_server", "Patient",
    dagster_run_id=context.run_id,
    mode=config.mode,
)

query = build_fhir_archive_query("arc.patient_ifdb", run_ctx.checkpoint, config)

with mssql.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]

    while True:
        raw_rows = cursor.fetchmany(BATCH_SIZE)
        if not raw_rows:
            break
        batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
        resources = transform_to_fhir(batch)
        post_to_fhir_server(resources)

if config.should_advance_checkpoint and total_fetched > 0:
    new_checkpoint = get_sysdatetime(mssql)
    dfm.set_checkpoint("fhir_server", "Patient", value=new_checkpoint, mode=config.mode)

dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=total_posted)
```

Key advantages of the single-asset pattern:
- **Memory-bounded**: server-side cursor with `fetchmany(BATCH_SIZE)` — never loads the full archive into memory
- **Simpler graph**: one asset instead of two, eliminating the need for in-memory data passing between assets
- **Single checkpoint owner**: no cross-asset checkpoint coordination needed

SQL-on-FHIR (`fhir` schema) assets follow the same single-asset pattern but write flattened tables to MSSQL instead of uploading to the FHIR server.

> **Historical note**: The design originally described a two-asset pattern (fhir_stg → fhir_server), analogous to stg → dim. The implementation adopted the single-asset batched pattern instead, as it proved simpler and more memory-efficient for FHIR workloads.

### 7.3 Ad-Hoc Override for FHIR

Same `RunModeConfig` pattern. When mode is `period` or `ids`, the FHIR staging asset filters the archive table using the override instead of the checkpoint:

- `mode="period"`: reprocess resources modified in a date range
- `mode="ids"`: reprocess specific IDs — e.g. `id_filter_column = "episode_of_care_id"` on the archive table

Checkpoint is NOT set — the next scheduled run picks up from where it left off.

### 7.4 Transition to Archive-Based FHIR

As archive tables are built out, FHIR assets that currently read from `ext.*` or directly from source can transition to reading from `arc.*`. The checkpoint on the FHIR layer then filters on `arc.DW_Timestamp_Update` rather than re-querying the source system.

---

## 8. DSO Sync Layer

> **Status: NOT YET IMPLEMENTED.** The design below describes the target architecture for DSO sync unification with DFM. Currently, DSO sync still uses the legacy `SyncStateManager` class with `meta.sync_watermarks` on the DSO database. This section documents the intended migration path.

The DSO (Data Staging Out) sync transfers data from staging to the reporting database via linked-server delta MERGE. Its state tracking is unified into the DFM system using the same physical `meta.dfm_state` and `meta.dfm_run_log` tables on the staging database.

### 8.1 Unification with DFM

Previously, DSO sync used a separate `SyncStateManager` class with a `meta.sync_watermarks` table on the DSO database. Under DFM, everything lives in one place:

| Aspect | Previous | DFM |
|---|---|---|
| State table | `meta.sync_watermarks` (DSO DB) | `meta.dfm_state` (staging DB) |
| Log table | None (no run log) | `meta.dfm_run_log` (staging DB) |
| Manager class | `SyncStateManager` | `DfmManager` |
| Checkpoint read | `get_last_sync_timestamp()` | `get_checkpoint()` or `start_dfm_run()` |
| Checkpoint write | `update_sync_state()` | `set_checkpoint()` + `complete_dfm_run()` |

### 8.2 Scope Column for DSO

Staging-side and DSO-side entries for the same table are distinguished by the `scope` column on `meta.dfm_state` (see section 9.2). This avoids artificial schema name prefixes and keeps the `schema_name` column truthful:

```python
dfm = DfmManager(mssql_target_staging, context)

# Staging-side dim checkpoint: scope="staging" (default)
dfm.get_checkpoint("dim", "Kreditor")

# DSO sync checkpoint: scope="dso"
run_ctx = dfm.start_dfm_run(
    "dim", "Kreditor",
    dagster_run_id=context.run_id,
    mode="delta",
    scope="dso",
)
```

This keeps all state in a single `meta.dfm_state` table with no ambiguity:

| `scope` | `schema_name` | `target_type` | `table_name` | Meaning |
|---|---|---|---|---|
| `staging` | `dim` | `dimension` | `Kreditor` | Last staging-side dim build |
| `dso` | `dim` | `dimension` | `Kreditor` | Last DSO sync of dim.Kreditor |
| `staging` | `fct` | `fact` | `Postering` | Last staging-side fact build |
| `dso` | `fct` | `fact` | `Postering` | Last DSO sync of fct.Postering |
| `staging` | `fhir` | `fhir_table` | `Encounter` | Last SQL-on-FHIR table build |
| `dso` | `fhir` | `fhir_table` | `Encounter` | Last DSO sync of fhir.Encounter |
| `staging` | `fhir_server` | `fhir_resource` | `Patient` | Last FHIR server upload |

### 8.3 Benefits of Unification

- **Single source of truth**: all checkpoint state in one table on one database — queryable with a single `SELECT`
- **Run logging**: DSO sync gains full run lifecycle logging (start/success/fail) with `meta.dfm_run_log`
- **Error tracking**: Failed syncs record `error_message` for diagnostics
- **Consistent API**: All layers use the same `DfmManager` class
- **Single migration path**: New DFM features (e.g. partitioned-period orchestrator) automatically available for DSO sync

---

## 9. DFM State and Run Log Tables

### 9.1 Schema-to-target_type Mapping

The `target_type` column is derived from `schema_name` internally. Callers only specify `schema_name` and `table_name`.

```python
SCHEMA_TO_TARGET_TYPE: dict[str, str] = {
    "ext": "extract",
    "arc": "archive",
    "pre_stg": "pre_staging",
    "stg": "staging",
    "dim": "dimension",
    "fct": "fact",
    "map": "mapping",
    "bridge": "bridge",
    "fhir_stg": "fhir_staging",
    "fhir": "fhir_table",
    "fhir_server": "fhir_resource",
    "codesystem": "codesystem",
    "conceptmap": "conceptmap",
    "ref": "reference",
    "dfm": "dfm",
}

def _target_type_for_schema(schema: str) -> str:
    if schema not in SCHEMA_TO_TARGET_TYPE:
        raise ValueError(f"Unknown schema {schema!r} — add it to SCHEMA_TO_TARGET_TYPE")
    return SCHEMA_TO_TARGET_TYPE[schema]
```

The DB tables retain `target_type` for readability in ad-hoc SQL queries, but it is always derived — never a separate input. DSO sync entries use the same schema names as staging (e.g. `dim`, `fct`) and are distinguished by the `scope` column (see section 8.2).

### 9.2 meta.dfm_state

One row per logical table per scope. Tracks the current checkpoint.

| Column | Type | Purpose |
|--------|------|---------|
| `scope` | NVARCHAR(50) | `'staging'` (default) or `'dso'` — distinguishes staging-side from DSO sync entries |
| `target_type` | NVARCHAR(50) | Derived from schema_name (archive, dimension, fact, fhir_table, ...) |
| `schema_name` | NVARCHAR(128) | Target schema (arc, dim, fct, fhir, fhir_server, ...) |
| `table_name` | NVARCHAR(128) | Target table/resource |
| `checkpoint_value` | NVARCHAR(50) | Current checkpoint (int or ISO datetime as string) |
| `last_mode` | NVARCHAR(50) | Mode of the last successful run: "delta", "full_load", "flow_start", or "reset" (manual checkpoint override, see section 10) |
| `updated_at` | DATETIME2 | When checkpoint was last advanced |

PK: `(scope, target_type, schema_name, table_name)`

### 9.3 meta.dfm_run_log

One row per run. Full audit trail.

| Column | Type | Purpose |
|--------|------|---------|
| `id` | BIGINT IDENTITY | Auto-increment |
| `scope` | NVARCHAR(50) | `'staging'` or `'dso'` |
| `target_type` | NVARCHAR(50) | Derived from schema_name |
| `schema_name` | NVARCHAR(128) | Target schema |
| `table_name` | NVARCHAR(128) | Target table/resource |
| `mode` | NVARCHAR(50) | "delta", "full_load", "period", "ids", "flow_start", or "reset" |
| `status` | NVARCHAR(50) | "running" → "success" or "failed"; "abandoned" when a row was stuck in "running" past the staleness threshold (process killed before complete/fail — set by `DfmManager.sweep_stale_runs()`, which runs at the start of every DFM run) |
| `checkpoint_before` | NVARCHAR(50) | Checkpoint at run start |
| `checkpoint_after` | NVARCHAR(50) | Checkpoint at run end (NULL for ad-hoc) |
| `rows_affected` | INT | Rows processed |
| `error_message` | NVARCHAR(MAX) | Error details on failure, or reason on reset (NULL on success) |
| `dagster_run_id` | NVARCHAR(128) | Traceability link |
| `run_started_at` | DATETIME2 | Start timestamp |
| `run_completed_at` | DATETIME2 | End timestamp |

### 9.4 Checkpoint Advancement Rules

Checkpoint advancement is controlled by `RunModeConfig.should_advance_checkpoint` (resolved from `advance_checkpoint` + `mode`):

| `advance_checkpoint` | mode | Resolved | Advances checkpoint? |
|------|------|----------|----------------------|
| `None` (default) | `delta` | `True` | Yes — to explicit value (e.g. `MAX(audit)`) |
| `None` (default) | `full_load` | `True` | Yes — to `SYSDATETIME()` or explicit value |
| `None` (default) | `period` | `False` | **No** — safe-by-default for ad-hoc runs |
| `None` (default) | `ids` | `False` | **No** — safe-by-default for ad-hoc runs |
| `True` | `period` | `True` | Yes (explicit opt-in, e.g. period orchestrator) |
| `True` | `ids` | `True` | Yes (explicit opt-in) |
| `False` | any | `False` | No (explicit block) |

Scheduled jobs leave `advance_checkpoint` at the default (`None`) — delta/full_load runs advance automatically, while ad-hoc period/ids runs are safe by default. The period orchestrator explicitly sets `advance_checkpoint=True` on child runs when per-chunk advancement is desired.

---

## 10. Manual DFM Reset

In addition to ad-hoc runs, the team can **manually reset the checkpoint** to change the starting point for the next scheduled run. This is a supported operational pattern.

### 10.1 Use Cases

| Need | Action |
|------|--------|
| Full reload on next scheduled run | Set checkpoint to `0` (audit-based) or `0001-01-01` (timestamp-based) |
| Reprocess last year of data on next scheduled run | Set checkpoint to `2025-01-01` |
| Recover from a stuck checkpoint | Set to a known-good value |

### 10.2 Stored Procedure: `meta.usp_dfm_reset`

The reset logic lives in a SQL stored procedure on the target database. This ensures atomicity (state update + log entry in one transaction) and makes it callable from SSMS, scripts, or Dagster assets.

```sql
CREATE OR ALTER PROCEDURE [meta].[usp_dfm_reset]
    @schema_name    NVARCHAR(128),
    @table_name     NVARCHAR(128),
    @new_value      NVARCHAR(50),
    @reason         NVARCHAR(MAX) = NULL,
    @scope          NVARCHAR(50)  = 'staging'
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRANSACTION;

    DECLARE @target_type NVARCHAR(50) = CASE @schema_name
        WHEN 'ext'         THEN 'extract'
        WHEN 'arc'         THEN 'archive'
        WHEN 'pre_stg'     THEN 'pre_staging'
        WHEN 'stg'         THEN 'staging'
        WHEN 'dim'         THEN 'dimension'
        WHEN 'fct'         THEN 'fact'
        WHEN 'map'         THEN 'mapping'
        WHEN 'bridge'      THEN 'bridge'
        WHEN 'fhir_stg'    THEN 'fhir_staging'
        WHEN 'fhir'        THEN 'fhir_table'
        WHEN 'fhir_server' THEN 'fhir_resource'
        WHEN 'codesystem'  THEN 'codesystem'
        WHEN 'conceptmap'  THEN 'conceptmap'
        WHEN 'ref'         THEN 'reference'
        ELSE @schema_name
    END;

    DECLARE @old_value NVARCHAR(50);

    SELECT @old_value = [checkpoint_value]
    FROM [meta].[dfm_state]
    WHERE [scope]       = @scope
      AND [target_type] = @target_type
      AND [schema_name] = @schema_name
      AND [table_name]  = @table_name;

    -- Upsert the state table
    MERGE [meta].[dfm_state] AS tgt
    USING (SELECT @scope, @target_type, @schema_name, @table_name)
        AS src (scope, target_type, schema_name, table_name)
    ON  tgt.[scope]       = src.[scope]
    AND tgt.[target_type] = src.[target_type]
    AND tgt.[schema_name] = src.[schema_name]
    AND tgt.[table_name]  = src.[table_name]
    WHEN MATCHED THEN
        UPDATE SET
            [checkpoint_value] = @new_value,
            [last_mode]        = 'reset',
            [updated_at]       = CAST(SYSDATETIME() AS datetime2(0))
    WHEN NOT MATCHED THEN
        INSERT ([scope], [target_type], [schema_name], [table_name], [checkpoint_value], [last_mode])
        VALUES (@scope, @target_type, @schema_name, @table_name, @new_value, 'reset');

    -- Log the reset
    INSERT INTO [meta].[dfm_run_log]
        ([scope], [target_type], [schema_name], [table_name],
         [mode], [status],
         [checkpoint_before], [checkpoint_after],
         [error_message],
         [run_started_at], [run_completed_at])
    VALUES
        (@scope, @target_type, @schema_name, @table_name,
         'reset', 'success',
         @old_value, @new_value,
         @reason,
         CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)));

    COMMIT;
END
```

**Usage from SSMS or any SQL client:**

```sql
-- Reset a staging-side checkpoint (scope defaults to 'staging')
EXEC [meta].[usp_dfm_reset]
    @schema_name = 'arc',
    @table_name  = 'spend_analysis_lines_kmd_uddata',
    @new_value   = '0',
    @reason      = 'Trigger full reload after source schema change';

-- Reset a DSO sync checkpoint
EXEC [meta].[usp_dfm_reset]
    @schema_name = 'dim',
    @table_name  = 'Kreditor',
    @new_value   = '0',
    @reason      = 'Re-sync dim.Kreditor to DSO',
    @scope       = 'dso';
```

### 10.3 Infrastructure Asset for the Stored Procedure

The stored procedure DDL is deployed via an infrastructure asset, following the standard centralized infrastructure pattern:

- **DDL file**: `infrastructure/ddl/tables/meta/04_usp_dfm_reset.sql`
- **Deployed by**: the existing `staging_infra_dfm` asset that runs `execute_sql_files_from_directory` on `tables/meta/`
- Uses `CREATE OR ALTER PROCEDURE` so it is idempotent and safe to re-run

### 10.4 Dagster Asset for Ad-Hoc Reset

A lightweight Dagster asset allows operators to trigger a checkpoint reset from the Dagster UI without needing direct DB access:

```python
class DfmResetConfig(dg.Config):
    schema_name: str                    # "arc", "dim", "fct", "fhir", "fhir_server"
    table_name: str                     # e.g. "spend_analysis_lines_kmd_uddata"
    new_value: str                      # e.g. "0", "0001-01-01", "2025-01-01"
    reason: str = ""                    # operational note logged to dfm_run_log
    scope: str = "staging"              # "staging" or "dso"


@dg.asset(
    name="dfm_reset",
    group_name="dfm_operations",
    compute_kind="mssql",
    description="Reset a checkpoint via meta.usp_dfm_reset. Ad-hoc use only.",
)
def dfm_reset(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: DfmResetConfig,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "EXEC [meta].[usp_dfm_reset] "
                "@schema_name = ?, @table_name = ?, "
                "@new_value = ?, @reason = ?, @scope = ?",
                (
                    config.schema_name,
                    config.table_name,
                    config.new_value,
                    config.reason,
                    config.scope,
                ),
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"DFM reset: [{config.scope}] {config.schema_name}.{config.table_name} "
        f"→ {config.new_value!r} (reason: {config.reason!r})"
    )
    return dg.MaterializeResult(
        metadata={
            "scope": config.scope,
            "schema_name": config.schema_name,
            "table_name": config.table_name,
            "new_value": config.new_value,
            "reason": config.reason,
        }
    )
```

A dedicated job exposes this in the Launchpad UI:

```python
dfm_reset_job = define_asset_job(
    name="dfm_reset_job",
    selection="dfm_reset",
    description="Reset a checkpoint to a specific value. Set config in Launchpad.",
)
```

### 10.5 Difference from Ad-Hoc Runs

| Approach | Effect on checkpoint | Effect on next scheduled run |
|----------|----------------------|------------------------------|
| Ad-hoc run (period/ids) | Unchanged | Continues from where it was |
| Manual DFM reset | Permanently changed | Starts from the new value |

Both approaches reuse the same assets. The difference is whether the checkpoint state is modified.

---

## 11. Ad-Hoc Runs — One Set of Assets

A core design goal is to avoid duplicating assets for ad-hoc scenarios. The same `@dg.asset` function handles all run modes.

### 11.1 How It Works

Each asset accepts an optional `RunModeConfig` parameter with sensible defaults:

```python
@dg.asset(...)
def arc_spend_analysis_lines_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,                  # defaults to mode="delta"
) -> dg.MaterializeResult:
    ...
```

- **Scheduled jobs** use default config (or set `mode="full_load"` for full-load tables)
- **Ad-hoc runs** can be launched by opening **any existing job** in the Dagster Launchpad UI and overriding the `RunModeConfig` — no separate ad-hoc job is required

### 11.2 Running Ad-Hoc via Existing Jobs

The Dagster Launchpad UI lets you override the config for any job at launch time. To run the normal `kmd_uddata_archive_job` in period mode:

1. Open `kmd_uddata_archive_job` in the Dagster UI
2. Click "Launchpad"
3. Override the config for each asset: set `mode = "period"`, `date_from`, `date_to`
4. Launch

This works because `RunModeConfig` has defaults — the scheduled run uses those defaults, and ad-hoc runs override them. No code changes or separate jobs needed.

### 11.3 Optional: Dedicated Ad-Hoc Jobs

For convenience, you can also define dedicated ad-hoc jobs with pre-filled config templates. These are **optional** — they just make the Launchpad UI show the right fields pre-populated:

```python
kmd_uddata_adhoc_job = define_asset_job(
    name="kmd_uddata_adhoc_job",
    selection=AssetSelection.groups("kmd_uddata_archive"),
    config=RunConfig(ops={
        "ext_unspsc_kmd_uddata": RunModeConfig(mode="period"),
        "arc_unspsc_kmd_uddata": RunModeConfig(mode="period"),
        "ext_spend_analysis_lines_kmd_uddata": RunModeConfig(mode="period"),
        "arc_spend_analysis_lines_kmd_uddata": RunModeConfig(mode="period"),
    }),
    description="Ad-hoc KMD archive run — set date_from/date_to or id_filter_values at launch.",
)
```

The operator fills in `date_from` / `date_to` (or `id_filter_column` + `id_filter_values`) in the Launchpad UI. The config flows through to both extract and archive assets in the same run.

### 11.4 Period Mode Example

Operator wants to reprocess KMD data from January–June 2021:

1. Launch `kmd_uddata_adhoc_job` with config:
   - `mode = "period"`
   - `date_from = "2021-01-01"`
   - `date_to = "2021-06-01"`
2. Extract asset builds `WHERE audit_date > '2021-01-01' AND audit_date < DATEADD(day, 1, '2021-06-01')`
3. Archive asset MERGEs as normal (updates/inserts into `arc.*`)
4. Checkpoint is **not** advanced — daily scheduled run continues from its current position
5. Log table records `mode="period"` for traceability

### 11.5 ID Mode Example

Operator wants to reprocess 3 specific patients after a data correction:

1. Launch with config:
   - `mode = "ids"`
   - `id_filter_column = "patient_id"`
   - `id_filter_values = ["12345", "67890", "11111"]`
2. Extract asset builds `WHERE [patient_id] IN ('12345', '67890', '11111')`
3. Archive MERGEs those rows (updates existing, inserts new)
4. Checkpoint unchanged
5. Downstream dim/fact/FHIR assets can then be re-run with the same `ids` config to propagate the fix through the full pipeline

For large ID sets (1000+), the operator pre-populates a staging table and sets `id_filter_table = "[adhoc].[filter_ids]"` instead of `id_filter_values`.

### 11.6 ID Mode at Dim/Fact/FHIR Level

Ad-hoc overrides work at **every** layer, not just extract/archive. When a dim/fact or FHIR asset runs with `mode="ids"`:

- The asset filters the **archive table** using `id_filter_column` — this is the column name in the `arc.*` table, not the source system
- Example: `id_filter_column = "episode_of_care_id"` filters `arc.encounter_ifdb` by episode of care
- The checkpoint is not advanced

This allows targeted reprocessing at any layer without re-extracting from the source system.

---

## 12. Partitioned-Period Orchestrator

For large-scale reprocessing (e.g. a full historical reload), running the entire date range in a single `mode="period"` run may be impractical — it could time out, use excessive memory, or make it hard to identify which portion failed. The partitioned-period orchestrator splits the range into chunks and runs each as a separate Dagster run.

### 12.1 Concept

The orchestrator is a **meta-asset** that:

1. Takes a date range and partition granularity (e.g. monthly)
2. Generates N period partitions
3. Launches N runs of a target job **sequentially**, each with `mode="period"` and the partition's date range
4. Stops on first failure — remaining partitions are skipped
5. Delegates checkpoint advancement to each child asset via `RunModeConfig.advance_checkpoint`

```
Orchestrator asset (sequential, advance_checkpoint=True on child runs)
    │
    ├── Run 1: 2021-01-01 → 2021-01-31  ✓  → child assets advance their checkpoints
    ├── Run 2: 2021-02-01 → 2021-02-28  ✓  → child assets advance their checkpoints
    ├── Run 3: 2021-03-01 → 2021-03-31  ✗  → STOP, checkpoint reflects last completed chunk
    │   ...
    └── Runs 4–12: skipped
```
On retry, the next scheduled delta run picks up from the checkpoint left by run 2.

```
Orchestrator asset (sequential, advance_checkpoint=False — default)
    │
    ├── Run 1: 2021-01-01 → 2021-01-31  ✓  → checkpoints unchanged
    ├── Run 2: 2021-02-01 → 2021-02-28  ✓  → checkpoints unchanged
    │   ...
    └── Run N: 2021-12-01 → 2021-12-31  ✓  → checkpoints unchanged
```

### 12.2 Config

```python
class PartitionedPeriodConfig(dg.Config):
    """Config for the partitioned-period orchestrator meta-asset."""

    job_name: str | None = None             # target job to launch, e.g. "ifdb_archive_job"
    asset_names: list[str] | None = None    # alternative: specific asset names to materialize
    date_from: str                          # start of range (ISO date)
    date_to: str                            # end of range (ISO date)
    granularity: str = "month"              # "day" | "week" | "month" | "quarter" | "year"
    delta_column: str | None = None         # override delta_column on child runs (optional)
    advance_checkpoint: bool = False        # propagated to child RunModeConfig.advance_checkpoint
    flow_start_override: str | None = None  # override flow-start on child runs (IFDB only)
```

**`flow_start_override`** is propagated to the `FlowStartConfig` of child runs when set. This allows the period orchestrator to pin the flow-start upper bound for IFDB delta extracts, preventing the `dfm_ifdb_flow_start` asset from querying Oracle `JOB_LOG` on each child run (which would return a live timestamp unrelated to the historical period being reprocessed). Only relevant for IFDB jobs that include the `dfm_ifdb_flow_start` asset.

### 12.3 Implementation

The orchestrator does **not** call `DfmManager` itself. It delegates checkpoint advancement to individual child assets by propagating `advance_checkpoint` to each child run's `RunModeConfig`. This keeps the orchestrator stateless and avoids double-advancement.

```python
@dg.asset(
    name="period_orchestrator",
    group_name="dfm_operations",
    compute_kind="python",
    description=(
        "Meta-asset: splits a date range into partitions and launches "
        "N sequential runs of a target job, each in mode='period'."
    ),
)
def period_orchestrator(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: PartitionedPeriodConfig,
) -> dg.MaterializeResult:
    instance = context.instance
    partitions = generate_date_partitions(
        config.date_from, config.date_to, config.granularity
    )

    context.log.info(
        f"Launching {len(partitions)} partitioned runs "
        f"({config.date_from} → {config.date_to}, granularity={config.granularity})"
    )

    results = []
    for partition_start, partition_end in partitions:
        # Build child run config: mode=period + advance_checkpoint from orchestrator config
        child_run_config = build_period_run_config(
            config, partition_start, partition_end
        )
        run = instance.create_run_for_job(
            job_name=config.job_name,
            run_config=child_run_config,
            tags={
                "dagster/parent_run_id": context.run_id,
                "dagster/partition_start": partition_start,
                "dagster/partition_end": partition_end,
            },
        )
        result = execute_run(instance, run, context)
        results.append(result)
        if result.status != "success":
            context.log.error(f"Partition {partition_start}→{partition_end} failed. Stopping.")
            break

    succeeded = [r for r in results if r.status == "success"]
    failed = [r for r in results if r.status != "success"]

    if failed:
        raise Exception(
            f"Partitioned period run incomplete: {len(failed)} of {len(results)} "
            f"partitions failed."
        )

    return dg.MaterializeResult(
        metadata={
            "date_from": config.date_from,
            "date_to": config.date_to,
            "granularity": config.granularity,
            "total_partitions": len(partitions),
            "succeeded": len(succeeded),
            "failed": len(failed),
        }
    )
```

### 12.4 Partition Generation

```python
def generate_date_partitions(
    date_from: str, date_to: str, granularity: str
) -> list[tuple[str, str]]:
    """Split a date range into (start, end) tuples per granularity.

    Example: ("2021-01-01", "2021-03-31", "month") →
        [("2021-01-01", "2021-01-31"),
         ("2021-02-01", "2021-02-28"),
         ("2021-03-01", "2021-03-31")]
    """
    ...
```

### 12.5 Checkpoint Advancement

The orchestrator's `advance_checkpoint` flag (default `False`) is propagated to each child run's `RunModeConfig.advance_checkpoint`. This means:

| `advance_checkpoint` | Behaviour |
|------|-----------|
| `False` (default) | Child assets do **not** advance checkpoints — safe for investigation/reload without affecting production state |
| `True` | Each child asset advances its own checkpoint on success — useful for incremental historical reloads where the next retry should continue from the last completed chunk |

This design avoids orchestrator-owned state management. The orchestrator is purely a sequencing tool; checkpoint logic remains with the individual assets.

### 12.6 Use Cases

| Scenario | Config |
|----------|--------|
| Full historical reload, resumable on failure | `date_from="2015-01-01"`, `date_to="2025-12-31"`, `granularity="month"`, `advance_checkpoint=True` |
| Reload without advancing (investigation/backfill) | `date_from="2025-01-01"`, `date_to="2025-06-30"`, `granularity="month"`, `advance_checkpoint=False` |
| Daily granularity for a tricky period | `granularity="day"`, `advance_checkpoint=True` |
| Specific assets only | `asset_names=["ext_encounter_ifdb", "arc_encounter_ifdb"]`, `advance_checkpoint=False` |

> **Future**: parallel execution may be added if sequential becomes a bottleneck. The current sequential approach is simpler and safer for IFDB (ordered data guarantees).

---

## 13. Shared Archive Execution

To avoid boilerplate repetition across archive assets, a shared function handles the common lifecycle:

```python
class ArchiveMergeResult(NamedTuple):
    """Result of execute_archive_merge with effective runtime settings."""

    row_count: int
    new_checkpoint: str | None
    detect_deletes: bool
    update_only_on_change: bool
    strategy: str
    upstream_is_full_load: bool
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_soft_deleted: int = 0


def execute_archive_merge(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    merge_config: ArchiveMergeConfig,
    run_config: RunModeConfig,
) -> ArchiveMergeResult:
    """Shared execution logic for all archive merge assets.

    The archive asset is a checkpoint owner — it reads its own checkpoint
    from dfm_state and sets it on success.

    Returns an ArchiveMergeResult with row counts (total, inserted, updated,
    deleted), the new checkpoint, and the effective config flags that were
    applied at runtime.
    """
```

The return type is `ArchiveMergeResult` rather than a plain tuple. For upsert strategies, `build_archive_merge_sql(track_actions=True)` wraps the MERGE in a batch that captures per-row actions via `OUTPUT $action` into a table variable and returns a summary row (inserted, updated, soft_deleted counts) as the next result set. When soft-delete detection is enabled, the tracking table includes an `is_soft_delete` BIT column that uses `inserted`/`deleted` pseudo-tables to distinguish a true soft-delete (the `DW_IsDeleted` column flipping from 0→1) from a regular update — since `$action` alone reports `'UPDATE'` for both. This provides accurate observability in the Dagster UI without requiring separate row-count queries. The `detect_deletes`, `update_only_on_change`, `strategy`, and `upstream_is_full_load` fields reflect the **effective** config after safety gates, so callers can include them in `MaterializeResult` metadata for debugging.

A companion helper `build_archive_metadata(result, config, *, source_table, target_table)` standardises the metadata dict across all archive assets.

### 13.1 Safety Gates

`execute_archive_merge` applies two safety gates before running the MERGE:

**Gate 1 — Partial extract guard** (same as section 5.2):

```python
if (
    merge_config.detect_deletes
    and run_config.mode != "full_load"
    and not merge_config.upstream_is_full_load
):
    context.log.warning(
        "Delete detection disabled — mode=... and upstream_is_full_load=False; "
        "NOT MATCHED BY SOURCE would incorrectly flag rows."
    )
    effective = dataclasses.replace(merge_config, detect_deletes=False)
```

**Gate 2 — Empty source guard** (prevents catastrophic soft-delete from failed extracts):

```python
if effective.detect_deletes:
    ext_count = count_rows(effective.source_schema, effective.source_table)
    if ext_count == 0:
        context.log.warning(
            f"Source has 0 rows — refusing to soft-delete all archive rows. "
            f"This usually indicates a failed upstream extract, not a genuine empty source."
        )
        effective = dataclasses.replace(effective, detect_deletes=False)
```

Even when the mode is `full_load` and delete detection would normally fire, an empty `ext` table almost always indicates a failed or incomplete upstream extract — not a legitimate scenario where all source rows have been deleted. Soft-deleting every row in the archive on a 0-row source would be catastrophic data loss. This gate prevents that.

### 13.2 Lifecycle

```python
    dfm = DfmManager(mssql, context)
    run_ctx = dfm.start_dfm_run(
        merge_config.target_schema,
        merge_config.target_table,
        dagster_run_id=context.run_id,
        mode=run_config.mode,
    )

    try:
        # Apply safety gates (section 13.1)
        effective = apply_safety_gates(merge_config, run_config, context)

        merge_sql = build_archive_merge_sql(effective, track_actions=is_upsert)

        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(merge_sql)

                if is_upsert:
                    cursor.nextset()
                    action_row = cursor.fetchone()
                    rows_inserted, rows_updated, rows_soft_deleted = action_row or (0, 0, 0)
                    row_count = rows_inserted + rows_updated + rows_soft_deleted
                else:
                    row_count = cursor.rowcount

                # Resolve new checkpoint: flow_start_key > checkpoint_query > SYSDATETIME fallback
                new_checkpoint = None
                if effective.flow_start_key:
                    fs_schema, fs_table = effective.flow_start_key
                    new_checkpoint = dfm.get_checkpoint(fs_schema, fs_table)
                elif effective.checkpoint_query:
                    cursor.execute(effective.checkpoint_query)
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row is not None else None

                conn.commit()
            finally:
                cursor.close()

        # Advance checkpoint only when should_advance_checkpoint resolves to True
        logged_checkpoint: str | None = None
        if run_config.should_advance_checkpoint and new_checkpoint is not None:
            dfm.set_checkpoint(
                merge_config.target_schema,
                merge_config.target_table,
                value=new_checkpoint,
                mode=run_config.mode,
            )
            logged_checkpoint = new_checkpoint
        elif (
            run_config.should_advance_checkpoint
            and new_checkpoint is None
            and effective.checkpoint_query is None
            and effective.flow_start_key is None
        ):
            dfm.set_checkpoint(
                merge_config.target_schema,
                merge_config.target_table,
                mode=run_config.mode,
            )
            logged_checkpoint = dfm.get_checkpoint(
                merge_config.target_schema, merge_config.target_table
            )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=logged_checkpoint,
            rows_affected=row_count,
        )
        return ArchiveMergeResult(
            row_count=row_count, new_checkpoint=new_checkpoint,
            detect_deletes=effective.detect_deletes,
            update_only_on_change=effective.update_only_on_change,
            strategy=effective.strategy,
            upstream_is_full_load=merge_config.upstream_is_full_load,
            rows_inserted=rows_inserted, rows_updated=rows_updated,
            rows_soft_deleted=rows_soft_deleted,
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
```

Each archive asset becomes thin — just config + delegation:

```python
SPEND_ANALYSIS_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="spend_analysis_lines_kmd_uddata",
    target_schema="arc",
    target_table="spend_analysis_lines_kmd_uddata",
    merge_keys=("DW_EK_SpendAnalysisLine",),
    non_key_columns=("DW_SK_Compliance", "DW_SK_Date_SpendAnalysis", ...),
    checkpoint_query=(
        "SELECT CAST(MAX([computed_max_audit]) AS NVARCHAR(50)) "
        "FROM [arc].[spend_analysis_lines_kmd_uddata]"
    ),
)


@dg.asset(name="arc_spend_analysis_lines_kmd_uddata", ...)
def arc_spend_analysis_lines_kmd_uddata(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    row_count, new_checkpoint = execute_archive_merge(
        context, mssql_target_staging, SPEND_ANALYSIS_CONFIG, config
    )
    return dg.MaterializeResult(metadata={...})
```

---

## 14. DfmManager API Reference

### 14.1 Class Definition

```python
from typing import NamedTuple


class DfmRunContext(NamedTuple):
    """Returned by DfmManager.start_dfm_run()."""
    log_id: int
    checkpoint: str | None


class DfmManager:
    """Manages DFM (Data Flow Management) state and run logging.

    Reads and writes to [meta].[dfm_state] and [meta].[dfm_run_log].
    All timestamps are set server-side via SYSDATETIME() to avoid clock skew.
    """

    STATE_TABLE = "[meta].[dfm_state]"
    LOG_TABLE = "[meta].[dfm_run_log]"

    def __init__(
        self,
        mssql_resource: MSSQLResource,
        context: dg.AssetExecutionContext,
    ) -> None: ...
```

### 14.2 Two-Tier Usage Pattern

Every pipeline asset calls `start_dfm_run` / `complete_dfm_run` for run logging and traceability. The difference is whether the asset **owns** a checkpoint in `meta.dfm_state`:

| Asset type | Checkpoint owner? | Calls `set_checkpoint`? | `checkpoint_from` param |
|---|---|---|---|
| **arc** (archive) | Yes | Yes | Not needed (reads own entry) |
| **dim / fct** | Yes | Yes | Not needed |
| **fhir / fhir_server** | Yes | Yes | Not needed |
| **ref** (reference) | Yes | Yes | Not needed |
| **dfm** (flow-start, etc.) | Yes | Yes | Not needed |
| **map** | No | No | Not needed (no checkpoint; processes entire stg table) |
| **ext** (extract) | No | No | `("arc", table_name)` |
| **pre_stg / stg** | No | No | `("dim", table_name)` or `("fhir", ...)` etc. |

Only checkpoint owners (arc, dim, fct, fhir, fhir_server) have rows in `meta.dfm_state`. When a checkpoint owner calls `start_dfm_run`, the row is auto-created if it doesn't exist (first run). Non-owners never create state rows.

**Checkpoint owner** (arc, dim, fct, fhir):
```python
dfm = DfmManager(mssql, context)
run_ctx = dfm.start_dfm_run("dim", "Kreditor", dagster_run_id=context.run_id, mode=config.mode)
try:
    # ... do work using run_ctx.checkpoint ...
    dfm.set_checkpoint("dim", "Kreditor", value=new_checkpoint)
    dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=new_checkpoint, rows_affected=n)
except Exception as exc:
    dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
    raise
```

**Log-only without checkpoint** (map):
```python
dfm = DfmManager(mssql, context)
run_ctx = dfm.start_dfm_run("map", "kreditor_mapping", dagster_run_id=context.run_id, mode=config.mode)
# run_ctx.checkpoint is None — map has no dfm_state entry
try:
    # ... update map from stg ...
    dfm.complete_dfm_run(run_ctx.log_id, rows_affected=n)
except Exception as exc:
    dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
    raise
```

**Upstream / log-only** (ext, pre_stg, stg):
```python
dfm = DfmManager(mssql, context)
run_ctx = dfm.start_dfm_run(
    "ext", "encounter_ifdb",
    checkpoint_from=("arc", "encounter_ifdb"),
    dagster_run_id=context.run_id,
    mode=config.mode,
)
try:
    # run_ctx.checkpoint was read from arc.encounter_ifdb's dfm_state entry
    # ... do work using run_ctx.checkpoint ...
    dfm.complete_dfm_run(run_ctx.log_id, rows_affected=n)
except Exception as exc:
    dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
    raise
```

**Multi-target upstream** (pre_stg feeding both dim and fhir):
```python
dfm = DfmManager(mssql, context)
cp_dim = dfm.get_checkpoint("dim", "Encounter")
cp_fhir = dfm.get_checkpoint("fhir_server", "Encounter")
effective_cp = min(cp_dim, cp_fhir)  # use the oldest checkpoint

run_ctx = dfm.start_dfm_run(
    "pre_stg", "encounter",
    dagster_run_id=context.run_id,
    mode=config.mode,
)
# run_ctx.checkpoint is not used here — we use effective_cp instead
```

### 14.3 Methods

**`get_checkpoint(schema, table, *, scope="staging") -> str | None`**
Read-only checkpoint peek. Does **not** create a log entry. Useful in two scenarios:
1. **Multi-target upstream assets** — when a pre_stg/stg asset feeds multiple checkpoint owners and needs the MIN checkpoint across them.
2. **Debugging / operational queries** — scripts or monitoring that want to inspect checkpoint values without side effects.

**`start_dfm_run(schema, table, *, dagster_run_id, mode, checkpoint_from=None, scope="staging") -> DfmRunContext`**
The primary entry point for all pipeline assets. Inserts a "running" entry into `meta.dfm_run_log` and returns a `DfmRunContext(log_id, checkpoint)`.

Checkpoint resolution depends on the asset type:

- **Checkpoint owners** (arc, dim, fct, fhir, fhir_server): `checkpoint_from` is `None`. Reads the checkpoint from the asset's own `(scope, schema, table)` entry in `meta.dfm_state`. If the entry doesn't exist (first run), it is auto-created with `checkpoint_value = NULL`.
- **Upstream assets with checkpoint_from** (ext, pre_stg, stg): reads the checkpoint from the target's `dfm_state` entry. The log entry is recorded under the asset's own schema/table for traceability, but no `dfm_state` row is created for the asset.
- **Non-owner without checkpoint_from** (map): `checkpoint_from` is `None` and the schema is not a checkpoint owner. Returns `checkpoint = None` without touching `dfm_state`. The run is still logged.

```python
CHECKPOINT_OWNER_SCHEMAS = frozenset({"arc", "dim", "fct", "fhir", "fhir_server", "dfm", "ref"})
```

**`set_checkpoint(schema, table, *, value, mode="delta", scope="staging") -> None`**
Set the checkpoint to an explicit value via MERGE upsert. Only called by **checkpoint owners** after successful processing. `value` is the new checkpoint string (e.g. a MAX timestamp or audit value). When `value` is not provided, `CAST(SYSDATETIME() AS NVARCHAR(50))` is used. `mode` is written to `last_mode` on the state row for observability.

**`complete_dfm_run(log_id, *, checkpoint_after=None, rows_affected=None) -> None`**
Mark a run log entry as `"success"` and record completion metadata. Called by **all** assets at the end of successful processing.

**`fail_dfm_run(log_id, *, error_message) -> None`**
Mark a run log entry as `"failed"` and record the error. Called by **all** assets in their exception handler.

---

## 15. Naming Rename Reference

Complete mapping from previous naming to DFM naming:

### Database Objects

| Previous | DFM |
|---|---|
| `meta.etl_watermark_state` | `meta.dfm_state` |
| `meta.etl_watermark_log` | `meta.dfm_run_log` |
| `meta.sync_watermarks` | Unified into `meta.dfm_state` |
| `meta.usp_reset_watermark` | `meta.usp_dfm_reset` |
| Column `entity_type` | `target_type` |
| Column `watermark_value` | `checkpoint_value` |
| Column `watermark_before` | `checkpoint_before` |
| Column `watermark_after` | `checkpoint_after` |

### Python

| Previous | DFM |
|---|---|
| `core/etl_watermark_manager.py` | `core/dfm_manager.py` |
| `EtlWatermarkManager` | `DfmManager` |
| `SyncStateManager` | Unified into `DfmManager` |
| `SCHEMA_TO_ENTITY` | `SCHEMA_TO_TARGET_TYPE` |
| `_entity_type_for_schema()` | `_target_type_for_schema()` |
| `get_watermark()` | `get_checkpoint()` |
| `get_watermark() + log_start_run()` | `start_dfm_run() -> DfmRunContext` |
| `advance_watermark()` | `set_checkpoint()` |
| `log_complete_run()` | `complete_dfm_run()` |
| `log_fail_run()` | `fail_dfm_run()` |
| `ArchiveMergeConfig.watermark_query` | `ArchiveMergeConfig.checkpoint_query` |
| `RunModeConfig.date_column` | `RunModeConfig.delta_column` |
| `WatermarkResetConfig` | `DfmResetConfig` |

### Dagster Assets and Jobs

| Previous | DFM |
|---|---|
| `staging_infra_etl_watermarks` | `staging_infra_dfm` |
| `staging_watermark_indexes` | `staging_dfm_indexes` |
| `dso_watermark_indexes` | `dso_dfm_indexes` |
| `reset_watermark` | `dfm_reset` |
| `reset_watermark_job` | `dfm_reset_job` |

### DDL Files

| Previous | DFM |
|---|---|
| `02_etl_watermark_state.sql` | `02_dfm_state.sql` |
| `03_etl_watermark_log.sql` | `03_dfm_run_log.sql` |
| `04_usp_reset_watermark.sql` | `04_usp_dfm_reset.sql` |
| `01_sync_watermarks.sql` | Removed (unified into `02_dfm_state.sql`) |

---

## 16. Summary

| Concern | Solution | Where |
|---------|----------|-------|
| Run mode | Single `mode` field: delta, full_load, period, ids, flow_start, reset | `RunModeConfig` (run-time) + internal |
| Filter column | `delta_column` (override) for period/delta, `id_filter_column` for ids | `RunModeConfig` (run-time) + extract config (design-time) |
| MERGE config | `ArchiveMergeConfig` with keys, columns, strategy | Design-time per table |
| Composite MERGE keys | `merge_keys: tuple[str, ...]` | `ArchiveMergeConfig` |
| Delete detection | `detect_deletes` + safety gate (only on `full_load`) | `ArchiveMergeConfig` + asset |
| Checkpoint set | Only on `delta` and `full_load` by checkpoint owners | `set_checkpoint` in `DfmManager` |
| Partitioned-period | Orchestrator meta-asset: split range → N runs → advance | `dfm_operations` group |
| Manual checkpoint reset | `meta.usp_dfm_reset` stored procedure + Dagster asset | `infrastructure/ddl/` + `dfm_operations` group |
| Observability | `mode` + `error_message` in run log | `meta.dfm_run_log` |
| Source JOINs | `select_from_expr` on `DfmExtractConfig` (Oracle only) | `DSI/connectors/extract_config.py` |
| One set of assets | `RunModeConfig` with defaults on all assets | All layers |
| Stg → dim/fct/map flow | stg reads arc using dim/fct checkpoint; dim/fct sets checkpoint on success | DSA stg/dim/fct/map assets |
| Map tables | Log-only, no checkpoint; coupled to dim/fct via shared stg | DSA map assets |
| FHIR single-asset | fhir_server (fetch from arc + transform + upload, checkpoint owner) | DSA FHIR IFDB DFM assets |
| Lookback days | Design-time default + run-time override for late-arriving data | Extract config + `RunModeConfig` |
| DSO sync tracking | Unified into `DfmManager` + `meta.dfm_state` (scope column) | DSO layer |
| Combined start API | `start_dfm_run()` returns `DfmRunContext(log_id, checkpoint)` | `DfmManager` |
| Two-tier logging | All assets log via `start_dfm_run`/`complete_dfm_run`; only owners call `set_checkpoint` | `DfmManager` |
| Multi-asset ad-hoc | Select stg + map + dim/fct, launch with one `RunModeConfig` | Dagster UI |

---

## 17. Current Rollout Status

The DFM system has replaced the legacy `EtlWatermarkManager` / `meta.etl_watermark_state` approach with a unified checkpoint and run-logging mechanism (`DfmManager` / `meta.dfm_state` / `meta.dfm_run_log`).

### FHIR pipeline

The IFDB → FHIR flow uses DFM for:

- **Extract + archive**: `ext_patient_ifdb`, `ext_episode_of_care_ifdb`, `ext_encounter_ifdb`, `ext_location_ifdb`, `arc_patient_ifdb`, `arc_episode_of_care_ifdb`, `arc_encounter_ifdb`, `arc_location_ifdb` — with a shared `dfm_ifdb_flow_start` asset that derives the upper-bound timestamp from Oracle `IFDB.JOB_LOG`.
- **FHIR transform + upload**: `fhir_server_patient_dfm`, `fhir_server_eoc_dfm`, `fhir_server_encounter_dfm`, `fhir_server_location_dfm` — using the single-asset batched pattern (fetch + transform + post in one asset) described in section 7.

### DWH ERP pipelines

All ERP pipelines that previously used `EtlWatermarkManager` now run on DFM. The extract assets are generated via the DFM factory (`DSI/mssql/extract/extract_configs.py`), while archive/staging/map/dim/fct assets use `DfmManager` directly.

| Pipeline | Source | Layers covered |
|----------|--------|----------------|
| **UNSPSC** | ILS + KMD UdData | ext → arc → stg → map → dim |
| **Lagerbeholdning** | ILS | ext → arc → pre_stg → stg → map → dim → fct |
| **AfdelingLagerbeholdning** | ILS | ext → arc → pre_stg → stg → map → dim → fct |
| **SpendAnalysisLines** | KMD UdData | ext → arc |

These 30 assets are orchestrated by `erp_daily_dfm_job`, which selects by group name across extract, staging, map, dim, fct, and DSO sync layers.

### Borger / Patient pipeline

The Borger (citizen/patient) assets have been migrated to `DfmManager`:

| Asset | Schema.Table | Checkpoint source |
|-------|-------------|-------------------|
| `ref_patient_borger` | `ref.Patient_Borger` | `MAX(last_updated)` from `fhir.Patient` |
| `ref_patient_address_borger` | `ref.Patient_Address_Borger` | `MAX(last_updated)` from `fhir.Patient_Address` |
| `dim_borger` | `dim.Borger` | `MAX(DW_Timestamp_Update, DW_Timestamp_Insert)` from both ref tables |

These are orchestrated by the existing `shared_dimensions_data_update` job (groups: `ref`, `shared_dimensions`, `shared_dimensions_dso_sync`).

The upstream `fhir.Patient` / `fhir.Patient_Address` tables are populated by DFM SQL-on-FHIR assets (`sof_patient_dfm`, `sof_patient_address_dfm`). The DFM borger assets declare these as Dagster lineage dependencies but use independent `last_updated`-based checkpoints — no DFM flow-start is needed at the SOF layer.

Legacy `_watermark` Borger asset variants and temporary Borger validation jobs have been removed after DFM verification.

### Legacy watermark system

With the Borger migration complete, no production assets use `EtlWatermarkManager`. The `core/etl_watermark_manager.py` module and `meta.etl_watermark_state` / `meta.etl_watermark_log` tables are retained for reference but can be removed when convenient.
