<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Archive MERGE Options

Reference for `ArchiveMergeConfig` flags that control how `ext.*` → `arc.*` MERGE statements behave.

## Strategies

| `strategy` | SQL pattern | Use case |
|---|---|---|
| `"upsert"` (default) | `MERGE ... WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED BY TARGET THEN INSERT` | Source has a stable primary key. Rows are inserted or updated. |
| `"insert_only"` | `INSERT INTO ... SELECT ... FROM ext` | Append-only snapshots (e.g. daily inventory full-loads where each run adds a new snapshot). |

## Delete Detection

| Flag | Effect |
|---|---|
| `detect_deletes=False` (default) | No `WHEN NOT MATCHED BY SOURCE` clause. Rows that disappear from the source stay as-is in the archive. |
| `detect_deletes=True` | Adds `WHEN NOT MATCHED BY SOURCE THEN UPDATE SET [DW_IsDeleted] = 1`. Rows in `arc` that are absent from `ext` are soft-deleted. |

**Safety guard**: delete detection requires that `ext` contains the *complete* set of current rows. A delta extract only contains changed rows — flagging everything else as deleted would be wrong. The MERGE engine therefore disables `detect_deletes` at runtime unless one of these is true:

- `run_config.mode == "full_load"` — the operator explicitly requested a full extraction.
- `upstream_is_full_load=True` on `ArchiveMergeConfig` — the extract asset *always* does a full load regardless of `RunModeConfig.mode` (e.g. `ext_location_ifdb` always extracts all rows from `IFDB.LOCATION`).

When disabled, a warning is logged:
```
Delete detection disabled — mode='delta' and upstream_is_full_load=False;
NOT MATCHED BY SOURCE would incorrectly flag rows.
```

### `upstream_is_full_load`

Set this to `True` when the upstream extract is unconditionally a full load — i.e. it always writes the complete current state of the source to `ext.*`, regardless of what `RunModeConfig.mode` says.

This decouples the archive's delete-detection logic from the orchestrator's run mode. Without it, a `period_orchestrator` run with `mode=period` or the default `mode=delta` would suppress delete detection even though the extract is complete.

| `upstream_is_full_load` | `run_config.mode` | `detect_deletes` on config | Effective? |
|---|---|---|---|
| `False` | `delta` | `True` | **No** (disabled, warning logged) |
| `False` | `full_load` | `True` | Yes |
| `True` | `delta` | `True` | Yes |
| `True` | `period` | `True` | Yes |
| any | any | `False` | No (not configured) |

## Update-Only-On-Change Guard

| Flag | Effect |
|---|---|
| `update_only_on_change=False` (default) | Every matched row gets an UPDATE, even if all column values are identical. |
| `update_only_on_change=True` | Adds an `AND (...)` condition to `WHEN MATCHED` that checks each non-key column with NULL-safe comparison. Only rows with actual data changes are updated. |

The generated condition uses `ISNULL(NULLIF(...))` for NULL-safe comparison:

```sql
WHEN MATCHED AND (
    ISNULL(NULLIF(tgt.[col1], src.[col1]),
           NULLIF(src.[col1], tgt.[col1])) IS NOT NULL
    OR ...
) THEN UPDATE SET ...
```

**When to use**: any table where the source frequently re-sends unchanged rows (e.g. full-load tables). Avoids unnecessary UPDATE writes, transaction log, and `DW_Timestamp_Update` churn.

**Performance impact**: negligible. The comparison runs on rows already fetched by the MERGE join — in-memory column comparison is far cheaper than the I/O saved by skipping unnecessary writes.

## Soft Delete Column

`soft_delete_column` (default `"DW_IsDeleted"`) controls which column is set to `1` when `detect_deletes=True`. The target `arc` table must have this column defined as `BIT NOT NULL DEFAULT 0`.

## Source Delete Column

| Flag | Effect |
|---|---|
| `source_delete_column=None` (default) | No source-side delete detection. |
| `source_delete_column="<column>"` | Reads the named column from the source row to set `soft_delete_column` on both UPDATE and INSERT. |

Unlike `detect_deletes` (which soft-deletes rows via `NOT MATCHED BY SOURCE` when the row disappears from a full-load extract), `source_delete_column` detects deletes **per-row** using an explicit flag column in the source data. This works with delta extracts because the source system provides the deletion status as a column value.

The generated SQL evaluates:
```sql
CASE WHEN src.[<source_delete_column>] IS NOT NULL
     AND src.[<source_delete_column>] NOT IN ('0', '0.0')
THEN 1 ELSE 0 END
```

This expression is applied in both the `WHEN MATCHED THEN UPDATE SET` and `WHEN NOT MATCHED BY TARGET THEN INSERT` clauses. The two delete mechanisms are independent and can be combined on the same table if needed.

## Example Configurations

**Delta source, no deletes** (encounter):
```python
ArchiveMergeConfig(
    ...,
    strategy="upsert",
    detect_deletes=False,
)
```

**Full-load source, soft deletes + change guard** (location):
```python
ArchiveMergeConfig(
    ...,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="DW_IsDeleted",
    upstream_is_full_load=True,
)
```

**Append-only snapshot** (daily inventory):
```python
ArchiveMergeConfig(
    ...,
    strategy="insert_only",
)
```
