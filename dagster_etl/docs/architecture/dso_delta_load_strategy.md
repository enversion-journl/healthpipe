<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DSO Delta Load Strategy - Decision Document

**Status:** Draft
**Date:** 2026-02-09
**Purpose:** Recommend optimal delta load strategy for DSA to DSO synchronization

---

## Executive Summary

This document compares three delta load strategies for DSA to DSO synchronization and recommends **Watermark-based Delta (Strategy 1)** based on:
- Lowest implementation complexity
- Full compatibility with existing `DSO/load/sync_utils.py` and linked server
- Leverages already existing `DW_Timestamp_Insert`/`DW_Timestamp_Update` columns
- Optimal suitability for both map tables and fact tables
- Scalable delete handling via `DW_IsDeleted` soft-delete pattern (no full table scans required)

---

## 1. Context and Current Implementation

### 1.1 Current Sync Architecture

```
DSA (Staging DB)                          DSO (DPA DB)
+-------------------+                     +-------------------+
|  dim.Kreditor     | ---STAGING_LINK-->  |  dim.Kreditor     |
|  fct.Faktura      |      (MERGE)        |  fct.Faktura      |
|  map.Kreditor     |                     |  (no maps)        |
+-------------------+                     +-------------------+
```

### 1.2 Current Implementation in `DSO/load/sync_utils.py`

| Component | Implementation |
|-----------|----------------|
| **Sync function** | `sync_table_via_linked_server()` |
| **MERGE builder** | `build_merge_statement()` |
| **Configuration** | `SyncTableConfig` dataclass |
| **Linked Server** | `STAGING_LINK` for 4-part naming |
| **Sync modes** | `merge` (MERGE statement) or `truncate_insert` |

### 1.3 Existing Audit Columns (all tables)

```sql
-- All DSA tables have these columns:
DW_Timestamp_Insert  DATETIME2(0)  -- Set on INSERT
DW_Timestamp_Update  DATETIME2(0)  -- Updated on each MERGE
```

### 1.4 Current Problem

The existing MERGE implementation performs **full table sync**:

```sql
-- Current: All rows are evaluated
MERGE [dim].[Kreditor] AS tgt
USING [STAGING_LINK].[DW].[dim].[Kreditor] AS src  -- ENTIRE table
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...;
```

**Consequence:** Performance degrades linearly with table size, regardless of number of changes.

---

## 2. Strategies Under Evaluation

### Strategy 1: Watermark-based Delta

**Concept:** Filter source data on `DW_Timestamp_Update > @last_sync_watermark`

```sql
-- Delta MERGE with watermark filter
MERGE [dim].[Kreditor] AS tgt
USING (
    SELECT * FROM [STAGING_LINK].[DW].[dim].[Kreditor]
    WHERE DW_Timestamp_Update > @last_sync_timestamp  -- DELTA FILTER
) AS src
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
```

#### Advantages
- Leverages existing `DW_Timestamp_Insert`/`DW_Timestamp_Update` columns
- Minimal change in `DSO/load/sync_utils.py` (add WHERE clause in `build_merge_statement`)
- Full compatibility with linked server approach
- Scales with number of changes, not table size
- Index-friendly (timestamp columns can be indexed)
- Simple debugging (compare timestamps)

#### Disadvantages
- Requires watermark tracking (new metadata table)
- Does not detect hard deletes directly (see section 2.1.1 for solution)
- Theoretical risk of clock skew between servers
- First sync is always a full load

#### 2.1.1 Handling Updates and Deletes

**Updates: Handled**

The watermark strategy handles updates correctly:

```sql
-- Example: Kreditor is updated in DSA
UPDATE [dim].[Kreditor]
SET Kreditor_Navn = 'New Name', DW_Timestamp_Update = SYSDATETIME()
WHERE DW_EK_Kreditor = 123;

-- At next sync: The row is included due to updated timestamp
MERGE [dim].[Kreditor] AS tgt
USING (
    SELECT * FROM [STAGING_LINK].[DW].[dim].[Kreditor]
    WHERE DW_Timestamp_Update > @last_watermark  -- Row is included
) AS src
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor
WHEN MATCHED THEN UPDATE SET ...  -- Existing DSO row is updated
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
```

**Deletes: Handled via `DW_IsDeleted` soft-delete pattern**

Hard deletes (physical row removal) are inherently invisible to a watermark-based strategy because deleted rows no longer exist in the source table and therefore cannot be captured by any timestamp filter. However, periodic full table scans using `NOT MATCHED BY SOURCE` to detect these deletions are **not scalable** for tables with tens or hundreds of millions of rows -- transferring all rows over a linked server purely to detect a small number of deletions is prohibitively expensive.

The recommended approach is **soft-delete via `DW_IsDeleted`** in the DSA layer. This ensures that deletions are captured by the same watermark mechanism used for inserts and updates, with zero additional overhead in the DSO sync.

**Design Principle:** DSO never tracks orphaned source rows. The DSA layer is solely responsible for marking rows as deleted. DSO simply mirrors the `DW_IsDeleted` flag through the normal MERGE process. See [DSA Delete Handling Strategy](architecture/dsa_delete_handling_strategy.md) for the DSA-side implementation plan.

**How it works:**

```sql
-- Step 1: DSA marks row as soft-deleted (instead of DELETE)
-- This happens in the DSA MERGE via: WHEN NOT MATCHED BY SOURCE THEN UPDATE SET ...
UPDATE [dim].[Kreditor]
SET DW_IsDeleted = 1,
    DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
WHERE DW_EK_Kreditor = 123;
-- DW_Timestamp_Update is updated, so the watermark filter picks it up

-- Step 2: DSO watermark sync captures the change automatically
MERGE [dim].[Kreditor] AS tgt
USING (
    SELECT * FROM [STAGING_LINK].[DW].[dim].[Kreditor]
    WHERE DW_Timestamp_Update > @last_watermark  -- Soft-deleted row is included
) AS src
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor
WHEN MATCHED THEN UPDATE SET
    ...,
    tgt.[DW_IsDeleted] = src.[DW_IsDeleted],  -- Flag propagated naturally
    tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT ...;
-- No special DSO-side delete logic required
```

**Why soft-delete over periodic full sync?**

| Approach | Rows transferred | Scalability | Complexity |
|----------|-----------------|-------------|------------|
| **Soft-delete (`DW_IsDeleted`)** | Only changed rows (O(changes)) | Scales to 100M+ rows | Low -- no DSO changes |
| **Periodic full MERGE with `NOT MATCHED BY SOURCE`** | All rows (O(table_size)) | Does not scale beyond ~1M rows over linked server | Low but expensive |
| **Delete-tracking table** | Only deleted PKs (O(deletes)) | Scales well | Moderate -- extra table + cleanup |
| **CDC** | All changes including deletes | Scales well | High -- DB-level configuration |

**Summary of delete handling options:**

| Option | Description | Scalability | Recommended |
|--------|-------------|-------------|-------------|
| **A: Soft-delete (`DW_IsDeleted`)** | DSA sets `DW_IsDeleted = 1` instead of hard delete. Watermark captures it automatically. | Excellent | **Yes -- primary** |
| **B: Periodic full sync** | Full MERGE with `NOT MATCHED BY SOURCE` on a schedule | Poor for large tables | Only for small tables (< 10K rows) |
| **C: Delete-tracking table** | Log deleted PKs to a separate table, sync via watermark | Good | Possible alternative |
| **D: CDC** | Captures all changes including deletes at DB level | Excellent | Overkill for this use case |

**Recommended: Option A (`DW_IsDeleted` soft-delete)**

This is the only approach that:
1. Requires **zero additional logic** in the DSO sync layer
2. Uses the **same watermark mechanism** for all change types (insert, update, delete)
3. Scales to **hundreds of millions of rows** without full table scans
4. Keeps the **DSO layer simple** -- it just mirrors what DSA provides

> **Dependency:** The DSA layer must implement soft-delete before DSO can propagate deletions.
> See [DSA Delete Handling Strategy](architecture/dsa_delete_handling_strategy.md) for the implementation plan.
> Until DSA implements `DW_IsDeleted`, deletions are not propagated to DSO.

#### Impact on `DSO/load/sync_utils.py`

```python
# SyncTableConfig with mode='watermark' (implemented)
@dataclass
class SyncTableConfig:
    # ... existing fields ...
    mode: Literal["merge", "watermark", "truncate_insert"] = "merge"
    watermark_column: str | None = None  # Required when mode='watermark'

# build_delta_merge_statement() generates a filtered MERGE
def build_delta_merge_statement(config, columns, last_sync_timestamp=None):
    source_table = f"[{linked_server}].[{staging_db}].[{config.schema}].[{config.table}]"

    if last_sync_timestamp is None:
        return build_merge_statement(config, columns)  # Full sync fallback

    where_clause = f"COALESCE([{config.watermark_column}], [DW_Timestamp_Insert]) >= '{timestamp}'"
    return f"""
        MERGE {target_table} AS tgt
        USING (SELECT * FROM {source_table} WHERE {where_clause}) AS src
        ...
    """
```

Note: No `DW_IsDeleted`-specific fields are needed in `SyncTableConfig`. The `DW_IsDeleted` column flows through the MERGE naturally as part of the normal column list -- it is included in both UPDATE SET and INSERT clauses just like any other data column.

#### Suitability per Table Type

| Table Type | Suitability | Rationale |
|------------|-------------|-----------|
| **Map tables** | Optimal | INSERT-only -> `DW_Timestamp_Insert` filter |
| **Dimension tables** | Optimal | INSERT/UPDATE/soft-delete -> `DW_Timestamp_Update` filter |
| **Fact tables (INSERT-heavy)** | Optimal | Primarily INSERTs -> efficient timestamp filter |
| **Fact tables (with updates)** | Good | UPDATE trigger updates timestamp |

---

### Strategy 2: Hash-based Change Detection

**Concept:** Compute HASHBYTES on rows and compare DSA vs DSO

```sql
-- Hash-based change detection
WITH dsa_hashes AS (
    SELECT
        DW_EK_Kreditor,
        HASHBYTES('SHA2_256', CONCAT_WS('|',
            Kreditor_Navn, CVR_Nummer, Adresse, PostNr, By
        )) AS row_hash
    FROM [STAGING_LINK].[DW].[dim].[Kreditor]
),
dso_hashes AS (
    SELECT
        DW_EK_Kreditor,
        HASHBYTES('SHA2_256', CONCAT_WS('|',
            Kreditor_Navn, CVR_Nummer, Adresse, PostNr, By
        )) AS row_hash
    FROM [dim].[Kreditor]
)
-- Find changed rows
SELECT dsa.DW_EK_Kreditor
FROM dsa_hashes dsa
LEFT JOIN dso_hashes dso ON dsa.DW_EK_Kreditor = dso.DW_EK_Kreditor
WHERE dso.row_hash IS NULL OR dsa.row_hash <> dso.row_hash;
```

#### Advantages
- Precise change detection (catches all changes)
- Requires no extra columns in source
- Independent of timestamps
- Can detect "phantom updates" (same data written again)

#### Disadvantages
- CPU-intensive hash computation on both sides
- Requires full table scan on BOTH DSA and DSO
- HASHBYTES over linked server is slow
- Complex implementation in `DSO/load/sync_utils.py`
- Hash function must be maintained per table (column list)
- Does not handle deletes (only change detection)

#### Impact on `DSO/load/sync_utils.py`

```python
# SIGNIFICANT change - new function required
def build_hash_comparison_query(config, columns):
    """Build hash-comparison CTE - complex and table-specific."""
    hash_columns = [c for c in columns if c not in ['DW_Timestamp_Insert', 'DW_Timestamp_Update']]
    hash_expr = f"HASHBYTES('SHA2_256', CONCAT_WS('|', {', '.join(hash_columns)}))"

    # Requires 2 full table scans + hash computation
    return f"""
        WITH dsa_hashes AS (
            SELECT {config.primary_key}, {hash_expr} AS row_hash
            FROM [{linked_server}].[{staging_db}].[{config.schema}].[{config.table}]
        ),
        dso_hashes AS (
            SELECT {config.primary_key}, {hash_expr} AS row_hash
            FROM [{config.schema}].[{config.table}]
        ),
        changed_rows AS (
            SELECT dsa.{config.primary_key}
            FROM dsa_hashes dsa
            LEFT JOIN dso_hashes dso ON dsa.{config.primary_key} = dso.{config.primary_key}
            WHERE dso.row_hash IS NULL OR dsa.row_hash <> dso.row_hash
        )
        -- Then MERGE only on changed_rows
        ...
    """
```

#### Suitability per Table Type

| Table Type | Suitability | Rationale |
|------------|-------------|-----------|
| **Map tables** | Overkill | INSERT-only does not need hash comparison |
| **Dimension tables** | Possible | But timestamp is simpler and faster |
| **Fact tables (INSERT-heavy)** | Inefficient | Full scan for primarily INSERTs |
| **Fact tables (with updates)** | Possible | But high overhead vs. benefit |

---

### Strategy 3: Full Refresh (Baseline)

**Concept:** Keep current MERGE or use TRUNCATE + INSERT for all syncs

```sql
-- Option A: Full MERGE (current)
MERGE [dim].[Kreditor] AS tgt
USING [STAGING_LINK].[DW].[dim].[Kreditor] AS src
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;

-- Option B: TRUNCATE + INSERT
TRUNCATE TABLE [dim].[Kreditor];
INSERT INTO [dim].[Kreditor]
SELECT * FROM [STAGING_LINK].[DW].[dim].[Kreditor];
```

#### Advantages
- Simple implementation (already implemented)
- Guarantees 100% consistency
- No metadata tracking
- Handles deletes implicitly (TRUNCATE+INSERT)

#### Disadvantages
- Performance degrades with table size
- Unnecessary I/O for large tables with few changes
- High load on linked server
- TRUNCATE+INSERT breaks referential integrity temporarily

#### Impact on `DSO/load/sync_utils.py`

No changes - this is the current implementation.

#### Suitability per Table Type

| Table Type | Suitability | Rationale |
|------------|-------------|-----------|
| **Map tables** | Acceptable for small tables | But inefficient as they grow |
| **Dimension tables** | Acceptable for small tables | Scales poorly |
| **Fact tables (INSERT-heavy)** | Inefficient | Large tables = long sync time |
| **Fact tables (with updates)** | Simple but slow | Full scan every time |

---

## 3. Decision Matrix

### 3.1 Parameter-based Comparison

| Parameter | Strategy 1: Watermark | Strategy 2: Hash | Strategy 3: Full Refresh |
|-----------|----------------------|------------------|-------------------------|
| **Latency** | Low (changes only) | High (2x full scan + hash) | High (full scan) |
| **Complexity** | Low (WHERE clause + metadata) | High (CTE, hash logic) | None (existing) |
| **Storage Overhead** | Minimal (~1KB per table) | None | None |
| **Maintenance** | Low (watermark reset on error) | High (hash column list) | Minimal |
| **Delete Handling** | Via `DW_IsDeleted` (no extra cost) | Not supported | Implicit (TRUNCATE+INSERT) |

### 3.2 Detailed Scoring

| Criterion | Weight | S1: Watermark | S2: Hash | S3: Full Refresh |
|-----------|--------|---------------|----------|------------------|
| **Latency (sync time)** | 30% | 5 | 2 | 2 |
| **Complexity** | 25% | 4 | 2 | 5 |
| **Storage Overhead** | 15% | 4 | 5 | 5 |
| **Maintenance** | 20% | 4 | 2 | 5 |
| **Linked Server compatibility** | 10% | 5 | 3 | 5 |

**Weighted Score:**

```
Strategy 1 (Watermark):
  Latency:       0.30 x 5 = 1.50
  Complexity:    0.25 x 4 = 1.00
  Storage:       0.15 x 4 = 0.60
  Maintenance:   0.20 x 4 = 0.80
  Linked Server: 0.10 x 5 = 0.50
  ─────────────────────────────
  TOTAL:                  4.40  RECOMMENDED

Strategy 2 (Hash):
  Latency:       0.30 x 2 = 0.60
  Complexity:    0.25 x 2 = 0.50
  Storage:       0.15 x 5 = 0.75
  Maintenance:   0.20 x 2 = 0.40
  Linked Server: 0.10 x 3 = 0.30
  ─────────────────────────────
  TOTAL:                  2.55

Strategy 3 (Full Refresh):
  Latency:       0.30 x 2 = 0.60
  Complexity:    0.25 x 5 = 1.25
  Storage:       0.15 x 5 = 0.75
  Maintenance:   0.20 x 5 = 1.00
  Linked Server: 0.10 x 5 = 0.50
  ─────────────────────────────
  TOTAL:                  4.10
```

### 3.3 Performance Estimates

```
Example: dim.Kreditor with 500,000 rows, ~1% daily changes (5,000 rows)

+---------------------+-------------------+-------------------+-------------------+
| Metric              | S1: Watermark     | S2: Hash          | S3: Full Refresh  |
+---------------------+-------------------+-------------------+-------------------+
| Rows read (DSA)     | 5,000             | 500,000           | 500,000           |
| Rows read (DSO)     | 0                 | 500,000           | 500,000           |
| Network I/O         | ~500 KB           | ~100 MB           | ~50 MB            |
| Estimated sync time | 2-5 seconds       | 3-5 minutes       | 1-2 minutes       |
| CPU overhead        | Low               | High (hash calc)  | Medium            |
+---------------------+-------------------+-------------------+-------------------+
```

---

## 4. Compatibility with Existing Implementation

### 4.1 Linked Server Compatibility

| Strategy | Compatibility | Notes |
|----------|---------------|-------|
| **S1: Watermark** | Full | WHERE clause works over linked server |
| **S2: Hash** | Partial | HASHBYTES over linked server is slow; requires optimization |
| **S3: Full Refresh** | Full | Current implementation |

### 4.2 Changes to `DSO/load/sync_utils.py`

| Strategy | Change Scope | Files Affected |
|----------|-------------|----------------|
| **S1: Watermark** | Small | `DSO/load/sync_utils.py`: Add watermark parameter + filter |
| **S2: Hash** | Large | `DSO/load/sync_utils.py`: New hash-builder function, CTE logic |
| **S3: Full Refresh** | None | - |

### 4.3 Suitability per Table Type (Summary)

| Table Type | Characteristics | S1: Watermark | S2: Hash | S3: Full |
|------------|----------------|---------------|----------|----------|
| **map.Kreditor** | INSERT-only, lookup | Optimal | Overkill | OK |
| **map.Vare** | INSERT-only, lookup | Optimal | Overkill | OK |
| **dim.Kreditor** | INSERT + UPDATE | Optimal | Possible | OK |
| **dim.Borger** | Type 2 SCD | Full refresh* | Complex | Required |
| **fct.Fakturalinje** | INSERT-heavy | Optimal | Inefficient | Slow |
| **fct.Kontakt** | INSERT-heavy | Optimal | Inefficient | Slow |

*Type 2 SCD with IDENTITY on `DW_SK` requires full refresh due to surrogate key generation.

---

## 5. Recommendation

### Primary Recommendation: Strategy 1 - Watermark-based Delta

**Choose Strategy 1** based on:

| Criterion | Rationale |
|-----------|-----------|
| **Highest score** | 4.40 vs. 2.55 (Hash) and 4.10 (Full Refresh) |
| **Lowest latency** | Only changed rows are synchronized |
| **Leverages existing infrastructure** | `DW_Timestamp_Insert`/`DW_Timestamp_Update` already exist |
| **Minimal code change** | Add WHERE clause in `build_merge_statement()` |
| **Linked server compatible** | WHERE clause works directly |
| **Optimal for both table types** | Map (INSERT) and Fact (INSERT-heavy) |
| **Scalable delete handling** | `DW_IsDeleted` propagates via same watermark mechanism |

### Hybrid Approach for Special Cases

| Table Type | Strategy |
|------------|----------|
| **Standard tables** | Watermark-based delta |
| **Type 2 SCD (dim.Borger)** | `merge` mode (DW_SK_Borger carried from ref.Patient_Address_Borger, no IDENTITY) |
| **Small lookup tables (< 1,000 rows)** | Full refresh acceptable |

### Implementation Recommendation for Task 2

```python
# Implemented SyncTableConfig (see DSO/load/sync_utils.py)
@dataclass
class SyncTableConfig:
    schema: str
    table: str
    primary_key: str | list[str]
    mode: Literal["merge", "watermark", "truncate_insert"] = "merge"
    exclude_columns: list[str] = field(default_factory=list)
    insert_only_columns: list[str] = field(default_factory=lambda: ["DW_Timestamp_Insert"])
    identity_insert: bool = False
    watermark_column: str | None = None  # Required when mode='watermark'
    chunk_size: int = 5000               # Python fallback batch size
```

Note: `DW_IsDeleted` does not require any special configuration in `SyncTableConfig`. When DSA adds the `DW_IsDeleted` column to tables, it flows through the existing MERGE as a normal data column -- included in both the UPDATE SET and INSERT clauses automatically. The watermark filter captures the change because `DW_Timestamp_Update` is updated when `DW_IsDeleted` is set.

### Summary: What Is Handled?

| Operation | Watermark Delta | Notes |
|-----------|----------------|-------|
| **INSERT** | Yes | New rows have `DW_Timestamp_Insert` >= watermark |
| **UPDATE** | Yes | Updated rows have `DW_Timestamp_Update` >= watermark |
| **Soft DELETE** | Yes | `DW_IsDeleted = 1` updates `DW_Timestamp_Update`, captured by watermark |
| **Hard DELETE** | No | Hard deletes are not used -- DSA uses soft-delete pattern exclusively |

---

## 6. Risks and Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Timestamp clock skew | Low | High | Use `SYSDATETIME()` on server, not client |
| Watermark corruption | Low | High | Transaction-safe updates, backup before sync |
| DSA `DW_IsDeleted` not yet implemented | High (current state) | Medium | Deletions not propagated until DSA implements soft-delete. See [DSA Delete Handling Strategy](architecture/dsa_delete_handling_strategy.md) |
| Initial sync slow | Low | Low | First sync = full load, then delta |

---

## 7. Next Steps (Task 2)

1. ~~**Create watermark metadata table** in DSO database~~ -- Done: `[meta].[sync_watermarks]` via `dso_infra_meta` asset
2. ~~**Extend `SyncTableConfig`** with `mode='watermark'` and `watermark_column`~~ -- Done
3. ~~**Implement `build_delta_merge_statement()`** with watermark WHERE filter~~ -- Done
4. ~~**Implement watermark read/write** via `SyncStateManager`~~ -- Done
5. ~~**Configure dim and fct tables** with `mode='watermark'`~~ -- Done (18 tables)
6. **Pilot test** on `dim.Kreditor`
7. **Gradual rollout** to remaining tables

**Prerequisite for delete propagation:**
7. **Implement `DW_IsDeleted` in DSA** (separate initiative, see [DSA Delete Handling Strategy](architecture/dsa_delete_handling_strategy.md))
8. **Add `DW_IsDeleted` column to DSO table DDLs** (mirrors DSA schema)

---

## Decision

| Field | Value |
|-------|-------|
| **Date** | _[To be filled]_ |
| **Recommended strategy** | Strategy 1: Watermark-based Delta |
| **Decision** | _[Approved/Rejected/Deferred]_ |
| **Decision makers** | _[Names]_ |
| **Comments** | _[Notes]_ |

---

## Appendix A: Watermark Table DDL

```sql
CREATE SCHEMA [meta] AUTHORIZATION [dbo];
GO

CREATE TABLE [meta].[sync_watermarks] (
    schema_name         NVARCHAR(128)   NOT NULL,
    table_name          NVARCHAR(128)   NOT NULL,
    last_sync_timestamp DATETIME2(0)    NOT NULL,
    last_sync_rows      INT             NULL,
    last_sync_duration_ms INT           NULL,
    last_sync_status    NVARCHAR(50)    NULL,
    created_at          DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    updated_at          DATETIME2(0)    NOT NULL DEFAULT SYSDATETIME(),
    CONSTRAINT PK_sync_watermarks PRIMARY KEY (schema_name, table_name)
);
GO
```

## Appendix B: Related Files

| File | Description |
|------|-------------|
| `dagster_etl/DSO/load/sync_utils.py` | Current sync implementation |
| `dagster_etl/DSO/load/sync_configs.py` | Table configurations |
| `dagster_etl/docs/implementation/sql_elt_guide.md` | DWH architecture |
| `dagster_etl/docs/architecture/dsa_delete_handling_strategy.md` | DSA-side delete handling plan |

## Appendix C: Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-09 | - | Initial version (Danish) |
| 1.1 | 2026-02-11 | - | Translated to English. Replaced periodic full sync with `DW_IsDeleted` soft-delete as primary delete strategy. Added reference to DSA Delete Handling Strategy. |
