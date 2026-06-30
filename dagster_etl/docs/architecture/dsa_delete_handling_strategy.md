<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DSA Delete Handling Strategy

**Status:** Draft
**Date:** 2026-02-11
**Purpose:** Define how the DSA (Data Staging Area) layer should detect and handle rows deleted from source systems

---

## 1. Problem Statement

### 1.1 Current State

All DSA MERGE statements use only `WHEN MATCHED THEN UPDATE` and `WHEN NOT MATCHED BY TARGET THEN INSERT`. No MERGE uses `WHEN NOT MATCHED BY SOURCE`, meaning rows that disappear from source systems are **never removed or marked** in DSA. They accumulate indefinitely as orphaned rows.

### 1.2 Affected MERGE Statements

| Domain | Table | File | Pattern |
|--------|-------|------|---------|
| erp | dim.Kreditor | `DSA/SQL/dim/DimUpd_kreditor.py` | MERGE (no delete) |
| erp | dim.Vare | `DSA/SQL/dim/DimUpd_vare.py` | MERGE (no delete) |
| erp | dim.Kontoplan | `DSA/SQL/dim/DimUpd_kontoplan.py` | MERGE (no delete) |
| erp | dim.FakturaEgenskaber | `DSA/SQL/dim/DimUpd_faktura_egenskaber.py` | MERGE (no delete) |
| erp | dim.Fakturalinje | `DSA/SQL/dim/DimUpd_fakturalinje.py` | MERGE (no delete) |
| erp | fct.Fakturalinje | `DSA/SQL/fct/FctUpd_fakturalinje.py` | MERGE (no delete) |
| erp | fct.Postering | `DSA/SQL/fct/FctUpd_postering.py` | MERGE (no delete) |
| shared_dimensions | ref.Patient_Borger | `DSA/SQL/ref/patient_borger.py` | MERGE + cascade soft-delete (watermark delta) |
| shared_dimensions | ref.Patient_Address_Borger | `DSA/SQL/ref/patient_address_borger.py` | MERGE + patient-scoped soft-delete (watermark delta) |
| shared_dimensions | dim.Borger | `DSA/SQL/dim/borger.py` | MERGE keyed on DW_SK_Borger (soft-delete via NOT MATCHED BY SOURCE) |
| organization | map.Organisation | `DSA/SQL/organization/map_organization.py` | MERGE (no delete) |
| organization | dim.Organisation | `DSA/SQL/organization/dim_organization.py` | MERGE (no delete) |

### 1.3 Impact

- **Data quality:** DSA (and downstream DSO) may contain rows that no longer exist in the source system
- **Row count drift:** Over time, DSA tables grow larger than the source, with no mechanism to detect or correct this
- **Downstream dependency:** The [DSO Delta Load Strategy](architecture/dso_delta_load_strategy.md) relies on `DW_IsDeleted` being set in DSA for watermark-based delete propagation to DSO

---

## 2. Recommended Solution: Soft-Delete Pattern

### 2.1 Design Principles

1. **No hard deletes in DSA** -- rows are never physically removed from DSA tables
2. **Soft-delete via `DW_IsDeleted`** -- a `BIT` column that marks rows as logically deleted
3. **Timestamp update on soft-delete** -- `DW_Timestamp_Update` must be updated when `DW_IsDeleted` is set, ensuring downstream watermark-based syncs capture the change
4. **Reversible** -- if a source row reappears (e.g., after a source system restore), the `DW_IsDeleted` flag can be reset to `0`

### 2.2 Target MERGE Pattern

```sql
-- DSA MERGE pattern with soft-delete:
MERGE [dim].[Kreditor] AS tgt
USING #source AS src
ON tgt.DW_EK_Kreditor = src.DW_EK_Kreditor

WHEN MATCHED THEN
    UPDATE SET
        tgt.Kreditor_Navn = src.Kreditor_Navn,
        ...,
        tgt.DW_IsDeleted = 0,  -- Reset flag if row reappears
        tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (DW_EK_Kreditor, Kreditor_Navn, ..., DW_IsDeleted, DW_Timestamp_Insert, DW_Timestamp_Update)
    VALUES (src.DW_EK_Kreditor, src.Kreditor_Navn, ..., 0, CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)))

-- NEW: Soft-delete rows no longer in source
WHEN NOT MATCHED BY SOURCE AND tgt.DW_IsDeleted = 0 THEN
    UPDATE SET
        tgt.DW_IsDeleted = 1,
        tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0));
```

Key details:
- The `NOT MATCHED BY SOURCE` clause uses `AND tgt.DW_IsDeleted = 0` to avoid repeatedly updating already-deleted rows (prevents unnecessary `DW_Timestamp_Update` changes)
- The `WHEN MATCHED` clause resets `DW_IsDeleted = 0` so that rows reappearing in the source are automatically "undeleted"
- `DW_Timestamp_Update` is always updated on soft-delete, ensuring the DSO watermark sync captures the change

---

## 3. Staging Completeness Analysis

The `NOT MATCHED BY SOURCE` clause requires the MERGE source to represent the **complete** current state. Analysis of the DSI extraction layer confirms this is the case for all current domains.

### 3.1 How Staging Tables Reflect Source State

| Domain | Source System | DSI Extract Strategy | Staging State |
|--------|-------------|---------------------|---------------|
| **erp** (master data) | MySupply, Sydax, ILS (MSSQL) | Full load: DROP + recreate | Complete snapshot |
| **erp** (partitioned) | MySupply, Sydax, ILS (MSSQL) | Incremental: delete partition + insert | Accumulated across all extracted partitions |
| **erp** (ILS full) | ILS (MSSQL) | Full with hash consistency check | Complete snapshot |
| **shared_dimensions** | MSSQL, SOR, IFDB | Full extract | Complete snapshot |
| **organization** | SOR, IFDB, Sydax | Full extract per source, combined in Python | Complete snapshot |

### 3.2 DSA MERGE Source Construction

All DSA MERGE assets build their `#source` temp tables from the **full** staging tables -- not filtered by partition or date. This means:

- **Dimension tables:** MERGE source = full current state from staging. Safe for `NOT MATCHED BY SOURCE`.
- **Map tables:** MERGE source = full current state from staging. Safe for `NOT MATCHED BY SOURCE`.
- **Fact tables:** MERGE source is built from full staging/pre-staging tables (e.g., `stg.fct_fakturalinje_mysupply`). However, the underlying staging data may have stale partitions -- partitions not recently re-extracted from the source.

### 3.3 The Stale Partition Problem (Fact Tables)

For incrementally extracted source tables, the staging table accumulates data across all partitions. When a partition is re-extracted, old data for that partition is replaced with current source data. But partitions that have not been recently re-extracted still contain their original data.

This means: a row hard-deleted in the source within a stale partition **remains visible** in staging (and thus in the DSA MERGE source). The `NOT MATCHED BY SOURCE` clause would not detect it as deleted.

**Impact per table type:**

| Table Type | Stale Partition Risk | Consequence |
|------------|---------------------|-------------|
| **Dimension tables** | None -- staging is full snapshot | `NOT MATCHED BY SOURCE` is safe |
| **Map tables** | None -- staging is full snapshot | `NOT MATCHED BY SOURCE` is safe |
| **Fact tables (partition-loaded)** | Yes -- erp stg tables accumulate partitions | Requires PK reconciliation (Tier 3) for stale partitions |

### 3.4 Multi-Source Tables: Archive Table Pattern

Some DSA tables are built by combining multiple source tables into a single DSA target. For example, if `source_A` and `source_B` are combined into `dim.Combined` in DSA, the standard `NOT MATCHED BY SOURCE` approach cannot be used directly -- the MERGE source in DSA is derived from multiple upstream tables, and no single source system holds the complete truth.

**Problem:** When a row is hard-deleted in `source_A`, the DSA MERGE source (which combines both `source_A` and `source_B`) will no longer contain that row. However, if the MERGE source is only rebuilt from the currently available data, there is no way to distinguish "deleted from source_A" from "never existed in source_A" without a reference point.

**Solution: Archive tables at the DSA pre-staging level.** For each contributing source table, maintain an archive table that preserves the full history of extracted rows:

```
Source Systems                DSA Archive Layer              DSA Layer
+------------+               +---------------------+        +------------------+
| source_A   | --extract-->  | stg.archive_A       |        |                  |
+------------+               | (full history,       | --+--> | dim.Combined     |
                             |  DW_IsDeleted flag)  |   |    | (merged result)  |
+------------+               +---------------------+   |    +------------------+
| source_B   | --extract-->  | stg.archive_B       | --+
+------------+               | (full history,       |
                             |  DW_IsDeleted flag)  |
                             +---------------------+
```

Each archive table:
1. Receives full extracts from its source system
2. Uses `NOT MATCHED BY SOURCE` to detect hard deletes against that specific source
3. Sets `DW_IsDeleted = 1` on rows no longer present in the source

The DSA MERGE then combines the archive tables (filtering `WHERE DW_IsDeleted = 0`) to produce the final DSA table. This way:
- Hard deletes are detected per-source at the archive level
- The DSA table reflects the combined, deletion-aware state
- Each archive table's `NOT MATCHED BY SOURCE` is valid because it compares against a single, complete source

**When to use this pattern:**
- DSA table is built from 2+ source tables that are joined or unioned
- Source systems may hard-delete rows
- Individual source completeness is required for accurate delete detection

### 3.5 SQL-on-FHIR: History Table Delete Detection

For FHIR-based data pipelines that use the SQL-on-FHIR pattern (flattening FHIR resources into SQL tables), delete detection follows a different mechanism than the standard DSA MERGE approach.

FHIR servers maintain **history tables** that record all versions of a resource, including deletions. When a FHIR resource is deleted, the history table receives an entry with a `request.method = DELETE` marker. This history is the authoritative source for detecting deletes.

**Pattern for SQL-on-FHIR delete handling:**
- When updating the SQL base tables from the FHIR server, query the FHIR history endpoint or history tables for resources that have been deleted since the last sync
- For each deleted resource found in history, set `DW_IsDeleted = 1` in the corresponding SQL-on-FHIR base table
- `DW_Timestamp_Update` must be updated to ensure downstream watermark-based syncs capture the deletion

**Note:** This is specific to FHIR resource pipelines (e.g., `DSA/FHIR/`). Standard ETL pipelines (erp) use the MERGE-based soft-delete pattern described in section 2.

---

## 4. Tiered Orphan Detection Strategy

Different table types require different approaches based on their extraction pattern, size, and delete likelihood. The strategy uses four tiers of increasing cost and scope.

### 4.1 Overview

```
Tier 1: MERGE-integrated          Tier 2: Partition-scoped         Tier 3: PK Reconciliation
(dims/maps, every sync)           (facts, every sync)              (large facts, scheduled)
+----------------------+          +----------------------+          +----------------------+
| MERGE ... AS tgt     |          | MERGE ... AS tgt     |          | Count comparison     |
| USING #source AS src |          | USING #source AS src |          | (source vs DSA)      |
| ...                  |          | ...                  |          | If mismatch:         |
| WHEN NOT MATCHED     |          | WHEN NOT MATCHED     |          |   Chunked PK EXCEPT  |
|   BY SOURCE          |          |   BY SOURCE          |          |   Soft-delete orphans|
|   SET IsDeleted = 1  |          |   AND PartitionDate  |          +----------------------+
+----------------------+          |     BETWEEN @s AND @e|
                                  |   SET IsDeleted = 1  |          Tier 4: No detection
                                  +----------------------+          (append-only facts)
                                                                    Documented decision
```

### 4.2 Tier 1: MERGE-Integrated Soft-Delete (Dimensions and Maps)

**Scope:** All dimension and map tables where the MERGE source represents the full current state.
**Frequency:** Every sync cycle.
**Cost:** Zero additional cost -- part of the existing MERGE operation.
**Pattern:** See section 2.2.

**Applicable tables:**

| Table | Domain | Rationale |
|-------|--------|-----------|
| dim.Kreditor | erp | Full staging snapshot |
| dim.Vare | erp | Full staging snapshot |
| dim.Kontoplan | erp | Full staging snapshot |
| dim.FakturaEgenskaber | erp | Full staging snapshot |
| dim.Fakturalinje | erp | Full staging snapshot |
| dim.Organisation | organization | Full extract |
| ref.Patient_Borger | shared_dimensions | MERGE + cascade soft-delete |
| ref.Patient_Address_Borger | shared_dimensions | MERGE + patient-scoped soft-delete |
| map.Organisation | organization | Full extract |
| All map.* tables | erp | Full staging snapshot |

**Note:** `dim.Borger` (Type 2 SCD) now uses MERGE keyed on `DW_SK_Borger` (carried from `ref.Patient_Address_Borger`). Soft-delete is handled via NOT MATCHED BY SOURCE with `DW_SK_Borger > 0` guard (excludes sentinel rows). Threshold: `core.etl_thresholds` (1000 rows).

### 4.3 Tier 2: Partition-Scoped Soft-Delete (Fact Tables, Active Partitions)

**Scope:** Fact tables loaded incrementally by partition, within the currently loaded partition range.
**Frequency:** Every sync cycle (for the active partition range only).
**Cost:** Minimal -- only checks rows within the partition window.

```sql
-- Partition-scoped soft-delete for fact tables
MERGE [fct].[Fakturalinje] AS tgt
USING #source AS src
ON tgt.DW_EK_Fakturalinje = src.DW_EK_Fakturalinje

WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED BY TARGET THEN INSERT ...

-- Only check rows within the same partition range
WHEN NOT MATCHED BY SOURCE
    AND tgt.DW_PartitionDate >= @partition_start
    AND tgt.DW_PartitionDate <= @partition_end
    AND tgt.DW_IsDeleted = 0
THEN
    UPDATE SET
        tgt.DW_IsDeleted = 1,
        tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0));
```

This catches deletions within recently reprocessed partitions but not in stale partitions. Stale partitions are addressed by Tier 3.

**Applicable tables:**

| Table | Domain | Partition Column |
|-------|--------|-----------------|
| fct.Fakturalinje | erp | DW_PartitionDate |
| fct.Postering | erp | DW_PartitionDate |


### 4.4 Tier 3: PK Reconciliation Job (Large Fact Tables, Stale Partitions)

**Scope:** Large fact tables where stale partitions may hide source deletions.
**Frequency:** Weekly or monthly (configurable per table).
**Cost:** Low to moderate -- PK-only comparison, not full row comparison.

This is a separate scheduled Dagster job that detects orphaned rows in fact tables by comparing only primary keys against the source system.

#### Two-Phase Approach

```
Phase 1: Count Comparison (sub-second, every run)
+--------------------+          +--------------------+
| Source COUNT(*)    |   vs     | DSA COUNT(*)       |
| FROM source_table  |          | WHERE IsDeleted=0  |
| = 12,345,678       |          | = 12,345,702       |
+--------------------+          +--------------------+
                                     24 orphans
If DSA active count > source count -> proceed to Phase 2
If DSA active count <= source count -> no orphans, skip (fast exit)
```

```
Phase 2: Chunked PK Comparison (only when counts differ)
+----------------------------------------------------------+
| Split PK range into chunks of N rows (e.g. 1,000,000)   |
| Compare COUNT(*) per chunk between source and DSA        |
|                                                          |
| Chunk 0-999999:     source=999,998  DSA=1,000,000  DIFF |
| Chunk 1000000-1999999: source=1,000,000  DSA=1,000,000  |
| Chunk 2000000-2999999: source=1,000,000  DSA=1,000,000  |
| ...                                                      |
| Only EXCEPT on mismatched chunks:                        |
|   SELECT PK FROM DSA WHERE PK BETWEEN 0 AND 999999      |
|   EXCEPT                                                 |
|   SELECT PK FROM source WHERE PK BETWEEN 0 AND 999999   |
|   -> Returns orphaned PKs                                |
+----------------------------------------------------------+
```

#### Cost Analysis

For a 100M row table with INT primary key:

| Phase | Operation | Data Transferred | Time Estimate |
|-------|-----------|-----------------|---------------|
| **Phase 1** | Two `COUNT(*)` queries | ~0 bytes (index metadata) | < 1 second |
| **Phase 2a** | Count per chunk (100 chunks) | ~0 bytes per chunk | 5-10 seconds |
| **Phase 2b** | PK EXCEPT on mismatched chunks | ~4MB per chunk (1M x 4 bytes) | 2-5 seconds per chunk |
| **Total (worst case)** | If 3 of 100 chunks differ | ~12 MB | < 30 seconds |

Compare to full table scan: ~50 GB transferred, 10+ minutes.

#### Implementation Pattern

```python
@asset(
    key=AssetKey(["dsa", "pk_reconciliation"]),
    group_name="dsa_maintenance",
    compute_kind="mssql",
    description="Detect orphaned rows in fact tables via PK reconciliation against source",
)
def pk_reconciliation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    mysupply_mssql: MySupplyMSSQLResource,
):
    """PK reconciliation for fact tables with stale partitions."""
    reconciliation_configs = [
        {
            "dsa_table": "fct.Fakturalinje",
            "dsa_pk": "DW_EK_Fakturalinje",
            "source_resource": mysupply_mssql,
            "source_table": "dbo.statistic_invoice_line",
            "source_pk": "Id",
            "chunk_size": 1_000_000,
            "safety_threshold": 0.10,  # Abort if > 10% would be soft-deleted
        },
        # ... additional tables
    ]

    for config in reconciliation_configs:
        # Phase 1: Count comparison
        source_count = query_count(config["source_resource"], config["source_table"])
        dsa_active_count = query_count(
            mssql_target_staging,
            config["dsa_table"],
            where="DW_IsDeleted = 0",
        )

        if dsa_active_count <= source_count:
            context.log.info(
                f"{config['dsa_table']}: No orphans detected "
                f"(source={source_count}, DSA active={dsa_active_count})"
            )
            continue

        orphan_estimate = dsa_active_count - source_count
        context.log.warning(
            f"{config['dsa_table']}: ~{orphan_estimate} potential orphans detected"
        )

        # Safety threshold check
        if orphan_estimate / dsa_active_count > config["safety_threshold"]:
            context.log.error(
                f"Safety threshold exceeded for {config['dsa_table']}: "
                f"{orphan_estimate}/{dsa_active_count} = "
                f"{orphan_estimate/dsa_active_count:.1%}. Skipping."
            )
            continue

        # Phase 2: Chunked PK comparison
        orphaned_pks = chunked_pk_except(
            dsa_resource=mssql_target_staging,
            source_resource=config["source_resource"],
            dsa_table=config["dsa_table"],
            dsa_pk=config["dsa_pk"],
            source_table=config["source_table"],
            source_pk=config["source_pk"],
            chunk_size=config["chunk_size"],
        )

        # Soft-delete orphaned rows
        if orphaned_pks:
            soft_delete_rows(
                mssql_target_staging,
                config["dsa_table"],
                config["dsa_pk"],
                orphaned_pks,
            )
            context.log.info(
                f"Soft-deleted {len(orphaned_pks)} orphaned rows in {config['dsa_table']}"
            )
```

#### Source System Access

PK reconciliation requires querying the source systems directly. Available source resources:

| Source System | Resource | Tables |
|---------------|----------|--------|
| MySupply | `mysupply_mssql` (MySupplyMSSQLResource) | fct.Fakturalinje source |
| Sydax | `sydax_mssql` (SydaxMSSQLResource) | fct.Postering source |


PK queries against source systems are lightweight index scans with minimal impact on the source.

### 4.5 Tier 4: No Detection (Append-Only Fact Tables)

**Scope:** Fact tables where source systems effectively never delete historical rows.
**Frequency:** N/A
**Cost:** Zero.

Many fact tables represent immutable historical events (financial transactions, postings). Source systems typically correct errors via reversal entries, not deletions. For these tables, soft-delete detection adds no value.

**Decision per table (to be validated per domain):**

| Table | Likely Delete Behavior | Recommended Tier |
|-------|----------------------|-----------------|
| fct.Fakturalinje | Invoices may be cancelled but rarely hard-deleted | Tier 2 + Tier 3 |
| fct.Postering | Financial postings are immutable; corrections via reversal | Tier 4 (no detection) |


These assessments should be validated with domain experts during implementation.

### 4.6 Tier Summary

| Tier | Detection Method | Frequency | Cost | Applicable Tables |
|------|-----------------|-----------|------|-------------------|
| **1** | `NOT MATCHED BY SOURCE` in MERGE | Every sync | Zero (part of MERGE) | All dims, all maps |
| **2** | Partition-scoped `NOT MATCHED BY SOURCE` | Every sync | Minimal | ERP facts (active partition range) |
| **3** | PK count comparison + chunked EXCEPT | Weekly/monthly | Low-moderate | ERP facts (stale partitions) |
| **4** | None (documented decision) | N/A | Zero | Append-only facts (e.g., fct.Postering) |

---

## 5. DDL Changes

### 5.1 Add `DW_IsDeleted` Column

All DSA tables that participate in DSO sync need the `DW_IsDeleted` column. This must be added to the DDL files in `infrastructure/ddl/`.

```sql
-- Pattern for adding DW_IsDeleted to existing tables
ALTER TABLE [dim].[Kreditor] ADD [DW_IsDeleted] BIT NOT NULL DEFAULT 0;
ALTER TABLE [dim].[Vare] ADD [DW_IsDeleted] BIT NOT NULL DEFAULT 0;
-- ... repeat for all affected tables

-- For new tables, include in CREATE TABLE:
CREATE TABLE [dim].[Example] (
    DW_EK_Example       INT             NOT NULL,
    -- ... business columns ...
    DW_IsDeleted        BIT             NOT NULL DEFAULT 0,
    DW_Timestamp_Insert DATETIME2(0)    NULL,
    DW_Timestamp_Update DATETIME2(0)    NULL,
    CONSTRAINT PK_Example PRIMARY KEY (DW_EK_Example)
);
```

### 5.2 DSO Table Mirroring

DSO tables must also have the `DW_IsDeleted` column to mirror DSA. The same ALTER TABLE pattern applies to the DSO DDL files.

### 5.3 Index Consideration

For tables where consumers frequently filter on `DW_IsDeleted = 0`, consider a filtered index:

```sql
-- Optional: Filtered index for active rows (on tables with significant delete volume)
CREATE NONCLUSTERED INDEX IX_Kreditor_Active
ON [dim].[Kreditor] (DW_EK_Kreditor)
WHERE DW_IsDeleted = 0;
```

---

## 6. Implementation Plan

### Phase 0: Prerequisites
- DSO watermark-based delta sync must be implemented first (see [DSO Delta Load Strategy](architecture/dso_delta_load_strategy.md))
- This ensures that when `DW_IsDeleted` changes propagate to DSO, the watermark mechanism is in place to capture them

### Phase 1: DDL Changes
1. Add `DW_IsDeleted BIT NOT NULL DEFAULT 0` to all DSA table DDLs in `infrastructure/ddl/`
2. Add the same column to all corresponding DSO table DDLs
3. Run infrastructure assets to apply schema changes

### Phase 2: Tier 1 -- Dimension and Map Table Rollout
1. Update dimension and map MERGE statements to include:
   - `WHEN NOT MATCHED BY SOURCE AND tgt.DW_IsDeleted = 0 THEN UPDATE SET DW_IsDeleted = 1, DW_Timestamp_Update = ...`
   - `DW_IsDeleted = 0` in the `WHEN MATCHED` clause (reset on reappearance)
   - Safety threshold check before MERGE (see Appendix A)
2. Start with a pilot table (e.g., `dim.Kreditor`)
4. Validate: verify soft-deleted rows appear with correct `DW_Timestamp_Update` and propagate to DSO

### Phase 3: Tier 2 -- Partition-Scoped Fact Table Rollout
1. Update erp fact table MERGE statements with partition-scoped `NOT MATCHED BY SOURCE`
2. Validate partition boundaries are correctly applied

### Phase 4: Tier 3 -- PK Reconciliation Job
1. Implement the `pk_reconciliation` Dagster asset in `DSA/SQL/maintenance/`
2. Configure reconciliation for each applicable fact table (source system, PK mapping, threshold)
3. Create a scheduled Dagster job (e.g., weekly for fct.Fakturalinje)
4. Validate: inject a test deletion in source, verify detection and soft-delete in DSA

### Phase 5: Tier Assessment and Documentation
1. For each fact table, validate the recommended tier with domain experts
2. Document tier assignments and rationale
3. For Tier 4 tables, explicitly document the decision to skip delete detection

### Phase 6: DSO View Layer Updates
1. Update DSO views to filter `WHERE DW_IsDeleted = 0` where appropriate
2. Some reporting views may want to include deleted rows (e.g., for audit) -- this is a consumer-level decision

---

## 7. Risks and Mitigation

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| `NOT MATCHED BY SOURCE` on partial source incorrectly marks rows as deleted | Medium | High | Only use on tables where MERGE source represents full current state (see staging completeness analysis in section 3). Add `DW_IsDeleted = 0` filter to prevent re-marking. |
| Large number of rows soft-deleted on first run | Medium | Low | Expected behavior on first run. Safety threshold (Appendix A) prevents runaway deletions. |
| Source system temporarily missing data (e.g., during maintenance) | Low | High | Safety threshold aborts if > X% would be soft-deleted. PK reconciliation (Tier 3) also has threshold. |
| PK reconciliation count comparison gives false negative | Low | Low | Possible if equal number of inserts and deletes cancel out. Tier 2 (partition-scoped) catches most cases. Full PK EXCEPT can be run periodically as a deeper check. |
| Source system PK does not map directly to DSA PK | Low | Medium | PK reconciliation config must map source PK to DSA PK correctly. Requires understanding the map table join path for each fact table. |
| `DW_IsDeleted` column not added to DSO tables | Low | Medium | Schema validation in `sync_utils.py` will catch mismatches between DSA and DSO schemas. |

---

## 8. Related Documents

| Document | Relationship |
|----------|-------------|
| [DSO Delta Load Strategy](architecture/dso_delta_load_strategy.md) | Downstream consumer of `DW_IsDeleted` -- propagates via watermark |
| [SQL etl Guide](implementation/sql_etl_guide.md) | DWH architecture and ETL patterns |

---

## Appendix A: Safety Threshold Pattern

To prevent accidental mass soft-deletion (e.g., when the source is temporarily empty due to maintenance), implement a safety check before the MERGE:

```sql
-- Count rows that would be soft-deleted
DECLARE @total_active INT, @would_delete INT;

SELECT @total_active = COUNT(*) FROM [dim].[Kreditor] WHERE DW_IsDeleted = 0;

SELECT @would_delete = COUNT(*)
FROM [dim].[Kreditor] tgt
WHERE tgt.DW_IsDeleted = 0
  AND NOT EXISTS (
      SELECT 1 FROM #source src WHERE src.DW_EK_Kreditor = tgt.DW_EK_Kreditor
  );

-- Abort if more than 10% would be soft-deleted (configurable threshold)
IF @total_active > 0 AND CAST(@would_delete AS FLOAT) / @total_active > 0.10
BEGIN
    RAISERROR('Safety threshold exceeded: %d of %d active rows (%d%%) would be soft-deleted. Aborting.', 16, 1,
              @would_delete, @total_active, @would_delete * 100 / @total_active);
    RETURN;
END
```

This threshold should be configurable per table, as some tables may legitimately have high delete rates.

## Appendix B: Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-02-11 | - | Initial version |
| 1.1 | 2026-02-11 | - | Added staging completeness analysis (section 3), tiered orphan detection strategy (section 4) with PK reconciliation job design, expanded implementation plan |
| 1.2 | 2026-02-11 | - | Added multi-source archive table pattern (section 3.4) and SQL-on-FHIR history table delete detection (section 3.5) |
