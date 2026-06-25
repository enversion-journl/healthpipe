<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DWH ETL Pipeline Architecture

This document describes the Data Warehouse (DWH) ETL pipeline architecture used for business domains that do not require FHIR representation (e.g., Finance/Procurement). It is intended for both human developers and LLM assistants that need to understand or extend the pipeline.

## Overview

The DWH pipeline transforms source data through a series of layers, from raw extraction to final presentation views. The architecture emphasizes:

- **CleanCode-based relationship resolution** - Business keys are standardized into CleanCodes, then resolved to surrogate keys (EK) via map tables
- **Late surrogate key resolution** - For Type 2 dimensions, surrogate keys (SK) are resolved in the view layer to simplify historical updates
- **Staging-driven updates** - All staging tables contain exactly the columns needed for target tables (no extra columns)

## Data Lineage Documentation (Required)

For all DSO-synced `dim.*` and `fct.*` targets, lineage documentation is mandatory.

- Always document lineage using the minimal YAML structure (`dso_lineage` root).
- Store files by schema in `dagster_etl/data_lineage/dim/` and `dagster_etl/data_lineage/fct/`.
- Use compact mappings (`from`, `coalesce`, `lookup`, `expr`) with explicit source references.
- Update lineage YAML whenever source mappings, keys/lookups, or transformation logic changes.

Reference: `dagster_etl/docs/implementation/data_lineage_guide.md`.

## Pipeline Layers

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              STAGING DATABASE                                            │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌───────┐│
│  │   ext    │──>│   arc    │──>│  pre_stg │──>│   stg    │──>│   map    │──>│ dim   ││
│  │ (DSI)    │   │(optional)│   │  (DSA)   │   │  (DSA)   │   │  (DSA)   │   │ fct   ││
│  │          │   │          │   │          │   │          │   │          │   │ (DSA) ││
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └───┬───┘│
│       ↑                                                                         │     │
│    Source                                                                        │     │
└─────────────────────────────────────────────────────────────────────────────────│─────┘
                                                                                  │
┌─────────────────────────────────────────────────────────────────────────────────│─────┐
│                              DSO / DPA DATABASE                                 ↓     │
│  ┌──────────────────┐    ┌────────────────────────┐    ┌────────────────────────────┐ │
│  │     dim/fct      │───>│     vw_dim/vw_fct      │───>│      Datamart Views        │ │
│  │   (DSO Sync)     │    │       (Views)          │    │   tabel_datamart_<navn>    │ │
│  └──────────────────┘    └────────────────────────┘    └────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────────────────────┘
```

### Layer Descriptions

| Layer | Location | Purpose |
|-------|----------|---------|
| **ext** | `DSI/oracle/`, `DSI/mssql/`, `DSI/connectors/` | 1:1 raw extraction from source systems to `ext.*` schema (configs under `DSI/oracle/` and `DSI/mssql/`; shared machinery under `DSI/connectors/`). No business logic or CleanCode generation. Only adds `SourceSystem` as technical ETL metadata. See [Extract Layer Patterns](#extract-layer-patterns). |
| **arc** (optional) | `DSI/mssql/archive/` | Persistent archive of extracted data. 1:1 reflection of source table plus `DW_Timestamp_Insert`/`DW_Timestamp_Update`. When enabled, downstream reads from `arc.*` instead of `ext.*`. See [Archive Layer](#archive-layer-optional). |
| **pre_stg** | `DSA/SQL/staging/` | Complex joins, deduplication, and CleanCode generation. Reads from `arc.*` (if archive enabled) or `ext.*`, writes to `stg.*`. |
| **stg** | `DSA/SQL/staging/` | Entity-level staging with bk_* columns. Reads from `stg.pre_*` or `ext.*`, writes to `stg.dim_*` / `stg.fct_*`. |
| **map** | `DSA/SQL/map/` | Mapping tables that translate CleanCodes to surrogate keys (DW_EK). One map table per dimension and per fact. |
| **dim / fct** | `DSA/SQL/dim/`, `DSA/SQL/fct/` | Final dimension and fact tables with resolved EK values. |
| **DSO Sync** | DSO / DPA Database | Dimensions and facts synced from DSA to DSO via linked-server MERGE (see `docs/implementation/dso_sync_guide.md`). Map tables are also synced. |
| **Views** | DSO / DPA Database | Presentation views that resolve SK keys for Type 2 dimensions and complex fact-to-fact relationships. |

## Key Architectural Decisions

### CleanCode Resolution Strategy

CleanCodes are resolved to EK values in the fact update step (`FctUpd`). This means:
- Staging tables contain CleanCodes (not EKs)
- Map tables translate CleanCodes → EKs
- Fact tables store resolved EK values

**Exception for complex relationships**: Fact-to-fact relationships or other complex relationships may remain as CleanCodes in DSA tables and be resolved in views (if performance allows). This is useful for:
- Relationships that may change over time (e.g., encounter matching)
- Intermediate tables with references that shift

### Type 2 Dimension Handling

For Type 2 (slowly changing) dimensions like `dim_borger`:
- **DW_EK** is stored in fact tables
- **DW_SK** (surrogate key for specific version) is resolved in the view layer

This simplifies handling of historical updates to dimensions, as the EK remains stable while SK versions change.

### Update Dependencies

Fact updates should depend on both dimension and map updates. This ensures:
1. The dimension is fully updated
2. EK values from map tables reference valid dimension rows

## DSI/DSA Contract

### DSI (Data Staging In) — ext-only
- Writes **only** to `ext.*` schema
- 1:1 extraction: every ext table maps to exactly one source table
- No business logic: no CleanCode generation, no joins, no aggregation
- Only adds technical ETL metadata: `SourceSystem`
- Asset keys use `ext_*` prefix (e.g., `ext_finanspostering_sydax_daily`)
- Group names: `erp_extract`, `erp_extract_daily`, `erp_extract_monthly`
- Code location: `DSI/mssql/` (MSSQL sources), `DSI/oracle/` (IFDB), with shared extraction in `DSI/connectors/`

#### Approved DSI Exceptions

**`ext.Finanspostering_enriched`** (`DSI/mssql/sydax_enriched.py`):
This asset performs multi-table JOINs directly against SydAX source tables and
writes enriched output to `ext.Finanspostering_enriched`. The exception exists
because the Postering pipeline requires cross-schema joins (Finans + Kreditor)
and master-data lookups that cannot be expressed via the standard `TableConfig`
factory. ALL data — transactional rows AND master-data lookups — is fetched
directly from the SydAX source system; no `[ext].*` tables are read by this asset.
Business key: `bk_ref_LedgertransId`. Grain: `ref_LedgertransId`.

**`ext.budgetpost_enriched`** (`DSI/mssql/sydax_enriched_budget.py`):
This asset performs multi-table JOINs directly against SydAX source tables and
writes the enriched result to `ext.budgetpost_enriched`. The exception exists
because the Budget pipeline requires cross-schema joins (Budgetpost + Budgetmodel
+ Finanskonto + OmkostningsstedHierarki + Hovedkonto + Gruppering + Art + Funktion
+ Dranst + BudgetpostStatus) that cannot be expressed via the standard `TableConfig`
factory. ALL data is fetched directly from the SydAX source system; no `[ext].*`
tables are read by this asset.
Business key: `ref_LedgerbudgetId`. Grain: `ref_LedgerbudgetId`.

**`ext.Finanskladdelinje_enriched`** (`DSI/mssql/sydax_enriched_finanskladde.py`):
This asset performs multi-table JOINs directly against SydAX source tables and
writes enriched output to `ext.Finanskladdelinje_enriched`. The exception exists
because the Finanskladdelinje pipeline requires resolving the unstable
`FinanskladdeId` IDENTITY FK into the stable `ref_LedgerjournaltableId`, plus
Finanskonto/Kontoplan and Omkostningssted hierarchy lookups and
Finanstransaktionstype resolution — all of which cannot be expressed via the
standard `TableConfig` factory. ALL data — transactional rows AND master-data
lookups — is fetched directly from the SydAX source system; no `[ext].*` tables
are read by this asset.
Business key: `bk_ref_LedgertransId`. Grain: `ref_LedgertransId`.

### DSA (Data Staging Area) — staging and transforms
- Owns **all** pre_stg, stg, map, dim, and fct assets
- Reads from `ext.*` tables, writes to `stg.*` tables
- Responsible for: CleanCode generation, cross-source joins, deduplication, bk_* column creation
- Asset keys: `pre_stg_*`, `stg_dim_*`, `stg_fct_*` (in `erp_staging` group)
- Code location: `DSA/SQL/staging/`
- CleanCode builders: `DSA/SQL/staging/clean_codes.py`

#### DSA map/dim/fct
- Owns **all** map, dim, and fct update assets
- Reads from `stg.*` tables, writes to `map.*`, `dim.*`, and `fct.*` tables
- Responsible for: CleanCode → EK resolution via map tables, dimension/fact MERGE updates
- Asset keys: `map_*`, `dim_*`, `fct_*` (in `erp_maps`, `erp_dimensions`, `erp_facts` groups)
- Code location: `DSA/SQL/map/`, `DSA/SQL/dim/`, `DSA/SQL/fct/`

## Schema Reference Rules (Critical)

> **NEVER reference `[stg].[raw_table]` for raw extracted data.** Raw data lives exclusively in the `ext.*` schema.

The `stg.*` schema is reserved for **DSA-produced output** only:
- `stg.pre_*` — output of pre-staging assets
- `stg.dim_*` — output of dimension staging assets
- `stg.fct_*` — output of fact staging assets

### Allowed read patterns by layer

| SQL Layer | Reads FROM | Writes TO |
|-----------|-----------|-----------|
| **pre_stg** | `[arc].[table_name]` (if archive enabled) or `[ext].[table_name]` | `[stg].[pre_table_name]` |
| **stg_dim / stg_fct** | `[stg].[pre_*]` or `[arc].[table_name]` or `[ext].[table_name]` | `[stg].[dim_*]` / `[stg].[fct_*]` |
| **map / dim / fct update** | `[stg].[dim_*]`, `[stg].[fct_*]`, `[map].[*]` | `[map].[*]`, `[dim].[*]`, `[fct].[*]` |

### CleanCode generation in pre_stg SQL

The `ext.*` tables contain raw source data only — **no CleanCode columns**. CleanCodes must be generated **inline in the pre-staging SQL** using CASE/CONCAT expressions. Examples:

```sql
-- CVR CleanCode from SydAX JuridiskEnhed
CASE
    WHEN kred.JuridiskEnhed IS NOT NULL AND LTRIM(RTRIM(kred.JuridiskEnhed)) <> ''
    THEN 'CVR_' + LTRIM(RTRIM(kred.JuridiskEnhed))
    ELSE NULL
END AS CleanCode_Kreditor

-- Kontoplan CleanCode from FinanskontoId
'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(FinanskontoId AS NVARCHAR(100)), ' ', ''))))
    AS Kontoplan_CleanCode
```

The CleanCode builder definitions in `DSA/SQL/staging/clean_codes.py` document the canonical business rules. When writing pre_stg SQL, replicate these rules as SQL CASE expressions.

## Extract Layer Patterns

The extract layer writes to `ext.*` schema (not `stg.*`). Downstream pre_stg and stg assets read from `ext.*` and write to `stg.*`.

### Shared connector layer (`DSI/connectors/`)

All extraction domains delegate to shared connector code under `DSI/connectors/` (and per-connector packages `DSI/connectors/mssql/`, `DSI/connectors/oracle/`):

- **`ExtractionConfig`** — protocol that domain-specific config classes must satisfy (requires `description` and `partition_column` fields)
- **`PartitionType`** — enum with values `DAILY`, `MONTHLY`, `FULL_LOAD`
- **`TableType`** — enum with values `MASTER_DATA`, `MASTER_DATA_PARTITIONED`, `TRANSACTIONAL`, `DEPENDENT`, `BRIDGE`
- **`create_extraction_assets()`** — generic factory that generates daily, monthly, and/or full_load partition variants for any extraction domain

Each domain provides an **adapter** that wraps the generic factory with domain-specific resource types and loading logic. See `DSI/connectors/mssql/asset_factory.py` (ERP/MSSQL) and `DSI/connectors/oracle/asset_factory.py` (IFDB/Oracle) for concrete implementations.

### Extraction Config

Domain-specific config classes describe how a source table is staged. They must satisfy the `ExtractionConfig` protocol (provide `description` and `partition_column`). Common fields across domains:

| Field | Purpose |
|-------|---------|
| `description` | Human-readable description of the extraction |
| `partition_column` | Date column for partition filtering (`None` for full-load-only tables) |
| `table_type` | Categorizes partition behaviour (see Table Types below) |

Individual domains add source-specific fields (e.g., `source_schema`/`source_table` for MSSQL, `table`/`columns` for Oracle).

### Table Types and Partition Behaviour

| Table Type | Partition Behaviour |
|------------|-------------------|
| `master_data` | Full load only (no date filtering) |
| `master_data_partitioned` | Filter on one or more date columns (OR-combined) |
| `transactional` | Filter on a single partition date column |
| `dependent` | Subquery filter via parent transactional table's partition column |
| `bridge` | Same as dependent |

### Partition Filtering Rules

- **Normal partitions:** filter on the partition date column (daily = equality, monthly = range).
- **First partition** (key == configured `start_date`): use **upper bound only** (`<= end`) so all pre-start historical data is included.
- **Scheduled catchup** (e.g., `erp_extraction_mode=scheduled_catchup`): overrides both rules with a rolling time window.
- **Dependent/bridge tables:** use subquery against the parent transactional table's partition column.
- **Full load** (`partition_type == "full_load"`): no date filtering — extracts the entire table.

Start dates and the `is_first_partition()` helper live in `core/partitions.py` (`PARTITION_START_KEYS`).

### Source-Specific Hooks

Domain adapters may apply source-specific data cleaning. For example, the ILS source system applies `_clean_ils_data()` to fix data type conversion issues (trailing `.0` in numeric strings, float-to-int normalization). Hooks are applied automatically in the domain adapter's loader.

## Pre-Staging Layer: First-Partition Hardening

First partitions load **all historical data** (upper-bound-only filter), producing 10-100x larger volumes than incremental runs. Pre-staging assets perform full rebuilds (`DROP TABLE IF EXISTS` + `SELECT INTO`) over this data.

### Null-Safe Diagnostics

Diagnostic queries on empty result sets must handle SQL `NULL` aggregates:

```python
# SQL: use COALESCE to avoid NULL from SUM on empty tables
COALESCE(SUM(CASE WHEN col IS NULL THEN 1 ELSE 0 END), 0) AS null_count

# Python: guard against None from fetchone()
null_count = stats[idx] or 0
if null_count > 0:
    context.log.warning(...)
```

### Monitoring

- Log row counts before and after dedup to detect unexpected empty results.
- First-partition pre-staging may take minutes rather than seconds.

## Naming Conventions

### CleanCodes

```
CleanCode_<table>_<specifier/role>_<source>
```

Examples:
- `CleanCode_Kreditor_CVR_ILS`
- `CleanCode_Vare_Varenummer`

### Surrogate Keys

```
DW_EK_<table>_<role>     -- Entity Key (stable identifier)
DW_SK_<table>            -- Surrogate Key (for Type 2 versioning)
```

Examples:
- `DW_EK_Org_SOR`
- `DW_EK_Dato_Bogfoering`

### Business Keys

```
bk_<key_name>
```

Use source system column names as the default. Add context when source names are ambiguous (e.g., many tables have `objekt_id`):
- `bk_ref_LedgertransId`
- `bk_encounter_objekt_id`
- `bk_laftd_lnr` (leverandoeraftale_linjenr)

For composite business keys in facts, use the `bk_` prefix for each component:
- `bk_FakturaLines`, `bk_FakturaHeader`, `bk_FakturaMetadata`

### Table Names

Use singular form for facts:
- `fct.kontakt`
- `fct.medicinordination`
- `fct.fakturalinje`
- `fct.vareaftale`

## Standard DWH Columns

### Audit Columns (All Tables)

| Column | Purpose |
|--------|---------|
| `DW_Timestamp_Insert` | Timestamp of first insert |
| `DW_Timestamp_Update` | Timestamp of last update (equals Insert on first insert) |

### Fact-Specific Columns

| Column | Purpose |
|--------|---------|
| `DW_haendelsestidspunkt` | Event timestamp for resolving SK in Type 2 dimension lookups |
| `DW_IsDeleted` | Soft delete marker |

### Type 2 Dimension Columns

| Column | Purpose |
|--------|---------|
| `DW_ValidFrom` | Version validity start |
| `DW_ValidTo` | Version validity end |
| `DW_IsDeleted` | Soft delete marker |

## Table Structures

### Staging Tables (stg)

```sql
-- Columns should contain ONLY what goes into the target table
CleanCode_<table>               -- Primary CleanCode
bk_<key1>                       -- Business key component(s)
bk_<key2>
CleanCode_<related_table>_<role> -- Related table CleanCodes
<attributes>                    -- For dimensions
<measures>                      -- For facts
DW_haendelsestidspunkt          -- For facts with Type 2 dimension relations
```

#### Example

```sql
-- Example: stg.fct_postering_sydax staging table
CREATE TABLE [stg].[fct_postering_sydax] (
    -- Primary CleanCode (grain = Finanspostering)
    CleanCode_Postering             NVARCHAR(100) NOT NULL,

    -- Business keys for traceability
    ref_VendTransId                 BIGINT NULL,

    -- Related table CleanCodes (for map table lookups)
    CleanCode_Kreditor                 NVARCHAR(100) NULL,
    CleanCode_Kontoplan                NVARCHAR(100) NULL,
    CleanCode_Faktura                  NVARCHAR(100) NULL,

    -- Related table CleanCodes with role (for map table lookups)
    CleanCode_Organisation_Leverandoer NVARCHAR(100) NULL,
    CleanCode_Organisation_Kunde       NVARCHAR(100) NULL,

    -- Measures
    Posteringsbeloeb                DECIMAL(18,2) NULL,
    Transaktionsbeloeb              DECIMAL(18,2) NULL,

    -- Event timestamp for Type 2 dimension lookups
    DW_haendelsestidspunkt          DATETIME2 NULL
);
```

### Map Tables

```sql
DW_EK_<table>     IDENTITY NOT NULL  -- Auto-generated surrogate key
CleanCode_<...>                      -- Indexed for lookup performance
bk_<key1>                            -- Business key
bk_<key2>
```

Every non-system column in a map table MUST have one of these prefixes: `DW_*`, `CleanCode_*`, or `bk_*`.

#### Example

```sql
-- Example: map.Kontoplan map table
CREATE TABLE [map].[Kontoplan] (
    [DW_EK_Kontoplan] INT IDENTITY(1,1) NOT NULL,
    [CleanCode_Kontoplan] NVARCHAR(255) NOT NULL,

    -- Business keys
    [bk_FinanskontoId] BIGINT NOT NULL,
    [bk_Kontonummer] NVARCHAR(255) NOT NULL,

    [DW_Timestamp_Insert] DATETIME2 NULL,
    [DW_Timestamp_Update] DATETIME2 NULL,
    CONSTRAINT [PK_Map_Kontoplan] PRIMARY KEY CLUSTERED ([CleanCode_Kontoplan] ASC)
);
```

#### Map Table Load Pattern

Map tables use **insert-only** logic by default:

```sql
INSERT INTO [map].[Entity] (CleanCode_Entity, DW_Timestamp_Insert)
SELECT source.CleanCode_Entity, CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[dim_entity] source
WHERE NOT EXISTS (
    SELECT 1 FROM [map].[Entity] m
    WHERE m.CleanCode_Entity = source.CleanCode_Entity
);
```

- NEVER use MERGE for pure map key creation.
- Map tables only store CleanCode-to-DW_EK mappings and optional business-key traceability columns (`bk_*`).
- Exceptions (e.g. updating a non-key column like `vnr` in `map.Vare`) must be documented in the asset docstring with justification.

### Fact Tables

```sql
DW_EK_<table>                    -- Fact's own surrogate key (IDENTITY or from map)
CleanCode_<table>                -- Canonical key (for lineage tracking)
bk_<primary_key>                 -- Primary business key (for traceability)
DW_EK_<related_dim>_<role>       -- Resolved dimension keys
<measures>
DW_Timestamp_Insert
DW_Timestamp_Update
DW_haendelsestidspunkt
DW_IsDeleted                     -- Optional
```

#### Example

```sql
-- Example: fct.Postering fact table
CREATE TABLE [fct].[Postering] (
    -- Primary business key / grain
    [ref_LedgertransId] BIGINT NOT NULL,

    -- Canonical key (from map.Postering lookup)
    [DW_EK_Postering] INT NOT NULL DEFAULT -1,
    [CleanCode_Postering] NVARCHAR(100) NULL,

    -- Resolved dimension keys (from map table lookups)
    [DW_EK_Kreditor]             INT NOT NULL DEFAULT -1,
    [DW_EK_Kontoplan]            INT NOT NULL DEFAULT -1,
    [DW_EK_Faktura]              INT NOT NULL DEFAULT -1,
    [DW_EK_Organisation]         INT    NOT NULL DEFAULT -1,

    -- Resolved date dimension keys
    [DW_EK_Dato_Godkendelse]     INT NULL,
    [DW_EK_Dato_Lukket]          INT NULL,
    [DW_EK_Dato_TransOprettet]   INT NULL,
    [DW_EK_Dato_Forfald]         INT NULL,
    [DW_EK_Dato_JournalFaktura]  INT NULL,
    [DW_EK_Dato_SidstUdlignet]   INT NULL,
    [DW_EK_Dato_Transaktion]     INT NULL,
    [DW_EK_Dato_Bogfoering]      INT NULL,
    [DW_EK_Dato_PostOprettet]    INT NULL,

    -- Measures
    [Posteringsbeloeb]           DECIMAL(30,12) NULL,
    [Transaktionsbeloeb]         DECIMAL(30,12) NULL,
    [UdlignetBeloeb]             DECIMAL(30,12) NULL,

    -- Descriptive attributes
    [FakturaNr]                  NVARCHAR(100) NULL,
    [Transaktionstekst]          NVARCHAR(MAX) NULL,
    [Posteringstekst]            NVARCHAR(MAX) NULL,

    -- Audit columns
    [DW_Timestamp_Insert]        DATETIME2 NULL,
    [DW_Timestamp_Update]        DATETIME2 NULL,

    CONSTRAINT [PK_Fct_Postering] PRIMARY KEY CLUSTERED ([ref_LedgertransId] ASC)
);
```

## Update Logic Principles

1. **Staging tables define the contract** - They contain exactly what goes into target tables, nothing more.

2. **Naming convention enables automation** - Using standard prefixes (`bk_`, `CleanCode_`, `DW_EK_`) allows:
   - Map tables to be updated with correct columns
   - All staging columns to flow to primary tables

3. **Map-then-update pattern**:
   - First update map tables with new CleanCode → EK mappings
   - Then update dimensions/facts with resolved EK values

4. **Map table loads**: Use simple `INSERT ... WHERE NOT EXISTS` by default. Avoid MERGE for insert-only map loads. Use MERGE or UPDATE only when the map must update non-key attributes for existing rows; document such exceptions in the asset docstring.

## Required Test Gates

Every new or modified NON_FHIR pipeline must pass these test gates:

### Unit Tests
- ext extraction configs use `ext_*` prefix only (no `stg_*`)
- TableConfig has no `clean_code_columns` attribute
- Partition filter logic covers daily, monthly, first-partition, and catchup modes
- CleanCode builders produce expected outputs for sample data

### Integration Tests
- ext tables contain 1:1 raw source data (no CleanCode columns)
- pre_stg/stg tables contain expected CleanCode and bk_* columns
- map/dim/fct tables are populated with correct EK resolution
- Full chain: ext -> pre_stg -> stg -> map -> dim -> fct

### Dagster End-to-End Tests
- erp_daily_etl job executes successfully with all groups
- Materialization metadata contains expected upstream lineage

## Archive Layer (Optional)

An optional persistent archive layer (`arc` schema) between `ext` and downstream staging. When enabled for a source table, it preserves all extracted data, enabling:

- **Fact reruns without hitting the source** — historical snapshots are retained in `arc`
- **Delta processing** — downstream only processes data that arrived since the last run
- **Decoupled extract from downstream** — archive is populated independently of how data flows to dimensions and facts
- **Reconciliation against source** — detect source deletions even when source doesn't use soft deletes

**Drawbacks:** Additional infrastructure to develop and maintain.

**Recommendation:** Implement for mission-critical sources, sources where history must be preserved, or sources that benefit from delta processing downstream.

### Core Principles

- `arc` tables are a **1:1 reflection of the source table** plus standard DWH columns
- `ext` tables remain volatile (DROP+CREATE each run) — they are the landing zone
- **Downstream reads from `arc`, never from `ext`** (when archive is enabled)
- Extract mode (full-load vs delta) and archive merge strategy (insert_only vs upsert) are **independent configurations per source table**

### Configuration Axes

| | Full-load extract | Delta extract |
|---|---|---|
| **insert_only archive** | Every run appends all source rows with new timestamp | Only changed/new rows appended with timestamp |
| **upsert archive** | All rows matched on business key, updated or inserted | Only changed/new rows matched and upserted |

Example combinations:

| Source table | Extract mode | Archive strategy | Rationale |
|-------------|-------------|-----------------|-----------|
| `lgrbeh` | full-load | insert_only | Current-state-only table, must accumulate snapshots |
| `vare` | full-load | upsert | Master data with stable business key (`vnr`) |
| `laftd` | delta | upsert | Transactional data with stable keys, delta-extractable |

### Standard DWH Columns for `arc` Tables

Every `arc` table includes these columns in addition to the 1:1 source columns:

| Column | Type | Purpose |
|--------|------|---------|
| `DW_Timestamp_Insert` | DATETIME2 | When this row was first inserted into the archive |
| `DW_Timestamp_Update` | DATETIME2 | When this row was last inserted or updated in the archive |

Behavior by strategy:

- **insert_only**: Both columns receive the same value on INSERT. No rows are ever updated.
- **upsert**: `DW_Timestamp_Insert` is set once on INSERT. `DW_Timestamp_Update` is refreshed on every MATCHED UPDATE.

`DW_Timestamp_Update` is the column that downstream consumers use as a watermark filter (see [Watermark & Audit System](#watermark--audit-system)).

### Naming

`arc.{source_table}_{system}` — consistent with the existing `ext.{source_table}_{system}` convention.

### Data Flow per Source Table

```
Source system
  → ext.{table}_{system}     (DROP + CREATE + load — full or delta)
  → arc.{table}_{system}     (MERGE — insert_only or upsert, independent of extract mode)
```

### Watermark & Audit System

A generalized delta tracking system that determines what data each asset needs to process. Extends the concept already used for DSO syncs (`meta.sync_watermarks` + `SyncStateManager`, see `docs/implementation/dso_sync_guide.md`) across the entire pipeline.

**Core principle:** Watermarks only exist on persistent tables. Transient tables (`ext`, `stg`) don't own watermarks — they derive their data boundaries from the persistent tables they serve.

#### `meta.etl_watermarks` — delta state

One row per tracked entity. Holds the current high-water mark.

```sql
CREATE TABLE [meta].[etl_watermarks] (
    [entity_type]      NVARCHAR(50)   NOT NULL,  -- archive, dimension, fact, dso_sync
    [schema_name]      NVARCHAR(128)  NOT NULL,  -- e.g. arc, dim, fct
    [table_name]       NVARCHAR(128)  NOT NULL,  -- e.g. lgrbeh_ils, Lagerbeholdning
    [watermark_value]  DATETIME2      NOT NULL,  -- last successfully processed timestamp
    [updated_at]       DATETIME2      NOT NULL,  -- when this watermark was last advanced
    CONSTRAINT [PK_etl_watermarks]
        PRIMARY KEY CLUSTERED ([entity_type], [schema_name], [table_name])
);
```

#### `meta.etl_audit_log` — transaction history

Append-only log of every ETL run. Never updated, only inserted.

```sql
CREATE TABLE [meta].[etl_audit_log] (
    [audit_id]          BIGINT IDENTITY(1,1) NOT NULL,
    [entity_type]       NVARCHAR(50)   NOT NULL,
    [schema_name]       NVARCHAR(128)  NOT NULL,
    [table_name]        NVARCHAR(128)  NOT NULL,
    [run_started_at]    DATETIME2      NOT NULL,
    [run_completed_at]  DATETIME2      NULL,
    [watermark_before]  DATETIME2      NULL,
    [watermark_after]   DATETIME2      NULL,
    [rows_affected]     INT            NULL,
    [status]            NVARCHAR(50)   NOT NULL,  -- success, failed, skipped
    [dagster_run_id]    NVARCHAR(128)  NULL,
    CONSTRAINT [PK_etl_audit_log]
        PRIMARY KEY CLUSTERED ([audit_id])
);
```

#### Who Reads vs. Who Advances the Watermark

The asset reading the data and the asset advancing the watermark are different assets:

| Asset | Reads watermark of | Advances watermark of |
|-------|-------------------|----------------------|
| Extract + archive merge | `archive` (to determine delta from source) | `archive` (after successful merge) |
| Staging (for a dimension) | `dimension` it feeds (to filter archive) | *Nothing* |
| Staging (for a fact) | `fact` it feeds (to filter archive) | *Nothing* |
| Dimension update | *Nothing* — uses data already in staging | `dimension` (after successful update) |
| Fact update | *Nothing* — uses data already in staging | `fact` (after successful update) |
| DSO sync | `dso_sync` | `dso_sync` |

**Staging assets know their downstream consumers** — they look up consumer watermarks to determine what to filter from the archive. If a staging table feeds both a dimension and a fact, it takes `MIN(watermark of dim, watermark of fct)` to ensure both consumers get the data they need.

**Dimension/fact assets are watermark-unaware at read time** — they process whatever is in the staging table, then advance their own watermark after success.

#### Relationship to Dagster Partitions

Dagster partitions define *which logical unit of work* to run (e.g. "2026-04-13"). The watermark system defines *what data to pull* within that run. They are complementary — the partition is the orchestration boundary, the watermark is the data boundary. Keeping the watermark in SQL makes it queryable, debuggable, and independent of Dagster's internal state.

#### Relationship to Existing `meta.sync_watermarks`

The existing DSO sync watermark table (`meta.sync_watermarks`) and `SyncStateManager` class implement the same concept for DSO syncs only. The target state either extends this to cover all entity types or replaces it with `meta.etl_watermarks` + `meta.etl_audit_log` and migrates DSO sync to use the new tables.

### Concrete Example: Lagerbeholdning

Walks through the full archive-enabled flow for `lgrbeh` (warehouse stock from ILS).

> **Note:** Lagerbeholdning runs in its own dedicated job (`lagerbeholdning_daily_job`), separate from the main `daily_etl_flow`. See [Dagster Orchestration](#dagster-orchestration--target-state) for details.

**Configuration:**

| Property | Value |
|----------|-------|
| Source table | `ILS.dbo.lgrbeh` |
| Extract mode | full-load |
| Archive strategy | insert_only |
| Archive table | `arc.lgrbeh_ils` |
| Rationale | Source contains current state only (rows updated in-place). Each extract must be preserved as a historical snapshot. |

**Phase 1 — Archive:**

1. The archive watermark for `arc.lgrbeh_ils` is checked. For full-load this does not filter the source — it always extracts everything.
2. `ext.lgrbeh_ils` is DROP+CREATE'd and loaded from the source (current behavior).
3. All rows from `ext.lgrbeh_ils` are INSERTed into `arc.lgrbeh_ils` with `DW_Timestamp_Insert` and `DW_Timestamp_Update` set to the current time.
4. The archive watermark is advanced so downstream knows what's new.

**Phase 2 — Dimensions:**

- `stg_dim_LagerbeholdningEgenskaber_ils` looks up the watermark of `dim.LagerbeholdningEgenskaber`, filters `arc.lgrbeh_ils` by `DW_Timestamp_Update > @watermark`, and produces a staging table with distinct property combinations.
- `dim_LagerbeholdningEgenskaber` reads from staging, runs Type 1 SCD MERGE, and advances its own watermark.

**Phase 3 — Facts:**

- `stg_fct_lagerbeholdning_ils` looks up the watermark of `fct.Lagerbeholdning`, filters `arc.lgrbeh_ils` by `DW_Timestamp_Update > @watermark`, groups by grain, and computes CleanCodes. The archive may contain multiple snapshots from the same day — staging picks the most recent per day using `DW_Timestamp_Update`.
- `map_Lagerbeholdning` inserts new CleanCodes into `map.Lagerbeholdning` (insert-only, assigns stable `DW_EK` via IDENTITY).
- `fct_Lagerbeholdning` reads from staging, resolves `DW_EK_Lagerbeholdning` via `map.Lagerbeholdning`, MERGEs into the fact table (MATCHED → update FKs/measures, NOT MATCHED BY TARGET → insert, NOT MATCHED BY SOURCE scoped to incoming snapshot dates → soft-delete via `DW_IsDeleted=1`), and advances its own watermark.

**Phase 4 — DSO sync:**

- `dso_sync_fct_Lagerbeholdning` and `dso_sync_dim_LagerbeholdningEgenskaber` use the existing watermark delta sync pattern.

**Shared pre-staging optimization:** If the dimension and fact share a large part of the same staging logic (both reading from `arc.lgrbeh_ils` and computing similar CleanCodes), they can share a single pre-staging table. The pre-staging asset reads `MIN(watermark of dim, watermark of fct)` from the archive, and both downstream staging assets read from the pre-staging output instead.

## Dagster Orchestration — Target State

### Two Job Types

Source tables fall into two categories that require different job structures:

**Backfillable sources** (e.g. `laftd`, `finanspostering`): Source contains historical data filterable by date. Backfill runs for past partitions retrieve actual historical data. These belong in the main `daily_etl_flow` job which supports partition-based backfilling.

**Current-state-only sources** (e.g. `lgrbeh`, `afdlgrbeh`): Source reflects only the current state — rows are updated in-place and have no date history. Running for a past date still returns today's data. Backfilling is meaningless. These require their own dedicated job with a snapshot-capture schedule.

Mixing these in a single job creates confusion: a backfill run would correctly re-extract historical transactional data but would incorrectly capture today's inventory as if it were historical.

### Job: `daily_etl_flow`

The main pipeline job. Selects all assets for backfillable source tables across five conceptual phases. Supports partition-based backfilling.

#### Asset Groups

| Phase | Group name | Example assets |
|-------|-----------|----------------|
| 1. Archive | `extract_ILS` | `ext_vare_ils`, `arc_vare_ils`, `ext_laftd_ils`, `arc_laftd_ils` |
| 1. Archive | `extract_SydAX` | `ext_finanspostering_sydax`, `arc_finanspostering_sydax` |
| 1. Archive | `extract_MySupply` | `ext_invoice_header_mysupply`, `arc_invoice_header_mysupply` |
| 2. Shared | `shared_dimensions_and_codesystems` | `dim_organisation`, `codesystem_ean` |
| 3. Dimensions | `dimensions_erp` | `dim_IlsVare`, `dim_Kreditor` |
| 4. Facts | `facts_erp` | `fct_Vareudlevering`, `fct_Fakturalinje` |
| 5. DSO sync | `dso_sync_erp` | `dso_sync_dim_IlsVare`, `dso_sync_fct_Vareudlevering` |

#### Schedule

| Property | Value |
|----------|-------|
| Name | `daily_etl_flow_schedule` |
| Frequency | Daily |
| Time | 06:00 `Europe/Copenhagen` |

### Job: `lagerbeholdning_daily_job`

Dedicated job for current-state-only inventory snapshot capture. Runs the full flow for `lgrbeh` and `afdlgrbeh` — from archive through to DSO sync. **Cannot be meaningfully backfilled** since the source always reflects current state.

#### Asset Groups

| Phase | Group name | Example assets |
|-------|-----------|----------------|
| 1. Archive | `extract_lagerbeholdning` | `ext_lgrbeh_ils`, `arc_lgrbeh_ils`, `ext_afdlgrbeh_ils`, `arc_afdlgrbeh_ils` |
| 2. Pre-staging | `pre_staging_lagerbeholdning` | `pre_stg_lgrbeh_ils`, `pre_stg_afdlgrbeh_ils` |
| 3. Dimensions | `dimensions_lagerbeholdning` | `dim_LagerbeholdningEgenskaber`, `dim_AfdelingLagerbeholdningEgenskaber` |
| 4. Facts | `facts_lagerbeholdning` | `map_Lagerbeholdning`, `map_AfdelingLagerbeholdning`, `fct_Lagerbeholdning`, `fct_AfdelingLagerbeholdning` |
| 5. DSO sync | `dso_sync_lagerbeholdning` | `dso_sync_fct_Lagerbeholdning`, `dso_sync_dim_LagerbeholdningEgenskaber` |

#### Schedule

| Property | Value |
|----------|-------|
| Name | `lagerbeholdning_daily_schedule` |
| Frequency | Daily |
| Time | 06:00 `Europe/Copenhagen` |

### Selective Execution

Individual groups can be run via `AssetSelection.groups(...)` for ad-hoc runs:

```python
# Re-run only archive layer for ILS (main flow)
erp_extract_only = define_asset_job(
    "erp_extract_only",
    selection=AssetSelection.groups("extract_ILS"),
)

# Re-run dimensions + facts without re-extracting (main flow)
erp_downstream_only = define_asset_job(
    "erp_downstream_only",
    selection=AssetSelection.groups("dimensions_erp", "facts_erp"),
)
```

Groups collapse in the Dagster UI, keeping the asset graph navigable even with many assets. No orchestration sensors or meta-jobs are needed.

## Master Data / Shared Dimensions

> **Note:** The architecture for master data and shared dimensions is not yet finalized.

The following are candidates for shared dimensions across business domains:

| Dimension | DWH Name | FHIR Equivalent |
|-----------|----------|-----------------|
| Person/Citizen | `dim_borger` | Patient |
| Staff | `dim_personale` | Practitioner |
| Organization | `dim_organization` | Organization |
| Date | `dim_dato` | (DWH only) |
| Time of Day | `dim_klok` | (DWH only) |

## View Layer Responsibilities

The view layer in DSO / DPA handles:

1. **SK resolution** - Resolve `DW_SK` for Type 2 dimensions using `DW_haendelsestidspunkt`
2. **Complex relationship resolution** - Fact-to-fact relationships that were kept as CleanCodes
3. **Filtering** - Apply `DW_IsDeleted = 0` and validity filters
4. **Datamart assembly** - Combine dimensions and facts for specific reporting needs

### View Naming

```
vw_dim_<table>              -- Dimension view
vw_fct_<table>              -- Fact view
tabel_datamart_<name>       -- Datamart-specific views
```

## Performance Guidelines for Map/Dim/Fct Updates

### Never wrap indexed columns in functions

`LTRIM(RTRIM(m.CleanCode))` in a JOIN or WHERE clause disables index seeks, forcing a full table scan. Clean data at write-time (in MapUpd INSERT) so that read-time JOINs (in DimUpd/FctUpd) can use the clustered index directly.

```sql
-- BAD: disables clustered index on map.Kreditor.CleanCode_Kreditor
LEFT JOIN [map].[Kreditor] m
    ON LTRIM(RTRIM(m.CleanCode_Kreditor)) = sk.CleanCode_Kreditor

-- GOOD: index-seekable, data already trimmed at insert time
LEFT JOIN [map].[Kreditor] m
    ON m.CleanCode_Kreditor = sk.CleanCode_Kreditor
```

### Index correlated subquery join columns

If a SELECT uses a correlated subquery against a large table (e.g., `SELECT TOP 1 ... FROM fct.Postering WHERE FakturaNr = ...`), ensure the join column has a nonclustered index with appropriate INCLUDE columns. Without this, each row in the outer query triggers a full table scan of the inner table.

```sql
-- DDL: filtered index for FakturaNr lookups
CREATE NONCLUSTERED INDEX [IX_Fct_Postering_FakturaNr]
ON [fct].[Postering] ([FakturaNr] ASC)
INCLUDE ([DW_EK_Kontoplan])
WHERE [FakturaNr] IS NOT NULL;
```

### Add clustered indexes on temp tables before MERGE

After `SELECT INTO #TempTable`, create a `UNIQUE CLUSTERED INDEX` on the MERGE join key before executing the MERGE. This dramatically improves MERGE performance on large target tables because SQL Server can use a merge join instead of nested loop/hash match.

```python
cursor.execute("SELECT ... INTO #Kreditor_Attributes FROM ...")
cursor.execute(
    "CREATE UNIQUE CLUSTERED INDEX IX_tmp ON #Kreditor_Attributes (DW_EK_Kreditor)"
)
cursor.execute("MERGE [dim].[Kreditor] AS tgt USING #Kreditor_Attributes AS src ...")
```

### Prefer UNION ALL over UNION in insert-only map loads

When combining multiple staging sources for `INSERT ... WHERE NOT EXISTS`, use `UNION ALL` instead of `UNION`. The `NOT EXISTS` check already prevents duplicate inserts against the target table, and the PK constraint provides a final safety net. The implicit `DISTINCT` from `UNION` adds an unnecessary sort over the full combined result set.

```sql
-- BAD: UNION forces a full sort+dedup before the NOT EXISTS check
INSERT INTO [map].[Entity] (CleanCode, bk_Key, DW_Timestamp_Insert)
SELECT DISTINCT f.CleanCode, f.bk_Key, SYSDATETIME()
FROM (
    SELECT ... FROM [stg].[source_a]
    UNION
    SELECT ... FROM [stg].[source_b]
) f
WHERE NOT EXISTS (SELECT 1 FROM [map].[Entity] m WHERE m.CleanCode = f.CleanCode);

-- GOOD: UNION ALL skips the sort; NOT EXISTS + PK handle dedup
INSERT INTO [map].[Entity] (CleanCode, bk_Key, DW_Timestamp_Insert)
SELECT f.CleanCode, f.bk_Key, SYSDATETIME()
FROM (
    SELECT ... FROM [stg].[source_a]
    UNION ALL
    SELECT ... FROM [stg].[source_b]
) f
WHERE NOT EXISTS (SELECT 1 FROM [map].[Entity] m WHERE m.CleanCode = f.CleanCode);
```

### Rebuild indexes after batch MERGE operations

With many monthly partitions of MERGEs, indexes on dim/fct tables accumulate fragmentation. Use a dedicated `erp_index_maintenance` asset that runs `ALTER INDEX ALL ON [table] REBUILD` after all facts complete. This keeps index performance consistent across partitions.

## SQL Migration Patterns

Migration files in `infrastructure/ddl/migrations/` run on every deployment via `staging_migrations` and `dso_migrations` assets. They MUST be idempotent — safe to re-run after applying, after columns are renamed by later migrations, and on fresh databases.

### GO batch separation

SQL Server parses an entire batch before execution. DDL that adds a column and DML that references it MUST be in separate `GO` batches:

```sql
IF NOT EXISTS (SELECT * FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'CleanCode_Postering')
    ALTER TABLE [fct].[Postering] ADD [CleanCode_Postering] NVARCHAR(100) NULL;
GO

-- Separate batch — column now exists at parse time
UPDATE f SET f.CleanCode_Postering = 'SYDAX_' + CAST(f.ref_LedgertransId AS NVARCHAR(50))
FROM [fct].[Postering] f WHERE f.CleanCode_Postering IS NULL;
GO
```

### Compile-time column validation

`IF EXISTS` guards do NOT prevent compile-time validation. If a column was renamed by a later migration, static DML referencing the old name fails the entire batch. Use dynamic SQL for any DML touching potentially-renamed columns:

```sql
-- BAD: fails at compile time if DW_EK_Date_Faktura was renamed
UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Faktura] = -1 WHERE [DW_EK_Date_Faktura] IS NULL;

-- GOOD: dynamic SQL bypasses compile-time validation
EXEC(N'UPDATE [fct].[Fakturalinje] SET [DW_EK_Date_Faktura] = -1 WHERE [DW_EK_Date_Faktura] IS NULL');
```

### sp_rename guards

`sp_rename` raises a non-catchable error if the target name already exists. Always check both names:

```sql
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[T]') AND name = 'DW_EK_Organization')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[T]') AND name = 'DW_EK_Organisation')
    EXEC sp_rename '[fct].[T].[DW_EK_Organization]', 'DW_EK_Organisation', 'COLUMN';
GO
```

"Add column" guards must also check for BOTH old and new names to prevent duplicates after a rename:

```sql
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[T]')
               AND name IN ('DW_EK_Organization', 'DW_EK_Organisation'))
    ALTER TABLE [fct].[T] ADD [DW_EK_Organization] INT NOT NULL DEFAULT -1;
GO
```

### DEFAULT constraint dependencies

Drop auto-named DEFAULT constraints before dropping columns:

```sql
DECLARE @dc NVARCHAR(256);
SELECT @dc = dc.name FROM sys.default_constraints dc
JOIN sys.columns c ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE c.object_id = OBJECT_ID(N'[fct].[T]') AND c.name = 'OldColumn';
IF @dc IS NOT NULL EXEC(N'ALTER TABLE [fct].[T] DROP CONSTRAINT [' + @dc + N']');
ALTER TABLE [fct].[T] DROP COLUMN [OldColumn];
```

### Idempotency checklist

1. Column references in DML use dynamic SQL or `sys.columns` lookup?
2. Every `sp_rename` checks that the NEW name does NOT already exist?
3. Every `ALTER TABLE ADD` checks for BOTH old and new column names?
4. Table name guards handle both pre- and post-rename names (`COALESCE(OBJECT_ID(old), OBJECT_ID(new))`)?
5. Auto-named DEFAULT constraints dropped before dropping columns?
6. Migration has been run twice locally — second run succeeds?

## Quick Reference: Creating a New Pipeline

1. **Extract** (`DSI/mssql/` or `DSI/oracle/` per source; shared `DSI/connectors/`): Create 1:1 source extraction to `ext.*` schema. Use `ext_*` asset key prefix (e.g., `ext_<table>_<source>_daily`). Apply partition filtering per [Extract Layer Patterns](#extract-layer-patterns) (upper-bound-only for the first partition, exact/range for all others, no filtering for full load). No CleanCode generation.
2. **Pre-staging** (`DSA/SQL/staging/`): If needed, create complex join tables. Reads from `ext.*`, writes to `stg.*`. CleanCode generation happens here.
3. **Staging** (`DSA/SQL/staging/`): Create staging tables with exact target columns, bk_* columns, and CleanCode generation. Reads from `stg.pre_*` or `ext.*`, writes to `stg.dim_*` / `stg.fct_*`.
4. **Map update** (`DSA/SQL/map/`): Create/update map table for CleanCode → EK resolution. Use INSERT when key not present; avoid MERGE unless updating non-key attributes (document exceptions)
5. **Dim/Fct update** (`DSA/SQL/dim/`, `DSA/SQL/fct/`): Create update logic resolving CleanCodes via map tables
6. **DSO Sync**: Add sync config in `DSO/load/sync_configs.py`, create asset in `DSO/load/sync_dim.py` or `sync_fct.py` (see `docs/implementation/dso_sync_guide.md`)
7. **Views**: Add view DDL to `infrastructure/ddl/views/erp/` or `infrastructure/ddl/views/security/` using `CREATE OR ALTER VIEW`
