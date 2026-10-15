<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# DWH ETL Pipeline Architecture

This document describes the Data Warehouse (DWH) ETL pipeline architecture used for business domains that do not require FHIR representation (e.g., Finance/Procurement). It is intended for developers that need to understand or extend the pipeline.

## Overview

The DWH pipeline transforms source data through a series of layers, from raw extraction to final presentation views. The architecture emphasizes:

- **DFM checkpoint-based delta extraction** - Source tables are extracted incrementally via checkpoints (`meta.DFMState`), not Dagster partitions
- **Persistent archive layer** - Extracted data is merged into `arc.*` tables; downstream reads from the archive
- **CleanCode-based relationship resolution** - Business keys are standardized into CleanCodes, then resolved to surrogate keys (EK) via map tables
- **Late surrogate key resolution** - For Type 2 dimensions, surrogate keys (SK) are resolved in the view layer to simplify historical updates
- **Staging-driven updates** - All staging tables contain exactly the columns needed for target tables (no extra columns)

## Data Lineage Documentation (Required)

For all DSO-synced `dim.*` and `fct.*` targets, lineage documentation is mandatory.

- Always document lineage using the minimal YAML structure (`dso_lineage` root).
- Store files by schema in `etl/data_lineage/dim/` and `etl/data_lineage/fct/`.
- Use compact mappings (`from`, `coalesce`, `lookup`, `expr`) with explicit source references.
- Update lineage YAML whenever source mappings, keys/lookups, or transformation logic changes.

Reference: `etl/docs/implementation/data_lineage_guide.md`.

## Pipeline Layers

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                              STAGING DATABASE                                            │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌───────┐│
│  │   ext    │──>│   arc    │──>│  pre_stg │──>│   stg    │──>│   map    │──>│ dim   ││
│  │ (DSI)    │   │ (DSI)    │   │  (DSA)   │   │  (DSA)   │   │  (DSA)   │   │ fct   ││
│  │          │   │          │   │          │   │          │   │          │   │ (DSA) ││
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘   └──────────┘   └───┬───┘│
│       ↑                                                                         │     │
│    Source (via DFM checkpoint delta)                                             │     │
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

A domain whose source grain already IS the reporting grain can end in a **datamart**
instead of conformed dim/fct tables — the dm path (today: HAIBA):

```
ext ──> arc ──> map ──> dm (fact + Egenskaber) ──> DSO sync ──> vw_dm.*
```

See [The Datamart Path (dm)](#the-datamart-path-dm).

### Layer Descriptions

| Layer | Location | Purpose |
|-------|----------|---------|
| **ext** | `DSI/mssql/extract/` (DFM configs), `DSI/connectors/` (shared machinery) | Volatile landing zone (`DROP` + reload each run). DFM extracts source deltas into `ext.*`. No business logic, no CleanCodes. Only adds `SourceSystem` as technical ETL metadata. |
| **arc** | `DSI/mssql/archive/` (`*_archive_configs.py`) | Persistent archive. 1:1 reflection of source table plus `dw_timestamp_insert`/`dw_timestamp_update`. Downstream reads from `arc.*`, not `ext.*`. See [Archive Layer](#archive-layer). |
| **pre_stg** | `DSA/SQL/staging/` | Complex joins, deduplication, and CleanCode generation. Reads from `arc.*`, writes to `stg.*`. |
| **stg** | `DSA/SQL/staging/` | Entity-level staging with bk_* columns. Reads from `stg.pre_*` or `arc.*`, writes to `stg.dim_*` / `stg.fct_*`. |
| **map** | `DSA/SQL/map/` | Mapping tables that translate CleanCodes to surrogate keys (`dw_ek_*`). One map table per dimension and per fact. |
| **dim / fct** | `DSA/SQL/dim/`, `DSA/SQL/fct/` | Final dimension and fact tables with resolved EK values. |
| **dm** | `DSA/SQL/dm/` | Resolved datamart layer: slim facts (DW keys + typed measures) paired with `Egenskaber` transaction dimensions (raw source values 1:1 on `dw_ek`). Full rebuild from `arc` each run. See [The Datamart Path (dm)](#the-datamart-path-dm). |
| **DSO Sync** | DSO / DPA Database | Dimensions and facts synced from DSA to DSO via linked-server MERGE (see `docs/implementation/dso_sync_guide.md`). |
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
- **dw_ek** (entity key) is stored in fact tables
- **dw_sk** (surrogate key for specific version) is resolved in the view layer

This simplifies handling of historical updates to dimensions, as the EK remains stable while SK versions change.

### Update Dependencies

Fact updates should depend on both dimension and map updates. This ensures:
1. The dimension is fully updated
2. EK values from map tables reference valid dimension rows

## DSI/DSA Contract

### DSI (Data Staging In) — ext and arc only
- Writes **only** to `ext.*` and `arc.*` schemas
- 1:1 extraction: every ext/arc table maps to exactly one source table
- No business logic: no CleanCode generation, no joins, no aggregation
- Only adds technical ETL metadata: `SourceSystem` and archive audit columns
- Code location: `DSI/mssql/extract/` (DFM extract configs per source), `DSI/mssql/archive/` (archive merge configs), shared machinery in `DSI/connectors/`

Multi-table enrichment joins do **not** belong in DSI. They live as SQL in `pre_stg` assets, reading from the individual `arc.*` tables.

### DSA (Data Staging Area) — staging and transforms
- Owns **all** pre_stg, stg, map, dim, and fct assets
- Reads from `arc.*` tables, writes to `stg.*` tables
- Responsible for: CleanCode generation, cross-source joins, deduplication, bk_* column creation
- Asset keys: `pre_stg_*`, `stg_dim_*`, `stg_fct_*`
- Code location: `DSA/SQL/staging/`
- Shared CleanCode SQL expressions: `DSA/SQL/common/` (e.g. `ils_clean_code_shared.py`)

#### DSA map/dim/fct
- Owns **all** map, dim, and fct update assets
- Reads from `stg.*` tables, writes to `map.*`, `dim.*`, and `fct.*` tables
- Responsible for: CleanCode → EK resolution via map tables, dimension/fact MERGE updates
- Asset keys: `map_*`, `dim_*`, `fct_*`
- Code location: `DSA/SQL/map/`, `DSA/SQL/dim/`, `DSA/SQL/fct/`

## Schema Reference Rules (Critical)

> **NEVER reference `[stg].[raw_table]` for raw extracted data.** Raw data lives in `ext.*` (volatile) and `arc.*` (persistent).

The `stg.*` schema is reserved for **DSA-produced output** only:
- `stg.pre_*` — output of pre-staging assets
- `stg.dim_*` — output of dimension staging assets
- `stg.fct_*` — output of fact staging assets

### Allowed read patterns by layer

| SQL Layer | Reads FROM | Writes TO |
|-----------|-----------|-----------|
| **pre_stg** | `[arc].[table_name]` | `[stg].[pre_table_name]` |
| **stg_dim / stg_fct** | `[stg].[pre_*]` or `[arc].[table_name]` | `[stg].[dim_*]` / `[stg].[fct_*]` |
| **map / dim / fct update** | `[stg].[dim_*]`, `[stg].[fct_*]`, `[map].[*]` | `[map].[*]`, `[dim].[*]`, `[fct].[*]` |
| **dm rebuild** | `[arc].[table_name]`, `[map].[*]`, shared dims (`[dim].[Organisation]`, `[ref].[PatientToBorger]`) as lookups | `[map].[*]`, `[dm].[*]` |

### CleanCode generation in pre_stg SQL

The `ext.*`/`arc.*` tables contain raw source data only — **no CleanCode columns**. CleanCodes must be generated **inline in the pre-staging SQL** using CASE/CONCAT expressions. Examples:

```sql
-- cvr CleanCode from SydAX juridisk_enhed
CASE
    WHEN kred.juridisk_enhed IS NOT NULL AND LTRIM(RTRIM(kred.juridisk_enhed)) <> ''
    THEN 'CVR_' + LTRIM(RTRIM(kred.juridisk_enhed))
    ELSE NULL
END AS clean_code_Kreditor

-- Kontoplan CleanCode from finanskonto_id
'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(finanskonto_id AS NVARCHAR(100)), ' ', ''))))
    AS clean_code_Kontoplan
```

Where several assets resolve against the same map, put the CleanCode expression in a shared builder under `DSA/SQL/common/` and import it, rather than hand-copying the CASE into each pre_stg asset. Assign it to a module-level constant so the contract can be unit-tested without running the asset. Hand-mirrored copies drift; a shared expression cannot.

Sharing the expression is necessary but not sufficient — **the expression must normalise exactly as much as the side that mints the map member, and no more.** A key is only reachable if it is spelled the way it was minted, so any extra normalisation on the lookup side fabricates a key the map cannot hold. For the ILS keys the minting side is the codesystem builder, which only does `str(value).strip()`; adding `REPLACE(v, ' ', '')` there made the genuine `map.Rekvirent` member `SGH_6000_RNR_SLB TELE` permanently unresolvable. Before adding a normalisation step, check what the `stg_dim_*` / codesystem asset that seeds the map actually does. Note also that case-folding is redundant: the databases collate `Danish_Norwegian_CI_AS`, so `=` already ignores case.

## Extract Layer: DFM (Checkpoint-Based Delta Extraction)

All DWH source extraction runs through **DFM** (Data Flow Management): checkpoint-based delta loading that replaces the earlier Dagster-partition-based extract pipeline. Each source table has a `DfmExtractConfig` (source table, delta column/expression, checkpoint type, optional column list) and assets are generated by `create_dfm_extraction_assets`.

Key facts:

- **Checkpoints, not partitions**: current state per table lives in `meta.DFMState`. A run extracts everything past the checkpoint, then advances it on success.
- **Run modes** (via `RunModeConfig`, overridable at launch time in the Dagster Launchpad):
  - `delta` — incremental from checkpoint (default)
  - `full_load` — entire table, ignores checkpoint
  - `period` — explicit date range, does not advance the checkpoint
  - `ids` — specific business keys (used for targeted replays, e.g. from the DQA remediation queue)
- **Configs**: `DSI/mssql/extract/{source}_extract_configs.py` (extract) and `DSI/mssql/archive/{source}_archive_configs.py` (archive merge).
- **Parallel full-load**: very large tables use the keyset-windowed parallel extractor in `DSI/connectors/mssql/parallel_extract.py` (process-pool workers; gated by `should_run_parallel()`).

References: `docs/implementation/dfm_extract_factory_guide.md` (writing configs), `docs/implementation/dfm_operations_guide.md` (run modes, checkpoint operations, ad-hoc runs).

## Archive Layer

The persistent archive (`arc` schema) sits between `ext` and downstream staging and is standard for all DFM-driven sources (ILS, SydAX, MySupply, KMD UdData). It enables:

- **Fact reruns without hitting the source** — historical snapshots are retained in `arc`
- **Delta processing** — downstream only processes data that arrived since the last run
- **Reconciliation against source** — detect source deletions even when source doesn't use soft deletes

### Core Principles

- `arc` tables are a **1:1 reflection of the source table** plus standard DWH columns
- `ext` tables remain volatile (dropped and reloaded each run) — they are the landing zone
- **Downstream reads from `arc`, never from `ext`**
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

| Column | Type | Purpose |
|--------|------|---------|
| `dw_timestamp_insert` | DATETIME2 | When this row was first inserted into the archive |
| `dw_timestamp_update` | DATETIME2 | When this row was last inserted or updated in the archive |

Behavior by strategy:

- **insert_only**: Both columns receive the same value on INSERT. No rows are ever updated.
- **upsert**: `dw_timestamp_insert` is set once on INSERT. `dw_timestamp_update` is refreshed on every MATCHED UPDATE.

Downstream staging assets filter the archive on `dw_timestamp_update` to pick up only new/changed rows.

### Naming

`arc.{system}_{source_table}`, mirroring `ext.{system}_{source_table}` — e.g. `arc.ils_vare`,
`arc.sydax_afdeling`, `arc.ifdb_encounter`. The source name comes first. (The legacy
partitioned ILS extract in `DSI/mssql/ils.py` writes source-suffixed `ext.{source_table}_ils`
tables; do not copy that for new work.) See [dsi_etl_guide.md](dsi_etl_guide.md#naming).

## Naming Conventions

Naming follows `docs/implementation/navngivningsstandard.md` (the single source).
That guide splits by layer: **`ext.*` / `arc.*` keep the source's own table and
column names** (spelling and casing). **From the archive onwards** (`pre_stg`,
`stg`, `dim`, `fct`, `dm`) the standard applies in full — tables in PascalCase,
everything else in snake_case, `ae`/`oe`/`aa` for Danish letters — except the
part of a column name that references a table name, which stays PascalCase.
Legacy uppercase columns (`CleanCode_*`, `DW_EK_*`, `DW_SK_*`) remain in older
tables — do NOT rename them opportunistically.

### CleanCodes

```
clean_code_<Tabel>[_<rolle>]
```

Examples:
- `clean_code_Kreditor`
- `clean_code_Leverandoervare`

All persisted CleanCode columns in base dim/fct/map DDL use this form. Two legacy variants
survive outside base DDL — recognize them, never use them in new code, and do not rename
them opportunistically:

| Variant | Where it lives | Example |
|---------|----------------|---------|
| `CleanCode_*` (prefix, pre-overhaul) | Older persisted stg-layer columns, migration scripts | `clean_code_LeverandoerVarenummer` (Vare chain) |
| `*_CleanCode` (suffix, oldest) | pre_stg SQL aliases and persisted pre_stg/stg columns in the ILS/mySupply chains; index names | `CVR_CleanCode`, `Vare_CleanCode`, `IX_Dim_Vareudlevering_clean_code` |

When editing an existing chain, keep its current names consistent end-to-end (pre_stg alias
→ stg column → MapUpd/DimUpd/FctUpd joins); a partial rename surfaces as "Invalid column
name" at runtime, not at import. Known base-DDL stragglers (frozen until a dedicated
cleanup pass): all-lowercase `clean_code_kreditor`/`clean_code_faktura` in `ref` tables,
and the bare `clean_code` column on `map.Organisation`.

### Surrogate Keys

```
dw_ek_<Tabel>[_<rolle>]  -- Entity Key (stable identifier)
dw_sk_<Tabel>            -- Surrogate Key (for Type 2 versioning)
```

Examples:
- `dw_ek_Organisation`, `dw_ek_Organisation_parent`
- `dw_ek_Dato_bogfoering`

### Business Keys

```
bk_<key_name>
```

Use source system column names as the default. Add context when source names are ambiguous:
- `bk_ref_ledgertrans_id`
- `bk_leverandoer_varenummer`
- `bk_finanskonto_id`

For composite business keys in facts, use the `bk_` prefix for each component:
- `bk_faktura_lines`, `bk_faktura_header`, `bk_faktura_metadata`

### Table Names

Use singular form and PascalCase for facts and dimensions:
- `fct.Postering`
- `fct.Fakturalinje`
- `dim.Kreditor`

## Standard DWH Columns

### Audit Columns (All Tables)

| Column | Purpose |
|--------|---------|
| `dw_timestamp_insert` | Timestamp of first insert |
| `dw_timestamp_update` | Timestamp of last update (equals Insert on first insert) |

### Fact-Specific Columns

| Column | Purpose |
|--------|---------|
| `dw_haendelsestidspunkt` | Event timestamp for resolving SK in Type 2 dimension lookups |
| `dw_is_deleted` | Soft delete marker |

### Type 2 Dimension Columns

| Column | Purpose |
|--------|---------|
| `dw_valid_from` | Version validity start |
| `dw_valid_to` | Version validity end |
| `dw_is_deleted` | Soft delete marker |

## Table Structures

### Staging Tables (stg)

```sql
-- Columns should contain ONLY what goes into the target table
clean_code_<Tabel>                    -- Primary CleanCode
bk_<key1>                             -- Business key component(s)
bk_<key2>
clean_code_<RelateretTabel>[_<rolle>] -- Related table CleanCodes
<attributes>                          -- For dimensions
<measures>                            -- For facts
dw_haendelsestidspunkt                -- For facts with Type 2 dimension relations
```

#### Example

```sql
-- Example: stg.fct_Postering_sydax staging table (abridged)
CREATE TABLE [stg].[fct_Postering_sydax] (
    -- Primary CleanCode (grain = Finanspostering)
    clean_code_Postering             NVARCHAR(100) NOT NULL,

    -- Business keys for traceability
    bk_ref_ledgertrans_id            BIGINT NULL,

    -- Related table CleanCodes (for map table lookups)
    clean_code_Kreditor              NVARCHAR(100) NULL,
    clean_code_Kontoplan             NVARCHAR(100) NULL,
    clean_code_Faktura               NVARCHAR(100) NULL,

    -- Measures
    posteringsbeloeb                 DECIMAL(30,12) NULL,
    transaktionsbeloeb               DECIMAL(30,12) NULL,

    -- Event timestamp for Type 2 dimension lookups
    dw_haendelsestidspunkt           DATETIME2 NULL
);
```

### Map Tables

```sql
dw_ek_<Tabel>     IDENTITY NOT NULL  -- Auto-generated surrogate key
clean_code_<...>                     -- Indexed for lookup performance
bk_<key1>                            -- Business key
bk_<key2>
```

Every non-system column in a map table MUST have one of these prefixes: `dw_*`, `clean_code_*`, or `bk_*` (legacy uppercase spellings of the first two still occur — do not rename them opportunistically; documented exceptions like `vnr` in `map.Leverandoervare` exist, see below).

#### Example

```sql
-- Example: map.Kontoplan map table
CREATE TABLE [map].[Kontoplan] (
    [dw_ek_Kontoplan] INT IDENTITY(1,1) NOT NULL,
    [clean_code_Kontoplan] NVARCHAR(255) NOT NULL,

    -- Business keys
    [bk_finanskonto_id] BIGINT NOT NULL,
    [bk_kontonummer] NVARCHAR(255) NOT NULL,

    [dw_timestamp_insert] DATETIME2 NULL,
    [dw_timestamp_update] DATETIME2 NULL,
    CONSTRAINT [PK_Map_Kontoplan] PRIMARY KEY CLUSTERED ([clean_code_Kontoplan] ASC)
);
```

#### Map Table Load Pattern

Map tables use **insert-only** logic by default:

```sql
INSERT INTO [map].[Entity] (clean_code_Entity, dw_timestamp_insert)
SELECT source.clean_code_Entity, CAST(SYSDATETIME() AS datetime2(0))
FROM [stg].[dim_entity] source
WHERE NOT EXISTS (
    SELECT 1 FROM [map].[Entity] m
    WHERE m.clean_code_Entity = source.clean_code_Entity
);
```

- NEVER use MERGE for pure map key creation.
- Map tables only store clean_code-to-dw_ek mappings and optional business-key traceability columns (`bk_*`).
- Exceptions (e.g. updating a non-key column like `vnr` in `map.Leverandoervare`) must be documented in the asset docstring with justification.

### Fact Tables

```sql
dw_ek_<Tabel>                    -- Fact's own surrogate key (IDENTITY or from map)
clean_code_<Tabel>               -- Canonical key (for lineage tracking)
bk_<primary_key>                 -- Primary business key (for traceability)
dw_ek_<RelateretDim>[_<rolle>]   -- Resolved dimension keys
<measures>
dw_timestamp_insert
dw_timestamp_update
dw_haendelsestidspunkt
dw_is_deleted                     -- Optional
```

#### Example

```sql
-- Example: fct.Postering fact table
CREATE TABLE [fct].[Postering] (
    -- Primary business key / grain
    [ref_ledgertrans_id] BIGINT NOT NULL,

    -- Canonical key (from map.Postering lookup)
    [dw_ek_Postering] INT NOT NULL DEFAULT -1,
    [clean_code_Postering] NVARCHAR(100) NULL,

    -- Resolved dimension keys (from map table lookups)
    [dw_ek_Kreditor]             INT NOT NULL DEFAULT -1,
    [dw_ek_Kontoplan]            INT NOT NULL DEFAULT -1,
    [dw_ek_Faktura]              INT NOT NULL DEFAULT -1,
    [dw_ek_Organisation]         INT    NOT NULL DEFAULT -1,

    -- Resolved date dimension keys
    [dw_ek_Dato_godkendelse]     INT NULL,
    [dw_ek_Dato_transaktion]     INT NULL,
    [dw_ek_Dato_bogfoering]      INT NULL,

    -- Measures
    [posteringsbeloeb]           DECIMAL(30,12) NULL,
    [transaktionsbeloeb]         DECIMAL(30,12) NULL,
    [udlignet_beloeb]             DECIMAL(30,12) NULL,

    -- Descriptive attributes
    [faktura_nr]                  NVARCHAR(100) NULL,
    [transaktionstekst]          NVARCHAR(MAX) NULL,

    -- Audit columns
    [dw_timestamp_insert]        DATETIME2 NULL,
    [dw_timestamp_update]        DATETIME2 NULL,

    CONSTRAINT [PK_Fct_Postering] PRIMARY KEY CLUSTERED ([ref_ledgertrans_id] ASC)
);
```

## The Datamart Path (dm)

Some domains arrive at the reporting grain: the source row IS the thing being
counted, and nothing needs conforming into shared fct tables. Those domains skip
`pre_stg`/`stg` entirely and end in the **dm** schema instead of `dim`/`fct`:

```
ext ──> arc ──> map (mint dw_ek) ──> dm.<X> (fact) + dm.<X>Egenskaber ──> DSO ──> vw_dm.<X>
```

The pattern is domain-neutral; whether a SOURCE belongs on it (and how its
archive should handle revisions) is covered by
[kpi_aggregat_data_guide.md](kpi_aggregat_data_guide.md). HAIBA
(hospital-acquired infections, weekly CSV snapshots from SDS) is the reference
implementation used as the example below; a new dm domain gets its own asset
groups, jobs and tables on the same shape.

**Example — the HAIBA chain, by layer:**

| Layer | Assets (group) | Code | Tables |
|-------|----------------|------|--------|
| Extract | `ext_haiba_*` (`haiba_extract`) | `DSI/csv_files/haiba.py` | `ext.haiba_*` |
| Archive | `arc_haiba_*` (`haiba_archive`) | `DSI/csv_files/haiba_archive.py` | `arc.haiba_*`, `arc.haiba_agg_revision`, `meta.HAIBALeverance` |
| Citizen stubs | `fhir_patient_haiba` (`haiba_borger`) | `DSA/FHIR/Patient/Patient_HAIBA.py` | (Aidbox Patients) |
| Datamart | `dm_haiba_*` (`haiba_dm`) | `DSA/SQL/dm/haiba.py` | `map.HAIBA_*`, `dm.HAIBA_*`, `dm.HAIBA_*Egenskaber` |
| Contact stamp | `dm_haiba_cases_*_kontakt` (`haiba_dm`) | `DSA/SQL/dm/haiba_kontakt.py` | relation keys on the facts, match quality on `Egenskaber` |
| DSO sync | `dso_sync_dm_haiba_*` (`haiba_dso_sync`) | `DSO/load/sync_haiba.py` | `dm.HAIBA_*` on DSO (`truncate_insert`) |
| Views | `dso_infra_views_haiba` | `infrastructure/ddl/views/haiba/01_vw_dm.sql` | `vw_dm.HAIBA_*` (filter `dw_is_deleted = 0`) |

Orchestration follows one general rule: split INGEST (extract + archive) from
PUBLISH (dm + DSO sync) whenever the rebuild depends on keys another pipeline
resolves in between, and let a sensor request the publish when a new delivery
has landed. (HAIBA: `haiba_ingest_job` daily 05:00, the nightly DFM chain
projects new citizen stubs into `ref.PatientToBorger`, and
`haiba_publish_sensor` requests `haiba_publish_job` — re-publishing on
following nights while the unresolved-citizen count falls.)

### The dm pattern

- **The fact/egenskaber pair is a transaction dimension**: `dm.<X>` carries ONLY
  DW keys and typed measures; `dm.<X>Egenskaber` carries the source's
  descriptive columns 1:1 on the shared `dw_ek_<X>` (minted append-only in
  `map.<X>` per clean_code — the map also serves a FACT here, not just dims).
- **Full rebuild, not MERGE**: each run is `TRUNCATE` + `INSERT ... WITH (TABLOCK)`
  from `arc` — deliveries are complete snapshots, so a rebuild is always
  consistent with the archive, and `dw_ek` stays stable through the map.
- **Typing happens in the rebuild** (`TRY_CONVERT(<type>, NULLIF(src, ''))`),
  not in a stg layer — with a per-column conversion-loss counter so a source
  format change surfaces instead of silently becoming NULL/-1.
- **`dw_is_deleted` is carried, never filtered**: soft-deleted rows stay in dm
  as the revision trail when the producer retracts history. Consumers read
  `vw_dm.*`, where the `dw_is_deleted = 0` filter is the default; the trail is
  read from the dm tables directly.
- **Person identifiers never reach dm**: when the source grain carries a
  person identifier (CPR or similar) it lives only in the map's clean_code and
  in `arc` — both staging-internal, never synced. The person is carried as its
  `dw_ek_*` entity key, so no access restriction is needed on DSO. (HAIBA:
  CPR-free by construction, citizen as `dw_ek_Borger`.)

### Guards, state and quality

- **In-run guards** (the rebuild fails hard): arc == fact == egenskaber row
  parity, empty-fact-with-source-rows, and a re-keying warning when a run mints
  more than `REKEY_WARN_FRACTION` of the source (a clean_code format change
  would otherwise silently re-key the whole history).
- **DFM**: every layer logs to `meta.DFMRunLog` (schema `dm` → target_type
  `datamart`). ext and dm runs are **log-only** with the mode locked to
  `full_load` — dm owns no checkpoint (a full rebuild has no delta position);
  the delivery watermark lives in `meta.HAIBALeverance`. The arc layer owns its
  checkpoint via the standard archive merge.
- **Lineage**: one YAML per dm table in `data_lineage/dm/`, verified by the
  lineage↔DDL checker like dim/fct/fhir (see `data_lineage_guide.md`).
- **DQA**: `dqa/config/haiba_subjects.py` — row parity (fact↔arc,
  fact↔egenskaber), the rebuild's own conversion-loss counter, sentinel rates,
  lineage-driven referential integrity plus the relation keys → `fhir.Encounter`
  / `fhir.Location`, match-quality checks, and delivery freshness on
  `meta.HAIBALeverance`. All at DAILY cadence: `dqa_dwh_haiba_daily_job` is
  sensor-chained onto `haiba_publish_job` (per publish), and the DSO
  reconciliation (`dqa_dso_haiba_job`) runs on the Saturday cron stagger.

## Update Logic Principles

1. **Staging tables define the contract** - They contain exactly what goes into target tables, nothing more.

2. **Naming convention enables automation** - Using standard prefixes (`bk_`, `clean_code_`, `dw_ek_`) allows:
   - Map tables to be updated with correct columns
   - All staging columns to flow to primary tables

3. **Map-then-update pattern**:
   - First update map tables with new CleanCode → EK mappings
   - Then update dimensions/facts with resolved EK values

4. **Map table loads**: Use simple `INSERT ... WHERE NOT EXISTS` by default. Avoid MERGE for insert-only map loads. Use MERGE or UPDATE only when the map must update non-key attributes for existing rows; document such exceptions in the asset docstring.

5. **Null-safe diagnostics**: Diagnostic queries on possibly-empty result sets must handle SQL `NULL` aggregates — use `COALESCE(SUM(...), 0)` in SQL and `stats[idx] or 0` in Python before comparing.

## Dagster Orchestration

### Asset Groups

The ERP pipeline is organized in **layer groups** — one group per pipeline layer, across all source systems:

| Phase | Group(s) | Contents |
|-------|----------|----------|
| Archive | `ils_archive`, `sydax_archive`, `mysupply_archive`, `kmd_uddata_archive` | DFM extract + archive merge per source system |
| Pre-staging | `erp_pre_staging` | `pre_stg_*` assets |
| Staging | `erp_staging` | `stg_dim_*`, `stg_fct_*` assets |
| Reference data | `erp_ref` | Shared reference/seed tables |
| Maps | `erp_map` | `map_*` assets |
| Dimensions | `erp_dimension` | `dim_*` assets |
| Facts | `erp_fact` | `fct_*` assets |
| DSO sync | `erp_dso_sync` | `dso_sync_dim_*`, `dso_sync_fct_*` assets |

### Jobs and Schedules

| Job | Contents | Trigger |
|-----|----------|---------|
| `erp_daily_dfm_job` (`jobs/erp_daily_dfm_jobs.py`) | All archive groups + the full ERP transform chain (`erp_pre_staging` → … → `erp_dso_sync`) | `erp_daily_dfm_schedule`, daily 23:00 Europe/Copenhagen |
| `ils_archive_job`, `sydax_archive_job`, `mysupply_archive_job`, `kmd_uddata_archive_job` | Archive layer for one source system | Ad-hoc |
| `lager_dim_job` (`jobs/lager_jobs.py`) | Standalone Lager dimension refresh | Ad-hoc |
| `erp_infrastructure_setup` (`jobs/erp_jobs.py`) | Infrastructure DDL (see `07-infrastructure-pipeline` rule / `full_infrastructure_job`) | Ad-hoc |
| `data_quality_job` (`jobs/data_quality_jobs.py`) | DQA checks | Scheduled per cadence |

Individual groups can be run via `AssetSelection.groups(...)` for ad-hoc runs (e.g. re-run dimensions + facts without re-extracting). Groups collapse in the Dagster UI, keeping the asset graph navigable.

### Delta State

- **DFM checkpoints** (`meta.DFMState`) drive what each extract pulls from the source. See `docs/implementation/dfm_operations_guide.md` for resetting checkpoints, period replays, and id-based replays (`dfm_reset_job`, run modes).
- **DSO sync watermarks** (`meta.SyncWatermarks`, managed by `SyncStateManager`) drive what each DSO sync pushes. See `docs/implementation/dso_sync_guide.md`.

There is no other generic watermark system; staging/dim/fct assets process whatever is in their upstream tables.

## Data Quality Assurance (DQA)

Declarative data-quality framework (see `etl/dqa/` and `docs/architecture/dqa/`):

- **Subjects** (configs in `dqa/config/`) declare a table, its grain, and its checks; `dqa/factory.py` generates one anchor asset per subject/cadence (group `data_quality`) with each check as an `@asset_check`.
- **Results** persist to `dqa.results`. Referential-integrity failures on subjects with a ReplaySpec record dangling keys in `dqa.remediation_queue`, replayable via DFM `ids` run mode.
- **DSO reconciliation** (`dqa/dso.py`) derives checks from the DSO sync configs and compares DSO tables against DSA via linked server (detection-only).

## Testing Expectations

Every test must be able to fail on a plausible bug — no config-assert or existence tests.

For a new or modified pipeline, the coverage that matters:

- **Unit**: real logic only — CleanCode builder rules, DFM WHERE-clause building for the run modes actually used, tricky dedup/grain logic.
- **Integration** (`USE_REAL_DB_FOR_TESTS=true`): the full chain materializes — arc → pre_stg → stg → map → dim → fct — with correct EK resolution, verified by direct SQL queries (not metadata alone).
- **Migrations**: run cleanly against both the staging and DSO databases; run the `staging_migrations` asset TWICE locally — the second run must succeed (skipped by the `[meta].[MigrationHistory]` tracker). Schema changes land in BOTH the migration and the base DDL (see the parity test pattern in `test_ilsvare_columns_migration.py`).

DDL-only, config-entry, and registration changes need no accompanying unit test.

## Master Data / Shared Dimensions

Shared dimensions used across domains (e.g. `dim.Organisation`, date dimensions) are maintained by their own jobs (`jobs/organization_jobs.py`, `jobs/shared_dimensions_jobs.py`) and consumed by ERP facts via map lookups. Reference/seed tables shared within ERP live in the `erp_ref` group.

## View Layer Responsibilities

The view layer in DSO / DPA handles:

1. **SK resolution** - Resolve `dw_sk` for Type 2 dimensions using `dw_haendelsestidspunkt`
2. **Complex relationship resolution** - Fact-to-fact relationships that were kept as CleanCodes
3. **Filtering** - Apply `dw_is_deleted = 0` and validity filters
4. **Datamart assembly** - Combine dimensions and facts for specific reporting needs

View DDL lives in `infrastructure/ddl/views/{erp,security,fhir,codesystem,conceptmap,dqa,haiba}/` and uses `CREATE OR ALTER VIEW`.

### View Naming

```
vw_dim_<table>              -- Dimension view
vw_fct_<table>              -- Fact view
vw_dm.<Table>               -- Datamart-layer view (own schema; the safe default read:
                            --  SELECT * ... WHERE dw_is_deleted = 0)
tabel_datamart_<name>       -- Datamart-specific views
```

## Performance Guidelines for Map/Dim/Fct Updates

### Never wrap indexed columns in functions

`LTRIM(RTRIM(m.clean_code_Entity))` in a JOIN or WHERE clause disables index seeks, forcing a full table scan. Clean data at write-time (in MapUpd INSERT) so that read-time JOINs (in DimUpd/FctUpd) can use the clustered index directly.

```sql
-- BAD: disables clustered index on map.Kreditor.clean_code_Kreditor
LEFT JOIN [map].[Kreditor] m
    ON LTRIM(RTRIM(m.clean_code_Kreditor)) = sk.clean_code_Kreditor

-- GOOD: index-seekable, data already trimmed at insert time
LEFT JOIN [map].[Kreditor] m
    ON m.clean_code_Kreditor = sk.clean_code_Kreditor
```

### Index correlated subquery join columns

If a SELECT uses a correlated subquery against a large table (e.g., `SELECT TOP 1 ... FROM fct.Postering WHERE faktura_nr = ...`), ensure the join column has a nonclustered index with appropriate INCLUDE columns. Without this, each row in the outer query triggers a full table scan of the inner table.

```sql
-- DDL: filtered index for faktura_nr lookups
CREATE NONCLUSTERED INDEX [IX_Fct_Postering_FakturaNr]
ON [fct].[Postering] ([faktura_nr] ASC)
INCLUDE ([dw_ek_Kontoplan])
WHERE [faktura_nr] IS NOT NULL;
```

### Add clustered indexes on temp tables before MERGE

After `SELECT INTO #TempTable`, create a `UNIQUE CLUSTERED INDEX` on the MERGE join key before executing the MERGE. This dramatically improves MERGE performance on large target tables because SQL Server can use a merge join instead of nested loop/hash match.

```python
cursor.execute("SELECT ... INTO #Kreditor_Attributes FROM ...")
cursor.execute(
    "CREATE UNIQUE CLUSTERED INDEX IX_tmp ON #Kreditor_Attributes (dw_ek_Kreditor)"
)
cursor.execute("MERGE [dim].[Kreditor] AS tgt USING #Kreditor_Attributes AS src ...")
```

### Prefer UNION ALL over UNION in insert-only map loads

When combining multiple staging sources for `INSERT ... WHERE NOT EXISTS`, use `UNION ALL` instead of `UNION`. The `NOT EXISTS` check already prevents duplicate inserts against the target table, and the PK constraint provides a final safety net. The implicit `DISTINCT` from `UNION` adds an unnecessary sort over the full combined result set.

```sql
-- BAD: UNION forces a full sort+dedup before the NOT EXISTS check
INSERT INTO [map].[Entity] (clean_code_Entity, bk_key, dw_timestamp_insert)
SELECT DISTINCT f.clean_code_Entity, f.bk_key, SYSDATETIME()
FROM (
    SELECT ... FROM [stg].[source_a]
    UNION
    SELECT ... FROM [stg].[source_b]
) f
WHERE NOT EXISTS (SELECT 1 FROM [map].[Entity] m WHERE m.clean_code_Entity = f.clean_code_Entity);

-- GOOD: UNION ALL skips the sort; NOT EXISTS + PK handle dedup
INSERT INTO [map].[Entity] (clean_code_Entity, bk_key, dw_timestamp_insert)
SELECT f.clean_code_Entity, f.bk_key, SYSDATETIME()
FROM (
    SELECT ... FROM [stg].[source_a]
    UNION ALL
    SELECT ... FROM [stg].[source_b]
) f
WHERE NOT EXISTS (SELECT 1 FROM [map].[Entity] m WHERE m.clean_code_Entity = f.clean_code_Entity);
```

### Rebuild indexes after batch MERGE operations

Repeated MERGE runs fragment indexes on dim/fct tables. The dedicated `erp_index_maintenance` asset runs `ALTER INDEX ALL ON [table] REBUILD` after facts complete, keeping index performance consistent.

## SQL Migration Patterns

Migration files in `infrastructure/ddl/migrations/` are applied at most once per DATABASE by the `staging_migrations` and `dso_migrations` assets, tracked by filename in `[meta].[MigrationHistory]` (see `execute_migrations` in `infrastructure/utils/ddl_loader.py`). The shared `migrations/` directory runs against BOTH the staging and DSO databases, each with its own tracker — a file must run cleanly (or no-op) on both. Idempotency is still required — defense in depth on servers that predate the tracker, and after a partial failure (the tracker row is written after the file's SQL commits, so a crash in between re-runs the whole file). Write them safe to re-run after applying, after columns are renamed by later migrations, and on fresh databases. Once applied to all environments, migration files can be removed — leftover tracker rows are harmless.

Hard correctness rules (the FIRST run fails or silently no-ops without these):

- Wrap column guards in a table-existence check (see "Table-existence guards" below).
- Static DML referencing potentially-renamed columns fails at T-SQL compile time — use dynamic SQL (see "Compile-time column validation" below).
- NEVER use T-SQL CURSOR/procedural batches — `execute_sql` silently drops them, and the tracker then records the no-op as applied. Keep migrations flat guarded `IF ... EXEC` blocks.
- Drop auto-named DEFAULT constraints before dropping columns (see "DEFAULT constraint dependencies" below).

### Every schema change lands in BOTH places

A migration patches databases that already exist. The base DDL in `tables/{domain}/` is what a *fresh* database gets — `baseline_migrations()` stamps every migration as applied without executing it, precisely because the base DDL is assumed to already carry their net effect. So a column that lives in only one of the two places reaches only half the environments.

ALWAYS change the migration and the `CREATE TABLE` in the same commit. See `test_ilsvare_columns_migration.py` for the per-change test pattern (`TestCreateDdl` + `TestMigration`), and `test_fhir_ddl_migration_parity.py` for the FHIR-wide invariant.

### Table-existence guards

Column guards alone do not imply the table exists: `COL_LENGTH`, `sys.columns` and `INFORMATION_SCHEMA.COLUMNS` all report "absent" for a table that is not there, so a bare column guard turns into an `ALTER TABLE` against a missing object. Wrap the whole guard:

```sql
IF OBJECT_ID(N'[fhir].[Condition]', 'U') IS NOT NULL
   AND COL_LENGTH('[fhir].[Condition]', 'category_code') IS NULL
BEGIN
    ALTER TABLE [fhir].[Condition] ADD [category_code] NVARCHAR(100) NULL;
END
GO
```

### GO batch separation

SQL Server parses an entire batch before execution. DDL that adds a column and DML that references it MUST be in separate `GO` batches:

```sql
IF NOT EXISTS (SELECT * FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[fct].[Postering]') AND name = 'clean_code_Postering')
    ALTER TABLE [fct].[Postering] ADD [clean_code_Postering] NVARCHAR(100) NULL;
GO

-- Separate batch — column now exists at parse time
UPDATE f SET f.clean_code_Postering = 'SYDAX_' + CAST(f.ref_ledgertrans_id AS NVARCHAR(50))
FROM [fct].[Postering] f WHERE f.clean_code_Postering IS NULL;
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
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[T]') AND name = 'dw_ek_Organisation')
    EXEC sp_rename '[fct].[T].[DW_EK_Organization]', 'dw_ek_Organisation', 'COLUMN';
GO
```

"Add column" guards must also check for BOTH old and new names to prevent duplicates after a rename:

```sql
IF NOT EXISTS (SELECT * FROM sys.columns WHERE object_id = OBJECT_ID(N'[fct].[T]')
               AND name IN ('DW_EK_Organization', 'dw_ek_Organisation'))
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

### Migration checklist

1. Column references in DML use dynamic SQL or a `sys.columns` lookup into a variable (no CURSOR batches)?
2. Every `sp_rename` checks that the NEW name does NOT already exist?
3. Every `ALTER TABLE ADD` checks for BOTH old and new column names?
4. Table name guards handle both pre- and post-rename names (`COALESCE(OBJECT_ID(old), OBJECT_ID(new))`)?
5. Auto-named DEFAULT constraints dropped before dropping columns?
6. Runs cleanly against BOTH the staging and DSO databases (or no-ops where the objects don't exist)?
7. Column guards wrapped in a table-existence check (`IF OBJECT_ID(...) IS NOT NULL AND ...`)?
8. Schema change landed in BOTH the migration AND the base `tables/{domain}/` DDL, same commit?
9. Run `staging_migrations` twice locally — the second run succeeds (skipped by the tracker, and the file stays re-runnable if the tracker row is ever cleared)?

## Quick Reference: Creating a New Pipeline

1. **Infrastructure DDL** (`infrastructure/ddl/tables/{domain}/`): map/dim/fct tables + registration chain.
2. **Extract + archive** (`DSI/mssql/extract/{source}_extract_configs.py`, `DSI/mssql/archive/{source}_archive_configs.py`): add a `DfmExtractConfig` and archive config for each source table. 1:1 raw, no CleanCodes. See `dfm_extract_factory_guide.md`.
3. **Pre-staging** (`DSA/SQL/staging/`): complex joins, dedup, CleanCode generation. Reads `arc.*`, writes `stg.pre_*`.
4. **Staging** (`DSA/SQL/staging/`): staging tables with exact target columns and bk_* columns. Writes `stg.dim_*` / `stg.fct_*`.
5. **Map update** (`DSA/SQL/map/`): CleanCode → EK resolution. `INSERT ... WHERE NOT EXISTS`; avoid MERGE unless updating non-key attributes (document exceptions).
6. **Dim/Fct update** (`DSA/SQL/dim/`, `DSA/SQL/fct/`): MERGE updates resolving CleanCodes via map tables.
7. **DSO Sync**: add sync config in `DSO/load/sync_configs.py`, create asset in `DSO/load/sync_dim.py` or `sync_fct.py` (see `dso_sync_guide.md`).
8. **Views**: add view DDL to `infrastructure/ddl/views/erp/` using `CREATE OR ALTER VIEW`.
9. **Lineage YAML**: `data_lineage/{dim,fct}/` — mirror the latest format on `development`.
10. **Groups/jobs**: assign assets to the layer groups above so they are picked up by `erp_daily_dfm_job`.
