<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# CodeSystem & ConceptMap Pipeline Guide

This guide describes the architecture and implementation patterns for FHIR CodeSystem and ConceptMap pipelines in the Healthpipe data platform. CodeSystems represent organisational hierarchies (hospitals, departments, cost centres), and ConceptMaps define mappings between them (e.g., Rekvirent → SOR).

**Related guides:**
- [DWH ETL Pipeline Architecture](sql_etl_guide.md) — downstream staging/map/dim assets that consume `codesystem.*` and `conceptmap.*` tables
- [FHIR ETL Flow Guide](fhir_etl_guide.md) — FHIR resource pipelines (Patient, Encounter, etc.)
- [Data Lineage Guide](data_lineage_guide.md) — YAML lineage documentation

## Overview

The pipeline transforms organisational reference data from source systems into persistent FHIR-aligned database tables. It follows the standard three-layer architecture:

```
Source Systems          DSI (ext.*)              DSA (codesystem.*/conceptmap.*)     DSA SQL (stg/map/dim)
──────────────          ───────────              ──────────────────────────────      ─────────────────────

ILS (sgh, rekv,    ──→  ext.ils_sgh          ──→  cs_rekvirent ─────────────┐
     rekvgrp)           ext.ils_rekv                                        ├──→ stg_dim_Rekvirent → map → dim
                        ext.ils_rekvgrp           cm_rekvirent_to_sor ──────┘

ILS (sgh, lgr)     ──→  ext.ils_sgh          ──→  cs_lager + cm_lager_to_sor ──→ stg_dim_Lager → map → dim
                        ext.ils_lgr

SydAX              ──→  ext.omkostningssted*  ──→  cs_okosted + cm_okosted_to_sor ──→ stg_organization_okosted

NemHandel API      ──→  ext.ean_nemhandel    ──→  cs_ean + cm_ean_to_sor ──→ stg_dim_EAN

Kuraterede CSV'er  ──→  ext_csv_*            ──→  cm_{ean,lager,rekvirent,okosted}_to_sor
(DSI/csv_files/data/)                             (eneste mapping-kilde — ingen fuzzy matching)

SOR Registry       ──→  ext.sor_register     ──→  cs_sor
```

**Key principles:**
1. **DSI is 1:1 raw extraction** — source tables are copied to `ext.*` schema without business logic
2. **DSA CodeSystem assets build hierarchies** — read from `ext.*`, construct concepts/properties/hierarchy, MERGE into `codesystem.*` schema
3. **DSA ConceptMap assets build mappings** — the ERP maps (ean/lager/rekvirent/okosted → SOR) come exclusively from curated CSV files in `DSI/csv_files/data/`; SHAK and SHAK-a-like come from authoritative source crosswalks. Everything MERGEs into the `conceptmap.*` schema
4. **Downstream staging assets read from database** — `stg_dim_*` assets SELECT from `codesystem.*` and `conceptmap.*` tables instead of CSV files

## Database Schema

### `codesystem` schema

Each codesystem produces three tables:

| Table | Primary Key | Columns | Purpose |
|-------|------------|---------|---------|
| `codesystem.{name}_concept` | `Code` | `Code`, `display`, `dw_timestamp_insert`, `dw_timestamp_update` | One row per concept in the codesystem |
| `codesystem.{name}_properties` | `(Code, PropertyCode)` | `Code`, `PropertyCode`, `value`, `Type`, audit columns | Property key-value pairs per concept (e.g., `entityType`, `sgh`, `rgrp`) |
| `codesystem.{name}_hierarchy` | `(ParentCode, ChildCode)` | `ParentCode`, `ChildCode`, audit columns | Parent-child relationships between concepts |

The five codesystems are: `rekvirent`, `lager`, `ean`, `sor`, `okosted`.

### `conceptmap` schema

Each conceptmap produces one table:

| Table | Primary Key | Columns | Purpose |
|-------|------------|---------|---------|
| `conceptmap.{source}_to_{target}` | `(source_code, target_code)` | `source_code`, `source_display`, `relationship`, `target_code`, `target_display`, audit columns | One row per source→target mapping |

The six conceptmaps are: `RekvirentToSOR`, `LagerToSOR`, `EANToSOR`, `OkostedToSOR`, `SHAKToSOR`, `SHAKAlikeToSOR`.

### Relationship types (FHIR standard)

| Relationship | Meaning |
|-------------|---------|
| `equal` | Identical meaning |
| `equivalent` | Clinically equivalent |
| `narrower` | Target is broader than source |
| `inexact` | Approximate match |

For the CSV-based ERP maps the relationship is whatever the curated CSV row
says; for SHAK→SOR the builder sets `equivalent` (direct crosswalk entry) or
`inexact` (indirect fallback), and SHAK-a-like→SOR is always `equivalent`.

## YAML Metadata

Each codesystem and conceptmap directory contains a `.yml` file with FHIR resource metadata. These are stored in git (not in the database) and provide the canonical URL, name, and description.

**CodeSystem** (`DSA/CodeSystem/{name}/codesystem.yml`):
```yaml
resourceType: CodeSystem
url: "https://example.org/ils/rekvirent"
name: "ILSRekvirent"
description: "Rekvirentorganisation fra ILS"
status: active
hierarchyMeaning: is-a
publisher: Your Organization
content: not-present
```

**ConceptMap** (`DSA/ConceptMap/{source}_to_{target}/conceptmap.yml`):
```yaml
resourceType: ConceptMap
id: "rekvirent-to-sor"
url: "http://example.dk/fhir/ConceptMap/rekvirent-to-sor"
status: active
name: "RekvirentToSOR"
sourceScopeUri: "https://example.org/ils/rekvirent"
targetScopeUri: "https://sundhedsdatastyrelsen.dk/sor"
group:
  - source: "https://example.org/ils/rekvirent"
    target: "https://sundhedsdatastyrelsen.dk/sor"
```

## Code Organization

```
etl/
├── DSI/mssql/
│   └── ils.py                             # ILS TableConfigs including sgh, lgr, rekv, rekvgrp
│
├── DSA/
│   ├── CodeSystem/
│   │   ├── __init__.py                    # Exports codesystem_dsa_assets
│   │   ├── utils.py                       # merge_codesystem_tables() shared MERGE logic
│   │   ├── rekvirent/
│   │   │   ├── rekvirent.py               # @asset cs_rekvirent + build_rekvirent_hierarchy()
│   │   │   └── codesystem.yml
│   │   ├── lager/
│   │   │   ├── lager.py                   # @asset cs_lager + build_lager_hierarchy()
│   │   │   └── codesystem.yml
│   │   ├── ean/
│   │   │   ├── ean.py                     # @asset cs_ean + build_ean_codesystem()
│   │   │   └── codesystem.yml
│   │   ├── sor/
│   │   │   ├── sor.py                     # @asset cs_sor + build_sor_codesystem()
│   │   │   └── codesystem.yml
│   │   └── okosted/
│   │       ├── okosted.py                 # @asset cs_okosted + build_okosted_hierarchy()
│   │       └── codesystem.yml
│   │
│   └── ConceptMap/
│       ├── __init__.py                    # Exports conceptmap_dsa_assets
│       ├── utils.py                       # merge_conceptmap_table() + conceptmap_rows_from_csv() + dw_ek resolution
│       ├── csv_to_sor.py                  # Factory for the four CSV-based assets: cm_{ean,lager,rekvirent,okosted}_to_sor
│       ├── rekvirent_to_sor/
│       │   └── conceptmap.yml             # FHIR metadata only — mappings live in DSI/csv_files/data/rekvirent_to_sor.csv
│       ├── lager_to_sor/
│       │   └── conceptmap.yml
│       ├── ean_to_sor/
│       │   └── conceptmap.yml
│       ├── okosted_to_sor/
│       │   └── conceptmap.yml
│       ├── shak_to_sor/
│       │   ├── shak_to_sor.py             # @asset cm_shak_to_sor (authoritative SHAKSORMap crosswalk)
│       │   └── conceptmap.yml
│       └── shakalike_to_sor/
│           ├── shakalike_to_sor.py        # @asset cm_shakalike_to_sor (EPJI SorIdentifier)
│           └── conceptmap.yml
│
├── infrastructure/
│   └── ddl/
│       ├── common/00_schemas.sql          # Includes codesystem + conceptmap schemas
│       └── tables/
│           ├── codesystem/01_codesystem_tables.sql
│           └── conceptmap/01_conceptmap_tables.sql
│
└── docs/implementation/
    └── codesystem_conceptmap_guide.md     # This file
```

## CodeSystem Asset Pattern

Each CodeSystem asset follows a three-step pattern: **read → build → MERGE**.

### Asset Definition

```python
@dg.asset(
    name="cs_rekvirent",
    deps=["ext_sgh_ils", "ext_rekv_ils", "ext_rekvgrp_ils"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "rekvirent"},
    description="Build Rekvirent codesystem from ext.* tables and MERGE into codesystem.rekvirent_*",
)
def cs_rekvirent(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    ...
```

### Step 1: Read from ext.* tables

```python
cursor.execute("SELECT * FROM [ext].[ils_sgh]")
sgh_rows = cursor.fetchall()
sgh_cols = [col[0] for col in cursor.description]
```

### Step 2: Build codesystem structure

Builder functions live alongside the asset in `DSA/CodeSystem/{name}/{name}.py`. They accept raw database rows (tuples or dicts) and return the standard triple:

```python
concepts, properties, hierarchy = build_rekvirent_hierarchy(
    sgh_rows, rekv_rows, rekvgrp_rows,
    sgh_cols, rekv_cols, rekvgrp_cols,
)
```

The output format is always:
- `concepts`: `[{"CODE": "SGH_55", "DISPLAY": "Sygehus Sønderjylland"}, ...]`
- `properties`: `[{"CODE": "SGH_55", "KEY": "entityType", "VALUE": "sygehus", "TYPE": "String"}, ...]`
- `hierarchy`: `[{"CODE": "SGH_55", "PARENT_CODE": "REGION_SYD"}, ...]`

### Step 3: MERGE into database

The shared `merge_codesystem_tables()` function handles the temp-table + MERGE pattern for all three table types:

```python
from DSA.CodeSystem.utils import merge_codesystem_tables

result = merge_codesystem_tables(cursor, "rekvirent", concepts, properties, hierarchy)
conn.commit()
```

Returns a dict with `concepts_inserted`, `concepts_updated`, `properties_inserted`, etc.

### CodeSystem assets reference

| Asset | Dependencies (ext.*) | Builder | Codesystem tables |
|-------|---------------------|---------|-------------------|
| `cs_rekvirent` | `ext_sgh_ils`, `ext_rekv_ils`, `ext_rekvgrp_ils` | `build_rekvirent_hierarchy` | `codesystem.rekvirent_*` |
| `cs_lager` | `ext_sgh_ils`, `ext_lgr_ils` | `build_lager_hierarchy` | `codesystem.lager_*` |
| `cs_okosted` | `ext_omkostningsstedhierarki_sydax` | `build_okosted_hierarchy` | `codesystem.okosted_*` |
| `cs_ean` | `extract_ean_nemhandel` | `build_ean_codesystem` | `codesystem.ean_*` |
| `cs_sor` | `extract_sor_register` | `build_sor_codesystem` | `codesystem.sor_*` |

### Okosted: composite node identity

Okosted is the one codesystem where a node is not "one row = one code". The
source (`arc.sydax_omkostningssted_hierarki`) is a path table: each row holds
up to 20 levels, and the same `nummer` value can recur at different levels as
a genuinely different organisational unit. `build_okosted_hierarchy` keys a
node on the pair `(nummer, niveau)` instead of on `nummer` alone, so it
returns a **4-tuple** — `(concepts, properties, hierarchy, counters)` — the
one builder that doesn't follow the plain 3-tuple pattern above.

- **Code**: `<nummer>_NIVEAU_<n>`, e.g. `RSD_NIVEAU_1`, `SHD.SHS_NIVEAU_4`.
- **Properties per node**: `nummer` (the raw source number, for lookups and
  reporting); `enhedstype` (`juridisk enhed` / `virksomhedsenhed` /
  `afdeling` / `omkostningssted`, derived from which typed id column —
  `jeId`/`veId`/`afId`/`osId` — is filled at that level; `Ukendt` when none
  is) and `startsdate`/`enddate`/`kildesystem` all come from the node's
  **newest row**, picked across *every* row where the node's `(nummer,
  niveau)` cell appears — own-row or ancestor-occurrence alike — so a pure
  grouping node can still carry these, inherited from a descendant-path row.
  Only `aktiv`/`slettet` and `OmkostningsstedHierarkiId` are genuinely
  own-row-sourced (rows where this cell is *that row's* deepest filled
  level) — a grouping node with no own row gets neither.
- **Hierarchy**: every edge goes from level N to level N−1 in the same
  source row, so the tree is 1:1 with the source path table and acyclic by
  construction — a cycle cannot arise regardless of source data quality.
- **`counters`**: `parent_conflicts`, `path_gaps` and `unknown_enhedstype` —
  data-quality signals (not failures) that `cs_okosted` logs as warnings and
  surfaces in its `MaterializeResult` metadata.

## ConceptMap Asset Pattern (CSV-based ERP maps)

The four ERP conceptmaps are generated by a single factory,
`DSA/ConceptMap/csv_to_sor.py`. Each asset follows **read CSV → convert → MERGE**:

1. **Read CSV** — the asset takes its `ext_csv_{name}` extract (the curated
   CSV in `DSI/csv_files/data/{name}.csv`, columns
   `source_code,source_display,target_code,target_display,relationship,note`)
   as its only upstream input.
2. **Convert** — `conceptmap_rows_from_csv()` turns the CSV dict rows into
   `(source_code, source_display, relationship, target_code, target_display)`
   tuples, skipping rows missing a required field and deduplicating
   `(source_code, target_code)` pairs.
3. **MERGE** — `merge_conceptmap_table(..., delete_stale=True)` writes the full
   snapshot: removing a CSV row also removes it from the database. Afterwards
   `update_conceptmap_dw_ek()` resolves the `dw_ek_*` columns against `map.*`.

A mapping exists **if and only if** it has a CSV row. New source codes are
unmapped (NULL `code_sor` downstream) until someone adds a row to the CSV —
there is no fuzzy matching and no hierarchical fallback.

### ConceptMap assets reference

| Asset | Mapping source | ConceptMap table |
|-------|----------------|-----------------|
| `cm_rekvirent_to_sor` | `DSI/csv_files/data/rekvirent_to_sor.csv` | `conceptmap.RekvirentToSOR` |
| `cm_lager_to_sor` | `DSI/csv_files/data/lager_to_sor.csv` | `conceptmap.LagerToSOR` |
| `cm_ean_to_sor` | `DSI/csv_files/data/ean_to_sor.csv` | `conceptmap.EANToSOR` |
| `cm_okosted_to_sor` | `DSI/csv_files/data/okosted_to_sor.csv` | `conceptmap.OkostedToSOR` |
| `cm_shak_to_sor` | `ext.shaksor_map` (Sundhedsdatastyrelsens SHAKSORMap.csv) | `conceptmap.SHAKToSOR` |
| `cm_shakalike_to_sor` | `arc.epji_location` (EPJI SorIdentifier) | `conceptmap.SHAKAlikeToSOR` |

### Shared utilities (`ConceptMap/utils.py`)

| Function | Purpose |
|----------|---------|
| `conceptmap_rows_from_csv()` | Convert `ext_csv_*` CSV dict rows to MERGE tuples (validation + dedup) |
| `merge_conceptmap_table()` | Temp-table + MERGE into `conceptmap.*` (optional `delete_stale` full-snapshot semantics) |
| `update_conceptmap_dw_ek()` | Resolve `dw_ek_*` columns from `map.*` via `conceptmap_dw_ek_resolution.sql` |
| `find_all_descendants()` | All codes in a hierarchy subtree from a root (also used by the security SOR expansion) |
| `rows_to_hierarchy_dict()` | Convert to `{parent: [children]}` dict (also used by the security SOR expansion) |

## Consuming CodeSystem/ConceptMap Data

Downstream staging assets (e.g., `stg_dim_Rekvirent`) read from `codesystem.*` and `conceptmap.*` tables via SQL JOINs. See the [DWH ETL Pipeline Architecture](sql_etl_guide.md) for the full staging → map → dim pipeline.

### Example: Reading codesystem data with property pivoting

```sql
SELECT
    c.code,
    c.display,
    h.parent_code,
    p_entityType.value AS entity_type,
    p_sgh.value AS sgh,
    p_rgrp.value AS rgrp,
    cm.target_code AS code_sor,
    cm.relationship AS sor_relationship
FROM [codesystem].[rekvirent_concept] c
LEFT JOIN [codesystem].[rekvirent_hierarchy] h
    ON h.child_code = c.code
LEFT JOIN [codesystem].[rekvirent_properties] p_entityType
    ON p_entityType.code = c.code AND p_entityType.property_code = 'entityType'
LEFT JOIN [codesystem].[rekvirent_properties] p_sgh
    ON p_sgh.code = c.code AND p_sgh.property_code = 'sgh'
LEFT JOIN [codesystem].[rekvirent_properties] p_rgrp
    ON p_rgrp.code = c.code AND p_rgrp.property_code = 'rgrp'
LEFT JOIN [conceptmap].[rekvirent_to_sor] cm
    ON cm.source_code = c.code
```

### Staging asset dependencies

Staging assets should depend on the relevant `cs_*` and `cm_*` assets:

```python
@dg.asset(
    name="stg_dim_Rekvirent",
    deps=["cs_rekvirent", "cm_rekvirent_to_sor"],
    ...
)
```

## Infrastructure Setup

### DDL files

| File | Content |
|------|---------|
| `infrastructure/ddl/common/00_schemas.sql` | `codesystem` and `conceptmap` schema creation |
| `infrastructure/ddl/tables/codesystem/01_codesystem_tables.sql` | All 15 codesystem tables (5 × 3) |
| `infrastructure/ddl/tables/conceptmap/01_conceptmap_tables.sql` | The four CSV-based conceptmap tables (SHAK-tabellerne har egne DDL-filer) |

### Infrastructure assets

| Asset | Group | Purpose |
|-------|-------|---------|
| `staging_infra_codesystem` | `staging_infrastructure` | Execute codesystem DDL |
| `staging_infra_conceptmap` | `staging_infrastructure` | Execute conceptmap DDL |

These depend on `staging_infra_schemas` and are included in the standard `full_infrastructure_job`.

## Adding a New CodeSystem

1. **Add ext.* extraction** — If source data is not already in ext.*, add a `TableConfig` to the appropriate DSI config file (e.g., `DSI/mssql/ils.py`)
2. **Create DDL** — Add three tables to `infrastructure/ddl/tables/codesystem/01_codesystem_tables.sql`
3. **Create asset** — Add `DSA/CodeSystem/{name}/{name}.py` with a `build_{name}_*()` function returning `(concepts, properties, hierarchy)` and a `@dg.asset` that reads ext.*, calls the builder, and calls `merge_codesystem_tables()`
4. **Create YAML** — Add `DSA/CodeSystem/{name}/codesystem.yml` with FHIR metadata
5. **Register** — Add to `DSA/CodeSystem/__init__.py`

## Adding a New ConceptMap

1. **Create DDL** — Add table to `infrastructure/ddl/tables/conceptmap/01_conceptmap_tables.sql`
2. **Create CSV** — Add `DSI/csv_files/data/{source}_to_{target}.csv` with columns `source_code,source_display,target_code,target_display,relationship,note` and register it in `DSI/csv_files/conceptmap_csv.py` (gives you the `ext_csv_*` extract asset)
3. **Create asset** — Add an entry to `_CSV_CONCEPTMAPS` in `DSA/ConceptMap/csv_to_sor.py` (CSV-based) — or, for an authoritative source crosswalk like SHAK, a dedicated builder module
4. **Create YAML** — Add `DSA/ConceptMap/{source}_to_{target}/conceptmap.yml`
5. **Register** — Add to `DSA/ConceptMap/__init__.py`
6. **dw_ek resolution** — Add a section to `DSA/ConceptMap/conceptmap_dw_ek_resolution.sql` for the new table

## Idempotency and MERGE Behaviour

All CodeSystem and ConceptMap assets use the MERGE pattern:

- **WHEN MATCHED** → UPDATE existing rows (updates `dw_timestamp_update`)
- **WHEN NOT MATCHED BY TARGET** → INSERT new rows (sets both `dw_timestamp_insert` and `dw_timestamp_update`)
- Running the same asset twice produces identical results (no duplicates)
- `OUTPUT $action` captures insert/update counts for metadata reporting

## Naming Conventions

### Asset names

| Type | Pattern | Example |
|------|---------|---------|
| CodeSystem | `cs_{name}` | `cs_rekvirent` |
| ConceptMap | `cm_{source}_to_{target}` | `cm_rekvirent_to_sor` |

### Groups

| Group | Assets |
|-------|--------|
| `codesystem` | All `cs_*` assets |
| `conceptmap` | All `cm_*` assets |

### Tags

All CodeSystem and ConceptMap assets include:
- `domain`: `codesystem` or `conceptmap`
- `layer`: `codesystem` or `conceptmap`
- `module`: The specific codesystem or conceptmap name

---

**Version**: 2.0 (fuzzy matching fjernet — kuraterede CSV'er er eneste mapping-kilde for ERP-maps)
**Last Updated**: 2026-10-14
