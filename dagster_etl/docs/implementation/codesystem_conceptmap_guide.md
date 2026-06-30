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

ILS (sgh, rekv,    ──→  ext.sgh_ils          ──→  cs_rekvirent ──→ cm_rekvirent_to_sor ──→ stg_dim_rekvirent
     rekvgrp)           ext.rekv_ils                                                      → map → dim
                        ext.rekvgrp_ils

ILS (sgh, lgr)     ──→  ext.sgh_ils          ──→  cs_lager ──→ cm_lager_to_sor ──→ stg_dim_lager
                        ext.lgr_ils                                                 → map → dim

SydAX              ──→  ext.omkostningssted*  ──→  cs_okosted ──→ cm_okosted_to_sor ──→ stg_organization_okosted

NemHandel API      ──→  ext.ean_nemhandel    ──→  cs_ean ──→ cm_ean_to_sor ──→ stg_dim_ean

SOR Registry       ──→  ext.sor_register     ──→  cs_sor (used by all ConceptMap assets)
```

**Key principles:**
1. **DSI is 1:1 raw extraction** — source tables are copied to `ext.*` schema without business logic
2. **DSA CodeSystem assets build hierarchies** — read from `ext.*`, construct concepts/properties/hierarchy, MERGE into `codesystem.*` schema
3. **DSA ConceptMap assets build mappings** — read from `codesystem.*`, apply fuzzy matching + manual overrides, MERGE into `conceptmap.*` schema
4. **Downstream staging assets read from database** — `stg_dim_*` assets SELECT from `codesystem.*` and `conceptmap.*` tables instead of CSV files

## Database Schema

### `codesystem` schema

Each codesystem produces three tables:

| Table | Primary Key | Columns | Purpose |
|-------|------------|---------|---------|
| `codesystem.{name}_concept` | `Code` | `Code`, `Display`, `DW_Timestamp_Insert`, `DW_Timestamp_Update` | One row per concept in the codesystem |
| `codesystem.{name}_properties` | `(Code, PropertyCode)` | `Code`, `PropertyCode`, `Value`, `Type`, audit columns | Property key-value pairs per concept (e.g., `entityType`, `sgh`, `rgrp`) |
| `codesystem.{name}_hierarchy` | `(ParentCode, ChildCode)` | `ParentCode`, `ChildCode`, audit columns | Parent-child relationships between concepts |

The five codesystems are: `rekvirent`, `lager`, `ean`, `sor`, `okosted`.

### `conceptmap` schema

Each conceptmap produces one table:

| Table | Primary Key | Columns | Purpose |
|-------|------------|---------|---------|
| `conceptmap.{source}_to_{target}` | `(SourceCode, TargetCode)` | `SourceCode`, `SourceDisplay`, `Relationship`, `TargetCode`, `TargetDisplay`, audit columns | One row per source→target mapping |

The four conceptmaps are: `rekvirent_to_sor`, `lager_to_sor`, `ean_to_sor`, `okosted_to_sor`.

### Relationship types (FHIR standard)

| Relationship | Meaning | When used |
|-------------|---------|-----------|
| `equal` | Identical meaning | Exact match (similarity ≥ 0.95) |
| `equivalent` | Clinically equivalent | Very close match (similarity ≥ 0.85) |
| `narrower` | Target is broader than source | Hierarchical fallback or moderate match (0.7–0.85) |
| `inexact` | Approximate match | Weak match (0.5–0.7) |
| `unmatched` | No suitable match | Below threshold |

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
dagster_etl/
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
│       ├── utils.py                       # merge_conceptmap_table() + shared matching helpers
│       ├── rekvirent_to_sor/
│       │   ├── rekvirent_to_sor.py        # @asset cm_rekvirent_to_sor + build_rekvirent_to_sor_mappings()
│       │   └── conceptmap.yml
│       ├── lager_to_sor/
│       │   ├── lager_to_sor.py            # @asset cm_lager_to_sor + build_lager_to_sor_mappings()
│       │   └── conceptmap.yml
│       ├── ean_to_sor/
│       │   ├── ean_to_sor.py              # @asset cm_ean_to_sor + build_ean_to_sor_mappings()
│       │   └── conceptmap.yml
│       └── okosted_to_sor/
│           ├── okosted_to_sor.py          # @asset cm_okosted_to_sor + build_okosted_to_sor_mappings()
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
cursor.execute("SELECT * FROM [ext].[sgh_ils]")
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

## ConceptMap Asset Pattern

Each ConceptMap asset follows a three-step pattern: **read codesystem tables → build mappings → MERGE**.

### Asset Definition

```python
@dg.asset(
    name="cm_rekvirent_to_sor",
    deps=["cs_rekvirent", "cs_sor"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "rekvirent_to_sor"},
    description="Build rekvirent-to-sor mappings and MERGE into conceptmap.rekvirent_to_sor",
)
def cm_rekvirent_to_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    ...
```

### Step 1: Read from codesystem.* tables

```python
cursor.execute("SELECT Code, Display FROM [codesystem].[rekvirent_concept]")
source_concepts = cursor.fetchall()

cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[rekvirent_properties]")
source_properties = cursor.fetchall()

cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[rekvirent_hierarchy]")
source_hierarchy = cursor.fetchall()

# Same for SOR codesystem
```

### Step 2: Build mappings

Matcher functions live alongside the asset in `DSA/ConceptMap/{source}_to_{target}/{source}_to_{target}.py`. They apply fuzzy matching, manual overrides, and hierarchical fallbacks:

```python
rows = build_rekvirent_to_sor_mappings(
    source_concepts, source_properties, source_hierarchy,
    sor_concepts, sor_properties, sor_hierarchy,
)
```

Returns `list[tuple]` of `(SourceCode, SourceDisplay, Relationship, TargetCode, TargetDisplay)`.

### Step 3: MERGE into database

```python
from DSA.ConceptMap.utils import merge_conceptmap_table

result = merge_conceptmap_table(cursor, "rekvirent_to_sor", rows)
conn.commit()
```

### ConceptMap assets reference

| Asset | Dependencies | Builder | ConceptMap table |
|-------|-------------|---------|-----------------|
| `cm_rekvirent_to_sor` | `cs_rekvirent`, `cs_sor` | `build_rekvirent_to_sor_mappings` | `conceptmap.rekvirent_to_sor` |
| `cm_lager_to_sor` | `cs_lager`, `cs_sor` | `build_lager_to_sor_mappings` | `conceptmap.lager_to_sor` |
| `cm_ean_to_sor` | `cs_ean`, `cs_sor` | `build_ean_to_sor_mappings` | `conceptmap.ean_to_sor` |
| `cm_okosted_to_sor` | `cs_okosted`, `cs_sor` | `build_okosted_to_sor_mappings` | `conceptmap.okosted_to_sor` |

## Fuzzy Matching Architecture

ConceptMap matchers use a layered matching strategy:

### Matching pipeline

1. **Manual overrides** — Standardised CSV files in `csv_files/` (repo root) loaded via `ext_os2mo_*` assets take precedence
2. **Exact match** — Case-insensitive string equality (relationship: `equal`)
3. **Fuzzy match** — Combined score from `SequenceMatcher` ratio (50%) and keyword overlap (50%), with domain-specific penalties
4. **Hierarchical fallback** — If no good match, map to parent's target with `narrower` relationship

### Shared utilities (`ConceptMap/utils.py`)

| Function | Purpose |
|----------|---------|
| `extract_keywords()` | Tokenize text, remove stop words, return keyword set (optional per-domain `extra_stop_words` / `important_acronyms`) |
| `semantic_similarity()` | 50/50 blend of `SequenceMatcher` ratio and keyword overlap; optional `scoring_adjuster` for domain penalties |
| `find_best_match()` | Fuzzy match a source display to SOR concepts (entityType filter, keyword cache, pluggable `extract_kw` / adjuster) |
| `find_all_descendants()` | All codes in a hierarchy subtree from a root |
| `get_parent_code()` | Parent code for a child in a parent→children map |
| `find_root_code_region_syd()` | Choose REGION_SYD / “region” root in a codesystem tree |
| `determine_relationship()` | Map similarity score to FHIR relationship type (supports known-mapping + hierarchical flags) |
| `rows_to_concept_dict()` | Convert `cursor.fetchall()` tuples to `{code: display}` dict |
| `rows_to_properties_dict()` | Convert to `{code: {key: value}}` nested dict |
| `rows_to_hierarchy_dict()` | Convert to `{parent: [children]}` dict |

### Domain-specific penalties

- **Rekvirent**: Department keywords boosted, non-department (warehouse) keywords penalized
- **Lager**: Warehouse keywords boosted, non-warehouse (clinical) keywords penalized
- **EAN**: No domain penalties (pure string + keyword matching)
- **Okosted**: Depth-ordered matching with parent fallback, manual mappings for top levels

## Consuming CodeSystem/ConceptMap Data

Downstream staging assets (e.g., `stg_dim_rekvirent`) read from `codesystem.*` and `conceptmap.*` tables via SQL JOINs. See the [DWH ETL Pipeline Architecture](sql_etl_guide.md) for the full staging → map → dim pipeline.

### Example: Reading codesystem data with property pivoting

```sql
SELECT
    c.Code,
    c.Display,
    h.ParentCode,
    p_entityType.Value AS EntityType,
    p_sgh.Value AS SGH,
    p_rgrp.Value AS RGRP,
    cm.TargetCode AS CodeSOR,
    cm.Relationship AS SORRelationship
FROM [codesystem].[rekvirent_concept] c
LEFT JOIN [codesystem].[rekvirent_hierarchy] h
    ON h.ChildCode = c.Code
LEFT JOIN [codesystem].[rekvirent_properties] p_entityType
    ON p_entityType.Code = c.Code AND p_entityType.PropertyCode = 'entityType'
LEFT JOIN [codesystem].[rekvirent_properties] p_sgh
    ON p_sgh.Code = c.Code AND p_sgh.PropertyCode = 'sgh'
LEFT JOIN [codesystem].[rekvirent_properties] p_rgrp
    ON p_rgrp.Code = c.Code AND p_rgrp.PropertyCode = 'rgrp'
LEFT JOIN [conceptmap].[rekvirent_to_sor] cm
    ON cm.SourceCode = c.Code
```

### Staging asset dependencies

Staging assets should depend on the relevant `cs_*` and `cm_*` assets:

```python
@dg.asset(
    name="stg_dim_rekvirent",
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
| `infrastructure/ddl/tables/conceptmap/01_conceptmap_tables.sql` | All 4 conceptmap tables |

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
2. **Create asset** — Add `DSA/ConceptMap/{source}_to_{target}/{source}_to_{target}.py` with a `build_{source}_to_{target}_mappings()` function and a `@dg.asset` that reads codesystem.* tables, calls the builder, and calls `merge_conceptmap_table()`
3. **Create YAML** — Add `DSA/ConceptMap/{source}_to_{target}/conceptmap.yml`
4. **Register** — Add to `DSA/ConceptMap/__init__.py`
5. **Manual mappings** — If needed, add a CSV file to `csv_files/` (repo root) with columns: `source_code,source_display,target_code,target_display,relationship,note`, register it in `DSI/csv_files/os2mo.py`, and add the `ext_os2mo_*` asset as a dependency

## Idempotency and MERGE Behaviour

All CodeSystem and ConceptMap assets use the MERGE pattern:

- **WHEN MATCHED** → UPDATE existing rows (updates `DW_Timestamp_Update`)
- **WHEN NOT MATCHED BY TARGET** → INSERT new rows (sets both `DW_Timestamp_Insert` and `DW_Timestamp_Update`)
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

**Version**: 1.0
**Last Updated**: 2026-04-07
