<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# FHIR ETL Flow Implementation Guide (v2)

This guide describes the standard architecture and implementation patterns for FHIR ETL flows in the Healthpipe data platform.

## Overview

A FHIR ETL pipeline transforms data from source systems into FHIR resources and loads them into a FHIR server. Each pipeline follows a **two-asset architecture** with consistent patterns for partitioning, reference resolution, and error handling.

**Key Principle**: One ETL pipeline typically results in updating one FHIR resource type. Each resource type has exactly **two assets**: one extract asset and one transform+post asset.

## Data Lineage Documentation (Required)

For FHIR flows that populate DSO-synced `fhir.*` tables, lineage documentation is mandatory.

- Always document lineage using the minimal YAML structure (`dso_lineage` root).
- Store files in `dagster_etl/data_lineage/fhir/` with `fhir_<entity>.yml` naming.
- Keep mappings compact (`from`, `fhir_path`, `expr`) and use explicit source references.
- Update lineage YAML whenever mappings, source tables, or view definitions change.

Reference: `dagster_etl/docs/implementation/data_lineage_guide.md`.

## ETL Architecture: Two-Asset Pattern

### Pattern Overview

```
Source System → [Extract Asset] → [Transform + Post Asset] → FHIR Server
                    (DSI)              (DSA)
```

The two-asset pattern simplifies the pipeline and ensures consistency:
1. **Extract Asset**: Extracts raw data from source systems
2. **Transform + Post Asset**: Applies business logic, transforms to FHIR, and posts to server

### 1. Extract Asset (DSI Layer)

**Purpose**: Extract raw data from source systems into standardized Python dictionaries.

**Location**: `DSI/oracle/` (IFDB configs, e.g. `ifdb.py`) and `DSI/connectors/oracle/` (shared Oracle extractor/factory); MSSQL sources live under `DSI/mssql/` with `DSI/connectors/mssql/`

**Standard Pattern**:
- Uses `TableExtractionConfig` and `create_extraction_assets` factory
- Asset key: `["ifdb_extract", "{entity}_{partition}"]` (e.g., `["ifdb_extract", "encounter_daily"]`)
- Group: `ifdb_extraction_{partition}` (e.g., `ifdb_extraction_daily`)

**Shared connector layer** (`DSI/connectors/`):
Both Oracle (IFDB) and MSSQL (ERP) extraction delegate to shared connector modules (`DSI/connectors/oracle/`, `DSI/connectors/mssql/`) plus cross-cutting helpers in `DSI/connectors/` (`protocol.py`, `types.py`, `factory.py`, `base.py`) that standardize
asset key generation, group naming, and partition variant creation. The shared layer provides:
- `ExtractionConfig` protocol (satisfied by both `TableExtractionConfig` and `TableConfig`)
- `PartitionType` and `TableType` enums for consistent terminology
- `create_extraction_assets()` generic factory

**Example** (Standard - using factory):

```python
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

ENCOUNTER_CONFIG = TableExtractionConfig(
    table="IFDB.ENCOUNTER",
    columns=["OBJECT_ID", "PATIENT_ID", ...],
    partition_column="EPJI_MODIFICATION_DATETIME",
    description="Encounter data from IFDB EPJ system.",
)

(
    extract_encounter_daily,
    extract_encounter_monthly,
    extract_encounter_full_load,
) = create_extraction_assets(
    entity_name="encounter",
    config=ENCOUNTER_CONFIG,
    source_deps=[],
)
```

### 2. Transform + Post Asset (DSA Layer)

**Purpose**: Apply business logic, transform to FHIR resources, resolve references, and post to FHIR server.

**Location**: `DSA/FHIR/{Resource}/` (e.g. `DSA/FHIR/Encounter/Encounter_IFDB.py`)

**Standard Pattern**:
- Asset name: `fhir_{resource}_{partition}_ifdb` (e.g., `fhir_encounter_daily_ifdb`)
- Group: `fhir_ifdb_{partition}` (e.g., `fhir_ifdb_daily`)
- Takes extract output as input
- Uses shared `_transform_{resource}_to_fhir()` function
- Uses shared `_post_{resource}_to_fhir()` function

**Example**:

```python
@dg.asset(
    name="fhir_encounter_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "encounter_daily"])
        ),
    },
    deps=["fhir_episode_of_care_daily_ifdb", "fhir_location_daily_ifdb"],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={...},
)
def fhir_encounter_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Encounter resources for daily partition."""
    if not extracted_data:
        return dg.MaterializeResult(metadata={"count": 0})

    # Transform
    encounters = _transform_encounters_to_fhir(extracted_data, context)

    # Post
    return _post_encounters_to_fhir(
        context=context,
        encounters=encounters,
        fhir_server=fhir_server,
        fhir_database=fhir_database,
    )
```

## Asset Naming and Grouping

### Extract Assets

| Component | Pattern | Example |
|-----------|---------|---------|
| Asset Key | `["ifdb_extract", "{entity}_{partition}"]` | `["ifdb_extract", "encounter_daily"]` |
| Group | `ifdb_extraction_{partition}` | `ifdb_extraction_daily` |
| Function Name | `extract_{entity}_{partition}` | `extract_encounter_daily` |

### Transform + Post Assets

| Component | Pattern | Example |
|-----------|---------|---------|
| Asset Name | `fhir_{resource}_{partition}_ifdb` | `fhir_encounter_daily_ifdb` |
| Group | `fhir_ifdb_{partition}` | `fhir_ifdb_daily` |

### Why This Naming Matters

- **Groups enable batch processing**: Jobs select assets by group (e.g., all `ifdb_extraction_daily` assets)
- **Consistent naming enables discovery**: Easy to find all assets for a resource type
- **Standard patterns enable automation**: New resources follow the same pattern

## Reference Resolution

Reference resolution converts identifier-based references to FHIR ID references, enabling proper resource navigation in FHIR UIs.

For the current shared identifier cache and parallel-worker architecture, see
[`../architecture/core_cache_parallel_execution.md`](../architecture/core_cache_parallel_execution.md).

### Identifier Systems

All identifier systems are centralized in `utils/fhir/identifier_systems.py`:

```python
# Person identifiers
SYSTEM_CPR = "urn:dk:cpr"

# Healthcare organization identifiers
SYSTEM_SOR = "https://sundhedsdatastyrelsen.dk/sor"

# IFDB-derived identifiers
SYSTEM_IFDB_PATIENT_ID = "urn:source:ifdb:patient_id"
SYSTEM_IFDB_EPISODE_OF_CARE_ID = "urn:source:ifdb:episode_of_care_id"

# Domain-specific identifiers
SYSTEM_CAREPLAN_KRAEFTPAKKE = "urn:dk:sundhed:kraeftpakke:careplan"
```

### Configuration-Based Resolution

**Top-level references** use `ReferenceFieldConfig`:

```python
ENCOUNTER_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
]
```

**Nested references** use `NestedReferenceFieldConfig`:

```python
CAREPLAN_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="activity.detail.performer",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
            SYSTEM_IFDB_STAFF: "Practitioner",
        },
        is_required=False,
        description="Activity performer (Organization or Practitioner)",
    ),
]
```

### Resolution Process

```python
def _post_resources_to_fhir(resources, fhir_database):
    # Step 1: Resolve existing resource IDs (for upsert)
    resources = resolve_resource_ids_for_upsert(
        resources=resources,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        preferred_id_systems=[SYSTEM_CAREPLAN_KRAEFTPAKKE],
    )

    # Step 2: Resolve all references (top-level + nested)
    ref_result = resolve_and_apply_all_references(
        resources=resources,
        reference_configs=REFERENCE_CONFIGS,
        nested_reference_configs=NESTED_REFERENCE_CONFIGS,
        fhir_database=fhir_database,
    )

    # Step 3: Post to FHIR database using direct insertion
    return post_resources_direct(
        context=context,
        resources=resources,
        fhir_database=fhir_database,
        resource_type="CarePlan",
        source_name="ifdb_oracle",
    )
```

## Missing Dependency Resolution

When processing resources in partitions, referenced resources (e.g., Patients, EpisodeOfCare) may not yet exist in the FHIR server because they were last modified in a different partition. The **missing dependency resolution** mechanism handles this by fetching and posting missing resources on-demand before reference resolution.

### How It Works

1. **Collect** all referenced IDs from the resources being posted (e.g., Patient IDs from `subject.identifier`)
2. **Check** which referenced resources already exist in the FHIR server (via the `FHIRDatabaseResource` identifier lookup facade)
3. **Fetch** missing resources from IFDB source, transform to FHIR, and post them
4. **Clear** the affected identifier caches so subsequent resolution sees the newly posted resources
5. **Proceed** with normal reference resolution

### Usage Pattern

Each `_post_*` function accepts optional `ifdb_oracle` and `resolve_missing_dependencies` parameters:

```python
def _post_resources_to_fhir(
    context,
    resources,
    fhir_database,
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
):
    # Resolve missing dependencies before reference resolution
    if resolve_missing_dependencies and ifdb_oracle:
        from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_patients

        resolve_missing_patients(patient_ids, ifdb_oracle, fhir_database, context)

    # Then resolve references as normal
    resolve_and_apply_all_references(resources, reference_configs, fhir_database)
```

All three partition variants (daily, monthly, full_load) should pass `ifdb_oracle` and `resolve_missing_dependencies=True` to ensure robustness.

### Key Implementation Detail: Cache Invalidation

After posting missing resources, clear only the affected resource-type caches through
the `FHIRDatabaseResource` facade. For production flows, prefer
`safe_clear_identifier_caches_for_resources({"Patient", "EpisodeOfCare"}, reason=...)`
so non-critical cleanup failures are logged without hiding the real job result.

Do not mutate `_identifier_cache` or instantiate `CacheCoordinator` from FHIR
assets. The resource facade owns cache backend selection, TTL, locking, mmap
connections, and invalidation.

### Utility: `dependency_resolver.py`

- **Module**: `DSA.FHIR.utils.ifdb.dependency_resolver`
- `resolve_missing_patients()` — Check and create missing Patient resources
- `resolve_missing_episodes_of_care()` — Check and create missing EpisodeOfCare (also resolves patient deps)
- `resolve_careplan_dependencies()` — Resolve both Patient and EpisodeOfCare dependencies for CarePlan resources

## Partition Strategies

All ETL flows must support three partition strategies:

| Partition | Group Suffix | Use Case |
|-----------|--------------|----------|
| **Full Load** | `_full_load` | Initial loads, full refresh, data reconciliation |
| **Daily** | `_daily` | Daily incremental updates, near real-time sync |
| **Monthly** | `_monthly` | Monthly batch processing, backfilling, catchup |

### Implementation Pattern

Create three asset variants that share transformation logic:

```python
# Shared transformation function
def _transform_resources_to_fhir(data, context) -> list[Resource]:
    ...

# Shared posting function
def _post_resources_to_fhir(context, resources, fhir_database):
    resources = resolve_resource_ids_for_upsert(resources, fhir_database, ...)
    return post_resources_direct(context, resources, fhir_database, ...)

# Full Load asset
@dg.asset(group_name="fhir_ifdb_full_load", partitions_def=full_load_partitions, ...)
def fhir_resource_full_load_ifdb(context, extracted_data, fhir_database, ...):
    resources = _transform_resources_to_fhir(extracted_data, context)
    return _post_resources_to_fhir(context, resources, fhir_database)

# Daily asset
@dg.asset(group_name="fhir_ifdb_daily", partitions_def=daily_partitions, ...)
def fhir_resource_daily_ifdb(context, extracted_data, fhir_database, ...):
    resources = _transform_resources_to_fhir(extracted_data, context)
    return _post_resources_to_fhir(context, resources, fhir_database)

# Monthly asset
@dg.asset(group_name="fhir_ifdb_monthly", partitions_def=monthly_partitions, ...)
def fhir_resource_monthly_ifdb(context, extracted_data, fhir_database, ...):
    resources = _transform_resources_to_fhir(extracted_data, context)
    return _post_resources_to_fhir(context, resources, fhir_database)
```

## Jobs

### Centralized Job Location

All jobs are now centralized in `dagster_etl/jobs/` for better discoverability:

- `jobs/fhir_ifdb_jobs.py` - FHIR IFDB ETL jobs

### Standard ETL Jobs

Standard ETL jobs are defined in `dagster_etl/jobs/fhir_ifdb_jobs.py`:

```python
# Daily job - includes all assets in extraction and FHIR groups
fhir_etl_ifdb_daily_job = dg.define_asset_job(
    name="fhir_etl_ifdb_daily_job",
    selection=fhir_infrastructure_assets
    | ifdb_extraction_daily_assets
    | fhir_ifdb_daily_assets,
)

# Monthly job
fhir_etl_ifdb_monthly_job = dg.define_asset_job(...)

# Full Load job
fhir_etl_ifdb_full_load_job = dg.define_asset_job(...)
```

### Domain-Specific Jobs

For running specific resource types independently, add to `dagster_etl/jobs/`:

```python
fhir_careplan_full_load_job = dg.define_asset_job(
    name="fhir_careplan_full_load_etl",
    selection=dg.AssetSelection.groups("fhir_careplan"),
    partitions_def=full_load_partitions,
)
```

## Implementation Checklist

### Pre-Development

1. **Create Field Mapping Document**
   - Map all source fields to FHIR attributes
   - Mark derived fields with `**(derived)**`
   - Mark extensions with `**(extension)**`
   - Prefer standard FHIR fields over extensions

2. **Review FHIR R4B Specification**
   - Identify required vs optional fields
   - Understand resource structure and relationships
   - Plan reference dependencies

3. **Plan Reference Resolution**
   - Identify all references (top-level and nested)
   - Determine identifier systems needed
   - Add new systems to `utils/fhir/identifier_systems.py` if needed

### Development Steps

1. **Extract Asset** (`DSI/oracle/` for IFDB, `DSI/mssql/` for ERP-style sources; shared machinery in `DSI/connectors/`)
   - Use factory for standard extraction (delegates to shared core), custom code for special delta logic
   - Use standard asset key pattern `["ifdb_extract", "{entity}_{partition}"]`
   - Use standard group pattern `ifdb_extraction_{partition}`
   - Partition variants: full_load/daily/monthly

2. **Transform + Post Asset** (`DSA/FHIR/{Resource}/{Resource}_IFDB.py`)
   - Create `_transform_{resource}_to_fhir()` function
   - Create `_post_{resource}_to_fhir()` function
   - Create full_load/daily/monthly asset variants
   - Use standard name pattern `fhir_{resource}_{partition}_ifdb`
   - Use standard group pattern `fhir_ifdb_{partition}`
   - **Multi-source naming**: When multiple source tables from the same source system
     populate the same FHIR resource, use `{Resource}_IFDB_{SourceTable}.py`
     (e.g. `Procedure_IFDB_AdminProc.py`, `Procedure_IFDB_RisYdelse.py`,
     `Observation_IFDB_Aktivitetsresultat.py`). Use the base `{Resource}_IFDB.py`
     name when only one source table exists for that source.

3. **Update `__init__.py` Files**
   - Export assets from `DSA/FHIR/{Resource}/__init__.py`
   - The resource directory's `__init__.py` collects assets from all source files (`*_IFDB.py`, `*_Cosmic.py`)
   - `DSA/FHIR/assets.py` aggregates all resource directories into `all_assets`

4. **Register Database Indexes** (`utils/fhir/index_config.py`)
   - Add identifier system to `IDENTIFIER_UNIQUE_INDEX_SPECS`
   - Add resource type to `TXID_BTREE_RESOURCES` and `SOF_TS_RESOURCES`
   - See [FHIR Database Indexes](#fhir-database-indexes) for details

5. **Optional: Domain-Specific Job**
   - Create job in `dagster_etl/jobs/` following the centralized pattern

### Code Organization

```
dagster_etl/
├── jobs/                      # Centralized job definitions
│   ├── __init__.py            # Exports all jobs
│   ├── fhir_ifdb_jobs.py      # FHIR IFDB jobs
│   ├── fhir_cosmic_jobs.py    # FHIR Cosmic jobs
│   ├── erp_jobs.py            # Procurement jobs
│   ├── security_jobs.py       # Security model jobs
│   ├── organization_jobs.py   # Organization dimension jobs
│   ├── shared_dimensions_jobs.py # Shared dimensions jobs
│   └── infrastructure_jobs.py # Infrastructure setup jobs
├── schedules/                 # Centralized schedule definitions
│   ├── __init__.py            # Exports all schedules
│   ├── fhir_schedules.py      # FHIR schedules
│   └── sql_schedules.py       # SQL domain schedules
├── DSI/
│   ├── connectors/            # Shared extraction abstractions + per-connector factories
│   │   ├── protocol.py        # ExtractionConfig protocol
│   │   ├── types.py           # PartitionType, TableType enums
│   │   ├── factory.py         # Generic create_extraction_assets()
│   │   ├── base.py            # SqlExtractor base class
│   │   ├── oracle/            # Oracle extractor + asset_factory + models
│   │   └── mssql/             # MSSQL extractor + asset_factory + models + bulk_copy + sql_builders
│   ├── oracle/                # IFDB configs (ifdb.py)
│   └── mssql/                 # ILS, SydAX, MySupply configs
├── DSA/
│   └── FHIR/
│       ├── assets.py          # Aggregates all_assets from resource directories
│       ├── {Resource}/        # PascalCase resource directories
│       │   ├── __init__.py    # Exports all_assets for this resource
│       │   ├── {Resource}_IFDB.py     # IFDB: transform + post (single source)
│       │   ├── {Resource}_IFDB_{Src}.py  # IFDB: multi-source variant (e.g. Procedure_IFDB_RisYdelse.py)
│       │   └── {Resource}_Cosmic.py   # Cosmic: transform + post (where applicable)
│       ├── SqlOnFhir/         # SQL on FHIR extraction
│       │   ├── {Resource}.py  # PascalCase SQL on FHIR asset files
│       │   └── view_definitions/
│       └── utils/             # Shared FHIR utilities
│           ├── ifdb/          # IFDB-specific (dependency_resolver, fhir_posting, transforms, etc.)
│           └── sql_on_fhir/   # SQL on FHIR-specific (sof_utils.py)
└── utils/
    └── fhir/
        └── identifier_systems.py  # Centralized identifier systems
```

## Key Utilities

### Reference Resolution
- **Module**: `DSA.FHIR.utils.reference_resolver`
- **Functions**:
  - `resolve_and_apply_resource_ids()` - For upsert operations
  - `resolve_and_apply_all_references()` - For reference resolution
- **Config Classes**: `ReferenceFieldConfig`, `NestedReferenceFieldConfig`

### Identifier Systems
- **Module**: `utils.fhir.identifier_systems`
- **Usage**: Import identifier system constants (e.g., `SYSTEM_IFDB_PATIENT_ID`)

### Posting Utilities
- **Module**: `DSA.FHIR.utils.ifdb.fhir_posting` - Direct database posting utilities
  - `post_resources_direct()` - Post resources to FHIR database using executemany()
  - `resolve_resource_ids_for_upsert()` - Resolve existing FHIR IDs for upsert
- **Module**: `core.resources.FHIRDatabaseResource.batch_upsert_resources()` - Low-level batch insertion
- **Config**: `DSA.FHIR.utils.ifdb.posting_config` - Posting configuration

### Deletion marking (`urn:fhir:isDeleted`)

One extension URL is used across **Patient**, **Encounter**, **EpisodeOfCare**, and **Observation**, and when the quality-check asset marks an IFDB orphan, so consumers can filter with a single FHIRPath expression:

`extension.where(url='urn:fhir:isDeleted').valueInteger = 1`

| `valueInteger` | Meaning |
|----------------|---------|
| 0 | Not deleted (IFDB `SLETTET` / `DELETED` absent or zero) |
| 1 | Deleted (source soft-delete flag, or quality check: row no longer in IFDB but resource still in FHIR) |

**Transforms** always emit the extension on IFDB-backed resources (`valueInteger` 0 or 1). For **Encounter** and **EpisodeOfCare**, when the source deletion flag is set, `DSA.FHIR.utils.status_mapping` maps FHIR `status` to `entered-in-error` in addition to the extension.

**Quality check**: `fhir_quality_check_deleted_resources` (`DSA/FHIR/DeletedResourcesCheck/DeletedResourcesCheck_IFDB.py`) loads IFDB-linked identifiers from the FHIR PostgreSQL store, checks existence in Oracle IFDB in chunks, and sets `urn:fhir:isDeleted=1` on orphans via direct JSONB `UPDATE` (no Python-side FHIR parsing).

**Code**: `EXTENSION_FHIR_IS_DELETED`, `build_is_deleted_extension()` in `utils/fhir/is_deleted_extension.py`.

## Example Implementation: CarePlan Kræftpakker

The CarePlan kræftpakker implementation demonstrates:
- Custom extract with patient-based delta logic
- Journey formation (business logic) integrated in transform+post asset
- Nested reference resolution for `activity.detail.performer`
- Mixed-type references (Organization + Practitioner)

**Extract Asset** (`DSI/oracle/ifdb.py` for eventmarker assets; `utils/eventmarker_utils.py` for shared EVENTMARKER SQL/delta logic):
- Uses patient-based delta: finds changed patients, extracts ALL their markers
- This ensures correct journey formation calculation

**Transform + Post Asset** (`DSA/FHIR/CarePlan/CarePlan_AlleMarkoerer_IFDB.py`):
- Includes journey formation logic (moved from separate staging asset)
- Defines reference configurations for top-level and nested references
- Uses `resolve_and_apply_all_references()` for reference resolution

## SQL on FHIR (SoF) -- Flattened Views for Analytics

After FHIR resources are posted to the Aidbox FHIR server, they are **projected into flat SQL tables** using the [SQL on FHIR v2](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/) specification. This enables downstream analytics and DW consumption without parsing nested FHIR JSON.

### How It Works

```
FHIR Server (Aidbox)  ->  ViewDefinition JSON  ->  Aidbox "sof" schema (PG view)
                                                            |
                                                   SOF Dagster asset
                                                            |
                                                   MSSQL fhir.* table (MERGE)
```

1. **ViewDefinition JSON files** (`DSA/FHIR/SqlOnFhir/view_definitions/*.json`) define which columns to extract from each FHIR resource type using FHIRPath expressions.
2. An **infrastructure asset** (`fhir_sof_infrastructure`, centralized in `infrastructure/assets/fhir_sof_infrastructure.py`) PUTs each ViewDefinition to Aidbox and calls `$materialize`, creating a SQL view in the Aidbox `sof` schema. Run `full_infrastructure_job` to set this up.
3. **Extraction assets** stream-query `sof.<view_name>` via direct PostgreSQL (`fetchmany`), joining with the raw resource table for `last_updated`/`created_at` timestamps.
4. Streamed rows are staged into MSSQL `#SofTemp` using `mssql-python` `bulkcopy()` (fallback: chunked `executemany`), then **MERGED** into `[fhir].<Table>`.

### SoF Load Strategy (Current Standard)

- Use `extract_and_merge_sof_view()` from `DSA/FHIR/utils/sql_on_fhir/sof_utils.py` as the single supported SoF load path.
- Do **not** materialize full SoF result sets in Python before insert.
- Keep inserts chunked and bounded in memory; create clustered index on `#SofTemp` PK columns before MERGE.

### Key Locations

| Component | Location |
|-----------|----------|
| ViewDefinition JSON files | `DSA/FHIR/SqlOnFhir/view_definitions/` |
| Infrastructure asset | `infrastructure/assets/fhir_sof_infrastructure.py` |
| Extraction + MERGE assets | `DSA/FHIR/SqlOnFhir/{Resource}.py` (PascalCase) |
| Shared utilities | `DSA/FHIR/utils/sql_on_fhir/sof_utils.py` |
| Resource config registry | `SOF_RESOURCE_CONFIGS` in `sof_utils.py` |
| MSSQL DDL (target tables) | `infrastructure/ddl/tables/fhir/01_tables.sql` |

### ViewDefinition JSON Format

Each file follows the SQL on FHIR `ViewDefinition` structure:

```json
{
  "resourceType": "ViewDefinition",
  "name": "encounter_view",
  "resource": "Encounter",
  "status": "active",
  "select": [
    {
      "column": [
        {"name": "encounter_id", "path": "id"},
        {"name": "patient_id", "path": "subject.id"}
      ]
    }
  ]
}
```

Key fields:
- **`name`**: Unique view name -- becomes `sof.<name>` in Aidbox and the key in `SOF_RESOURCE_CONFIGS`.
- **`resource`**: FHIR resource type to project.
- **`select[].column[]`**: Flat columns extracted via FHIRPath `path`.
- **`select[].forEach`** / **`forEachOrNull`**: Unnest arrays (one row per element). Parent columns cross-join with unnested rows.
- **`where[]`**: FHIRPath boolean filters (e.g., filter CarePlans by category).

### SOF_RESOURCE_CONFIGS Registry

Every ViewDefinition needs a corresponding entry in `SOF_RESOURCE_CONFIGS` (`sof_utils.py`):

```python
"encounter_view": {
    "view_name": "encounter_view",
    "resource_table": "encounter",       # Aidbox raw table (lowercase)
    "id_column": "encounter_id",         # SoF view column matching r.id
    "mssql_table": "Encounter",          # Target [fhir].<Table>
    "primary_keys": ["encounter_id"],    # MERGE ON columns
},
```

Optional: **`null_sentinel_columns`** -- `forEachOrNull` PK columns that may be NULL, replaced with `''` before MERGE.

### Asset Naming

| Component | Pattern | Example |
|-----------|---------|---------|
| Asset name | `sof_{view_short_name}_{partition}` | `sof_encounter_daily` |
| Group (daily) | `fhir_sof_daily` | -- |
| Group (monthly) | `fhir_sof_monthly` | -- |
| Infrastructure | `fhir_sof_infrastructure` (in `staging_infrastructure` group) | -- |

### MSSQL Target Tables

DDL is in `infrastructure/ddl/tables/fhir/01_tables.sql`. Each table lives in the `[fhir]` schema and includes:
- Resource-specific columns matching the ViewDefinition output
- `last_updated` / `created_at` (from Aidbox `r.ts` / `r.cts`)
- `DW_Timestamp_Insert` / `DW_Timestamp_Update` (audit)

### Adding a New SoF View

1. Create **ViewDefinition JSON** in `DSA/FHIR/SqlOnFhir/view_definitions/<name>.json`
2. Add **DDL** for the target table in `infrastructure/ddl/tables/fhir/01_tables.sql`
3. Add **config entry** to `SOF_RESOURCE_CONFIGS` in `DSA/FHIR/utils/sql_on_fhir/sof_utils.py`
4. Create **asset file** in `DSA/FHIR/SqlOnFhir/{Resource}.py` (PascalCase, daily + monthly)
5. **Register assets** in `DSA/FHIR/SqlOnFhir/__init__.py`
6. **Register indexes** in `utils/fhir/index_config.py` (see [FHIR Database Indexes](#fhir-database-indexes))

## FHIR Database Indexes

All index configuration is centralized in `utils/fhir/index_config.py`. When adding a new FHIR resource, register it in the relevant config lists and re-materialize the corresponding infrastructure asset.

| Config List | Index Created | Purpose | Asset to Materialize |
|---|---|---|---|
| `IDENTIFIER_UNIQUE_INDEX_SPECS` | GIN + UNIQUE on identifier system | Reference resolution and idempotent upserts | `fhir_ifdb_database_indexes` |
| `TXID_BTREE_RESOURCES` | Btree on `txid` | `MAX(txid)` lookup in `batch_upsert_resources` | `fhir_ifdb_database_indexes` |
| `SOF_TS_RESOURCES` | Btree on `(ts) INCLUDE (cts)` | Partition-window extraction for SoF views | `fhir_ifdb_database_indexes` |

## Architectural Principles

1. **Two-Asset Pattern**: Extract → Transform + Post (no separate staging asset)
2. **Identifier-Based References**: Transform creates identifiers, post resolves to FHIR IDs
3. **Configuration-Driven**: Reference resolution uses declarative configuration
4. **Partition Strategy Support**: All flows support full_load/daily/monthly partitions
5. **Standard Naming**: Consistent asset keys, names, and groups enable batch processing
6. **FHIR Standard Compliance**: Prefer standard fields, minimize extensions
7. **Centralized Identifiers**: All identifier systems in one file for consistency
8. **Shared Functions**: Transform and post functions shared across partition variants
9. **SQL on FHIR for Analytics**: FHIR resources are flattened via SoF ViewDefinitions into MSSQL `fhir.*` tables for downstream consumption

---

**Version**: 2.6
**Last Updated**: 2026-05-26

**Changes from v2.5**:
- Added "FHIR Database Indexes" section documenting the three index config lists in `utils/fhir/index_config.py` (`IDENTIFIER_UNIQUE_INDEX_SPECS`, `TXID_BTREE_RESOURCES`, `SOF_TS_RESOURCES`).
- Added index registration step to "Development Steps" and "Adding a New SoF View" checklists.

**Changes from v2.4**:
- Documented SoF as a streaming load path (`extract_and_merge_sof_view`) with bounded-memory PostgreSQL reads and MSSQL `bulkcopy` staging fallback to chunked `executemany`.
- Removed references to legacy full-materialization SoF load behavior for `fhir.*` table sync.

**Changes from v2.3**:
- Updated all file paths to reflect the restructured codebase: FHIR assets organized by resource type (`DSA/FHIR/{Resource}/{Resource}_{Source}.py`), FHIR utils consolidated under `DSA/FHIR/utils/{source}/`, SQL on FHIR assets use PascalCase (`DSA/FHIR/SqlOnFhir/{Resource}.py`)
- Removed references to old source-based directories (`DSA/FHIR/IFDB/assets/`, `DSA/FHIR/Cosmic/assets/`, `DSA/FHIR/SOF/assets/`)
- Updated `dependency_resolver` and `fhir_posting` module paths to `DSA.FHIR.utils.ifdb.*`

**Changes from v2.2**:
- Documented unified `urn:fhir:isDeleted` extension, transform behaviour, `entered-in-error` status for deleted Encounter/EpisodeOfCare, and `fhir_quality_check_deleted_resources` (Postgres + IFDB reconciliation).

**Changes from v2.1**:
- Renamed "bulk" partition terminology to "full_load" across all assets, groups, and jobs
- Documented shared DSI connector layer (`DSI/connectors/`) with Protocol and factories
- Updated all code examples to use full_load_partitions and _full_load naming

**Changes from v2.0**:
- Added SQL on FHIR (SoF) section: ViewDefinitions, extraction, MERGE into MSSQL
- Documented SoF key locations, config registry, asset naming, and new-view checklist

**Changes from v1**:
- Removed three-asset pattern (extract → staging → post)
- Adopted two-asset pattern (extract → transform+post)
- Updated naming conventions to match standard patterns
- Added guidance on asset key and group patterns
- Clarified when to use factory vs custom extract assets
