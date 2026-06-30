<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Dagster Development Guide

This guide explains how to develop data pipelines using Dagster in this project.

## Quick Start

```bash
# Start Dagster web server
cd dagster_etl
dagster dev

# Access UI at http://localhost:3000
```

## Project Structure (DSI/DSA/DSO)

The ETL pipelines follow a three-layer architecture:

```
dagster_etl/
├── definitions.py              # Central Dagster definitions (assets, jobs, schedules, resources)
├── core/                       # Shared infrastructure
│   ├── config.py              # Configuration management
│   ├── partitions.py          # Partition definitions (daily, monthly, full_load)
│   ├── resources.py           # Database and service resources
│   └── branch_postfix.py      # Multi-deployment feature branch postfixing
├── DSI/                        # Data Staging In (extract only)
│   ├── connectors/            # Shared extraction abstractions + per-connector factories
│   │   ├── protocol.py        # ExtractionConfig protocol (shared interface)
│   │   ├── types.py           # PartitionType, TableType enums
│   │   ├── factory.py         # Generic create_extraction_assets()
│   │   ├── base.py            # SqlExtractor base class, ExtractResult
│   │   ├── mssql/             # MSSQL extractor, asset_factory, models, bulk_copy, sql_builders
│   │   └── oracle/            # Oracle extractor, asset_factory, models
│   ├── mssql/                 # MSSQL source configs (ils.py, sydax.py, mysupply.py)
│   ├── oracle/                # Oracle source configs (ifdb.py)
│   └── organisation/          # Organisation codesystems and concept maps
│       ├── codesystems/
│       └── conceptmaps/
├── DSA/                        # Data Staging Area (staging + transforms)
│   ├── FHIR/                  # FHIR transformations
│   │   ├── assets.py          # Aggregates all FHIR assets for definitions.py
│   │   ├── {Resource}/        # PascalCase resource directories (Patient/, Encounter/, etc.)
│   │   │   ├── __init__.py    # Exports all_assets for the resource
│   │   │   ├── {Resource}_IFDB.py    # IFDB source: transform + post
│   │   │   └── {Resource}_Cosmic.py  # Cosmic source: transform + post (where applicable)
│   │   ├── SqlOnFhir/         # SQL on FHIR (ViewDefinitions, extraction → MSSQL fhir.*)
│   │   │   ├── {Resource}.py  # PascalCase SQL on FHIR asset files
│   │   │   └── view_definitions/  # ViewDefinition JSON files
│   │   └── utils/             # Shared FHIR utilities
│   │       ├── *.py           # bulk_importer, gdpr_masking, fhir_utils, etc.
│   │       ├── ifdb/          # IFDB-specific utils (dependency_resolver, fhir_posting, etc.)
│   │       └── sql_on_fhir/   # SQL on FHIR-specific utils (sof_utils.py)
│   └── SQL/                   # SQL staging + transforms (artifact-type-based, not domain-based)
│       ├── staging/           # All pre_stg_* and stg_* files across all domains
│       ├── map/               # All map-update files (MapUpd_*, map_organization)
│       ├── dim/               # All dimension-update files (DimUpd_*, dim_organization, borger)
│       ├── fct/               # All fact-update files (FctUpd_*)
│       ├── checks/            # Data quality checks
│       ├── cleanup.py         # Cross-cutting staging cleanup
│       └── index_maintenance.py  # Cross-cutting index rebuild
└── DSO/                        # Data Staging Out
    ├── load/                  # Target system sync
    │   ├── sync_configs.py    # Source of truth: which tables are synced
    │   ├── sync_utils.py      # Core sync logic (MERGE, linked server, fallback)
    │   ├── sync_state.py      # SyncStateManager for watermark tracking
    │   ├── sync_dim.py        # Dimension sync assets (ERP + shared dimensions)
    │   ├── sync_fct.py        # Fact sync assets
    │   └── sync_fhir.py       # FHIR flat-table sync assets
    └── (views managed via SQL DDL under infrastructure/ddl/views/)
```

### Layer Descriptions

| Layer | Purpose | Location |
|-------|---------|----------|
| **DSI** | 1:1 raw extraction to `ext.*` schema (no business logic, no CleanCodes) | `DSI/connectors/` (shared machinery), `DSI/mssql/` and `DSI/oracle/` (source configs), `DSI/organisation/` (codesystems/conceptmaps) |
| **DSA** | Staging and transforms (FHIR or SQL) | `DSA/SQL/staging/`, `DSA/SQL/map/`, `DSA/SQL/dim/`, `DSA/SQL/fct/`, `DSA/FHIR/{Resource}/` |
| **DSO** | Sync to reporting DB and presentation views | `DSO/load/sync_dim.py`, `sync_fct.py`, `sync_fhir.py`; views via `infrastructure/ddl/views/` |

### Design Principles

- **Artifact-type organization (not domain-based)**: DSA/SQL files are organized by artifact type (`staging/`, `map/`, `dim/`, `fct/`) rather than by domain. Domain ownership lives in Dagster configuration (groups, jobs, asset tags).
- **Resource-based FHIR organization**: DSA/FHIR files are organized by FHIR resource type (PascalCase directories like `Patient/`, `Encounter/`), with each source as a separate file (`Patient_IFDB.py`, `Patient_Cosmic.py`).
- **Connector-based extraction**: DSI separates reusable connector logic (`DSI/connectors/`) from source-specific configuration (`DSI/mssql/`, `DSI/oracle/`). One parameterized factory per connector type.

## Dagster Concepts

### Assets

Assets are the core building blocks representing data artifacts:

```python
import dagster as dg
from core.partitions import daily_partitions
from core.resources import IFDBOracleResource


@dg.asset(
    name="fhir_encounter_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "encounter_daily"])
        ),
    },
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
)
def fhir_encounter_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post Encounter resources for daily partition."""
    encounters = _transform_encounters_to_fhir(extracted_data, context)
    return _post_encounters_to_fhir(context, encounters, fhir_server, fhir_database)
```

### Partitions

Partitions enable incremental processing. Defined in `core/partitions.py`:

```python
from core.partitions import (
    daily_partitions,  # Daily incremental loads (since 2024-01-01)
    monthly_partitions,  # Monthly aggregations
    full_load_partitions,  # Full historical loads (single "full_load" partition)
)
```

Use partitions to:
- Process data incrementally by date
- Re-run specific time periods
- Manage backfills efficiently

### Jobs

Jobs group assets for coordinated execution:

```python
fhir_etl_ifdb_daily_job = dg.define_asset_job(
    name="fhir_etl_ifdb_daily_job",
    selection=fhir_infrastructure_assets
    | ifdb_extraction_daily_assets
    | fhir_ifdb_daily_assets,
)
```

### Schedules

Automate job execution on a cron schedule:

```python
@dg.schedule(
    cron_schedule="0 2 * * *",  # 2 AM daily
    job=fhir_etl_ifdb_daily_job,
)
def daily_fhir_schedule():
    return {}
```

### Resources

Resources provide shared connections and services. Defined in `core/resources.py` and composed in `definitions.py`:

```python
# MSSQL: staging (DSI/DSA) and DSO
"mssql_source_staging", "mssql_target_staging"  # Staging DB (read/write)
"mssql_source_dso", "mssql_target_dso"         # DSO/Report DB

# Source-specific MSSQL (extraction)
"mysupply_mssql", "sydax_mssql", "ils_mssql"

# Oracle (IFDB)
"ifdb_oracle": IFDBOracleResource()

# FHIR and terminology (from fhir_defs, terminology_defs)
```

Resources are injected into assets by parameter name:

```python
@dg.asset
def my_asset(context, ifdb_oracle: IFDBOracleResource):
    with ifdb_oracle.get_connection() as conn:
        pass
```

## Development Workflow

### 1. Create a New Asset

Choose the appropriate location based on what you are building:

| What | Location | Example Files |
|------|----------|---------------|
| FHIR transform+post | `DSA/FHIR/{Resource}/` | `Patient/Patient_IFDB.py`, `Encounter/Encounter_IFDB.py` |
| SQL staging (pre_stg/stg) | `DSA/SQL/staging/` | `pre_stg_postering_sydax.py`, `stg_dim_kreditor_ils.py` |
| SQL map updates | `DSA/SQL/map/` | `MapUpd_kreditor.py`, `map_organization.py` |
| SQL dim updates | `DSA/SQL/dim/` | `DimUpd_kreditor.py`, `dim_organization.py`, `borger.py` |
| SQL fact updates | `DSA/SQL/fct/` | `FctUpd_postering.py`, `FctUpd_fakturalinje.py` |
| MSSQL source config | `DSI/mssql/` | `ils.py`, `sydax.py`, `mysupply.py` |
| Oracle source config | `DSI/oracle/` | `ifdb.py` |
| DSO sync asset | `DSO/load/sync_dim.py` / `sync_fct.py` / `sync_fhir.py` | Inside existing files |
| DSO views | `infrastructure/ddl/views/erp/` / `infrastructure/ddl/views/security/` | SQL DDL files |
| SQL on FHIR flattening | `DSA/FHIR/SqlOnFhir/` | `Patient.py`, `Encounter.py` |

### 2. Define the Asset

```python
# DSA/FHIR/Patient/Patient_IFDB.py

import dagster as dg
from core.partitions import daily_partitions


@dg.asset(
    name="fhir_patient_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "patient_daily"])
        ),
    },
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
)
def fhir_patient_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    if not extracted_data:
        return dg.MaterializeResult(metadata={"count": 0})

    patients = _transform_patients_to_fhir(extracted_data, context)
    return _post_patients_to_fhir(context, patients, fhir_server, fhir_database)
```

### 3. Register in definitions.py

Assets are aggregated via module `__init__.py` exports, then imported in `definitions.py`:

```python
# definitions.py

# FHIR assets are collected by DSA/FHIR/assets.py which aggregates from resource directories
from DSA.FHIR.assets import all_assets as fhir_assets

# SQL assets are imported from artifact-type directories
from DSA.SQL.staging import all_assets as staging_assets
from DSA.SQL.dim import all_assets as dim_assets
from DSA.SQL.map import all_assets as map_assets
from DSA.SQL.fct import all_assets as fct_assets

# DSO sync (views are managed by infrastructure SQL DDL)
from DSO.load import all_dso_sync_dim_assets, all_dso_sync_fct_assets
from DSO.load.sync_fhir import all_fhir_dso_sync_assets

# DSI extraction
from DSI.mssql import all_extract_assets as mssql_extract_assets
from DSI.oracle import all_extraction_assets as oracle_extract_assets
```

### 4. Test Locally

```bash
# Start Dagster
dagster dev

# In UI:
# 1. Navigate to Assets
# 2. Find your asset
# 3. Click "Materialize" to run
```

## Asset Groups

| Group | Purpose | Domain |
|-------|---------|--------|
| `fhir_ifdb_daily` / `fhir_ifdb_monthly` / `fhir_ifdb_full_load` | IFDB → FHIR transforms | FHIR |
| `fhir_sof_daily` / `fhir_sof_monthly` | SQL-on-FHIR flattening (fhir.* tables) | FHIR |
| `fhir_dso_sync_daily` / `fhir_dso_sync_monthly` | FHIR tables sync to DSO | DSO |
| `erp_staging` / `erp_staging_daily` / `erp_staging_monthly` | Procurement extract & staging | DSI/DSA |
| `erp_maps` | Procurement map tables | DSA |
| `erp_dimensions` / `erp_facts` | Procurement dims & facts | DSA |
| `erp_dso_sync` | ERP sync to DSO | DSO |
| `erp_views` | ERP presentation views (SQL DDL) | Infrastructure |
| `dwh_organization` | Organization dimension | DSA |
| `shared_dimensions_dso_sync` | Shared dims sync to DSO | DSO |
| `staging_infrastructure` / `dso_infrastructure` | DDL and schema setup | Infrastructure |

## Common Commands

```bash
# Start development server
dagster dev

# Validate definitions (check for errors)
dagster definitions validate

# List all assets
dagster asset list

# Materialize specific asset
dagster asset materialize my_asset_name

# Materialize with upstream dependencies
dagster asset materialize my_asset --select +my_asset

# Run a job
dagster job execute -j fhir_etl_ifdb_daily_job

# Run job with specific partition
dagster job execute -j fhir_etl_ifdb_daily_job --partition 2024-12-01
```

## Dagster UI Features

### Asset Catalog
- View all assets and metadata
- Browse by group
- See asset descriptions and dependencies

### Asset Graph
- Visualize dependency chains
- Understand data lineage
- Identify upstream/downstream assets

### Runs
- Monitor pipeline executions
- View logs and errors
- Retry failed runs

### Launchpad
- Manually trigger jobs
- Configure run parameters
- Select specific partitions

## Troubleshooting

### Assets Not Loading

1. Check imports in `definitions.py`
2. Look for Python errors in Dagster logs
3. Verify `__init__.py` files export assets correctly
4. Run `dagster definitions validate`

### Resource Connection Errors

1. Check `.env` configuration
2. Verify network access to databases
3. Test connection in Python REPL:

```python
from core.resources import IFDBOracleResource

resource = IFDBOracleResource()
with resource.get_connection() as conn:
    print("Connected!")
```

### Partition Issues

1. Verify partition key format matches definition
2. Check partition range covers requested date
3. Ensure partition column exists in source data

### Import Errors

```bash
# Run from project root (parent of dagster_etl)
cd /path/to/healthpipe

# Or from dagster_etl - ensure parent is on PYTHONPATH for "definitions" module
cd dagster_etl
PYTHONPATH=.. dagster dev
```

## Testing

See the [Testing Guide](../../tests/README.md) for comprehensive testing documentation including:
- Unit tests with mocks
- Integration tests with real resources
- Patient-specific test workflows
- Test markers and configuration

Quick test commands:

```bash
# Run unit tests
pytest tests/unit/ -v

# Run integration tests
pytest tests/integration/ -v

# Run specific domain tests
pytest tests/integration/datawarehouse/ -v
pytest tests/integration/fhir_ifdb/ -v
```

## Multi-Deployment / Feature Branch Testing

The project supports deploying multiple feature branches simultaneously to the same Dagster instance. This enables developers to test their changes in isolation without conflicts.

### How It Works

When the `DAGSTER_BRANCH_POSTFIX` environment variable is set **and starts with "feature"**, all assets, jobs, and schedules are automatically postfixed with that value. This prevents name collisions between deployments.

**Important**: Only branches starting with "feature" get postfixed. Other branches (main, develop, hotfix-*, etc.) are NOT modified.

```
# Without postfix (production/main):
fhir_observation_daily_ifdb

# With DAGSTER_BRANCH_POSTFIX=feature-patient-validation:
fhir_observation_daily_ifdb_feature_patient_validation

# With DAGSTER_BRANCH_POSTFIX=main (NOT a feature branch - no change):
fhir_observation_daily_ifdb
```

### Setting Up a Feature Branch Deployment

1. **Set the environment variable** before starting Dagster:

```bash
# Using branch name (must start with "feature")
export DAGSTER_BRANCH_POSTFIX=feature-add-patient-validation

# Using feature ticket format
export DAGSTER_BRANCH_POSTFIX=feature-JIRA-1234

# Start Dagster
dagster dev
```

2. **In your deployment configuration** (e.g., Nomad job file):

```hcl
env {
  DAGSTER_BRANCH_POSTFIX = "feature-branch-name"
}
```

3. **In Docker Compose**:

```yaml
services:
  dagster:
    environment:
      - DAGSTER_BRANCH_POSTFIX=feature-branch-name
```

### Best Practices

| Practice | Description |
|----------|-------------|
| Use "feature-" prefix | Only branches starting with "feature" get postfixed |
| Keep names short | Shorter postfixes improve readability in the UI |
| Sanitize automatically | Special characters are converted to underscores |
| Production has no postfix | Leave `DAGSTER_BRANCH_POSTFIX` unset or use non-feature name |

### Postfix Sanitization

The postfix is automatically sanitized:
- Special characters → underscore
- Converted to lowercase
- Leading/trailing underscores removed

```
feature/add-patient     →  _feature_add_patient
Feature-JIRA-1234       →  _feature_jira_1234
feature.my.branch.name  →  _feature_my_branch_name
main                    →  (no postfix - not a feature branch)
hotfix-123              →  (no postfix - not a feature branch)
```

### Programmatic Access

You can access the branch postfix utilities in your code:

```python
from core.branch_postfix import (
    get_branch_postfix,  # Get current postfix
    is_feature_branch_deployment,  # Check if postfix is active
    get_postfixed_name,  # Postfix a string
    get_postfixed_asset_key,  # Postfix an AssetKey
)

if is_feature_branch_deployment():
    print(f"Running with postfix: {get_branch_postfix()}")

name = get_postfixed_name("my_asset")  # Returns "my_asset_feature_xyz" or "my_asset"
```

### Considerations

1. **Database isolation**: The postfix only affects Dagster asset/job names. You may also want to:
   - Use separate database schemas per branch
   - Use separate FHIR server instances
   - Configure different target databases via environment variables

2. **Resource sharing**: All deployments share the same resources (databases, FHIR servers) unless you also configure those separately.

3. **Cleanup**: Remember to tear down feature branch deployments when done to free resources.

4. **Non-feature branches**: Branches like `main`, `develop`, `hotfix-*` will NOT be postfixed, ensuring production deployments are unaffected.

## Related Documentation

- [FHIR ETL Guide](./fhir_etl_guide.md) - FHIR pipeline patterns and IFDB extraction
- [SQL ETL Guide](./sql_etl_guide.md) - DWH architecture and ETL patterns
- [DSO Sync Guide](./dso_sync_guide.md) - Syncing staging to DSO database
- [Data Lineage Guide](./data_lineage_guide.md) - YAML lineage structure
- [Testing Guide](../../tests/README.md) - Testing framework
- [Dagster Documentation](https://docs.dagster.io/) - Official Dagster docs
