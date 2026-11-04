<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Repository Tour

Orientation for a developer who has the environment running and needs to know
where things live and what the words mean. Read this before your first change;
the [Dagster Development Guide](dagster_development.md) then covers the day-to-day
run loop.

The data flow itself — DSI → DSA (FHIR and DWH tracks) → DSO → datamarts — is
drawn in [00 — Overblik](../basismodel/00_overblik.md#de-tre-lag). This document
maps that flow onto directories.

## Project structure

```
etl/
├── definitions.py              # Central Dagster definitions (assets, jobs, schedules, resources)
├── core/                       # Shared infrastructure
│   ├── config.py              # Configuration management
│   ├── partitions.py          # Partition definitions (daily, monthly, full_load)
│   ├── resources.py           # Database and service resources
│   ├── dfm_config.py          # DfmExtractConfig consumers, ArchiveMergeConfig, RunModeConfig
│   ├── dfm_manager.py         # Checkpoint read/write and run logging
│   ├── archive_merge.py       # ext.* → arc.* MERGE engine
│   └── branch_postfix.py      # Multi-deployment feature branch postfixing
├── DSI/                        # Data Staging In (extract only)
│   ├── connectors/            # Shared extraction abstractions + per-connector factories
│   │   ├── protocol.py        # ExtractionConfig protocol (shared interface)
│   │   ├── types.py           # PartitionType, TableType enums
│   │   ├── extract_config.py  # DfmExtractConfig dataclass
│   │   ├── factory.py         # create_dfm_extraction_assets()
│   │   ├── base.py            # SqlExtractor base class, ExtractResult
│   │   ├── mssql/             # MSSQL extractor, models, bulk_copy, sql_builders, parallel_extract
│   │   ├── oracle/            # Oracle extractor, models
│   │   └── csv_files/         # CSV extractor
│   ├── mssql/                 # MSSQL DFM configs per source
│   │   ├── extract/           # ext.* extract configs (ils, sydax, mysupply, kmd_uddata)
│   │   └── archive/           # arc.* archive configs (same sources)
│   ├── oracle/                # Oracle source configs + DFM extract/archive (ifdb.py, ifdb_extract.py, ifdb_archive.py)
│   ├── extract/               # Column contracts asserted by the Oracle schema-verification tests (IFDB/)
│   ├── csv_files/             # CSV-based sources (conceptmap_csv.py, shaksor_map.py)
│   └── organisation/          # Organisation codesystems and concept maps
├── DSA/                        # Data Staging Area (staging + transforms)
│   ├── FHIR/                  # FHIR transformations
│   │   ├── assets.py          # Aggregates all FHIR assets for definitions.py
│   │   ├── {Resource}/        # PascalCase resource directories (Patient/, Encounter/, ...)
│   │   │   ├── {Resource}_IFDB.py      # Classic partitioned transform + post
│   │   │   └── {Resource}_IFDB_dfm.py  # DFM checkpoint-based variant (transitional suffix)
│   │   ├── SqlOnFhir/         # SQL on FHIR (ViewDefinitions, extraction → MSSQL fhir.*)
│   │   └── utils/             # Shared FHIR utilities (+ ifdb/, sql_on_fhir/)
│   ├── CodeSystem/            # FHIR CodeSystem hierarchy builders + assets (codesystem.* schema)
│   ├── ConceptMap/            # FHIR ConceptMap mapping assets fra kuraterede CSV'er/crosswalks (conceptmap.* schema)
│   └── SQL/                   # SQL staging + transforms (artifact-type-based, not domain-based)
│       ├── staging/           # All pre_stg_* and stg_* files across all domains
│       ├── map/               # All map-update files (map_*.py)
│       ├── dim/               # All dimension-update files (dim_*.py, borger.py)
│       ├── fct/               # All fact-update files (fct_*.py + relation appliers)
│       ├── ref/               # Reference/relation tables
│       ├── dfm/               # DFM operations + period orchestrator assets
│       ├── checks/            # Data quality checks
│       └── dev/               # Dev-only utilities (gated out of prod)
├── DSO/                        # Data Staging Out
│   └── load/                  # Target system sync
│       ├── sync_configs.py    # Source of truth: which tables are synced
│       ├── sync_utils.py      # Core sync logic (MERGE, linked server, fallback)
│       ├── sync_state.py      # SyncStateManager for watermark tracking
│       └── sync_dim.py / sync_fct.py / sync_fhir.py
├── dqa/                        # Data Quality Assurance framework
├── data_quality/               # Standalone FHIR grain + deleted-resource checks (separate from dqa/)
├── data_lineage/               # YAML lineage per table (bridge/, dim/, dm/, fct/, fhir/) + tools
├── infrastructure/             # DDL setup assets (schemas, tables, views), security, seed data
├── migrations/                 # Tracked SQL migrations + migration runner asset
├── jobs/                       # Job definitions per domain
├── schedules/                  # Cron schedules per domain
├── sensors/                    # Run-status and failure sensors
├── sources/                    # Source-system query/config helpers (fhir/, mssql/, oracle/)
├── utils/                      # Shared utilities (fhir/, sql/, azure_devops/, sql_safety.py)
├── scripts/                    # Operational one-off scripts
├── tests/                      # Unit + integration tests (see tests/README.md)
└── docs/                       # Documentation (see docs/README.md)
```

Views are not in this tree: DSO presentation views are SQL DDL under
`infrastructure/ddl/views/`, executed by infrastructure assets.

### Layer summary

| Layer | Purpose | Where the code lives |
|-------|---------|----------------------|
| **DSI** | 1:1 raw extraction to `ext.*`, merged into `arc.*` (no business logic, no CleanCodes) | `DSI/connectors/` (shared machinery), `DSI/mssql/` and `DSI/oracle/` (source configs), `DSI/organisation/` |
| **DSA** | Staging and transforms, FHIR or SQL | `DSA/SQL/staging/`, `map/`, `dim/`, `fct/`; `DSA/FHIR/{Resource}/` |
| **DSO** | Sync to the reporting DB and presentation views | `DSO/load/sync_*.py`; views via `infrastructure/ddl/views/` |

### Design principles

- **Artifact-type organization, not domain-based.** `DSA/SQL/` files are grouped by
  artifact type (`staging/`, `map/`, `dim/`, `fct/`) rather than by business domain.
  Domain ownership lives in Dagster configuration — groups, jobs, asset tags.
- **Resource-based FHIR organization.** `DSA/FHIR/` is grouped by FHIR resource type
  (PascalCase directories), one file per source/variant: `Condition_IFDB.py`,
  `Patient_IFDB_dfm.py`.
- **Connector-based extraction.** DSI separates reusable connector logic
  (`DSI/connectors/`) from source-specific configuration (`DSI/mssql/`, `DSI/oracle/`).
  One parameterized factory per connector type.
- **Centralized infrastructure.** All DDL setup assets live in `infrastructure/`,
  never inside `DSA/`.

## Where a change goes

| What you are building | Location | Guide |
|---|---|---|
| Extract + archive from a source | `DSI/mssql/` or `DSI/oracle/` | [DSI ETL guide](dsi_etl_guide.md) |
| FHIR transform + post | `DSA/FHIR/{Resource}/` | [FHIR ETL guide](fhir_etl_guide.md) |
| SQL staging (`pre_stg`/`stg`) | `DSA/SQL/staging/` | [SQL ETL guide](sql_etl_guide.md) |
| Map / dim / fact update | `DSA/SQL/map/`, `dim/`, `fct/` | [SQL ETL guide](sql_etl_guide.md) |
| SQL on FHIR flattening | `DSA/FHIR/SqlOnFhir/` | [FHIR ETL guide](fhir_etl_guide.md#sql-on-fhir-sof----flattened-views-for-analytics) |
| CodeSystem / ConceptMap | `DSA/CodeSystem/`, `DSA/ConceptMap/` | [CodeSystem & ConceptMap guide](codesystem_conceptmap_guide.md) |
| DSO sync of a table | `DSO/load/sync_dim.py` / `sync_fct.py` / `sync_fhir.py` | [DSO sync guide](dso_sync_guide.md) |
| DSO view | `infrastructure/ddl/views/{domain}/` | [SQL ETL guide](sql_etl_guide.md#view-layer-responsibilities) |
| Table DDL | `infrastructure/ddl/tables/{domain}/` | [DSI ETL guide](dsi_etl_guide.md) |
| Data quality check | `dqa/config/` | [DQA README](../architecture/dqa/README.md) |
| Lineage documentation | `data_lineage/{bridge,dim,dm,fct,fhir}/` | [Data lineage guide](data_lineage_guide.md) |

## Dagster terminology, as used here

Dagster's own concepts are documented at [docs.dagster.io](https://docs.dagster.io/).
What follows is only how this project uses them.

| Term | In this project |
|---|---|
| **Asset** | One table, one FHIR resource load, or one DDL step. Named after what it produces (`ext_ils_vare`, `arc_ils_vare`, `fhir_patient_daily_ifdb`). |
| **Group** | The unit jobs select on. Named `{domain}_{layer}` — `ils_archive`, `erp_staging`, `fhir_ifdb_daily`. |
| **Job** | A named selection of groups, defined per domain in `jobs/`. |
| **Schedule** | A cron trigger for a job, defined per domain in `schedules/`. |
| **Resource** | A database or service connection, defined in `core/resources.py` and injected by parameter name. |
| **Partition** | Date-slicing for the classic FHIR pattern. Most new work uses DFM checkpoints instead — see below. |
| **Asset check** | A DQA check bound to an anchor asset; results land in `dqa.Results`. |
| **Config** | Per-run overrides. `RunModeConfig` is the important one: it switches an asset between `delta`, `full_load`, `period` and `ids` at launch time. |

### Partitions vs. DFM checkpoints

Two incremental-loading mechanisms coexist, and knowing which one you are in matters
more than any other piece of terminology here.

- **DFM checkpoints** — the standard for new work. An unpartitioned asset records
  "where it left off" in `meta.DFMState` and reads the delta itself. Re-running a
  period or a set of IDs is a launch-time config change, not a backfill. See
  [dfm_design.md](../architecture/dfm_design.md).
- **Dagster partitions** — the classic FHIR pattern, now **retired**. Assets were
  defined with `daily_partitions` / `monthly_partitions` / `full_load_partitions`
  from `core/partitions.py`, and re-running meant backfilling partitions. Those
  definitions are gone along with the last partitioned producer; no asset or job
  in the deployment carries a `partitions_def` any more. You will still meet this
  in older commits, and `core/partitions.py` keeps two key-format helpers the SQL
  extractors call with a `partition_key` that is always None on the DFM path.

The `_dfm` suffix on files and assets exists only to disambiguate during the
migration; it is now redundant.

### Assets

```python
@dg.asset(
    name="fhir_server_encounter_dfm",
    group_name="fhir_ifdb_dfm",
    deps=["arc_encounter_ifdb", "fhir_server_patient_dfm"],
)
def fhir_server_encounter_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    """Transform and post Encounter resources for daily partition."""
    encounters = _transform_encounters_to_fhir(extracted_data, context)
    return _post_encounters_to_fhir(context, encounters, fhir_server)
```

Resources arrive by **parameter name** — `ifdb_oracle` matches the key in the
`resources` dict in `definitions.py`.

### Resources

Defined in `core/resources.py`, composed in `definitions.py`:

```python
# MSSQL: staging (DSI/DSA) and DSO
"mssql_source_staging", "mssql_target_staging"   # Staging DB (read/write)
"mssql_source_dso", "mssql_target_dso"           # DSO / reporting DB

# Source-specific MSSQL (extraction)
"mysupply_mssql", "sydax_mssql", "ils_mssql", "kmd_uddata_mssql"

# Oracle (IFDB)
"ifdb_oracle"

# FHIR and terminology (from fhir_defs, terminology_defs)
```

### Jobs and schedules

```python
ifdb_dfm_daily_job = dg.define_asset_job(
    name="ifdb_dfm_daily_job",
    selection=ifdb_archive_assets | fhir_ifdb_dfm_assets | fhir_sof_dfm_assets | ...,
)

ifdb_dfm_daily_schedule = dg.ScheduleDefinition(
    job=ifdb_dfm_daily_job,
    cron_schedule="0 6 * * *",
    execution_timezone="Europe/Copenhagen",
)
```

## Asset groups

| Group | Purpose | Layer |
|-------|---------|-------|
| `fhir_ifdb_daily` / `fhir_ifdb_monthly` / `fhir_ifdb_full_load` | IFDB → FHIR transforms (classic partitioned) | DSA |
| `fhir_ifdb_dfm` | IFDB → FHIR transforms (DFM checkpoint-based) | DSA |
| `fhir_sof_daily` / `fhir_sof_monthly` | SQL-on-FHIR flattening (`fhir.*` tables) | DSA |
| `fhir_dso_sync_daily` / `fhir_dso_sync_monthly` | FHIR tables sync to DSO | DSO |
| `ils_archive` / `sydax_archive` / `mysupply_archive` / `kmd_uddata_archive` | DFM extract + archive per source | DSI |
| `erp_pre_staging` / `erp_staging` | Procurement pre-staging and staging | DSA |
| `erp_ref` / `erp_map` | Reference data and map tables | DSA |
| `erp_dimension` / `erp_fact` | Procurement dims and facts | DSA |
| `erp_dso_sync` / `shared_dimensions_dso_sync` | Sync to DSO | DSO |
| `erp_views` | ERP presentation views (SQL DDL) | Infrastructure |
| `dwh_organisation` / `dwh_okosted` / `dwh_shak` | Organisation, Økosted and SHAK dimensions | DSA |
| `udredningsret_extract` / `udredningsret_archive` / `udredningsret_borger` | SDS udredningsret: file-drop extract, archive merge on business keys, Patient stubs (`udredningsret_ingest_job`) | DSI / DSA |
| `udredningsret_pre_staging` / `udredningsret_staging` | SDS udredningsret publish chain (`udredningsret_publish_job`): scope table, pre-staging and staging | DSA |
| `udredningsret_map` | SDS udredningsret key minting (`map.Helbredsforloeb`, `Udredningsforloeb`, `UdredningsretKlassifikation`, `Forloebsmarkoer`, `UdredningsretMarkoer`), append-only | DSA |
| `udredningsret_dimension` | SDS udredningsret dims: `UdredningsretKlassifikation`, `UdredningsretEgenskaber` (shared PK), `Forloebsmarkoer` | DSA |
| `udredningsret_fact` | SDS udredningsret facts: `fct.Udredningsforloeb`, `fct.UdredningsretMarkoer` | DSA |
| `udredningsret_dso_sync` | SDS udredningsret CPR-free dim/fct sync to DSO (three dims, then `fct.Udredningsforloeb`, then `fct.UdredningsretMarkoer` — FK order on DSO); all watermark on `dw_timestamp_update`. No maps, no `arc`/`ext`/`meta` | DSO |
| `staging_infrastructure` / `dso_infrastructure` | DDL and schema setup | Infrastructure |
| `dqa_fhir` / `dqa_dwh` / `dqa_arc` / `dqa_dso` | Data quality checks per track | DQA |

## How assets reach definitions

Assets are aggregated by module `__init__.py` exports, then imported centrally.
A new asset that is not in one of these lists simply does not exist as far as
Dagster is concerned.

```python
# definitions.py
from DSA.FHIR.assets import all_assets as fhir_assets
from DSA.SQL.staging import all_assets as staging_assets
from DSA.SQL.dim import all_assets as dim_assets
from DSA.SQL.map import all_assets as map_assets
from DSA.SQL.fct import all_assets as fct_assets
from DSO.load import all_dso_sync_dim_assets, all_dso_sync_fct_assets
from DSI.mssql import all_archive_assets, all_factory_extract_assets
from DSI.oracle import all_epji_archive_assets, all_ifdb_archive_assets
```

## Conventions that apply everywhere

- **Absolute imports only**, from the package root: `core.*`, `DSA.*`, `DSI.*`,
  `DSO.*`, `utils.*`. Never relative imports, never the old `etl_pipelines.*` path.
- **Naming** follows [navngivningsstandard_basismodel.md](navngivningsstandard_basismodel.md) — tables in
  PascalCase, everything else snake_case, `dw_*` prefixes for warehouse columns.

## Related documentation

- [Dagster Development Guide](dagster_development.md) — the local run loop
- [Docs index & glossary](../README.md)
- [Testing Guide](../../tests/README.md)
- [Feature-branch deployments](feature_branch_deployments.md)
