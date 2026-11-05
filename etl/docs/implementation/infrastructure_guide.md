<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Infrastructure Guide — DDL, Migrations and Setup Assets

This guide describes `etl/infrastructure/`: how tables, views and functions get onto the
staging and DSO databases, how a schema change reaches the existing environments, and
which jobs run the setup. The other guides link here instead of repeating these rules.

Terms used below:

- **Setup asset**: a Dagster asset that creates or updates database objects or server
  configuration. A setup asset must be safe to run again, so every statement is guarded
  (`IF NOT EXISTS`, `CREATE OR ALTER`). The exception is `fhir_structure_definitions`
  (see [section 9](#9-jobs-and-deploy)).
- **Base DDL**: the `.sql` files under `ddl/tables/`, `ddl/sources/` and `ddl/views/`. They describe the
  current shape of each object. A fresh database gets only the base DDL.
- **Migration**: a `.sql` file under `ddl/migrations/` that changes an object that already
  exists in an environment. Each database runs each migration once.
- **Staging** and **DSO**: the two MSSQL databases. Staging holds the whole pipeline
  (`ext`, `arc`, `stg`, `map`, `dim`, `fct`, ...). DSO holds what consumers read.

## 1. What the folder owns

`etl/infrastructure/` owns setup:

- MSSQL DDL on staging and DSO (schemas, functions, tables, views, migrations, seeds)
- the linked server from DSO to staging and the indexes that the DSO sync needs
- FHIR server and Aidbox setup (SQL on FHIR ViewDefinitions, PostgreSQL indexes,
  terminology published to the FHIR server)
- the AD security load (`security/`)
- disk space monitoring of the two databases (`db_space.py`, `assets/db_space.py`) and
  index maintenance (`assets/index_maintenance.py`)

It does not own pipeline steps or operations. Put those beside the code they operate on:

| Kind of asset | Where it goes | Examples |
|---|---|---|
| Operation of a pipeline (a meta-asset, a switch) | the domain folder of that pipeline | `DSA/SQL/dfm/ifdb_fhir_fuldload.py`, `DSO/load/sfi_consumers_absent.py` |
| Ad hoc, temporary, benchmark or one-off asset, or a reset across layers | `etl/adhoc/` | `adhoc/sof_merge_benchmark.py`, `adhoc/migrate_dso_sync_watermarks.py`, `adhoc/sfi_genkoersel_nulstilling.py` |

An asset in `etl/adhoc/` never joins a setup group. Most of them run only when an operator
materialises them or starts their own job. One is also a step of an operation:
`ifdb_fhir_fuldload` runs `sfi_genkoersel_verifikation`. Before you delete an asset, `git grep`
its key and remove every reference to it.

## 2. Folder map

| Path | Contents |
|---|---|
| `assets/staging_infrastructure.py` | one `staging_infra_{domain}` asset per domain, plus `staging_migrations` |
| `assets/dso_infrastructure.py` | one `dso_infra_{domain}` asset per domain that DSO needs, the `dso_infra_views_{domain}` assets, plus `dso_migrations` |
| `assets/*.py` (other) | linked server, watermark indexes, FHIR and Aidbox setup, index maintenance, disk space, AD load |
| `utils/ddl_loader.py` | the only loader: `execute_sql`, `execute_sql_file`, `execute_sql_files_from_directory`, `apply_migrations` |
| `ddl/common/` | schemas (`00_schemas.sql`, the same on both databases), database settings, functions |
| `ddl/tables/{domain}/` | base DDL for tables, numbered `NN_name.sql` |
| `ddl/tables/{domain}/dso/` | base DDL for a DSO table whose shape differs from the staging table (today only `conceptmap/dso/`) |
| `ddl/sources/{source}/` | archive DDL of one source on staging, one file per table, named after the table (`sources/ils/ils_vare.sql`) |
| `ddl/views/{domain}/` | view DDL, `CREATE OR ALTER VIEW` |
| `ddl/migrations/` | all migrations, for both databases |
| `seed/*.yml` | seed data that people review as data, and code rules that Python reads |
| `security/` | the AD export and load libraries |

## 3. How DDL reaches a database

Each domain has an **asset pair**: `staging_infra_{domain}` in the `staging_infrastructure`
group and, when DSO needs the domain, `dso_infra_{domain}` in the `dso_infrastructure` group.
The resource key selects the database: `mssql_target_staging` or `mssql_target_dso`.

- `execute_sql_files_from_directory(resource, "tables/{domain}", context)` runs every
  `*.sql` file directly in the folder, sorted by file name. It does not read subfolders.
- `execute_sql_file(resource, "tables/{domain}/NN_name.sql", context)` runs one file.
- `execute_sql` splits a file on `GO`, and runs and commits each batch on its own. It
  reads every result set of a batch, so an error in any statement of the batch raises.

The `NN_` prefix sets the order inside a domain. The numbers and the file set differ per
domain, so read the domain folder before you add a file. The base DDL must be safe to run
again: wrap each `CREATE TABLE` in `IF OBJECT_ID(...) IS NULL`, and use `CREATE OR ALTER`
for views and functions.

## 4. Staging and DSO

DSO is for what consumers read: `dim`, `fct`, `bridge`, `dm`, `ref`, `fhir`, `codesystem`,
`conceptmap`, `sec`, the `meta` tables that DSO itself uses, and the views. A new domain
puts no `arc`, `map` or `ext` table on DSO. Map tables mint the `dw_ek_*` keys on staging,
and `arc` and `ext` are pipeline internals.

The rule for a new domain:

- The staging asset loads the whole domain folder.
- The DSO asset loads an **explicit file list** with only the files DSO serves. Models:
  `dso_infra_drg`, `dso_infra_haiba`, `dso_infra_udredningsret`.
- A table that has a different shape on DSO gets its own file in `tables/{domain}/dso/`,
  loaded by the DSO asset (model: `dso_infra_conceptmap`).

Most older DSO assets load the whole domain folder: `dso_infra_erp`, `dso_infra_meta`,
`dso_infra_ref`, `dso_infra_organisation`, `dso_infra_security`, `dso_infra_codesystem`,
`dso_infra_etl_routing`, `dso_infra_fhir` and `dso_infra_shared_dimensions`. Some of them
pass `skip_schemas=DSO_EXCLUDED_SCHEMAS`, so DSO skips every GO batch that names an `ext`, `arc`,
`map` or `stg` object (`dso_infra_erp`, `dso_infra_organisation`, `dso_infra_codesystem`). Do not copy
them for a new domain. When you add a file to one of their folders, the file also reaches DSO.

## 5. Migrations

Migration files in `infrastructure/ddl/migrations/` are applied at most once per DATABASE by the `staging_migrations` and `dso_migrations` assets, tracked by filename in `[meta].[MigrationHistory]` (see `execute_migrations` in `infrastructure/utils/ddl_loader.py`). The shared `migrations/` directory runs against BOTH the staging and DSO databases, each with its own tracker — a file must run cleanly (or no-op) on both. On DSO, `dso_migrations` skips every GO batch that names an object in `DSO_EXCLUDED_SCHEMAS` (`ext`, `arc`, `map`, `stg`), and records the file as applied even when it skips every batch; so keep a `dim`/`fct` change out of a batch that also names one of those schemas. Idempotency is still required — defense in depth on servers that predate the tracker, and after a partial failure (the tracker row is written after the file's SQL commits, so a crash in between re-runs the whole file). Write them safe to re-run after applying, after columns are renamed by later migrations, and on fresh databases. Once applied to all environments, migration files can be removed — leftover tracker rows are harmless.

File name: `yyyymmdd_descriptive_name.sql`. A letter after the date orders two files of
the same day (`20260827b_...`). Never rename a file after it is applied: the tracker keys on
the file name, so a renamed file runs again.

When only one database loads the base DDL of an object, guard the migration with
`DB_NAME()` (for example `IF DB_NAME() LIKE N'%staging%'`). Without the guard the migration
also creates the object on the other database.

Hard correctness rules (the FIRST run fails or silently no-ops without these):

- Wrap column guards in a table-existence check (see "Table-existence guards" below).
- Static DML referencing potentially-renamed columns fails at T-SQL compile time — use dynamic SQL (see "Compile-time column validation" below).
- A `DECLARE ... CURSOR` loop is allowed. `execute_sql` reads every result set of a batch, so the whole loop runs, and an error in any iteration fails the batch and rolls it back. Prefer flat guarded `IF ... EXEC` blocks when they are enough.
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

1. Column references in DML use dynamic SQL or a `sys.columns` lookup into a variable?
2. Every `sp_rename` checks that the NEW name does NOT already exist?
3. Every `ALTER TABLE ADD` checks for BOTH old and new column names?
4. Table name guards handle both pre- and post-rename names (`COALESCE(OBJECT_ID(old), OBJECT_ID(new))`)?
5. Auto-named DEFAULT constraints dropped before dropping columns?
6. Runs cleanly against BOTH the staging and DSO databases (or no-ops where the objects don't exist)?
7. Column guards wrapped in a table-existence check (`IF OBJECT_ID(...) IS NOT NULL AND ...`)?
8. Schema change landed in BOTH the migration AND the base `tables/{domain}/` or `sources/{source}/` DDL, same commit?
9. Run `staging_migrations` twice locally — the second run succeeds (skipped by the tracker, and the file stays re-runnable if the tracker row is ever cleared)?

## 6. Seeds

- Static rows (the `-1` UNKNOWN row of a dimension, fixed code lists) go in
  `tables/{domain}/NN_seeds.sql` as guarded `INSERT` or `MERGE` statements.
- Use a YAML file in `seed/` and a seed asset only when people review the rules as data,
  or when Python also reads the file. Model: `ifdb_aktivitetstype_routing.yml`, which
  `staging_infra_etl_routing_seed` merges into its table.

## 7. SQL in Python

Keep static DDL in `.sql` files under `ddl/`. Build SQL in Python only when it depends on
configuration or secrets: the linked server and its staging login (secrets from the
environment), the watermark indexes (generated from `DSO/load/sync_configs.py`) and the
Aidbox PostgreSQL indexes.

## 8. Add a new domain

1. Add the base DDL in `ddl/tables/{domain}/` (and `ddl/views/{domain}/` for views).
2. Add `staging_infra_{domain}` to `assets/staging_infrastructure.py`. When DSO needs the
   domain, add `dso_infra_{domain}` with an explicit file list to
   `assets/dso_infrastructure.py`.
3. Import the new assets from their modules in `infrastructure/__init__.py`, and add them
   to `all_staging_assets` / `all_dso_assets`.

The archive tables of a new source follow the same chain with one staging asset,
`staging_infra_source_{source}`, that executes `ddl/sources/{source}/`. A source needs no DSO asset.

A domain asset always depends on the schemas and on the migrations of its database, so a
migration never runs after the base DDL that it patches:

```python
@dg.asset(
    name="staging_infra_source_mysource",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=["staging_infra_schemas", "staging_migrations"],
    description="Create arc.* tables for the MySource archive.",
)
def staging_infra_source_mysource(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    results = execute_sql_files_from_directory(mssql_target_staging, "sources/mysource", context)
    return dg.MaterializeResult(metadata={"files_executed": len(results)})
```

The DSO asset uses `deps=["dso_infra_schemas", "dso_migrations"]`. Add a `deps` entry for
each other domain whose tables your DDL references.

Groups: MSSQL DDL goes in `staging_infrastructure` or `dso_infrastructure`, never in a
domain group. Aidbox and PostgreSQL setup goes in `fhir_infrastructure`. Terminology that
is published to the FHIR server goes in `fhir_setup`.

## 9. Jobs and deploy

| Job | Selects |
|---|---|
| `full_infrastructure_setup` | the groups `staging_infrastructure`, `dso_infrastructure` and `fhir_infrastructure` |
| `staging_infrastructure_setup`, `dso_infrastructure_setup` | one of the two MSSQL groups |
| `erp_infrastructure_setup` | both MSSQL groups |
| `dfm_infrastructure_setup` | the DFM tables and the IFDB archive tables |
| `linked_server_setup` | the linked server, daily |
| `fhir_codesystem_job` | the `fhir_setup` assets except `fhir_structure_definitions`, with the SKS, SOR and SHAK extract, codesystem and DSO sync assets |

**Release contract.** On a `release-*` (QA) or `main-*` (production) deployment the
release infrastructure sensor runs `full_infrastructure_setup` once per release
(`sensors/release_infrastructure_sensor.py`). Until that run succeeds, every schedule tick
of the code location skips or fails. A new setup asset in one of the three groups therefore
runs on the next release without manual work. Development, feature and local code
locations are not gated: there you run `full_infrastructure_setup` yourself.

These assets run only by hand:

- `fhir_structure_definitions` posts the custom extension StructureDefinitions without an
  id, so each run adds another copy on the FHIR server. Run it once on a new FHIR server.
- `erp_index_maintenance` rebuilds fragmented ERP indexes (see
  [sql_etl_guide.md](sql_etl_guide.md#rebuild-indexes-after-batch-merge-operations)).
- `migrate_dso_sync_watermarks_to_dfm` (in `etl/adhoc/`, started with
  `migrate_dso_sync_watermarks_to_dfm_job`) is the one-off cutover from DSO
  `SyncWatermarks` to `DFMState` (see
  [dfm_operations_guide.md](dfm_operations_guide.md)).

The MR description must name the setup assets that a deployer must materialise before the
first scheduled run, and every migration that deletes or truncates DSO data, with what
publishes the data again.

## 10. Guard tests

| Test | Checks |
|---|---|
| `tests/unit/test_ddl_folder_conventions.py` | no `CREATE VIEW` under `tables/`; no suffix folders such as `conceptmap_dso` |
| `tests/unit/test_migration_structure.py` | no `migrations/` folder under `tables/`; no PostgreSQL SQL in `ddl/migrations/` |
| `tests/unit/test_infra_migrations_ordering.py` | every asset in the two main asset files that reads `tables/` depends on its `*_migrations` asset |

Run `staging_migrations` and `dso_migrations` twice against a local SQL Server for every
new migration. The second run must succeed.
