<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Upgrade guide

This guide tells you how to upgrade a Healthpipe deployment from one release to a newer
release. It also applies to an upgrade over more than one release, and to a release with
**Breaking:** entries in the [CHANGELOG](../../CHANGELOG.md). The last section tells you
how to go back to the previous release.

The guide uses these terms:

- **Release deployment**: a code location where `DAGSTER_BRANCH_POSTFIX` starts with
  `release-` or `main-`.
- **Setup run**: a run of the job `full_infrastructure_setup`.
- **Backup set**: the backups of all the databases in section 3.1, made in one window
  while no run writes.

## 1. What runs automatically

### 1.1 Deployment kinds

Two environment variables of the code location control the schedules, the sensors and
the release gate. The code compares the values without case and without spaces at the
ends.

| Code location | Schedules and sensors start by default | Release gate |
|---|---|---|
| `DAGSTER_BRANCH_POSTFIX` starts with `release-` or `main-` | Yes | Yes |
| `DAGSTER_CODE_LOCATION_NAME` is `development`, `production` or `prod`, and no `release-` or `main-` postfix | Yes | No |
| All other code locations | No | No |

The source is [`schedules/schedule_default_status.py`](../schedules/schedule_default_status.py)
and [`sensors/release_infrastructure_sensor.py`](../sensors/release_infrastructure_sensor.py).
Some schedules and sensors are always stopped by default, for example
`ifdb_fhir_fuldload_schedule`.

Dagster uses the default status only until you start or stop a schedule or a sensor in
the UI. After that, Dagster keeps the status that you set. Dagster keeps this status for
each code location. A code location with a new name starts again from the default status.

WARNING: A code location with `DAGSTER_CODE_LOCATION_NAME` set to `development`, `production`
or `prod`, and no `release-` or `main-` postfix, starts its schedules. It has no release
gate. A schedule can then run the new code before the migrations. On this kind of code
location, stop the schedules before the deploy and start the setup run yourself.

### 1.2 The release gate

On a release deployment, the sensor `release_infrastructure_sensor` starts one setup run
for each release:

- The release identity is `<DAGSTER_BRANCH_POSTFIX>@<code hash>`. The code hash comes from
  the `.py`, `.sql` and `.json` files under `etl/`. Thus, new code gives a new identity,
  also when the postfix does not change.
- The setup run has the run key and the tag `release=<identity>`.
- Each schedule of the code location checks for a successful setup run with this tag.
  While the setup run is queued or in progress, each tick skips.
- When the setup run failed or did not start, each tick fails. A tick also fails when the
  setup run is in progress for more than 3 hours. The error names the release.
- Dagster does not retry a tick that skips or fails. Thus, a scheduled run that falls in
  this window does not run.
- `release_infrastructure_failure_sensor` logs an error when the setup run fails.
- To open the gate after a failure, re-execute the failed run. The new run keeps the
  `release` tag.

The gate applies only to schedules. The sensors that start jobs, for example
`os2mo_delivery_sensor` and `drg_queue_sensor`, do not wait for the setup run.

On all other code locations, nothing starts the setup run. Start it yourself. The
[infrastructure guide](implementation/infrastructure_guide.md#9-jobs-and-deploy) describes
the setup jobs.

### 1.3 The migration runner

`full_infrastructure_setup` selects the asset groups `staging_infrastructure`,
`dso_infrastructure` and `fhir_infrastructure`. The assets `staging_migrations` and
`dso_migrations` run the migrations:

- Each SQL file in `etl/infrastructure/ddl/migrations/` runs once on each database, in the
  order of the file names.
- `[meta].[MigrationHistory]` on each database records the file name and the time of each
  applied file.
- The runner stops at the first error. The files before the error stay recorded. The files
  after the error do not run.
- Each `GO` batch commits on its own, and the runner records the file after its last batch.
  Thus, a failed file can leave a part of its changes. The next setup run runs the whole
  file again. The rules for a migration require that it is safe to run again.
- The runner tracks the file name. A file with a new name runs again on each database.
- On the DSO database, the runner skips each batch that names an object in the schemas
  `ext`, `arc`, `map` or `stg`. It records the file also when it skips all its batches.

The source is `apply_migrations` and `execute_migrations` in
[`infrastructure/utils/ddl_loader.py`](../infrastructure/utils/ddl_loader.py). The
[infrastructure guide](implementation/infrastructure_guide.md#5-migrations) gives the
rules for a migration.

There are no down migrations. Some migrations drop or empty data, or reset DFM
checkpoints. Examples from the CHANGELOG:

- 1.1.0: a migration drops the schemas `ext`, `arc`, `map` and `stg` from the DSO database.
- 1.1.0: a migration resets the archive checkpoints of ten ILS tables.
- 1.0.5: a migration empties the DRG tables in staging and in the DSO database.

### 1.4 What does not run automatically

You do these tasks yourself:

- the backups
- `dagster instance migrate` when the Dagster version changes
- the stop and the start of the schedules and the sensors
- the environment variables that a release adds or changes
- the operator tasks in the CHANGELOG, for example a schedule with a new name
- the assets in the group `fhir_setup`. The setup run does not select them. `fhir_codesystem_job`
  runs them on its weekly schedule, except `fhir_structure_definitions`.
- the assets that run only by hand, for example `fhir_structure_definitions`. The
  [infrastructure guide](implementation/infrastructure_guide.md#9-jobs-and-deploy) lists them.

## 2. Plan the upgrade

Do these steps some days before the upgrade:

1. Read the CHANGELOG section of each release after your current release. Include the
   target release. Read the release notes on the GitHub releases page.
2. For each **Breaking:** entry, write down the task that it gives. Examples:
   - 1.1.0: set the `MSSQL_LINKED_SERVER_*` variables before a DSO sync runs.
   - 1.0.6: set all the variables in `.env.template`.
   - 1.0.8: change each query that reads a DSO `dw_timestamp_*` value as Danish local time.
3. Tell the users of the DSO database about each change to a table, a column or a value.
4. Find the new environment variables. Compare `.env.template` between the two release tags.
   The [configuration reference](configuration.md) describes each variable. In the commands
   of this section, replace `v1.1.0` with the tag of your current release. Replace `v1.1.1`
   with the tag of the target release.

   ```bash
   git diff v1.1.0 v1.1.1 -- .env.template
   ```

5. List the migrations of the upgrade:

   ```bash
   git diff --name-status v1.1.0 v1.1.1 -- etl/infrastructure/ddl/migrations/
   ```

   - An `A` line is a new migration.
   - An `R` line is a migration with a new name. It runs again.
   - A `D` line is a removed migration. Make sure that `[meta].[MigrationHistory]` on staging
     and on DSO has this file name. If a database does not have it, first upgrade to a
     release that still has the file.

6. Find out if the Dagster version changes. Compare the `dagster==` line in
   `pyproject.toml`:

   ```bash
   git diff v1.1.0 v1.1.1 -- pyproject.toml
   ```

7. Find out if the Python packages change. `code-server-base.Dockerfile`
   installs the packages from `pyproject.toml` and `uv.lock`. If one of these files
   changes, build the base image again.
8. Choose a time window outside the schedule times. Include the time of the backups and of
   the setup run.

## 3. Upgrade procedure

### 3.1 Back up

Make the backup set when no run writes. Include these databases:

| Database | What it holds for the upgrade | Commands |
|---|---|---|
| Dagster PostgreSQL | The runs, the event log, the schedule and sensor status. The compute logs are files. | [Operations guide, section 4](operations.md#4-backup-and-restore-of-the-dagster-database) |
| Staging (SQL Server) | The archive and the model. The DFM state in `[meta].[DFMState]` and `[meta].[DFMRunLog]`. `[meta].[MigrationHistory]`. | The backup of your SQL Server platform |
| DSO (SQL Server) | The tables that the consumers read. Its own `[meta]` tables and `[meta].[MigrationHistory]`. | The backup of your SQL Server platform |
| FHIR server database (Aidbox PostgreSQL) | The FHIR resources. The indexes and the SQL on FHIR ViewDefinitions from `fhir_infrastructure`. | `pg_dump`, as for the Dagster database |

The DFM state of the DSO sync is in `[meta].[DFMState]` on staging, with `scope='dso'`. The
checkpoints of the FHIR server loads are also in `[meta].[DFMState]` on staging. Thus, the
staging backup holds the positions of the DSO data and of the FHIR server data.

Also keep a copy of these items:

- the environment variables of the code location
- the image tags of the current release

### 3.2 Steps

CAUTION: Do not start the upgrade while a run is in progress. A run can write to the
databases during the backup.

WARNING: If the new release has a code location with a new name, Dagster does not keep
the status from steps 1 and 2. The schedules and the sensors start with their default
status. The release gate holds only the schedules, until the setup run succeeds.

1. In the Dagster UI, stop all the schedules.
2. Stop all the sensors. On a release deployment, do not stop `release_infrastructure_sensor`
   and `release_infrastructure_failure_sensor`.
3. Wait until no run is in progress, or terminate the runs.
4. Make the backup set (section 3.1).
5. Set the new and the changed environment variables of the code location.
6. If the Dagster version changes:
   1. Stop the Dagster daemon and the webserver.
   2. Set `DAGSTER_HOME` to the folder with the `dagster.yaml` of your instance.
   3. Run `dagster instance migrate` with the new Dagster version.
   4. Start the webserver and the daemon with the new Dagster version. The repository does
      not supply their images.
7. Deploy the code-server image of the new release.
8. Let the setup run complete:
   - On a release deployment, the sensor starts the setup run. Make sure that the Dagster
     daemon runs.
   - On all other code locations, launch `full_infrastructure_setup` in the Dagster UI.
9. Verify the upgrade (section 3.3).
10. Do the operator tasks from the CHANGELOG that come after the setup run.
11. Start the schedules and the sensors that you stopped. Start the schedules with new
    names, if you used the old schedules.
12. Look at the first scheduled runs (section 3.4).

### 3.3 Verify

Do these checks before you start the schedules:

1. Do the health checks of the webserver and the daemons in the
   [operations guide](operations.md#2-health-checks).
2. Make sure that the code location loads without errors in the Dagster UI.
3. Make sure that the setup run has the status `SUCCESS`. On a release deployment, find the
   run with the tag `release` and the new identity.
4. Open the materialization of `staging_migrations` and of `dso_migrations`. The metadata
   `applied` lists the files that ran. Compare it with the list from section 2, step 5.
5. Query the migration tracker on staging and on DSO:

   ```sql
   SELECT TOP 20 filename, applied_at
   FROM [meta].[MigrationHistory]
   ORDER BY applied_at DESC;
   ```

### 3.4 After the start

Look at the first runs of each schedule:

- In the Dagster UI, look for failed ticks and failed runs.
- Query the failed DFM runs on staging:

  ```sql
  SELECT TOP 50 scope, schema_name, table_name, mode, error_message, run_started_at
  FROM [meta].[DFMRunLog]
  WHERE status = 'failed'
  ORDER BY run_started_at DESC;
  ```

- Look for the DSO sync markers in the logs. The
  [operations guide](operations.md#12-log-format) lists them.

A tick that skipped or failed during the upgrade does not run again. If you need its run,
launch the job yourself.

## 4. If the setup run fails

1. Read the error of the failed step in the run view.
2. Correct the cause, for example an environment variable that is not set, or a database
   permission.
3. Re-execute the failed run. On a release deployment, the new run keeps the `release` tag.
   When it succeeds, the schedules can run.

A failed migration does not roll back the batches that it committed. The next setup run
runs the whole file again. Do not deploy the previous image at this point. Read section 5.

## 5. Roll back

### 5.1 Rollback policy

The migrations go forward only. Some migrations drop or empty data, or move DFM
checkpoints. The previous code does not know the new schema.

WARNING: A rollback of the code only, after a run of the new release started, is not
supported. Restore the backup set, then deploy the previous release.

When no run of the new release started, the SQL Server and FHIR databases did not
change. Then you can deploy the previous image without a restore of these databases. If you
ran `dagster instance migrate`, restore the Dagster database.

### 5.2 Restore procedure

CAUTION: The restore replaces all the data that the pipeline wrote after the backup. The
next delta runs start from the restored checkpoints. Make sure that the sources still have
the data from after the backup time.

1. Stop all the schedules and the sensors.
2. Terminate the runs that are in progress.
3. Stop the Dagster daemon, the webserver and the code server.
4. Stop the FHIR server.
5. Restore the staging database.
6. Restore the DSO database.
7. Restore the FHIR server database.
8. Restore the Dagster database. The
   [operations guide](operations.md#42-restore) gives the commands. This step is necessary
   when you ran `dagster instance migrate`.
9. Set the environment variables of the previous release.
10. Deploy the previous images. Use the previous Dagster version for the webserver and the
    daemon, if it changed.
11. Start the FHIR server, the webserver, the daemon and the code server.
12. Verify (section 3.3). On a release deployment, the restored Dagster database has the
    successful setup run of the previous release identity. Then the gate opens without a new
    setup run. If the identity is different, the sensor starts a new setup run.
13. Start the schedules and the sensors.

Restore all databases from the same backup set. Do not start a run before all the restores
are complete. When nothing runs, the order of steps 5 to 8 has no effect on the result.

### 5.3 Why the databases must match

The DFM checkpoints on staging tell each job where to continue:

- The DSO sync reads the staging rows after its checkpoint (`scope='dso'`) and merges them
  into DSO.
- The FHIR server loads post the resources after their checkpoints.

If a target database is older than its checkpoint, the next run does not read the rows in
between. The target then does not have these rows. If a target database is newer than its
checkpoint, the next run reads some rows again.

### 5.4 Restore of one database only

If you must restore one database alone, reset the checkpoints that point into it. Then the
next run reads the data again in full:

- DSO: reset the DSO sync checkpoints. Read
  [DSO sync](implementation/dfm_operations_guide.md#10-dso-sync-scopedso) in the DFM
  operations guide.
- FHIR server database: reset the checkpoints of the FHIR server loads. Read
  [Checkpoint Reset](implementation/dfm_operations_guide.md#5-checkpoint-reset).

If the backup is older than the upgrade, the restored database does not have the new
migrations and setup objects. Launch `full_infrastructure_setup` before the next run.

Do not restore staging alone. Staging holds the checkpoints of all the other targets.
