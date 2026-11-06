<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Operations guide

This guide describes the logs, the health checks, the error rules and the backup of
HealthPipe. The facts come from the code. Each section names the source files.
The commands are for the devcontainer stack in
[`.devcontainer/docker-compose.yml`](../../.devcontainer/docker-compose.yml).
Advice for production is general. Adapt it to your platform.

## 1. Logs

### 1.1 Where the logs go

[`.devcontainer/dagster_home/dagster.yaml`](../../.devcontainer/dagster_home/dagster.yaml)
sets the log storage:

| Log | Storage |
|---|---|
| Run events and `context.log` messages | The Dagster event log in PostgreSQL (`event_log_storage`) |
| Runs and schedules | PostgreSQL (`run_storage`, `schedule_storage`) |
| The stdout and the stderr of each step | Files in `.devcontainer/dagster_home/compute_logs` (`LocalComputeLogManager`) |

`python_logs.python_log_level` is `DEBUG`. The file sets no `managed_python_loggers`.
Thus, only the Dagster loggers write to the event log: `context.log` and
`dagster.get_dagster_logger()`. A module logger from `logging.getLogger(__name__)`
writes to the stderr of the process.

### 1.2 Log format

The project sets no custom formatter and no custom Dagster logger. Dagster uses its
default console logger. The line format is:

```text
%(asctime)s - %(name)s - %(levelname)s - %(message)s
```

The date format is `%Y-%m-%d %H:%M:%S %z`. For a run message, Dagster puts these
parts in front of the text, separated by ` - `: the job name, the run ID, the process
ID, the step key and the event type. The Dagster UI shows the same records in the run
view, with filters for the level and the step.

The DSO sync writes fixed markers in its messages. Use them for alerts:

| Marker | Level | Meaning |
|---|---|---|
| `INTEGRITY_CONFLICT_RECOVERY` | WARNING | A MERGE hit a unique constraint. The sync does a TRUNCATE+INSERT. |
| `ROW_COUNT_MISMATCH_RECOVERY` | WARNING | DSO has fewer rows than staging. The sync does a full MERGE. |
| `RECOVERY FAILED` | ERROR | The row counts do not match after the full MERGE. |

### 1.3 CPR masking

[`etl/core/logging_redaction.py`](../core/logging_redaction.py) masks Danish CPR
numbers in log records. [`etl/definitions.py`](../definitions.py) calls
`install_pii_redaction()` before it loads the assets.

- `CprRedactingFilter` is a `logging.Filter`. It attaches to the root logger, to
  `dagster` and to `dagster.builtin`, and to the handlers of these loggers.
- A patch of `DagsterLogManager.makeRecord` masks the records from `context.log`. It
  also masks the error message, the stack trace, the cause and the context of Dagster
  events, and the description of a step failure.
- The pattern is `\b\d{6}-?\d{4}\b` (in `etl/DSA/FHIR/utils/gdpr_masking.py`). Each
  match becomes `XXXXXX-XXXX`. The pattern also masks other numbers with 10 digits.
- `DfmManager.fail_dfm_run` masks CPR numbers in the error text before it writes
  the text to `[meta].[DFMRunLog]`.

The masking has limits:

- It masks only CPR numbers. It does not mask names or addresses. The code masks
  FHIR resources at the specific sinks with `mask_fhir_resource_for_logging`.
- It does not cover `print` and other raw writes to stdout. The compute log manager
  stores these writes without the filter.

[Data protection](data_protection.md#4-cpr-masking-in-the-logs) lists all known limits.

Do not write personal data to the logs. The masking is a safety net, not a control.

## 2. Health checks

### 2.1 Devcontainer services

`.devcontainer/docker-compose.yml` gives five services a Docker health check:

| Service | Test | Interval | Timeout | Retries | Start period |
|---|---|---|---|---|---|
| `postgres` (Dagster storage) | `pg_isready -U <user> -d <db>` | 3s | default | 30 | none |
| `fhir-server-db` (Aidbox database) | `pg_isready -U <user> -d <db>` | 3s | default | 60 | none |
| `fhir-server` (Aidbox) | `curl -sf http://localhost:8888/__healthcheck` | 5s | 3s | 60 | none |
| `mssql` | `sqlcmd ... -Q "SELECT 1"` | 10s | 5s | 30 | 60s |
| `oracle` | `sqlplus -L system/<password>@localhost:1521/<db> @/dev/null` | 30s | 10s | 30 | 60s |

The `devcontainer` service has no health check and no `depends_on`. `fhir-server`
depends on `fhir-server-db` without a health condition. Thus, Docker starts the
services in parallel. A process in the devcontainer can start before a database is
ready.

Show the health status of the containers on the host:

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
docker inspect --format '{{json .State.Health}}' <container>
```

### 2.2 Dagster webserver and daemon

`dagster dev` starts the webserver and the daemon. The webserver listens on port 3000.

- `GET /server_info` returns JSON with the versions of `dagster-webserver`, `dagster`
  and `dagster-graphql`. An HTTP 200 shows that the webserver serves requests. It
  does not test the storage.

  ```bash
  curl -sf http://localhost:3000/server_info
  ```

- The GraphQL API shows the heartbeat of each daemon, for example the scheduler, the
  sensor daemon and the run queue:

  ```bash
  curl -s http://localhost:3000/graphql -H 'Content-Type: application/json' \
    -d '{"query":"{ instance { daemonHealth { allDaemonStatuses { daemonType required healthy } } } }"}'
  ```

  The UI shows the same status under **Deployment > Daemons**.

In production, use `/server_info` as the liveness probe of the webserver. Use the
daemon status for an alert. This advice is general.

## 3. Errors and retries

### 3.1 Dagster retry policy

The DSO sync assets have no Dagster `RetryPolicy`. A failure fails the asset at once.
Many DSA and DSI assets set a `RetryPolicy`, most with `max_retries=2, delay=30`. The SQL on FHIR
assets use `SOF_ASSET_RETRY_POLICY` (`max_retries=2, delay=120`). No job sets an
`op_retry_policy`, and `dagster.yaml` sets no run retries.

### 3.2 DSO sync retries in the code

[`etl/DSO/load/sync_utils.py`](../DSO/load/sync_utils.py) retries transient
connectivity errors. `_is_retryable_connectivity_error` matches the error text on these
patterns: `Communication link failure`, `TCP Provider`, `Error code 0x2749`,
`connection was closed` and `reported a change in schema version`. Other errors do not
retry.

The backoff is 5, 15 and 45 seconds (four attempts) at these points:

- `can_connect_to_staging`: the `SELECT 1` probe of the staging database.
- `_stage_batches_via_openquery`: a batch of the batched linked-server sync. A retry
  opens a new connection and stages the table again from the start.

### 3.3 DSO sync failure rules

`sync_table_via_linked_server` applies these rules:

1. Staging is not reachable after the probe retries:
   - If `DAGSTER_CODE_LOCATION_NAME` is set, the asset fails.
   - If not, and the DSO table has rows, the sync returns `local_dev_skip`.
   - If not, and the DSO table is empty, the asset fails.
2. The staging schema and the DSO schema are different: the asset fails with
   `SchemaValidationError`. The sync never changes a DSA schema.
3. The linked server is not available: the asset fails.
4. A MERGE hits a unique constraint: the sync does a TRUNCATE+INSERT of the table.
5. A watermark delta leaves DSO with fewer rows than staging: the sync does one full
   MERGE. It does not retry after that.
6. After the sync, `compare_row_counts` fails the asset with `ROW_COUNT_MISMATCH` if DSO
   has fewer rows than staging. If DSO has more rows, the asset stays green with
   `counts_match: False`.

Each failure sets the status `failed` on the run in `[meta].[DFMRunLog]`. The
checkpoint in `[meta].[DFMState]` moves only when a watermark run completes with rows. Thus, the next run
reads again from the last good checkpoint.

For the sync modes, the watermarks and the troubleshooting steps, read the
[DSO sync guide](implementation/dso_sync_guide.md).

## 4. Backup and restore of the Dagster database

The Dagster database holds the runs, the event log, the schedules and the sensor
state. It does not hold the pipeline data or the DFM checkpoints. These are in SQL
Server (`[meta].[DFMState]`). The compute logs are files, not rows.

In the devcontainer, the database is the `postgres` service (`postgres:17`). The data
is in the named volume `postgresdata`. The default user is `postgres_user` and the
default database is `postgres_db` (`POSTGRES_USER`, `POSTGRES_DB`). The devcontainer
has no PostgreSQL client. Thus, run the tools in the `postgres` container.

### 4.1 Back up

Run these commands on the host, in the repository root:

```bash
PG=$(docker ps -q --filter label=com.docker.compose.service=postgres)
docker exec "$PG" pg_dump -U postgres_user -d postgres_db -Fc > dagster-$(date +%F).dump
tar -czf compute_logs-$(date +%F).tar.gz -C .devcontainer/dagster_home compute_logs
```

If more than one Compose project has a `postgres` service, add
`--filter label=com.docker.compose.project=<project>` to `docker ps`.

### 4.2 Restore

CAUTION: The restore replaces the Dagster data in the database.

1. Stop `dagster dev`, so that the webserver and the daemon do not write.
2. Restore the dump:

   ```bash
   PG=$(docker ps -q --filter label=com.docker.compose.service=postgres)
   docker exec -i "$PG" pg_restore -U postgres_user -d postgres_db --clean --if-exists < dagster-<date>.dump
   ```

3. If the dump comes from an older Dagster version, run `dagster instance migrate`
   in the devcontainer.
4. Start `dagster dev`.

To start with an empty Dagster database, remove the `postgresdata` volume. This
removes the run history.

### 4.3 Production

This advice is general:

- Use the backup of your database platform, with point-in-time recovery if it has one.
- Keep the same Dagster version for the backup and the restore. Run
  `dagster instance migrate` after an upgrade.
- Stop the daemon and the webserver before a restore.
- Test a restore on a copy at a fixed interval.
- Back up the SQL Server databases separately. They hold the data and the DFM state.
