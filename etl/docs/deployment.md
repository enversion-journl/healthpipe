<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Production deployment

This page describes how to run Healthpipe outside the devcontainer. It lists the
components, the configuration files, the container images, the variables and the first
start. The last section lists the parts of the code that are specific to one region.

The repository gives no deployment manifest for a specific platform, for example
Kubernetes or Docker Compose. Use this page to write the manifest for your platform.

**Contents:** [Components](#components) · [Versions](#versions) ·
[Container images](#container-images) · [Dagster configuration](#dagster-configuration) ·
[Containers and volumes](#containers-and-volumes) · [Configuration](#configuration) ·
[Deployment-mode variables](#deployment-mode-variables) · [Secrets](#secrets) ·
[Database prerequisites](#database-prerequisites) · [First start](#first-start) ·
[Sizing](#sizing) · [Region-specific parts](#region-specific-parts)

## Components

| Component | What it does | Who connects to it |
|---|---|---|
| Dagster webserver | Serves the UI and the GraphQL API on port 3000. | Users, the health checks |
| Dagster daemon | Runs the scheduler, the sensors and the run queue. | none |
| Code server | Serves the Healthpipe definitions over gRPC on port 4000. With the default run launcher, the runs also execute in this container. | The webserver, the daemon |
| Dagster PostgreSQL | Stores the runs, the event log, the schedules and the sensor state. | The webserver, the daemon, the run processes |
| SQL Server `staging` | Holds DSI and DSA: the schemas `ext`, `arc`, `stg`, `map`, `dim`, `fct` and others. | The run processes |
| SQL Server `dso` | Holds the finished tables for the consumers. It reads from `staging` through a linked server. | The run processes |
| Linked server from DSO to staging | The DSO sync runs a MERGE on the DSO server and reads `staging` through this link. | The DSO SQL Server |
| Oracle sources | IFDB and EPJI. The code uses `oracledb` in thin mode, so you need no Oracle Instant Client. | The run processes |
| SQL Server sources | The source databases in the [configuration reference](configuration.md#application-connection-variables). | The run processes |
| Aidbox FHIR server | Stores the FHIR resources. | The run processes (HTTP) |
| Aidbox PostgreSQL | The database of Aidbox. The pipelines also read it directly and create indexes and functions in it. | Aidbox, the run processes |
| SDS file shares | Read-only CIFS mounts with the file deliveries, for example LPR3, DRG and HAIBA. | The run processes |
| Active Directory (optional) | LDAPS source for the security model. | The run processes |
| OS2mo (optional) | The GraphQL API of OS2mo. The pipelines send codesystem files to it and read data from it. `OS2MO_ENABLED=true` turns it on. | The run processes |

The run processes execute where the run launcher starts them. This page uses the
`DefaultRunLauncher`. It starts each run as a subprocess of the code server. Thus, the
code-server container needs the network access, the mounts and the variables of all
pipelines.

## Versions

The devcontainer and the lock file set these versions. The repository does not define a
minimum version for the external services.

| Component | Tested version | Minimum version |
|---|---|---|
| Python | 3.12 (`requires-python = "==3.12.*"`) | 3.12 |
| Dagster, `dagster-webserver` | 1.13.18 | 1.13.18 (pinned) |
| `dagster-postgres`, `dagster-docker`, `dagster-pandas` | 0.29.18 | 0.29.18 (pinned) |
| Microsoft SQL Server | 2022 (`mcr.microsoft.com/mssql/server:2022-latest`) | `<not defined>` |
| PostgreSQL (Dagster storage) | 17 (`postgres:17`) | `<not defined>` |
| Aidbox | 2508 (`healthsamurai/aidboxone:2508`) | `<not defined>` |
| Aidbox PostgreSQL | 17.5 (`healthsamurai/aidboxdb:17.5`) | `<not defined>` |
| Oracle Database | Oracle Database Free (`gvenzl/oracle-free:full-faststart`) | `<not defined>` |
| CPU architecture of the images | `linux/amd64` (the published images) | `<not tested on other architectures>` |

The DSO SQL Server must support linked servers. Azure SQL Edge does not. On Azure SQL
Edge, the setup asset `dso_infra_linked_server` skips the link, and the DSO sync then
fails.

## Container images

The repository has two Dockerfiles in
[`infrastructure/containers/dockerfiles/`](../../infrastructure/containers/dockerfiles/):

| Dockerfile | Content |
|---|---|
| `code-server-base.Dockerfile` | `python:3.12-slim-bookworm`, `build-essential`, `curl`, two system libraries for `mssql-python`, and the locked Python dependencies from `uv.lock`. It installs the `dev` extra too. |
| `code-server.Dockerfile` | The base image plus `etl/`. It sets `PYTHONPATH=/opt/dagster/app/etl`, exposes port 4000 and starts `dagster api grpc -m definitions`. |

The repository has no separate image for the webserver and the daemon. The base image
contains `dagster-webserver` and `dagster-postgres`. Thus, you can run the webserver and
the daemon from the code-server image with another command. See
[Containers and volumes](#containers-and-volumes).

### Published images

Each published GitHub release builds the two images and pushes them to the GitHub
Container Registry:

| Image | Tags |
|---|---|
| `ghcr.io/enversion-journl/healthpipe-code-server-base` | `<version>` (for example `1.2.0`), `sha-<full commit SHA>` |
| `ghcr.io/enversion-journl/healthpipe-code-server` | `<version>`, `sha-<full commit SHA>` |

The release workflow is [`.github/workflows/docker.yml`](../../.github/workflows/docker.yml).
It builds the code-server image on the base image of the same run, by digest. Each image
has an SPDX SBOM and a build provenance attestation. The release also has the two SBOMs
as files. The releases up to and including v1.1.1 have no published images.

There is no `latest` tag. Use the digest in production:

```bash
docker buildx imagetools inspect ghcr.io/enversion-journl/healthpipe-code-server:<version>
```

The command shows the digest. Then refer to the image as
`ghcr.io/enversion-journl/healthpipe-code-server@sha256:<digest>`.

To read the SBOM and the provenance of an image:

```bash
docker buildx imagetools inspect ghcr.io/enversion-journl/healthpipe-code-server:<version> \
  --format '{{ json .SBOM.SPDX }}'
docker buildx imagetools inspect ghcr.io/enversion-journl/healthpipe-code-server:<version> \
  --format '{{ json .Provenance.SLSA }}'
```

### Build the images yourself

Use [`infrastructure/devops/scripts/build-push.sh`](../../infrastructure/devops/scripts/build-push.sh)
when you build the images for your own registry. The script builds with `docker buildx`
and pushes the result. It does not depend on one CI system.

1. Log in to your registry, or set `REGISTRY_USERNAME` and `REGISTRY_PASSWORD`.
2. Build and push the base image:

   ```bash
   REGISTRY=<your-registry> \
   IMAGE_NAME=<namespace>/healthpipe-code-server-base \
   DOCKERFILE=infrastructure/containers/dockerfiles/code-server-base.Dockerfile \
   TAGS="<version>" \
   PLATFORM=linux/amd64 \
   GIT_SHA=$(git rev-parse HEAD) \
   infrastructure/devops/scripts/build-push.sh
   ```

3. Build and push the code-server image on that base image:

   ```bash
   REGISTRY=<your-registry> \
   IMAGE_NAME=<namespace>/healthpipe-code-server \
   DOCKERFILE=infrastructure/containers/dockerfiles/code-server.Dockerfile \
   TAGS="<version>" \
   PLATFORM=linux/amd64 \
   GIT_SHA=$(git rev-parse HEAD) \
   EXTRA_BUILD_ARGS="BASE_IMAGE=<your-registry>/<namespace>/healthpipe-code-server-base:<version>" \
   infrastructure/devops/scripts/build-push.sh
   ```

Always set `BASE_IMAGE`. The default in `code-server.Dockerfile` is
`dagster-code-server-base:latest`, and a later build of the same release can then get
another base image.

Set `DRY_RUN=1` to show the build command without a build. The script uses a registry
layer cache by default (`USE_CACHE=true`, `CACHE_REF`). It always pushes, and it turns
the provenance attestation off (`--provenance=false`).

## Dagster configuration

Dagster reads `dagster.yaml` and `workspace.yaml` from the folder in `DAGSTER_HOME`.
The devcontainer files are in `.devcontainer/dagster_home/`. They contain paths of the
devcontainer, for example `/workspaces/healthpipe`. Do not use them in production.

### dagster.yaml

This example keeps the scheduler and the run queue of the devcontainer file. It reads
all paths and connection values from variables:

```yaml
scheduler:
  module: dagster.core.scheduler
  class: DagsterDaemonScheduler

run_coordinator:
  module: dagster.core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 5
    tag_concurrency_limits:
      - key: "concurrency_key"
        value: "one"
        limit: 1

run_launcher:
  module: dagster.core.launcher
  class: DefaultRunLauncher

compute_logs:
  module: dagster.core.storage.local_compute_log_manager
  class: LocalComputeLogManager
  config:
    base_dir:
      env: DAGSTER_COMPUTE_LOG_DIR

storage:
  postgres:
    postgres_db:
      hostname:
        env: DAGSTER_POSTGRES_HOST
      username:
        env: DAGSTER_POSTGRES_USER
      password:
        env: DAGSTER_POSTGRES_PASSWORD
      db_name:
        env: DAGSTER_POSTGRES_DB
      port:
        env: DAGSTER_POSTGRES_PORT

python_logs:
  python_log_level: INFO

telemetry:
  enabled: false
```

Notes:

- The `storage.postgres` block sets the run storage, the event log storage and the
  schedule storage together. The devcontainer file sets the three storages one by one,
  with the same values.
- `DAGSTER_COMPUTE_LOG_DIR` is a name of this example only. The code does not read it.
  Set it in each container that reads this `dagster.yaml`.
- The run processes write the compute logs, and the webserver reads them. Thus, mount
  one shared volume at this path in the code server and in the webserver.
- The devcontainer file sets `python_log_level: DEBUG`. This example sets `INFO`.
- `telemetry` is optional. Remove the block to keep the Dagster default.
- `max_concurrent_runs: 5` is the devcontainer value. Change it for your capacity.

### workspace.yaml

In production, the webserver and the daemon load the code from the code server over
gRPC:

```yaml
load_from:
  - grpc_server:
      host:
        env: DAGSTER_CODE_SERVER_HOST
      port:
        env: DAGSTER_CODE_SERVER_PORT
      location_name: "pipelines"
```

Set `DAGSTER_CODE_SERVER_HOST` to the host name of the code-server container. Set
`DAGSTER_CODE_SERVER_PORT` to `4000`.

`location_name` is the name that the UI shows. The code does not read it. The variable
`DAGSTER_CODE_LOCATION_NAME` is a different setting. See
[Deployment-mode variables](#deployment-mode-variables).

The file `.devcontainer/dagster_home/workspace-dynamic.yaml.tpl` uses a template syntax
for one service-discovery tool. Dagster cannot read it. Use the static file above.

## Containers and volumes

Use one image for the three Dagster containers:

| Container | Command | Port |
|---|---|---|
| Code server | The default command of the image: `dagster api grpc -h 0.0.0.0 -p 4000 -m definitions -d /opt/dagster/app/etl` | 4000 |
| Webserver | `dagster-webserver -h 0.0.0.0 -p 3000 -w $DAGSTER_HOME/workspace.yaml` | 3000 |
| Daemon | `dagster-daemon run -w $DAGSTER_HOME/workspace.yaml` | none |

Give each container these items:

| Item | Code server | Webserver | Daemon |
|---|---|---|---|
| `DAGSTER_HOME` with `dagster.yaml` | yes | yes | yes |
| `workspace.yaml` in `DAGSTER_HOME` | no | yes | yes |
| `DAGSTER_POSTGRES_*` | yes | yes | yes |
| `DAGSTER_CODE_SERVER_HOST`, `DAGSTER_CODE_SERVER_PORT` | no | yes | yes |
| `DAGSTER_COMPUTE_LOG_DIR` | yes | yes | yes |
| Compute log volume at `DAGSTER_COMPUTE_LOG_DIR` | yes | yes | no |
| All variables in [configuration.md](configuration.md) | yes | no | no |
| [Deployment-mode variables](#deployment-mode-variables) | yes | no | no |
| Persistent volume for `$DAGSTER_HOME/storage` and `$DAGSTER_HOME/cache` | yes | no | no |
| Persistent volume at `DATA_DIR` | yes | no | no |
| SDS file shares (read-only) | yes | no | no |

The code uses `DAGSTER_HOME` for its own files too:

- The IO managers write the asset outputs to `$DAGSTER_HOME/storage`
  (`etl/definitions.py`). If `DAGSTER_HOME` is not set, they use `./storage`.
- The identifier caches are in `$DAGSTER_HOME/cache/identifier_caches`
  (`etl/core/cache/sqlite_cache.py`).
- `DATA_DIR` holds the file downloads, for example SOR. The default is
  `/workspaces/healthpipe/data` (`etl/core/org_config.py`), a devcontainer path. Set it
  in production.

WARNING: Do not put a `.env` file in the code-server container, in `etl/` or in a folder
above `etl/`. `etl/definitions.py` loads `.env` with `override=True`. The values in the
file then replace the variables of the container.

CAUTION: With the `DefaultRunLauncher`, the runs are subprocesses of the code server. A
restart of the code-server container stops the runs in it. Wait until no run is active
before you replace the container.

The image also contains `dagster-docker`. With the `DockerRunLauncher`, each run gets its
own container. This page does not describe that setup.

## Configuration

The [configuration reference](configuration.md) lists each variable of `.env.template`.
In production, set the variables in the environment of the code-server container. Do not
copy a `.env` file into the image.

Set these groups:

- The staging and DSO connections: `MSSQL_SOURCE_STAGING_*`, `MSSQL_TARGET_STAGING_*`,
  `MSSQL_SOURCE_DSO_*` and `MSSQL_TARGET_DSO_*`.
- The linked server: `MSSQL_LINKED_SERVER_*`. See
  [Database prerequisites](#database-prerequisites).
- The source systems that you use, for example `IFDB_PROD_ORACLE_*`.
- The FHIR server: `FHIR_SERVER_URL`, `FHIR_CLIENT_USERNAME`, `FHIR_CLIENT_PASSWORD` and
  `USE_FHIR_API`.
- The Aidbox database: `PG_FHIR_HOST`, `PG_FHIR_PORT`, `PG_FHIR_DATABASE`, `PG_FHIR_USER`
  and `PG_FHIR_PASSWORD`.
- The Dagster database: `DAGSTER_POSTGRES_*`.
- The folders of the SDS file shares: `*_BASE_DIR` and `*_FILE_ENCODING`.
- `FHIR_PUBLISHER`: the name of your organization on the published CodeSystems.
- `DATA_DIR`: a persistent folder for downloads.

The variables of the "Root .env" table configure the devcontainer database containers.
They have no effect on a production deployment.

An SDS source with no `*_BASE_DIR` fails when it runs. This is intentional: a share that
is not mounted must not look like "no new deliveries". Set the variable only for the
sources that you receive.

## Deployment-mode variables

These variables tell the code which type of deployment it is. The configuration reference
does not list them.

| Variable | Values | Effect |
|---|---|---|
| `DAGSTER_ENVIRONMENT` | A name. Use a value with `prod` in production. | A value with `prod` marks a production deployment. The code then does not register the dev assets that truncate tables, and it refuses to load test users (`SECURITY_SEED_ENABLED`). A target host name with `prod` has the same effect. |
| `DAGSTER_CODE_LOCATION_NAME` | `development`, `production`, `prod` or another name | `development`, `production` and `prod` start most schedules and sensors as RUNNING. Some stay STOPPED in all deployments, for example the LPR3 schedules. Any value makes the DSO sync fail when staging is not reachable. Without the variable, the sync skips a table that already has rows in DSO when staging is not reachable (local mode). |
| `DAGSTER_BRANCH_POSTFIX` | `main-<release>`, `release-<release>`, `feature-<name>` or empty | `main-*` and `release-*` start most schedules and sensors as RUNNING and turn on the release gate (see [First start](#first-start)). `feature*` adds a postfix to each asset, job and schedule name. See [Feature-branch deployments](implementation/feature_branch_deployments.md). |
| `DQA_ENVIRONMENT` | A name | The environment label of the DQA results. Default: the value of `DAGSTER_BRANCH_POSTFIX`, else `local`. |
| `DAGSTER_WEBSERVER_URL` | The URL of the UI | The nightly failure reports use it for the links to the runs. |
| `NIGHTLY_REPORT_ENV` | `dev`, `qa` or `prod` | Overrides the environment of the nightly failure reports. |
| `ADO_ORG_URL`, `ADO_PROJECT`, `ADO_REPOSITORY`, `ADO_PAT` | Azure DevOps settings | The nightly failure sensor writes reports to an Azure DevOps Git repository. Without these variables, the sensor skips. |

For a production deployment, use these values:

```bash
DAGSTER_ENVIRONMENT=production
DAGSTER_CODE_LOCATION_NAME=production
DAGSTER_BRANCH_POSTFIX=main-<version>
```

## Secrets

The configuration has passwords, tokens and keys, for example `*_PASSWORD`,
`FHIR_CLIENT_PASSWORD`, `OS2MO_CLIENT_SECRET`, `LDAP_PASSWORD`, `ADO_PAT` and
`AIDBOX_LICENSE`.

- Keep the secrets in the secret store of your platform.
- Give the secrets to the containers as environment variables at run time.
- Do not put a secret in an image, in `dagster.yaml` or in `workspace.yaml`. The example
  files read the secrets from variables.
- Use a separate database user for each connection. Give each user only the permissions
  that [Database prerequisites](#database-prerequisites) lists.

WARNING: Do not use the devcontainer values of Aidbox in production. The file
`.devcontainer/init-bundle.json` creates a user and a client with the password `secret`
and gives both full access. `docker-compose.yml` sets fixed values for
`BOX_AUTH_KEYS_SECRET`, `BOX_AUTH_KEYS_PRIVATE` and `BOX_AUTH_KEYS_PUBLIC`. Make your own
values for production.

The log filter masks CPR numbers, but it is not a control. See the
[operations guide](operations.md#13-cpr-masking).

## Database prerequisites

### SQL Server

1. Create two empty databases: one for `staging` and one for `dso`. They can be on the
   same server or on two servers.
2. Give the user of `MSSQL_TARGET_STAGING_*` the permissions to create schemas, tables,
   views and functions in `staging`. The setup assets create them.
3. Give the user of `MSSQL_TARGET_STAGING_*` the permissions to create a login and a
   user, and to grant `SELECT`. The asset `staging_infra_linked_server_user` runs
   `CREATE LOGIN`, `CREATE USER` and `GRANT SELECT ON SCHEMA`.
4. Give the user of `MSSQL_TARGET_DSO_*` the permissions of step 2 in `dso`.
5. Give the user of `MSSQL_TARGET_DSO_*` the server role `setupadmin` or `sysadmin` on
   the DSO server. The asset `dso_infra_linked_server` needs it to create the linked
   server.

### Linked server from DSO to staging

The DSO sync writes only through the linked server. Without it, the sync fails and its
checkpoint does not move.

The setup assets create the link:

- `staging_infra_linked_server_user` creates a login and a read-only user in `staging`.
  The user gets `SELECT` on the schemas `map`, `dim`, `fct`, `meta`, `bridge`, `fhir`,
  `sec`, `ref` and `conceptmap`. If the user is `sa` or `dbo`, the asset skips this step.
- `dso_infra_linked_server` creates the linked server in `dso`. The default name is
  `STAGING_LINK`.
- The job `linked_server_setup` runs both assets and a connection check every day.

Set these variables:

| Variable | Value |
|---|---|
| `MSSQL_LINKED_SERVER_HOST` | The host name of the staging server, as the DSO server sees it. |
| `MSSQL_LINKED_SERVER_PORT` | The port of the staging server. |
| `MSSQL_LINKED_SERVER_DB` | The name of the staging database. |
| `MSSQL_LINKED_SERVER_USER` | The login that the link uses. Do not use `sa` in production. |
| `MSSQL_LINKED_SERVER_PASSWORD` | The password of that login. |
| `MSSQL_LINKED_SERVER_NAME` | The name of the linked server. Default: `STAGING_LINK`. |
| `MSSQL_LINKED_SERVER_PROVSTR` | Optional. A provider string for the link. |

If `MSSQL_LINKED_SERVER_HOST`, `_PORT`, `_DB`, `_USER` or `_PASSWORD` is not set, the
code uses the `MSSQL_SOURCE_STAGING_*` variable with the same suffix.

The DSO server opens the link, not the Dagster container. Thus, the DSO server must reach
the staging server on the network.

### Aidbox

The devcontainer runs Aidbox with `AIDBOX_DEV_MODE=true` and a development license in
`AIDBOX_LICENSE`. Read the license terms of the vendor for production. Then set
`AIDBOX_LICENSE` to your production license.

Use the same FHIR settings as the devcontainer in
[`.devcontainer/docker-compose.yml`](../../.devcontainer/docker-compose.yml):

- FHIR version 4.0.1 with the package `hl7.fhir.r4.core#4.0.1`.
- Schema validation on (`BOX_FHIR_SCHEMA_VALIDATION`).
- The hybrid terminology engine with an external server that the network cannot reach.
  Then no code leaves your network.
- A maximum request body of 256 MB (`BOX_WEB_MAX_BODY`). Large CodeSystem uploads need it.
- The search parameter `Questionnaire-assemble-expectation` from
  `.devcontainer/init-bundle.json`.

The pipelines connect to the Aidbox PostgreSQL with `PG_FHIR_*`. The setup group
`fhir_infrastructure` creates functions, indexes and views in that database. Give the user of
`PG_FHIR_*` these permissions.

### Dagster PostgreSQL

Create an empty database and a user that owns it. Dagster creates its tables on the first
start. After a Dagster upgrade, run `dagster instance migrate`. The
[operations guide](operations.md#4-backup-and-restore-of-the-dagster-database) describes
the backup.

### SDS file shares

Mount each share read-only in the code-server container. Set `*_BASE_DIR` to the mount
path. The example in `.env.template` uses `/mnt/ftproot/Udredningsret`. The default file encoding
of the SDS sources is `cp1252`.

## First start

CAUTION: With `DAGSTER_CODE_LOCATION_NAME=production` and no `main-*` or `release-*`
postfix, the schedules start at once. They then run before the database objects exist.
Use `DAGSTER_BRANCH_POSTFIX=main-<version>`, or stop the schedules until step 6 is
complete.

1. Create the databases and the users. See [Database prerequisites](#database-prerequisites).
2. Start the Dagster PostgreSQL, the SQL Server databases and Aidbox.
3. Start the code server. Make sure that the log shows no error for a missing variable.
4. Start the webserver and the daemon.
5. Open the UI. Make sure that the code location `pipelines` loads without errors.
6. Run the job `full_infrastructure_setup`. It creates the schemas, the tables, the views,
   the indexes and the linked server on `staging` and `dso`. It also creates the
   functions, the indexes and the views in the Aidbox database.
   - With `DAGSTER_BRANCH_POSTFIX=main-*` or `release-*`, the sensor
     `release_infrastructure_sensor` starts the job once for each release. Until the job
     succeeds, each schedule tick skips or fails.
   - Without such a postfix, start the job yourself in the UI.
7. On a new FHIR server, materialize the asset `fhir_structure_definitions` one time. Each
   run adds another copy of the StructureDefinitions, so do not run it again.
8. Run the job `linked_server_setup`. Make sure that `dso_verify_linked_server` succeeds.

The [infrastructure guide](implementation/infrastructure_guide.md#9-jobs-and-deploy)
describes the setup jobs and the release gate. Before an upgrade, read the
[CHANGELOG](../../CHANGELOG.md). It marks each breaking change with **Breaking:**.

For the health checks of the webserver and the daemon, see the
[operations guide](operations.md#22-dagster-webserver-and-daemon).

## Sizing

PLACEHOLDER: The project has no measured sizing for production. Replace the values below
with values from your own load tests.

| Component | CPU | Memory | Disk |
|---|---|---|---|
| Code server (with the runs) | `<cpu>` | `<memory>` | `<disk>` for `DAGSTER_HOME` and `DATA_DIR` |
| Webserver | `<cpu>` | `<memory>` | none |
| Daemon | `<cpu>` | `<memory>` | none |
| Dagster PostgreSQL | `<cpu>` | `<memory>` | `<disk>` |
| SQL Server `staging` and `dso` | `<cpu>` | `<memory>` | `<disk>` |
| Aidbox and its PostgreSQL | `<cpu>` | `<memory>` | `<disk>` |

Known data points:

- The parallel extract runs in the code server. Its README gives about 20 GB of memory for
  the production code server and about 4 GB for a test code server
  (`etl/core/parallel/README.md`).
- The devcontainer caps the JVM heap of Aidbox at 2560 MB (`JAVA_TOOL_OPTIONS`), because
  the large CodeSystem uploads use much memory.

## Region-specific parts

Healthpipe started in one Danish region. Some values in the code are specific to that
region. Another region must change them in its own copy of the code. Only the
Active Directory OU has a variable.

| What | Where | What to change |
|---|---|---|
| The region code `1083` in the file names of the SDS deliveries | `etl/DSI/csv_files/drg.py` (`ARCHIVE_RE`, `DATA_MEMBER_RE`), `etl/DSI/csv_files/udredningsret.py` (`ARCHIVE_RE`, `DATA_MEMBER_RE`), `etl/DSI/csv_files/udredningsret_datasets.py` (`REGION`), `etl/DSI/csv_files/lpr3.py` (`DELIVERY_UNIT`, `ARCHIVE_RE`), `etl/DSI/csv_files/lpr3_datasets.py` (`data_filename`), `etl/DSI/csv_files/_lpr3_control.py` (`MEMBER_RE`) | Your region code. The deliveries of another region do not match the patterns. |
| The DFM checkpoint keys of the deliveries | `etl/DSI/csv_files/udredningsret.py` (`DELIVERY_UNIT`, from `REGION`), `etl/DSI/csv_files/lpr3.py` (`DELIVERY_UNIT`) | The keys in `meta.DFMState` contain the region code. A new region code gives a new key, and the first run starts with no checkpoint. |
| The region code in the HAIBA data checks | `etl/dqa/config/haiba_subjects.py` (the filter on the column `RegionKode`) | Your region code. |
| The example file names in the LPR3 metadata | `etl/DSI/csv_files/lpr3_metadata.json` (`datafile`) | Your region code. |
| A fixed region marker in the HAIBA file pattern | `etl/DSI/csv_files/haiba.py` (`FILE_RE`) | The marker in the file names of your HAIBA deliveries. |
| The region names of Statistics Denmark | `etl/DSA/SQL/staging/udredningsret_kolonner.py` (`REGION_TEKSTER`) | The row for `1083` has the text `Example Region`, not the official name. Set the official name. The `Udredningsret` dimension uses this text. |
| The map from municipality code to the hospital with population responsibility | `etl/DSA/SQL/ref/kommune_to_populationsansvar.py` (`POPULATIONSANSVAR_BY_KOMMUNEKODE`, 22 municipalities) | The municipalities and hospitals of your region. `dim_Borger` joins this map, and the AD security export uses its hospital codes (`etl/infrastructure/security/export_healthpipe_ad_groups.py`). |
| The root unit of the region in SOR | `etl/DSA/CodeSystem/sor/sor.py` (`SOR_ROOT`) | The SOR code of your region. Two properties of the SOR CodeSystem and the OS2mo SOR export start from this unit. |
| The Active Directory groups of the security model | `etl/infrastructure/security/export_healthpipe_ad_groups.py` (`_DEFAULT_OU_DN`, `_ORG_GROUP_RE`, `_ROLE_GROUP_RE`) | Set `LDAP_HP_OU_DN` to your OU. Change the two patterns to the group names of your directory. |
| The root code of the requester and store CodeSystems | `etl/DSA/CodeSystem/rekvirent/rekvirent.py` (`root_code`), `etl/DSA/CodeSystem/lager/lager.py` (`LAGER_ROOT`) | A root code and a display name for your region. |
| The FHIR namespace URLs and the `urn:enversion:*` identifiers | `etl/utils/fhir/identifier_systems.py` and other modules | These are published data contracts, not configuration (see `etl/core/org_config.py`). A change gives new identifiers to all emitted resources. |
| The set of source systems | The resources in `etl/definitions.py`, and the [Application connection variables](configuration.md#application-connection-variables) | The source systems of your region. Remove the assets of the sources that you do not have. |

To find other places, search the code:

```bash
grep -rn "1083" etl --include='*.py' --include='*.json' | grep -v '/tests/'
grep -rn "Example Region" etl --include='*.py'
```
