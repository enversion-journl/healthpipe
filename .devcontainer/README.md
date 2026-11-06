<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Healthpipe devcontainer — developer guide

This folder defines Healthpipe's self-contained development environment: a Docker
Compose stack (Dagster + Microsoft SQL Server + Oracle + PostgreSQL + Aidbox/FHIR)
wired up as a VS Code **Dev Container**. The root [README](../README.md)
has the short quick-start; this guide goes deeper for day-to-day development.

## Prerequisites

- **[Docker](https://www.docker.com/products/docker-desktop/)** (Docker Desktop on
  macOS/Windows), running before you start.
  - **macOS Apple Silicon**: the default SQL Server image is x86-only and runs
    under emulation — install Rosetta (`softwareupdate --install-rosetta`). This
    works but is slower; for a native alternative override `MSSQL_IMAGE` +
    `MSSQL_PLATFORM` (see [.env.template](.env.template)).
- **[VS Code](https://code.visualstudio.com/)** with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
- **SSH keys** (optional) — if you push over SSH, your `~/.ssh` is mounted into the
  container (see `mounts` in [devcontainer.json](devcontainer.json)).

## Getting started

1. **Clone and open** the repository in VS Code, opening the repo folder
   directly so the workspace path resolves correctly.
2. **Create your env file** (required):
   ```bash
   cp .env.template .env
   ```
   The defaults work as-is for a local stack — the FHIR server runs in dev mode
   without a license. An Aidbox development license (`AIDBOX_LICENSE`, free from the
   Aidbox portal) is optional; set it only if your Aidbox build/features require one.
   To override the DB container images, credentials, or names, copy
   [.env.template](.env.template) → `.devcontainer/.env` (the interpolation file).
3. **Reopen in Container** (`Ctrl+Shift+P` → *Dev Containers: Reopen in Container*).
   On first create the devcontainer:
   - builds the local toolchain image ([Dockerfile](Dockerfile)),
   - installs the locked dependencies from `uv.lock` (the same recipe as CI) and the project;
     after a `pyproject.toml` change, run `uv lock` first, or this step stops on the stale lock,
   - creates the empty application databases
     ([startup_scripts/create_databases.py](startup_scripts/create_databases.py)),
   - and starts Postgres, SQL Server, Oracle, and the Aidbox FHIR server.
4. **Start Dagster** *(local devcontainer only)*: open **Run and Debug**
   (`Ctrl+Shift+D`) → run the **Dagster** configuration
   ([../.vscode/launch.json](../.vscode/launch.json)). The web UI opens at
   http://localhost:3000.
5. **Build the warehouse schema**: in the Dagster UI, materialize the
   `full_infrastructure_setup` job, then materialize assets / launch jobs.

## Services

| Service               | Address                    | Notes                                              |
| --------------------- | -------------------------- | -------------------------------------------------- |
| Dagster web UI        | http://localhost:3000      | started via the launch config (local)             |
| FHIR server (Aidbox)  | http://localhost:8888      | dev mode; `AIDBOX_LICENSE` optional               |
| SQL Server            | `mssql:1433`               | user `sa`; password from `MSSQL_SA_PASSWORD`       |
| PostgreSQL            | `postgres:5432`            | Dagster metadata                                   |
| Oracle                | `oracle:1521` (host `1522`) | source DB (currently IFDB)                   |

Hostnames are the compose service names, reachable from inside the devcontainer.
Credentials come from `.env` — never hardcode them.

## How configuration works (two `.env` files)

- **Root `.env`** (from [../.env.template](../.env.template)) is injected into the
  app containers via `env_file`. It holds the application connection variables the
  Dagster pipelines read.
- **`.devcontainer/.env`** (from [.env.template](.env.template), optional) is the
  file Docker Compose reads for `${VAR}` interpolation in
  [docker-compose.yml](docker-compose.yml). This is where you override the DB
  container **images** (`MSSQL_IMAGE` / `ORACLE_IMAGE` — e.g. images preloaded with
  test data), the platform pin, or container credentials.

The two files mirror each other's defaults, so a fresh `cp .env.template .env`
yields a coherent stack.

## Architecture

- **Dagster** — ETL orchestration (assets, jobs, resources).
- **Microsoft SQL Server** — staging / DSO warehouse layers and source databases.
- **Oracle** — source data (currently IFDB; more sources to follow).
- **PostgreSQL** — Dagster metadata store.
- **Aidbox (FHIR)** — FHIR resource store.

## Project structure

```
.
├── etl/                    # Dagster application code
│   ├── sources/            # source-system extractors
│   ├── DSI/  DSA/  DSO/     # ingest / transform / serve layers
│   ├── core/               # resources & shared helpers
│   ├── infrastructure/     # DW schema (ddl/) + setup assets
│   └── tests/              # unit + integration tests
├── .devcontainer/          # this dev environment (compose, Dockerfile, scripts)
└── docs/                   # documentation
```

## Running tests

Tests run inside the devcontainer. From `etl/`:

```bash
pytest tests/unit -q                                    # unit tests (no live DB)
USE_REAL_DB_FOR_TESTS=true pytest tests/integration -q  # integration (needs local DBs)
```

Some Oracle / heavy integration tests are gated behind `RUN_*` environment flags —
see the individual test module docstrings.

## Upgrading the data-baked DB images

The SQL Server and Oracle services use **data-baked images** (`MSSQL_IMAGE` /
`ORACLE_IMAGE`), and their data lives in named volumes (`mssql-data-v1`,
`oracle-v1`). Docker seeds these volumes from the image on **first use
only** — after that the volume *shadows* the image. So pulling a newer baked
image while keeping an old volume leaves you on the old data **and** the old
`sa` / Oracle password, which surfaces as `Login failed for user 'sa'`.

To force a clean re-seed we **bump the volume's `-vN` suffix** in
[docker-compose.yml](docker-compose.yml) whenever the baked image's credentials
or schema layout change. Your next *Rebuild Container* then mounts a fresh,
empty volume that re-seeds from the new image — no manual step required.

The previously-named volumes are left orphaned (they take disk space but are no
longer used). After a successful rebuild you can delete them:

```bash
# list orphaned data volumes (anything ending in _mssql-data / _oracle-ifdbdata
# without the current -vN suffix)
docker volume ls | grep -E '_(mssql-data|oracle-ifdbdata)$'

# remove a specific orphan (compose prefixes the project name)
docker volume rm <project>_mssql-data <project>_oracle-ifdbdata
```

`docker volume rm` refuses to delete a volume still in use, so this is safe.

## Troubleshooting

1. **Port conflicts** — ensure `3000`, `1433`, `8888`, `1522` are free
   (`lsof -i :3000` on macOS/Linux).
2. **Apple Silicon SQL Server** — the default `mssql/server:2022` image is x86-only
   and runs under emulation via Rosetta (Microsoft ships no arm64 SQL Server image).
   This works but is slower; if it won't start, or for native speed, set `MSSQL_IMAGE`
   (an arm64-compatible image) + `MSSQL_PLATFORM=linux/arm64` in `.devcontainer/.env`.
   Postgres, Oracle, and the FHIR server run natively.
3. **Container startup** — inspect / restart:
   ```bash
   docker compose -f .devcontainer/docker-compose.yml logs
   docker compose -f .devcontainer/docker-compose.yml restart
   ```
4. **Database connection** — wait for the SQL Server / Oracle health checks to pass
   (30–60 s on first startup); verify settings in `.env`.
5. **FHIR server won't start** — it runs in dev mode without a license; if you set
   `AIDBOX_LICENSE`, confirm the value is valid (an invalid license blocks startup).

### Useful commands

```bash
docker ps                                               # running containers
docker logs <container-name>                            # container logs
docker exec -it <container-name> /bin/bash              # shell into a container
docker compose -f .devcontainer/docker-compose.yml ps   # compose services
```

## Additional resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Aidbox Documentation](https://docs.aidbox.app/)
- [Dev Containers Documentation](https://code.visualstudio.com/docs/remote/containers)
