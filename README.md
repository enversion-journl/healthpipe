# Healthpipe

A containerized ETL pipeline built on [Dagster](https://dagster.io/), designed to work with Microsoft SQL Server, Oracle, PostgreSQL, and FHIR (Aidbox). It ships a self-contained devcontainer so you can run the full stack locally with Docker.

## Prerequisites

- [Docker](https://www.docker.com/products/docker-desktop/) (Docker Desktop on macOS/Windows)
  - **macOS Apple Silicon**: install Rosetta for x86 image support: `softwareupdate --install-rosetta`
- [VS Code](https://code.visualstudio.com/) or [Cursor](https://cursor.com/) with the [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)

## Quick start

1. **Clone and open the repository** in VS Code / Cursor.

2. **Create your environment file (required):**
   ```bash
   cp .env.template .env
   ```
   The devcontainer will not start without a root `.env` — `docker-compose.yml`
   loads it via `env_file`. The defaults work as-is; you only need to fill in
   the FHIR server's free Aidbox development license (`AIDBOX_LICENSE`).
   To override the database container images, credentials, or names, see
   `.devcontainer/.env.template`.

3. **Reopen in the devcontainer:** `Ctrl+Shift+P` → **Dev Containers: Reopen in Container**. On first create, the container builds, installs dependencies, and creates the empty databases automatically.

4. **Start Dagster** *(local devcontainer only)*: open **Run and Debug** (`Ctrl+Shift+D`) and launch the **Dagster** configuration. The web UI opens at http://localhost:3000. *(On a remote / Nomad-hosted VS Code server, Dagster is started by the deployment — skip this step.)*

5. **Build the data-warehouse schema:** in the Dagster UI (http://localhost:3000), materialize the `full_infrastructure_setup` job.

6. **Run the pipeline:** materialize assets or launch jobs from the Dagster UI.

All services start from public container images defined in `.devcontainer/docker-compose.yml`.

## Services

Once the devcontainer is running:

- **Dagster Web UI**: http://localhost:3000
- **FHIR Server (Aidbox)**: http://localhost:8888
- **SQL Server**: `mssql:1433` (user `sa`; password from `MSSQL_SA_PASSWORD` in `.env`)
- **PostgreSQL**: `postgres:5432` (Dagster metadata)
- **Oracle**: `oracle:1521`

## Architecture

- **Dagster** — ETL orchestration and data-pipeline management
- **Microsoft SQL Server** — staging / DSO data-warehouse layers
- **Oracle** — IFDB source data
- **PostgreSQL** — Dagster metadata storage
- **Aidbox (FHIR)** — FHIR resource store

### FHIR (Aidbox) connectivity

The FHIR server runs as part of the docker-compose stack. The Dagster code reads:
- `FHIR_SERVER_URL` (defaults to `http://fhir-server:8888` inside the devcontainer)
- `FHIR_CLIENT_USERNAME` and `FHIR_CLIENT_PASSWORD` (configured in `.env`)

## Project structure

```
.
├── etl/           # Dagster application code (assets, jobs, resources)
│   └── infrastructure/    # DW schema (ddl/) + setup assets
├── .devcontainer/         # DevContainer configuration (compose, Dockerfile, startup scripts)
└── docs/                  # Documentation
```

## Troubleshooting

1. **Port conflicts** — ensure ports `3000`, `1433`, `8888` are free (`lsof -i :3000` on macOS/Linux).
2. **Container startup issues** — inspect logs:
   ```bash
   docker compose -f .devcontainer/docker-compose.yml logs
   docker compose -f .devcontainer/docker-compose.yml restart
   ```
3. **Database connection issues** — wait for the SQL Server / Oracle health checks to pass (can take 30–60 s on first startup); verify connection settings in `.env`.
4. **Apple Silicon (macOS arm64)** — the default `mcr.microsoft.com/mssql/server:2022-latest` image is x86-only and may fail to start under emulation (Microsoft publishes no arm64 SQL Server image). If the `mssql` container won't come up, set both `MSSQL_IMAGE` (an arm64-compatible image) and `MSSQL_PLATFORM=linux/arm64` in `.devcontainer/.env`. Postgres, Oracle, and the FHIR server run natively.

### Useful commands

```bash
docker ps                                                      # running containers
docker logs <container-name>                                   # container logs
docker exec -it <container-name> /bin/bash                     # shell into a container
docker compose -f .devcontainer/docker-compose.yml ps          # compose services
```

## Additional resources

- [Dagster Documentation](https://docs.dagster.io/)
- [Aidbox Documentation](https://docs.aidbox.app/)
- [Dev Containers Documentation](https://code.visualstudio.com/docs/remote/containers)

## Contributing

1. Create a feature branch
2. Make your changes
3. Test with the devcontainer
4. Submit a pull request

## License

Licensed under the GNU Affero General Public License v3.0 (AGPL-3.0). See [LICENSE](LICENSE).
