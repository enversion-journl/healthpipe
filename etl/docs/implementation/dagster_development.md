<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Dagster Development Guide

The local development loop: getting Dagster running, making a change, verifying it,
and finding out why it does not work.

For *where things live* and *what the words mean*, read the
[Repository Tour](repo_tour.md) first.

## Prerequisites

Development runs inside the devcontainer — see the
[devcontainer guide](../../../.devcontainer/README.md) for the full setup (Docker,
VS Code/Cursor, and the Compose stack: Dagster + MSSQL + Oracle + PostgreSQL +
Aidbox/FHIR). In short:

- **Environment** — copy `.env.template` to `.env` in the repo root before first
  start. The devcontainer guide explains the two `.env` files and every setting.
- **Pre-commit hooks** — installed automatically on container create
  (`.devcontainer/startup_scripts/setup-precommit.sh`): gitleaks (secret/PII scan of
  the staged diff) plus black, ruff and bandit. CI runs the same checks. Never commit
  with `--no-verify`.

## Quick start

```bash
cd etl
dagster dev
```

The UI is at http://localhost:3000.

## The development loop

### 1. Decide where the change goes

See [Where a change goes](repo_tour.md#where-a-change-goes) in the Repository Tour, then follow the
guide for that layer — [DSI](dsi_etl_guide.md), [FHIR](fhir_etl_guide.md) or
[SQL/DWH](sql_etl_guide.md).

### 2. Write the asset

Follow the patterns in the neighbouring files rather than inventing new ones. Most
new work is a config entry consumed by an existing factory, not a hand-written asset.

### 3. Register it

Assets reach `definitions.py` through module `__init__.py` exports. An asset that is
not in one of those lists does not exist as far as Dagster is concerned — this is the
single most common reason a new asset "does not show up". See
[How assets reach definitions](repo_tour.md#how-assets-reach-definitions) in the
Repository Tour.

### 4. Validate

```bash
dagster definitions validate
```

This catches import errors, duplicate asset keys and unresolved dependencies without
starting the UI. Run it before every commit that adds or renames an asset.

### 5. Materialize

From the UI: **Assets** → find the asset → **Materialize**. Or from the CLI:

```bash
dagster asset materialize --select my_asset_name
dagster asset materialize --select +my_asset_name        # with upstream dependencies
```

### 6. Verify the data

Materializing without errors is not the same as being correct. Check row counts,
grain and — for anything checkpoint-based — that a second run is a no-op. The layer
guides list what to verify for each kind of asset.

### 7. Test

See [Testing](#testing) below, and read the test-value policy in
[`05-testing.mdc`](../../../.cursor/rules/05-testing.mdc) *before* writing a test.
Config-entry, DDL-only and registration changes do not warrant unit tests.

## Common commands

```bash
# Start the development server
dagster dev

# Validate definitions (import errors, duplicate keys, unresolved deps)
dagster definitions validate

# List all assets
dagster asset list

# Materialize a specific asset
dagster asset materialize --select my_asset_name

# Materialize with upstream dependencies
dagster asset materialize --select +my_asset_name

# Run a job
dagster job execute -j fhir_etl_ifdb_daily_job

# Run a job for a specific partition
dagster job execute -j fhir_etl_ifdb_daily_job --partition 2024-12-01
```

Assets that use DFM run modes (`delta`, `full_load`, `period`, `ids`) are launched
with a `RunModeConfig` from the UI Launchpad — see
[dfm_operations_guide.md](dfm_operations_guide.md) for the exact YAML per mode.

## Dagster UI

| View | Use it for |
|---|---|
| **Asset catalog** | Browsing assets by group, reading descriptions and metadata |
| **Asset graph** | Seeing dependency chains and lineage; picking a selection to materialize |
| **Runs** | Logs, errors, retries |
| **Launchpad** | Manual runs with config — this is where `RunModeConfig` overrides go |
| **Checks** | DQA asset-check results with severities |

## Testing

Full documentation in the [Testing Guide](../../tests/README.md): unit tests with
mocks, integration tests with real resources, patient-specific workflows, markers and
configuration.

```bash
pytest tests/unit/ -v                            # unit tests
pytest tests/integration/ -v                     # integration tests
pytest tests/integration/datawarehouse/ -v       # one domain
pytest tests/integration/fhir_ifdb/ -v
```

Integration tests against real resources require `USE_REAL_DB_FOR_TESTS=true` and
`TEST_ENV=dev`. There is a hard safety gate: tests abort if `TEST_ENV` or
`DAGSTER_ENVIRONMENT` contains `prod`.

## Troubleshooting

### Assets not loading

1. Run `dagster definitions validate` — it names the failing import.
2. Check that the asset is exported from its module's `__init__.py` **and** included
   in the aggregate list that `definitions.py` imports.
3. Look for Python errors in the Dagster logs.

### Resource connection errors

1. Check `.env` — a missing variable usually surfaces as a resource construction
   error at import time, not at run time.
2. Verify network access to the database from inside the container.
3. Test the connection directly:

```python
from core.resources import IFDBOracleResource

resource = IFDBOracleResource()
with resource.get_connection() as conn:
    print("Connected!")
```

### Import errors

Imports must be absolute from the package root (`core.*`, `DSA.*`, `DSI.*`, `DSO.*`,
`utils.*`). Relative imports break depending on how Dagster loads the module.

Because those are *top-level* modules, `etl/` has to be the source root on
`sys.path` — that is what `PYTHONPATH` is for. Run the Dagster CLI from the repo
root, where the `[tool.dagster] module_name = "definitions"` block lives:

```bash
cd /workspaces/sydbi_basismodel && echo "$PYTHONPATH"
```

It must print `/workspaces/sydbi_basismodel/etl`. If it is empty or names another
path, the CLI fails with `ModuleNotFoundError: No module named 'definitions'`. The
devcontainer supplies the value from `.devcontainer/docker-compose.yml`, where
`environment:` overrides anything `env_file: ../.env` sets — so a stale `PYTHONPATH`
line in your own root `.env` is inert. A wrong value means the container predates
that change: rebuild it.

### Partition issues

1. Verify the partition key format matches the partition definition.
2. Check that the partition range covers the requested date.
3. Confirm the partition column exists in the source data.

Note that most new work uses DFM checkpoints rather than partitions — if you are
debugging "missing data" on a DFM asset, the checkpoint in `meta.DFMState` is the
thing to inspect, not partitions. See
[dfm_operations_guide.md](dfm_operations_guide.md).

## Deploying a feature branch

Multiple feature branches can run against the same Dagster instance —
see [Feature-branch deployments](feature_branch_deployments.md).

## Related documentation

- [Repository Tour](repo_tour.md) — structure, terminology, asset groups
- [Docs index & glossary](../README.md)
- [DSI ETL Guide](dsi_etl_guide.md) — source system → archive table
- [FHIR ETL Guide](fhir_etl_guide.md) — FHIR pipeline patterns
- [SQL ETL Guide](sql_etl_guide.md) — DWH architecture and ETL patterns
- [DFM Operations Guide](dfm_operations_guide.md) — running, replaying, resetting
- [Testing Guide](../../tests/README.md)
- [Dagster documentation](https://docs.dagster.io/)
