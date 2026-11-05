<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Dagster Development Guide

The development loop: making a change, getting it running on the Dev environment,
verifying it, and finding out why it does not work.

For *how to get set up at all* — the devcontainer or a local venv — read
[Development Setup](../../../CONTRIBUTING.md#development-setup) first.
For *where things live* and *what the words mean*, read the
[Repository Tour](repo_tour.md).

## Where the code runs

Assets run in the Dagster UI on the **Dev environment**, in the code location that
belongs to your feature branch. Pushing the branch is what deploys it: the first
push builds the branch's image, and every push after that is picked up by the
running code server within seconds. Nothing runs on your own machine.

Asset, job and schedule names in a feature-branch code location carry the branch
name as a postfix — see [feature_branch_deployments.md](feature_branch_deployments.md).

## The development loop

### 1. Decide where the change goes

See [Where a change goes](repo_tour.md#where-a-change-goes) in the Repository Tour,
then follow the guide for that layer — [DSI](dsi_etl_guide.md),
[FHIR](fhir_etl_guide.md) or [SQL/DWH](sql_etl_guide.md).

### 2. Write the asset

Follow the patterns in the neighbouring files rather than inventing new ones. Most
new work is a config entry consumed by an existing factory, not a hand-written asset.

### 3. Register it

Assets reach `definitions.py` through aggregate lists in module `__init__.py` files. An asset that is
not in one of those lists does not exist as far as Dagster is concerned — this is the
most common reason a new asset "does not show up". See
[How assets reach definitions](repo_tour.md#how-assets-reach-definitions) in the
Repository Tour.

### 4. Push, and check that the code location loaded

The pipeline's `static-checks` job runs `dagster definitions validate` (along with
mypy and the lineage check) and fails the pipeline on a broken definition. A code
location can still fail to load in the Dagster UI for reasons the job does not see.

So when your assets are missing or the code location is in error after a push:

1. Read the code location's error in the Dagster UI — it names the failing import.
2. Read the `static-checks` job log in the pipeline for the same information.
3. Check that the asset is included in the aggregate list that `definitions.py` imports.

### 5. Materialize

In the Dagster UI: **Assets** → find the asset in your code location →
**Materialize**. Use the asset graph to select an asset together with its upstream
dependencies.

Assets that use DFM run modes (`delta`, `full_load`, `period`, `ids`) are launched
from the **Launchpad** with a `RunModeConfig` — see
[dfm_operations_guide.md](dfm_operations_guide.md) for the exact YAML per mode.

### 6. Verify the data

Materializing without errors is not the same as being correct. Check row counts,
grain and — for anything checkpoint-based — that a second run is a no-op. The layer
guides list what to verify for each kind of asset. Query the databases from VS Code
with the dbcode extension.

Remember that a feature-branch deployment shares the databases, the FHIR server and
the DFM checkpoints with everything else on Dev — see
[What it does not isolate](feature_branch_deployments.md#what-it-does-not-isolate).

### 7. Test

Unit tests run in the pipeline (`pytest tests/unit/` with coverage) on every push.
Read the test-value policy and the suite layout in the
[Testing Guide](../../tests/README.md) before writing a test: config-entry, DDL-only
and registration changes do not warrant unit tests.

There is a hard safety gate on tests that talk to real resources: they abort if
`TEST_ENV` or `DAGSTER_ENVIRONMENT` contains `prod`.

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
dagster job execute -j ifdb_dfm_daily_job
```

No job in this deployment is partitioned — re-running a window is a
`RunModeConfig` (`mode: period` / `full_load` / `ids`), not `--partition`.

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
| **Deployment → Code locations** | Whether your branch's code location loaded, and its error if not |

## Troubleshooting

### Assets not loading

See [step 4](#4-push-and-check-that-the-code-location-loaded) above: the code
location's error and the `static-checks` log name the cause, and an asset missing
from an aggregate list is the usual one.

### Resource connection errors

Resource connection settings are `dg.EnvVar` fields. Dagster reads them when a run that
uses the resource starts, and a missing variable fails that run. When the code location
loads, `definitions.py` logs one warning that names every unset or empty variable of the
registered resources; the location still loads. The required variables per source are
listed in [`.env.template`](../../../.env.template), and the values come from the
deployment's environment.

### Import errors

Imports must be absolute from the package root (`core.*`, `DSA.*`, `DSI.*`, `DSO.*`,
`utils.*`). Relative imports break depending on how Dagster loads the module.

Because those are *top-level* modules, `etl/` has to be the source root on
`sys.path` — that is what `PYTHONPATH` is for, and the root pyproject's
`[tool.dagster] module_name = "definitions"` then resolves. A `PYTHONPATH` that does
not point at the repository's `etl/` directory fails with
`ModuleNotFoundError: No module named 'definitions'`.

### Partition issues

1. Verify the partition key format matches the partition definition.
2. Check that the partition range covers the requested date.
3. Confirm the partition column exists in the source data.

Note that most new work uses DFM checkpoints rather than partitions — if you are
debugging "missing data" on a DFM asset, the checkpoint in `meta.DFMState` is the
thing to inspect, not partitions. See
[dfm_operations_guide.md](dfm_operations_guide.md).

## Related documentation

- [Development Setup](../../../CONTRIBUTING.md#development-setup) — devcontainer or
  local venv
- [Repository Tour](repo_tour.md) — structure, terminology, asset groups
- [Docs index & glossary](../README.md)
- [DSI ETL Guide](dsi_etl_guide.md) — source system → archive table
- [FHIR ETL Guide](fhir_etl_guide.md) — FHIR pipeline patterns
- [SQL ETL Guide](sql_etl_guide.md) — DWH architecture and ETL patterns
- [DFM Operations Guide](dfm_operations_guide.md) — running, replaying, resetting
- [Feature-branch deployments](feature_branch_deployments.md)
- [Testing Guide](../../tests/README.md)
- [Dagster documentation](https://docs.dagster.io/)
