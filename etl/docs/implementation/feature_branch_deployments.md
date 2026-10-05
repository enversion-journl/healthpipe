<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Feature-Branch Deployments

Multiple feature branches can be deployed to the same Dagster instance at once, so
developers can test changes in isolation. This document covers how that works and
what it does — and does not — isolate.

## How it works

When `DAGSTER_BRANCH_POSTFIX` is set **and starts with `feature`**, every asset, job
and schedule name is postfixed with that value, preventing collisions between
deployments.

Branches that do not start with `feature` are **not** postfixed — production
deployments on `main`, `development` or `hotfix-*` are unaffected.

```
# Without postfix (production / main):
fhir_aktivitetsresultat_daily_ifdb

# With DAGSTER_BRANCH_POSTFIX=feature-patient-validation:
fhir_aktivitetsresultat_daily_ifdb_feature_patient_validation

# With DAGSTER_BRANCH_POSTFIX=main (not a feature branch — unchanged):
fhir_aktivitetsresultat_daily_ifdb
```

## Setting one up

Set the environment variable before starting Dagster:

```bash
export DAGSTER_BRANCH_POSTFIX=feature-add-patient-validation
dagster dev
```

In a deployment configuration (Nomad):

```hcl
env {
  DAGSTER_BRANCH_POSTFIX = "feature-branch-name"
}
```

In Docker Compose:

```yaml
services:
  dagster:
    environment:
      - DAGSTER_BRANCH_POSTFIX=feature-branch-name
```

## Postfix sanitization

The postfix is normalized automatically — special characters become underscores, the
value is lowercased, and leading/trailing underscores are stripped:

```
feature/add-patient     →  _feature_add_patient
Feature-JIRA-1234       →  _feature_jira_1234
feature.my.branch.name  →  _feature_my_branch_name
main                    →  (no postfix — not a feature branch)
hotfix-123              →  (no postfix — not a feature branch)
```

Keep postfixes short: they show up in every asset name in the UI.

## What it does not isolate

The postfix affects **Dagster names only**. Everything else is shared unless you
configure it separately:

- **Databases.** All deployments write to the same staging and DSO databases. Use
  separate schemas or separate target databases via environment variables if your
  change touches data that others rely on.
- **FHIR server.** Shared unless you point the deployment at another Aidbox instance.
- **DFM checkpoints.** `meta.DFMState` is keyed by schema and table, not by
  deployment — a feature branch running a delta extract will advance the production
  checkpoint for that table. Use ad-hoc `period` or `ids` runs (see
  [dfm_operations_guide.md](dfm_operations_guide.md)) when you need to test an extract
  without moving the checkpoint.

## Programmatic access

```python
from core.branch_postfix import (
    get_branch_postfix,            # current postfix
    is_feature_branch_deployment,  # is a postfix active?
    get_postfixed_name,            # postfix a string
    get_postfixed_asset_key,       # postfix an AssetKey
)

if is_feature_branch_deployment():
    print(f"Running with postfix: {get_branch_postfix()}")
```

## Cleanup

Tear down feature-branch deployments when the branch is merged or abandoned — they
hold resources and clutter the asset graph.
