<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Nightly Auto-Triage — Runbook (DEV rollout)

Companion to `etl/docs/architecture/nightly_auto_triage_design.md`. This gets the DEV slice live.

## What ships in this repo

| Piece | Location |
|---|---|
| Failure sensor (run FAILURE → JSON report on nightly branch) | `etl/sensors/nightly_failure_sensor.py`, registered via `etl/sensors/__init__.py` |
| ADO reports-branch client (Git Pushes REST API) + fingerprinting | `etl/utils/azure_devops/reports_branch.py` |
| Unit tests | `etl/tests/unit/test_nightly_failure_report.py` |
| Cursor agent prompt | `.cursor/agents/nightly-triage.md` |

How it works: on each failed schedule/sensor-triggered run, the sensor commits one `.nightly/reports/<fingerprint>.json` per failed step to branch `nightly/<yyyy-mm-dd>-dev` (created from `development` when missing) via the ADO REST API — no local git clone. Repeat failures append occurrences to the existing report. The report date rolls at 07:00, so the whole overnight window (22:00–06:59) lands on the branch the agent reads at its next 07:00 run.

The sensor auto-starts only on the static `development` location (via `get_sensor_default_status()`); feature deployments skip reporting entirely (`NIGHTLY_REPORT_ENV` overrides for testing). Only runs with schedule/sensor lineage tags are reported — ad-hoc developer runs never create reports. Without a PAT the sensor skips with a visible reason, so deploying the code before the secret is harmless.

## Environment variables (code server)

| Variable | Example | Notes |
|---|---|---|
| `ADO_ORG_URL` | `https://dev.azure.com/enversion` | required |
| `ADO_PROJECT` | `RegionSyddanmark` | required |
| `ADO_PAT` | PAT, scope **Code Read & Write** | required; `ADO_WORKITEM_PAT` is accepted as a legacy fallback key |
| `ADO_REPOSITORY` | `RegionSyddanmark` (default: project name) | optional |
| `NIGHTLY_REPORT_BASE_BRANCH` | `development` (default) | branch the nightly branch is created from |
| `DAGSTER_WEBSERVER_URL` | `https://dagster-dev.example.org` | optional; makes reports link to the run |
| `NIGHTLY_REPORT_ENV` | `dev` | optional override of env detection (used for smoke tests) |

## Setup steps

1. **Create the PAT** in Azure DevOps (User settings → Personal access tokens): name `nightly-triage-dev`, scope **Code Read & Write**, note the expiry in your calendar. This PAT can push code — branch policies on `development`/`main` are the guardrail; the sensor only ever pushes to `nightly/*` branches.
2. **Seed the Nomad variable** (must happen BEFORE deploying the updated job — a missing key fails template rendering):

   ```bash
   nomad var get -namespace=sydbi nomad/jobs/dagster-code-server-dev -out=json > current.json
   # add ADO_ORG_URL, ADO_PROJECT and the PAT key to Items, then:
   nomad var put -namespace=sydbi @current.json
   ```

3. **Deploy** the updated `nomad_jobs/dagster/code-server-dev.nomad` (sydbi-infrastructure) and merge this repo's changes to `development`.
4. **Verify the sensor**: Dagster UI → Automation → `nightly_failure_report_sensor` should be RUNNING. Trigger a failure via a schedule/sensor-triggered run — or launch a failing asset from the Launchpad with tag `dagster/triggered_by: smoke-test` — then confirm branch `nightly/<date>-dev` exists and contains `.nightly/reports/<fingerprint>.json` with the root-cause class and full chain. Fail the same asset twice: the report must gain a second occurrence, not a second file.
5. **Create the Cursor scheduled agent** (cursor.com → Agents, ADO integration must be connected):
   - schedule: daily 07:00 Europe/Copenhagen
   - repo: `enversion/RegionSyddanmark`, base branch `development`
   - prompt: contents of `.cursor/agents/nightly-triage.md` (or an instruction to read that file first)
   - do a manual dry-run of the agent against a real nightly branch before enabling the schedule
6. **Morning workflow**: review PR `Nightly auto-triage yyyy-mm-dd (dev)`; the fix branch auto-deploys as a DEV feature location (existing CI + watcher), so failing assets can be re-materialized there before merging. Delete the `nightly/<date>-dev` branch after the PR is handled.

## Notes & caveats

- **Report date rolls at 07:00**: a failure at 23:00 and one at 05:00 land on the same branch — the one the agent reads next morning. A failure after 07:00 is filed for the following morning.
- **Cross-night duplicates**: dated branches carry no memory. The agent prompt's duplicate guard (search open PRs / recent findings docs for the fingerprint) is what prevents a recurring failure from getting a new fix PR every night.
- The sensor pushes to `nightly/*` only — that prefix triggers no CI pipelines.

## QA/PROD later

Enable per env by seeding the same variables into the qa/prod code-server Nomad vars and setting `NIGHTLY_REPORT_BASE_BRANCH` (current `release/*` for qa, `main` for prod). Each env writes its own `nightly/<date>-<env>` branch; the agent compares fingerprints across the branches at 07:00 and fixes each unique failure only in the lowest affected environment.
