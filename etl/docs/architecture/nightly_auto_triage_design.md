<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Nightly Auto-Triage & Fix Pipeline — Design

**Date:** 2026-07-10 · **Status:** Phase 1 (DEV) implemented — see `etl/docs/guides/nightly_auto_triage_runbook.md` · **Owner:** Daniel

## Goal

Every night, failed Dagster runs on DEV/QA/PROD are committed as JSON failure reports to a dated `nightly/<date>-<env>` branch. At 07:00 a scheduled Cursor cloud agent reads the reports, dedups across environments, creates `feature/<yyyy-mm-dd>-automatic-{env}` branches with a findings document plus one commit per fix, and opens PRs — ready for developer review first thing in the morning.

Non-goals: auto-merge (a human always reviews), fixing source-system/infra outages (these get flagged for humans), touching QA/PROD for issues that also exist in DEV.

## Architecture

```
 DEV / QA / PROD (restricted network, egress-only)
 ┌──────────────────────────────────────────────┐
 │ Dagster code server                          │
 │  nightly runs (22:00 → ~06:30)               │
 │  └─ run_status_sensor(FAILURE)               │
 │      └─ root-cause chain + fingerprint       │
 │      └─ ADO Git Pushes API ──────────────────┼──► branch nightly/<date>-<env>
 └──────────────────────────────────────────────┘      .nightly/reports/<fp>.json
                                                               │
                       07:00 daily                             ▼
                  ┌────────────────────┐            checks out nightly branch,
                  │ Cursor scheduled    │◄───────── reads the JSON reports
                  │ cloud agent         │
                  └────────┬───────────┘
                           ▼
        branch feature/yyyy-mm-dd-automatic-{env}
        · commit 1: docs/nightly/yyyy-mm-dd-{env}.md (findings)
        · commit 2..n: one fix per fingerprint [<fp> in message]
        · PR → development | release/x.y.z | main
```

## Component 1 — Failure reporter (Dagster, per env)

A `run_status_sensor(run_status=FAILURE)` monitoring all jobs in the code location (static registration via `all_sensors` — it needs no postfixed job references).

| File | Purpose |
|---|---|
| `etl/utils/azure_devops/reports_branch.py` | Thin `requests` client for the ADO Git REST API (refs, items, pushes) plus fingerprinting. No local git clone — a sensor tick pushes commits via the Pushes API with a compare-and-swap retry on the branch head. |
| `etl/sensors/nightly_failure_sensor.py` | Sensor factory: extract failures from the run's event log, walk to the root cause, fingerprint, upsert the JSON report. |

Behavior per failed run:

1. Pull the STEP_FAILURE events (RUN_FAILURE as fallback) from the event log and walk each error's cause chain to the root — Dagster wraps op exceptions in `DagsterExecutionStepExecutionError`, so class, message, and frames come from the innermost error while the report carries the full outer-to-root chain.
2. Compute fingerprint: `sha1(step_key | root exception class | top 5 stack frames normalized to repo-relative path + function, line numbers stripped)[:12]`.
3. Upsert `.nightly/reports/<fingerprint>.json` on branch `nightly/<report-date>-<env>` (created from `NIGHTLY_REPORT_BASE_BRANCH`, default `development`, when missing): new fingerprint → new report file; repeat → append an occurrence (run id/url, job, partition, timestamp, truncated chain; capped at 10, newest kept).
4. The report date rolls at 07:00 so the overnight window 22:00–06:59 lands on a single branch — the one the agent reads at its next run.

Environment label from `DAGSTER_CODE_LOCATION_NAME` / `DAGSTER_BRANCH_POSTFIX` (existing convention); `NIGHTLY_REPORT_ENV` overrides for testing. Feature-branch deployments never report. Only runs with schedule/sensor lineage tags (`dagster/schedule_name`, `dagster/triggered_by` — both inherited by re-executions) are reported; ad-hoc developer runs are skipped. Sensor default status via `get_sensor_default_status()`.

## Component 2 — Nightly branch as the transport

One dated branch per env per morning; one JSON file per unique failure. Same-night dedup is the file upsert; the branch is the raw record the agent consumes and can be deleted once the morning PR is handled.

What dated branches deliberately do NOT carry is cross-night memory: a failure recurring every night produces a fresh report each morning. The agent's duplicate guard covers this — before fixing a fingerprint it searches open PRs into `development` and recent `docs/nightly/` findings docs, and skips fingerprints already in review.

Issues the agent judges non-code-fixable (source system down, credentials, infra, bad source data) are recorded as `needs-human` in the findings doc.

## Component 3 — Cursor scheduled cloud agent (07:00)

A Cursor cloud agent on a daily schedule against the ADO repo (integration is in public beta, Cloud Agents only, `dev.azure.com` — matches this repo). The prompt lives versioned at `.cursor/agents/nightly-triage.md`.

Prompt outline (DEV phase):

1. Check out `nightly/<today>-dev`; absent branch = no failures, stop. Read `.nightly/reports/*.json`.
2. Duplicate guard: skip fingerprints with an open fix PR.
3. Create `feature/<today>-automatic-dev` off `development`; first commit adds `docs/nightly/<today>-dev.md` summarizing every report (including `needs-human` and `already in review` ones); then one commit per fingerprint it can confidently fix, message `fix(nightly): <summary> [<fingerprint>]`.
4. Run the relevant tests; if a fix can't be validated, document it instead of committing speculative code.
5. Open PR into `development`, reviewer Daniel, description listing fingerprints, root causes, and run links.
6. Hard limits: max 5 fixes per night per env; never touch `.env*`, secrets, or `infrastructure/ddl/migrations/` unless the failure is itself a migration error; never push to `development`/`main` or the `nightly/` branch.

Multi-env routing (later phases): each env writes its own nightly branch; the agent compares fingerprints across them — present in dev → fix on `development` only; qa without dev → current `release/*`; prod only → `main`.

A pleasant side effect of the `feature/…-automatic-dev` naming: the existing CI + ACR watcher builds and deploys it as a live feature code location in DEV, so the previously failing asset can be re-materialized there before merging. The sensor's own pushes go to `nightly/*`, which triggers no CI.

## Secrets & infra changes (sydbi-infrastructure)

- PAT with scope **Code Read & Write**, injected as `ADO_PAT` (legacy key `ADO_WORKITEM_PAT` also accepted) plus `ADO_ORG_URL` / `ADO_PROJECT` via Nomad Variables on the code-server jobs — same mechanism as the existing gitpoll PAT. One PAT per env for revocability.
- This PAT can push code from inside the restricted network. Mitigations: branch policies on `development`/`main` (the PAT cannot merge), the sensor only writes `nightly/*` refs, and the key is scoped per env.
- Egress: `dev.azure.com` must be reachable from the code servers — already true (gitpoll clones from it).
- Cursor side: credentials live only in the Cursor environment, never inside the restricted network.
- Note: no prod dagster job specs exist yet in `nomad_jobs/dagster/` (only the prod watcher). Prod reporter rollout depends on those.

## Security — stacktraces leave the network

Team-confirmed: pipeline code does not log data values, so no content redaction is applied. The reporter ships exception class, message, and code frames, truncated to 3,000 chars per occurrence. If a data-bearing exception message ever shows up in a report, add a scrubber in `reports_branch.py` at that point. The reports live in the repo itself, so repo access governs report access.

## Timing

07:00 is a hard requirement — PRs must be ready before the workday starts, ahead of clients noticing issues. All nightly runs are expected to be finished by then (schedules run 22:00 → 06:00). The report-date roll at 07:00 files any later failure for the following morning automatically.

## Rollout plan

1. **Reporter in DEV only** — ✅ implemented: `sensors/nightly_failure_sensor.py`, `utils/azure_devops/reports_branch.py`, tests in `tests/unit/test_nightly_failure_report.py`, agent prompt in `.cursor/agents/nightly-triage.md`, Nomad wiring in sydbi-infrastructure `code-server-dev.nomad`. Watch a week of nightly branches for noise/fingerprint quality.
2. **Enable QA (and PROD when job specs exist)** — seed the ADO vars per env, set `NIGHTLY_REPORT_BASE_BRANCH`; extend the agent prompt's routing beyond dev.
3. **Manual agent dry-runs** — run the triage prompt by hand against real nightly branches a few mornings; tune prompt and limits.
4. **Enable the 07:00 schedule.**
5. **Optional:** wire the synthetic MSSQL/Oracle databases into the Cursor environment (`environment.json` services) so the agent can reproduce failures, not just read traces.

## Open questions

- PROD Dagster deployment specs are missing from `nomad_jobs/dagster/` — where does prod actually run today?
- QA base branch discovery: `NIGHTLY_REPORT_BASE_BRANCH` must be updated when a new `release/*` is cut — acceptable, or resolve dynamically?
- Nightly-branch retention: delete after the morning PR, or keep as an audit trail?
- Should the findings doc also be posted somewhere visible (Teams webhook exists in the watcher) as a morning digest?
