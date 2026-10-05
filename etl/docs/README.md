<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Documentation Index

Entry point for all pipeline documentation. New to the project? Read in this
order: root [README](../../README.md) → [devcontainer
guide](../../.devcontainer/README.md) (environment setup) → the
[glossary](#glossary) below → [Repository Tour](implementation/repo_tour.md)
(structure and terminology) → [Dagster Development
Guide](implementation/dagster_development.md) (the local run loop).

Then read the guide for the track you are working in: **[DSI](implementation/dsi_etl_guide.md)**
(source → archive), **[FHIR](implementation/fhir_etl_guide.md)** or
**[SQL/DWH](implementation/sql_etl_guide.md)**.

Documents are split into **living guides** (kept current, authoritative) and an
**[archive](#archive-historical)** (completed one-off plans and analyses, kept
for history — do not rely on them for current behavior).

## Living guides

### Implementation (how to build and operate the pipelines)

| Document | What it covers |
|---|---|
| [repo_tour.md](implementation/repo_tour.md) | Repository structure, Dagster terminology as used here, asset groups, where a change goes |
| [dagster_development.md](implementation/dagster_development.md) | The local development loop: prerequisites, validate/materialize, troubleshooting |
| [dsi_etl_guide.md](implementation/dsi_etl_guide.md) | DSI: onboarding a new source system, and adding an archive table (`ext` → `arc`) |
| [navngivningsstandard.md](implementation/navngivningsstandard.md) | The naming standard (snake_case grammar, `dw_*` columns) — single source of truth for naming |
| [sql_etl_guide.md](implementation/sql_etl_guide.md) | DWH architecture and SQL ETL patterns: `ext` → `arc` → `pre_stg`/`stg` → `map`/`dim`/`fct` |
| [fhir_etl_guide.md](implementation/fhir_etl_guide.md) | FHIR pipeline patterns and IFDB extraction |
| [codesystem_conceptmap_guide.md](implementation/codesystem_conceptmap_guide.md) | CodeSystem & ConceptMap pipeline patterns |
| [dfm_extract_factory_guide.md](implementation/dfm_extract_factory_guide.md) | Building DFM-based extract/archive assets |
| [dfm_operations_guide.md](implementation/dfm_operations_guide.md) | DFM checkpoint operations, ad-hoc runs, period orchestrator |
| [dso_sync_guide.md](implementation/dso_sync_guide.md) | Syncing staging tables to the DSO database |
| [data_lineage_guide.md](implementation/data_lineage_guide.md) | YAML lineage structure under `data_lineage/` |
| [ldap_ad_guide.md](implementation/ldap_ad_guide.md) | LDAP/AD integration |
| [sikkerhedsmodel.md](implementation/sikkerhedsmodel.md) | Security model |
| [archive_merge_options.md](implementation/archive_merge_options.md) | Archive-merge strategy options (`ArchiveMergeConfig` flag reference) |
| [feature_branch_deployments.md](implementation/feature_branch_deployments.md) | Running multiple feature branches on one Dagster instance |

### Architecture (designs still in effect)

| Document | What it covers |
|---|---|
| [dqa/README.md](architecture/dqa/README.md) | The DQA framework: config-driven data-quality checks, tracks, orchestration |
| [dqa/remediation_runbook.md](architecture/dqa/remediation_runbook.md) | Operating the DQA remediation assets (replay, cleanup, delete propagation) |
| [dfm_design.md](architecture/dfm_design.md) | Data Flow Management design: checkpoints, archive, ad-hoc overrides |
| [dsa_delete_handling_strategy.md](architecture/dsa_delete_handling_strategy.md) | Soft-delete handling in DSA |
| [dso_delta_load_strategy.md](architecture/dso_delta_load_strategy.md) | Watermark-based delta sync to DSO |
| [core_cache_parallel_execution.md](architecture/core_cache_parallel_execution.md) | Identifier caches and parallel chunked execution |
| [nightly_auto_triage_design.md](architecture/nightly_auto_triage_design.md) | Nightly failure auto-triage design |

### Guides (runbooks and user guides)

| Document | What it covers |
|---|---|
| [nightly_auto_triage_runbook.md](guides/nightly_auto_triage_runbook.md) | Operating the nightly auto-triage |
| [qr_sfi_datapoint_runbook.md](guides/qr_sfi_datapoint_runbook.md) | QR → SFI datapoint chain: preflight, reconciliation, verification, rollback |
| [sfi_datapoint_brugervejledning.md](guides/sfi_datapoint_brugervejledning.md) | SFI datapoint user guide |

### Contracts & reference data

| Document | What it covers |
|---|---|
| [fhir_careplan_patientrettigheder.md](contracts/fhir_careplan_patientrettigheder.md) | CarePlan data contract (patientrettigheder) |
| [shared_dimensions.md](shared_dimensions.md) | Shared dimensions across domains |
| [source_systems/](source_systems/) | Per-source schema references (IFDB, BOM_IFDB, ILS, SydAX, MySupply, KMD, Cosmic) |

## Archive (historical)

Historical plans and analyses stay in the private source repository and are not published.

## Glossary

| Term | Meaning |
|---|---|
| **DSI** | Data Staging In — extraction layer (`etl/DSI/`). 1:1 raw extraction from source systems into the `ext.*` schema; no business logic. |
| **DSA** | Data Staging Area — transform layer (`etl/DSA/`). SQL staging/dims/facts and FHIR transforms; lives in the staging database. |
| **DSO** | Data Staging Out — delivery layer (`etl/DSO/`). Dims, facts and flat FHIR tables synced to the reporting database, plus presentation views. |
| **DFM** | Data Flow Management — the checkpoint/archive framework (see [dfm_design.md](architecture/dfm_design.md)): each pipeline layer tracks "where it left off", extracts land in volatile `ext.*` and are merged into the persistent `arc.*` archive, with ad-hoc period/ID runs that don't advance the production checkpoint. |
| **DQA** | Data Quality Assurance — the config-driven check framework in `etl/dqa/` (see [dqa/README.md](architecture/dqa/README.md)): one `DqaSubject` per table, checks run as Dagster `@asset_check`s, results persist to `dqa.Results`, with semi-automatic remediation. Not to be confused with `etl/data_quality/`, a small standalone set of FHIR grain/deleted-resource checks that predates the framework and is wired directly into `definitions.py`. |
| **ext / arc** | The two DSI schemas: `ext.*` is the volatile landing zone (dropped and reloaded each run), `arc.*` the persistent archive (1:1 source reflection + `dw_timestamp_*`). Downstream always reads from `arc.*`, never `ext.*`. |
| **CleanCode** | Standardized business-key string generated in pre-staging SQL (never present in `ext.*`/`arc.*`). Map tables translate CleanCodes to surrogate entity keys; facts store the resolved keys. See [sql_etl_guide.md](implementation/sql_etl_guide.md). |
| **`dw_ek_` / `dw_sk_`** | Column prefixes from the [naming standard](implementation/navngivningsstandard.md): `dw_ek_` = entity key (resolved from CleanCodes via map tables; `-1` = unknown member), `dw_sk_` = surrogate key (Type 2 dimensions; resolved in the view layer). |
| **SOF / SQL on FHIR** | Flattening FHIR resources into MSSQL `fhir.*` tables via ViewDefinitions (`DSA/FHIR/SqlOnFhir/`). |
| **IFDB** | The Oracle-based clinical source system feeding the FHIR pipeline. |
