<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Documentation Index

## Start here — primary documentation (Danish)

**[basismodel/](basismodel/)** is the primary documentation for the platform,
written in Danish, and the place a new team member starts. It walks the whole
path from a request to a delivered table: [00 overblik](basismodel/00_overblik.md)
→ [01 afklar behov](basismodel/01_afklar_behov.md) →
[02 DSI](basismodel/02_DSI_kilde_til_arkiv.md) →
[03a FHIR](basismodel/03a_DSA_arkiv_til_FHIR.md) /
[03a1 SQL on FHIR](basismodel/03a1_DSA_FHIR_til_SQLonFHIR.md) /
[03b dimensionel](basismodel/03b_DSA_arkiv_til_dimensionel_model.md) /
[03c dm-skema](basismodel/03c_DSA_arkiv_til_dmskema.md) →
[04 DSO](basismodel/04_DSO_sync_til_DSO.md) →
[05 dokumentation](basismodel/05_udfyld_dokumentation.md).

Everything below is **reference documentation**: deeper detail per subject, still
mostly in English, linked from the chapters above where relevant.

## Reference documentation

Read in this order for environment and orientation: root
[README](../../README.md) → [Development Setup](../../CONTRIBUTING.md#development-setup)
(devcontainer or local venv) → the
[glossary](#glossary) below → [Repository Tour](implementation/repo_tour.md)
(structure and terminology) → [Dagster Development
Guide](implementation/dagster_development.md) (the development loop).

The per-track reference guides are **[DSI](implementation/dsi_etl_guide.md)**
(source → archive), **[FHIR](implementation/fhir_etl_guide.md)** and
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
| [navngivningsstandard_basismodel.md](implementation/navngivningsstandard_basismodel.md) | The naming standard for this repository — every naming rule in one place, from branches and files to assets, tables, columns and indexes |
| [navngivningsstandard.md](implementation/navngivningsstandard.md) | The naming standard for the data mart repository |
| [sql_etl_guide.md](implementation/sql_etl_guide.md) | DWH architecture and SQL ETL patterns: `ext` → `arc` → `pre_stg`/`stg` → `map`/`dim`/`fct` |
| [kpi_aggregat_data_guide.md](implementation/kpi_aggregat_data_guide.md) | Externally delivered KPI/aggregate data as a source shape: revision strategy, ingestion guards, the dm path, and the five pitfalls |
| [fhir_etl_guide.md](implementation/fhir_etl_guide.md) | FHIR pipeline patterns and IFDB extraction |
| [codesystem_conceptmap_guide.md](implementation/codesystem_conceptmap_guide.md) | CodeSystem & ConceptMap pipeline patterns |
| [dfm_extract_factory_guide.md](implementation/dfm_extract_factory_guide.md) | Building DFM-based extract/archive assets |
| [dfm_operations_guide.md](implementation/dfm_operations_guide.md) | DFM checkpoint operations, ad-hoc runs, period orchestrator |
| [dso_sync_guide.md](implementation/dso_sync_guide.md) | Syncing staging tables to the DSO database |
| [os2mo_integration_guide.md](implementation/os2mo_integration_guide.md) | OS2mo (Magenta): the daily codesystem full loads (SOR, Okosted) out, and OS2mo's organisation back in |
| [data_lineage_guide.md](implementation/data_lineage_guide.md) | YAML lineage structure under `data_lineage/` |
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

### Guides (runbooks and user guides)

| Document | What it covers |
|---|---|
| [operations.md](operations.md) | Operations: the log format and the CPR masking, the health checks, the DSO sync retries and failure rules, the backup of the Dagster database |
| [qr_sfi_datapoint_runbook.md](guides/qr_sfi_datapoint_runbook.md) | QR → SFI datapoint chain: preflight, reconciliation, verification, rollback |
| [sfi_datapoint_brugervejledning.md](guides/sfi_datapoint_brugervejledning.md) | SFI datapoint user guide |
| [ifdb_relationer_og_felter.md](guides/ifdb_relationer_og_felter.md) | IFDB tables, columns and joins on the consumer surface |

### Contracts & reference data

| Document | What it covers |
|---|---|
| [configuration.md](configuration.md) | Every variable in `.env.template`: group, default and description |
| [shared_dimensions.md](shared_dimensions.md) | Shared dimensions across domains |
| [source_systems/](source_systems/) | Per-source schema references (IFDB, BOM_IFDB, ILS, SydAX, MySupply, KMD, Cosmic, DRG, Udredningsret) |
| [source_systems/udredningsret/udredningsret_model.md](source_systems/udredningsret/udredningsret_model.md) | SDS Udredningsret: målmodel, staging-regler og beslutninger U1–U18 (US-3 → US-6); kilden og arkivet i [udredningsret_kilde.md](source_systems/udredningsret/udredningsret_kilde.md) |
| [source_systems/lpr3/lpr3_kilde.md](source_systems/lpr3/lpr3_kilde.md) | SDS LPR3 Grunddata: extract, nøglebeslutninger og målinger på rigtige data |

## Archive (historical)

Historical plans and analyses stay in the private source repository and are not published.

## Glossary

| Term | Meaning |
|---|---|
| **DSI** | Data Staging In — extraction layer (`etl/DSI/`). 1:1 raw extraction from source systems into the `ext.*` schema; no business logic. |
| **DSA** | Data Staging Area — transform layer (`etl/DSA/`). SQL staging/dims/facts and FHIR transforms; lives in the staging database. |
| **DSO** | Data Staging Out — delivery layer (`etl/DSO/`). Dims, facts and flat FHIR tables synced to the DSO database, plus a view on top of a table where there is a reason for one (see [04 — DSO](basismodel/04_DSO_sync_til_DSO.md#5-viewet--kun-når-der-er-en-grund)). |
| **DFM** | Data Flow Management — the checkpoint/archive framework (see [dfm_design.md](architecture/dfm_design.md)): each pipeline layer tracks "where it left off", extracts land in volatile `ext.*` and are merged into the persistent `arc.*` archive, with ad-hoc period/ID runs that don't advance the production checkpoint. |
| **DQA** | Data Quality Assurance — the config-driven check framework in `etl/dqa/` (see [dqa/README.md](architecture/dqa/README.md)): one `DqaSubject` per table, checks run as Dagster `@asset_check`s, results persist to `dqa.Results`, with semi-automatic remediation. Not to be confused with `etl/data_quality/`, a small standalone set of FHIR grain/deleted-resource checks that predates the framework and is wired directly into `definitions.py`. |
| **ext / arc** | The two DSI schemas: `ext.*` is the volatile landing zone (dropped and reloaded each run), `arc.*` the persistent archive (1:1 source reflection + `dw_timestamp_*`). Downstream always reads from `arc.*`, never `ext.*`. |
| **CleanCode** | Standardized business-key string generated in pre-staging SQL (never present in `ext.*`/`arc.*`). Map tables translate CleanCodes to surrogate entity keys; facts store the resolved keys. See [sql_etl_guide.md](implementation/sql_etl_guide.md). |
| **`dw_ek_` / `dw_sk_`** | Column prefixes from the [naming standard](implementation/navngivningsstandard_basismodel.md): `dw_ek_` = entity key (resolved from CleanCodes via map tables; `-1` = unknown member), `dw_sk_` = surrogate key (Type 2 dimensions; resolved when the fact is read, via `dw_haendelsestidspunkt`). |
| **SOF / SQL on FHIR** | Flattening FHIR resources into MSSQL `fhir.*` tables via ViewDefinitions (`DSA/FHIR/SqlOnFhir/`). |
| **IFDB** | The Oracle-based clinical source system feeding the FHIR pipeline. |
