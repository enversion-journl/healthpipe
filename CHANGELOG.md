# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.0.4] - 2026-10-16

### Added

- New source system: EPJI.
- Location dimension `dim.SHAKAlike` from EPJI (the EPJI locations as a SHAK-like
  hierarchy). The CodeSystem tables `codesystem.SHAKAlikeConcept`,
  `codesystem.SHAKAlikeProperties` and `codesystem.SHAKAlikeHierarchy` and the ConceptMap
  `conceptmap.SHAKAlikeToSOR` (EPJI location to SOR) come with it.
- New source system: SDS Udredningsret (LPR_Udredningsret). The daily
  `udredningsret_ingest_job` loads the deliveries into the archive. No DSO table uses
  the data yet.
- Operator asset `careplan_genkoersel_nulstilling`. It deletes the CarePlan data and
  resets its load checkpoints before a full reload.

### Changed

- **Breaking:** The FHIR CarePlan from IFDB (all markers) has one living CarePlan for
  each person, not for each IFDB patient ID. `fhir.CarePlan.identifier_value` is now
  `<FHIR patient id>_alle_markoerer`. In each environment, run
  `careplan_genkoersel_nulstilling` (dry run first) and then a full load of the CarePlan
  assets and their DSO sync. The CarePlan load stops while `fhir.CarePlan` has rows with
  the old identifier.
- `fhir.CarePlan_Activity.markoer_display` comes from the IFDB `EVENT_CODE` as
  `<type>: <code>`, not from `EVENT_TEXT`. A marker that is removed from a living
  CarePlan gets `dw_is_deleted = 1`.
- **Breaking:** `fhir.SFI_datapoint_store` changes to a clustered columnstore with
  monthly partitions. A migration drops the old store, so `fhir.SFI_datapoint` is empty
  until the first refresh is complete. The refresh can take hours. Apply this release
  in a time window in which an empty `fhir.SFI_datapoint` is acceptable.
- The IFDB to FHIR pipeline is one daily job, `ifdb_dfm_daily_job`, with the schedule
  `ifdb_dfm_daily_schedule` at 06:00. It replaces `fhir_ifdb_dfm_base_job`, the CarePlan
  job and the sensors that started them.
- The FHIR ServiceRequest load from IFDB creates the missing patients from the archive.
  It skips a request that it cannot resolve, and the nightly run continues.
- The DSO sync frees the cached query plans when it creates the linked server to staging
  again, and then reads all result sets.
- The migration `20260903_01_reset_vareudlevering_checkpoints.sql` is now
  `20260903_02_reset_vareudlevering_checkpoints.sql`, because a new migration takes the
  first rank of that date. A database that applied 1.0.3 runs the reset again. The reset
  is idempotent, but the next run loads the ILS history of `fct.Vareudlevering` again.

### Removed

- The legacy IFDB CarePlan job, the DFM base-job sensors and the CarePlan categories
  without a producer (only `alle_markoerer` stays).

## [1.0.3] - 2026-10-13

### Added

- Agreement steering bridge `bridge.AftaleLager` from ILS. It tells which hospitals and
  storage locations can buy on an agreement.

### Changed

- **Breaking:** `dim.Aftale` changes from SCD type 2 to type 1. The history columns
  `dw_valid_from` and `dw_valid_to` and the column `dw_is_deleted` are removed. The
  primary key is `dw_ek_Aftale`. Expired and deleted agreements stay as rows. The new
  column `alle_maa_benytte` comes from ILS.
- **Breaking:** `fct.AftaleVarePris` changes its grain. The old grain was one row for
  each price version of an agreement line. The new grain is one row for each price
  version, or for each agreement line without a price, and for each supplier item
  number. The columns `kreditorvarenummer1` to `kreditorvarenummer3` are removed. The
  new columns are `kilder` and `har_prisversion`. `dw_valid_to` is now the start of the
  next price version of the agreement line.
- **Breaking:** `dim.Dato` renames `kvartals_startdato` to `kvartal_startdato` and
  `kvartals_slutdato` to `kvartal_slutdato`.
- `dim.Leverandoervare` and `bridge.LeverandoervareILSVare` also load the alternative
  supplier item numbers from ILS.
- `fct.Vareudlevering`, `fct.Lagerbeholdning` and `fct.AfdelingLagerbeholdning` find
  more storage locations and requesters. A row without a storage location or a
  requester links to its hospital. A migration resets the load checkpoints, so the next
  run loads the ILS history again.
- The daily `dim.Dato` schedule starts automatically in the development, production and
  release deployments.

## [1.0.2] - 2026-10-07

### Added

- New source system: SDS DRG (somatic and psychiatric DRG deliveries).
- New source system: HAIBA.
- DRG contact fact `fct.DRGKontakt` from SDS DRG, with `dim.DRGKontaktEgenskaber` and
  `dim.DRGKontaktKlassifikation`.
- DRG diagnoses `dim.DRGDiagnose` from SDS DRG, with the group bridge
  `bridge.DRGDiagnoseGruppe`.
- DRG procedures `dim.DRGProcedure` from SDS DRG, with the group bridge
  `bridge.DRGProcedureGruppe`.
- HAIBA infection aggregates `dm.HAIBA_AggInfektion` and orthopaedic infection
  aggregates `dm.HAIBA_AggOrtopaedi`, each with its `Egenskaber` table.
- HAIBA infection cases `dm.HAIBA_CasesInfektion` and surgical site infection cases
  `dm.HAIBA_CasesSSI`, each with its `Egenskaber` table.
- FHIR DocumentReference resource from IFDB (discharge summaries and discharge
  reports), with `fhir.DocumentReference_Recipient`. The DSO sync now loads both tables.
- Data quality checks for the HAIBA tables, with new freshness and row-parity checks.
- Daily disk-space monitoring for the DSA and DSO databases.

### Changed

- **Breaking:** `dim.Organisation` renames the column `parent_kode` to `kode_parent`.
  The new column `fhir_Organization_id` holds the FHIR Organization id.
- `dim.Vareudlevering` gets more data from ILS: project name, storage location,
  dispatch number and stock item type.
- `fhir.Condition` also loads diagnoses from IFDB activity results. New columns:
  `episode_of_care_id` and `lpr3_ignored`.
- `fhir.Procedure` and `fhir.Procedure_tillaegskode` also load procedures and
  supplementary codes from IFDB activity results. `fhir.Procedure` gets sub-operations,
  knife-time columns and `lpr3_ignored`.
- `fhir.Observation` contains only curated SFI measurements. Decimal values are in
  `value_quantity`.

## [1.0.1] - 2026-10-05

### Added

- GitHub CI (lint, unit tests with coverage, definitions validate, mypy, pip-audit,
  SPDX headers), CodeQL, OpenSSF Scorecard, dependency review and gitleaks workflows.
- Issue forms, pull request template and CODEOWNERS.
- Devcontainer with SQL Server, Oracle, PostgreSQL and Aidbox (FHIR).
- Pre-commit hooks for secrets and PII (gitleaks) and SPDX license headers.
- Data quality assessment (`etl/dqa/`).
- DCO sign-off check for pull requests.
- CI job that builds the code-server Docker images.
- Release-note categories (`.github/release.yml`), `CHANGELOG.md` and `.editorconfig`.

### Changed

- The application package moved from `dagster_etl/` to `etl/`.
- Jupyter moved from a runtime dependency to the optional `notebooks` extra.
- Ruff replaces flake8 as the only linter. Bandit now blocks high-severity findings.
- `dagster definitions validate` is now a required CI check.
- CODEOWNERS points to `@enversion-journl/developers`.
- The code-server base image no longer installs the Microsoft ODBC driver. It installs
  only `libltdl7` and `libgssapi-krb5-2`, which `mssql-python` needs.

## [1.0.0] - 2026-06-30

### Added

- First public release.

[Unreleased]: https://github.com/enversion-journl/healthpipe/compare/v1.0.4...HEAD
[1.0.4]: https://github.com/enversion-journl/healthpipe/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/enversion-journl/healthpipe/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/enversion-journl/healthpipe/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/enversion-journl/healthpipe/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/enversion-journl/healthpipe/releases/tag/v1.0.0
