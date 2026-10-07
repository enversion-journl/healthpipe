# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/enversion-journl/healthpipe/compare/v1.0.2...HEAD
[1.0.2]: https://github.com/enversion-journl/healthpipe/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/enversion-journl/healthpipe/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/enversion-journl/healthpipe/releases/tag/v1.0.0
