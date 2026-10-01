<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- DCO sign-off check for pull requests.
- CI job that builds the code-server Docker images.
- Dependabot updates for pre-commit hooks.
- Release-note categories (`.github/release.yml`), `CHANGELOG.md` and `.editorconfig`.

### Changed

- Ruff replaces flake8 as the only linter. Bandit now blocks high-severity findings.
- `dagster definitions validate` is now a required CI check.
- CODEOWNERS points to `@enversion-journl/developers`.

## [1.0.75] - 2026-09-30

### Added

- GitHub CI (lint, unit tests with coverage, definitions validate, mypy, pip-audit,
  SPDX headers), CodeQL, OpenSSF Scorecard, dependency review and gitleaks workflows.
- Issue forms, pull request template, CODEOWNERS and Dependabot configuration.
- Devcontainer with SQL Server, Oracle, PostgreSQL and Aidbox (FHIR).
- Pre-commit hooks for secrets and PII (gitleaks) and SPDX license headers.
- Data quality assessment (`etl/dqa/`).

### Changed

- The application package moved from `dagster_etl/` to `etl/`.
- Jupyter moved from a runtime dependency to the optional `notebooks` extra.

## [1.0.0] - 2026-06-30

### Added

- First public release.

[Unreleased]: https://github.com/enversion-journl/healthpipe/compare/v1.0.75...HEAD
[1.0.75]: https://github.com/enversion-journl/healthpipe/compare/v1.0.0...v1.0.75
[1.0.0]: https://github.com/enversion-journl/healthpipe/releases/tag/v1.0.0
