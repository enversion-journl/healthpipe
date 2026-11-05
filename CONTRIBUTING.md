# Contributing to Healthpipe

Thank you for your help with Healthpipe. We welcome bug reports, documentation, tests and code.

[Enversion Journl ApS](https://docs.journl.dk/v1/) develops Healthpipe as part of the Journl ecosystem for healthcare data integration and FHIR-based solutions. This guide gives the process and the rules for a contribution.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). When you take part, you agree to obey it. Report unacceptable behaviour as the Code of Conduct describes.

## How a Release Replaces the Tree

The maintainers develop Healthpipe in an internal source and publish it to this repository. Each release replaces the full tree on `main` with one signed-off commit, `release: <version>`. A change that only this repository contains does not stay after the next release.

When a maintainer merges your pull request into `main`:

1. The maintainers carry the change into the internal source or into the publish configuration.
2. The next release contains the change and its `CHANGELOG.md` entry.

Dependabot opens monthly update pull requests for four ecosystems. The maintainers do not merge them. They apply each update upstream, and the pull request closes when a release contains the update:

- `github-actions`: the maintainers apply the update to the workflows in the publish configuration. Each action stays pinned to a full commit SHA.
- `uv`: the maintainers update `uv.lock` in the internal source with `uv lock --upgrade-package <name>`.
- `docker`: the maintainers update the Dockerfiles of `infrastructure/containers/dockerfiles/` in the internal source.
- `devcontainers`: the maintainers update the feature versions of `.devcontainer/` in the internal source.

## Questions

Use the **Question** issue form: **Issues → New issue → Question**. Blank issues are off. Report a security issue privately, as [SECURITY.md](SECURITY.md) describes.

If you look for a first task, find the issues with the label **`good first issue`** or **`help wanted`**.

## Reporting Bugs

Use the **Bug report** issue form: **Issues → New issue → Bug report** ([`.github/ISSUE_TEMPLATE/bug_report.yml`](.github/ISSUE_TEMPLATE/bug_report.yml)). Give this data:

1. **Summary**: a short description of the problem
2. **Steps to reproduce**: the minimum steps that cause the problem
3. **Expected behaviour** and **actual behaviour**
4. **Environment**: the Python version, the OS and the versions of the related services
5. **Logs and tracebacks**: the related error output, without credentials

## Suggesting Features

Use the **Feature request** issue form: **Issues → New issue → Feature request** ([`.github/ISSUE_TEMPLATE/feature_request.yml`](.github/ISSUE_TEMPLATE/feature_request.yml)). The form adds the **`enhancement`** label. Describe the problem, your solution and the alternatives. Agree on the design in the issue before you start a large change.

## Development Setup

You need [uv](https://docs.astral.sh/uv/) and [just](https://just.systems/). Install `just` with `brew install just`, or run it without an install: `uvx --from rust-just just <target>`. The [configuration reference](etl/docs/configuration.md) describes each variable of `.env`.

### Option A: Dev Containers (recommended)

1. Clone the repository and open it in VS Code with the Dev Containers extension.
2. Run `cp .env.template .env`. Set your credentials in `.env`.
3. Select "Reopen in Container". The container installs the locked dependencies and starts SQL Server, PostgreSQL, Oracle and the FHIR server.
4. Run `uvx --from rust-just just dev`. Open Dagster at http://localhost:3000.

### Option B: Local setup with uv

1. Clone the repository.
2. Run `cp .env.template .env`. Set the connection details of your MSSQL, Oracle, FHIR and PostgreSQL servers.
3. Run `just install`. This runs `uv sync --locked --extra dev` and installs the pre-commit hooks.
4. Run `export DAGSTER_HOME=$(pwd)/.devcontainer/dagster_home`.
5. Run `just dev`. Open Dagster at http://localhost:3000.

### Commands

Run `just` to list the targets. `just test` and `just validate` need the `.env` file.

- `just install`: install the locked dependencies and the pre-commit hooks
- `just lint`: run the pre-commit hooks on the files that changed since `origin/main`, and check the license headers
- `just format`: run Black and the Ruff fixes
- `just test`: run the unit tests with coverage (add pytest arguments, for example `just test -k fhir`)
- `just test-integration`: run the integration tests against the devcontainer databases
- `just typecheck`: run mypy on `etl`
- `just validate`: load the Dagster definitions
- `just dev`: start the Dagster web UI on http://localhost:3000

The [Dagster Development Guide](etl/docs/implementation/dagster_development.md) gives the workflow. The [operations guide](etl/docs/operations.md) describes logs, health checks and backup.

### Test time budget

The CI job `test (pytest unit)` takes about 305 seconds. Of this time, pytest takes about 250 seconds and the install takes about 45 seconds. On an Apple M4 Pro, `just test` takes about 80 seconds. CI does not run the integration tests. Keep the CI job near 305 seconds:

- Mock the databases and the network in a unit test.
- Put a test that needs a real database in `etl/tests/integration/`.

## Contribution Process

1. Fork the repository and clone your fork.
2. Create a branch from `main`, for example `git checkout -b fix/short-name`.
3. Make your change. Obey the [coding standards](#coding-standards).
4. Run `just lint`, `just test` and `just typecheck`.
5. Commit with a [commit message](#commit-messages) and a [DCO sign-off](#dco-developer-certificate-of-origin).
6. Push the branch to your fork.
7. Open a pull request against `main`.

## Pull Request Requirements

The [PR template](.github/PULL_REQUEST_TEMPLATE.md) contains the definition of done. Fill it in. A pull request is done when:

- It contains one logical change, and its description gives the change and the reason.
- New functionality has tests, and `just test` passes.
- `just lint` passes.
- The guides in `etl/docs/` agree with the changed architecture or flows.
- `CHANGELOG.md` has an entry under `[Unreleased]` for a change that users can see. Start the entry with `**Breaking:**` when the change breaks the existing behaviour or output.
- Each commit has a DCO sign-off.

## Commit Messages

Use [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/): `<type>(<scope>): <summary>`.

- Use one of these types: `feat`, `fix`, `docs`, `test`, `refactor`, `perf`, `build`, `ci` or `chore`.
- The scope is optional. Use the area of the change, for example `fix(DSO): ...`.
- Write the summary in the imperative mood and in lower case, without a full stop.
- Add `!` before the colon for a breaking change, for example `feat(DSO)!: ...`.
- Sign off each commit with `git commit -s`.

Only the maintainers use the type `release`, for the release commits.

## DCO (Developer Certificate of Origin)

When you contribute, you agree that your contribution has the AGPL-3.0 license. The DCO shows that the rights holder gives this permission. The `DCO sign-off` check fails for a commit without a `Signed-off-by:` line.

```bash
git commit -s -m "fix(DSO): correct the sync of deleted rows"
```

`-s` adds `Signed-off-by: Your Name <your@email.com>` to the commit message. With the sign-off, you certify:

> I certify that I have the right to submit this contribution under the project's open source license (AGPL-3.0).

To add the sign-off to the last commit, run `git commit --amend -s --no-edit`. To add it to all commits of the branch, run `git rebase --signoff origin/main` and force-push.

## Code Reviews

Each pull request needs a review before it merges. The review examines the correctness, the tests, the project patterns, the names and the regressions. Push your corrections to the same branch.

## Coding Standards

- Use Python 3.12.
- Black formats the code, and Ruff lints it. Both run as pre-commit hooks (`.pre-commit-config.yaml`), with the line length 100 from `pyproject.toml`.
- New code in `etl/utils` must pass `mypy --strict etl/utils`. CI runs this check. The mypy check of the rest of `etl` only reports.
- Use absolute imports (`core.*`, `DSA.*`, `DSI.*`, `DSO.*`, `utils.*`). Do not use relative imports.
- Follow the existing patterns of the directory that you change.

## Releases

Each release is one signed-off commit `release: <version>` on `main`, with a tag and a `CHANGELOG.md` section. While the major number is 1, a release with a **Breaking:** entry increases the minor number, for example from 1.0.8 to 1.1.0. All other releases increase the patch number, for example from 1.0.8 to 1.0.9.
