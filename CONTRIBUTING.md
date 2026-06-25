# Contributing to Healthpipe

Thank you for your interest in contributing to Healthpipe! Whether you're fixing a bug, improving documentation, or building a new feature — all contributions are welcome.

Healthpipe is developed by [Enversion Journl ApS](https://docs.journl.dk/v1/) as part of the broader Journl ecosystem for healthcare data integration and FHIR-based solutions. This guide describes the process and requirements for contributions.

## Code of Conduct

This project follows the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behaviour as described in the Code of Conduct.

## Ways to Contribute

There are many ways to contribute beyond writing code:

- **Bug reports** — found something broken? Open an issue (see below)
- **Feature requests** — have an idea? Propose it as an issue for discussion
- **Documentation** — improve guides, fix typos, add examples
- **Tests** — increase coverage or add missing test cases
- **Code** — fix bugs, refactor, or implement new features

If you're unsure where to start, look for issues labelled **`good first issue`** or **`help wanted`**.

## Reporting Bugs

When filing a bug report, include:

1. **Summary** — a clear, concise description of the problem
2. **Steps to reproduce** — minimal steps to trigger the issue
3. **Expected behaviour** — what you expected to happen
4. **Actual behaviour** — what actually happened
5. **Environment** — Python version, OS, relevant service versions
6. **Logs / tracebacks** — paste relevant error output (redact any credentials)

> 💡 The easiest way is the **Bug report** issue form: **Issues → New issue → Bug report**. It walks you through every field above. (Form definition: [`.github/ISSUE_TEMPLATE/bug_report.yml`](.github/ISSUE_TEMPLATE/bug_report.yml).)

## Suggesting Features

Open an issue with the **`enhancement`** label. Describe:

- The problem or use case you want to address
- Your proposed solution (if any)
- Alternatives you have considered

Feature discussions help ensure alignment before significant implementation effort.

> 💡 Use the **Feature request** issue form (**Issues → New issue → Feature request**) — it applies the **`enhancement`** label for you. (Form definition: [`.github/ISSUE_TEMPLATE/feature_request.yml`](.github/ISSUE_TEMPLATE/feature_request.yml).)

## Development Setup

### Option A: Dev Containers (recommended)

1. Clone the repository and open it in VS Code / Cursor with the Dev Containers extension
2. Copy `.env.template` to `.env` and fill in your credentials (substitute your own variables if necessary)
3. Select "Reopen in Container" when prompted — this sets up all dependencies automatically (Python 3.12, SQL Server, PostgreSQL, FHIR server, Oracle)
4. Access Dagster at http://localhost:3000

### Option B: Local venv (without Dev Containers)

1. Clone the repository
2. Copy `.env.template` to `.env` and update the connection details to point at your actual database servers (MSSQL, Oracle, FHIR, PostgreSQL)
3. Create a virtual environment and install dependencies:
   ```bash
   python3.12 -m venv .venv
   source .venv/bin/activate
   pip install -e ".[dev]"
   ```
4. Set environment variables:
   ```bash
   export DAGSTER_HOME=$(pwd)/.devcontainer/dagster_home
   export PYTHONPATH=$(pwd)/dagster_etl
   ```
5. Run Dagster: `dagster dev -m definitions`
6. Access Dagster at http://localhost:3000


See the [Dagster Development Guide](dagster_etl/docs/implementation/dagster_development.md) for detailed setup and workflow instructions.

## Contribution Process

1. **Fork** the repository and clone to your machine
2. **Create a branch** for your change (`git checkout -b feature/my-feature`)
3. **Develop** your change — follow the project's coding standards
4. **Test** that your change works (see below)
5. **Commit** with DCO sign-off (see below)
6. **Push** to your fork
7. **Open a Pull Request** against main/master

## Pull Request Requirements

When you open a pull request, the [PR template](.github/PULL_REQUEST_TEMPLATE.md) loads automatically with a checklist covering the points below — please fill it in.

- **Description**: Clear description of the change and motivation
- **Scope**: Keep pull requests focused — one logical change per PR
- **Tests**: New functionality should have tests; existing tests must still pass
- **Formatting**: Use Black (line-length=100) and Ruff for linting
- **Imports**: Use absolute imports (`core.*`, `DSA.*`, `DSI.*`, `DSO.*`, `utils.*`) — never relative imports
- **Documentation**: Update relevant guides in `dagster_etl/docs/` when changing architecture or flows

### Run tests

```bash
cd dagster_etl
pytest
```

For specific markers, e.g.:

```bash
pytest -m "not integration"
```

## Code Reviews

All submissions require review before merging. We use pull requests for this purpose — consult the [GitHub documentation on pull requests](https://docs.github.com/en/pull-requests) if you are new to the process.

During review, maintainers may ask for changes. Please address feedback promptly and push updated commits to the same branch. Reviews focus on:

- Correctness and test coverage
- Adherence to project patterns and coding standards
- Clear naming and readability
- No regressions in existing functionality

## DCO (Developer Certificate of Origin)

By contributing, you agree that your contributions are licensed under AGPL-3.0. We use DCO to ensure all contributions have permission from the rights holder.

**Every commit must include sign-off:**

```bash
git commit -s -m "Your descriptive commit message"
```

`-s` adds `Signed-off-by: Your Name <your@email.com>` to the commit message. By signing off, you certify:

> I certify that I have the right to submit this contribution under the project's open source license (AGPL-3.0).

If you forget sign-off, you can add it to the last commit:

```bash
git commit --amend -s --no-edit
```

## Coding Standards

- **Python 3.12**, Black formatting (line-length=100), Ruff linting
- Follow existing patterns in the directory you are editing

## Questions

Open an issue for questions about the contribution process or architecture. There are no bad questions — we'd rather help you contribute than have you stuck.
