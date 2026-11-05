# Security policy

## Supported versions

The latest release of healthpipe receives security fixes. Older releases do not.

| Version | Supported |
| ------- | --------- |
| Latest 1.x release | Yes |
| Older releases | No |

See [CHANGELOG.md](CHANGELOG.md) for the latest release.

## Report a vulnerability

Do not open a public issue for a vulnerability.

Report the vulnerability privately with the GitHub form
[Report a vulnerability](https://github.com/enversion-journl/healthpipe/security/advisories/new).
If you cannot use GitHub, send an e-mail to support@journl.dk.

Include:

- A description of the vulnerability.
- The steps to reproduce it.
- The impact: which data or which service an attacker can reach.
- The healthpipe version, or the commit.
- A suggested fix, if you have one.

Do not send real patient data, CPR numbers or credentials in the report.
Use synthetic data to show the problem.

## What happens next

1. We acknowledge your report within 5 working days.
2. We confirm the problem and agree a disclosure date with you.
3. We release a fix and publish a GitHub security advisory.
4. We credit you in the advisory, if you agree. You can stay anonymous.

## Scope

In scope:

- The code in this repository (`etl/`, `infrastructure/`, `.devcontainer/`).
- The GitHub Actions workflows in `.github/workflows/`.
- The container images that `infrastructure/containers/dockerfiles/` defines.

Out of scope:

- The default credentials in `.env.template` and `.devcontainer/.env.template`.
  They are for the local devcontainer only. Change them before you use healthpipe outside the devcontainer.
- Vulnerabilities in Dagster, SQL Server, Oracle, PostgreSQL or Aidbox. Report them to the vendor.
  Tell us if healthpipe uses an affected version.

## Security practices for contributors

- Do not commit credentials, API keys or other secrets.
- Do not commit real patient data or CPR numbers. Use synthetic test data only.
- Keep `.env` out of git. `.gitignore` already ignores it.
- The pre-commit hook and the `secret-scan` workflow run gitleaks with `.gitleaks.toml`.
  Do not bypass them with `--no-verify`.
- Give database and API accounts the minimum permissions that the pipeline needs.
