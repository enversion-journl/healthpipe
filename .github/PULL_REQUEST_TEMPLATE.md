<!--
  Thanks for contributing to Healthpipe! This mirrors the
  "Pull Request Requirements" in CONTRIBUTING.md.
-->

## Description

<!-- What does this PR change, and why? -->

## Related issues

<!-- Use a closing keyword so the issue auto-closes on merge, e.g. "Closes #123". Leave blank if not applicable. -->

## Type of change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (alters existing behaviour or output)
- [ ] Data pipeline / ETL change (new or modified source, dim/fact, or DSO sync)
- [ ] Documentation
- [ ] Refactor / tech debt (no functional change)

## How has this been tested?

- [ ] `cd etl && pytest`
- [ ] Other (describe below)

## Checklist

- [ ] **Scope** — this PR is focused on one logical change
- [ ] **Tests** — new functionality has tests; existing tests still pass
- [ ] **Formatting** — code is formatted with Black (line-length=100) and passes Ruff
- [ ] **Imports** — absolute imports only (`core.*`, `DSA.*`, `DSI.*`, `DSO.*`, `utils.*`); no relative imports
- [ ] **Documentation** — updated relevant guides in `etl/docs/` for any architecture or flow changes
- [ ] **DCO sign-off** — every commit is signed off with `git commit -s` (see `CONTRIBUTING.md`)
