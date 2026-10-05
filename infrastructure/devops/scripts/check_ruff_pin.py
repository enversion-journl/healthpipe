#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Assert the ruff version is pinned identically everywhere it is declared.

pyproject.toml pinned `ruff>=0.6.0` while .pre-commit-config.yaml pinned rev
v0.6.9, so `pip install -e ".[dev]"` and `pre-commit` ran different ruff versions
in the same repo — CI and local could disagree about the same file.

Checks:
  1. pyproject.toml declares an exact `ruff==X`
  2. .pre-commit-config.yaml pins ruff-pre-commit at `rev: vX`
  3. the installed `ruff --version` is X (skipped when ruff is not installed,
     e.g. a pre-commit run in an environment without the dev extras)

Exits non-zero with a message naming the mismatch.
"""

from __future__ import annotations

import re
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PYPROJECT = REPO_ROOT / "pyproject.toml"
PRE_COMMIT = REPO_ROOT / ".pre-commit-config.yaml"
RUFF_PRE_COMMIT_REPO = "https://github.com/astral-sh/ruff-pre-commit"


def pinned_in_pyproject() -> str:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    specs = data.get("project", {}).get("optional-dependencies", {}).get("dev", [])
    for spec in specs:
        # Split the distribution name off any version specifier / extra / marker.
        name = re.split(r"[\[=<>!~;\s]", spec, maxsplit=1)[0].strip()
        if name != "ruff":
            continue
        match = re.fullmatch(r"ruff==([0-9][^\s;]*)", spec.strip())
        if not match:
            raise SystemExit(
                f"pyproject.toml: ruff must be pinned exactly (ruff==X.Y.Z), found {spec!r}"
            )
        return match.group(1)
    raise SystemExit("pyproject.toml: no ruff entry in [project.optional-dependencies].dev")


def pinned_in_pre_commit() -> str:
    """Read the rev of the ruff-pre-commit repo without importing yaml.

    Keeps this runnable as a `language: system` hook in an environment that has
    no third-party packages installed.
    """
    text = PRE_COMMIT.read_text(encoding="utf-8")
    pattern = re.compile(
        rf"-\s*repo:\s*{re.escape(RUFF_PRE_COMMIT_REPO)}\s*\n\s*rev:\s*v?([0-9][^\s]*)",
        re.MULTILINE,
    )
    match = pattern.search(text)
    if not match:
        raise SystemExit(f".pre-commit-config.yaml: no rev found for {RUFF_PRE_COMMIT_REPO}")
    return match.group(1)


def installed_version() -> str | None:
    if shutil.which("ruff") is None:
        return None
    out = subprocess.run(  # noqa: S603
        ["ruff", "--version"], capture_output=True, text=True, check=True
    ).stdout
    match = re.search(r"([0-9]+\.[0-9]+\.[0-9]+)", out)
    return match.group(1) if match else None


def main() -> int:
    expected = pinned_in_pyproject()
    hook = pinned_in_pre_commit()

    problems = []
    remedies = []
    if hook != expected:
        problems.append(
            f".pre-commit-config.yaml pins ruff-pre-commit v{hook}, "
            f"pyproject.toml pins ruff=={expected}"
        )
        remedies.append(
            f"set the ruff-pre-commit rev to v{expected} in .pre-commit-config.yaml "
            "(or change the pyproject pin to match)"
        )

    installed = installed_version()
    if installed is not None and installed != expected:
        problems.append(f"installed ruff is {installed}, pyproject.toml pins ruff=={expected}")
        # Stale environment, not a repo defect: the usual cause is an existing
        # devcontainer, whose deps are installed once by postCreateCommand.
        remedies.append('refresh your environment:  pip install -e ".[dev]"')

    if problems:
        print("ruff version pins disagree:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print("\nTo fix:", file=sys.stderr)
        for remedy in remedies:
            print(f"  - {remedy}", file=sys.stderr)
        return 1

    where = f"pyproject + pre-commit{'' if installed is None else ' + installed'}"
    print(f"ruff pinned consistently at {expected} ({where})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
