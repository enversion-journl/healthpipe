#!/usr/bin/env python3

# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Assert every script a pre-commit `local` hook runs is committed to git.

`.gitignore` ignores `scripts/*` (only `scripts/naming_overhaul/` is negated), so a
helper dropped there is silently untracked: it works for its author and fails for
everyone else — and for CI — with "No such file or directory". Hook entries are
also invisible to the normal lint hooks, so nothing else catches it.

Scans .pre-commit-config.yaml for `entry:` lines that invoke a repo-relative file
and reports any that is missing or untracked. Tracked CI/tooling helpers belong in
infrastructure/devops/scripts/.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
PRE_COMMIT = REPO_ROOT / ".pre-commit-config.yaml"

# entry: <interpreter> <path.py|path.sh> ...   (interpreter form only; bare
# executables like `gitleaks` resolve from PATH and are not repo files)
ENTRY = re.compile(
    r"^\s*entry:\s*(?:python3?|bash|sh)\s+(?P<path>[\w./-]+\.(?:py|sh))",
    re.MULTILINE,
)


def tracked(path: str) -> bool:
    return (
        subprocess.run(  # noqa: S603
            ["git", "ls-files", "--error-unmatch", path],
            cwd=REPO_ROOT,
            capture_output=True,
        ).returncode
        == 0
    )


def main() -> int:
    if not PRE_COMMIT.is_file():
        print(f"{PRE_COMMIT} not found", file=sys.stderr)
        return 1

    paths = sorted(set(ENTRY.findall(PRE_COMMIT.read_text(encoding="utf-8"))))
    if not paths:
        print("no file-based pre-commit hook entries found")
        return 0

    problems = []
    for path in paths:
        if not (REPO_ROOT / path).is_file():
            problems.append(f"{path}: referenced by a hook but does not exist")
        elif not tracked(path):
            problems.append(f"{path}: exists but is NOT tracked by git (check .gitignore)")

    if problems:
        print("pre-commit hook entries are not usable by others:", file=sys.stderr)
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        print(
            "\nMove the script somewhere tracked — infrastructure/devops/scripts/ "
            "holds the host-neutral CI helpers.",
            file=sys.stderr,
        )
        return 1

    print(f"{len(paths)} pre-commit hook script(s) tracked: {', '.join(paths)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
