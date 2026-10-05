# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DDL folder convention: no view DDL under tables/, no codesystem/conceptmap suffix folders."""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DDL = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"

FORBIDDEN_TABLE_DIRS = {"conceptmap_dso", "ref_codesystem"}

# Matches CREATE VIEW / CREATE OR ALTER VIEW with any whitespace (incl. newlines).
_CREATE_VIEW_RE = re.compile(r"CREATE\s+(?:OR\s+ALTER\s+)?VIEW\b", re.IGNORECASE)


def _strip_sql_comments(text: str) -> str:
    """Remove SQL line comments (--) and block comments (/* ... */) from text."""
    text = re.sub(r"--[^\n]*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return text


def test_no_suffix_schema_folders():
    table_dirs = {p.name for p in (DDL / "tables").iterdir() if p.is_dir()}
    leftover = table_dirs & FORBIDDEN_TABLE_DIRS
    assert not leftover, f"suffix folders must be removed: {leftover}"


def test_no_view_ddl_under_tables():
    offenders = []
    for sql in (DDL / "tables").rglob("*.sql"):
        text = sql.read_text(encoding="utf-8", errors="ignore")
        if _CREATE_VIEW_RE.search(_strip_sql_comments(text)):
            offenders.append(str(sql.relative_to(DDL)))
    assert not offenders, f"view DDL found under tables/: {offenders}"
