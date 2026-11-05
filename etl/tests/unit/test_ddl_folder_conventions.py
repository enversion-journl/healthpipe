# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DDL folder conventions: no view DDL under tables/ or sources/, and one arc or
meta table per file under sources/.

A view defined under ``tables/`` or ``sources/`` is deployed with the tables in
the wrong order and silently never refreshed — nothing fails, the view just goes
stale. These are rules over the filesystem, so they also cover files added
tomorrow.
"""

import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

DDL = Path(__file__).resolve().parents[2] / "infrastructure" / "ddl"
TABLE_ROOTS = (DDL / "tables", DDL / "sources")

_CREATE_TABLE_RE = re.compile(r"CREATE\s+TABLE\s+\[(\w+)\]\.\[(\w+)\]", re.IGNORECASE)

# Matches CREATE VIEW / CREATE OR ALTER VIEW with any whitespace (incl. newlines).
_CREATE_VIEW_RE = re.compile(r"CREATE\s+(?:OR\s+ALTER\s+)?VIEW\b", re.IGNORECASE)


def _strip_sql_comments(text: str) -> str:
    """Remove SQL line comments (--) and block comments (/* ... */) from text."""
    text = re.sub(r"--[^\n]*", "", text)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.DOTALL)
    return text


def _sql_files(root: Path) -> list[Path]:
    scanned = list(root.rglob("*.sql"))
    # Vacuous-pass guard: if the folder is moved or renamed, rglob returns
    # nothing and the checks below would pass green forever.
    assert scanned, f"no .sql files found under {root} — DDL layout changed"
    return scanned


def test_no_view_ddl_under_tables():
    offenders = [
        str(sql.relative_to(DDL))
        for root in TABLE_ROOTS
        for sql in _sql_files(root)
        if _CREATE_VIEW_RE.search(
            _strip_sql_comments(sql.read_text(encoding="utf-8", errors="ignore"))
        )
    ]
    assert not offenders, f"view DDL found under tables/ or sources/: {offenders}"


def test_sources_hold_one_arc_or_meta_table_per_file_named_after_the_table():
    offenders = []
    for sql in _sql_files(DDL / "sources"):
        created = _CREATE_TABLE_RE.findall(_strip_sql_comments(sql.read_text(encoding="utf-8")))
        if len(created) != 1 or created[0][0] not in ("arc", "meta") or created[0][1] != sql.stem:
            offenders.append((str(sql.relative_to(DDL)), created))
    assert (
        not offenders
    ), f"sources/ files must create exactly the table they are named after: {offenders}"
