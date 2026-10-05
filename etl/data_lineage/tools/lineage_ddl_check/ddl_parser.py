# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path


class DdlParseError(Exception):
    """Raised when a CREATE TABLE block cannot be parsed unambiguously."""


@dataclass(frozen=True)
class DdlTable:
    name: str  # "schema.Table"
    columns: tuple[str, ...]
    source_file: str


_CREATE_RE = re.compile(
    r"CREATE\s+TABLE\s+\[?(?P<schema>\w+)\]?\.\[?(?P<table>\w+)\]?\s*\(",
    re.IGNORECASE,
)

# Non-bracketed leading tokens that mark a table-level constraint/index, not a column.
_CONSTRAINT_KEYWORDS = {
    "CONSTRAINT",
    "PRIMARY",
    "UNIQUE",
    "FOREIGN",
    "CHECK",
    "INDEX",
    "KEY",
}


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    sql = re.sub(r"--[^\n]*", "", sql)
    return sql


def _split_top_level(body: str) -> list[str]:
    """Split a CREATE TABLE body on commas that are not nested in parentheses.

    Single-quoted T-SQL string literals (``'...'``, with a doubled ``''`` quote
    as the escape) are treated as opaque: parentheses and commas inside a literal
    are ignored. Without this a ``DEFAULT 'a,b'`` would be wrongly split.
    """
    items: list[str] = []
    depth = 0
    start = 0
    in_string = False
    i = 0
    n = len(body)
    while i < n:
        ch = body[i]
        if in_string:
            if ch == "'":
                if i + 1 < n and body[i + 1] == "'":
                    i += 2  # escaped quote ('') -> stay in the string
                    continue
                in_string = False
            i += 1
            continue
        if ch == "'":
            in_string = True
        elif ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            items.append(body[start:i])
            start = i + 1
        i += 1
    items.append(body[start:])
    return items


def _extract_columns(body: str, table: str, source_file: str) -> list[str]:
    columns: list[str] = []
    for raw in _split_top_level(body):
        item = raw.strip()
        if not item:
            continue
        if item.startswith("["):
            match = re.match(r"\[(?P<name>[^\]]+)\]", item)
            if match is None:  # pragma: no cover - defensive
                raise DdlParseError(f"{source_file}: {table}: malformed bracketed item: {item!r}")
            columns.append(match.group("name"))
            continue
        first = item.split(None, 1)[0].upper()
        if first in _CONSTRAINT_KEYWORDS:
            continue
        raise DdlParseError(f"{source_file}: {table}: cannot classify definition item: {item!r}")
    return columns


def _iter_tables(sql: str, source_file: str, schemas: tuple[str, ...]) -> Iterator[DdlTable]:
    for match in _CREATE_RE.finditer(sql):
        schema = match.group("schema")
        table = match.group("table")
        if schema.lower() not in schemas:
            continue
        open_idx = match.end() - 1  # index of the '(' that opens the body
        depth = 0
        end_idx: int | None = None
        in_string = False
        i = open_idx
        n = len(sql)
        while i < n:
            ch = sql[i]
            if in_string:
                # Single-quoted T-SQL literal: ignore parens inside it; a
                # doubled '' is an escaped quote, not the end of the string.
                if ch == "'":
                    if i + 1 < n and sql[i + 1] == "'":
                        i += 2
                        continue
                    in_string = False
                i += 1
                continue
            if ch == "'":
                in_string = True
            elif ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    end_idx = i
                    break
            i += 1
        if end_idx is None:
            raise DdlParseError(f"{source_file}: unterminated CREATE TABLE for {schema}.{table}")
        body = sql[open_idx + 1 : end_idx]
        name = f"{schema}.{table}"
        columns = _extract_columns(body, name, source_file)
        yield DdlTable(name=name, columns=tuple(columns), source_file=source_file)


def parse_ddl(tables_dir: Path, schemas: tuple[str, ...]) -> dict[str, DdlTable]:
    """Parse all base-DDL CREATE TABLE blocks under *tables_dir* for *schemas*.

    Files anywhere under a ``migrations/`` directory are excluded — the base DDL
    end-state is the single source of truth.
    """
    wanted = tuple(s.lower() for s in schemas)
    tables: dict[str, DdlTable] = {}
    for path in sorted(tables_dir.rglob("*.sql")):
        if "migrations" in path.parts:
            continue
        sql = _strip_comments(path.read_text(encoding="utf-8"))
        rel = str(path.relative_to(tables_dir))
        for parsed in _iter_tables(sql, rel, wanted):
            if parsed.name in tables:
                raise DdlParseError(
                    f"duplicate CREATE TABLE for {parsed.name} in {rel} "
                    f"(already defined in {tables[parsed.name].source_file})"
                )
            tables[parsed.name] = parsed
    return tables
