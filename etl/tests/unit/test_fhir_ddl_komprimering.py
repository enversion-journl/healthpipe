# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Kontrakt: de fem store fhir-tabeller er PAGE-komprimerede i DDL'en, tabel og indeks,
og QuestionnaireResponse_item har intet indeks på qr_id alene (PK-præfikset dækker)."""

from __future__ import annotations

import re

import pytest
from infrastructure.utils.ddl_loader import get_ddl_path

pytestmark = pytest.mark.unit

TABLES_DDL = get_ddl_path() / "tables" / "fhir" / "01_tables.sql"
KOMPRIMEREDE = (
    "QuestionnaireResponse",
    "QuestionnaireResponse_item",
    "Observation",
    "Procedure",
    "Condition",
)


@pytest.fixture(scope="module")
def ddl() -> str:
    return TABLES_DDL.read_text(encoding="utf-8")


def _table_body_end(ddl: str, table: str) -> int:
    """Position lige efter CREATE TABLE-blokkens afsluttende ')' for tabellen."""
    m = re.search(rf"CREATE TABLE \[fhir\]\.\[{re.escape(table)}\]\s*\(", ddl)
    assert m, table
    depth, i = 1, m.end()
    while depth:
        if ddl[i] == "(":
            depth += 1
        elif ddl[i] == ")":
            depth -= 1
        i += 1
    return i


def test_the_five_tables_are_page_compressed(ddl):
    for table in KOMPRIMEREDE:
        end = _table_body_end(ddl, table)
        tail = ddl[end : end + 80]
        assert re.match(r"\s*ON \[PRIMARY\] WITH \(DATA_COMPRESSION = PAGE\);", tail), (table, tail)


def test_every_index_on_the_five_tables_is_page_compressed(ddl):
    pattern = re.compile(
        r"CREATE (?:UNIQUE )?(?:NONCLUSTERED\s+)?INDEX \[(?P<name>[^\]]+)\]\s+ON \[fhir\]\.\[(?P<table>[^\]]+)\]"
        r"(?P<rest>.*?);",
        re.S,
    )
    seen = {t: 0 for t in KOMPRIMEREDE}
    for m in pattern.finditer(ddl):
        if m["table"] not in seen:
            continue
        seen[m["table"]] += 1
        assert "WITH (DATA_COMPRESSION = PAGE)" in m["rest"], (m["table"], m["name"])
    assert all(seen.values()), seen


def test_item_table_has_no_standalone_qr_id_index(ddl):
    assert "IX_FHIR_QRItem_QrId" not in ddl
    assert re.search(
        r"PRIMARY KEY CLUSTERED \(\[qr_id\], \[link_id\], \[item_key\], \[answer_index\]\)", ddl
    ), "PK-præfikset qr_id skal bære opslag pr. forælder"
