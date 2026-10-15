# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Guard: the udredningsret arc DDL must match the dataset specification.

DDL (infrastructure/ddl/tables/udredningsret/00_archive.sql) and
DSI/csv_files/udredningsret_datasets.py are kept in lockstep: columns (name +
order), primary key, NOT NULL on the computed keys, and the ext table having
the same widths as arc (HAIBA lesson [internal ticket]).
"""

import re
from pathlib import Path

import pytest
from DSI.csv_files import udredningsret
from DSI.csv_files.udredningsret_datasets import (
    ARCHIVED_DATASETS,
    CONTENT_EXCLUDED,
    LOEBENUMMER_HASH_EXCLUDED,
    NOEGLE_WIDTH,
    SURROGATE_ID_COLUMNS,
    UDREDNINGSRET_MONT,
    VALUE_WIDTH,
)

pytestmark = pytest.mark.unit

DDL_DIR = (
    Path(__file__).resolve().parents[2] / "infrastructure" / "ddl" / "tables" / "udredningsret"
)
TAIL_COLUMNS = ("leverance_dato", "dw_is_deleted", "dw_timestamp_insert", "dw_timestamp_update")


def _parse_tables(sql: str) -> dict[str, dict]:
    tables: dict[str, dict] = {}
    pattern = re.compile(
        r"CREATE TABLE \[(?P<schema>\w+)\]\.\[(?P<name>\w+)\] \((?P<body>.*?)\n    \);", re.DOTALL
    )
    for m in pattern.finditer(sql):
        columns: list[str] = []
        not_null: set[str] = set()
        types: dict[str, str] = {}
        for line in m.group("body").splitlines():
            col = re.match(
                r"\[(\w+)\]\s+(NVARCHAR\(\d+\)|CHAR\(\d+\)|DATE|BIT|INT|BIGINT|DECIMAL\([\d,]+\)|DATETIME2\(0\))\s+(NOT NULL|NULL)",
                line.strip(),
            )
            if col:
                columns.append(col.group(1))
                types[col.group(1)] = col.group(2)
                if col.group(3) == "NOT NULL":
                    not_null.add(col.group(1))
        pk_match = re.search(r"PRIMARY KEY CLUSTERED \(\s*(.*?)\s*\)", m.group("body"), re.DOTALL)
        pk = re.findall(r"\[(\w+)\]", pk_match.group(1)) if pk_match else []
        tables[f"{m.group('schema')}.{m.group('name')}"] = {
            "columns": columns,
            "pk": pk,
            "not_null": not_null,
            "types": types,
        }
    return tables


@pytest.fixture(scope="module")
def ddl_sql() -> str:
    return "\n".join(p.read_text(encoding="utf-8") for p in sorted(DDL_DIR.glob("*.sql")))


@pytest.fixture(scope="module")
def ddl_tables(ddl_sql) -> dict[str, dict]:
    return _parse_tables(ddl_sql)


def test_kolonner_matcher_spec(ddl_tables):
    for d in ARCHIVED_DATASETS:
        expected = [*d.arc_columns, *TAIL_COLUMNS]
        actual = ddl_tables[f"arc.{d.ext_table}"]["columns"]
        assert actual == expected, (
            f"arc.{d.ext_table}: the DDL columns do not match udredningsret_datasets.py.\n"
            f"  DDL:  {actual}\n  Spec: {expected}"
        )


def test_primaernoegle_matcher_merge_keys(ddl_tables):
    for d in ARCHIVED_DATASETS:
        assert ddl_tables[f"arc.{d.ext_table}"]["pk"] == list(d.merge_keys)


def test_beregnede_noegler_er_not_null_og_typede(ddl_tables):
    table = ddl_tables[f"arc.{UDREDNINGSRET_MONT.ext_table}"]
    for added in UDREDNINGSRET_MONT.arc_added:
        assert added.name in table["not_null"], f"{added.name} must be NOT NULL"
        assert table["types"][added.name] == added.sql_type
    # SHA2_256 as hex is exactly 64 characters — the width is an external fact, not the spec's
    assert table["types"]["markoer_noegle"] == f"CHAR({NOEGLE_WIDTH})" == "CHAR(64)"


def test_kildekolonner_har_samme_bredde_i_ext_og_arc(ddl_tables):
    """arc and ext must share the width so the merge never truncates."""
    table = ddl_tables[f"arc.{UDREDNINGSRET_MONT.ext_table}"]
    ext_sql = udredningsret._ext_create_sql(UDREDNINGSRET_MONT)
    for column in UDREDNINGSRET_MONT.source_columns:
        assert table["types"][column] == f"NVARCHAR({VALUE_WIDTH})", column
        assert f"[{column}] NVARCHAR({VALUE_WIDTH}) NULL" in ext_sql, column


def test_ingen_clean_code_og_ingen_domaenepraefiks_paa_arc_pk(ddl_sql):
    """The arc layer uses *_noegle (as DRG) and PK_arc_<table> (127 arc PKs in the repo)."""
    assert "[clean_code" not in ddl_sql  # no clean_code_* columns; comments may mention it
    assert "CONSTRAINT [PK_arc_udredningsret_mont]" in ddl_sql
    assert "PK_udredningsret_arc" not in ddl_sql


def test_defaults_er_navngivne(ddl_sql):
    """An unnamed DEFAULT cannot be dropped in a later migration."""
    assert "NOT NULL DEFAULT (0)" not in ddl_sql
    assert "NOT NULL DEFAULT SYSDATETIME()" not in ddl_sql
    assert "NOT NULL DEFAULT (CAST(SYSDATETIME()" not in ddl_sql
    assert "CONSTRAINT [DF_udredningsret_mont_dw_is_deleted]" in ddl_sql


def test_meta_tabel_har_kolonnerne_record_delivery_skriver(ddl_tables):
    meta = ddl_tables["meta.UdredningsretLeverance"]
    # the ON clause of record_delivery's MERGE — another PK would duplicate deliveries
    assert meta["pk"] == ["kildetabel", "leverance_tidsstempel"]
    # everything record_delivery writes and the direction guard reads
    for column in (
        "kadence",
        "leverance_dato",
        "lpropdateringsdato",
        "indlaesningstype",
        "raekker_ext",
        "raekker_kilde",
        "raekker_indsat",
        "raekker_opdateret",
        "raekker_soft_slettet",
        "helbredsforloeb_genfindelse",
    ):
        assert column in meta["columns"], column


def test_spec_har_ingen_dubletkolonner():
    assert len(set(UDREDNINGSRET_MONT.source_columns)) == len(UDREDNINGSRET_MONT.source_columns)
    assert len(UDREDNINGSRET_MONT.source_columns) == 28  # SDS' Metadata_UdredningsretMont


def test_loebenummer_hash_udelader_alle_ider_der_omnummereres():
    """Review F13: the rank must exclude every id the content hash excludes AND every id
    change-detect ignores as renumbering — otherwise the rank flips with the renumbering."""
    excluded = set(LOEBENUMMER_HASH_EXCLUDED)
    assert set(CONTENT_EXCLUDED) <= excluded
    assert set(SURROGATE_ID_COLUMNS) <= excluded
    assert "DW_EK_HelbredsForloeb" in excluded  # SDS' surrogate, renumbered in 2021
    assert excluded < set(UDREDNINGSRET_MONT.source_columns)  # and it excludes far from everything
    for column in excluded:
        assert column not in UDREDNINGSRET_MONT.merge_keys


def test_manifest_er_ikke_i_ddl(ddl_sql):
    """ext.udredningsret_leverance_manifest is volatile and created by the extract per run."""
    assert "udredningsret_leverance_manifest" not in ddl_sql
