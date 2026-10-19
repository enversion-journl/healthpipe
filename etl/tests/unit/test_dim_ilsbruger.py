# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the dim.ILSBruger chain.

The chain's contract is its column set: staging, the dim MERGE, the DDL and the
lineage YAML must name the same columns.
"""

import re
import sys
from pathlib import Path

import DSA.SQL.dim.dim_ILSBruger  # noqa: F401
import DSA.SQL.staging.stg_dim_ILSBruger  # noqa: F401
import pytest
import yaml

pytestmark = [pytest.mark.unit, pytest.mark.erp]

_stg = sys.modules["DSA.SQL.staging.stg_dim_ILSBruger"]
_dim = sys.modules["DSA.SQL.dim.dim_ILSBruger"]

ETL = Path(__file__).resolve().parents[2]
LINEAGE = ETL / "data_lineage" / "dim" / "dim_ILSBruger.yml"
DIMS_DDL = ETL / "infrastructure" / "ddl" / "tables" / "erp" / "02_dims.sql"


def _ddl_columns() -> list[str]:
    ddl = DIMS_DDL.read_text(encoding="utf-8")
    body = re.search(r"CREATE TABLE \[dim\]\.\[ILSBruger\] \((.*?)\n\) ON", ddl, re.S)
    assert body, "CREATE TABLE [dim].[ILSBruger] not found"
    return re.findall(r"^\s+\[(\w+)\]", body.group(1), re.M)


def _staged_columns() -> list[str]:
    select = _stg.BUILD_SQL.split("INTO [stg].[dim_ILSBruger]")[0].rsplit("SELECT", 1)[1]
    return [
        re.search(r"(\w+)\s*$", line.strip().rstrip(",")).group(1)
        for line in select.strip().splitlines()
    ]


class TestColumnContract:
    def test_staging_matches_dim_attributes(self):
        assert _staged_columns() == list(_dim.ATTRIBUTE_COLUMNS)

    def test_ddl_holds_key_attributes_and_audit(self):
        expected = [
            "dw_ek_ILSBruger",
            *_dim.ATTRIBUTE_COLUMNS,
            "dw_timestamp_insert",
            "dw_timestamp_update",
        ]
        assert _ddl_columns() == expected

    def test_lineage_documents_every_non_audit_column(self):
        cols = yaml.safe_load(LINEAGE.read_text(encoding="utf-8"))["dso_lineage"]["Columns"]
        assert list(cols) == ["dw_ek_ILSBruger", *_dim.ATTRIBUTE_COLUMNS]


def test_staging_reads_the_archive_not_ext():
    assert "[ext]." not in _stg.BUILD_SQL
