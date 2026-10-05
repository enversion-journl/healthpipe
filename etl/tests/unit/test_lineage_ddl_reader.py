# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import textwrap

import pytest
from data_lineage.tools.lineage_ddl_check.lineage_reader import read_lineage

pytestmark = pytest.mark.unit


def _write(tmp_path, schema, filename, text):
    d = tmp_path / schema
    d.mkdir(parents=True, exist_ok=True)
    (d / filename).write_text(textwrap.dedent(text), encoding="utf-8")


def test_reads_table_and_columns(tmp_path):
    _write(
        tmp_path,
        "dim",
        "dim_EAN.yml",
        """
        dso_lineage:
          Type: dimension
          TableName: dim.Ean
          Columns:
            dw_ek_EAN: { Type: PK, DataType: INT }
            ean: { DataType: NVARCHAR(50) }
        """,
    )
    tables, malformed = read_lineage(tmp_path, ("dim", "fct", "fhir"))
    assert malformed == []
    assert tables["dim.Ean"].columns == ("dw_ek_EAN", "ean")
    assert tables["dim.Ean"].source_file == "dim/dim_EAN.yml"


def test_missing_columns_is_malformed(tmp_path):
    _write(tmp_path, "dim", "x.yml", "dso_lineage:\n  TableName: dim.X\n")
    tables, malformed = read_lineage(tmp_path, ("dim",))
    assert tables == {}
    assert any("Columns" in m for m in malformed)


def test_missing_tablename_is_malformed(tmp_path):
    _write(tmp_path, "dim", "x.yml", "dso_lineage:\n  Columns:\n    A: {}\n")
    tables, malformed = read_lineage(tmp_path, ("dim",))
    assert tables == {}
    assert any("TableName" in m for m in malformed)


def test_missing_dso_lineage_is_malformed(tmp_path):
    _write(tmp_path, "dim", "x.yml", "something_else:\n  foo: bar\n")
    tables, malformed = read_lineage(tmp_path, ("dim",))
    assert tables == {}
    assert any("dso_lineage" in m for m in malformed)


def test_reads_source_systems(tmp_path):
    _write(
        tmp_path,
        "dim",
        "dim_kreditor.yml",
        """
        dso_lineage:
          Type: dimension
          TableName: dim.Kreditor
          SourceSystems:
            - sydax: [Kreditor.Kreditor]
            - ils: [dbo.lev, dbo.vare]
          Columns:
            dw_ek_Kreditor: { Type: PK, DataType: INT }
        """,
    )
    tables, malformed = read_lineage(tmp_path, ("dim",))
    assert malformed == []
    assert tables["dim.Kreditor"].source_systems == {
        "sydax": ("Kreditor.Kreditor",),
        "ils": ("dbo.lev", "dbo.vare"),
    }


def test_source_systems_wrong_shape_is_malformed(tmp_path):
    _write(
        tmp_path,
        "dim",
        "x.yml",
        """
        dso_lineage:
          TableName: dim.X
          SourceSystems:
            - sydax: Kreditor.Kreditor
          Columns:
            a: {}
        """,
    )
    tables, malformed = read_lineage(tmp_path, ("dim",))
    assert tables == {}
    assert any("SourceSystems" in m for m in malformed)
