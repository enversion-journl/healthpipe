# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

import textwrap

import pytest
from data_lineage.tools.lineage_ddl_check.ddl_parser import DdlParseError, parse_ddl

pytestmark = pytest.mark.unit


def _write(tmp_path, rel, text):
    p = tmp_path / rel
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(textwrap.dedent(text), encoding="utf-8")
    return p


def test_parses_basic_table_ignoring_wrapper_constraint_index_and_go(tmp_path):
    _write(
        tmp_path,
        "erp/02_dims.sql",
        """
        IF NOT EXISTS (SELECT * FROM sys.objects
            WHERE object_id = OBJECT_ID(N'[dim].[Kontoplan]') AND type = 'U')
        BEGIN
            CREATE TABLE [dim].[Kontoplan](
                [dw_ek_Kontoplan] INT NOT NULL,
                [kontonummer] NVARCHAR(255) NULL,
                [Beloeb] DECIMAL(30, 12) NULL,
                CONSTRAINT [PK_Dim_Kontoplan] PRIMARY KEY CLUSTERED ([dw_ek_Kontoplan] ASC)
            ) ON [PRIMARY];
        END
        GO
        CREATE NONCLUSTERED INDEX [IX_x] ON [dim].[Kontoplan] ([kontonummer]) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert set(tables) == {"dim.Kontoplan"}
    assert tables["dim.Kontoplan"].columns == ("dw_ek_Kontoplan", "kontonummer", "Beloeb")


def test_strips_in_body_line_comments(tmp_path):
    _write(
        tmp_path,
        "fhir/01_tables.sql",
        """
        CREATE TABLE [fhir].[Patient] (
            [patient_id] NVARCHAR(255) NOT NULL,
            -- Identifiers (dk-core slices)
            [patient_ssn] NVARCHAR(10) NULL,
            [dw_timestamp_insert] DATETIME2 NULL
        ) ON [PRIMARY];
        GO
        """,
    )
    tables = parse_ddl(tmp_path, ("dim", "fct", "fhir"))
    assert tables["fhir.Patient"].columns == (
        "patient_id",
        "patient_ssn",
        "dw_timestamp_insert",
    )


def test_ignores_non_target_schema(tmp_path):
    _write(
        tmp_path,
        "meta/01.sql",
        "CREATE TABLE [meta].[Watermark] ([id] INT NOT NULL) ON [PRIMARY];\nGO\n",
    )
    assert parse_ddl(tmp_path, ("dim", "fct", "fhir")) == {}


def test_excludes_migrations_dir(tmp_path):
    _write(
        tmp_path,
        "migrations/20260101_x.sql",
        "CREATE TABLE [dim].[Foo] ([id] INT NOT NULL) ON [PRIMARY];\nGO\n",
    )
    assert parse_ddl(tmp_path, ("dim", "fct", "fhir")) == {}


def test_duplicate_table_raises(tmp_path):
    _write(tmp_path, "a.sql", "CREATE TABLE [dim].[Foo] ([id] INT NOT NULL);\nGO\n")
    _write(tmp_path, "b.sql", "CREATE TABLE [dim].[Foo] ([id] INT NOT NULL);\nGO\n")
    with pytest.raises(DdlParseError):
        parse_ddl(tmp_path, ("dim", "fct", "fhir"))


def test_unclassifiable_item_raises(tmp_path):
    _write(
        tmp_path,
        "a.sql",
        "CREATE TABLE [dim].[Foo] ([id] INT NOT NULL, GIBBERISH) ON [PRIMARY];\nGO\n",
    )
    with pytest.raises(DdlParseError):
        parse_ddl(tmp_path, ("dim", "fct", "fhir"))
