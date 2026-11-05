# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The DSO infrastructure skips every batch in the schemas that stay on staging only."""

import dagster as dg
import pytest
from infrastructure.assets.dso_infrastructure import DSO_EXCLUDED_SCHEMAS, dso_migrations
from infrastructure.utils import ddl_loader
from infrastructure.utils.ddl_loader import references_schema

pytestmark = pytest.mark.unit


class _RecordingCursor:
    rowcount = -1

    def __init__(self, executed):
        self._executed = executed

    def execute(self, sql, *params):
        self._executed.append(sql)

    def fetchone(self):
        # One user table and one tracker row: apply_migrations migrates instead of baselining.
        return (1,)

    def fetchall(self):
        return []

    def close(self):
        pass


class _RecordingConnection:
    def __init__(self):
        self.autocommit = False
        self.executed = []

    def cursor(self):
        return _RecordingCursor(self.executed)

    def commit(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class _RecordingResource:
    def __init__(self):
        self.connection = _RecordingConnection()

    def get_connection(self):
        return self.connection


@pytest.mark.parametrize(
    ("batch", "expected"),
    [
        ("CREATE TABLE [arc].[ils_lev] ([lnr] INT)", True),
        (
            "IF OBJECT_ID(N'map.Organisation', 'U') IS NULL CREATE TABLE map.Organisation (a INT)",
            True,
        ),
        ("EXEC('CREATE SCHEMA [stg]');", True),
        ("-- map.Borger was removed\nCREATE TABLE [dim].[Borger] ([dw_ek_Borger] INT)", False),
        ("/* source: arc.ils_rekv */\nCREATE TABLE [ref].[Rekvirent] ([rnr] INT)", False),
        ("CREATE TABLE [dim].[stg_map] ([map_id] INT, [ext_kode] INT)", False),
        ("SELECT sitemap.kode FROM [dim].[Kode] AS sitemap", False),
        (
            "SELECT N'DROP SCHEMA ' + QUOTENAME(s.name) FROM sys.schemas AS s "
            "WHERE s.name IN (N'ext', N'arc', N'map', N'stg')",
            False,
        ),
    ],
    ids=[
        "bracketed",
        "bare",
        "create-schema",
        "line-comment",
        "block-comment",
        "name-prefix",
        "name-suffix",
        "schema-name-as-string",
    ],
)
def test_references_schema_matches_only_qualified_names_outside_comments(batch, expected):
    assert references_schema(batch, DSO_EXCLUDED_SCHEMAS) is expected


def test_dso_migrations_skips_excluded_schema_batches_and_records_the_file(tmp_path, monkeypatch):
    excluded_batch = "CREATE TABLE [arc].[x] ([id] INT NOT NULL);"
    kept_batch = "ALTER TABLE [dim].[Borger] ADD [nyt_felt] INT NULL;"
    migrations = tmp_path / "migrations"
    migrations.mkdir()
    (migrations / "20991231_mixed.sql").write_text(
        f"{excluded_batch}\nGO\n{kept_batch}\nGO\n", encoding="utf-8"
    )
    monkeypatch.setattr(ddl_loader, "get_ddl_path", lambda: tmp_path)
    resource = _RecordingResource()

    with dg.instance_for_test() as instance:
        result = dg.materialize(
            [dso_migrations], resources={"mssql_target_dso": resource}, instance=instance
        )

    executed = resource.connection.executed
    assert kept_batch in executed
    assert excluded_batch not in executed
    [materialization] = result.asset_materializations_for_node("dso_migrations")
    assert materialization.metadata["applied"].value == ["20991231_mixed.sql"]
