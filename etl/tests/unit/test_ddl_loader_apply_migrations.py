# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""A database provisioned from scratch must be baselined, not migrated.

``tables/**.sql`` already describes the schema at migration head, so on a fresh
database the historical migrations are at best redundant and at worst fatal:
26 of the 184 currently patch ``dim``/``fct``/``fhir``/``meta`` objects that do
not exist yet, and fail with "Invalid object name". ``baseline_migrations()``
exists for exactly this — it stamps every file as applied without running it —
but nothing called it, so a from-scratch provision ran them all and broke.

Detection has to be conservative in one direction only. Baselining a database
that is genuinely *behind* head silently skips the migrations it still needs, so
"fresh" means provably empty: no user tables beyond the tracker itself, and no
rows in the tracker. A pre-tracker database that already holds tables must
migrate normally, even though its tracker is empty.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from infrastructure.utils import ddl_loader
from infrastructure.utils.ddl_loader import apply_migrations

pytestmark = pytest.mark.unit

MIGRATION_BODY = "ALTER TABLE [dim].[Borger] ADD [nyt_felt] INT NULL;"


@pytest.fixture
def cursor_mock():
    cursor = MagicMock()
    cursor.rowcount = 0
    cursor.fetchall.return_value = []
    # execute_sql drains a batch with `while cursor.nextset(): pass`. An
    # unconfigured MagicMock returns a truthy mock from every call, which no
    # real driver does and which would spin that loop forever.
    cursor.nextset.return_value = False
    return cursor


@pytest.fixture
def mssql_resource(cursor_mock):
    conn = MagicMock()
    conn.cursor.return_value = cursor_mock
    resource = MagicMock()
    resource.get_connection.return_value.__enter__.return_value = conn
    resource.get_connection.return_value.__exit__.return_value = None
    return resource


@pytest.fixture
def migrations_dir(tmp_path: Path, monkeypatch) -> Path:
    migrations = tmp_path / "ddl" / "migrations"
    migrations.mkdir(parents=True)
    (migrations / "20260101_add_column.sql").write_text(MIGRATION_BODY)
    monkeypatch.setattr(ddl_loader, "get_ddl_path", lambda: tmp_path / "ddl")
    return migrations


def _shape(cursor_mock, user_tables: int, applied_rows: int) -> None:
    """Make the database look like it holds `user_tables` and `applied_rows`."""
    cursor_mock.fetchone.side_effect = [(user_tables,), (applied_rows,)]


def _migration_sql_was_executed(cursor_mock) -> bool:
    return any(
        MIGRATION_BODY in str(call.args[0])
        for call in cursor_mock.execute.call_args_list
        if call.args
    )


def _was_stamped(cursor_mock) -> bool:
    return any(
        "INSERT INTO [meta].[MigrationHistory]" in str(call.args[0])
        for call in cursor_mock.execute.call_args_list
        if call.args
    )


def test_empty_database_is_baselined_without_running_the_sql(
    mssql_resource, cursor_mock, migrations_dir
):
    _shape(cursor_mock, user_tables=0, applied_rows=0)

    result = apply_migrations(mssql_resource, "migrations")

    assert not _migration_sql_was_executed(cursor_mock), (
        "a from-scratch database must not execute historical migrations — "
        "they patch objects the base DDL has not created yet"
    )
    assert _was_stamped(cursor_mock), "the migrations must still be recorded as applied"
    assert result == {"20260101_add_column.sql": "baselined"}


def test_database_with_tables_but_no_tracker_still_migrates(
    mssql_resource, cursor_mock, migrations_dir
):
    # A pre-tracker deployment: real schema, empty MigrationHistory. Baselining
    # this would permanently skip every migration it has not had yet.
    _shape(cursor_mock, user_tables=42, applied_rows=0)

    apply_migrations(mssql_resource, "migrations")

    assert _migration_sql_was_executed(
        cursor_mock
    ), "an existing database must run its pending migrations, even though its tracker is empty"


def test_database_with_applied_migrations_still_migrates(
    mssql_resource, cursor_mock, migrations_dir
):
    _shape(cursor_mock, user_tables=42, applied_rows=180)

    apply_migrations(mssql_resource, "migrations")

    assert _migration_sql_was_executed(cursor_mock)


def test_tracker_rows_without_tables_is_not_treated_as_fresh(
    mssql_resource, cursor_mock, migrations_dir
):
    # Odd state (tables dropped, tracker kept). Refuse to baseline: the safe
    # direction is to migrate.
    _shape(cursor_mock, user_tables=0, applied_rows=5)

    apply_migrations(mssql_resource, "migrations")

    assert _migration_sql_was_executed(cursor_mock)


def test_fresh_database_is_baselined_in_full_even_when_only_is_given(
    migrations_dir, mssql_resource, cursor_mock
):
    (migrations_dir / "20260102_own.sql").write_text("PRINT 'own';")
    _shape(cursor_mock, user_tables=0, applied_rows=0)

    result = apply_migrations(mssql_resource, only=["20260102_own.sql"])

    assert set(result) == {"20260101_add_column.sql", "20260102_own.sql"}
    assert not _migration_sql_was_executed(cursor_mock)
