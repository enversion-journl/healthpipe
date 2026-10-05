# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the [meta].[MigrationHistory] runner-bootstrap pre-flight in
infrastructure.utils.ddl_loader.

Naming overhaul (PR 2, task B1): the tracker table itself is renamed from
[meta].[migration_history] to [meta].[MigrationHistory] by the generated
migration 00_meta file. Because the runner bootstraps its own tracker table
BEFORE any migration file executes, the bootstrap must pre-flight-rename an
already-deployed legacy table (and its PK) into the new name, then create the
new name if it is still missing (fresh databases). Every subsequent SELECT /
INSERT must address [meta].[MigrationHistory].

These tests capture executed SQL via a mocked cursor and assert on the exact
guard/rename literals rather than on live DB behavior.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from infrastructure.utils import ddl_loader
from infrastructure.utils.ddl_loader import baseline_migrations, execute_migrations

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Fixtures and helpers (mirrors test_ddl_loader_migrations.py)
# ---------------------------------------------------------------------------


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


def _make_migrations_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    migrations = tmp_path / "ddl" / "migrations"
    migrations.mkdir(parents=True)
    for name, content in files.items():
        (migrations / name).write_text(content)
    return tmp_path / "ddl"


def _patch_ddl_root(monkeypatch, ddl_root: Path) -> None:
    monkeypatch.setattr(ddl_loader, "get_ddl_path", lambda: ddl_root)


def _execute_call_strings(cursor_mock) -> list[str]:
    return [call.args[0] for call in cursor_mock.execute.call_args_list]


# ---------------------------------------------------------------------------
# Pre-flight rename batch (module-level SQL constant)
# ---------------------------------------------------------------------------


def test_bootstrap_sql_constant_contains_preflight_guard_before_create():
    """The bootstrap SQL constant itself: pre-flight rename guard must appear
    textually before the CREATE-if-missing block, and target MigrationHistory."""
    sql = ddl_loader._MIGRATION_HISTORY_BOOTSTRAP_SQL

    preflight_marker = "OBJECT_ID('meta.migration_history', 'U') IS NOT NULL"
    create_marker = "CREATE TABLE"

    assert preflight_marker in sql
    assert "OBJECT_ID('meta.MigrationHistory', 'U') IS NULL" in sql
    assert "EXEC sp_rename 'meta.migration_history', 'MigrationHistory'" in sql

    # Guarded PK rename to the new naming-overhaul convention.
    assert "PK_MigrationHistory" in sql
    assert "sp_rename" in sql

    assert sql.index(preflight_marker) < sql.index(create_marker)

    # The CREATE-if-missing now targets the new bracketed name.
    assert "[meta].[MigrationHistory]" in sql
    assert "[meta].[migration_history]" not in sql


def test_bootstrap_is_flat_guarded_statements_no_cursor():
    """Fail-closed: migrations must be flat guarded statements, never T-SQL
    CURSORs (execute_sql / the underlying driver silently drops cursor work)."""
    sql = ddl_loader._MIGRATION_HISTORY_BOOTSTRAP_SQL
    assert "CURSOR" not in sql.upper()


# ---------------------------------------------------------------------------
# execute_migrations: bootstrap behavior end-to-end via mocked cursor
# ---------------------------------------------------------------------------


def test_first_run_executes_preflight_then_create_then_select_new_name(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    """A single bootstrap batch is executed (pre-flight + create together),
    followed by a SELECT against the NEW table name."""
    ddl_root = _make_migrations_dir(tmp_path, {"001_first.sql": "PRINT 'first';"})
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    execute_migrations(mssql_resource, "migrations")

    sql_calls = _execute_call_strings(cursor_mock)
    # First call is the bootstrap batch: contains both the pre-flight rename
    # guard and the create-if-missing, in that order.
    bootstrap_sql = sql_calls[0]
    assert "sp_rename 'meta.migration_history', 'MigrationHistory'" in bootstrap_sql
    assert bootstrap_sql.index("sp_rename") < bootstrap_sql.index("CREATE TABLE")

    # Second call is the SELECT, addressed at the new bracketed name.
    assert sql_calls[1].strip().upper().startswith("SELECT FILENAME FROM")
    assert "[meta].[MigrationHistory]" in sql_calls[1]
    assert "[meta].[migration_history]" not in sql_calls[1]


def test_insert_targets_new_table_name(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    ddl_root = _make_migrations_dir(tmp_path, {"001_first.sql": "PRINT 'first';"})
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    execute_migrations(mssql_resource, "migrations")

    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 1
    assert "[meta].[MigrationHistory]" in insert_calls[0].args[0]
    assert "[meta].[migration_history]" not in insert_calls[0].args[0]


def test_bootstrap_runs_on_every_call_and_is_idempotent_guard_shape(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    """The pre-flight is safe to run every time: it is itself guarded by
    OBJECT_ID existence checks (idempotent), not a one-shot flag."""
    ddl_root = _make_migrations_dir(tmp_path, {"001_first.sql": "PRINT 'first';"})
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = [("001_first.sql",)]

    execute_migrations(mssql_resource, "migrations")

    sql_calls = _execute_call_strings(cursor_mock)
    bootstrap_sql = sql_calls[0]
    # Guarded shape: NOT EXISTS / IS NULL / IS NOT NULL checks gate every
    # mutating statement — nothing unconditional.
    assert "IF OBJECT_ID('meta.migration_history', 'U') IS NOT NULL" in bootstrap_sql
    assert "IF NOT EXISTS" in bootstrap_sql


# ---------------------------------------------------------------------------
# baseline_migrations: same bootstrap contract
# ---------------------------------------------------------------------------


def test_baseline_bootstrap_also_uses_new_table_name(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    ddl_root = _make_migrations_dir(tmp_path, {"001_first.sql": "PRINT 'first';"})
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    baseline_migrations(mssql_resource, "migrations")

    sql_calls = _execute_call_strings(cursor_mock)
    # sql_calls[0] is the meta-schema guard, sql_calls[1] is the bootstrap batch.
    bootstrap_sql = sql_calls[1]
    assert "sp_rename 'meta.migration_history', 'MigrationHistory'" in bootstrap_sql

    select_sql = sql_calls[2]
    assert "[meta].[MigrationHistory]" in select_sql

    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 1
    assert "[meta].[MigrationHistory]" in insert_calls[0].args[0]
