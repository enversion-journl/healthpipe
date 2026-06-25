# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for execute_migrations in infrastructure.utils.ddl_loader.

Verifies the migration tracking behavior:
- Bootstrap of meta.migration_history on first run.
- SELECT of already-applied filenames; matching files are skipped.
- Pending files are executed in sorted order.
- Each successful migration produces an INSERT in meta.migration_history.
- Fail-fast: an exception during a migration stops later migrations and
  leaves the tracker without an entry for the failing file.
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from infrastructure.utils import ddl_loader
from infrastructure.utils.ddl_loader import baseline_migrations, execute_migrations

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def cursor_mock():
    """A cursor mock with safe defaults for execute_sql arithmetic."""
    cursor = MagicMock()
    cursor.rowcount = 0
    cursor.fetchall.return_value = []
    return cursor


@pytest.fixture
def mssql_resource(cursor_mock):
    """An MSSQLResource mock whose get_connection() yields a connection
    that always returns `cursor_mock` from .cursor()."""
    conn = MagicMock()
    conn.cursor.return_value = cursor_mock
    resource = MagicMock()
    resource.get_connection.return_value.__enter__.return_value = conn
    resource.get_connection.return_value.__exit__.return_value = None
    return resource


def _make_migrations_dir(tmp_path: Path, files: dict[str, str]) -> Path:
    """Create tmp_path/ddl/migrations with the given filename -> content map."""
    migrations = tmp_path / "ddl" / "migrations"
    migrations.mkdir(parents=True)
    for name, content in files.items():
        (migrations / name).write_text(content)
    return tmp_path / "ddl"


def _patch_ddl_root(monkeypatch, ddl_root: Path) -> None:
    monkeypatch.setattr(ddl_loader, "get_ddl_path", lambda: ddl_root)


def _execute_call_strings(cursor_mock) -> list[str]:
    """Return the first positional argument (SQL text) of each cursor.execute call."""
    return [call.args[0] for call in cursor_mock.execute.call_args_list]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_first_run_bootstraps_table_and_applies_all(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    """Empty tracker → bootstrap CREATE runs, every file is applied and inserted."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []  # tracker empty

    result = execute_migrations(mssql_resource, "migrations")

    assert result == {"001_first.sql": "applied", "002_second.sql": "applied"}

    sql_calls = _execute_call_strings(cursor_mock)
    # First two cursor.execute calls = bootstrap + SELECT
    assert "migration_history" in sql_calls[0]
    assert "CREATE TABLE" in sql_calls[0].upper()
    assert sql_calls[1].strip().upper().startswith("SELECT FILENAME FROM")

    # Each migration's SQL was executed
    assert "PRINT 'first';" in sql_calls
    assert "PRINT 'second';" in sql_calls

    # INSERTs happened, one per file, with the filename as a parameter
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 2
    inserted_names = sorted(call.args[1][0] for call in insert_calls)
    assert inserted_names == ["001_first.sql", "002_second.sql"]


def test_subsequent_run_skips_applied_files(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """Tracker has 2 of 3 filenames → only the unapplied file runs."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
            "003_third.sql": "PRINT 'third';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = [("001_first.sql",), ("003_third.sql",)]

    result = execute_migrations(mssql_resource, "migrations")

    assert result == {
        "001_first.sql": "skipped",
        "002_second.sql": "applied",
        "003_third.sql": "skipped",
    }

    sql_calls = _execute_call_strings(cursor_mock)
    # Skipped files' SQL was never executed
    assert "PRINT 'first';" not in sql_calls
    assert "PRINT 'third';" not in sql_calls
    # The pending file ran
    assert "PRINT 'second';" in sql_calls
    # Exactly one INSERT, for 002
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1] == ("002_second.sql",)


def test_all_applied_no_op(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """Tracker already has every filename → no migration SQL runs, no INSERT."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = [("001_first.sql",), ("002_second.sql",)]

    result = execute_migrations(mssql_resource, "migrations")

    assert result == {
        "001_first.sql": "skipped",
        "002_second.sql": "skipped",
    }

    sql_calls = _execute_call_strings(cursor_mock)
    assert "PRINT 'first';" not in sql_calls
    assert "PRINT 'second';" not in sql_calls
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert insert_calls == []


def test_failure_mid_sequence_stops_and_does_not_record_failed(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    """File 2 raises → file 1 is recorded, file 2 is not, file 3 never runs."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
            "003_third.sql": "PRINT 'third';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    boom = RuntimeError("simulated migration failure")

    def execute_side_effect(sql, *args, **kwargs):
        if sql == "PRINT 'second';":
            raise boom
        return None

    cursor_mock.execute.side_effect = execute_side_effect

    with pytest.raises(RuntimeError, match="simulated migration failure"):
        execute_migrations(mssql_resource, "migrations")

    sql_calls = _execute_call_strings(cursor_mock)
    # File 1 ran
    assert "PRINT 'first';" in sql_calls
    # File 2 attempted (raised)
    assert "PRINT 'second';" in sql_calls
    # File 3 never attempted
    assert "PRINT 'third';" not in sql_calls

    # Exactly one INSERT, for file 1; file 2 was not recorded
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1] == ("001_first.sql",)


def test_files_are_executed_in_sorted_order(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """Files supplied in non-sorted filesystem order still run in sorted order."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "20260101_b.sql": "PRINT 'b';",
            "20260102_c.sql": "PRINT 'c';",
            "20251231_a.sql": "PRINT 'a';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    execute_migrations(mssql_resource, "migrations")

    sql_calls = _execute_call_strings(cursor_mock)
    migration_calls = [s for s in sql_calls if s in ("PRINT 'a';", "PRINT 'b';", "PRINT 'c';")]
    assert migration_calls == ["PRINT 'a';", "PRINT 'b';", "PRINT 'c';"]


# ---------------------------------------------------------------------------
# baseline_migrations
# ---------------------------------------------------------------------------


def test_baseline_records_all_without_executing_sql(
    tmp_path, monkeypatch, mssql_resource, cursor_mock
):
    """Empty tracker → every file is stamped, but its SQL is NEVER executed."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []  # tracker empty

    result = baseline_migrations(mssql_resource, "migrations")

    assert result == {
        "001_first.sql": "baselined",
        "002_second.sql": "baselined",
    }

    sql_calls = _execute_call_strings(cursor_mock)
    # The load-bearing property: migration SQL content is NEVER executed.
    assert "PRINT 'first';" not in sql_calls
    assert "PRINT 'second';" not in sql_calls
    # meta-schema guard runs first, then bootstrap, then SELECT.
    assert "SCHEMA_ID('meta')" in sql_calls[0]
    assert "migration_history" in sql_calls[1]
    assert "CREATE TABLE" in sql_calls[1].upper()
    # One INSERT per file, filename as parameter.
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert sorted(call.args[1][0] for call in insert_calls) == [
        "001_first.sql",
        "002_second.sql",
    ]


def test_baseline_skips_already_present(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """Files already in the tracker are reported 'already-present', not re-inserted."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = [("001_first.sql",)]

    result = baseline_migrations(mssql_resource, "migrations")

    assert result == {
        "001_first.sql": "already-present",
        "002_second.sql": "baselined",
    }
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1] == ("002_second.sql",)


def test_baseline_only_allowlist_stamps_subset(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """`only` restricts stamping to the named files; others are untouched."""
    ddl_root = _make_migrations_dir(
        tmp_path,
        {
            "001_first.sql": "PRINT 'first';",
            "002_second.sql": "PRINT 'second';",
            "003_third.sql": "PRINT 'third';",
        },
    )
    _patch_ddl_root(monkeypatch, ddl_root)
    cursor_mock.fetchall.return_value = []

    result = baseline_migrations(
        mssql_resource, "migrations", only=["001_first.sql", "003_third.sql"]
    )

    assert result == {"001_first.sql": "baselined", "003_third.sql": "baselined"}
    insert_calls = [
        call for call in cursor_mock.execute.call_args_list if "INSERT INTO" in call.args[0].upper()
    ]
    assert sorted(call.args[1][0] for call in insert_calls) == [
        "001_first.sql",
        "003_third.sql",
    ]


def test_baseline_allowlist_not_on_disk_raises(tmp_path, monkeypatch, mssql_resource, cursor_mock):
    """An `only` entry that does not exist on disk is a hard error."""
    ddl_root = _make_migrations_dir(tmp_path, {"001_first.sql": "PRINT 'first';"})
    _patch_ddl_root(monkeypatch, ddl_root)

    with pytest.raises(RuntimeError, match="not found on disk"):
        baseline_migrations(mssql_resource, "migrations", only=["999_missing.sql"])


def test_baseline_empty_dir_raises(tmp_path, monkeypatch, mssql_resource):
    """No migration files discovered → refuse rather than baseline nothing."""
    ddl_root = _make_migrations_dir(tmp_path, {})  # creates ddl/migrations, empty
    _patch_ddl_root(monkeypatch, ddl_root)

    with pytest.raises(RuntimeError, match="no .sql files found"):
        baseline_migrations(mssql_resource, "migrations")
