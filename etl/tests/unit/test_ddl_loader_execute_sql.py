# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Behavioral tests for the connection handling in ``ddl_loader.execute_sql``.

``execute_sql`` flips ``conn.autocommit`` for the duration of a script (ALTER
DATABASE is rejected inside the driver's implicit transaction) and restores the
previous value afterwards. Restoring a connection attribute is an ODBC
``SQLSetConnectAttr`` call, which the driver rejects while a statement handle
still holds unconsumed results:

    Driver Error: General error; DDBC Error:
    [Microsoft]Connection is busy with results for another command

That is exactly what ``common/01_db_settings.sql`` produces whenever one of its
``ALTER DATABASE`` branches actually fires, so the restore must happen only
after the cursor is closed.
"""

import pytest
from infrastructure.utils.ddl_loader import execute_sql

pytestmark = pytest.mark.unit


class _ConnectionBusyError(Exception):
    """Stand-in for mssql_python.exceptions.OperationalError."""


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 0
        self.closed = False

    def execute(self, sql, *params):
        # A executed batch leaves results on the connection until the cursor
        # is closed (or every result set is drained).
        self._conn.has_pending_results = True

    def close(self):
        self.closed = True
        self._conn.has_pending_results = False


class _FakeConnection:
    """Models the driver rule that broke ``*_infra_db_settings``.

    Setting ``autocommit`` maps to ``SQLSetConnectAttr``, which fails while a
    statement handle still has results pending.
    """

    def __init__(self):
        self._autocommit = False
        self.has_pending_results = False
        self.cursors = []

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value):
        if self.has_pending_results:
            raise _ConnectionBusyError(
                "Driver Error: General error; DDBC Error: [Microsoft]"
                "Connection is busy with results for another command"
            )
        self._autocommit = value

    def cursor(self):
        cursor = _FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        pass

    def rollback(self):
        pass


class _FakeResource:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        conn = self._conn

        class _Ctx:
            def __enter__(self):
                return conn

            def __exit__(self, *args):
                return False

        return _Ctx()


def test_autocommit_is_restored_after_a_batch_leaves_results_pending():
    conn = _FakeConnection()

    execute_sql(_FakeResource(conn), "ALTER DATABASE [dso] SET AUTO_CLOSE OFF", autocommit=True)

    assert conn.autocommit is False, "the previous autocommit value must be restored"
    assert all(cursor.closed for cursor in conn.cursors), "every cursor must be closed"


def test_autocommit_is_restored_when_a_batch_raises():
    conn = _FakeConnection()

    class _FailingCursor(_FakeCursor):
        def execute(self, sql, *params):
            super().execute(sql, *params)
            raise RuntimeError("boom")

    conn.cursor = lambda: conn.cursors.append(_FailingCursor(conn)) or conn.cursors[-1]

    with pytest.raises(RuntimeError, match="boom"):
        execute_sql(_FakeResource(conn), "SELECT 1", autocommit=True)

    assert (
        conn.autocommit is False
    ), "a failing batch must not leave the connection on the temporary autocommit setting"
