# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Behavioral tests for batch error visibility and commit isolation in ``execute_sql``.

Both behaviours below were reproduced against SQL Server 2022 with the
``mssql-python`` driver this project uses, so the fake here models measured
driver behaviour rather than a guess.

1. When a batch's first statement produces a row count (any UPDATE/INSERT/DELETE
   with NOCOUNT off), an error raised by a LATER statement in that same batch is
   not reported by ``cursor.execute()``. It surfaces only when the result sets
   are drained via ``cursor.nextset()``. A migration whose ALTER COLUMN failed
   with

       Msg 4922 -- ALTER TABLE ALTER COLUMN <col> failed because one or more
                   objects access this column

   therefore looked like a success and got recorded in
   ``[meta].[MigrationHistory]``, which stops it from ever running again.

2. That error still rolls back the transaction open at the time on the server.
   Because a whole migration file used to run as ONE transaction that committed
   after the last batch, one hidden failure silently discarded every uncommitted
   change made by earlier batches in the same file — while ``commit()`` reported
   success. Work done in batches AFTER the error committed normally, so a file
   ended up partially applied depending on batch order.

Committing per batch bounds the damage to the failing batch, and draining the
result sets makes the failure loud so the file is not recorded as applied.
"""

import pytest
from infrastructure.utils.ddl_loader import execute_sql

pytestmark = pytest.mark.unit

# A batch whose failure is only observable by draining the result sets.
POISON = "ALTER TABLE [dbo].[B] ALTER COLUMN [v] INT NOT NULL;"


class _LateBatchError(Exception):
    """Stand-in for the error the driver only reports on nextset()."""


class _FakeCursor:
    """Models a statement handle over a batch that may hide a late error."""

    def __init__(self, conn):
        self._conn = conn
        self.rowcount = 1
        self.closed = False
        self._pending_error = None
        self._sets_left = 0

    def execute(self, sql, *params):
        self._conn.executed.append(sql)
        self._conn.has_pending_results = True
        self._sets_left = 1
        if POISON in sql:
            # The server aborts the batch and rolls back the open transaction.
            # Nothing is raised to the client here — that is the whole bug.
            self._conn.staged.clear()
            self._pending_error = _LateBatchError(
                "DDBC Error: [Microsoft][SQL Server]The index 'IX_B' is dependent on column 'v'."
            )
        else:
            self._conn.staged.append(sql)

    def nextset(self):
        self._conn.drain_calls += 1
        if self._pending_error is not None:
            err, self._pending_error = self._pending_error, None
            self._sets_left = 0
            self._conn.has_pending_results = False
            raise err
        if self._sets_left:
            self._sets_left -= 1
            return True
        self._conn.has_pending_results = False
        return False

    def close(self):
        self.closed = True
        self._conn.has_pending_results = False


class _FakeConnection:
    def __init__(self):
        self.autocommit = False
        self.has_pending_results = False
        self.drain_calls = 0
        self.executed = []
        self.staged = []  # applied but not yet committed
        self.committed = []  # durable
        self.commits = 0
        self.rollbacks = 0
        self.cursors = []

    def cursor(self):
        cursor = _FakeCursor(self)
        self.cursors.append(cursor)
        return cursor

    def commit(self):
        self.commits += 1
        self.committed.extend(self.staged)
        self.staged.clear()

    def rollback(self):
        self.rollbacks += 1
        self.staged.clear()


class _FakeResource:
    def __init__(self, conn):
        self._conn = conn

    def get_connection(self):
        conn = self._conn

        class _Ctx:
            def __enter__(self):
                return conn

            def __exit__(self, *args):
                # An uncommitted transaction dies with the connection.
                conn.staged.clear()
                return False

        return _Ctx()


def _script(*batches):
    return "\nGO\n".join(batches)


def test_error_from_a_later_statement_in_a_batch_is_raised():
    """The failure is invisible to execute(); draining the result sets exposes it."""
    conn = _FakeConnection()

    with pytest.raises(_LateBatchError, match="dependent on column"):
        execute_sql(_FakeResource(conn), POISON, description="migration")


def test_a_hidden_failure_cannot_discard_an_earlier_batchs_work():
    """Batch 1 must already be durable when batch 2 fails."""
    conn = _FakeConnection()
    good = "UPDATE [dbo].[A] SET v = 99 WHERE v IS NULL;"

    with pytest.raises(_LateBatchError):
        execute_sql(_FakeResource(conn), _script(good, POISON), description="migration")

    assert good in conn.committed, (
        "a later batch's hidden rollback must not silently discard work that an "
        "earlier batch completed successfully"
    )


def test_each_batch_is_committed_separately():
    conn = _FakeConnection()
    batches = [
        "UPDATE [dbo].[A] SET v = 1;",
        "UPDATE [dbo].[B] SET v = 2;",
        "UPDATE [dbo].[C] SET v = 3;",
    ]

    execute_sql(_FakeResource(conn), _script(*batches), description="migration")

    assert conn.commits == 3, "each batch gets its own commit"
    assert conn.committed == batches
    assert conn.staged == [], "nothing may be left uncommitted"


def test_collect_errors_commits_the_good_batches_and_reports_the_failure():
    """The directory path keeps running after a failure; good work must survive."""
    conn = _FakeConnection()
    first = "UPDATE [dbo].[A] SET v = 1;"
    last = "UPDATE [dbo].[C] SET v = 3;"

    with pytest.raises(RuntimeError, match="1 batch\\(es\\) failed"):
        execute_sql(
            _FakeResource(conn),
            _script(first, POISON, last),
            description="ddl",
            collect_errors=True,
        )

    assert conn.committed == [first, last], "batches that succeeded must be durable"


def test_result_sets_are_drained_so_the_connection_is_not_left_busy():
    conn = _FakeConnection()

    execute_sql(_FakeResource(conn), "UPDATE [dbo].[A] SET v = 1;", description="ddl")

    assert conn.drain_calls > 0, (
        "the result sets must be drained explicitly -- that is what exposes a late "
        "error, and it also clears the 'Connection is busy with results for another "
        "command' state before the next connection-level call"
    )
    assert conn.has_pending_results is False


def test_autocommit_mode_does_not_issue_explicit_commits():
    """With autocommit on, the driver commits; an explicit commit is wrong."""
    conn = _FakeConnection()

    execute_sql(_FakeResource(conn), "ALTER DATABASE [dso] SET AUTO_CLOSE OFF", autocommit=True)

    assert conn.commits == 0
