# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL ownership of shared LPR3 landing tables across Dagster runs.

An exclusive application lock serialises claims. Shared session locks permit
the four workers concurrently, but prevent takeover while any old worker is
still running (including an orphan after Dagster marked its run failed).
The persisted owner protects the gaps between assets. Only a terminal Dagster
run may be replaced; unknown owners fail closed. No lock timeout guesses when
a multi-hour archive is safe to overwrite.
"""

from contextlib import contextmanager

LOCK_NAME = "healthpipe:lpr3:landing"
RUN_TABLE = "lpr3_leverance_run"


@contextmanager
def _application_lock(conn, mode):
    cursor = conn.cursor()
    acquired = False
    try:
        cursor.execute(
            "DECLARE @result INT; EXEC @result=sys.sp_getapplock "
            "@Resource=?, @LockMode=?, @LockOwner='Session', @LockTimeout=0; SELECT @result",
            (LOCK_NAME, mode),
        )
        if cursor.fetchone()[0] < 0:
            raise ValueError("LPR3 landing is in use by another active operation")
        acquired = True
        yield cursor
    finally:
        if acquired:
            cursor.execute(
                "EXEC sys.sp_releaseapplock @Resource=?, @LockOwner='Session'", (LOCK_NAME,)
            )
        cursor.close()


@contextmanager
def claim_run(conn, context):
    """Claim before resetting manifest/ext, holding exclusivity through that reset."""
    with _application_lock(conn, "Exclusive") as cursor:
        cursor.execute(
            f"IF OBJECT_ID(N'ext.{RUN_TABLE}',N'U') IS NULL "
            f"CREATE TABLE [ext].[{RUN_TABLE}] ("
            "[slot] BIT NOT NULL PRIMARY KEY CHECK ([slot]=1), "
            "[run_id] NVARCHAR(64) NOT NULL)"
        )
        cursor.execute(f"SELECT [run_id] FROM [ext].[{RUN_TABLE}] WHERE [slot]=1")
        row = cursor.fetchone()
        if row and row[0] != context.run_id:
            previous = context.instance.get_run_by_id(row[0])
            if previous is None or not previous.is_finished:
                raise ValueError(
                    "LPR3 landing belongs to another unfinished or unknown Dagster run"
                )
        cursor.execute(f"DELETE FROM [ext].[{RUN_TABLE}] WHERE [slot]=1")
        cursor.execute(
            f"INSERT INTO [ext].[{RUN_TABLE}] ([slot],[run_id]) VALUES (1,?)",
            (context.run_id,),
        )
        conn.commit()
        yield


@contextmanager
def owned_landing(conn, run_id):
    """Hold for the whole load/merge so even orphaned processes cannot be displaced."""
    with _application_lock(conn, "Shared") as cursor:
        cursor.execute(f"SELECT [run_id] FROM [ext].[{RUN_TABLE}] WHERE [slot]=1")
        row = cursor.fetchone()
        if row is None or row[0] != run_id:
            raise ValueError("LPR3 landing belongs to a different Dagster run")
        yield
