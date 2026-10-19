# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Session locks for cooperating workflows spanning several SQL connections."""

from contextlib import contextmanager


@contextmanager
def application_lock(database, name: str, *, mode: str = "Exclusive", timeout_ms: int = 30_000):
    """Hold a SQL Server application lock until the entire operation finishes.

    Session ownership survives commits on the owner connection. Release it
    explicitly because a pooled ODBC connection can outlive its Python handle.
    All cooperating readers/writers must use the same database and principal.
    """
    if mode not in {"Shared", "Exclusive"} or not 0 <= timeout_ms <= 300_000:
        raise ValueError("Invalid application lock options")
    with database.get_connection() as conn:
        cursor = conn.cursor()
        acquired = False
        try:
            cursor.execute(
                "SET NOCOUNT ON; DECLARE @result int; "
                "EXEC @result=sys.sp_getapplock @Resource=?, @LockMode=?, "
                "@LockOwner='Session', @LockTimeout=?, @DbPrincipal='public'; "
                "SELECT @result;",
                (name, mode, timeout_ms),
            )
            row = cursor.fetchone()
            if row is None or row[0] < 0:
                raise RuntimeError("Source publication lock unavailable")
            acquired = True
            conn.commit()
            yield
        finally:
            if acquired:
                cursor.execute(
                    "EXEC sys.sp_releaseapplock @Resource=?, @LockOwner='Session', "
                    "@DbPrincipal='public';",
                    (name,),
                )
                conn.commit()
            cursor.close()
