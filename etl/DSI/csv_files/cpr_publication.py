# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Publication boundary shared by the CPR importer and person composition."""

from contextlib import contextmanager

from core.mssql_application_lock import application_lock

CPR_PUBLICATION_LOCK = "cpr-publication"


def register_exists(database) -> bool:
    """Is the register deployed here at all? Without its control table there is nothing to read."""
    with database.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s ON s.schema_id = t.schema_id "
                "WHERE s.name = ? AND t.name = ?",
                ("meta", "CPRLeverance"),
            )
            return bool(cursor.fetchone()[0])
        finally:
            cursor.close()


@contextmanager
def published_cpr(database):
    """Yield whether a complete register exists, under the shared lock; raise on a delivery that is not fully archived.

    An environment without the control table yields False without taking the lock.
    """
    if not register_exists(database):
        yield False
        return
    with application_lock(database, CPR_PUBLICATION_LOCK, mode="Shared"):
        with database.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "SELECT COUNT_BIG(*), COALESCE(SUM(CASE WHEN arkiveret_tidspunkt "
                    "IS NULL THEN CAST(1 AS bigint) ELSE 0 END),0) FROM meta.CPRLeverance"
                )
                total, pending = cursor.fetchone()
            finally:
                cursor.close()
        if pending:
            raise RuntimeError("CPR register has an unpublished delivery")
        yield bool(total)


def begin_archive_publication(database, delivery_date, expected_tables):
    """Invalidate the completion marker before any of the 14 table writes."""
    with database.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM meta.CPRLeverance WHERE leverance_dato=CONVERT(date,?,112)",
                (delivery_date,),
            )
            if cursor.fetchone()[0] != expected_tables:
                raise RuntimeError("CPR delivery registration is incomplete")
            cursor.execute(
                "UPDATE meta.CPRLeverance SET arkiveret_tidspunkt=NULL "
                "WHERE leverance_dato=CONVERT(date,?,112)",
                (delivery_date,),
            )
            conn.commit()
        finally:
            cursor.close()
