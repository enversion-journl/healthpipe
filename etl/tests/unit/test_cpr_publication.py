# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Publication boundaries are tested with synthetic connection responses."""

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest
from core.mssql_application_lock import application_lock
from DSI.csv_files import cpr_publication

pytestmark = pytest.mark.unit


def database_with_row(row, *, register_exists=True):
    """The control table's existence is asked first, then the delivery counts."""
    db = MagicMock()
    conn = db.get_connection.return_value.__enter__.return_value
    cursor = conn.cursor.return_value
    cursor.fetchone.side_effect = [(1 if register_exists else 0,), row, row, row]
    return db, conn, cursor


def test_session_lock_is_released_even_when_body_fails():
    db, conn, cursor = database_with_row((0,))
    cursor.fetchone.side_effect = None
    cursor.fetchone.return_value = (0,)
    with pytest.raises(ValueError, match="synthetic"):
        with application_lock(db, "synthetic-lock"):
            raise ValueError("synthetic")
    calls = cursor.execute.call_args_list
    assert "sp_getapplock" in calls[0].args[0]
    assert "sp_releaseapplock" in calls[-1].args[0]
    assert "Session" in calls[0].args[0] and "Session" in calls[-1].args[0]
    assert conn.commit.call_count == 2
    cursor.close.assert_called_once()


@pytest.mark.parametrize("status", [-1, -2, -3, -999])
def test_unavailable_lock_never_enters_body(status):
    db, _, cursor = database_with_row((status,))
    cursor.fetchone.side_effect = None
    cursor.fetchone.return_value = (status,)
    entered = False
    with pytest.raises(RuntimeError, match="unavailable"):
        with application_lock(db, "synthetic-lock"):
            entered = True
    assert not entered
    assert len(cursor.execute.call_args_list) == 1
    cursor.close.assert_called_once()


@pytest.mark.parametrize("row,available", [((0, 0), False), ((14, 0), True)])
def test_reader_holds_shared_lock_until_all_reads_finish(monkeypatch, row, available):
    events = []

    @contextmanager
    def lock(db, name, *, mode):
        assert mode == "Shared" and name == "cpr-publication"
        events.append("acquire")
        yield
        events.append("release")

    monkeypatch.setattr(cpr_publication, "application_lock", lock)
    db, _, _ = database_with_row(row)
    with cpr_publication.published_cpr(db) as exists:
        assert exists is available
        assert events == ["acquire"]
    assert events == ["acquire", "release"]


def test_failed_archive_cannot_be_read_as_published(monkeypatch):
    @contextmanager
    def lock(*args, **kwargs):
        yield

    monkeypatch.setattr(cpr_publication, "application_lock", lock)
    db, _, _ = database_with_row((28, 14))
    with pytest.raises(RuntimeError, match="unpublished"):
        with cpr_publication.published_cpr(db):
            pytest.fail("reader entered a partial archive")


def test_missing_register_yields_unavailable_without_a_lock(monkeypatch):
    """An environment without the register reads nothing and takes no lock."""
    monkeypatch.setattr(
        cpr_publication, "application_lock", lambda *a, **k: pytest.fail("lock taken")
    )
    db, _, _ = database_with_row((0, 0), register_exists=False)
    with cpr_publication.published_cpr(db) as exists:
        assert exists is False


def test_replaying_completed_delivery_invalidates_marker_before_writes():
    db, conn, cursor = database_with_row((14,))
    cursor.fetchone.side_effect = None
    cursor.fetchone.return_value = (14,)
    cpr_publication.begin_archive_publication(db, "20990101", 14)
    assert "arkiveret_tidspunkt=NULL" in cursor.execute.call_args_list[-1].args[0]
    conn.commit.assert_called_once()
