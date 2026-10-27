# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""A FHIR builder must never move its checkpoint past rows it has not read.

Builders read arc on ``dw_timestamp_update > checkpoint``. They used to set the
checkpoint to SYSDATETIME() *after* posting, so any archive row written while a
builder ran fell behind the checkpoint and was never read. With the backfill
posting in its own window, that overlap with the daily chain became likely.
"""

from __future__ import annotations

import pathlib
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from core.dfm_manager import DfmManager
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint

_FHIR_DIR = pathlib.Path(__file__).resolve().parents[2] / "DSA" / "FHIR"
_FINISH_TIME_QUERY = "SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)"

# Captures taken BEFORE reading are fine; these are the known ones.
_ALLOWED_BEFORE_READ = {
    # Captures the window's upper bound before reading.
    "Organization/Organization_SOR_dfm.py",
    # Seeds the child watermark the first time only; not a post-read advance.
    "Encounter/Encounter_IFDB_dfm.py",
}


def _config(advance: bool = True, mode: str = "delta") -> SimpleNamespace:
    return SimpleNamespace(should_advance_checkpoint=advance, mode=mode)


class TestAdvanceFhirCheckpoint:
    def test_moves_to_the_safe_read_start(self) -> None:
        dfm = MagicMock()
        dfm.safe_reader_checkpoint.return_value = "2026-09-29 05:59:00.000"
        run_ctx = SimpleNamespace(started_at="2026-09-29 06:00:00")

        value = advance_fhir_checkpoint(dfm, run_ctx, "Patient", _config(), total_fetched=3)

        dfm.safe_reader_checkpoint.assert_called_once_with("2026-09-29 06:00:00")
        dfm.set_checkpoint.assert_called_once_with(
            "fhir_server", "Patient", value="2026-09-29 05:59:00.000", mode="delta"
        )
        assert value == "2026-09-29 05:59:00.000"

    def test_nothing_fetched_leaves_the_checkpoint(self) -> None:
        dfm = MagicMock()
        assert advance_fhir_checkpoint(dfm, SimpleNamespace(), "Patient", _config(), 0) is None
        dfm.set_checkpoint.assert_not_called()

    def test_a_run_that_may_not_advance_leaves_the_checkpoint(self) -> None:
        dfm = MagicMock()
        cfg = _config(advance=False, mode="period")
        assert advance_fhir_checkpoint(dfm, SimpleNamespace(), "Patient", cfg, 5) is None
        dfm.set_checkpoint.assert_not_called()


class TestSafeReaderCheckpoint:
    def _manager_capturing_sql(self) -> tuple[DfmManager, list]:
        seen: list = []
        cursor = MagicMock()
        cursor.execute.side_effect = lambda sql, params=None: seen.append((sql, params))
        cursor.fetchone.return_value = ("2026-09-29 05:00:00.000",)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        manager = DfmManager.__new__(DfmManager)
        manager._get_conn = MagicMock()
        manager._get_conn.return_value.__enter__.return_value = conn
        return manager, seen

    def test_is_held_back_to_any_archive_merge_in_flight(self) -> None:
        """A MERGE stamps its rows at statement start but they appear at commit."""
        manager, seen = self._manager_capturing_sql()
        assert manager.safe_reader_checkpoint("2026-09-29 06:00:00") == "2026-09-29 05:00:00.000"
        sql, _ = seen[0]
        assert "[status] = 'running'" in sql
        assert "MIN([run_started_at])" in sql
        # A 'running' row older than a day is a crashed run, not a MERGE.
        assert "DATEADD(hour, -24, SYSDATETIME())" in sql

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("writer", "expected"),
        [
            (None, ("2026-09-29 06:00:00", "archive")),
            (("dim", "Borger"), ("2026-09-29 06:00:00", "dimension", "dim", "Borger")),
        ],
    )
    def test_is_held_back_only_by_the_named_writer(self, writer, expected) -> None:
        """A reader of dim.Borger waits for a dim_Borger run, not for an archive MERGE."""
        manager, seen = self._manager_capturing_sql()
        manager.safe_reader_checkpoint("2026-09-29 06:00:00", writer=writer)
        sql, params = seen[0]
        assert params == expected
        assert sql.count("?") == len(params)


class TestNoBuilderMovesItsCheckpointToTheFinishTime:
    def test_builders_use_the_shared_advance(self) -> None:
        offenders = []
        for path in sorted(_FHIR_DIR.rglob("*.py")):
            rel = path.relative_to(_FHIR_DIR).as_posix()
            if rel in _ALLOWED_BEFORE_READ:
                continue
            if _FINISH_TIME_QUERY in path.read_text(encoding="utf-8"):
                offenders.append(rel)
        assert not offenders, (
            "these builders set their checkpoint to the time they finished; "
            "use DSA.FHIR.utils.fhir_checkpoint.advance_fhir_checkpoint: " + ", ".join(offenders)
        )
