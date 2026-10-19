# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The pre-commit volume gate on the SilkeborgData E26PERS extract.

The gate exists for one bug the rest of the load cannot see: a delivery file
that is truncated but not empty. Every line it does contain is well-formed, so
field-count validation passes, the settle window passes, and the 0-usable-rows
failure does not fire. The rows then reach the archive, where `active` is
recomputed for the whole population, and everyone the short file omits is marked
as departed all the way into the reporting database.

So these tests are about two things, and nothing else is worth a test here:

  - the RULE — which periods get judged, what each is compared against, and
    where the boundary sits. Tested against _volume_verdicts, which is pure.
  - the PLACEMENT — that a rejection happens before the commit and takes the
    staged rows with it. A gate that fires after the commit is not a gate; it is
    an alarm on a load that already happened, which is what the asset check
    downstream already does.
"""

from __future__ import annotations

from datetime import date

import pytest
from DSI.csv_files.silkeborgdata import write_to_ext_table
from DSI.csv_files.silkeborgdata_e26_pers import (
    VOLUME_TOLERANCE_PCT,
    VolumeGateError,
    _assert_volume_within_tolerance,
    _iter_ext_rows,
    _volume_verdicts,
)

pytestmark = pytest.mark.unit


class _StubLog:
    def __init__(self) -> None:
        self.infos: list[str] = []
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def info(self, msg: str) -> None:
        self.infos.append(msg)

    def warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def error(self, msg: str) -> None:
        self.errors.append(msg)


class _StubContext:
    def __init__(self) -> None:
        self.log = _StubLog()


def _verdicts(run_counts, archive_counts, tolerance_pct=VOLUME_TOLERANCE_PCT):
    return _volume_verdicts(
        run_counts=run_counts,
        archive_counts=archive_counts,
        tolerance_pct=tolerance_pct,
    )


def _by_period(verdicts) -> dict[str, dict]:
    return {verdict["period"]: verdict for verdict in verdicts}


# ---------------------------------------------------------------------------
# The rule — which period is compared against which
# ---------------------------------------------------------------------------


def test_a_normal_delivery_passes():
    verdicts = _verdicts({date(2026, 8, 31): 78_500}, {date(2026, 7, 31): 78_464})
    assert [verdict["passed"] for verdict in verdicts] == [True]
    assert verdicts[0]["previous_period"] == "2026-07-31"
    assert verdicts[0]["compared"] is True


def test_a_truncated_delivery_is_rejected():
    """The bug the gate exists for: a file cut at a line break parses cleanly."""
    verdicts = _verdicts({date(2026, 8, 31): 55_000}, {date(2026, 7, 31): 78_464})
    assert verdicts[0]["passed"] is False
    assert verdicts[0]["deviation_pct"] < -29


def test_a_shrink_just_inside_the_tolerance_passes_and_just_outside_does_not():
    """The boundary itself, from both sides. A gate whose limit is off by a
    rounding step either blocks normal deliveries or lets a real truncation
    through, and neither shows up in a test that only uses obvious numbers."""
    previous = {date(2026, 7, 31): 100_000}
    inside = _verdicts({date(2026, 8, 31): 97_000}, previous)  # exactly -3.0 %
    outside = _verdicts({date(2026, 8, 31): 96_999}, previous)
    assert inside[0]["passed"] is True
    assert outside[0]["passed"] is False


def test_growth_is_gated_too():
    """Not only shrinkage. A doubled file — the same period delivered twice into
    one folder, or a wrong file in the folder — is just as wrong as a short one,
    and would pass a one-sided test."""
    verdicts = _verdicts({date(2026, 8, 31): 156_000}, {date(2026, 7, 31): 78_000})
    assert verdicts[0]["passed"] is False
    assert verdicts[0]["deviation_pct"] > 0


def test_a_period_already_in_the_archive_is_not_judged():
    """Re-reading an archived period cannot shrink it — the archive MERGE
    updates and inserts, it never deletes. Judging it anyway would make every
    full_load a re-litigation of the whole history, and the gate would be the
    first thing anyone turned off."""
    verdicts = _verdicts(
        {date(2026, 7, 31): 40_000, date(2026, 8, 31): 78_500},
        {date(2026, 6, 30): 78_400, date(2026, 7, 31): 78_464},
    )
    assert _by_period(verdicts).keys() == {"2026-08-31"}


def test_two_new_periods_chain_instead_of_sharing_one_baseline():
    """SD delivering two months at once. Month 2 must be measured against month
    1 of the same run; against the archive's old month both would read as one
    large drift, and a run that is behaving correctly would be blocked."""
    verdicts = _by_period(
        _verdicts(
            {date(2026, 8, 31): 80_000, date(2026, 9, 30): 81_500},
            {date(2026, 7, 31): 78_500},
        )
    )
    assert verdicts["2026-08-31"]["previous_period"] == "2026-07-31"
    assert verdicts["2026-09-30"]["previous_period"] == "2026-08-31"
    assert all(verdict["passed"] for verdict in verdicts.values())


def test_a_later_period_is_not_measured_against_a_newer_one():
    """The baseline is the nearest STRICTLY OLDER period. Picking the nearest in
    absolute distance would let an out-of-order backfill measure a period
    against its own successor."""
    verdicts = _verdicts(
        {date(2026, 6, 30): 78_000},
        {date(2026, 5, 31): 77_900, date(2026, 8, 31): 40_000},
    )
    assert verdicts[0]["previous_period"] == "2026-05-31"


def test_a_period_older_than_everything_archived_is_not_compared():
    """Nothing to compare it against, so it is recorded and waved through rather
    than measured against a period that comes after it."""
    verdicts = _verdicts({date(2020, 1, 31): 12}, {date(2026, 7, 31): 78_464})
    assert verdicts[0]["compared"] is False
    assert verdicts[0]["passed"] is True
    assert verdicts[0]["previous_period"] is None


# ---------------------------------------------------------------------------
# _assert_volume_within_tolerance — logging, metadata and the raise
# ---------------------------------------------------------------------------


def test_an_empty_archive_is_not_gated():
    """A first load has no previous period. Chaining the comparison through a
    bootstrap of the whole history would test SD's past reorganisations rather
    than this file, and a failure there is unactionable."""
    context = _StubContext()
    metadata = _assert_volume_within_tolerance(
        run_counts={date(2026, 7, 31): 10, date(2026, 8, 31): 78_464},
        archive_counts={},
        tolerance_pct=VOLUME_TOLERANCE_PCT,
        overridden=False,
        context=context,
    )
    assert "skipped" in metadata["volume_gate"].value


def test_a_rejection_names_the_way_past_it():
    """The operator reading this message at 07:00 has to be able to tell a
    truncation from a real reorganisation, and act on the second without
    editing code. Without the config key in the message, the reachable options
    are 'edit the constant' and 'disable the gate'."""
    context = _StubContext()
    with pytest.raises(VolumeGateError) as excinfo:
        _assert_volume_within_tolerance(
            run_counts={date(2026, 8, 31): 55_000},
            archive_counts={date(2026, 7, 31): 78_464},
            tolerance_pct=VOLUME_TOLERANCE_PCT,
            overridden=False,
            context=context,
        )
    message = str(excinfo.value)
    assert "volume_tolerance_pct" in message
    assert "2026-08-31" in message and "2026-07-31" in message
    # It must also say that nothing was committed; otherwise the first reaction
    # is to go looking for rows to clean up.
    assert "Nothing was committed" in message
    assert context.log.errors


def test_an_override_lets_a_confirmed_change_through_and_says_so():
    context = _StubContext()
    metadata = _assert_volume_within_tolerance(
        run_counts={date(2026, 8, 31): 55_000},
        archive_counts={date(2026, 7, 31): 78_464},
        tolerance_pct=40.0,
        overridden=True,
        context=context,
    )
    assert metadata["volume_gate_tolerance_overridden"].value is True
    assert metadata["volume_gate_tolerance_pct"].value == 40.0


def test_a_passing_gate_still_reports_the_number_it_accepted():
    """A gate that is silent when it passes cannot be audited afterwards, and a
    drift that creeps up to just under the limit stays invisible."""
    context = _StubContext()
    metadata = _assert_volume_within_tolerance(
        run_counts={date(2026, 8, 31): 78_500},
        archive_counts={date(2026, 7, 31): 78_464},
        tolerance_pct=VOLUME_TOLERANCE_PCT,
        overridden=False,
        context=context,
    )
    verdicts = metadata["volume_gate_verdicts"].value
    assert verdicts[0]["deviation_pct"] == pytest.approx(0.05, abs=0.01)


# ---------------------------------------------------------------------------
# Placement — the gate runs before the commit, and a rejection rolls back
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, log: list[tuple[str, object]]) -> None:
        self._log = log

    def execute(self, sql: str) -> None:
        self._log.append(("execute", sql.strip().split()[0].upper()))

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        self._log.append(("executemany", len(rows)))

    def close(self) -> None:
        pass


class _FakeConn:
    """Records the order of the statements, so 'before the commit' is testable."""

    def __init__(self) -> None:
        self.log: list[tuple[str, object]] = []

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self.log)

    def commit(self) -> None:
        self.log.append(("commit", None))

    def rollback(self) -> None:
        self.log.append(("rollback", None))


def _write(conn, rows, pre_commit=None, context=None):
    return write_to_ext_table(
        conn,
        "silkeborgdata_e26_pers",
        {"Inst": "NVARCHAR(10)"},
        iter(rows),
        context or _StubContext(),
        pre_commit=pre_commit,
    )


def test_pre_commit_runs_after_the_last_insert_and_before_the_commit():
    conn = _FakeConn()
    seen: list[int] = []
    _write(conn, [{"Inst": "01"}, {"Inst": "02"}], pre_commit=seen.append)

    verbs = [entry[0] for entry in conn.log]
    assert verbs[-1] == "commit"
    assert "executemany" in verbs
    # The callback is handed the number of rows actually staged, not an estimate.
    assert seen == [2]


def test_a_rejecting_gate_rolls_back_and_never_commits():
    """The whole point of the placement. If the commit went first, the archive
    MERGE in the same job would already have the rows and the gate would be an
    alarm rather than a stop."""

    def _reject(_total: int) -> None:
        raise VolumeGateError("nope")

    conn = _FakeConn()
    context = _StubContext()
    with pytest.raises(VolumeGateError):
        _write(conn, [{"Inst": "01"}], pre_commit=_reject, context=context)

    verbs = [entry[0] for entry in conn.log]
    assert "rollback" in verbs
    assert "commit" not in verbs
    assert context.log.errors, "a rolled-back load must leave a trace in the run log"


def test_write_to_ext_table_still_commits_without_a_gate():
    """The other callers of write_to_ext_table pass no pre_commit; they must be
    unaffected."""
    conn = _FakeConn()
    assert _write(conn, [{"Inst": "01"}]) == 1
    assert ("commit", None) in conn.log


# ---------------------------------------------------------------------------
# The counts the gate is fed
# ---------------------------------------------------------------------------


def _write_delivery(tmp_path, folder: str, periods: list[tuple[str, int]]):
    from DSI.csv_files.silkeborgdata import Delivery
    from DSI.csv_files.silkeborgdata_e26_pers import FILE_COLUMNS

    lines = []
    for period, count in periods:
        for serial in range(count):
            values = {"Inst": "01", "Tjenestenummer": str(serial), "Udtraeksdato": period}
            lines.append(";".join(values.get(column, "") for column in FILE_COLUMNS))
    directory = tmp_path / folder
    directory.mkdir()
    path = directory / "E26PERS.TXT"
    path.write_text("".join(line + "\n" for line in lines), encoding="cp865")
    return Delivery(
        delivery_date=date(int(folder[:4]), int(folder[4:6]), int(folder[6:])),
        folder=folder,
        path=str(path),
    )


def test_usable_rows_are_counted_per_period_not_per_folder(tmp_path):
    """The archive groups by Udtraeksdato, so the gate has to as well — a folder
    is only a delivery date, and comparing one grouping against the other would
    put the drift in the wrong place."""
    delivery = _write_delivery(tmp_path, "20260831", [("2026-07-31", 3), ("2026-08-31", 5)])
    per_period: dict[date, int] = {}
    list(_iter_ext_rows([delivery], "cp865", _stats(), False, _StubContext(), {}, per_period))
    assert per_period == {date(2026, 7, 31): 3, date(2026, 8, 31): 5}


def test_a_re_issued_period_folds_with_max_instead_of_summing(tmp_path):
    """Two folders carrying the same period is a re-issue, and deduplication
    keeps one row per key. Summing would read a healthy backfill as the period
    doubling, and the gate would block on its own bookkeeping."""
    newest = _write_delivery(tmp_path, "20260831", [("2026-08-31", 4)])
    reissued = _write_delivery(tmp_path, "20260815", [("2026-08-31", 4)])
    per_period: dict[date, int] = {}
    list(
        _iter_ext_rows([newest, reissued], "cp865", _stats(), False, _StubContext(), {}, per_period)
    )
    assert per_period == {date(2026, 8, 31): 4}


def _stats() -> dict[str, int]:
    return {
        "rows_rejected_blank_key": 0,
        "duplicates_collapsed": 0,
        "duplicates_across_deliveries": 0,
        "unparsable_dates": 0,
        "unparsable_decimals": 0,
        "trailing_minus_values": 0,
    }


# ---------------------------------------------------------------------------
# The gate and the asset check must not drift apart
# ---------------------------------------------------------------------------


def test_the_asset_check_reads_the_extract_s_tolerance():
    """Two copies of a threshold drift, and a check laxer than the gate it backs
    up would never fire — the archive would be the only place the number lived,
    and nobody would notice it had stopped agreeing with the gate."""
    from DSA.SQL.checks import silkeborgdata_checks

    assert silkeborgdata_checks._VOLUME_TOLERANCE_PCT is VOLUME_TOLERANCE_PCT
