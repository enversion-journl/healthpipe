# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Delivery discovery and DFM-driven selection for SilkeborgData.

This is where the expensive bug lives: select the deliveries wrongly and you
either load nothing for months or reload the whole history every day — and both
look like a green run. Everything here runs against tmp_path, with no share, no
network and no database, which is what makes it the cheapest place to get the
DFM logic right.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest
from DSI.csv_files.silkeborgdata import (
    Delivery,
    discover_deliveries,
    is_delivery_settled,
    parse_checkpoint_date,
    parse_delivery_date,
    partition_settled_deliveries,
    select_deliveries,
)

pytestmark = pytest.mark.unit

FILE_NAME = "E26PERS.TXT"


class _StubLog:
    def __init__(self) -> None:
        self.warnings: list[str] = []

    def warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def info(self, msg: str) -> None:  # pragma: no cover - convenience
        pass


class _StubContext:
    def __init__(self) -> None:
        self.log = _StubLog()


class _Config:
    """Stands in for SilkeborgDataExtractConfig's run-mode fields."""

    def __init__(self, mode="delta", date_from=None, date_to=None):
        self.mode = mode
        self.date_from = date_from
        self.date_to = date_to


def _delivery(folder: str) -> Delivery:
    return Delivery(
        delivery_date=date(int(folder[:4]), int(folder[4:6]), int(folder[6:])),
        folder=folder,
        path=f"/tmp/{folder}/{FILE_NAME}",
    )


_JUNE = _delivery("20260630")
_JULY = _delivery("20260730")
_AUGUST = _delivery("20260831")
_ALL = [_JUNE, _JULY, _AUGUST]


# ---------------------------------------------------------------------------
# parse_delivery_date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("folder", "expected"),
    [
        ("20260730", date(2026, 7, 30)),
        ("20260101", date(2026, 1, 1)),
        ("2026-07-30", None),
        ("temp", None),
        ("gammel", None),
        ("", None),
        ("20261340", None),  # month 13
    ],
)
def test_parse_delivery_date_only_accepts_the_delivery_format(folder, expected):
    """A non-delivery folder under BASE_DIR must be skipped, not crash the run."""
    assert parse_delivery_date(folder) == expected


# ---------------------------------------------------------------------------
# parse_checkpoint_date
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-07-01", date(2026, 7, 1)),
        # period mode with advance_checkpoint writes "<date_to> 23:59:59"
        ("2026-07-01 23:59:59", date(2026, 7, 1)),
        (None, None),
        ("", None),
        ("   ", None),
    ],
)
def test_parse_checkpoint_date_handles_both_dfm_forms(value, expected):
    """A time suffix must not crash the delta run, and an empty checkpoint is a
    legitimate first run."""
    assert parse_checkpoint_date(value) == expected


def test_parse_checkpoint_date_raises_on_garbage():
    """Silently treating an unreadable checkpoint as 'load everything' would
    reload the whole history on every run — green."""
    with pytest.raises(ValueError, match="Unreadable DFM checkpoint"):
        parse_checkpoint_date("ikke-en-dato")


# ---------------------------------------------------------------------------
# select_deliveries — the mode matrix
# ---------------------------------------------------------------------------


def test_delta_takes_only_folders_strictly_newer_than_the_checkpoint():
    """The boundary is strictly greater than: the checkpoint's own delivery must
    not be re-fetched, and the one after it must not be skipped."""
    selected = select_deliveries(_ALL, _Config("delta"), "2026-07-30")
    assert [d.folder for d in selected] == ["20260831"]


def test_delta_with_no_checkpoint_takes_everything():
    """A first-run bug here would leave a permanently empty archive."""
    selected = select_deliveries(_ALL, _Config("delta"), None)
    assert [d.folder for d in selected] == ["20260831", "20260730", "20260630"]


def test_delta_accepts_a_checkpoint_with_a_time_suffix():
    selected = select_deliveries(_ALL, _Config("delta"), "2026-06-30 23:59:59")
    assert [d.folder for d in selected] == ["20260831", "20260730"]


def test_full_load_ignores_the_checkpoint_completely():
    selected = select_deliveries(_ALL, _Config("full_load"), "2026-08-31")
    assert [d.folder for d in selected] == ["20260831", "20260730", "20260630"]


def test_period_bounds_are_both_inclusive():
    """An off-by-one would cut off the very delivery you meant to reload."""
    selected = select_deliveries(
        _ALL, _Config("period", date_from="2026-06-30", date_to="2026-07-30"), "2026-08-31"
    )
    assert [d.folder for d in selected] == ["20260730", "20260630"]


def test_period_without_date_to_has_an_open_upper_bound():
    """date_to is optional in RunModeConfig — only date_from is required."""
    selected = select_deliveries(_ALL, _Config("period", date_from="2026-07-01"), "2026-08-31")
    assert [d.folder for d in selected] == ["20260831", "20260730"]


def test_period_without_date_from_is_rejected():
    with pytest.raises(ValueError, match="date_from"):
        select_deliveries(_ALL, _Config("period"), None)


def test_ids_falls_back_to_delta_with_a_warning():
    """It must not fail, and it must not silently return everything."""
    context = _StubContext()
    selected = select_deliveries(_ALL, _Config("ids"), "2026-07-30", context)
    assert [d.folder for d in selected] == ["20260831"]
    assert any("ids" in w for w in context.log.warnings)


def test_selection_is_descending_by_date():
    """Precondition for the extract's dedup: the newest delivery must win."""
    selected = select_deliveries(_ALL, _Config("full_load"), None)
    dates = [d.delivery_date for d in selected]
    assert dates == sorted(dates, reverse=True)


# ---------------------------------------------------------------------------
# discover_deliveries — against a real directory tree
# ---------------------------------------------------------------------------


def _make_delivery_tree(tmp_path) -> None:
    for folder in ("20260630", "20260730"):
        directory = tmp_path / folder
        directory.mkdir()
        (directory / FILE_NAME).write_text("data\n", encoding="cp865")
        # The neighbour that must never be picked up.
        (directory / "E26PERSF.TXT").write_text("andet\n", encoding="cp865")
    # Folders that are not deliveries at all.
    (tmp_path / "temp").mkdir()
    (tmp_path / "gammel").mkdir()
    # A delivery folder without the file we want.
    (tmp_path / "20260531").mkdir()
    (tmp_path / "20260531" / "E26TILL.TXT").write_text("x\n", encoding="cp865")


def test_discover_deliveries_finds_only_folders_holding_the_file(tmp_path):
    _make_delivery_tree(tmp_path)
    found = discover_deliveries(str(tmp_path), FILE_NAME)
    assert [d.folder for d in found] == ["20260630", "20260730"]
    assert all(d.path.endswith(FILE_NAME) for d in found)


def test_discover_deliveries_never_matches_the_neighbour_file(tmp_path):
    """A prefix or glob match would hit E26PERSF.TXT too, and because neither
    file has a header the wrong one would not necessarily fail on field count —
    it would just fill the archive with the wrong extract."""
    _make_delivery_tree(tmp_path)
    found = discover_deliveries(str(tmp_path), FILE_NAME)
    assert all(os.path.basename(d.path) == FILE_NAME for d in found)

    other = discover_deliveries(str(tmp_path), "E26PERSF.TXT")
    assert all(os.path.basename(d.path) == "E26PERSF.TXT" for d in other)


def test_discover_deliveries_matches_case_insensitively(tmp_path):
    """A case-sensitive CIFS mount would make the delivery 'disappear' silently
    if SD ever switched to lowercase."""
    directory = tmp_path / "20260930"
    directory.mkdir()
    (directory / "e26pers.txt").write_text("data\n", encoding="cp865")
    found = discover_deliveries(str(tmp_path), FILE_NAME)
    assert [d.folder for d in found] == ["20260930"]


def test_discover_deliveries_skips_foreign_folders_without_failing(tmp_path):
    _make_delivery_tree(tmp_path)
    (tmp_path / "loose_file.txt").write_text("x\n", encoding="cp865")
    found = discover_deliveries(str(tmp_path), FILE_NAME)
    assert [d.folder for d in found] == ["20260630", "20260730"]


def test_discover_deliveries_returns_ascending_dates(tmp_path):
    _make_delivery_tree(tmp_path)
    found = discover_deliveries(str(tmp_path), FILE_NAME)
    dates = [d.delivery_date for d in found]
    assert dates == sorted(dates)


# ---------------------------------------------------------------------------
# is_delivery_settled — guard against a half-written delivery
# ---------------------------------------------------------------------------


def test_is_delivery_settled_rejects_a_just_written_file(tmp_path):
    """A file read mid-write archives half a delivery and moves the checkpoint
    past it, so the finished file is never fetched."""
    path = tmp_path / FILE_NAME
    path.write_text("data\n", encoding="cp865")
    assert is_delivery_settled(str(path), min_age_minutes=60) is False


def test_is_delivery_settled_accepts_an_old_file(tmp_path):
    path = tmp_path / FILE_NAME
    path.write_text("data\n", encoding="cp865")
    old = (date.today() - timedelta(days=1)).toordinal()
    stale = os.stat(path).st_mtime - 3 * 60 * 60
    os.utime(path, (stale, stale))
    assert old  # keep the intent explicit: mtime is what matters, not today
    assert is_delivery_settled(str(path), min_age_minutes=60) is True


# ---------------------------------------------------------------------------
# partition_settled_deliveries — an unsettled folder must block newer ones too
# ---------------------------------------------------------------------------


def _aged_delivery(tmp_path, folder: str, hours_old: float) -> Delivery:
    directory = tmp_path / folder
    directory.mkdir()
    path = directory / FILE_NAME
    path.write_text("data\n", encoding="cp865")
    stamp = os.stat(path).st_mtime - hours_old * 3600
    os.utime(path, (stamp, stamp))
    return Delivery(
        delivery_date=date(int(folder[:4]), int(folder[4:6]), int(folder[6:])),
        folder=folder,
        path=str(path),
    )


def test_all_settled_deliveries_are_loadable(tmp_path):
    items = [
        _aged_delivery(tmp_path, "20260630", 5),
        _aged_delivery(tmp_path, "20260730", 5),
    ]
    loadable, unsettled, deferred = partition_settled_deliveries(items, min_age_minutes=60)
    assert [d.folder for d in loadable] == ["20260730", "20260630"]
    assert unsettled == [] and deferred == []


def test_an_unsettled_delivery_also_defers_everything_newer(tmp_path):
    """The regression this function exists for.

    Dropping only the unsettled folder while loading the newer one would move the
    archive checkpoint (MAX(dw_leverance_dato)) PAST the unsettled folder, and the
    delta selection — strictly newer than the checkpoint — would never offer it
    again. The half-written delivery would be lost permanently, silently, on a
    green run.
    """
    old = _aged_delivery(tmp_path, "20260630", 5)
    writing = _aged_delivery(tmp_path, "20260730", 0)
    newer = _aged_delivery(tmp_path, "20260831", 5)

    loadable, unsettled, deferred = partition_settled_deliveries(
        [newer, writing, old], min_age_minutes=60
    )

    assert [d.folder for d in loadable] == ["20260630"]
    assert [d.folder for d in unsettled] == ["20260730"]
    # 20260831 is settled but must wait, or it would strand 20260730.
    assert [d.folder for d in deferred] == ["20260831"]


def test_an_unsettled_newest_delivery_leaves_the_older_ones_loadable(tmp_path):
    """The ordinary case: only the folder SD is writing right now is held back."""
    old = _aged_delivery(tmp_path, "20260630", 5)
    writing = _aged_delivery(tmp_path, "20260730", 0)
    loadable, unsettled, deferred = partition_settled_deliveries([writing, old], min_age_minutes=60)
    assert [d.folder for d in loadable] == ["20260630"]
    assert [d.folder for d in unsettled] == ["20260730"]
    assert deferred == []


def test_partition_returns_newest_first(tmp_path):
    """Precondition for the extract's newest-delivery-wins dedup."""
    items = [
        _aged_delivery(tmp_path, "20260630", 5),
        _aged_delivery(tmp_path, "20260831", 5),
        _aged_delivery(tmp_path, "20260730", 5),
    ]
    loadable, _, _ = partition_settled_deliveries(items, min_age_minutes=60)
    assert [d.delivery_date for d in loadable] == sorted(
        (d.delivery_date for d in loadable), reverse=True
    )
