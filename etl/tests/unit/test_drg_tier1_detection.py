# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for DRG tier-1 delivery detection.

Design: docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md.

Tier 1 decides, from the FILENAME alone, whether a delivery has already been
ingested. It is what turns a run with no new delivery from ~16 GB of
decompression into an os.listdir.
"""

from datetime import date, datetime

import pytest
from DSI.connectors.delivery_detection import delivery_identity
from DSI.csv_files import drg

pytestmark = pytest.mark.unit


def _file(takstsystem="T26", kadence="M", ts="20260818114549"):
    name = (
        f"Region_1083_DRG_{takstsystem}_{kadence}_{ts}.tar.bz2"
        if kadence
        else f"Region_1083_DRG_{takstsystem}_{ts}.tar.bz2"
    )
    parsed = drg.parse_archive_name(name)
    assert parsed is not None, name
    ltype, takst, kad, timestamp = parsed
    return drg.DrgArchiveFile(
        path=f"/mnt/{name}",
        leverancetype=ltype,
        takstsystem=takst,
        kadence=kad,
        timestamp=timestamp,
    )


class TestKadenceFilter:
    def test_monthly_job_takes_m_files(self):
        assert drg.matches_kadence(_file(kadence="M"), "M") is True

    def test_monthly_job_also_takes_files_with_no_cadence_letter(self):
        """The 8 oldest T24/T25 deliveries predate the M/U convention. Without
        this they would be loaded by no job at all."""
        assert drg.matches_kadence(_file(kadence=None), "M") is True

    def test_monthly_job_rejects_weekly_files(self):
        assert drg.matches_kadence(_file(kadence="U"), "M") is False

    def test_weekly_job_takes_only_u_files(self):
        assert drg.matches_kadence(_file(kadence="U"), "U") is True
        assert drg.matches_kadence(_file(kadence="M"), "U") is False

    def test_weekly_job_rejects_cadence_less_files_strictly(self):
        """They must land in the M bucket, and in exactly one bucket."""
        assert drg.matches_kadence(_file(kadence=None), "U") is False

    def test_no_cadence_configured_takes_everything(self):
        for kadence in ("M", "U", None):
            assert drg.matches_kadence(_file(kadence=kadence), None) is True


class TestSelectPending:
    def test_first_run_everything_is_pending(self):
        files = [_file(ts="20260818114549"), _file(takstsystem="T25", ts="20260818120000")]
        pending, notes = drg.select_pending(files, {})
        assert len(pending) == 2
        assert notes == []

    def test_already_ingested_delivery_is_dropped(self):
        """The point of the whole feature: a rerun reads nothing."""
        file = _file(ts="20260818114549")
        checkpoint = delivery_identity(file.timestamp)
        pending, notes = drg.select_pending([file], {"T26": checkpoint})
        assert pending == []
        assert "allerede indlaest" in notes[0]

    def test_newer_delivery_is_pending(self):
        older = _file(ts="20260811114549")
        newer = _file(ts="20260818114549")
        pending, _ = drg.select_pending([newer], {"T26": delivery_identity(older.timestamp)})
        assert pending == [newer]

    def test_older_delivery_is_not_pending(self):
        older = _file(ts="20260811114549")
        newer = _file(ts="20260818114549")
        pending, notes = drg.select_pending([older], {"T26": delivery_identity(newer.timestamp)})
        assert pending == []
        assert notes

    def test_same_day_files_are_distinguished_by_time(self):
        """Date-only identity would make the second file of a day invisible."""
        morning = _file(ts="20260818030000")
        evening = _file(ts="20260818230000")
        pending, _ = drg.select_pending([evening], {"T26": delivery_identity(morning.timestamp)})
        assert pending == [evening]

    def test_checkpoints_are_per_takstsystem(self):
        """T26 being up to date must not hide a pending T25 delivery."""
        t25 = _file(takstsystem="T25", ts="20260818114549")
        t26 = _file(takstsystem="T26", ts="20260818114549")
        pending, _ = drg.select_pending([t25, t26], {"T26": delivery_identity(t26.timestamp)})
        assert pending == [t25]

    def test_a_takstsystem_with_no_checkpoint_is_pending(self):
        """Rollover: T27 appears and is loaded without any configuration."""
        t27 = _file(takstsystem="T27")
        pending, _ = drg.select_pending([t27], {"T26": "2026-08-18T11:45:49"})
        assert pending == [t27]


class TestDetectionIsOffForDeliberateReloads:
    """A pin or an unset cadence is an ad-hoc reload: detection would skip the
    very delivery the operator is trying to re-read."""

    def test_on_for_a_normal_scheduled_run(self):
        assert drg.detection_enabled("M", None) is True
        assert drg.detection_enabled("U", None) is True

    def test_off_when_a_delivery_is_pinned(self):
        assert drg.detection_enabled("M", date(2026, 8, 1)) is False

    def test_off_when_no_cadence_is_configured(self):
        assert drg.detection_enabled(None, None) is False


class TestFreshness:
    """The standalone staleness check. Scoped to the two highest takstsystemer
    per leverancetype — measured from the delivery inventory (§4), and what
    makes the check survive a tariff-year rollover without configuration."""

    def _share(self):
        # T24 closed (last M in March), T25 and T26 both still delivering M on
        # the same day, U only on T26 — the real shape of the share.
        return [
            _file("T24", "M", "20260317120000"),
            _file("T25", "M", "20260818120000"),
            _file("T26", "M", "20260818114549"),
            _file("T26", "U", "20260824190546"),
        ]

    def test_healthy_share_has_no_problems(self):
        problems, ages = drg.evaluate_freshness(self._share(), date(2026, 8, 25))
        assert problems == []
        assert ages["DRG/U"] == 1

    def test_closed_tariff_year_does_not_alarm(self):
        """T24 stopped in March and is third-highest — it must leave the check
        set by itself, or every closed year would alarm forever."""
        problems, ages = drg.evaluate_freshness(self._share(), date(2026, 8, 25))
        assert not any("T24" in p for p in problems)
        assert not any("T24" in k for k in ages)

    def test_dead_weekly_feed_is_caught_even_while_monthly_arrives(self):
        """The defect in the old inline check: it saw the newest file per
        takstsystem REGARDLESS of cadence, so a fresh M hid a dead U."""
        share = [f for f in self._share() if f.kadence != "U"]
        share.append(_file("T26", "U", "20260601120000"))
        problems, _ = drg.evaluate_freshness(share, date(2026, 8, 25))
        assert any("DRG/U" in p for p in problems)

    def test_dead_monthly_feed_on_the_second_highest_is_caught(self):
        """T25 M and T26 M deliver together; checking only the highest would
        hide a dead T25."""
        share = [f for f in self._share() if not (f.takstsystem == "T25")]
        share.append(_file("T25", "M", "20260401120000"))
        problems, _ = drg.evaluate_freshness(share, date(2026, 8, 25))
        assert any("T25" in p for p in problems)

    def test_new_tariff_year_without_a_weekly_series_does_not_alarm(self):
        """T27 M starts in December, its U only months later, while T26 U is
        still alive. A per-takstsystem U check would false-alarm throughout."""
        share = [
            _file("T26", "M", "20251201120000"),
            _file("T26", "U", "20251214120000"),
            _file("T27", "M", "20251214120000"),
        ]
        problems, _ = drg.evaluate_freshness(share, date(2025, 12, 15))
        assert problems == []

    def test_psyk_is_never_checked_for_a_weekly_series(self):
        """Psyk ships M only; a U check would alarm forever."""
        share = [
            drg.DrgArchiveFile(
                path="/mnt/x",
                leverancetype="DRGPSYK",
                takstsystem="T26",
                kadence="M",
                timestamp=datetime(2026, 8, 12, 0, 0, 0),
            )
        ]
        problems, ages = drg.evaluate_freshness(share, date(2026, 8, 25))
        assert problems == []
        assert "DRGPSYK/U" not in ages
