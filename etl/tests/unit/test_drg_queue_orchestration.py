# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the DRG ingest queue.

Design: docs/source_systems/drg/drg_leverancedetektion_beslutningsnotat.md.

The queue is what serialises the runs of one leverancetype — they share ext
tables — and what makes "U only after every M succeeded" a property of the
order rather than a separate trigger.
"""

import json
from datetime import date, datetime

import pytest
from DSI.csv_files.drg import DrgArchiveFile
from schedules import drg_schedules

pytestmark = pytest.mark.unit


def _file(leverancetype="DRG", takstsystem="T26", kadence="M"):
    return DrgArchiveFile(
        path=f"/mnt/{leverancetype}_{takstsystem}_{kadence}",
        leverancetype=leverancetype,
        takstsystem=takstsystem,
        kadence=kadence,
        timestamp=datetime(2026, 8, 18, 11, 45, 49),
    )


class TestQueueOrder:
    def test_every_monthly_entry_precedes_every_weekly_entry(self):
        """The gating: a failed M stops the chain before any U is requested."""
        archives = [
            _file(takstsystem="T25", kadence="M"),
            _file(takstsystem="T26", kadence="M"),
            _file(takstsystem="T26", kadence="U"),
        ]
        queue = drg_schedules.build_queue(archives, "DRG")
        kadencer = [k for _, k in queue]
        assert kadencer == sorted(kadencer, key=lambda k: k != "M")
        assert queue[-1] == ["T26", "U"]

    def test_one_entry_per_takstsystem_and_cadence(self):
        archives = [
            _file(takstsystem="T24", kadence="M"),
            _file(takstsystem="T25", kadence="M"),
            _file(takstsystem="T26", kadence="M"),
            _file(takstsystem="T26", kadence="U"),
        ]
        assert drg_schedules.build_queue(archives, "DRG") == [
            ["T24", "M"],
            ["T25", "M"],
            ["T26", "M"],
            ["T26", "U"],
        ]

    def test_a_takstsystem_without_a_weekly_series_gets_no_weekly_entry(self):
        """A brand-new tariff year has M for months before its first U. A U run
        there could only ever be a no-op."""
        archives = [_file(takstsystem="T27", kadence="M")]
        assert drg_schedules.build_queue(archives, "DRG") == [["T27", "M"]]

    def test_cadence_less_files_queue_as_monthly(self):
        archives = [_file(takstsystem="T24", kadence=None)]
        assert drg_schedules.build_queue(archives, "DRG") == [["T24", "M"]]

    def test_psyk_never_queues_a_weekly_entry(self):
        archives = [_file(leverancetype="DRGPSYK", takstsystem="T26", kadence="M")]
        assert drg_schedules.build_queue(archives, "DRGPSYK") == [["T26", "M"]]

    def test_queues_are_per_leverancetype(self):
        archives = [
            _file(leverancetype="DRG", takstsystem="T26", kadence="M"),
            _file(leverancetype="DRGPSYK", takstsystem="T26", kadence="M"),
        ]
        assert drg_schedules.build_queue(archives, "DRG") == [["T26", "M"]]
        assert drg_schedules.build_queue(archives, "DRGPSYK") == [["T26", "M"]]

    def test_empty_share_gives_an_empty_queue(self):
        assert drg_schedules.build_queue([], "DRG") == []


class TestRunRequest:
    def test_carries_the_unit_as_run_config_and_the_rest_as_a_tag(self):
        request = drg_schedules.queue_run_request(
            "DRG", ["T26", "M"], [["T26", "U"]], date(2026, 8, 18)
        )
        ops = request.run_config["ops"]
        assert list(ops) == ["ext_drg"]
        assert ops["ext_drg"]["config"] == {"kadence": "M", "takstsystemer": ["T26"]}
        assert json.loads(request.tags[drg_schedules.QUEUE_TAG]) == [["T26", "U"]]
        assert request.tags[drg_schedules.LEVERANCETYPE_TAG] == "DRG"
        # The unit is tagged so it is visible in the UI and readable by the
        # sensors without digging the postfixed op key out of the run config.
        assert request.tags[drg_schedules.UNIT_TAG] == "T26/M"

    def test_run_key_is_unique_per_unit_and_day(self):
        """Two ticks the same day must not launch the same unit twice; the next
        day must be able to."""
        first = drg_schedules.queue_run_request("DRG", ["T26", "M"], [], date(2026, 8, 18))
        same = drg_schedules.queue_run_request("DRG", ["T26", "M"], [], date(2026, 8, 18))
        other_unit = drg_schedules.queue_run_request("DRG", ["T25", "M"], [], date(2026, 8, 18))
        next_day = drg_schedules.queue_run_request("DRG", ["T26", "M"], [], date(2026, 8, 19))

        assert first.run_key == same.run_key
        assert first.run_key != other_unit.run_key
        assert first.run_key != next_day.run_key

    def test_the_cycle_date_travels_as_a_tag(self):
        """The queue crosses midnight (20:00 start), so each link rebuilds its
        run_key from this tag rather than from its own UTC creation time —
        otherwise one cycle's run_keys would span two days and the dedup
        property would be accidental."""
        request = drg_schedules.queue_run_request("DRG", ["T26", "M"], [], date(2026, 8, 18))
        assert request.tags[drg_schedules.CYCLE_DATE_TAG] == "2026-08-18"

        chained = drg_schedules.queue_run_request(
            "DRG",
            ["T26", "U"],
            [],
            date.fromisoformat(request.tags[drg_schedules.CYCLE_DATE_TAG]),
        )
        assert "2026-08-18" in chained.run_key

    def test_psyk_configures_its_own_extract_asset(self):
        request = drg_schedules.queue_run_request("DRGPSYK", ["T26", "M"], [], date(2026, 8, 18))
        assert list(request.run_config["ops"]) == ["ext_drgpsyk"]

    def test_last_entry_carries_an_empty_queue(self):
        """The sensor stops on an empty queue rather than looping."""
        request = drg_schedules.queue_run_request("DRG", ["T26", "U"], [], date(2026, 8, 18))
        assert json.loads(request.tags[drg_schedules.QUEUE_TAG]) == []


class TestSchedulesAreRegistered:
    def test_all_three_schedules_are_exported(self):
        from schedules import all_schedules

        names = {s.name for s in all_schedules}
        assert {
            "drg_somatik_ingest_schedule",
            "drg_psyk_ingest_schedule",
            "drg_freshness_daily_schedule",
        } <= names

    def test_freshness_runs_after_the_ingest_window(self):
        """It reports on the share, so it must not depend on the night's loads
        having succeeded — but it should run once they would have finished."""
        assert drg_schedules.drg_freshness_daily_schedule.cron_schedule == "0 7 * * *"
        assert drg_schedules.drg_somatik_ingest_schedule.cron_schedule == "0 20 * * *"

    def test_the_queue_starts_early_enough_to_beat_the_borger_chain(self):
        """The whole queue — every takstsystem in sequence — must finish before
        ifdb_dfm_daily_schedule mints the stub Patients' keys; 20:00 gives ten
        hours. Both queues share the hour: they use separate ext tables."""
        from schedules.ifdb_dfm_schedules import ifdb_dfm_daily_schedule

        somatik = drg_schedules.drg_somatik_ingest_schedule.cron_schedule
        psyk = drg_schedules.drg_psyk_ingest_schedule.cron_schedule

        assert somatik == psyk == "0 20 * * *"
        assert ifdb_dfm_daily_schedule.cron_schedule == "0 6 * * *"
        # Evening start, so the window runs across midnight into the chain.
        assert int(somatik.split()[1]) > int(ifdb_dfm_daily_schedule.cron_schedule.split()[1])


class TestQueueSensor:
    def _sensor(self):
        from jobs.drg_jobs import (
            drg_ingest_psyk_maaned_job,
            drg_ingest_somatik_maaned_job,
            drg_ingest_somatik_uge_job,
        )
        from sensors.drg_queue_sensor import build_drg_queue_sensor

        jobs = [
            drg_ingest_somatik_maaned_job,
            drg_ingest_somatik_uge_job,
            drg_ingest_psyk_maaned_job,
        ]
        return build_drg_queue_sensor(
            ingest_jobs=jobs,
            job_for_unit={
                ("DRG", "M"): drg_ingest_somatik_maaned_job,
                ("DRG", "U"): drg_ingest_somatik_uge_job,
                ("DRGPSYK", "M"): drg_ingest_psyk_maaned_job,
            },
        )

    def test_sensor_is_constructible(self):
        """Construction is the check: a run_status_sensor validates its
        monitored and requested jobs at definition time."""
        sensor = self._sensor()
        assert sensor.name == "drg_queue_sensor"

    def test_sensor_can_request_every_unit_job(self):
        sensor = self._sensor()
        requested = {j.name for j in sensor.jobs}
        assert requested == {
            "drg_ingest_somatik_maaned_job",
            "drg_ingest_somatik_uge_job",
            "drg_ingest_psyk_maaned_job",
        }

    def test_sensor_can_request_the_weekly_job(self):
        """The queue switches jobs when it reaches its U entries, so the weekly
        job must be among the sensor's requestable jobs."""
        sensor = self._sensor()
        requested = {j.name for j in sensor.jobs}
        assert "drg_ingest_somatik_uge_job" in requested


class TestQueueFailureSensor:
    """A failed run stops the cycle: the chain advances on SUCCESS only, so
    every unit queued behind the failure is silently never requested. The
    failing run is red in the UI; the runs that never happened are an absence,
    and that is what this alarm names."""

    def _sensor(self):
        from jobs.drg_jobs import DRG_INGEST_JOBS
        from sensors.drg_queue_sensor import build_drg_queue_failure_sensor

        return build_drg_queue_failure_sensor(ingest_jobs=list(DRG_INGEST_JOBS))

    def test_sensor_is_constructible(self):
        assert self._sensor().name == "drg_queue_failure_sensor"

    def test_message_names_the_units_that_will_not_be_processed(self):
        from sensors.drg_queue_sensor import queue_halt_message

        message = queue_halt_message(
            "DRG", "T24/M", [["T25", "M"], ["T26", "M"], ["T26", "U"]], "job", "abc123"
        )
        assert "T25/M" in message and "T26/M" in message and "T26/U" in message
        assert "T24/M" in message

    def test_message_says_recovery_needs_no_action(self):
        """The next tick rebuilds the queue and everything ingested is a no-op —
        an operator should not go hunting for a retry button."""
        from sensors.drg_queue_sensor import queue_halt_message

        message = queue_halt_message("DRG", "T24/M", [["T25", "M"]], "job", "abc123")
        assert "forfra" in message

    def test_failure_on_the_last_unit_does_not_claim_units_were_skipped(self):
        from sensors.drg_queue_sensor import queue_halt_message

        message = queue_halt_message("DRG", "T26/U", [], "job", "abc123")
        assert "behandles IKKE" not in message
        assert "SIDSTE" in message
