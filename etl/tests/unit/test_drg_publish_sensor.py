# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Tests for the DRG publish sensor.

Run before ``ifdb_dfm_daily_job``, the publish resolves CPRs against a
dim.Borger without the night's stub patients and ships dw_ek_Borger = -1 to
DSO. The sensor is deliberately ungated (see its module docstring), and these
tests pin that it keeps firing every night.
"""

import pytest
from sensors.drg_publish_sensor import SENSOR_NAME, build_run_request

pytestmark = pytest.mark.unit

PUBLISH_JOB = "drg_publish_job"


def test_request_targets_the_publish_job():
    request = build_run_request(PUBLISH_JOB, "run-abc")

    assert request.job_name == PUBLISH_JOB
    assert request.tags["dagster/triggered_by"] == SENSOR_NAME
    assert request.tags["dagster/parent_run_id"] == "run-abc"


def test_one_publish_per_nightly_success():
    """The run_key dedupes a retried tick for the SAME success..."""
    assert build_run_request(PUBLISH_JOB, "run-abc").run_key == (
        build_run_request(PUBLISH_JOB, "run-abc").run_key
    )


def test_a_later_night_publishes_again():
    """...but must NOT dedupe across nights: the nightly re-run is what
    re-resolves contacts left at dw_ek_Borger = -1."""
    first = build_run_request(PUBLISH_JOB, "run-1").run_key
    second = build_run_request(PUBLISH_JOB, "run-2").run_key

    assert first != second


def test_sensor_is_registered_and_requests_the_publish_job():
    """A sensor that is built but never added to postfixed_sensors would
    silently restore the untriggered state this replaced."""
    from definitions import drg_publish_sensor, postfixed_sensors

    assert drg_publish_sensor in postfixed_sensors
    assert [t.job_name for t in drg_publish_sensor.targets] == [PUBLISH_JOB]


def test_sensor_reads_no_database():
    """Ungated means stateless: a resource requirement would mean a gate had
    been reintroduced."""
    from definitions import drg_publish_sensor

    assert drg_publish_sensor.required_resource_keys == frozenset()


def test_sensor_waits_for_the_borger_chain():
    """Nothing else enforces the order — drg_publish_job does not materialize
    dim_Borger. The monitored job is not exposed on the definition, so this
    pins the description the factory builds from ``borger_chain_job.name``."""
    from definitions import drg_publish_sensor

    assert "ifdb_dfm_daily_job SUCCESS" in drg_publish_sensor.description


def test_publish_job_still_has_no_schedule_of_its_own():
    """The sensor is the trigger. A cron alongside it would race the Borger
    chain again, which is the bug this replaced."""
    from schedules import all_schedules

    for schedule in all_schedules:
        assert "drg_publish" not in schedule.name
