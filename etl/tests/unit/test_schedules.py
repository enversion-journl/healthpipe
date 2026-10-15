# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for the schedule definitions.

The IFDB → FHIR pipeline is one unpartitioned job on one schedule, so what is
worth pinning is the trigger contract: which job, when, in which timezone. The
partitioned ``fhir_etl_schedule`` helper these tests used to exercise is gone
along with the partitioned daily job.
"""

import pytest
from schedules import all_schedules
from schedules.ifdb_dfm_schedules import ifdb_dfm_daily_schedule


@pytest.mark.unit
class TestIfdbDfmDailySchedule:
    """The nightly IFDB → FHIR trigger."""

    def test_targets_the_unified_pipeline_job(self):
        assert ifdb_dfm_daily_schedule.job.name == "ifdb_dfm_daily_job"

    def test_runs_at_0600_copenhagen(self):
        # 06:00 CET, after haiba_ingest_job at 05:00 — the stub patients it posts
        # must be in Aidbox before the Borger chain projects them.
        assert ifdb_dfm_daily_schedule.cron_schedule == "0 6 * * *"
        assert ifdb_dfm_daily_schedule.execution_timezone == "Europe/Copenhagen"

    def test_is_registered(self):
        assert ifdb_dfm_daily_schedule in all_schedules

    def test_is_unpartitioned(self):
        # A partitioned job would need a partition key per run; the whole point
        # of the DFM migration is that no producer needs one any more.
        assert ifdb_dfm_daily_schedule.job.partitions_def is None
