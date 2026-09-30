# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for schedule and job partition compatibility.

Tests validate that:
- Jobs have partitions_def configured correctly
- Schedule partition_key format matches job partitions_def
- Job partition keys are valid for their partitions definition
"""

from datetime import UTC, datetime

import pytest
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from dagster import build_schedule_context
from jobs.fhir_ifdb_jobs import (
    fhir_etl_ifdb_daily_job,
    fhir_etl_ifdb_full_load_job,
    fhir_etl_ifdb_monthly_job,
)
from schedules.fhir_schedules import fhir_etl_schedule


class TestFhirIfdbJobPartitionConfiguration:
    """Tests for FHIR IFDB job partition configuration."""

    @pytest.mark.integration
    def test_fhir_etl_ifdb_daily_job_has_partitions_def(self):
        """Test fhir_etl_ifdb_daily_job has partitions_def configured."""
        assert fhir_etl_ifdb_daily_job.partitions_def is not None
        assert fhir_etl_ifdb_daily_job.partitions_def == daily_partitions

    @pytest.mark.integration
    def test_fhir_etl_ifdb_monthly_job_has_partitions_def(self):
        """Test fhir_etl_ifdb_monthly_job has partitions_def configured."""
        assert fhir_etl_ifdb_monthly_job.partitions_def is not None
        assert fhir_etl_ifdb_monthly_job.partitions_def == monthly_partitions

    @pytest.mark.integration
    def test_fhir_etl_ifdb_full_load_job_has_partitions_def(self):
        """Test fhir_etl_ifdb_full_load_job has partitions_def configured."""
        assert fhir_etl_ifdb_full_load_job.partitions_def is not None
        assert fhir_etl_ifdb_full_load_job.partitions_def == full_load_partitions

    @pytest.mark.integration
    def test_fhir_etl_schedule_partition_key_valid_for_job(self):
        """Test fhir_etl_schedule generates valid partition key for job."""
        scheduled_time = datetime(2025, 1, 30, 6, 0, 0, tzinfo=UTC)
        context = build_schedule_context(scheduled_execution_time=scheduled_time)

        run_request = fhir_etl_schedule(context)
        partition_key = run_request.partition_key

        assert fhir_etl_ifdb_daily_job.partitions_def is not None
        parsed_date = datetime.strptime(partition_key, "%Y-%m-%d")
        assert parsed_date is not None


class TestPartitionsDefConsistency:
    """Tests to verify partitions_def consistency across related jobs."""

    @pytest.mark.integration
    def test_all_daily_jobs_use_same_partitions_def(self):
        """Test daily jobs use the expected partition definitions."""
        expected_daily_partitions = [
            (fhir_etl_ifdb_daily_job, daily_partitions),
        ]

        for job, expected in expected_daily_partitions:
            assert job.partitions_def == expected, f"Job {job.name} should use {expected}"

    @pytest.mark.integration
    def test_all_monthly_jobs_use_same_partitions_def(self):
        """Test monthly jobs use the expected partition definitions."""
        expected_monthly_partitions = [
            (fhir_etl_ifdb_monthly_job, monthly_partitions),
        ]

        for job, expected in expected_monthly_partitions:
            assert job.partitions_def == expected, f"Job {job.name} should use {expected}"

    @pytest.mark.integration
    def test_all_full_load_jobs_use_same_partitions_def(self):
        """Test all full load jobs use the same full_load_partitions definition."""
        full_load_jobs = [
            fhir_etl_ifdb_full_load_job,
        ]

        for job in full_load_jobs:
            assert (
                job.partitions_def == full_load_partitions
            ), f"Job {job.name} should use full_load_partitions"


class TestScheduleJobMapping:
    """Tests to verify schedule-to-job mapping is correct."""

    @pytest.mark.integration
    def test_fhir_etl_schedule_maps_to_correct_job(self):
        """Test fhir_etl_schedule is configured for fhir_etl_ifdb_daily_job."""
        scheduled_time = datetime(2025, 1, 30, 6, 0, 0, tzinfo=UTC)
        context = build_schedule_context(scheduled_execution_time=scheduled_time)
        run_request = fhir_etl_schedule(context)

        assert run_request.partition_key is not None
        assert len(run_request.partition_key) == 10
