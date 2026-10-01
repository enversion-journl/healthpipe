# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for schedule definitions.

Tests validate that:
- Schedules return RunRequest objects with correct partition_key format
- Schedules include expected tags
- Partition key format matches job partition definitions
"""

from datetime import UTC, datetime

import pytest
from dagster import build_schedule_context
from schedules.fhir_schedules import fhir_etl_schedule


class TestFhirEtlSchedule:
    """Unit tests for fhir_etl_schedule (IFDB FHIR ETL)."""

    @pytest.mark.unit
    def test_returns_run_request_with_partition_key(self):
        """Test schedule returns RunRequest with partition_key in YYYY-MM-DD format."""
        scheduled_time = datetime(2025, 1, 30, 6, 0, 0, tzinfo=UTC)
        context = build_schedule_context(scheduled_execution_time=scheduled_time)

        run_request = fhir_etl_schedule(context)

        assert run_request is not None
        assert run_request.partition_key == "2025-01-30"

    @pytest.mark.unit
    def test_partition_key_format_matches_daily_partitions(self):
        """Test partition_key format is compatible with daily_partitions definition."""
        scheduled_time = datetime(2025, 12, 25, 6, 0, 0, tzinfo=UTC)
        context = build_schedule_context(scheduled_execution_time=scheduled_time)

        run_request = fhir_etl_schedule(context)

        partition_key = run_request.partition_key
        assert len(partition_key) == 10
        assert partition_key.count("-") == 2
        parsed_date = datetime.strptime(partition_key, "%Y-%m-%d")
        assert parsed_date.year == 2025
        assert parsed_date.month == 12
        assert parsed_date.day == 25

    @pytest.mark.unit
    def test_includes_extraction_mode_tag(self):
        """Test schedule includes ifdb_extraction_mode=scheduled_catchup tag."""
        scheduled_time = datetime(2025, 1, 30, 6, 0, 0, tzinfo=UTC)
        context = build_schedule_context(scheduled_execution_time=scheduled_time)

        run_request = fhir_etl_schedule(context)

        assert run_request.tags is not None
        assert "ifdb_extraction_mode" in run_request.tags
        assert run_request.tags["ifdb_extraction_mode"] == "scheduled_catchup"


class TestSchedulePartitionKeyEdgeCases:
    """Test partition key edge cases across all schedules."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "schedule_fn,scheduled_time,expected_partition_key",
        [
            # Test first day of year
            (
                fhir_etl_schedule,
                datetime(2025, 1, 1, 6, 0, 0, tzinfo=UTC),
                "2025-01-01",
            ),
            # Test last day of year
            (
                fhir_etl_schedule,
                datetime(2025, 12, 31, 6, 0, 0, tzinfo=UTC),
                "2025-12-31",
            ),
            # Test leap year (Feb 29)
            (
                fhir_etl_schedule,
                datetime(2024, 2, 29, 6, 0, 0, tzinfo=UTC),
                "2024-02-29",
            ),
        ],
    )
    def test_partition_key_edge_cases(self, schedule_fn, scheduled_time, expected_partition_key):
        """Test partition key is correct for edge case dates."""
        context = build_schedule_context(scheduled_execution_time=scheduled_time)

        run_request = schedule_fn(context)

        assert run_request.partition_key == expected_partition_key
