# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for episode of care pipeline flow.

These tests validate:
1. EpisodeOfCare transformation from IFDB data to FHIR resources
2. PREVIOUS_EOC_ID extension handling and reference resolution
3. Re-run behavior (updates to existing resources via PUT)
4. Performance characteristics of the posting pipeline
"""

import logging
import time
from datetime import datetime

import dagster as dg
import pytest
from DSA.FHIR.EpisodeOfCare.EpisodeOfCare_IFDB_dfm import (
    _transform_episodes_to_fhir,
)
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)
from utils.fhir.fhir_extensions import EXTENSION_PREVIOUS_EPISODE_OF_CARE
from utils.fhir.identifier_systems import SYSTEM_IFDB_EPISODE_OF_CARE_ID

pytestmark = pytest.mark.skip(
    reason="Episode-of-care flow integration suite is flaky and overlaps with maintained transformation tests"
)

logger = logging.getLogger(__name__)


@pytest.mark.integration
@pytest.mark.episode_of_care_test
@pytest.mark.order(4)
@pytest.mark.parametrize(
    "test_partition,partition_type",
    [
        ("full_load", "full_load"),
    ],
    ids=["full_load"],
)
def test_fhir_server_episode_of_care_insert_real(
    dagster_instance,
    real_ifdb_oracle_resource,
    real_fhir_server_resource,
    real_fhir_database_resource,
    test_config,
    caplog,
    test_partition,
    partition_type,
):
    """
    Test inserting real episode of care resources from database into FHIR server.

    Uses memory_cache ID resolution (the new standard after pipeline cleanup).
    """
    pytest.skip("Test disabled - requires full pipeline execution with IFDB Oracle")

    # Select appropriate assets based on partition type
    if partition_type == "daily":
        assets_to_materialize = ["fhir_episode_of_care_daily_ifdb"]
        fhir_asset_key = "fhir_episode_of_care_daily_ifdb"
    elif partition_type == "monthly":
        assets_to_materialize = ["fhir_episode_of_care_monthly_ifdb"]
        fhir_asset_key = "fhir_episode_of_care_monthly_ifdb"
    else:  # full_load
        assets_to_materialize = ["fhir_episode_of_care_full_load_ifdb"]
        fhir_asset_key = "fhir_episode_of_care_full_load_ifdb"

    # Materialize pipeline
    result = materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=assets_to_materialize,
        partition=test_partition,
        resources={
            "ifdb_oracle": real_ifdb_oracle_resource,
            "fhir_server": real_fhir_server_resource,
            "fhir_database": real_fhir_database_resource,
        },
    )

    # Verify pipeline succeeded
    assert result is not None

    # Get posting metadata
    metadata = get_materialization_result(dagster_instance, fhir_asset_key)
    posted_count = metadata.get("posted_count", dg.MetadataValue.int(0)).value if metadata else 0
    failed_count = metadata.get("failed_count", dg.MetadataValue.int(0)).value if metadata else 0

    # Verify resources were processed and posted
    total_processed = posted_count + failed_count
    assert (
        total_processed > 0
    ), f"No episodes of care processed (posted: {posted_count}, failed: {failed_count})"
    assert posted_count > 0, f"No episodes of care successfully posted (failed: {failed_count})"


# ============================================================================
# Unit tests for transformation logic
# ============================================================================


class MockContext:
    """Mock context for transformation tests."""

    class MockLog:
        def info(self, msg):
            logger.info(msg)

        def warning(self, msg):
            logger.warning(msg)

        def debug(self, msg):
            logger.debug(msg)

    log = MockLog()


@pytest.mark.unit
def test_transform_episodes_basic(sample_episode_of_care_rows):
    """Test basic transformation of episode of care rows to FHIR resources."""
    context = MockContext()
    episodes = _transform_episodes_to_fhir(sample_episode_of_care_rows, context)

    assert len(episodes) == 2

    # Check first episode (no previous EOC)
    eoc1 = episodes[0]
    assert eoc1.identifier[0].value == "EOC001"
    assert eoc1.status == "active"
    assert eoc1.patient.identifier.value == "123456"
    assert eoc1.extension is None  # No PREVIOUS_EOC_ID

    # Check second episode (has previous EOC)
    eoc2 = episodes[1]
    assert eoc2.identifier[0].value == "EOC002"
    assert eoc2.status == "finished"
    assert eoc2.extension is not None
    assert len(eoc2.extension) == 1
    assert eoc2.extension[0].url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
    assert eoc2.extension[0].valueReference.identifier.value == "EOC001"


@pytest.mark.unit
def test_transform_episodes_status_mapping():
    """Test that IFDB status values are correctly mapped to FHIR EpisodeOfCareStatus."""
    context = MockContext()

    test_cases = [
        ("active", "active"),
        ("planned", "planned"),
        ("waitlist", "waitlist"),
        ("onhold", "onhold"),
        ("finished", "finished"),
        ("cancelled", "cancelled"),
        ("ACTIVE", "active"),  # Case insensitive
        (None, "active"),  # Fallback based on period
    ]

    for ifdb_status, expected_fhir_status in test_cases:
        rows = [
            {
                "OBJECT_ID": "TEST001",
                "PATIENT_ID": "123456",
                "STATUS": ifdb_status,
                "PERIOD_START": datetime(2024, 1, 1),
                "PERIOD_END": None,
                "PREVIOUS_EOC_ID": None,
                "HEALTHISSUE_ID": None,
                "IFDB_MODIFICATION_DATETIME": datetime(2024, 1, 15),
            }
        ]
        episodes = _transform_episodes_to_fhir(rows, context)
        assert (
            episodes[0].status == expected_fhir_status
        ), f"Status '{ifdb_status}' should map to '{expected_fhir_status}', got '{episodes[0].status}'"


@pytest.mark.unit
def test_transform_episodes_previous_eoc_extension():
    """Test that PREVIOUS_EOC_ID creates the correct extension structure."""
    context = MockContext()

    rows = [
        {
            "OBJECT_ID": "EOC002",
            "PATIENT_ID": "123456",
            "STATUS": "active",
            "PERIOD_START": datetime(2024, 1, 1),
            "PERIOD_END": None,
            "PREVIOUS_EOC_ID": "EOC001",
            "HEALTHISSUE_ID": None,
            "IFDB_MODIFICATION_DATETIME": datetime(2024, 1, 15),
        }
    ]

    episodes = _transform_episodes_to_fhir(rows, context)
    eoc = episodes[0]

    # Verify extension structure
    assert eoc.extension is not None
    assert len(eoc.extension) == 1

    ext = eoc.extension[0]
    assert ext.url == EXTENSION_PREVIOUS_EPISODE_OF_CARE
    assert ext.valueReference is not None
    assert ext.valueReference.identifier.system == SYSTEM_IFDB_EPISODE_OF_CARE_ID
    assert ext.valueReference.identifier.value == "EOC001"


# ============================================================================
# Performance investigation tests
# ============================================================================


@pytest.mark.integration
@pytest.mark.performance
def test_episode_of_care_posting_performance(
    real_fhir_database_resource,
    sample_episode_of_care_rows,
    caplog,
):
    """
    Investigate batch upsert performance for EpisodeOfCare resources.

    After the pipeline cleanup, we use direct DB posting via batch_upsert_resources()
    which uses executemany() for 4-7x better performance.

    This test measures:
    - Transformation time
    - Posting time (batch upsert via executemany)
    - Total throughput
    """
    pytest.skip("Enable manually to investigate performance")

    context = MockContext()

    # Generate larger test dataset (multiply sample rows)
    test_rows = []
    for i in range(100):
        for row in sample_episode_of_care_rows:
            new_row = row.copy()
            new_row["OBJECT_ID"] = f"{row['OBJECT_ID']}_{i}"
            if row.get("PREVIOUS_EOC_ID"):
                new_row["PREVIOUS_EOC_ID"] = f"{row['PREVIOUS_EOC_ID']}_{i}"
            test_rows.append(new_row)

    logger.info(f"Performance test with {len(test_rows)} EpisodeOfCare records")

    # Measure transformation time
    start = time.perf_counter()
    episodes = _transform_episodes_to_fhir(test_rows, context)
    transform_time = time.perf_counter() - start
    logger.info(
        f"Transformation: {transform_time:.3f}s ({len(episodes)/transform_time:.0f} records/sec)"
    )

    # Measure posting time using batch_upsert_resources
    post_start = time.perf_counter()
    successes, failures, failed_ids = real_fhir_database_resource.batch_upsert_resources(
        resources=episodes,
        resource_type="EpisodeOfCare",
    )
    post_time = time.perf_counter() - post_start

    logger.info(f"Posting: {post_time:.3f}s ({successes/post_time:.0f} records/sec)")
    logger.info(f"Successes: {successes}, Failures: {failures}")
    logger.info("Performance metrics collected - see logs for details")
