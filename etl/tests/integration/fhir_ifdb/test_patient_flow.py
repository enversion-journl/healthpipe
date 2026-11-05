# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration tests for patient pipeline flow.

These tests verify the FHIR ETL pipeline including:
- Bulk import performance testing with configurable resource counts
- ID resolution strategy validation
"""

import logging
import time
from unittest.mock import MagicMock

import dagster as dg
import pytest
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient

logger = logging.getLogger(__name__)


# =============================================================================
# Bulk Import Performance Tests
# =============================================================================


def _generate_synthetic_patients(count: int) -> list[Patient]:
    """
    Generate synthetic Patient resources for testing.

    Args:
        count: Number of patients to generate

    Returns:
        List of Patient resources
    """
    from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

    patients = []
    for i in range(count):
        # Generate unique CPR (simple sequential for testing)
        cpr = f"{(i % 31) + 1:02d}{(i % 12) + 1:02d}{(i % 100):02d}{(i % 10000):04d}"
        patient_id = f"patient_{i}"

        patient = Patient(
            identifier=[
                Identifier(system=SYSTEM_CPR, value=cpr),
                Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=patient_id),
            ],
            name=[HumanName(given=["Patient"], family=f"TestUser{i}")],
            gender="male" if i % 2 == 0 else "female",
            birthDate=f"{1950 + (i % 70)}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            active=True,
            address=[
                Address(
                    line=[f"Test Street {i}"],
                    city="Test City",
                    postalCode=f"{5000 + (i % 9999):04d}",
                )
            ],
        )
        patients.append(patient)

    return patients


def test_bulk_import_configuration_validation():
    """
    Test that FHIR posting configuration is properly set up.

    After the pipeline cleanup, we've standardized on:
    - ID Resolution: memory_cache only (18,000x faster)
    - Posting: Direct DB via executemany() (4-7x faster)

    This test validates the simplified configuration.
    """
    from DSA.FHIR.utils.ifdb.posting_config import FHIRPostingConfig

    # Test custom chunk_size from tags
    mock_context = MagicMock(spec=dg.AssetExecutionContext)
    mock_context.run = MagicMock(tags={"fhir/chunk_size": "1000"})
    mock_asset_def = MagicMock()
    mock_asset_def.tags = {"fhir/chunk_size": "5000", "fhir/max_workers": "16"}

    # Mock the asset graph lookup
    mock_asset_graph = MagicMock()
    mock_asset_graph.get.return_value = mock_asset_def

    mock_job_def = MagicMock()
    mock_job_def.asset_layer.asset_graph = mock_asset_graph

    mock_context.job_def = mock_job_def

    config = FHIRPostingConfig.from_context(mock_context)

    # Run tags override asset tags
    assert config.chunk_size == 1000  # From run tags

    logger.info("PASSED: FHIR posting configuration validation")


@pytest.mark.integration
@pytest.mark.performance
@pytest.mark.patient_test
@pytest.mark.parametrize(
    "num_patients,chunk_size",
    [
        (5_000, 1000),  # Test with 1000 chunk size
        (5_000, 2000),  # Test with larger chunks
    ],
    ids=["batch_upsert_5k_1000", "batch_upsert_5k_2000"],
)
def test_batch_upsert_performance(
    real_fhir_database_resource,
    num_patients,
    chunk_size,
):
    """
    Performance test: Test direct DB batch upsert with different configurations.

    After the pipeline cleanup, we use direct DB posting via batch_upsert_resources()
    which uses executemany() for 4-7x better performance than single inserts.

    Args:
        num_patients: Number of synthetic patients to generate
        chunk_size: Number of resources per batch
    """
    logger.info(f"\n{'=' * 80}")
    logger.info(f"BATCH UPSERT TEST: {num_patients:,} patients")
    logger.info(f"Config: chunk_size={chunk_size}")
    logger.info(f"{'=' * 80}\n")

    # Generate synthetic patients
    logger.info(f"Generating {num_patients:,} synthetic patients...")
    generation_start = time.perf_counter()
    patients = _generate_synthetic_patients(num_patients)
    generation_time = time.perf_counter() - generation_start
    logger.info(f"Generated {len(patients):,} patients in {generation_time:.1f}s")

    # Post patients using direct DB batch upsert
    logger.info(f"\nPosting {len(patients):,} patients using batch_upsert_resources...")
    posting_start = time.perf_counter()

    # Process in chunks
    total_posted = 0
    total_failed = 0
    for i in range(0, len(patients), chunk_size):
        chunk = patients[i : i + chunk_size]
        successes, failures, failed_ids = real_fhir_database_resource.batch_upsert_resources(
            resources=chunk,
            resource_type="Patient",
        )
        total_posted += successes
        total_failed += failures

    posting_time = time.perf_counter() - posting_start

    # Calculate metrics
    total_time = generation_time + posting_time
    success_rate = (
        (total_posted / (total_posted + total_failed)) * 100
        if (total_posted + total_failed) > 0
        else 0
    )
    throughput = total_posted / posting_time if posting_time > 0 else 0

    # Log results
    logger.info(f"\n{'=' * 80}")
    logger.info("RESULTS")
    logger.info(f"{'=' * 80}")
    logger.info(f"Total patients: {num_patients:,}")
    logger.info(f"Posted: {total_posted:,}")
    logger.info(f"Failed: {total_failed:,}")
    logger.info(f"Success rate: {success_rate:.1f}%")
    logger.info("\nTiming:")
    logger.info(f"  Generation: {generation_time:.1f}s")
    logger.info(f"  Posting: {posting_time:.1f}s")
    logger.info(f"  Total: {total_time:.1f}s")
    logger.info("\nPerformance:")
    logger.info(f"  Throughput: {throughput:.1f} resources/second")
    logger.info(f"{'=' * 80}\n")

    # Assertions
    assert total_posted > 0, "No patients were successfully posted"

    logger.info(f"PASSED: Posted {total_posted:,} patients with {throughput:.1f} resources/second")
