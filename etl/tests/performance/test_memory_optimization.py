# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Memory optimization tests for targeted preload ID resolution.

This test suite validates and benchmarks the memory optimization improvements
for the targeted preload (SQL-level filtering) strategy.

Test scenarios:
- 100,000 resources with references
- Validation that SQL-level filtering reduces cache size
- Memory footprint validation
- Correctness validation

Prerequisites:
    1. FHIR database with seeded test data
    2. Environment: USE_REAL_DB_FOR_TESTS=true

Usage:
    # Run all tests
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_memory_optimization.py -v -s

    # Run specific test
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_memory_optimization.py::TestMemoryOptimization::test_100k_resources_targeted_preload -v -s

    # Quick validation (smaller dataset)
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_memory_optimization.py::TestMemoryOptimization::test_correctness_validation -v -s
"""

import gc
import logging
import os
import time
import tracemalloc
import uuid
from dataclasses import dataclass
from typing import Any

import psutil
import pytest
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.reference_resolver import batch_resolve_references
from fhir.resources.R4B.episodeofcare import EpisodeOfCare
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.reference import Reference

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_PERFORMANCE_TESTS", "false").lower() != "true",
    reason="Performance tests are opt-in (set RUN_PERFORMANCE_TESTS=true)",
)

# =============================================================================
# Test Configuration
# =============================================================================

# Identifier system URIs used in tests
SYSTEM_CPR = "urn:dk:cpr"
SYSTEM_PATIENT_ID = "urn:region-syddanmark:ifdb:patient-id"
SYSTEM_EPISODE_OF_CARE_ID = "urn:region-syddanmark:ifdb:episode-of-care-id"


# =============================================================================
# Performance Measurement Utilities
# =============================================================================


@dataclass
class MemoryStats:
    """Container for memory measurement results."""

    strategy: str
    data_size: int
    memory_before_mb: float
    memory_peak_mb: float
    memory_after_mb: float
    duration_seconds: float
    resolved_count: int
    unresolved_count: int

    @property
    def memory_delta_mb(self) -> float:
        """Memory increase during operation (final - initial)."""
        return self.memory_after_mb - self.memory_before_mb

    @property
    def peak_memory_delta_mb(self) -> float:
        """Peak memory usage relative to start."""
        return self.memory_peak_mb - self.memory_before_mb

    @property
    def resolution_rate(self) -> float:
        """Resolutions per second."""
        return self.resolved_count / self.duration_seconds if self.duration_seconds > 0 else 0

    @property
    def memory_per_resolution_kb(self) -> float:
        """Peak memory usage per resolution (KB per resolution)."""
        return (
            (self.peak_memory_delta_mb * 1024) / self.resolved_count
            if self.resolved_count > 0
            else float("inf")
        )

    def __str__(self) -> str:
        return (
            f"{self.strategy} ({self.data_size:,} items): "
            f"Peak: +{self.peak_memory_delta_mb:.1f}MB, "
            f"Duration: {self.duration_seconds:.2f}s, "
            f"Rate: {self.resolution_rate:.0f}/sec, "
            f"Resolved: {self.resolved_count:,}/{self.data_size:,}"
        )


def measure_memory(
    strategy: str,
    data_size: int,
    func,
    *args,
    **kwargs,
) -> tuple[MemoryStats, Any]:
    """
    Measure memory and performance of a function execution.

    Args:
        strategy: Name of the strategy being tested
        data_size: Size of dataset being processed
        func: Function to measure
        *args, **kwargs: Arguments to pass to function

    Returns:
        Tuple of (MemoryStats, function result)
    """
    # Force garbage collection before starting
    gc.collect()

    # Get initial memory
    process = psutil.Process()
    memory_before = process.memory_info().rss / 1024 / 1024

    # Start tracemalloc for detailed tracking
    tracemalloc.start()

    start_time = time.perf_counter()

    try:
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start_time

        # Get memory stats
        current, peak = tracemalloc.get_traced_memory()
        memory_after = process.memory_info().rss / 1024 / 1024

        # Convert tracemalloc peak to MB and combine with RSS
        peak_mb = max(peak / 1024 / 1024, memory_after - memory_before)

        # Extract resolution counts from result
        resolved_count = len(result) if isinstance(result, dict) else 0
        unresolved_count = data_size - resolved_count

        stats = MemoryStats(
            strategy=strategy,
            data_size=data_size,
            memory_before_mb=memory_before,
            memory_peak_mb=memory_before + peak_mb,
            memory_after_mb=memory_after,
            duration_seconds=duration,
            resolved_count=resolved_count,
            unresolved_count=unresolved_count,
        )

        return stats, result

    finally:
        tracemalloc.stop()


def clear_caches(fhir_database: FHIRDatabaseResource) -> None:
    """Clear all caches and force garbage collection."""
    if hasattr(fhir_database, "_identifier_cache"):
        fhir_database._identifier_cache = None
    gc.collect()


# =============================================================================
# Test Data Management
# =============================================================================


def seed_patients(fhir_database: FHIRDatabaseResource, count: int) -> list[str]:
    """
    Seed test Patient resources into the FHIR database.

    Returns list of patient identifier values that were created.
    """
    logger.info(f"Seeding {count:,} Patient resources...")

    patients = []
    patient_identifiers = []

    for i in range(count):
        patient_id = f"test-patient-{i:08d}"
        cpr = f"{1000000000 + i}"  # Synthetic CPR

        patient = Patient(
            id=str(uuid.uuid4()),
            identifier=[
                Identifier(system=SYSTEM_PATIENT_ID, value=patient_id),
                Identifier(system=SYSTEM_CPR, value=cpr),
            ],
            active=True,
        )
        patients.append(patient)
        patient_identifiers.append(patient_id)

    # Batch upsert
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=patients,
        resource_type="Patient",
    )

    logger.info(f"Seeded {successes:,} patients ({failures} failures)")
    return patient_identifiers


def seed_episodes_with_patient_refs(
    fhir_database: FHIRDatabaseResource,
    patient_identifiers: list[str],
) -> tuple[list[str], dict[str, str]]:
    """
    Seed test EpisodeOfCare resources with Patient references.

    Returns:
        - List of episode identifier values
        - Dict mapping episode_id -> patient_id (expected references)
    """
    count = len(patient_identifiers)
    logger.info(f"Seeding {count:,} EpisodeOfCare resources with Patient references...")

    episodes = []
    episode_identifiers = []
    expected_refs: dict[str, str] = {}

    for i, patient_id in enumerate(patient_identifiers):
        episode_id = f"test-episode-{i:08d}"

        episode = EpisodeOfCare(
            id=str(uuid.uuid4()),
            status="active",
            identifier=[
                Identifier(system=SYSTEM_EPISODE_OF_CARE_ID, value=episode_id),
            ],
            patient=Reference(
                identifier=Identifier(system=SYSTEM_PATIENT_ID, value=patient_id),
                type="Patient",
            ),
        )
        episodes.append(episode)
        episode_identifiers.append(episode_id)
        expected_refs[episode_id] = patient_id

    # Batch upsert
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=episodes,
        resource_type="EpisodeOfCare",
    )

    logger.info(f"Seeded {successes:,} episodes ({failures} failures)")
    return episode_identifiers, expected_refs


def cleanup_test_data(fhir_database: FHIRDatabaseResource) -> None:
    """Remove all test data from the FHIR database."""
    logger.info("Cleaning up test data...")

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            # Delete test episodes first (foreign key to patient)
            cur.execute(
                """
                DELETE FROM episodeofcare
                WHERE resource->'identifier' @> '[{"system": "urn:region-syddanmark:ifdb:episode-of-care-id"}]'
                AND resource->'identifier'->>0 LIKE '%test-episode%'
            """
            )
            episode_count = cur.rowcount

            # Delete test patients
            cur.execute(
                """
                DELETE FROM patient
                WHERE resource->'identifier' @> '[{"system": "urn:region-syddanmark:ifdb:patient-id"}]'
                AND resource->'identifier'->>0 LIKE '%test-patient%'
            """
            )
            patient_count = cur.rowcount

            conn.commit()

    logger.info(f"Cleaned up {patient_count:,} patients and {episode_count:,} episodes")


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def fhir_database(real_fhir_database_resource) -> FHIRDatabaseResource:
    """Get the real FHIR database resource."""
    return real_fhir_database_resource


@pytest.fixture
def seeded_test_data(fhir_database: FHIRDatabaseResource):
    """
    Seed test data and return identifiers for testing.

    This fixture seeds:
    - 100,000 Patient resources
    - 100,000 EpisodeOfCare resources (with patient references)

    Data is cleaned up after all tests complete.
    """
    test_size = 100_000

    # Clean up any previous test data
    cleanup_test_data(fhir_database)

    # Seed patients
    patient_ids = seed_patients(fhir_database, test_size)

    # Seed episodes with patient references
    episode_ids, expected_refs = seed_episodes_with_patient_refs(fhir_database, patient_ids)

    yield {
        "patient_ids": patient_ids,
        "episode_ids": episode_ids,
        "expected_refs": expected_refs,
        "test_size": test_size,
    }

    # Cleanup after tests
    cleanup_test_data(fhir_database)


@pytest.fixture
def small_test_data(fhir_database: FHIRDatabaseResource):
    """
    Seed smaller test data for quick validation tests.

    Seeds 1,000 resources for fast correctness validation.
    """
    test_size = 1_000

    # Clean up any previous test data
    cleanup_test_data(fhir_database)

    # Seed patients
    patient_ids = seed_patients(fhir_database, test_size)

    # Seed episodes with patient references
    episode_ids, expected_refs = seed_episodes_with_patient_refs(fhir_database, patient_ids)

    yield {
        "patient_ids": patient_ids,
        "episode_ids": episode_ids,
        "expected_refs": expected_refs,
        "test_size": test_size,
    }

    # Cleanup after tests
    cleanup_test_data(fhir_database)


# =============================================================================
# Test Classes
# =============================================================================


@pytest.mark.performance
@pytest.mark.slow
class TestMemoryOptimization:
    """Test memory optimization improvements for ID resolution."""

    def test_targeted_preload_filters_at_sql_level(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """
        Validate that targeted preload filters at SQL level, not in Python.

        The optimized preload_identifier_cache should:
        1. Only fetch identifiers for the specified systems
        2. Result in smaller cache size when filtering by system
        """
        test_size = small_test_data["test_size"]
        logger.info(f"Testing targeted preload with {test_size:,} resources")

        # Clear any existing cache
        clear_caches(fhir_database)

        # Preload with specific system filter
        start = time.perf_counter()
        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=[SYSTEM_PATIENT_ID],
        )
        filtered_duration = time.perf_counter() - start

        filtered_cache_size = len(fhir_database._identifier_cache)
        logger.info(
            f"Filtered preload: {filtered_cache_size:,} entries in {filtered_duration:.2f}s"
        )

        # Verify cache contains only the requested system
        for key in list(fhir_database._identifier_cache.keys())[:10]:
            system, value = key
            assert system == SYSTEM_PATIENT_ID, f"Unexpected system in cache: {system}"

        # Clear cache
        clear_caches(fhir_database)

        # Preload without filter (all systems)
        start = time.perf_counter()
        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=None,
        )
        unfiltered_duration = time.perf_counter() - start

        unfiltered_cache_size = len(fhir_database._identifier_cache)
        logger.info(
            f"Unfiltered preload: {unfiltered_cache_size:,} entries in {unfiltered_duration:.2f}s"
        )

        # Filtered cache should have fewer entries (only one system instead of two)
        # Each patient has 2 identifiers (SYSTEM_PATIENT_ID and SYSTEM_CPR)
        assert filtered_cache_size < unfiltered_cache_size, (
            f"Filtered cache ({filtered_cache_size}) should be smaller than "
            f"unfiltered cache ({unfiltered_cache_size})"
        )

        # Filtered should be approximately half the size (1 system vs 2)
        expected_ratio = 0.6  # Allow some tolerance
        actual_ratio = filtered_cache_size / unfiltered_cache_size
        logger.info(f"Cache size ratio (filtered/unfiltered): {actual_ratio:.2f}")
        assert actual_ratio < expected_ratio, (
            f"Filtered cache should be significantly smaller. "
            f"Ratio: {actual_ratio:.2f}, expected < {expected_ratio}"
        )

    def test_correctness_validation(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """
        Validate that targeted preload correctly resolves all identifiers.

        This is the critical correctness test - the optimized preload
        must resolve the same identifiers as before.
        """
        patient_ids = small_test_data["patient_ids"]
        test_size = small_test_data["test_size"]

        logger.info(f"Testing correctness with {test_size:,} resources")

        # Create identifier dict for resolution
        identifiers = {pid: "" for pid in patient_ids}

        # Test memory_cache strategy with targeted preload
        clear_caches(fhir_database)
        result = batch_resolve_references(
            identifiers=identifiers,
            resource_type="Patient",
            identifier_system=SYSTEM_PATIENT_ID,
            fhir_database=fhir_database,
            id_resolution_strategy="memory_cache",
        )

        # Verify all identifiers were resolved
        assert (
            len(result) == test_size
        ), f"Expected all {test_size:,} identifiers to be resolved, got {len(result):,}"

        # Verify each patient_id maps to a valid FHIR ID (UUID)
        for patient_id, fhir_id in result.items():
            assert patient_id.startswith("test-patient-"), f"Unexpected patient_id: {patient_id}"
            assert fhir_id is not None, f"FHIR ID is None for {patient_id}"
            assert len(fhir_id) == 36, f"FHIR ID doesn't look like UUID: {fhir_id}"

        logger.info(f"Correctness validated: resolved {len(result):,}/{test_size:,} identifiers")

    def test_100k_resources_targeted_preload(
        self,
        fhir_database: FHIRDatabaseResource,
        seeded_test_data: dict,
    ):
        """
        Benchmark targeted preload with 100,000 resources.

        This is the main benchmark test that validates the memory improvements.
        """
        patient_ids = seeded_test_data["patient_ids"]
        test_size = seeded_test_data["test_size"]

        logger.info(f"\n{'='*60}")
        logger.info(f"Targeted preload benchmark with {test_size:,} resources")
        logger.info(f"{'='*60}")

        # Create identifier dict for resolution
        identifiers = {pid: "" for pid in patient_ids}

        # Test with targeted preload (filtered)
        clear_caches(fhir_database)
        logger.info("\n--- Testing targeted preload (filtered) ---")

        stats, result = measure_memory(
            strategy="targeted_preload",
            data_size=test_size,
            func=batch_resolve_references,
            identifiers=identifiers,
            resource_type="Patient",
            identifier_system=SYSTEM_PATIENT_ID,
            fhir_database=fhir_database,
            id_resolution_strategy="memory_cache",
        )

        logger.info(f"  {stats}")

        # Verify all identifiers were resolved
        assert (
            len(result) == test_size
        ), f"Expected all {test_size:,} identifiers to be resolved, got {len(result):,}"

        # Report results
        logger.info(f"\n{'='*60}")
        logger.info("RESULTS SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"  Data size:     {test_size:,} resources")
        logger.info(f"  Peak memory:   +{stats.peak_memory_delta_mb:.1f}MB")
        logger.info(f"  Duration:      {stats.duration_seconds:.2f}s")
        logger.info(f"  Rate:          {stats.resolution_rate:.0f} resolutions/sec")
        logger.info(f"  Resolved:      {stats.resolved_count:,}/{test_size:,}")

        logger.info(f"\n{'='*60}")
        logger.info("TEST PASSED")
        logger.info(f"{'='*60}")

    def test_skip_strategy(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """Test that skip strategy returns empty results without database access."""
        patient_ids = small_test_data["patient_ids"]

        identifiers = {pid: "" for pid in patient_ids[:100]}

        clear_caches(fhir_database)
        result = batch_resolve_references(
            identifiers=identifiers,
            resource_type="Patient",
            identifier_system=SYSTEM_PATIENT_ID,
            fhir_database=fhir_database,
            id_resolution_strategy="skip",
        )

        assert len(result) == 0, "Skip strategy should return empty results"
        assert fhir_database._identifier_cache is None, "Skip strategy should not populate cache"

        logger.info("Skip strategy correctly returned empty results")


@pytest.mark.performance
class TestTargetedPreloadOptimization:
    """Tests specifically for the targeted preload optimization."""

    def test_preload_with_single_system(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """Test preload with a single identifier system."""
        clear_caches(fhir_database)

        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=[SYSTEM_PATIENT_ID],
        )

        cache = fhir_database._identifier_cache
        assert cache is not None
        assert len(cache) > 0

        # Verify all entries have the correct system
        for key in cache:
            system, value = key
            assert system == SYSTEM_PATIENT_ID

    def test_preload_with_multiple_systems(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """Test preload with multiple identifier systems."""
        clear_caches(fhir_database)

        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=[SYSTEM_PATIENT_ID, SYSTEM_CPR],
        )

        cache = fhir_database._identifier_cache
        assert cache is not None
        assert len(cache) > 0

        # Verify entries have one of the allowed systems
        allowed_systems = {SYSTEM_PATIENT_ID, SYSTEM_CPR}
        for key in cache:
            system, value = key
            assert system in allowed_systems

    def test_preload_without_filter(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """Test preload without system filter (loads all)."""
        clear_caches(fhir_database)

        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=None,
        )

        cache = fhir_database._identifier_cache
        assert cache is not None
        assert len(cache) > 0

    def test_cache_memory_efficiency(
        self,
        fhir_database: FHIRDatabaseResource,
        small_test_data: dict,
    ):
        """
        Test that filtered cache uses less memory than unfiltered.
        """
        small_test_data["test_size"]

        # Measure filtered cache memory
        clear_caches(fhir_database)
        gc.collect()

        tracemalloc.start()
        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=[SYSTEM_PATIENT_ID],
        )
        filtered_current, filtered_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        filtered_cache_size = len(fhir_database._identifier_cache)

        # Measure unfiltered cache memory
        clear_caches(fhir_database)
        gc.collect()

        tracemalloc.start()
        fhir_database.preload_identifier_cache(
            resource_type="Patient",
            identifier_systems=None,
        )
        unfiltered_current, unfiltered_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        unfiltered_cache_size = len(fhir_database._identifier_cache)

        # Log results
        logger.info(
            f"Filtered cache: {filtered_cache_size:,} entries, peak: {filtered_peak / 1024 / 1024:.2f}MB"
        )
        logger.info(
            f"Unfiltered cache: {unfiltered_cache_size:,} entries, peak: {unfiltered_peak / 1024 / 1024:.2f}MB"
        )

        # Filtered should use less memory
        assert filtered_peak < unfiltered_peak, (
            f"Filtered cache should use less memory. "
            f"Filtered: {filtered_peak / 1024 / 1024:.2f}MB, "
            f"Unfiltered: {unfiltered_peak / 1024 / 1024:.2f}MB"
        )

        memory_reduction = (1 - filtered_peak / unfiltered_peak) * 100
        logger.info(f"Memory reduction: {memory_reduction:.1f}%")
