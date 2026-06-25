# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Memory profiling tests comparing ID resolution strategies.

Focus: Memory Cache vs Direct PostgreSQL - measuring memory footprint vs speed trade-off.

These tests measure:
- Memory usage of memory_cache vs direct_postgres strategies
- Performance impact of each approach
- Memory vs speed trade-off analysis

Prerequisites:
    1. Oracle IFDB with test data
    2. FHIR server running
    3. Environment: USE_REAL_DB_FOR_TESTS=true

Usage:
    # Run all tests
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_memory_cache_vs_postgres.py -v -s

    # Run specific test
    pytest tests/performance/test_memory_cache_vs_postgres.py::TestIDResolutionStrategies::test_memory_vs_speed_tradeoff -v -s
"""

import gc
import logging
import os
import time
import tracemalloc
from dataclasses import dataclass

import psutil
import pytest
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.reference_resolver import batch_resolve_references

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_PERFORMANCE_TESTS", "false").lower() != "true",
    reason="Performance tests are opt-in (set RUN_PERFORMANCE_TESTS=true)",
)

# =============================================================================
# Memory and Performance Measurement Utilities
# =============================================================================


@dataclass
class PerformanceStats:
    """Container for performance measurement results."""

    strategy: str
    data_size: int
    memory_initial_mb: float
    memory_peak_mb: float
    memory_final_mb: float
    duration_seconds: float
    resolved_count: int
    unresolved_count: int

    @property
    def memory_delta_mb(self) -> float:
        """Memory increase during operation."""
        return self.memory_final_mb - self.memory_initial_mb

    @property
    def peak_memory_delta_mb(self) -> float:
        """Peak memory usage relative to start."""
        return self.memory_peak_mb - self.memory_initial_mb

    @property
    def resolution_rate(self) -> float:
        """Resolutions per second."""
        return self.resolved_count / self.duration_seconds if self.duration_seconds > 0 else 0

    @property
    def memory_efficiency(self) -> float:
        """Memory usage per resolution (MB per resolution)."""
        return (
            self.peak_memory_delta_mb / self.resolved_count
            if self.resolved_count > 0
            else float("inf")
        )


def measure_performance(strategy: str, data_size: int, func, *args, **kwargs) -> PerformanceStats:
    """
    Measure memory and performance of a function execution.

    Args:
        strategy: Name of the strategy being tested
        data_size: Size of dataset being processed
        func: Function to measure
        *args, **kwargs: Arguments to pass to function

    Returns:
        PerformanceStats with measurements
    """
    # Force garbage collection before starting
    gc.collect()

    # Get initial memory
    process = psutil.Process()
    initial_memory = process.memory_info().rss / 1024 / 1024

    # Start tracemalloc for detailed tracking
    tracemalloc.start()

    start_time = time.time()

    try:
        result = func(*args, **kwargs)
        duration = time.time() - start_time

        # Get memory stats
        current, peak = tracemalloc.get_traced_memory()
        final_memory = process.memory_info().rss / 1024 / 1024

        # Convert to MB
        current / 1024 / 1024
        peak_mb = peak / 1024 / 1024

        # Extract resolution counts from result
        resolved_count = len(result) if isinstance(result, dict) else 0
        unresolved_count = 0  # Would need to calculate based on input vs output

        return PerformanceStats(
            strategy=strategy,
            data_size=data_size,
            memory_initial_mb=initial_memory,
            memory_peak_mb=max(peak_mb, final_memory),
            memory_final_mb=final_memory,
            duration_seconds=duration,
            resolved_count=resolved_count,
            unresolved_count=unresolved_count,
        )

    finally:
        tracemalloc.stop()


# =============================================================================
# Test Data Generation
# =============================================================================


def generate_test_identifiers(count: int, prefix: str = "test") -> dict[str, str]:
    """
    Generate synthetic identifier values for testing.

    Args:
        count: Number of identifiers to generate
        prefix: Prefix for identifier values

    Returns:
        Dictionary mapping identifier values to placeholder values
    """
    identifiers = {}
    for i in range(count):
        # Generate realistic-looking CPR numbers (10 digits)
        identifier_value = f"{prefix}{str(i % 1000000 + 1000000000)[:10]}"
        identifiers[identifier_value] = ""  # Placeholder for resolution

    return identifiers


# =============================================================================
# ID Resolution Strategy Tests
# =============================================================================


@pytest.fixture
def test_data_sizes():
    """Different data sizes to test with."""
    return [50, 100, 250]  # Start small for faster testing


@pytest.mark.performance
@pytest.mark.slow
class TestIDResolutionStrategies:
    """Test memory footprint vs speed trade-off of ID resolution strategies."""

    def test_memory_cache_vs_direct_postgres_memory_usage(
        self, real_fhir_database_resource: FHIRDatabaseResource, test_data_sizes: list[int]
    ):
        """Compare memory usage: memory_cache vs direct_postgres strategies."""

        results = []

        for data_size in test_data_sizes:
            logger.info(f"Testing with {data_size} identifiers...")

            # Generate test identifiers
            test_identifiers = generate_test_identifiers(data_size)

            # Test configurations
            strategies = [
                ("memory_cache", "memory_cache"),
                ("direct_postgres", "direct_postgres"),
            ]

            for strategy_name, id_resolution_strategy in strategies:
                logger.info(f"  Testing {strategy_name} strategy...")

                def resolve_with_strategy():
                    return batch_resolve_references(
                        identifiers=test_identifiers,
                        resource_type="Patient",
                        identifier_system="urn:dk:cpr",
                        id_resolution_strategy=id_resolution_strategy,
                        fhir_server=None,  # Not needed for database-only strategies
                        fhir_database=real_fhir_database_resource,
                    )

                stats = measure_performance(
                    strategy=strategy_name, data_size=data_size, func=resolve_with_strategy
                )

                results.append(stats)

                logger.info(
                    f"    {strategy_name} ({data_size} ids): "
                    f"Peak: {stats.memory_peak_mb:.1f}MB (+{stats.peak_memory_delta_mb:.1f}MB), "
                    f"Duration: {stats.duration_seconds:.2f}s, "
                    f"Rate: {stats.resolution_rate:.1f}/sec, "
                    f"Efficiency: {stats.memory_efficiency:.6f}MB/resolution"
                )

                # Clear memory cache between tests to ensure fair comparison
                if hasattr(real_fhir_database_resource, "_identifier_cache"):
                    real_fhir_database_resource._identifier_cache = None
                gc.collect()

        # Analyze results
        self._analyze_strategy_comparison(results, test_data_sizes)

    def test_memory_vs_speed_tradeoff_analysis(
        self, real_fhir_database_resource: FHIRDatabaseResource, test_data_sizes: list[int]
    ):
        """Analyze the memory vs speed trade-off between strategies."""

        results = []

        # Test with largest dataset for clearest comparison
        max_data_size = max(test_data_sizes)
        test_identifiers = generate_test_identifiers(max_data_size)

        logger.info(f"Trade-off analysis with {max_data_size} identifiers")

        # Test both strategies
        strategies = [
            ("memory_cache", "memory_cache"),
            ("direct_postgres", "direct_postgres"),
        ]

        for strategy_name, id_resolution_strategy in strategies:
            logger.info(f"Testing {strategy_name} strategy...")

            def resolve_with_strategy():
                return batch_resolve_references(
                    identifiers=test_identifiers,
                    resource_type="Patient",
                    identifier_system="urn:dk:cpr",
                    id_resolution_strategy=id_resolution_strategy,
                    fhir_server=None,
                    fhir_database=real_fhir_database_resource,
                )

            # Run multiple times to get average performance
            runs = 3 if strategy_name == "direct_postgres" else 1  # Memory cache only needs 1 run
            run_results = []

            for run in range(runs):
                if runs > 1:
                    logger.info(f"  Run {run + 1}/{runs}...")

                stats = measure_performance(
                    strategy=strategy_name, data_size=max_data_size, func=resolve_with_strategy
                )
                run_results.append(stats)

                # Clear cache between runs for direct_postgres
                if strategy_name == "direct_postgres" and hasattr(
                    real_fhir_database_resource, "_identifier_cache"
                ):
                    real_fhir_database_resource._identifier_cache = None
                gc.collect()

            # Average the results
            avg_stats = self._average_performance_stats(run_results)
            results.append(avg_stats)

            logger.info(
                f"{strategy_name} results: "
                f"Peak Memory: {avg_stats.memory_peak_mb:.1f}MB (+{avg_stats.peak_memory_delta_mb:.1f}MB), "
                f"Duration: {avg_stats.duration_seconds:.2f}s, "
                f"Rate: {avg_stats.resolution_rate:.1f}/sec"
            )

        # Analyze trade-off
        self._analyze_tradeoff(results)

    def test_memory_reduction_assertions(
        self, real_fhir_database_resource: FHIRDatabaseResource, test_data_sizes: list[int]
    ):
        """Assert expected memory reductions with direct_postgres vs memory_cache."""

        # Test with medium dataset
        test_size = 1000  # Good balance of speed vs meaningful data
        test_identifiers = generate_test_identifiers(test_size)

        logger.info(f"Memory reduction test with {test_size} identifiers")

        # Test memory_cache strategy
        def test_memory_cache():
            return batch_resolve_references(
                identifiers=test_identifiers,
                resource_type="Patient",
                identifier_system="urn:dk:cpr",
                id_resolution_strategy="memory_cache",
                fhir_server=None,
                fhir_database=real_fhir_database_resource,
            )

        memory_cache_stats = measure_performance(
            strategy="memory_cache", data_size=test_size, func=test_memory_cache
        )

        # Clear cache for fair comparison
        if hasattr(real_fhir_database_resource, "_identifier_cache"):
            real_fhir_database_resource._identifier_cache = None
        gc.collect()

        # Test direct_postgres strategy (run multiple times for stable measurement)
        postgres_runs = []
        for _i in range(3):

            def test_direct_postgres():
                return batch_resolve_references(
                    identifiers=test_identifiers,
                    resource_type="Patient",
                    identifier_system="urn:dk:cpr",
                    id_resolution_strategy="direct_postgres",
                    fhir_server=None,
                    fhir_database=real_fhir_database_resource,
                )

            stats = measure_performance(
                strategy="direct_postgres", data_size=test_size, func=test_direct_postgres
            )
            postgres_runs.append(stats)

            # Clear cache between runs
            if hasattr(real_fhir_database_resource, "_identifier_cache"):
                real_fhir_database_resource._identifier_cache = None
            gc.collect()

        # Average PostgreSQL results
        direct_postgres_stats = self._average_performance_stats(postgres_runs)

        # Log results
        logger.info("Memory reduction comparison:")
        logger.info(f"  Memory Cache: {memory_cache_stats.peak_memory_delta_mb:.1f}MB peak delta")
        logger.info(
            f"  Direct PostgreSQL: {direct_postgres_stats.peak_memory_delta_mb:.1f}MB peak delta"
        )
        logger.info(
            f"  Memory reduction: {memory_cache_stats.peak_memory_delta_mb / direct_postgres_stats.peak_memory_delta_mb:.1f}x"
        )

        # Assert memory reduction
        memory_reduction_ratio = (
            memory_cache_stats.peak_memory_delta_mb / direct_postgres_stats.peak_memory_delta_mb
        )

        # Expect at least 3x memory reduction (conservative estimate)
        assert memory_reduction_ratio >= 3.0, (
            f"Expected at least 3x memory reduction with direct_postgres, "
            f"got {memory_reduction_ratio:.1f}x "
            f"(Memory Cache: {memory_cache_stats.peak_memory_delta_mb:.1f}MB, "
            f"Direct PostgreSQL: {direct_postgres_stats.peak_memory_delta_mb:.1f}MB)"
        )

        # For larger datasets, expect even greater reduction
        if test_size >= 1000:
            assert memory_reduction_ratio >= 5.0, (
                f"For {test_size} identifiers, expected at least 5x memory reduction, "
                f"got {memory_reduction_ratio:.1f}x"
            )

        logger.info("✅ Memory reduction assertions passed!")

    def test_quick_memory_comparison_demo(self, real_fhir_database_resource: FHIRDatabaseResource):
        """Quick demonstration of memory difference between strategies."""

        # Use a small dataset for quick demo
        test_identifiers = generate_test_identifiers(50)

        print("Quick memory comparison demo with 50 identifiers")
        logger.info("Quick memory comparison demo with 50 identifiers")

        # Test memory cache preload only (most memory-intensive part)
        def preload_cache_only():
            real_fhir_database_resource.preload_identifier_cache(
                resource_type="Patient", identifier_systems=["urn:dk:cpr"]
            )
            return len(real_fhir_database_resource._identifier_cache or {})

        preload_stats = measure_performance(
            strategy="cache_preload_only", data_size=50, func=preload_cache_only
        )

        cache_size = preload_cache_only()
        print(
            f"Cache preload: {cache_size} identifiers loaded, "
            f"Peak memory: {preload_stats.memory_peak_mb:.1f}MB "
            f"(+{preload_stats.peak_memory_delta_mb:.1f}MB), "
            f"Duration: {preload_stats.duration_seconds:.2f}s"
        )
        logger.info(
            f"Cache preload: {cache_size} identifiers loaded, "
            f"Peak memory: {preload_stats.memory_peak_mb:.1f}MB "
            f"(+{preload_stats.peak_memory_delta_mb:.1f}MB), "
            f"Duration: {preload_stats.duration_seconds:.2f}s"
        )

        # Clear cache
        real_fhir_database_resource._identifier_cache = None
        gc.collect()

        # Test direct query (no preload)
        def direct_query_only():
            return batch_resolve_references(
                identifiers=test_identifiers,
                resource_type="Patient",
                identifier_system="urn:dk:cpr",
                id_resolution_strategy="direct_postgres",
                fhir_server=None,
                fhir_database=real_fhir_database_resource,
            )

        direct_stats = measure_performance(
            strategy="direct_postgres_only", data_size=50, func=direct_query_only
        )

        print(
            f"Direct query: Peak memory: {direct_stats.memory_peak_mb:.1f}MB "
            f"(+{direct_stats.peak_memory_delta_mb:.1f}MB), "
            f"Duration: {direct_stats.duration_seconds:.2f}s"
        )
        logger.info(
            f"Direct query: Peak memory: {direct_stats.memory_peak_mb:.1f}MB "
            f"(+{direct_stats.peak_memory_delta_mb:.1f}MB), "
            f"Duration: {direct_stats.duration_seconds:.2f}s"
        )

        # Show the difference - compare absolute peak memory usage
        memory_ratio = preload_stats.peak_memory_mb / direct_stats.peak_memory_mb
        speed_ratio = direct_stats.duration_seconds / preload_stats.duration_seconds

        print("=" * 50)
        print("MEMORY VS SPEED COMPARISON:")
        print(f"  Cache Preload - Peak Memory: {preload_stats.peak_memory_mb:.1f}MB")
        print(f"  Direct Query - Peak Memory: {direct_stats.peak_memory_mb:.1f}MB")
        print(f"  Memory overhead: Cache uses {memory_ratio:.2f}x more peak memory")
        print(
            f"  Speed: {'Cache is faster' if speed_ratio > 1 else 'Direct query is faster'} by {max(speed_ratio, 1/speed_ratio):.2f}x"
        )
        print(
            f"  Recommendation: {'Use Direct PostgreSQL for memory-constrained environments' if memory_ratio > 2 else 'Memory difference is minimal'}"
        )
        print("=" * 50)
        logger.info("=" * 50)
        logger.info("MEMORY VS SPEED COMPARISON:")
        logger.info(f"  Memory usage: Cache preload uses {memory_ratio:.1f}x more memory")
        logger.info(
            f"  Speed: {'Cache is faster' if speed_ratio > 1 else 'Direct query is faster'} by {max(speed_ratio, 1/speed_ratio):.1f}x"
        )
        logger.info("=" * 50)

        # Assert memory difference
        assert (
            memory_ratio > 2.0
        ), f"Expected cache preload to use more memory, got {memory_ratio:.1f}x"

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _average_performance_stats(self, stats_list: list[PerformanceStats]) -> PerformanceStats:
        """Average a list of PerformanceStats."""
        if not stats_list:
            raise ValueError("Cannot average empty stats list")

        if len(stats_list) == 1:
            return stats_list[0]

        # Average the numeric fields
        avg_initial = sum(s.memory_initial_mb for s in stats_list) / len(stats_list)
        avg_peak = sum(s.memory_peak_mb for s in stats_list) / len(stats_list)
        avg_final = sum(s.memory_final_mb for s in stats_list) / len(stats_list)
        avg_duration = sum(s.duration_seconds for s in stats_list) / len(stats_list)
        avg_resolved = sum(s.resolved_count for s in stats_list) / len(stats_list)
        avg_unresolved = sum(s.unresolved_count for s in stats_list) / len(stats_list)

        return PerformanceStats(
            strategy=stats_list[0].strategy,
            data_size=stats_list[0].data_size,
            memory_initial_mb=avg_initial,
            memory_peak_mb=avg_peak,
            memory_final_mb=avg_final,
            duration_seconds=avg_duration,
            resolved_count=int(avg_resolved),
            unresolved_count=int(avg_unresolved),
        )

    def _analyze_strategy_comparison(self, results: list[PerformanceStats], data_sizes: list[int]):
        """Analyze and log strategy comparison results."""
        logger.info("\n" + "=" * 60)
        logger.info("STRATEGY COMPARISON ANALYSIS")
        logger.info("=" * 60)

        for data_size in data_sizes:
            size_results = [r for r in results if r.data_size == data_size]
            if len(size_results) < 2:
                continue

            memory_cache = next((r for r in size_results if r.strategy == "memory_cache"), None)
            direct_postgres = next(
                (r for r in size_results if r.strategy == "direct_postgres"), None
            )

            if memory_cache and direct_postgres:
                memory_ratio = (
                    memory_cache.peak_memory_delta_mb / direct_postgres.peak_memory_delta_mb
                )
                speed_ratio = direct_postgres.duration_seconds / memory_cache.duration_seconds

                logger.info(f"Data Size: {data_size} identifiers")
                logger.info(
                    f"  Memory Cache: {memory_cache.peak_memory_delta_mb:.1f}MB peak, {memory_cache.duration_seconds:.2f}s"
                )
                logger.info(
                    f"  Direct PostgreSQL: {direct_postgres.peak_memory_delta_mb:.1f}MB peak, {direct_postgres.duration_seconds:.2f}s"
                )
                logger.info(f"  Memory reduction: {memory_ratio:.1f}x")
                logger.info(
                    f"  Speed difference: {speed_ratio:.1f}x {'faster' if speed_ratio > 1 else 'slower'}"
                )
                logger.info("")

    def _analyze_tradeoff(self, results: list[PerformanceStats]):
        """Analyze memory vs speed trade-off."""
        logger.info("\n" + "=" * 60)
        logger.info("MEMORY VS SPEED TRADE-OFF ANALYSIS")
        logger.info("=" * 60)

        if len(results) != 2:
            logger.warning("Need exactly 2 strategies for trade-off analysis")
            return

        memory_cache = next((r for r in results if r.strategy == "memory_cache"), None)
        direct_postgres = next((r for r in results if r.strategy == "direct_postgres"), None)

        if memory_cache and direct_postgres:
            memory_ratio = memory_cache.peak_memory_delta_mb / direct_postgres.peak_memory_delta_mb
            speed_ratio = direct_postgres.duration_seconds / memory_cache.duration_seconds

            logger.info("Trade-off Summary:")
            logger.info(f"  Memory Usage: Direct PostgreSQL uses {1/memory_ratio:.1f}x less memory")
            logger.info(
                f"  Performance: Memory Cache is {speed_ratio:.1f}x {'faster' if speed_ratio < 1 else 'slower'}"
            )

            if memory_ratio > 5 and speed_ratio < 2:
                logger.info(
                    "  ✅ Recommendation: Use Direct PostgreSQL for memory-constrained environments"
                )
            elif memory_ratio > 10:
                logger.info(
                    "  ✅ Recommendation: Use Direct PostgreSQL - memory savings outweigh performance cost"
                )
            else:
                logger.info(
                    "  ⚠️  Consider use case: Memory Cache may be worth the cost for high-frequency operations"
                )


# =============================================================================
# Standalone Benchmark Script
# =============================================================================


def run_memory_vs_speed_benchmark():
    """
    Run memory vs speed benchmark as a standalone script.

    Usage:
        python tests/performance/test_memory_cache_vs_postgres.py
    """
    import os
    import sys

    # Set up environment
    os.environ["USE_REAL_DB_FOR_TESTS"] = "true"

    # Add project root to path
    project_root = "/workspaces/healthpipe"
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    # Import required modules
    from core.resources import FHIRDatabaseResource

    print("Starting Memory vs Speed Benchmark")
    print("=" * 50)
    logger.info("Starting Memory vs Speed Benchmark")
    logger.info("=" * 50)

    try:
        # Initialize resources
        fhir_db = FHIRDatabaseResource()

        # Run a quick demo test
        test_instance = TestIDResolutionStrategies()
        test_instance.test_quick_memory_comparison_demo(fhir_db)

        logger.info("✅ Benchmark completed successfully!")

    except Exception as e:
        logger.error(f"Benchmark failed: {e}")
        raise


if __name__ == "__main__":
    # Allow running benchmark directly
    run_memory_vs_speed_benchmark()
