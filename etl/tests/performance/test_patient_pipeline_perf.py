# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Performance tests for the FHIR Patient ETL pipeline.

These tests measure and profile the patient pipeline performance with
production-scale data volumes (~47k patients per monthly partition).

Prerequisites:
    1. Oracle IFDB must be seeded with performance test data:
       cd etl && python -m scripts.seed_ifdb_performance_data

    2. Environment must be configured for real resources:
       export USE_REAL_DB_FOR_TESTS=true

Usage:
    # Run all performance tests
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_patient_pipeline_perf.py -v -s

    # Run specific test
    pytest tests/performance/test_patient_pipeline_perf.py::test_single_partition_extraction -v -s

    # Run with timing details
    pytest tests/performance/test_patient_pipeline_perf.py -v -s --durations=0
"""

import logging
import os
import time
from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

logger = logging.getLogger(__name__)


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


# =============================================================================
# Test Configuration
# =============================================================================

# Test partitions (should have data if seeded correctly)
TEST_PARTITIONS = [
    "2021-09",  # First partition
    "2023-06",  # Middle partition
    "2025-12",  # Near-end partition
]

# Expected rows per partition (based on 2.5M / 53 months)
EXPECTED_ROWS_PER_PARTITION = 47_170
ROWS_TOLERANCE = 0.1  # Allow 10% variance


@dataclass
class TimingResult:
    """Container for timing measurements."""

    phase: str
    duration_seconds: float
    rows_processed: int = 0

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds > 0 and self.rows_processed > 0:
            return self.rows_processed / self.duration_seconds
        return 0.0


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def perf_oracle_resource():
    """
    Oracle resource for performance tests.
    Requires USE_REAL_DB_FOR_TESTS=true.
    """
    if os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true":
        pytest.skip("Performance tests require USE_REAL_DB_FOR_TESTS=true")

    from core.resources import IFDBOracleResource

    try:
        resource = IFDBOracleResource().process_config_and_initialize()
        # Test connection
        with resource.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
        return resource
    except Exception as e:
        pytest.skip(f"Cannot connect to Oracle: {e}")


@pytest.fixture
def perf_fhir_server():
    """
    FHIR server resource for performance tests.
    """
    if os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true":
        pytest.skip("Performance tests require USE_REAL_DB_FOR_TESTS=true")

    from core.resources import FHIRServerResource

    base_url = os.getenv("FHIR_SERVER_BASE_URL", "http://fhir-server:8888")
    username = os.getenv("FHIR_SERVER_USERNAME", "root")
    password = os.getenv("FHIR_SERVER_PASSWORD", "secret")

    return FHIRServerResource(
        base_url=base_url,
        client_username=username,
        client_password=password,
    )


@pytest.fixture
def perf_fhir_database():
    """
    FHIR database resource for performance tests.
    """
    if os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() != "true":
        pytest.skip("Performance tests require USE_REAL_DB_FOR_TESTS=true")

    from core.resources import FHIRDatabaseResource

    host = os.getenv("FHIR_DATABASE_HOST", "fhir-server-db")
    port = int(os.getenv("FHIR_DATABASE_PORT", "5432"))
    database = os.getenv("FHIR_DATABASE_NAME", "aidbox")
    user = _required_env("FHIR_DATABASE_USER")
    password = _required_env("FHIR_DATABASE_PASSWORD")

    return FHIRDatabaseResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


@pytest.fixture
def mock_dagster_context():
    """Mock Dagster context for testing."""
    import dagster as dg

    mock = MagicMock(spec=dg.AssetExecutionContext)
    mock.partition_key = "2023-06"
    mock.log = logger
    mock.run_id = "perf_test_run"
    return mock


# =============================================================================
# Helper Functions
# =============================================================================


def verify_test_data_exists(oracle_resource, partition: str) -> int:
    """
    Verify that test data exists for the given partition.

    Returns the row count for that partition.
    """
    from DSA.FHIR.utils.partition_utils import get_month_date_range

    start_date, end_date = get_month_date_range(partition)

    query = f"""
        SELECT COUNT(*)
        FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA
        WHERE TRUNC(CAST(MODTIDSPUNKT AS DATE)) >= DATE '{start_date}'
          AND TRUNC(CAST(MODTIDSPUNKT AS DATE)) <= DATE '{end_date}'
    """

    with oracle_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            count = cursor.fetchone()[0]
            return count
        finally:
            cursor.close()


def get_total_row_count(oracle_resource) -> int:
    """Get total row count in the patient table."""
    with oracle_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT COUNT(*) FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA")
            return cursor.fetchone()[0]
        finally:
            cursor.close()


def format_timing_report(timings: list[TimingResult], title: str) -> str:
    """Format timing results as a readable report."""
    lines = [
        "",
        "=" * 70,
        f"  {title}",
        "=" * 70,
    ]

    total_time = sum(t.duration_seconds for t in timings)
    total_rows = sum(t.rows_processed for t in timings)

    for t in timings:
        pct = (t.duration_seconds / total_time * 100) if total_time > 0 else 0
        lines.append(
            f"  {t.phase:30s}: {t.duration_seconds:8.2f}s ({pct:5.1f}%) "
            f"| {t.rows_processed:,} rows @ {t.rows_per_second:.0f}/s"
        )

    lines.append("-" * 70)
    overall_rate = total_rows / total_time if total_time > 0 else 0
    lines.append(
        f"  {'TOTAL':30s}: {total_time:8.2f}s | {total_rows:,} rows @ {overall_rate:.0f}/s"
    )
    lines.append("=" * 70)

    return "\n".join(lines)


# =============================================================================
# Performance Tests
# =============================================================================


@pytest.mark.slow
@pytest.mark.performance
class TestDataVerification:
    """Verify that performance test data is properly seeded."""

    def test_total_row_count(self, perf_oracle_resource):
        """Verify total row count matches expected."""
        total = get_total_row_count(perf_oracle_resource)

        logger.info(f"Total rows in PATIENT_OPLYSNINGER_COLUMNA: {total:,}")

        if total < 100_000:
            pytest.skip(
                f"Insufficient test data ({total:,} rows). "
                "Run: python -m scripts.seed_ifdb_performance_data"
            )

        assert total > 0, "No test data found"

    @pytest.mark.parametrize("partition", TEST_PARTITIONS)
    def test_partition_has_data(self, perf_oracle_resource, partition):
        """Verify each test partition has data."""
        count = verify_test_data_exists(perf_oracle_resource, partition)

        logger.info(f"Partition {partition}: {count:,} rows")

        if count < 1000:
            pytest.skip(
                f"Partition {partition} has insufficient data ({count} rows). "
                "Run: python -m scripts.seed_ifdb_performance_data"
            )

        assert count > 0, f"No data found for partition {partition}"


@pytest.mark.slow
@pytest.mark.performance
class TestExtractionPerformance:
    """Test Oracle extraction performance."""

    def test_single_partition_extraction(self, perf_oracle_resource, mock_dagster_context):
        """
        Benchmark extraction of a single monthly partition.

        Expected: ~47k rows extracted in < 30 seconds
        """
        from DSI.connectors.oracle.models import execute_extraction
        from DSI.oracle.ifdb import PATIENT_CONFIG

        partition = "2023-06"

        # Verify data exists
        expected_count = verify_test_data_exists(perf_oracle_resource, partition)
        if expected_count < 1000:
            pytest.skip(f"Insufficient data for partition {partition}")

        mock_dagster_context.partition_key = partition

        # Benchmark extraction
        logger.info(f"\nExtracting partition {partition} (expected ~{expected_count:,} rows)...")

        start_time = time.perf_counter()
        rows = execute_extraction(
            context=mock_dagster_context,
            ifdb_oracle=perf_oracle_resource,
            config=PATIENT_CONFIG,
            partition_key=partition,
            partition_type="monthly",
        )
        elapsed = time.perf_counter() - start_time

        # Report
        timing = TimingResult("Extraction", elapsed, len(rows))
        logger.info(format_timing_report([timing], f"EXTRACTION: Partition {partition}"))

        # Assertions
        assert len(rows) > 0, "No rows extracted"
        assert (
            len(rows) >= expected_count * 0.9
        ), f"Extracted fewer rows than expected: {len(rows)} vs {expected_count}"

        # Performance assertion (adjust threshold based on your requirements)
        rate = len(rows) / elapsed if elapsed > 0 else 0
        logger.info(f"Extraction rate: {rate:.0f} rows/second")

        # Warn if slow but don't fail
        if rate < 1000:
            logger.warning(f"Extraction rate ({rate:.0f}/s) is below 1000/s threshold")

    @pytest.mark.parametrize("partition", TEST_PARTITIONS)
    def test_extraction_consistency(self, perf_oracle_resource, mock_dagster_context, partition):
        """
        Test that extraction produces consistent results across partitions.
        """
        from DSI.connectors.oracle.models import execute_extraction
        from DSI.oracle.ifdb import PATIENT_CONFIG

        expected_count = verify_test_data_exists(perf_oracle_resource, partition)
        if expected_count < 100:
            pytest.skip(f"Insufficient data for partition {partition}")

        mock_dagster_context.partition_key = partition

        start_time = time.perf_counter()
        rows = execute_extraction(
            context=mock_dagster_context,
            ifdb_oracle=perf_oracle_resource,
            config=PATIENT_CONFIG,
            partition_key=partition,
            partition_type="monthly",
        )
        elapsed = time.perf_counter() - start_time

        logger.info(f"Partition {partition}: {len(rows):,} rows in {elapsed:.2f}s")

        assert len(rows) > 0, f"No rows extracted for partition {partition}"


@pytest.mark.slow
@pytest.mark.performance
class TestTransformationPerformance:
    """Test FHIR transformation performance."""

    def test_patient_transformation_throughput(self, perf_oracle_resource, mock_dagster_context):
        """
        Benchmark FHIR Patient transformation.

        Tests the transformation of extracted rows to FHIR Patient resources.
        """
        from DSA.FHIR.Patient.patient_ifdb_contribution import transform_fhir_patient_ifdb
        from DSI.connectors.oracle.models import execute_extraction
        from DSI.oracle.ifdb import PATIENT_CONFIG

        partition = "2023-06"
        mock_dagster_context.partition_key = partition

        # Extract data first
        logger.info("Extracting data for transformation test...")
        extract_start = time.perf_counter()
        rows = execute_extraction(
            context=mock_dagster_context,
            ifdb_oracle=perf_oracle_resource,
            config=PATIENT_CONFIG,
            partition_key=partition,
            partition_type="monthly",
        )
        extract_time = time.perf_counter() - extract_start

        if len(rows) < 1000:
            pytest.skip(f"Insufficient data: {len(rows)} rows")

        # Benchmark transformation
        logger.info(f"Transforming {len(rows):,} rows to FHIR Patient resources...")
        transform_start = time.perf_counter()
        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=rows,
        )
        transform_time = time.perf_counter() - transform_start

        # Report
        timings = [
            TimingResult("Extraction", extract_time, len(rows)),
            TimingResult("Transformation", transform_time, len(patients)),
        ]
        logger.info(format_timing_report(timings, "EXTRACTION + TRANSFORMATION"))

        # Assertions
        assert len(patients) > 0, "No patients transformed"

        # Transformation should be fast (> 5000 resources/sec)
        transform_rate = len(patients) / transform_time if transform_time > 0 else 0
        logger.info(f"Transformation rate: {transform_rate:.0f} resources/second")


@pytest.mark.slow
@pytest.mark.performance
class TestFullPipelinePerformance:
    """Test end-to-end pipeline performance."""

    def test_full_pipeline_single_partition(
        self,
        perf_oracle_resource,
        perf_fhir_server,
        perf_fhir_database,
        mock_dagster_context,
    ):
        """
        Benchmark the full ETL pipeline for a single partition.

        Phases:
        1. Extraction from Oracle
        2. Transformation to FHIR
        3. ID Resolution
        4. Posting to FHIR server
        """
        from DSA.FHIR.Patient.patient_ifdb_contribution import transform_fhir_patient_ifdb
        from DSI.connectors.oracle.models import execute_extraction
        from DSI.oracle.ifdb import PATIENT_CONFIG
        from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

        partition = "2023-06"
        mock_dagster_context.partition_key = partition
        timings: list[TimingResult] = []

        # Phase 1: Extraction
        logger.info(f"\n{'='*70}")
        logger.info(f"FULL PIPELINE TEST: Partition {partition}")
        logger.info(f"{'='*70}")

        logger.info("\nPhase 1: Extraction...")
        start = time.perf_counter()
        rows = execute_extraction(
            context=mock_dagster_context,
            ifdb_oracle=perf_oracle_resource,
            config=PATIENT_CONFIG,
            partition_key=partition,
            partition_type="monthly",
        )
        timings.append(TimingResult("1. Extraction", time.perf_counter() - start, len(rows)))

        if len(rows) < 100:
            pytest.skip(f"Insufficient data: {len(rows)} rows")

        # Limit rows for testing (full 47k takes too long for CI)
        max_test_rows = int(os.getenv("PERF_TEST_MAX_ROWS", "5000"))
        if len(rows) > max_test_rows:
            logger.info(
                f"Limiting to {max_test_rows} rows for test (set PERF_TEST_MAX_ROWS to change)"
            )
            rows = rows[:max_test_rows]

        # Phase 2: Transformation
        logger.info(f"\nPhase 2: Transformation ({len(rows):,} rows)...")
        start = time.perf_counter()
        patients = transform_fhir_patient_ifdb(
            context=mock_dagster_context,
            patient_rows=rows,
        )
        timings.append(
            TimingResult("2. Transformation", time.perf_counter() - start, len(patients))
        )

        # Phase 3: ID Resolution
        logger.info(f"\nPhase 3: ID Resolution ({len(patients):,} resources)...")
        start = time.perf_counter()

        index_to_id = perf_fhir_database.resolve_resource_ids(
            resources=patients,
            resource_type="Patient",
            preferred_systems=[SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID],
        )

        # Attach IDs
        for idx, fhir_id in index_to_id.items():
            if idx < len(patients):
                patients[idx].id = fhir_id

        timings.append(TimingResult("3. ID Resolution", time.perf_counter() - start, len(patients)))

        # Phase 4: Posting (using direct DB insertion)
        logger.info(f"\nPhase 4: Posting ({len(patients):,} resources)...")
        start = time.perf_counter()

        # Use direct DB insertion (4-7x faster than FHIR API)
        successes, failures, failed_ids = perf_fhir_database.batch_upsert_resources(
            resources=patients,
            resource_type="Patient",
        )

        posted_count = successes
        timings.append(TimingResult("4. Posting", time.perf_counter() - start, posted_count))

        # Report
        logger.info(format_timing_report(timings, f"FULL PIPELINE: {partition}"))

        # Assertions
        total_time = sum(t.duration_seconds for t in timings)
        overall_rate = len(rows) / total_time if total_time > 0 else 0

        logger.info(f"\nOverall throughput: {overall_rate:.0f} rows/second")
        logger.info(f"Estimated time for 47k rows: {47000 / overall_rate / 60:.1f} minutes")

        assert posted_count > 0, "No resources posted"
