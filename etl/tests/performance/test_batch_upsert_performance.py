# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Performance tests comparing different batch upsert strategies for FHIR resources.

This tests 5 different approaches:
1. Current implementation (single row inserts in loop)
2. executemany() - psycopg's built-in batch execution
3. execute_values() - psycopg's optimized multi-value INSERT
4. COPY protocol - PostgreSQL's fastest bulk insert method
5. Chunked single inserts - batching commits in chunks of N

Prerequisites:
    1. FHIR PostgreSQL database running (fhir-server-db)
    2. Environment: USE_REAL_DB_FOR_TESTS=true

Usage:
    # Run the benchmark
    cd /workspaces/healthpipe/etl
    USE_REAL_DB_FOR_TESTS=true python tests/performance/test_batch_upsert_performance.py

    # Or via pytest
    USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_batch_upsert_performance.py -v -s
"""

import gc
import io
import json
import os
import statistics
import sys
import time
import uuid
from dataclasses import dataclass, field

import psycopg


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable {name}")
    return value


# Add project root to path
project_root = "/workspaces/healthpipe/etl"
if project_root not in sys.path:
    sys.path.insert(0, project_root)


# =============================================================================
# Test Configuration
# =============================================================================


@dataclass
class BenchmarkConfig:
    """Configuration for the benchmark."""

    # Test sizes to benchmark
    test_sizes: list[int] = field(default_factory=lambda: [100, 500, 1000, 2000])
    # Number of runs per test for averaging
    runs_per_test: int = 3
    # Chunk sizes to test for chunked approaches
    chunk_sizes: list[int] = field(default_factory=lambda: [50, 100, 250, 500])
    # Database connection settings
    db_host: str = "fhir-server-db"
    db_port: int = 5432
    db_name: str = "aidbox"
    db_user: str = field(default_factory=lambda: _required_env("PG_FHIR_USER"))
    db_password: str = field(default_factory=lambda: _required_env("PG_FHIR_PASSWORD"))


@dataclass
class BenchmarkResult:
    """Result from a single benchmark run."""

    approach: str
    data_size: int
    chunk_size: int | None
    duration_seconds: float
    rows_per_second: float
    successes: int
    failures: int

    def __str__(self) -> str:
        chunk_info = f" (chunk={self.chunk_size})" if self.chunk_size else ""
        return (
            f"{self.approach}{chunk_info}: "
            f"{self.duration_seconds:.3f}s, "
            f"{self.rows_per_second:.0f} rows/sec, "
            f"{self.successes}/{self.data_size} succeeded"
        )


# =============================================================================
# Mock FHIR Resource Generator
# =============================================================================


def generate_mock_fhir_resources(count: int) -> list[dict]:
    """
    Generate mock FHIR EpisodeOfCare resources for testing.

    Returns dicts (already serialized) to isolate DB performance from serialization.
    """
    resources = []
    for i in range(count):
        resource_id = str(uuid.uuid4())
        resource = {
            "resourceType": "EpisodeOfCare",
            "id": resource_id,
            "status": "active",
            "patient": {
                "resourceType": "Patient",
                "id": str(uuid.uuid4()),
            },
            "managingOrganization": {
                "resourceType": "Organization",
                "id": str(uuid.uuid4()),
            },
            "period": {
                "start": "2024-01-01T00:00:00Z",
            },
            "identifier": [
                {
                    "system": "urn:test:benchmark",
                    "value": f"BENCH-{i:06d}",
                }
            ],
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/episodeofcare-type",
                            "code": "hacc",
                            "display": "Home and Community Care",
                        }
                    ]
                }
            ],
        }
        resources.append(resource)
    return resources


# =============================================================================
# Database Utilities
# =============================================================================


def get_connection(config: BenchmarkConfig) -> psycopg.Connection:
    """Get a database connection."""
    return psycopg.connect(
        host=config.db_host,
        port=config.db_port,
        dbname=config.db_name,
        user=config.db_user,
        password=config.db_password,
    )


def ensure_test_table(config: BenchmarkConfig, table_name: str = "benchmark_test"):
    """Create a test table that mirrors the FHIR resource table structure."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            # Check if resource_status type exists
            cur.execute(
                """
                SELECT EXISTS (
                    SELECT 1 FROM pg_type WHERE typname = 'resource_status'
                );
            """
            )
            type_exists = cur.fetchone()[0]

            if not type_exists:
                # Create the enum type if it doesn't exist
                cur.execute(
                    """
                    CREATE TYPE resource_status AS ENUM ('created', 'updated', 'deleted');
                """
                )

            # Create test table
            cur.execute(
                f"""
                DROP TABLE IF EXISTS {table_name};
                CREATE TABLE {table_name} (
                    id TEXT PRIMARY KEY,
                    txid BIGINT NOT NULL,
                    status resource_status NOT NULL DEFAULT 'created',
                    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                    resource JSONB NOT NULL
                );
            """
            )
            conn.commit()


def cleanup_test_table(config: BenchmarkConfig, table_name: str = "benchmark_test"):
    """Truncate the test table."""
    with get_connection(config) as conn:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table_name};")
            conn.commit()


def get_next_txid(cur, table_name: str) -> int:
    """Get the next available txid."""
    cur.execute(f"SELECT COALESCE(MAX(txid), 0) + 1 FROM {table_name};")
    return cur.fetchone()[0]


# =============================================================================
# Approach 1: Current Implementation (Single Row Inserts)
# =============================================================================


def approach_1_single_inserts(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
) -> tuple[int, int, float]:
    """
    Current implementation: single row inserts in a loop.

    This is the baseline - each resource is inserted with a separate execute() call.
    """
    successes = 0
    failures = 0

    start_time = time.perf_counter()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            for i, resource in enumerate(resources):
                try:
                    resource_id = resource.get("id") or str(uuid.uuid4())
                    txid = next_txid + i

                    cur.execute(
                        f"""
                        INSERT INTO {table_name} (id, txid, status, resource)
                        VALUES (%s, %s, 'created'::resource_status, %s)
                        ON CONFLICT (id) DO UPDATE SET
                            txid = EXCLUDED.txid,
                            status = 'updated'::resource_status,
                            ts = CURRENT_TIMESTAMP,
                            resource = EXCLUDED.resource
                        """,
                        (resource_id, txid, json.dumps(resource)),
                    )
                    successes += 1
                except Exception:
                    failures += 1

            conn.commit()

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Approach 2: executemany() - Batch Execution
# =============================================================================


def approach_2_executemany(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
) -> tuple[int, int, float]:
    """
    Use psycopg's executemany() for batch parameter binding.

    executemany() prepares the statement once and executes with different parameters.
    """
    start_time = time.perf_counter()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            # Prepare the data as a list of tuples
            params = []
            for i, resource in enumerate(resources):
                resource_id = resource.get("id") or str(uuid.uuid4())
                txid = next_txid + i
                params.append((resource_id, txid, json.dumps(resource)))

            try:
                cur.executemany(
                    f"""
                    INSERT INTO {table_name} (id, txid, status, resource)
                    VALUES (%s, %s, 'created'::resource_status, %s)
                    ON CONFLICT (id) DO UPDATE SET
                        txid = EXCLUDED.txid,
                        status = 'updated'::resource_status,
                        ts = CURRENT_TIMESTAMP,
                        resource = EXCLUDED.resource
                    """,
                    params,
                )
                conn.commit()
                successes = len(resources)
                failures = 0
            except Exception as e:
                print(f"executemany failed: {e}")
                successes = 0
                failures = len(resources)

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Approach 3: execute_values() - Multi-Value INSERT
# =============================================================================


def approach_3_execute_values(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
    page_size: int = 1000,
) -> tuple[int, int, float]:
    """
    Use psycopg's execute_values() for multi-row INSERT statements.

    This generates: INSERT INTO ... VALUES (...), (...), (...), ...
    Much faster than individual inserts as it reduces round-trips.

    Note: psycopg3 doesn't have execute_values directly, so we build the query manually.
    """
    start_time = time.perf_counter()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            # Build multi-value INSERT
            # For upsert, we need to use a temp table or CTE approach
            # since multi-value INSERT with ON CONFLICT requires special handling

            values_list = []
            params = []
            for i, resource in enumerate(resources):
                resource_id = resource.get("id") or str(uuid.uuid4())
                txid = next_txid + i
                values_list.append("(%s, %s, 'created'::resource_status, %s)")
                params.extend([resource_id, txid, json.dumps(resource)])

            try:
                # Build the full query with all values
                values_sql = ", ".join(values_list)
                query = f"""
                    INSERT INTO {table_name} (id, txid, status, resource)
                    VALUES {values_sql}
                    ON CONFLICT (id) DO UPDATE SET
                        txid = EXCLUDED.txid,
                        status = 'updated'::resource_status,
                        ts = CURRENT_TIMESTAMP,
                        resource = EXCLUDED.resource
                """
                cur.execute(query, params)
                conn.commit()
                successes = len(resources)
                failures = 0
            except Exception as e:
                print(f"execute_values failed: {e}")
                successes = 0
                failures = len(resources)

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Approach 4: COPY Protocol (Fastest for Bulk Insert)
# =============================================================================


def approach_4_copy_protocol(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
) -> tuple[int, int, float]:
    """
    Use PostgreSQL's COPY protocol for maximum throughput.

    COPY is PostgreSQL's fastest bulk loading mechanism. However, it doesn't
    support ON CONFLICT directly, so we use a staging table approach:
    1. COPY data to a temp staging table
    2. Upsert from staging to target table
    """
    start_time = time.perf_counter()
    staging_table = f"{table_name}_staging"

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            try:
                # Create staging table
                cur.execute(
                    f"""
                    DROP TABLE IF EXISTS {staging_table};
                    CREATE TEMP TABLE {staging_table} (
                        id TEXT,
                        txid BIGINT,
                        resource JSONB
                    );
                """
                )

                # Prepare data for COPY
                # Format: id\ttxid\tresource_json
                buffer = io.StringIO()
                for i, resource in enumerate(resources):
                    resource_id = resource.get("id") or str(uuid.uuid4())
                    txid = next_txid + i
                    # Escape special characters for COPY format
                    resource_json = (
                        json.dumps(resource)
                        .replace("\\", "\\\\")
                        .replace("\t", "\\t")
                        .replace("\n", "\\n")
                    )
                    buffer.write(f"{resource_id}\t{txid}\t{resource_json}\n")

                buffer.seek(0)

                # Use COPY to load into staging
                with cur.copy(f"COPY {staging_table} (id, txid, resource) FROM STDIN") as copy:
                    copy.write(buffer.read())

                # Upsert from staging to target
                cur.execute(
                    f"""
                    INSERT INTO {table_name} (id, txid, status, resource)
                    SELECT id, txid, 'created'::resource_status, resource
                    FROM {staging_table}
                    ON CONFLICT (id) DO UPDATE SET
                        txid = EXCLUDED.txid,
                        status = 'updated'::resource_status,
                        ts = CURRENT_TIMESTAMP,
                        resource = EXCLUDED.resource
                """
                )

                # Cleanup staging
                cur.execute(f"DROP TABLE IF EXISTS {staging_table};")

                conn.commit()
                successes = len(resources)
                failures = 0

            except Exception as e:
                print(f"COPY protocol failed: {e}")
                import traceback

                traceback.print_exc()
                successes = 0
                failures = len(resources)

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Approach 5: Chunked Single Inserts with Intermediate Commits
# =============================================================================


def approach_5_chunked_inserts(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
    chunk_size: int = 100,
) -> tuple[int, int, float]:
    """
    Chunked single inserts with commits after each chunk.

    This balances between single inserts and full batch:
    - Reduces transaction log pressure
    - Provides progress visibility
    - Allows partial success on failures
    """
    successes = 0
    failures = 0

    start_time = time.perf_counter()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            # Process in chunks
            for chunk_start in range(0, len(resources), chunk_size):
                chunk = resources[chunk_start : chunk_start + chunk_size]

                for i, resource in enumerate(chunk):
                    try:
                        resource_id = resource.get("id") or str(uuid.uuid4())
                        txid = next_txid + chunk_start + i

                        cur.execute(
                            f"""
                            INSERT INTO {table_name} (id, txid, status, resource)
                            VALUES (%s, %s, 'created'::resource_status, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                txid = EXCLUDED.txid,
                                status = 'updated'::resource_status,
                                ts = CURRENT_TIMESTAMP,
                                resource = EXCLUDED.resource
                            """,
                            (resource_id, txid, json.dumps(resource)),
                        )
                        successes += 1
                    except Exception:
                        failures += 1

                # Commit after each chunk
                conn.commit()

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Approach 5b: Chunked Multi-Value INSERT
# =============================================================================


def approach_5b_chunked_multi_value(
    config: BenchmarkConfig,
    resources: list[dict],
    table_name: str = "benchmark_test",
    chunk_size: int = 100,
) -> tuple[int, int, float]:
    """
    Chunked multi-value INSERT statements.

    Combines the benefits of:
    - Multi-value INSERT (fewer round-trips)
    - Chunking (memory management, partial progress)
    """
    successes = 0
    failures = 0

    start_time = time.perf_counter()

    with get_connection(config) as conn:
        with conn.cursor() as cur:
            next_txid = get_next_txid(cur, table_name)

            # Process in chunks
            for chunk_start in range(0, len(resources), chunk_size):
                chunk = resources[chunk_start : chunk_start + chunk_size]

                try:
                    # Build multi-value INSERT for this chunk
                    values_list = []
                    params = []
                    for i, resource in enumerate(chunk):
                        resource_id = resource.get("id") or str(uuid.uuid4())
                        txid = next_txid + chunk_start + i
                        values_list.append("(%s, %s, 'created'::resource_status, %s)")
                        params.extend([resource_id, txid, json.dumps(resource)])

                    values_sql = ", ".join(values_list)
                    query = f"""
                        INSERT INTO {table_name} (id, txid, status, resource)
                        VALUES {values_sql}
                        ON CONFLICT (id) DO UPDATE SET
                            txid = EXCLUDED.txid,
                            status = 'updated'::resource_status,
                            ts = CURRENT_TIMESTAMP,
                            resource = EXCLUDED.resource
                    """
                    cur.execute(query, params)
                    conn.commit()
                    successes += len(chunk)

                except Exception as e:
                    print(f"Chunk failed: {e}")
                    failures += len(chunk)

    duration = time.perf_counter() - start_time
    return successes, failures, duration


# =============================================================================
# Benchmark Runner
# =============================================================================


def run_benchmark(config: BenchmarkConfig) -> list[BenchmarkResult]:
    """Run all benchmarks and collect results."""
    results: list[BenchmarkResult] = []
    table_name = "benchmark_test"

    # Ensure test table exists
    print("Setting up test table...")
    ensure_test_table(config, table_name)

    for data_size in config.test_sizes:
        print(f"\n{'='*60}")
        print(f"Testing with {data_size} resources")
        print("=" * 60)

        # Generate test data once per size
        print(f"Generating {data_size} mock resources...")
        resources = generate_mock_fhir_resources(data_size)

        # Define approaches to test
        approaches = [
            (
                "1. Single Inserts (Current)",
                lambda: approach_1_single_inserts(config, resources, table_name),
                None,
            ),
            (
                "2. executemany()",
                lambda: approach_2_executemany(config, resources, table_name),
                None,
            ),
            (
                "3. Multi-Value INSERT",
                lambda: approach_3_execute_values(config, resources, table_name),
                None,
            ),
            (
                "4. COPY Protocol",
                lambda: approach_4_copy_protocol(config, resources, table_name),
                None,
            ),
        ]

        # Add chunked approaches with different chunk sizes
        for chunk_size in config.chunk_sizes:
            approaches.append(
                (
                    f"5a. Chunked Single (chunk={chunk_size})",
                    lambda cs=chunk_size: approach_5_chunked_inserts(
                        config, resources, table_name, cs
                    ),
                    chunk_size,
                )
            )
            approaches.append(
                (
                    f"5b. Chunked Multi-Value (chunk={chunk_size})",
                    lambda cs=chunk_size: approach_5b_chunked_multi_value(
                        config, resources, table_name, cs
                    ),
                    chunk_size,
                )
            )

        for approach_name, approach_func, chunk_size in approaches:
            print(f"\n  Testing: {approach_name}")

            run_times = []
            run_successes = []
            run_failures = []

            for run in range(config.runs_per_test):
                # Clean table before each run
                cleanup_test_table(config, table_name)
                gc.collect()

                successes, failures, duration = approach_func()
                run_times.append(duration)
                run_successes.append(successes)
                run_failures.append(failures)

                print(f"    Run {run + 1}: {duration:.3f}s ({successes} succeeded)")

            # Calculate averages
            avg_duration = statistics.mean(run_times)
            avg_successes = int(statistics.mean(run_successes))
            avg_failures = int(statistics.mean(run_failures))
            rows_per_sec = data_size / avg_duration if avg_duration > 0 else 0

            result = BenchmarkResult(
                approach=approach_name,
                data_size=data_size,
                chunk_size=chunk_size,
                duration_seconds=avg_duration,
                rows_per_second=rows_per_sec,
                successes=avg_successes,
                failures=avg_failures,
            )
            results.append(result)

            print(f"    Average: {avg_duration:.3f}s, {rows_per_sec:.0f} rows/sec")

    return results


def print_summary(results: list[BenchmarkResult]):
    """Print a summary table of results."""
    print("\n" + "=" * 80)
    print("BENCHMARK SUMMARY")
    print("=" * 80)

    # Group results by data size
    sizes = sorted(set(r.data_size for r in results))

    for size in sizes:
        print(f"\n--- {size} Resources ---")
        size_results = [r for r in results if r.data_size == size]

        # Sort by speed (fastest first)
        size_results.sort(key=lambda r: r.duration_seconds)

        fastest = size_results[0]

        print(f"{'Approach':<45} {'Time (s)':<12} {'Rows/sec':<12} {'vs Baseline':<12}")
        print("-" * 80)

        baseline_time = None
        for r in size_results:
            if "Single Inserts" in r.approach:
                baseline_time = r.duration_seconds
                break

        for r in size_results:
            speedup = ""
            if baseline_time and r.duration_seconds > 0:
                ratio = baseline_time / r.duration_seconds
                if ratio > 1:
                    speedup = f"{ratio:.1f}x faster"
                elif ratio < 1:
                    speedup = f"{1/ratio:.1f}x slower"
                else:
                    speedup = "baseline"

            is_fastest = r == fastest
            marker = " *" if is_fastest else ""
            print(
                f"{r.approach:<45} {r.duration_seconds:<12.3f} {r.rows_per_second:<12.0f} {speedup}{marker}"
            )

    # Overall recommendation
    print("\n" + "=" * 80)
    print("RECOMMENDATIONS")
    print("=" * 80)

    # Find best approach for largest dataset
    largest_size = max(sizes)
    largest_results = [r for r in results if r.data_size == largest_size]
    largest_results.sort(key=lambda r: r.duration_seconds)

    best = largest_results[0]
    baseline = next((r for r in largest_results if "Single Inserts" in r.approach), None)

    if baseline:
        speedup = baseline.duration_seconds / best.duration_seconds
        print(f"\nFor {largest_size} resources:")
        print(f"  Best approach: {best.approach}")
        print(f"  Speed: {best.rows_per_second:.0f} rows/sec ({speedup:.1f}x faster than current)")
        print(f"  Time: {best.duration_seconds:.3f}s vs {baseline.duration_seconds:.3f}s baseline")


# =============================================================================
# Main Entry Point
# =============================================================================


def main():
    """Run the benchmark."""
    print("=" * 80)
    print("FHIR Batch Upsert Performance Benchmark")
    print("=" * 80)

    # Check environment
    use_real = os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"
    if not use_real:
        print("\nWARNING: USE_REAL_DB_FOR_TESTS is not set to 'true'")
        print("Set USE_REAL_DB_FOR_TESTS=true to run benchmarks against real database")
        return

    # Load config from environment
    config = BenchmarkConfig(
        db_host=os.getenv("PG_FHIR_HOST", "fhir-server-db"),
        db_port=int(os.getenv("PG_FHIR_PORT", "5432")),
        db_name=os.getenv("PG_FHIR_DATABASE", "aidbox"),
        db_user=_required_env("PG_FHIR_USER"),
        db_password=_required_env("PG_FHIR_PASSWORD"),
        # Reduce test sizes for quicker runs - increase for more thorough testing
        test_sizes=[100, 1000, 10000, 50000],
        runs_per_test=3,
        chunk_sizes=[50, 500, 1000],
    )

    print(f"\nDatabase: {config.db_host}:{config.db_port}/{config.db_name}")
    print(f"Test sizes: {config.test_sizes}")
    print(f"Chunk sizes: {config.chunk_sizes}")
    print(f"Runs per test: {config.runs_per_test}")

    # Test connection
    print("\nTesting database connection...")
    try:
        with get_connection(config) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                version = cur.fetchone()[0]
                print(f"Connected! PostgreSQL version: {version[:50]}...")
    except Exception as e:
        print(f"Connection failed: {e}")
        print("\nMake sure the FHIR database is running.")
        return

    # Run benchmark
    results = run_benchmark(config)

    # Print summary
    print_summary(results)


if __name__ == "__main__":
    main()
