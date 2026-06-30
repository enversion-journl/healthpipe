# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR IFDB Database Indexing Assets.

This module provides assets for creating optimized database indexes to improve
ID resolution performance during ETL operations.

The indexes are created once and reused across all partition types (daily, monthly, full load).
"""

import json
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import check_sof_required_indexes
from utils.fhir.identifier_index_names import sanitize_system_for_identifier
from utils.fhir.index_config import (
    IDENTIFIER_UNIQUE_INDEX_SPECS,
    SOF_TS_RESOURCES,
    TXID_BTREE_RESOURCES,
)

# Safe-character validation for identifier system constants lives in
# utils.fhir.index_config and runs at import time for every entry
# in IDENTIFIER_UNIQUE_INDEX_SPECS — no need to duplicate it here.


_EXTRACT_IDENTIFIER_VALUE_DDL = """
CREATE OR REPLACE FUNCTION extract_identifier_value(
    resource jsonb, identifier_system text
) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT elem->>'value'
    FROM jsonb_array_elements(resource->'identifier') AS elem
    WHERE elem->>'system' = identifier_system
    LIMIT 1
$$;
"""


def _ensure_identifier_extract_function(cur: Any) -> None:
    # IMMUTABLE so PostgreSQL accepts it in index expressions; the underlying
    # jsonb_array_elements is STABLE, but the function is deterministic for
    # a given (resource, system) input which is all that matters for index
    # correctness.
    cur.execute(_EXTRACT_IDENTIFIER_VALUE_DDL)


def _drop_invalid_index_if_exists(cur: Any, index_name: str, logger: Any) -> None:
    """Drop an INVALID index left behind by a failed CREATE INDEX CONCURRENTLY."""
    cur.execute(
        "SELECT 1 FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = %s AND NOT i.indisvalid",
        (index_name,),
    )
    if cur.fetchone():
        logger.warning(f"[INDEXING] Dropping INVALID index {index_name} before re-creation")
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")


def _create_unique_identifier_index(
    cur: Any, resource_type: str, identifier_system: str, logger: Any
) -> str:
    table_name = resource_type.lower()
    system_safe = sanitize_system_for_identifier(identifier_system)
    index_name = f"idx_{table_name}_{system_safe}_uniq"
    system_filter = json.dumps([{"system": identifier_system}])

    _drop_invalid_index_if_exists(cur, index_name, logger)

    cur.execute(
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
        f"ON {table_name} ("
        f"    extract_identifier_value(resource, '{identifier_system}')"
        f") "
        f"WHERE resource->'identifier' @> '{system_filter}'::jsonb"
        f"  AND extract_identifier_value(resource, '{identifier_system}') "
        f"IS NOT NULL"
    )
    return index_name


@dg.asset(
    name="fhir_ifdb_database_indexes",
    group_name="fhir_infrastructure",
    description=(
        "Create optimized indexes for FHIR IFDB resources: UNIQUE expression indexes "
        "on identifier for idempotent upserts and fast targeted lookups, btree indexes "
        "on txid to speed up MAX(txid) lookups during batch upserts, and (ts) INCLUDE "
        "(cts) btree indexes to speed up partition-window extraction by the SQL on FHIR "
        "pipeline. Legacy GIN identifier indexes are dropped (the UNIQUE expression "
        "indexes supersede them for all query patterns)."
    ),
    io_manager_key="io_manager",
)
def fhir_ifdb_database_indexes(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """
    Create optimized indexes for FHIR IFDB resources to improve query performance.

    Phases:
      0. Drop legacy GIN identifier indexes (superseded by UNIQUE expression indexes)
      1. UNIQUE indexes on identifier for idempotent upserts
      2. txid btree indexes for MAX(txid) in batch upserts
      3. (ts) INCLUDE (cts) btree indexes for SQL on FHIR extract

    Uses a single connection in autocommit mode so that each DDL statement is
    its own transaction. This is required for CREATE INDEX CONCURRENTLY (avoids
    ACCESS EXCLUSIVE locks on large tables) and means a failure on one index
    does not roll back previously created indexes. A single ANALYZE pass per
    touched table runs at the end.
    """
    logger = dg.get_dagster_logger()
    logger.info("[INDEXING] Starting creation of optimized FHIR IFDB database indexes")

    indexes_created: list[str] = []
    indexes_dropped: list[str] = []
    errors: list[str] = []
    # UNIQUE-index issues are tracked separately because they reflect data
    # quality (duplicate identifiers in Aidbox) rather than infrastructure
    # failure. Surfacing them as warnings keeps the daily/monthly ETL
    # pipeline unblocked when historic data has a single bad row, while
    # still making the problem highly visible in Dagster metadata.
    unique_index_warnings: list[str] = []
    unique_indexes_skipped: list[str] = []
    tables_to_analyze: set[str] = set()

    unique_index_specs = IDENTIFIER_UNIQUE_INDEX_SPECS

    with fhir_database.get_connection() as conn:
        # autocommit required for CREATE INDEX CONCURRENTLY (cannot run inside
        # a transaction block). Each statement commits immediately.
        conn.autocommit = True

        with conn.cursor() as cur:
            # --- Phase 0: Drop legacy GIN identifier indexes ---
            # The _opt GIN indexes were superseded by the _uniq expression
            # indexes which the query planner always prefers (confirmed by
            # pg_stat_user_indexes showing 0 scans on every _opt index).
            # Keeping them wastes write I/O on every upsert and buffer cache
            # space on multi-GB tables.
            for resource_type, identifier_system in unique_index_specs:
                table_name = resource_type.lower()
                system_safe = sanitize_system_for_identifier(identifier_system)
                gin_index_name = f"idx_{table_name}_{system_safe}_opt"
                try:
                    cur.execute(
                        "SELECT 1 FROM pg_class WHERE relname = %s",
                        (gin_index_name,),
                    )
                    if cur.fetchone():
                        logger.info(f"[INDEXING] Dropping legacy GIN index {gin_index_name}")
                        cur.execute(f"DROP INDEX IF EXISTS {gin_index_name}")
                        indexes_dropped.append(gin_index_name)
                        logger.info(f"[INDEXING] Dropped legacy GIN index {gin_index_name}")
                except Exception as exc:
                    logger.warning(f"[INDEXING] Failed to drop GIN index {gin_index_name}: {exc}")

            # --- Phase 1: UNIQUE indexes on identifier for idempotent upserts ---
            try:
                _ensure_identifier_extract_function(cur)
            except Exception as exc:
                error_msg = f"Failed to create extract_identifier_value function: {exc}"
                logger.error(f"[INDEXING] {error_msg}")
                errors.append(error_msg)
            else:
                for resource_type, identifier_system in unique_index_specs:
                    table_name = resource_type.lower()
                    system_safe = sanitize_system_for_identifier(identifier_system)
                    index_name = f"idx_{table_name}_{system_safe}_uniq"

                    try:
                        logger.info(
                            f"[INDEXING] Creating UNIQUE index {index_name} " f"for {resource_type}"
                        )
                        _create_unique_identifier_index(
                            cur, resource_type, identifier_system, logger
                        )

                        indexes_created.append(f"{resource_type}:{identifier_system}:unique")
                        tables_to_analyze.add(table_name)
                        logger.info(f"[INDEXING] Created UNIQUE index for {resource_type}")
                    except Exception as exc:
                        # Treat UNIQUE-index DDL failures as warnings, not
                        # fatal. The most common cause is historic duplicate
                        # identifier values in Aidbox; failing the asset would
                        # block upstream daily/monthly ETL jobs until the dirty
                        # row is cleaned up manually. INVALID indexes left by
                        # failed CONCURRENTLY builds are auto-dropped on the
                        # next run by _drop_invalid_index_if_exists.
                        warning_msg = (
                            f"UNIQUE index {index_name} for {resource_type} "
                            f"failed (non-fatal): {exc}"
                        )
                        logger.warning(f"[INDEXING] {warning_msg}")
                        unique_index_warnings.append(warning_msg)
                        unique_indexes_skipped.append(f"{resource_type}:{identifier_system}")

            # --- Phase 2: txid btree indexes for MAX(txid) in batch upserts ---
            for resource_type in TXID_BTREE_RESOURCES:
                table_name = resource_type.lower()
                index_name = f"idx_{table_name}_txid"
                try:
                    logger.info(
                        f"[INDEXING] Creating btree index {index_name} " f"on {resource_type}(txid)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} " f"ON {table_name} (txid)"
                    )
                    indexes_created.append(f"{resource_type}:txid")
                    tables_to_analyze.add(table_name)
                    logger.info(f"[INDEXING] Created txid index for {resource_type}")
                except Exception as exc:
                    error_msg = f"Failed to create txid index for {resource_type}: {exc}"
                    logger.error(f"[INDEXING] {error_msg}")
                    errors.append(error_msg)

            # --- Phase 3: (ts) INCLUDE (cts) btree for SQL on FHIR extract ---
            logger.info(
                f"[INDEXING] Ensuring (ts) INCLUDE (cts) indexes on "
                f"{len(SOF_TS_RESOURCES)} raw resource tables"
            )
            for resource_type in SOF_TS_RESOURCES:
                table_name = resource_type.lower()
                index_name = f"idx_{table_name}_ts_cts"
                try:
                    logger.info(
                        f"[INDEXING] Creating btree index {index_name} "
                        f"on {resource_type}(ts) INCLUDE (cts)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} "
                        f"ON {table_name} (ts) INCLUDE (cts)"
                    )
                    indexes_created.append(f"{resource_type}:ts_cts")
                    tables_to_analyze.add(table_name)
                except Exception as exc:
                    error_msg = f"Failed to create ts/cts index for {resource_type}: {exc}"
                    logger.error(f"[INDEXING] {error_msg}")
                    errors.append(error_msg)

            # --- Single ANALYZE pass per table ---
            for table_name in sorted(tables_to_analyze):
                try:
                    logger.info(f"[INDEXING] ANALYZE {table_name}")
                    cur.execute(f"ANALYZE {table_name}")
                except Exception as exc:
                    logger.warning(f"[INDEXING] ANALYZE failed for {table_name}: {exc}")

    success_count = len(indexes_created)
    dropped_count = len(indexes_dropped)
    error_count = len(errors)
    warning_count = len(unique_index_warnings)

    logger.info(
        f"[INDEXING] Completed: {success_count} indexes created, "
        f"{dropped_count} legacy GIN indexes dropped, "
        f"{warning_count} unique-index warnings, {error_count} fatal errors"
    )

    missing_sof_index_warnings = check_sof_required_indexes(fhir_database, context)

    metadata: dict[str, dg.MetadataValue] = {
        "indexes_created": dg.MetadataValue.int(success_count),
        "gin_indexes_dropped": dg.MetadataValue.int(dropped_count),
        "errors": dg.MetadataValue.int(error_count),
        "unique_index_warnings": dg.MetadataValue.int(warning_count),
        "index_specs": dg.MetadataValue.json(indexes_created),
        "missing_sof_indexes_after_run": dg.MetadataValue.int(len(missing_sof_index_warnings)),
    }
    if indexes_dropped:
        metadata["gin_indexes_dropped_list"] = dg.MetadataValue.json(indexes_dropped)
    if missing_sof_index_warnings:
        metadata["missing_sof_index_details"] = dg.MetadataValue.json(missing_sof_index_warnings)
    if unique_indexes_skipped:
        metadata["unique_indexes_skipped"] = dg.MetadataValue.json(unique_indexes_skipped)
        metadata["unique_index_warning_details"] = dg.MetadataValue.json(unique_index_warnings)

    if errors:
        logger.warning(f"[INDEXING] Errors encountered: {'; '.join(errors)}")
        metadata["error_details"] = dg.MetadataValue.json(errors)
        raise dg.Failure(
            description=(f"{error_count} index creation(s) failed: {'; '.join(errors)}"),
            metadata=metadata,
        )

    return dg.MaterializeResult(metadata=metadata)
