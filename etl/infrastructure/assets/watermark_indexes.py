# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Watermark indexes for delta sync (programmatic).

Ensures a nonclustered index on (watermark_column, fallback_col) exists for every
table that uses mode='watermark' in sync config. This makes the linked-server
delta predicate COALESCE(watermark_column, fallback) >= @ts sargable on the source.

Column naming (see docs/implementation/dso_sync_guide §3.3, docs/architecture/dso_delta_load_strategy): standard is
dw_timestamp_update (primary) and dw_timestamp_insert (fallback).

Indexes are created automatically from DSO.load.sync_configs.get_watermark_sync_tables().
Run after table infrastructure (staging and DSO) so tables exist.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSO.load.sync_configs import get_watermark_sync_tables


def _index_name(schema: str, table: str) -> str:
    """Standard name for the watermark index (e.g. IX_dim_Fakturalinje_Watermark)."""
    return f"IX_{schema}_{table}_Watermark"


def _ensure_watermark_indexes(
    mssql_resource: MSSQLResource,
    context: dg.AssetExecutionContext | None,
) -> list[str]:
    """
    For each watermark sync table, create the watermark index if the table exists
    and the index does not. Returns list of (schema.table) where index was created.
    """
    created: list[str] = []
    tables = get_watermark_sync_tables()

    with mssql_resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for schema, table, watermark_column in tables:
                full_name = f"{schema}.{table}"
                index_name = _index_name(schema, table)

                # Check table exists (schema/table from sync config - safe to format)
                cursor.execute(
                    f"""
                    SELECT 1
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}'
                    """
                )
                if cursor.fetchone() is None:
                    if context:
                        context.log.debug(f"Skipping {full_name}: table does not exist")
                    continue

                # Check watermark column exists on the table (may not if audit rename migration hasn't run)
                cursor.execute(
                    f"""
                    SELECT 1 FROM sys.columns c
                    INNER JOIN sys.tables t ON c.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}' AND c.name = N'{watermark_column.replace("'", "''")}'
                    """
                )
                if cursor.fetchone() is None:
                    if context:
                        context.log.warning(
                            f"Skipping {full_name}: watermark column {watermark_column} does not exist"
                        )
                    continue

                # Check index already exists
                cursor.execute(
                    f"""
                    SELECT 1
                    FROM sys.indexes i
                    JOIN sys.tables t ON i.object_id = t.object_id
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}' AND i.name = N'{index_name.replace("'", "''")}'
                    """
                )
                if cursor.fetchone() is not None:
                    if context:
                        context.log.debug(
                            f"Skipping {full_name}: index {index_name} already exists"
                        )
                    continue

                # Second column = fallback in COALESCE(watermark, fallback). Standard: dw_timestamp_insert (not dw_timestamp_update). Organization: dw_timestamp_insert.
                cursor.execute(
                    f"""
                    SELECT 1 FROM sys.columns c
                    INNER JOIN sys.tables t ON c.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}' AND c.name = N'dw_timestamp_insert'
                    """
                )
                has_ts_insert = cursor.fetchone() is not None
                cursor.execute(
                    f"""
                    SELECT 1 FROM sys.columns c
                    INNER JOIN sys.tables t ON c.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = N'{schema.replace("'", "''")}' AND t.name = N'{table.replace("'", "''")}' AND c.name = N'dw_timestamp_insert'
                    """
                )
                has_insert_datetime = cursor.fetchone() is not None

                if has_ts_insert:
                    index_cols = f"([{watermark_column}] ASC, [dw_timestamp_insert] ASC)"
                elif has_insert_datetime:
                    index_cols = f"([{watermark_column}] ASC, [dw_timestamp_insert] ASC)"
                else:
                    index_cols = f"([{watermark_column}] ASC)"

                sql = (
                    f"CREATE NONCLUSTERED INDEX [{index_name}] "
                    f"ON [{schema}].[{table}] "
                    f"{index_cols}"
                )
                try:
                    cursor.execute(sql)
                    created.append(full_name)
                    if context:
                        context.log.info(f"Created index {index_name} on [{schema}].[{table}]")
                except Exception as e:
                    if context:
                        context.log.error(f"Failed to create {index_name} on {full_name}: {e}")
                    raise

            conn.commit()
        finally:
            cursor.close()

    return created


@dg.asset(
    name="staging_watermark_indexes",
    group_name="staging_infrastructure",
    compute_kind="mssql",
    deps=[
        "staging_infra_erp",
        "staging_infra_shared_dimensions",
        "staging_infra_organisation",
        "staging_infra_conceptmap",
    ],
    tags={"layer": "staging", "type": "infrastructure", "object": "index"},
    description="Ensure watermark indexes exist on staging for all watermark-delta sync tables",
)
def staging_watermark_indexes(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Create watermark indexes on staging (source for linked-server delta MERGE)."""

    created = _ensure_watermark_indexes(mssql_target_staging, context)
    context.log.info(f"Watermark indexes on staging: {len(created)} created")

    return dg.MaterializeResult(
        metadata={
            "indexes_created": created,
            "count": len(created),
        }
    )


@dg.asset(
    name="dso_watermark_indexes",
    group_name="dso_infrastructure",
    compute_kind="mssql",
    deps=[
        "dso_infra_erp",
        "dso_infra_shared_dimensions",
        "dso_infra_organisation",
        "dso_infra_conceptmap",
    ],
    tags={"layer": "dso", "type": "infrastructure", "object": "index"},
    description="Ensure watermark indexes exist on DSO for all watermark-delta sync tables",
)
def dso_watermark_indexes(
    context: dg.AssetExecutionContext,
    mssql_target_dso: MSSQLResource,
) -> dg.MaterializeResult:
    """Create watermark indexes on DSO (target; keeps schema aligned with staging)."""

    created = _ensure_watermark_indexes(mssql_target_dso, context)
    context.log.info(f"Watermark indexes on DSO: {len(created)} created")

    return dg.MaterializeResult(
        metadata={
            "indexes_created": created,
            "count": len(created),
        }
    )
