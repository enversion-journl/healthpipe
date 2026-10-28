# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSO Sync State Manager (legacy)

DEPRECATED for DSO table sync (dim/fct/fhir/…): use DfmManager with scope='dso'
on staging — see DSO.load.sync_dfm and dfm_design.md §8.

Still used by refresh_sfi_datapoint.py until the SFI follow-up US migrates SFI
incremental refresh off SyncWatermarks.

The watermark table DDL (see infrastructure/ddl/tables/meta/01_sync_watermarks.sql):
    CREATE TABLE [meta].[SyncWatermarks] (
        schema_name           NVARCHAR(128)   NOT NULL,
        table_name            NVARCHAR(128)   NOT NULL,
        last_sync_timestamp   DATETIME2(0)    NOT NULL,
        last_sync_rows        INT             NULL,
        last_sync_duration_ms INT             NULL,
        last_sync_status      NVARCHAR(50)    NULL,
        created_at            DATETIME2(0)    NOT NULL DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        updated_at            DATETIME2(0)    NOT NULL DEFAULT (CAST(SYSDATETIME() AS datetime2(0))),
        CONSTRAINT PK_SyncWatermarks PRIMARY KEY (schema_name, table_name)
    );

Deployed via the dso_infra_meta asset (group: dso_infrastructure).
"""

from datetime import datetime

import dagster as dg
from core.resources import MSSQLResource


class SyncStateManager:
    """
    Manages watermark state for delta sync between DSA and DSO.

    Reads and writes to [meta].[SyncWatermarks] on the DSO database to track
    the last successful sync timestamp per table. This timestamp is used as the
    watermark filter in build_delta_merge_statement().

    Usage:
        state_mgr = SyncStateManager(mssql_target_dso, context)
        last_ts = state_mgr.get_last_sync_timestamp("dim", "Kreditor")
        # ... perform sync ...
        state_mgr.update_sync_state("dim", "Kreditor", rows_affected=500)
    """

    WATERMARK_TABLE = "[meta].[SyncWatermarks]"

    def __init__(
        self,
        mssql_target_dso: MSSQLResource,
        context: dg.AssetExecutionContext,
    ) -> None:
        self._mssql = mssql_target_dso
        self._context = context

    def get_last_sync_timestamp(
        self,
        schema: str,
        table: str,
    ) -> datetime | None:
        """
        Get the last successful sync timestamp for a table.

        Args:
            schema: Schema name (e.g. 'dim')
            table: Table name (e.g. 'Kreditor')

        Returns:
            Last sync timestamp, or None if no previous sync exists (first run)
        """
        query = f"""
            SELECT last_sync_timestamp
            FROM {self.WATERMARK_TABLE}
            WHERE schema_name = '{schema}' AND table_name = '{table}'
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                row = cursor.fetchone()
                if row is not None:
                    return row[0]
                return None
            except Exception as e:
                self._context.log.warning(
                    f"Could not read watermark for {schema}.{table}: {e}. "
                    "Falling back to full sync."
                )
                return None
            finally:
                cursor.close()

    def update_sync_state(
        self,
        schema: str,
        table: str,
        rows_affected: int = 0,
        duration_ms: int | None = None,
        status: str = "success",
        watermark: datetime | None = None,
    ) -> None:
        """
        Update the sync state after a successful sync.

        Uses MERGE to insert or update the watermark row. ``watermark`` is the source
        high-water mark the run read up to; without it the watermark is SYSDATETIME() on
        the DSO server. Pass it whenever the source timestamps are compared against the
        watermark: those are written in UTC, SYSDATETIME() is server local time, and the
        end of a run is later than the newest row it read.

        Args:
            schema: Schema name (e.g. 'dim')
            table: Table name (e.g. 'Kreditor')
            rows_affected: Number of rows inserted/updated in this sync
            duration_ms: Duration of the sync in milliseconds (optional)
            status: Sync status string (default: 'success')
            watermark: Source high-water mark to store (optional)
        """
        duration_expr = f"{duration_ms}" if duration_ms is not None else "NULL"
        watermark_expr = (
            f"CAST('{watermark:%Y-%m-%d %H:%M:%S}' AS datetime2(0))"
            if watermark is not None
            else "CAST(SYSDATETIME() AS datetime2(0))"
        )

        merge_sql = f"""
            MERGE {self.WATERMARK_TABLE} AS tgt
            USING (
                SELECT
                    '{schema}' AS schema_name,
                    '{table}' AS table_name
            ) AS src
            ON tgt.schema_name = src.schema_name AND tgt.table_name = src.table_name

            WHEN MATCHED THEN
                UPDATE SET
                    last_sync_timestamp = {watermark_expr},
                    last_sync_rows = {rows_affected},
                    last_sync_duration_ms = {duration_expr},
                    last_sync_status = '{status}',
                    updated_at = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED THEN
                INSERT (schema_name, table_name, last_sync_timestamp,
                        last_sync_rows, last_sync_duration_ms, last_sync_status)
                VALUES ('{schema}', '{table}', {watermark_expr},
                        {rows_affected}, {duration_expr}, '{status}');
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(merge_sql)
                conn.commit()
            finally:
                cursor.close()

        self._context.log.info(
            f"Updated sync watermark for {schema}.{table} "
            f"(rows={rows_affected}, status={status})"
        )
