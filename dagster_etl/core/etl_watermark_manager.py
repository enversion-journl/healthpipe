# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ETL Watermark Manager

Tracks delta state for ETL operations. Records the last successful watermark
per (entity_type, schema, table) in [meta].[etl_watermark_state] and logs
every run lifecycle (start / success / failure) in [meta].[etl_watermark_log].

Tables:
    [meta].[etl_watermark_state] — current watermark per (entity_type, schema, table)
    [meta].[etl_watermark_log]   — run lifecycle log (start → complete/fail)

See infrastructure/ddl/tables/meta/02_etl_watermarks.sql

The watermark column is NVARCHAR(50). Callers can store any string —
typically a stringified integer (audit-MAX delta) or an ISO datetime
(timestamp delta). The default `advance_watermark()` (no value passed)
writes the current server time as an ISO datetime string for backward
compatibility with the original timestamp-based pattern.

Usage:
    mgr = EtlWatermarkManager(mssql_staging, context)
    watermark = mgr.get_watermark("fact", "fct", "Lagerbeholdning")
    log_id = mgr.log_start_run("fact", "fct", "Lagerbeholdning",
                               watermark_before=watermark,
                               dagster_run_id=context.run_id)
    try:
        # ... perform incremental load ...
        mgr.advance_watermark("fact", "fct", "Lagerbeholdning")               # default: SYSDATETIME()
        # or with an explicit value (e.g. computed audit-MAX):
        mgr.advance_watermark("archive", "arc", "spend_analysis_lines_kmd_uddata",
                              value="123456")
        mgr.log_complete_run(log_id, watermark_after=new_watermark, rows_affected=n)
    except Exception:
        mgr.log_fail_run(log_id)
        raise
"""

import dagster as dg
from core.resources import MSSQLResource


class EtlWatermarkManager:
    """
    Manages ETL watermark state for delta-load operations.

    Reads and writes to [meta].[etl_watermark_state] to track the last
    successful watermark per (entity_type, schema, table). Also logs every
    run lifecycle into [meta].[etl_watermark_log].

    The watermark value is an arbitrary NVARCHAR(50) string — callers
    interpret it (e.g. parse as int for audit-MAX delta, or parse as ISO
    datetime for timestamp delta). The default `advance_watermark()` writes
    SYSDATETIME() as an ISO string, server-side, to avoid client clock skew.
    """

    STATE_TABLE = "[meta].[etl_watermark_state]"
    LOG_TABLE = "[meta].[etl_watermark_log]"

    def __init__(
        self,
        mssql_resource: MSSQLResource,
        context: dg.AssetExecutionContext,
    ) -> None:
        self._mssql = mssql_resource
        self._context = context

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_watermark(
        self,
        entity_type: str,
        schema: str,
        table: str,
    ) -> str | None:
        """
        Get the current watermark value for a table.

        Args:
            entity_type: Entity type (e.g. 'archive', 'dimension', 'fact')
            schema: Schema name (e.g. 'stg')
            table: Table name (e.g. 'Lagerbeholdning')

        Returns:
            The stored watermark string, or None if no row exists (first run)
            or on database error (logs a warning in that case).
        """
        query = f"""
            SELECT watermark_value
            FROM {self.STATE_TABLE}
            WHERE entity_type = '{entity_type}'
              AND schema_name = '{schema}'
              AND table_name = '{table}'
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
                    "Falling back to full load."
                )
                return None
            finally:
                cursor.close()

    def advance_watermark(
        self,
        entity_type: str,
        schema: str,
        table: str,
        value: str | None = None,
    ) -> None:
        """
        Advance the watermark to a new value via MERGE upsert.

        When ``value`` is None (default), the watermark is set to
        ``SYSDATETIME()`` (server-side ISO datetime string) — preserves the
        legacy behaviour for timestamp-based callers.

        When ``value`` is provided, it is written verbatim. Callers are
        responsible for the format (e.g. stringified int for audit-MAX).

        Commits immediately after the MERGE.

        Args:
            entity_type: Entity type (e.g. 'archive', 'dimension', 'fact')
            schema: Schema name
            table: Table name
            value: Optional watermark value to store. None → SYSDATETIME().
        """
        if value is None:
            value_expr = "CONVERT(NVARCHAR(50), SYSDATETIME(), 121)"
        else:
            escaped = value.replace("'", "''")
            value_expr = f"N'{escaped}'"

        merge_sql = f"""
            MERGE {self.STATE_TABLE} AS tgt
            USING (
                SELECT
                    '{entity_type}' AS entity_type,
                    '{schema}'      AS schema_name,
                    '{table}'       AS table_name
            ) AS src
            ON  tgt.entity_type = src.entity_type
            AND tgt.schema_name = src.schema_name
            AND tgt.table_name  = src.table_name

            WHEN MATCHED THEN
                UPDATE SET
                    watermark_value = {value_expr},
                    updated_at      = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED THEN
                INSERT (entity_type, schema_name, table_name, watermark_value)
                VALUES ('{entity_type}', '{schema}', '{table}', {value_expr});
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(merge_sql)
                conn.commit()
            finally:
                cursor.close()

    def log_start_run(
        self,
        entity_type: str,
        schema: str,
        table: str,
        watermark_before: str | None = None,
        dagster_run_id: str | None = None,
    ) -> int:
        """
        Insert a 'running' log entry and return its auto-generated id.

        Args:
            entity_type: Entity type (e.g. 'archive', 'dimension', 'fact')
            schema: Schema name
            table: Table name
            watermark_before: The watermark value at the start of this run (optional)
            dagster_run_id: The Dagster run id for traceability (optional)

        Returns:
            The id of the newly inserted log row.
        """
        if watermark_before is None:
            wm_expr = "NULL"
        else:
            escaped = watermark_before.replace("'", "''")
            wm_expr = f"N'{escaped}'"

        run_id_expr = f"'{dagster_run_id}'" if dagster_run_id is not None else "NULL"

        insert_sql = f"""
            INSERT INTO {self.LOG_TABLE}
                (entity_type, schema_name, table_name,
                 status, watermark_before, dagster_run_id, run_started_at)
            OUTPUT INSERTED.id
            VALUES (
                '{entity_type}',
                '{schema}',
                '{table}',
                'running',
                {wm_expr},
                {run_id_expr},
                CAST(SYSDATETIME() AS datetime2(0))
            );
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(insert_sql)
                row = cursor.fetchone()
                conn.commit()
                return row[0]
            finally:
                cursor.close()

    def log_complete_run(
        self,
        log_id: int,
        watermark_after: str | None = None,
        rows_affected: int | None = None,
    ) -> None:
        """
        Mark a log entry as 'success' and record the completion timestamp.

        Args:
            log_id: The id returned by log_start_run()
            watermark_after: The new watermark value after this run (optional)
            rows_affected: Number of rows processed in this run (optional)
        """
        if watermark_after is None:
            wm_expr = "NULL"
        else:
            escaped = watermark_after.replace("'", "''")
            wm_expr = f"N'{escaped}'"

        rows_expr = f"{rows_affected}" if rows_affected is not None else "NULL"

        update_sql = f"""
            UPDATE {self.LOG_TABLE}
            SET
                status           = 'success',
                watermark_after  = {wm_expr},
                rows_affected    = {rows_expr},
                run_completed_at = CAST(SYSDATETIME() AS datetime2(0))
            WHERE id = {log_id};
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(update_sql)
                conn.commit()
            finally:
                cursor.close()

    def log_fail_run(self, log_id: int) -> None:
        """
        Mark a log entry as 'failed' and record the completion timestamp.

        Args:
            log_id: The id returned by log_start_run()
        """
        update_sql = f"""
            UPDATE {self.LOG_TABLE}
            SET
                status           = 'failed',
                run_completed_at = CAST(SYSDATETIME() AS datetime2(0))
            WHERE id = {log_id};
        """
        with self._mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(update_sql)
                conn.commit()
            finally:
                cursor.close()
