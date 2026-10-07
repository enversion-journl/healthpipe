# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Data Flow Management (DFM) Manager

Unified checkpoint tracking and run logging for all pipeline layers.

Tables:
    [meta].[DFMState]   — current checkpoint per (scope, target_type, schema, table)
    [meta].[DFMRunLog] — run lifecycle log (start → success / failed)

See infrastructure/ddl/tables/meta/02_dfm_state.sql
    infrastructure/ddl/tables/meta/03_dfm_run_log.sql

Usage — checkpoint owner (arc, dim, fct, fhir, fhir_server):
    dfm = DfmManager(mssql, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run("arc", "ifdb_encounter",
                                    dagster_run_id=context.run_id, mode="delta")
        try:
            # ... do work using run_ctx.checkpoint ...
            dfm.set_checkpoint("arc", "ifdb_encounter", value=new_cp)
            dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=new_cp, rows_affected=n)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

Usage — upstream / log-only (ext, stg, pre_stg):
    dfm = DfmManager(mssql, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run("ext", "ifdb_encounter",
                                    checkpoint_from=("arc", "ifdb_encounter"),
                                    dagster_run_id=context.run_id, mode="delta")
        # run_ctx.checkpoint comes from arc.ifdb_encounter's DFMState entry

Without the ``connection()`` context manager the old per-call pattern still works
(each method opens and closes its own connection).
"""

from contextlib import contextmanager
from typing import NamedTuple

import dagster as dg
from core.resources import MSSQLResource
from DSA.FHIR.utils.gdpr_masking import mask_cpr_numbers

SCHEMA_TO_TARGET_TYPE: dict[str, str] = {
    "ext": "extract",
    "arc": "archive",
    "pre_stg": "pre_staging",
    "stg": "staging",
    "dim": "dimension",
    "fct": "fact",
    "dm": "datamart",
    "map": "mapping",
    "bridge": "bridge",
    "fhir_stg": "fhir_staging",
    "fhir": "fhir_table",
    "fhir_server": "fhir_resource",
    "codesystem": "codesystem",
    "conceptmap": "conceptmap",
    "ref": "reference",
    "dfm": "dfm",
    "sec": "security",
}

CHECKPOINT_OWNER_SCHEMAS: frozenset[str] = frozenset(
    {"arc", "bridge", "conceptmap", "dim", "fct", "fhir", "fhir_server", "dfm", "ref"}
)


def _target_type_for_schema(schema: str) -> str:
    if schema not in SCHEMA_TO_TARGET_TYPE:
        raise ValueError(f"Unknown schema {schema!r} — add it to SCHEMA_TO_TARGET_TYPE")
    return SCHEMA_TO_TARGET_TYPE[schema]


class DfmRunContext(NamedTuple):
    """Returned by DfmManager.start_dfm_run()."""

    log_id: int
    checkpoint: str | None


class DfmManager:
    """Manages DFM (Data Flow Management) state and run logging.

    Reads and writes to [meta].[DFMState] and [meta].[DFMRunLog].
    All timestamps are set server-side via SYSDATETIME() to avoid clock skew.

    Use the :meth:`connection` context manager to reuse a single DB connection
    across all DFM calls within one asset run (reduces 3-5 round-trips to one)::

        dfm = DfmManager(mssql, context)
        with dfm.connection():
            run_ctx = dfm.start_dfm_run(...)
            dfm.set_checkpoint(...)
            dfm.complete_dfm_run(...)

    Without the context manager, each method opens and closes its own connection
    (backward-compatible, but higher latency).
    """

    STATE_TABLE = "[meta].[DFMState]"
    LOG_TABLE = "[meta].[DFMRunLog]"

    def __init__(
        self,
        mssql_resource: MSSQLResource,
        context: dg.AssetExecutionContext,
    ) -> None:
        self._mssql = mssql_resource
        self._context = context
        self._shared_conn = None

    @contextmanager
    def connection(self):
        """Hold a single DB connection for the lifetime of this block.

        All DfmManager methods called inside this block reuse the same
        connection instead of opening a new one per call.  Each method
        still commits independently so that partial state (e.g. a
        'running' log entry) is durable even if a later step fails.
        """
        with self._mssql.get_connection() as conn:
            self._shared_conn = conn
            try:
                yield self
            finally:
                self._shared_conn = None

    @contextmanager
    def _get_conn(self):
        """Yield the shared connection if active, otherwise open a fresh one."""
        if self._shared_conn is not None:
            yield self._shared_conn
        else:
            with self._mssql.get_connection() as conn:
                yield conn

    @staticmethod
    def _execute_commit(conn, sql: str, params: tuple) -> None:
        """Execute a single statement on conn and commit, closing the cursor."""
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            conn.commit()
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_checkpoint(
        self,
        schema: str,
        table: str,
        *,
        scope: str = "staging",
    ) -> str | None:
        """Read-only checkpoint peek. Does NOT create a log entry.

        Useful for multi-target upstream assets that need the MIN checkpoint
        across several downstream owners, or for debugging.

        Returns None if the row does not exist (first run). Raises on DB errors
        to prevent accidental full-load fallback from transient failures.
        """
        target_type = _target_type_for_schema(schema)
        query = (
            f"SELECT [checkpoint_value] FROM {self.STATE_TABLE} "
            f"WHERE [scope] = ? AND [target_type] = ? "
            f"AND [schema_name] = ? AND [table_name] = ?"
        )
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, (scope, target_type, schema, table))
                row = cursor.fetchone()
                return row[0] if row is not None else None
            finally:
                cursor.close()

    def start_dfm_run(
        self,
        schema: str,
        table: str,
        *,
        dagster_run_id: str,
        mode: str,
        checkpoint_from: tuple[str, str] | None = None,
        scope: str = "staging",
    ) -> DfmRunContext:
        """Primary entry point for all pipeline assets.

        Inserts a "running" entry into DFMRunLog and returns a
        DfmRunContext(log_id, checkpoint).

        Checkpoint resolution:
          - checkpoint_from is set (ext, stg, pre_stg): reads checkpoint from
            the specified downstream owner's DFMState entry.
          - checkpoint_from is None AND schema is a checkpoint owner: reads
            the checkpoint from the asset's own DFMState entry. Auto-creates
            the entry if it doesn't exist (first run).
          - checkpoint_from is None AND schema is NOT a checkpoint owner (map):
            checkpoint is None. No DFMState row is created or read.
        """
        target_type = _target_type_for_schema(schema)

        # Opportunistic sweep: close out log rows orphaned by hard-killed
        # processes so they don't sit in 'running' forever. Must never block
        # the actual run.
        try:
            swept = self.sweep_stale_runs()
            if swept:
                self._context.log.warning(
                    f"Swept {swept} stale 'running' DFMRunLog row(s) to 'abandoned'"
                )
        except Exception as sweep_exc:
            self._context.log.warning(f"Stale-run sweep failed (continuing): {sweep_exc}")

        # Resolve checkpoint
        checkpoint: str | None = None
        if checkpoint_from is not None:
            cp_schema, cp_table = checkpoint_from
            checkpoint = self.get_checkpoint(cp_schema, cp_table, scope=scope)
        elif schema in CHECKPOINT_OWNER_SCHEMAS:
            checkpoint = self._ensure_state_row_and_get_checkpoint(
                scope, target_type, schema, table, mode
            )

        # Insert log entry
        log_id = self._insert_run_log(
            scope=scope,
            target_type=target_type,
            schema=schema,
            table=table,
            mode=mode,
            checkpoint_before=checkpoint,
            dagster_run_id=dagster_run_id,
        )

        return DfmRunContext(log_id=log_id, checkpoint=checkpoint)

    def set_checkpoint(
        self,
        schema: str,
        table: str,
        *,
        value: str | None = None,
        mode: str = "delta",
        scope: str = "staging",
    ) -> None:
        """Set the checkpoint to an explicit value via MERGE upsert.

        Only called by checkpoint owners after successful processing.
        When value is None, SYSDATETIME() is used.

        Value and mode are passed as bound parameters to prevent SQL injection.
        Table/column names remain dynamic (they are internal constants, not
        user input).
        """
        target_type = _target_type_for_schema(schema)

        merge_sql = f"""
            MERGE {self.STATE_TABLE} AS tgt
            USING (
                SELECT
                    ? AS [scope],
                    ? AS [target_type],
                    ? AS [schema_name],
                    ? AS [table_name]
            ) AS src
            ON  tgt.[scope]       = src.[scope]
            AND tgt.[target_type] = src.[target_type]
            AND tgt.[schema_name] = src.[schema_name]
            AND tgt.[table_name]  = src.[table_name]
            WHEN MATCHED THEN
                UPDATE SET
                    [checkpoint_value] = COALESCE(?, CONVERT(NVARCHAR(50), SYSDATETIME(), 121)),
                    [last_mode]        = ?,
                    [updated_at]       = CAST(SYSDATETIME() AS datetime2(0))
            WHEN NOT MATCHED THEN
                INSERT ([scope], [target_type], [schema_name], [table_name],
                        [checkpoint_value], [last_mode])
                VALUES (?, ?, ?, ?,
                        COALESCE(?, CONVERT(NVARCHAR(50), SYSDATETIME(), 121)),
                        ?);
        """
        params = (
            scope,
            target_type,
            schema,
            table,
            value,
            mode,
            scope,
            target_type,
            schema,
            table,
            value,
            mode,
        )
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(merge_sql, params)
                conn.commit()
            finally:
                cursor.close()

    def complete_dfm_run(
        self,
        log_id: int,
        *,
        checkpoint_after: str | None = None,
        rows_affected: int | None = None,
    ) -> None:
        """Mark a run log entry as 'success' and record completion metadata."""
        update_sql = f"""
            UPDATE {self.LOG_TABLE}
            SET
                [status]           = 'success',
                [checkpoint_after] = ?,
                [rows_affected]    = ?,
                [run_completed_at] = CAST(SYSDATETIME() AS datetime2(0))
            WHERE [id] = ?;
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(update_sql, (checkpoint_after, rows_affected, log_id))
                conn.commit()
            finally:
                cursor.close()

    def fail_dfm_run(
        self,
        log_id: int,
        *,
        error_message: str | None = None,
    ) -> None:
        """Mark a run log entry as 'failed' and record the error.

        Never raises: this method is called from except paths, where an error
        here (e.g. the shared connection died together with the original
        failure) would mask the asset's real exception. If the shared
        connection fails, one retry is made on a fresh connection; if that
        also fails, a warning is logged and the row is left 'running' —
        :meth:`sweep_stale_runs` closes it out later.
        """
        # error_message is typically str(exc); a driver error can echo offending
        # row values. Mask CPR before it is persisted to [meta].[DFMRunLog].
        if error_message is not None:
            error_message = mask_cpr_numbers(error_message)
        update_sql = f"""
            UPDATE {self.LOG_TABLE}
            SET
                [status]           = 'failed',
                [error_message]    = ?,
                [run_completed_at] = CAST(SYSDATETIME() AS datetime2(0))
            WHERE [id] = ?;
        """
        params = (error_message, log_id)
        try:
            with self._get_conn() as conn:
                self._execute_commit(conn, update_sql, params)
            return
        except Exception as exc:
            first_exc = exc

        # The shared connection may have died with the original failure —
        # retry once on a fresh connection so the row still gets marked.
        if self._shared_conn is not None:
            try:
                with self._mssql.get_connection() as conn:
                    self._execute_commit(conn, update_sql, params)
                return
            except Exception:
                pass

        self._context.log.warning(
            f"fail_dfm_run: could not mark DFMRunLog id={log_id} as failed "
            f"({first_exc!r}); the row will be closed out by the stale-run sweeper"
        )

    STALE_RUN_HOURS = 24

    def sweep_stale_runs(self, *, stale_hours: int = STALE_RUN_HOURS) -> int:
        """Close out DFMRunLog rows stuck in 'running'.

        A row that has been 'running' for longer than ``stale_hours`` belongs
        to a process that died before reaching complete/fail (hard kill, OOM,
        connection lost in the failure path). Such rows are marked
        'abandoned' so the run log reflects reality.

        Called opportunistically at the start of every run (start_dfm_run).
        Returns the number of rows swept.
        """
        update_sql = f"""
            UPDATE {self.LOG_TABLE}
            SET
                [status]           = 'abandoned',
                [error_message]    = 'Swept by sweep_stale_runs: still ''running'' past the staleness threshold (process likely killed before complete/fail)',
                [run_completed_at] = CAST(SYSDATETIME() AS datetime2(0))
            WHERE [status] = 'running'
              AND [run_started_at] < DATEADD(hour, -?, SYSDATETIME());
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(update_sql, (stale_hours,))
                swept = cursor.rowcount or 0
                conn.commit()
                return swept
            finally:
                cursor.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ensure_state_row_and_get_checkpoint(
        self,
        scope: str,
        target_type: str,
        schema: str,
        table: str,
        mode: str,
    ) -> str | None:
        """For checkpoint owners: read existing checkpoint or auto-create row on first run.

        Returns None only when the row does not exist yet (first run).
        Raises on DB errors to prevent accidental full-load fallback.
        """
        query = (
            f"SELECT [checkpoint_value] FROM {self.STATE_TABLE} "
            f"WHERE [scope] = ? AND [target_type] = ? "
            f"AND [schema_name] = ? AND [table_name] = ?"
        )
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query, (scope, target_type, schema, table))
                row = cursor.fetchone()
                if row is not None:
                    return row[0]

                insert_sql = (
                    f"INSERT INTO {self.STATE_TABLE} "
                    f"([scope], [target_type], [schema_name], [table_name], "
                    f"[checkpoint_value], [last_mode]) "
                    f"VALUES (?, ?, ?, ?, NULL, ?)"
                )
                cursor.execute(insert_sql, (scope, target_type, schema, table, mode))
                conn.commit()
                self._context.log.info(
                    f"First run: created DFMState entry for {scope}/{schema}.{table}"
                )
                return None
            finally:
                cursor.close()

    def _insert_run_log(
        self,
        *,
        scope: str,
        target_type: str,
        schema: str,
        table: str,
        mode: str,
        checkpoint_before: str | None,
        dagster_run_id: str | None,
    ) -> int:
        """Insert a 'running' row into DFMRunLog and return its id."""
        insert_sql = f"""
            INSERT INTO {self.LOG_TABLE}
                ([scope], [target_type], [schema_name], [table_name],
                 [mode], [status], [checkpoint_before],
                 [dagster_run_id], [run_started_at])
            OUTPUT INSERTED.[id]
            VALUES (?, ?, ?, ?, ?, 'running', ?, ?,
                    CAST(SYSDATETIME() AS datetime2(0)));
        """
        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    insert_sql,
                    (
                        scope,
                        target_type,
                        schema,
                        table,
                        mode,
                        checkpoint_before,
                        dagster_run_id,
                    ),
                )
                row = cursor.fetchone()
                conn.commit()
                return row[0]
            finally:
                cursor.close()
