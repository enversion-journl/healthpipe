# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive MERGE utilities.

build_archive_merge_sql() generates readable T-SQL MERGE from an ArchiveMergeConfig.
execute_archive_merge()   is the shared execution lifecycle for all archive assets.

The generated SQL is designed to be copy-pasteable into SSMS for investigation.
"""

import dataclasses
from typing import NamedTuple

import dagster as dg
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


class ArchiveMergeResult(NamedTuple):
    """Result of execute_archive_merge with effective runtime settings."""

    row_count: int
    new_checkpoint: str | None
    detect_deletes: bool
    update_only_on_change: bool
    strategy: str
    upstream_is_full_load: bool
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_soft_deleted: int = 0
    rows_dedup_dropped: int = 0


def build_archive_merge_sql(config: ArchiveMergeConfig, *, track_actions: bool = False) -> str:
    """Generate a T-SQL MERGE statement from an ArchiveMergeConfig.

    The output is formatted for readability and manual debugging.

    When *track_actions* is True the MERGE is wrapped in a batch that
    captures per-row actions via ``OUTPUT $action`` into a table variable
    and returns a summary row (inserted, updated, deleted counts) as the
    next result set.
    """
    if config.strategy == "insert_only":
        return _build_insert_only_sql(config)
    return _build_upsert_sql(config, track_actions=track_actions)


def _build_source_expr(config: ArchiveMergeConfig) -> str:
    """The source the MERGE/fast-load reads from — the ext table, or a deduped view of it.

    With ``dedup_source_on_keys`` the ext table is wrapped in a ROW_NUMBER
    derived table that keeps one row per ``merge_keys`` value. Without it a
    single duplicate pair in a source that has no upstream primary key aborts
    the whole MERGE on the arc PK (Violation of PRIMARY KEY constraint).

    The winner sorts first on ``non_key_columns`` DESC — deterministic across
    reruns, and populated rows beat NULL-heavy ones. When every column is a
    merge key there is nothing to order by and nothing to lose — the duplicates
    are identical — so the ordering degenerates to a constant.
    """
    plain = f"[{config.source_schema}].[{config.source_table}]"
    if not config.dedup_source_on_keys:
        return plain

    columns = list(config.merge_keys) + list(config.non_key_columns)
    if config.source_delete_column and config.source_delete_column not in columns:
        columns.append(config.source_delete_column)

    column_list = ", ".join(f"[{c}]" for c in columns)
    partition_by = ", ".join(f"[{c}]" for c in config.merge_keys)
    # DESC: T-SQL sorts NULL lowest, so descending puts populated rows first —
    # when a duplicate group disagrees, the row WITH values wins, never the
    # NULL-heavy one.
    order_by = ", ".join(f"[{c}] DESC" for c in config.non_key_columns) or "(SELECT NULL)"

    return (
        "(\n"
        f"        SELECT {column_list}\n"
        "        FROM (\n"
        f"            SELECT {column_list},\n"
        f"                   ROW_NUMBER() OVER (PARTITION BY {partition_by}\n"
        f"                                      ORDER BY {order_by}) AS [_dedup_rn]\n"
        f"            FROM {plain}\n"
        "        ) AS deduped\n"
        "        WHERE deduped.[_dedup_rn] = 1\n"
        "    )"
    )


def _build_change_check(config: ArchiveMergeConfig) -> str:
    """Build a NULL-safe change-detection condition for WHEN MATCHED AND (...).

    Key-only tables (empty non_key_columns) have nothing that can change, so
    the base condition is a constant false — appended soft-delete clauses
    (resurrection) remain the only way such a row can update. An explicit
    change_detect_columns list, including an empty one, is used as-is.
    """
    columns = (
        config.change_detect_columns
        if config.change_detect_columns is not None
        else config.non_key_columns
    )
    comparisons = []
    for c in columns:
        comparisons.append(
            f"ISNULL(NULLIF(tgt.[{c}], src.[{c}]), " f"NULLIF(src.[{c}], tgt.[{c}])) IS NOT NULL"
        )
    if not comparisons:
        return "1 = 0"
    return "\n            OR ".join(comparisons)


def _build_upsert_sql(config: ArchiveMergeConfig, *, track_actions: bool = False) -> str:
    src = _build_source_expr(config)
    tgt = f"[{config.target_schema}].[{config.target_table}]"

    on_clause = " AND ".join(f"tgt.[{k}] = src.[{k}]" for k in config.merge_keys)

    all_cols = list(config.merge_keys) + list(config.non_key_columns)
    insert_columns = ", ".join(f"[{c}]" for c in all_cols)
    insert_values = ", ".join(f"src.[{c}]" for c in all_cols)

    change_guard = ""
    if config.update_only_on_change:
        change_check = _build_change_check(config)
        if config.detect_deletes and not config.source_delete_column:
            change_check += f"\n            OR tgt.[{config.soft_delete_column}] <> 0"
        change_guard = f" AND (\n            {change_check}\n        )"

    delete_insert_col = ""
    delete_insert_val = ""
    update_lines = [f"tgt.[{c}] = src.[{c}]" for c in config.non_key_columns]
    if config.source_delete_column:
        sc = config.source_delete_column
        sdc = config.soft_delete_column
        case_expr = (
            f"CASE WHEN src.[{sc}] IS NOT NULL "
            f"AND src.[{sc}] NOT IN ('0', '0.0') THEN 1 ELSE 0 END"
        )
        update_lines.append(f"tgt.[{sdc}] = {case_expr}")
        delete_insert_col = f", [{sdc}]"
        delete_insert_val = f",\n        {case_expr}"
    elif config.detect_deletes:
        update_lines.append(f"tgt.[{config.soft_delete_column}] = 0")
    update_lines.append("tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))")
    update_assignments = ",\n        ".join(update_lines)

    merge_body = f"""MERGE {tgt} AS tgt
USING {src} AS src
    ON {on_clause}
WHEN MATCHED{change_guard} THEN UPDATE SET
        {update_assignments}
WHEN NOT MATCHED BY TARGET THEN INSERT (
        {insert_columns},
        [dw_timestamp_insert], [dw_timestamp_update]{delete_insert_col}
    )
    VALUES (
        {insert_values},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)){delete_insert_val}
    )"""

    if config.detect_deletes:
        delete_scope = ""
        if config.delete_scope_predicate:
            delete_scope = f" AND ({config.delete_scope_predicate})"
        merge_body += f"""
WHEN NOT MATCHED BY SOURCE AND tgt.[{config.soft_delete_column}] = 0{delete_scope} THEN UPDATE SET
        tgt.[{config.soft_delete_column}] = 1,
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))"""

    if not track_actions:
        return merge_body + ";\n"

    sdc = config.soft_delete_column
    has_soft_delete = config.detect_deletes or config.source_delete_column is not None
    if has_soft_delete:
        return (
            "DECLARE @_merge_actions TABLE ([action] NVARCHAR(10), [is_soft_delete] BIT);\n"
            + merge_body
            + "\nOUTPUT $action,\n"
            + "       CASE WHEN $action = 'UPDATE'\n"
            + f"            AND inserted.[{sdc}] = 1\n"
            + f"            AND deleted.[{sdc}] = 0\n"
            + "            THEN 1 ELSE 0 END\n"
            + "INTO @_merge_actions([action], [is_soft_delete]);\n"
            + "SELECT\n"
            + "    SUM(CASE WHEN [action] = 'INSERT' THEN 1 ELSE 0 END),\n"
            + "    SUM(CASE WHEN [action] = 'UPDATE' AND [is_soft_delete] = 0 THEN 1 ELSE 0 END),\n"
            + "    SUM(CASE WHEN [is_soft_delete] = 1 THEN 1 ELSE 0 END)\n"
            + "FROM @_merge_actions;\n"
        )
    else:
        return (
            "DECLARE @_merge_actions TABLE ([action] NVARCHAR(10));\n"
            + merge_body
            + "\nOUTPUT $action INTO @_merge_actions;\n"
            + "SELECT\n"
            + "    SUM(CASE WHEN [action] = 'INSERT' THEN 1 ELSE 0 END),\n"
            + "    SUM(CASE WHEN [action] = 'UPDATE' THEN 1 ELSE 0 END),\n"
            + "    0\n"
            + "FROM @_merge_actions;\n"
        )


def _build_insert_only_sql(config: ArchiveMergeConfig) -> str:
    tgt = f"[{config.target_schema}].[{config.target_table}]"
    src = f"[{config.source_schema}].[{config.source_table}]"

    all_cols = list(config.merge_keys) + list(config.non_key_columns)
    insert_columns = ", ".join(f"[{c}]" for c in all_cols)
    select_columns = ", ".join(f"[{c}]" for c in all_cols)

    sql = f"""INSERT INTO {tgt} (
        {insert_columns},
        [dw_timestamp_insert], [dw_timestamp_update]
    )
SELECT
        {select_columns},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM {src};
"""
    return sql


def _build_empty_target_fastload_sql(config: ArchiveMergeConfig) -> str:
    """Build the single INSERT...SELECT for a cold first load into an empty arc.

    When the target arc table is empty, a MERGE degenerates to an all-INSERT
    pass — but still pays for the OUTPUT ``$action`` table-variable tracking and
    the per-row change-detection predicate. This statement copies the disposable
    ext heap into arc in one pass instead:

        INSERT INTO arc WITH (TABLOCK) (<cols>, ...)
        SELECT <cols>, <insert ts>, <update ts> FROM ext

    ext is left untouched — matching the MERGE path, where ext also retains the
    rows it delivered. The next extract drops and recreates ext anyway.

    Why a single statement and not a batched ``DELETE TOP (n) ... OUTPUT ...
    INTO arc`` drain: DELETE does not deallocate heap pages when the rows leave
    via ``OUTPUT ... INTO`` (even WITH (TABLOCK)), and ``DELETE TOP`` with no
    ORDER BY is an allocation-order scan restarting from the front — so each
    batch re-reads the growing prefix of dead pages and the drain is O(n²).
    The single INSERT...SELECT reads the heap exactly once — O(n) — and under
    SIMPLE/BULK_LOGGED recovery the TABLOCK insert into an empty clustered
    target with no enabled nonclustered indexes is minimally logged.
    """
    src = _build_source_expr(config)
    if config.dedup_source_on_keys:
        # A derived table needs a name of its own in the FROM clause; the MERGE
        # path gets one from its "AS src".
        src = f"{src} AS src"
    tgt = f"[{config.target_schema}].[{config.target_table}]"

    all_cols = list(config.merge_keys) + list(config.non_key_columns)
    insert_cols = ",\n        ".join(f"[{c}]" for c in all_cols)
    select_cols = ",\n        ".join(f"[{c}]" for c in all_cols)
    ts = "CAST(SYSDATETIME() AS datetime2(0))"

    return f"""INSERT INTO {tgt} WITH (TABLOCK) (
        {insert_cols},
        [dw_timestamp_insert], [dw_timestamp_update]
    )
SELECT
        {select_cols},
        {ts}, {ts}
FROM {src};
"""


def _nonclustered_index_names(cursor, schema: str, table: str) -> list[str]:
    """Return the disable-able nonclustered index names on a table.

    Excludes the clustered PK (disabling it would take the table offline) and
    unique constraints/indexes (whose uniqueness we must keep enforced during
    the load, and whose REBUILD would fail on a duplicate).
    """
    cursor.execute(
        "SELECT i.name FROM sys.indexes i "
        f"WHERE i.object_id = OBJECT_ID('[{schema}].[{table}]') "
        "AND i.type_desc = 'NONCLUSTERED' "
        "AND i.is_primary_key = 0 AND i.is_unique_constraint = 0 "
        "AND i.is_unique = 0 AND i.name IS NOT NULL"
    )
    return [row[0] for row in cursor.fetchall()]


def _run_empty_target_fastload(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    config: ArchiveMergeConfig,
) -> int:
    """Copy ext into an empty arc table via one INSERT...SELECT.

    Returns the number of rows inserted. ext is not consumed — like the MERGE
    path, it retains its rows until the next extract drops and recreates it.

    Failure semantics: if the INSERT fails it rolls back whole — arc is still
    empty and ext untouched, so a rerun takes this fast path again from scratch.
    If the run dies after the INSERT committed but before the checkpoint
    advanced, a rerun sees a non-empty target and falls back to MERGE, which
    re-upserts the still-present ext rows idempotently (they match on
    merge_keys).

    Nonclustered indexes on the target are DISABLEd before the load and REBUILT
    afterwards. This is both a performance measure (one sorted rebuild instead
    of per-row B-tree maintenance across tens of millions of inserts) and a
    precondition for minimal logging of the TABLOCK insert. The rebuild runs in
    a ``finally`` so the indexes are never left disabled — on failure the table
    is empty and the rebuild is instant.
    """
    insert_sql = _build_empty_target_fastload_sql(config)
    context.log.info(
        f"Empty-target fast load (single INSERT...SELECT WITH (TABLOCK)):\n{insert_sql}"
    )

    schema, table = config.target_schema, config.target_table
    total = 0
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        disabled: list[str] = []
        try:
            disabled = _nonclustered_index_names(cursor, schema, table)
            if disabled:
                for name in disabled:
                    cursor.execute(f"ALTER INDEX [{name}] ON [{schema}].[{table}] DISABLE;")
                conn.commit()
                context.log.info(
                    f"Disabled {len(disabled)} nonclustered index(es) on "
                    f"{schema}.{table} for fast load: {', '.join(disabled)}"
                )

            cursor.execute(insert_sql)
            total = cursor.rowcount or 0
            conn.commit()
            context.log.info(f"Fast load complete: {total} rows inserted into {schema}.{table}")
        finally:
            for name in disabled:
                cursor.execute(f"ALTER INDEX [{name}] ON [{schema}].[{table}] REBUILD;")
                conn.commit()
            if disabled:
                context.log.info(
                    f"Rebuilt {len(disabled)} nonclustered index(es) on "
                    f"{schema}.{table}: {', '.join(disabled)}"
                )
            cursor.close()
    return total


def _derive_new_checkpoint(
    mssql: MSSQLResource,
    run_config: RunModeConfig,
    config: ArchiveMergeConfig,
    flow_start: str | None,
) -> str | None:
    """Resolve the new checkpoint value after a successful load.

    Resolution order (see execute_archive_merge docstring): period date_to →
    flow_start → checkpoint_query → None. Only checkpoint_query touches the
    database, and it runs after the load has committed, so it observes the newly
    written rows.
    """
    if run_config.mode == "period" and run_config.date_to:
        # In period mode the extract's real upper bound is date_to — NOT the
        # flow-start ceiling (which tracks "now") nor SYSDATETIME(). Pinning the
        # checkpoint to the end of the processed period keeps a chunked
        # historical reload resumable: if chunk N fails, the checkpoint reflects
        # chunk N-1's boundary, and even a *missed* manual resume self-heals
        # because the next scheduled delta run picks up everything after that
        # boundary. (Sub-second rows on the date_to day may be re-processed by
        # the next delta run — harmless, the MERGE is an idempotent upsert.)
        return f"{run_config.date_to} 23:59:59"
    if flow_start is not None:
        return flow_start
    if config.checkpoint_query:
        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(config.checkpoint_query)
                row = cursor.fetchone()
                return row[0] if row is not None else None
            finally:
                cursor.close()
    return None


def execute_archive_merge(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    merge_config: ArchiveMergeConfig,
    run_config: RunModeConfig,
) -> ArchiveMergeResult:
    """Shared execution logic for all archive merge assets.

    The archive asset is a checkpoint owner — it reads its own checkpoint
    from DFMState and sets it on success.

    Checkpoint resolution order:
      0. period mode with date_to — pins the checkpoint to the end of the
         processed period (``"{date_to} 23:59:59"``). In period mode the
         extract's real upper bound is date_to, not the flow-start ceiling
         (which tracks "now"), so this keeps chunked historical reloads
         resumable and self-healing. Takes precedence over flow_start /
         checkpoint_query. (An open-ended period with no date_to falls through
         to the rules below.)
      1. flow_start_key (from ArchiveMergeConfig) — reads a flow-start
         checkpoint from DFMState and uses it as the new checkpoint.
         This ties the archive checkpoint to the upper bound used by the
         preceding extract, ensuring gap-free delta windows.
      2. checkpoint_query — executes a SQL query after MERGE to derive
         the new checkpoint (e.g. MAX of a delta column).
      3. Fallback — uses SYSDATETIME() as the new checkpoint.

    Returns an ArchiveMergeResult with row counts, checkpoint, and effective
    config flags.
    """
    dfm = DfmManager(mssql, context)
    with dfm.connection():
        run_ctx = dfm.start_dfm_run(
            merge_config.target_schema,
            merge_config.target_table,
            dagster_run_id=context.run_id,
            mode=run_config.mode,
        )

        try:
            effective = merge_config
            is_upsert = effective.strategy != "insert_only"

            # Empty-target fast path probe (see fast load below). Opt-in via
            # enable_bulk_insert_on_first_load, since it inserts ext rows
            # directly and relies on them being unique on merge_keys. Runs first:
            # an empty target has nothing to soft-delete, so delete detection is
            # moot and its COUNT(*) probe can be skipped entirely.
            use_fast_load = False
            if is_upsert and effective.merge_keys and effective.enable_bulk_insert_on_first_load:
                with mssql.get_connection() as conn:
                    probe = conn.cursor()
                    try:
                        probe.execute(
                            f"SELECT TOP (1) 1 FROM "
                            f"[{effective.target_schema}].[{effective.target_table}]"
                        )
                        use_fast_load = probe.fetchone() is None
                    finally:
                        probe.close()

            source_is_complete = (
                merge_config.upstream_is_full_load or run_config.mode == "full_load"
            )
            if not use_fast_load and merge_config.detect_deletes and not source_is_complete:
                context.log.warning(
                    f"Delete detection disabled — mode={run_config.mode!r} "
                    f"and upstream_is_full_load=False; "
                    f"NOT MATCHED BY SOURCE would incorrectly flag rows."
                )
                effective = dataclasses.replace(merge_config, detect_deletes=False)

            if not use_fast_load and effective.detect_deletes:
                src = f"[{effective.source_schema}].[{effective.source_table}]"
                with mssql.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {src}")
                        ext_count = cursor.fetchone()[0]
                    finally:
                        cursor.close()
                if ext_count == 0:
                    context.log.warning(
                        f"Source {src} has 0 rows — refusing to soft-delete all "
                        f"archive rows. This usually indicates a failed upstream "
                        f"extract, not a genuine empty source."
                    )
                    effective = dataclasses.replace(effective, detect_deletes=False)

            rows_dedup_dropped = 0
            if is_upsert and effective.dedup_source_on_keys:
                src = f"[{effective.source_schema}].[{effective.source_table}]"
                keys = ", ".join(f"[{k}]" for k in effective.merge_keys)
                with mssql.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(
                            f"SELECT COUNT(*) - (SELECT COUNT(*) FROM "
                            f"(SELECT DISTINCT {keys} FROM {src}) d) FROM {src}"
                        )
                        rows_dedup_dropped = int(cursor.fetchone()[0] or 0)
                    finally:
                        cursor.close()
                if rows_dedup_dropped:
                    context.log.warning(
                        f"Source dedup will drop {rows_dedup_dropped} duplicate-key "
                        f"rows from {src}. A non-zero drop means the source delivered "
                        f"several rows per merge key — verify they are identical "
                        f"copies; if they differ, the asserted grain is wrong."
                    )

            will_advance = run_config.should_advance_checkpoint
            context.log.info(
                f"Effective archive config — "
                f"strategy={effective.strategy}, "
                f"detect_deletes={effective.detect_deletes}, "
                f"update_only_on_change={effective.update_only_on_change}, "
                f"upstream_is_full_load={merge_config.upstream_is_full_load}, "
                f"advance_checkpoint={will_advance} "
                f"(explicit={run_config.advance_checkpoint}), "
                f"mode={run_config.mode}"
            )

            flow_start: str | None = None
            if merge_config.flow_start_key is not None:
                fs_schema, fs_table = merge_config.flow_start_key
                flow_start = dfm.get_checkpoint(fs_schema, fs_table)
                context.log.info(f"Flow-start checkpoint ({fs_schema}.{fs_table}): {flow_start}")

            rows_inserted = rows_updated = rows_soft_deleted = 0

            if use_fast_load:
                # On a cold first load the MERGE would degenerate to all-INSERT
                # yet still pay for OUTPUT $action tracking and per-row change
                # detection. Copy the ext heap with one minimally-logged
                # INSERT...SELECT instead.
                context.log.info(
                    f"Target arc.{effective.target_table} is empty — using a "
                    f"single INSERT...SELECT fast load instead of MERGE."
                )
                rows_inserted = _run_empty_target_fastload(context, mssql, effective)
                row_count = rows_inserted
            else:
                merge_sql = build_archive_merge_sql(effective, track_actions=is_upsert)
                context.log.info(f"Archive MERGE SQL:\n{merge_sql}")

                with mssql.get_connection() as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(merge_sql)

                        if is_upsert:
                            cursor.nextset()
                            action_row = cursor.fetchone()
                            if action_row:
                                rows_inserted = action_row[0] or 0
                                rows_updated = action_row[1] or 0
                                rows_soft_deleted = action_row[2] or 0
                            row_count = rows_inserted + rows_updated + rows_soft_deleted
                        else:
                            row_count = cursor.rowcount
                            rows_inserted = row_count

                        conn.commit()
                    finally:
                        cursor.close()

            # Checkpoint derivation is shared by both paths and runs after the
            # load has committed (checkpoint_query, if any, observes new rows).
            new_checkpoint = _derive_new_checkpoint(mssql, run_config, effective, flow_start)

            context.log.info(
                f"Archive action counts — "
                f"inserted: {rows_inserted}, updated: {rows_updated}, "
                f"soft_deleted: {rows_soft_deleted}"
            )

            logged_checkpoint: str | None = None
            if will_advance and new_checkpoint is not None:
                dfm.set_checkpoint(
                    merge_config.target_schema,
                    merge_config.target_table,
                    value=new_checkpoint,
                    mode=run_config.mode,
                )
                logged_checkpoint = new_checkpoint
            elif (
                will_advance
                and new_checkpoint is None
                and effective.checkpoint_query is None
                and effective.flow_start_key is None
            ):
                dfm.set_checkpoint(
                    merge_config.target_schema,
                    merge_config.target_table,
                    mode=run_config.mode,
                )
                logged_checkpoint = dfm.get_checkpoint(
                    merge_config.target_schema, merge_config.target_table
                )

            dfm.complete_dfm_run(
                run_ctx.log_id,
                checkpoint_after=logged_checkpoint,
                rows_affected=row_count,
            )
            return ArchiveMergeResult(
                row_count=row_count,
                new_checkpoint=new_checkpoint,
                detect_deletes=effective.detect_deletes,
                update_only_on_change=effective.update_only_on_change,
                strategy=effective.strategy,
                upstream_is_full_load=merge_config.upstream_is_full_load,
                rows_inserted=rows_inserted,
                rows_updated=rows_updated,
                rows_soft_deleted=rows_soft_deleted,
                rows_dedup_dropped=rows_dedup_dropped,
            )

        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise


def build_archive_metadata(
    result: ArchiveMergeResult,
    config: RunModeConfig,
    *,
    source_table: str,
    target_table: str,
) -> dict:
    """Build a standardized metadata dict for archive MaterializeResult.

    Ensures all archive assets expose the same set of keys for consistent
    observability in the Dagster UI.
    """
    return {
        "row_count": result.row_count,
        "rows_inserted": result.rows_inserted,
        "rows_updated": result.rows_updated,
        "rows_soft_deleted": result.rows_soft_deleted,
        "rows_dedup_dropped": result.rows_dedup_dropped,
        "source_table": source_table,
        "target_table": target_table,
        "mode": config.mode,
        "advance_checkpoint": config.should_advance_checkpoint,
        "strategy": result.strategy,
        "detect_deletes": result.detect_deletes,
        "update_only_on_change": result.update_only_on_change,
    }
