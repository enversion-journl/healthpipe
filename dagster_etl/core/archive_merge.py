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


def _build_change_check(config: ArchiveMergeConfig) -> str:
    """Build a NULL-safe change-detection condition for WHEN MATCHED AND (...)."""
    comparisons = []
    for c in config.non_key_columns:
        comparisons.append(
            f"ISNULL(NULLIF(tgt.[{c}], src.[{c}]), " f"NULLIF(src.[{c}], tgt.[{c}])) IS NOT NULL"
        )
    return "\n            OR ".join(comparisons)


def _build_upsert_sql(config: ArchiveMergeConfig, *, track_actions: bool = False) -> str:
    src = f"[{config.source_schema}].[{config.source_table}]"
    tgt = f"[{config.target_schema}].[{config.target_table}]"

    on_clause = " AND ".join(f"tgt.[{k}] = src.[{k}]" for k in config.merge_keys)

    update_assignments = ",\n        ".join(
        f"tgt.[{c}] = src.[{c}]" for c in config.non_key_columns
    )

    all_cols = list(config.merge_keys) + list(config.non_key_columns)
    insert_columns = ", ".join(f"[{c}]" for c in all_cols)
    insert_values = ", ".join(f"src.[{c}]" for c in all_cols)

    change_guard = ""
    if config.update_only_on_change:
        change_check = _build_change_check(config)
        if config.detect_deletes and not config.source_delete_column:
            change_check += f"\n            OR tgt.[{config.soft_delete_column}] <> 0"
        change_guard = f" AND (\n            {change_check}\n        )"

    delete_update = ""
    delete_insert_col = ""
    delete_insert_val = ""
    resurrect_update = ""
    if config.source_delete_column:
        sc = config.source_delete_column
        sdc = config.soft_delete_column
        case_expr = (
            f"CASE WHEN src.[{sc}] IS NOT NULL "
            f"AND src.[{sc}] NOT IN ('0', '0.0') THEN 1 ELSE 0 END"
        )
        delete_update = f"\n        tgt.[{sdc}] = {case_expr},"
        delete_insert_col = f", [{sdc}]"
        delete_insert_val = f",\n        {case_expr}"
    elif config.detect_deletes:
        sdc = config.soft_delete_column
        resurrect_update = f"\n        tgt.[{sdc}] = 0,"

    merge_body = f"""MERGE {tgt} AS tgt
USING {src} AS src
    ON {on_clause}
WHEN MATCHED{change_guard} THEN UPDATE SET
        {update_assignments},{delete_update}{resurrect_update}
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN INSERT (
        {insert_columns},
        [DW_Timestamp_Insert], [DW_Timestamp_Update]{delete_insert_col}
    )
    VALUES (
        {insert_values},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0)){delete_insert_val}
    )"""

    if config.detect_deletes:
        merge_body += f"""
WHEN NOT MATCHED BY SOURCE AND tgt.[{config.soft_delete_column}] = 0 THEN UPDATE SET
        tgt.[{config.soft_delete_column}] = 1,
        tgt.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))"""

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
        [DW_Timestamp_Insert], [DW_Timestamp_Update]
    )
SELECT
        {select_columns},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM {src};
"""
    return sql


def execute_archive_merge(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    merge_config: ArchiveMergeConfig,
    run_config: RunModeConfig,
) -> ArchiveMergeResult:
    """Shared execution logic for all archive merge assets.

    The archive asset is a checkpoint owner — it reads its own checkpoint
    from dfm_state and sets it on success.

    Checkpoint resolution order:
      0. period mode with date_to — pins the checkpoint to the end of the
         processed period (``"{date_to} 23:59:59"``). In period mode the
         extract's real upper bound is date_to, not the flow-start ceiling
         (which tracks "now"), so this keeps chunked historical reloads
         resumable and self-healing. Takes precedence over flow_start /
         checkpoint_query. (An open-ended period with no date_to falls through
         to the rules below.)
      1. flow_start_key (from ArchiveMergeConfig) — reads a flow-start
         checkpoint from dfm_state and uses it as the new checkpoint.
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
            source_is_complete = (
                merge_config.upstream_is_full_load or run_config.mode == "full_load"
            )
            if merge_config.detect_deletes and not source_is_complete:
                context.log.warning(
                    f"Delete detection disabled — mode={run_config.mode!r} "
                    f"and upstream_is_full_load=False; "
                    f"NOT MATCHED BY SOURCE would incorrectly flag rows."
                )
                effective = dataclasses.replace(merge_config, detect_deletes=False)

            if effective.detect_deletes:
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

            is_upsert = effective.strategy != "insert_only"
            merge_sql = build_archive_merge_sql(effective, track_actions=is_upsert)
            context.log.info(f"Archive MERGE SQL:\n{merge_sql}")

            rows_inserted = rows_updated = rows_soft_deleted = 0

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

                    new_checkpoint: str | None = None
                    if run_config.mode == "period" and run_config.date_to:
                        # In period mode the extract's real upper bound is
                        # date_to — NOT the flow-start ceiling (which tracks
                        # "now") nor SYSDATETIME(). Pinning the checkpoint to the
                        # end of the processed period keeps a chunked historical
                        # reload resumable: if chunk N fails, the checkpoint
                        # reflects chunk N-1's boundary, and even a *missed*
                        # manual resume self-heals because the next scheduled
                        # delta run picks up everything after that boundary.
                        # (Sub-second rows on the date_to day may be re-processed
                        # by the next delta run — harmless, the MERGE is an
                        # idempotent upsert.)
                        new_checkpoint = f"{run_config.date_to} 23:59:59"
                    elif flow_start is not None:
                        new_checkpoint = flow_start
                    elif effective.checkpoint_query:
                        cursor.execute(effective.checkpoint_query)
                        row = cursor.fetchone()
                        new_checkpoint = row[0] if row is not None else None

                    conn.commit()
                finally:
                    cursor.close()

            context.log.info(
                f"MERGE action counts — "
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
        "source_table": source_table,
        "target_table": target_table,
        "mode": config.mode,
        "advance_checkpoint": config.should_advance_checkpoint,
        "strategy": result.strategy,
        "detect_deletes": result.detect_deletes,
        "update_only_on_change": result.update_only_on_change,
    }
