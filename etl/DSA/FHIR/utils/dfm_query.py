# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared archive query builder for DFM-based FHIR assets.

All DFM FHIR assets that do ``SELECT * FROM [arc].[table]`` with standard
RunModeConfig filtering (period / ids / checkpoint) use this single builder
to avoid duplicating WHERE-clause logic.
"""

from __future__ import annotations

from typing import Any

from core.dfm_config import RunModeConfig
from utils.sql_safety import safe_bracket_quote, safe_table_ref, validate_checkpoint_value

_DEFAULT_DELTA_COL = "dw_timestamp_update"


def build_fhir_archive_query(
    table: str,
    checkpoint: str | None,
    run_config: RunModeConfig,
    *,
    include_deleted: bool = False,
    select_clause: str | None = None,
    order_by: str | None = None,
    extra_or_predicate: str | None = None,
    delta_upper_bound: str | None = None,
) -> str:
    """Build a ``SELECT`` query against an archive table.

    Args:
        table: Fully qualified archive table, e.g. ``"arc.ifdb_patient"``.
        checkpoint: Current DFM checkpoint value (datetime string or None).
        run_config: Dagster run configuration with mode / filters.
        include_deleted: When True the query returns *all* rows regardless of
            ``dw_is_deleted``.  When False (default) only non-deleted rows are
            returned.  Patient needs True because deleted patients must still
            be posted so the FHIR isDeleted extension is updated; the CPR
            dedup picks the non-deleted row when both exist.
        select_clause: Custom SELECT clause (e.g.
            ``"SELECT DISTINCT [LOCATION_SORCODE]"``).  When None (default),
            ``SELECT *`` is used.
        order_by: Optional ``ORDER BY`` clause (column list without the
            keyword, e.g. ``"[OBJECT_ID]"``).  Appended verbatim, so it MUST be
            a trusted, code-defined value — never operator/runtime input.  Used
            e.g. to stream self-referencing resources in a deterministic order
            so a referenced row is processed before the row that references it.
        extra_or_predicate: Optional predicate OR'ed with the mode's own row
            filter, e.g. ``"[OBJECT_ID] IN (SELECT ...)"`` to pull in rows a
            child table says are affected.  Appended verbatim, so it MUST be a
            trusted, code-defined value.  The mode filter and this predicate
            are wrapped in one parenthesis, so the delete filter and any
            ``ORDER BY`` keep applying to both branches — writing
            ``f"{query} OR ..."`` at the call site would leak past ``AND
            [dw_is_deleted] = 0`` and break outright with ``order_by``.
        delta_upper_bound: Optional inclusive upper bound on the delta column in
            delta mode, from ``next_delta_slice_bound``.  Ignored in other modes.
    """
    col = safe_bracket_quote(run_config.delta_column or _DEFAULT_DELTA_COL)
    select = select_clause or "SELECT *"
    deleted_filter = "" if include_deleted else " AND [dw_is_deleted] = 0"
    safe_tbl = safe_table_ref(table)
    base = f"{select} FROM {safe_tbl} WHERE 1=1{deleted_filter}"

    if run_config.mode == "full_load":
        where = ""
    elif run_config.mode == "period":
        safe_from = run_config.date_from.replace("'", "''")
        where = f" AND {col} >= '{safe_from}'"
        if run_config.date_to:
            safe_to = run_config.date_to.replace("'", "''")
            where += f" AND {col} < DATEADD(day, 1, CAST('{safe_to}' AS DATE))"
    elif run_config.mode == "ids":
        safe_id_col = safe_bracket_quote(run_config.id_filter_column)
        if run_config.id_filter_table:
            safe_tbl = safe_table_ref(run_config.id_filter_table)
            # Alias + qualify the subquery column: if id_filter_column doesn't exist
            # in the filter table, the query fails loudly instead of silently
            # correlating to the outer table (always-true → full extract).
            where = (
                f" AND {safe_id_col} IN " f"(SELECT [flt].{safe_id_col} FROM {safe_tbl} AS [flt])"
            )
        else:
            escaped = ", ".join(
                f"'{v.replace(chr(39), chr(39) * 2)}'" for v in run_config.id_filter_values
            )
            where = f" AND {safe_id_col} IN ({escaped})"
    elif run_config.mode == "delta" and checkpoint:
        validate_checkpoint_value(checkpoint)
        safe_cp = checkpoint.replace("'", "''")
        lookback = run_config.lookback_days or 0
        if lookback > 0:
            where = f" AND {col} > DATEADD(day, -{lookback}, " f"CAST('{safe_cp}' AS DATETIME2))"
        else:
            where = f" AND {col} > '{safe_cp}'"
    else:
        where = ""

    if run_config.mode == "delta" and delta_upper_bound:
        validate_checkpoint_value(delta_upper_bound)
        where += f" AND {col} <= '{delta_upper_bound}'"

    if extra_or_predicate:
        # No mode filter (full_load) already selects every row, so an OR would
        # only widen nothing — and an empty parenthesis is a syntax error.
        where = f" AND ({where.removeprefix(' AND ')} OR {extra_or_predicate})" if where else ""

    query = base + where
    if order_by:
        query += f" ORDER BY {order_by}"
    return query


def next_delta_slice_bound(
    conn: Any, table: str, lower: str | None, max_rows: int, ceiling: str | None = None
) -> str | None:
    """Inclusive delta bound for the next slice after ``lower``; None when no rows are left.

    The bound is the last delta value whose running row count stays within
    ``max_rows``. A slice never splits rows that share one value, because an
    archive MERGE stamps all its rows alike: when the first value alone holds
    more than ``max_rows``, the slice is that one value. Rows after ``ceiling``
    are left for the next run.
    """
    col = safe_bracket_quote(_DEFAULT_DELTA_COL)
    filters: list[str] = []
    params: list[object] = []
    if lower:
        filters.append(f"{col} > ?")
        params.append(validate_checkpoint_value(lower))
    if ceiling:
        filters.append(f"{col} <= ?")
        params.append(validate_checkpoint_value(ceiling))
    params.append(max_rows)
    where = f" WHERE {' AND '.join(filters)}" if filters else ""
    query = f"""
        WITH [grp] AS (
            SELECT {col} AS [t], COUNT_BIG(*) AS [n]
            FROM {safe_table_ref(table)}{where}
            GROUP BY {col}
        ), [running] AS (
            SELECT [t], SUM([n]) OVER (ORDER BY [t] ROWS UNBOUNDED PRECEDING) AS [total]
            FROM [grp]
        )
        SELECT CONVERT(NVARCHAR(50),
            COALESCE(MAX(CASE WHEN [total] <= ? THEN [t] END), MIN([t])), 121)
        FROM [running]
    """
    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(params))
        row = cursor.fetchone()
    finally:
        cursor.close()
    return row[0] if row and row[0] else None
