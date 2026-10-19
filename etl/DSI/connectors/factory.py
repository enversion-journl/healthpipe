# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Generic asset factory for the DSI extraction layer.

Provides `create_dfm_extraction_assets()` — the DFM checkpoint-based,
dialect-aware factory. Domain-specific adapters (IFDB, ERP) call it with their
own asset_builder and resource parameters.

The partition-based sibling (`create_extraction_assets`, daily/monthly/full_load)
is gone: every source is on the DFM path, so nothing built partitioned assets any
more.
"""

from __future__ import annotations

import unicodedata
from collections.abc import Callable, Sequence
from typing import Any

import dagster as dg
from core.dfm_source import resource_source
from DSI.connectors.extract_config import DfmExtractConfig

# ---------------------------------------------------------------------------
# DFM checkpoint-based extraction factory
# ---------------------------------------------------------------------------


def _append_where_to_select_from_expr(select_from_expr: str, where_clause: str | None) -> str:
    """Append a predicate to a select_from_expr, using AND if it already has WHERE.

    Whitespace-insensitive match on a top-level WHERE token. We do not parse
    SQL — we assume the convention that any embedded WHERE in select_from_expr
    is the top-level one. CTEs or subqueries that contain WHERE internally must
    still expose a top-level WHERE if they need filtering — that is the
    documented contract.

    If where_clause is empty or None the expression is returned unchanged.
    """
    if not where_clause:
        return select_from_expr
    import re

    has_top_where = re.search(r"\bWHERE\b", select_from_expr, re.IGNORECASE) is not None
    if has_top_where:
        return f"{select_from_expr} AND ({where_clause})"
    return f"{select_from_expr} WHERE {where_clause}"


def build_oracle_extract_query(extract_config: DfmExtractConfig, where_clause: str | None) -> str:
    """The SELECT an Oracle DFM extract runs against the source, WHERE applied.

    The one place this query is assembled. Anything that must see exactly the
    rows an extract fetched — such as a backfill window's reconciliation —
    builds its source with this function instead of a copy.
    """
    if extract_config.select_from_expr:
        query = extract_config.select_from_expr
    else:
        col_list = ", ".join(extract_config.columns)
        query = (
            f"SELECT {col_list} FROM {extract_config.source_schema}.{extract_config.source_table}"
        )
    return _append_where_to_select_from_expr(query, where_clause)


def _execute_dfm_mssql_extract(
    context: dg.AssetExecutionContext,
    source_resource,
    mssql_target_staging,
    run_config,
    extract_config: DfmExtractConfig,
    source_system: str,
    where_builder: Callable,
) -> dg.MaterializeResult:
    """Shared MSSQL execution logic for DFM extract assets.

    Handles: DfmManager lifecycle, WHERE clause build, MssqlExtractor I/O.
    """
    from core.dfm_manager import DfmManager
    from DSI.connectors.mssql.extractor import MssqlExtractor
    from DSI.connectors.mssql.models import TableConfig, TableLoadResult

    dfm = DfmManager(mssql_target_staging, context)

    mode = run_config.mode if hasattr(run_config, "mode") else "delta"
    effective_mode = "full_load" if extract_config.is_full_load else mode

    run_ctx = dfm.start_dfm_run(
        "ext",
        extract_config.target_table,
        dagster_run_id=context.run_id,
        mode=effective_mode,
        checkpoint_from=extract_config.checkpoint_from,
        **resource_source(
            source_resource, f"{extract_config.source_schema}.{extract_config.source_table}"
        ),
    )
    checkpoint = run_ctx.checkpoint
    context.log.info(
        f"DFM extract [{extract_config.source_schema}.{extract_config.source_table}] "
        f"mode={effective_mode}, checkpoint={checkpoint!r}"
    )

    try:
        where_clause = where_builder(run_config, checkpoint, extract_config)
        if where_clause:
            context.log.info(f"WHERE clause: {where_clause}")

        if extract_config.select_from_expr:
            # Custom SELECT+FROM expression: assemble full query and use
            # custom_stream_extract_and_insert so the WHERE clause is appended
            # correctly (AND if the expression already has a top-level WHERE).
            query = _append_where_to_select_from_expr(extract_config.select_from_expr, where_clause)
            context.log.info(f"MSSQL extract SQL (select_from_expr):\n{query}")
            # TableConfig for DDL/bulkcopy plumbing; full query is already built,
            # so custom_where_clause=None to avoid double-applying the filter.
            table_config = TableConfig(
                output_name=f"ext_{extract_config.target_table}",
                source_schema=extract_config.source_schema,
                source_table=extract_config.source_table,
                target_table=extract_config.target_table,
                description=extract_config.description,
                is_full_load=True,
                custom_where_clause=None,
                exclude_columns=extract_config.exclude_columns,
            )
        else:
            # Default path: Uses existing TableConfig class logic (hence is_full_load=True,
            # even though DfmExtractConfig is not necessarily full load)
            table_config = TableConfig(
                output_name=f"ext_{extract_config.target_table}",
                source_schema=extract_config.source_schema,
                source_table=extract_config.source_table,
                target_table=extract_config.target_table,
                description=extract_config.description,
                is_full_load=True,
                custom_where_clause=where_clause,
                exclude_columns=extract_config.exclude_columns,
            )

        with source_resource.get_connection() as source_conn:
            with mssql_target_staging.get_connection() as target_conn:
                with mssql_target_staging.get_connection() as bulkcopy_conn:
                    extractor = MssqlExtractor(
                        source_conn,
                        target_conn,
                        context=context,
                        source_system=source_system,
                        bulkcopy_conn=bulkcopy_conn,
                        mssql_target_resource=mssql_target_staging,
                    )
                    if extract_config.select_from_expr:
                        # custom_stream_extract_and_insert returns a bare row
                        # count; wrap it so downstream code can treat the result
                        # uniformly with stream_extract_and_insert's TableLoadResult.
                        row_count = extractor.custom_stream_extract_and_insert(
                            query, table_config, partition_key=None
                        )
                        result = TableLoadResult(
                            source_table=(
                                f"{extract_config.source_schema}.{extract_config.source_table}"
                            ),
                            target_table=f"ext.{extract_config.target_table}",
                            row_count=row_count,
                        )
                    else:
                        result = extractor.stream_extract_and_insert(
                            table_config, partition_key=None
                        )

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=result.row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            **result.as_metadata(),
            "mode": effective_mode,
            "checkpoint_used": str(checkpoint),
        }
    )


# Run-tag escape hatch for the Oracle ext write path: set
# ``ext_insert_method: executemany`` on a run to force the legacy executemany
# insert instead of bulk-copy (also the lever for before/after benchmarks).
_ORACLE_INSERT_METHOD_TAG = "ext_insert_method"

# Refresh the persistent bulk-copy connection every N batches (mirrors
# stream_bulkcopy_chunks' guard against long-session wedging).
_BULKCOPY_REFRESH_INTERVAL = 100


def _oracle_insert_method_from_tags(context: dg.AssetExecutionContext) -> str:
    """Resolve the requested ext insert method ("bulkcopy" unless opted out)."""
    if context.has_tag(_ORACLE_INSERT_METHOD_TAG):
        value = (context.run.tags.get(_ORACLE_INSERT_METHOD_TAG) or "").strip().lower()
        if value == "executemany":
            return "executemany"
        if value and value != "bulkcopy":
            context.log.warning(
                f"Unknown {_ORACLE_INSERT_METHOD_TAG}={value!r} run tag; using bulkcopy."
            )
    return "bulkcopy"


def _executemany_insert_batch(
    mssql_target_staging,
    insert_sql: str,
    params: list[tuple],
    insert_chunk: int,
) -> None:
    """Legacy executemany write path for one fetch batch.

    Reached only via the explicit ``ext_insert_method=executemany`` run tag —
    a bulk-copy failure fails the asset rather than silently degrading."""
    with mssql_target_staging.get_connection() as ins_conn:
        ins_cursor = ins_conn.cursor()
        try:
            for i in range(0, len(params), insert_chunk):
                ins_cursor.executemany(insert_sql, params[i : i + insert_chunk])
            ins_conn.commit()
        finally:
            ins_cursor.close()


def _execute_dfm_oracle_extract(
    context: dg.AssetExecutionContext,
    source_resource,
    mssql_target_staging,
    run_config,
    extract_config: DfmExtractConfig,
    source_system: str,
    where_builder: Callable,
) -> dg.MaterializeResult:
    """Shared Oracle execution logic for DFM extract assets.

    Handles: DfmManager lifecycle, WHERE clause build, Oracle→MSSQL streaming.

    The ext insert uses mssql-python's bulk-copy protocol (one persistent
    bulk-copy connection, retry/refresh against wedging) — same ext table,
    same arc-MERGE, only the INSERT method differs. A bulk-copy failure fails
    the asset; the legacy executemany loop is reachable only via the explicit
    ``ext_insert_method=executemany`` run tag.
    """
    import time

    from core.dfm_manager import DfmManager
    from DSI.connectors.mssql.bulk_copy import (
        bulkcopy_insert_with_retry,
        refresh_bulkcopy_conn,
    )
    from DSI.connectors.mssql.models import TableConfig
    from DSI.connectors.oracle.models import iter_rows_as_dict_batches, stringify_oracle_value

    dfm = DfmManager(mssql_target_staging, context)

    mode = run_config.mode if hasattr(run_config, "mode") else "delta"
    effective_mode = "full_load" if extract_config.is_full_load else mode

    source_label = f"{extract_config.source_schema}.{extract_config.source_table}"

    with dfm.connection():
        run_ctx = dfm.start_dfm_run(
            "ext",
            extract_config.target_table,
            dagster_run_id=context.run_id,
            mode=effective_mode,
            checkpoint_from=extract_config.checkpoint_from,
            **resource_source(source_resource, source_label),
        )
        checkpoint = run_ctx.checkpoint
        context.log.info(
            f"DFM extract [{source_label}] " f"mode={effective_mode}, checkpoint={checkpoint!r}"
        )

        try:
            ceiling: str | None = None
            if extract_config.flow_start_from:
                fs_schema, fs_table = extract_config.flow_start_from
                ceiling = dfm.get_checkpoint(fs_schema, fs_table)
                if ceiling:
                    context.log.info(f"Flow-start ceiling from {fs_schema}.{fs_table}: {ceiling}")
            upper_bound = ceiling if effective_mode == "delta" else None

            where_clause = where_builder(run_config, checkpoint, extract_config, upper_bound)
            if where_clause:
                context.log.info(f"WHERE clause: {where_clause}")

            columns = extract_config.columns
            if not columns:
                raise ValueError(
                    f"DfmExtractConfig for {extract_config.source_table}: "
                    "Oracle extracts require an explicit 'columns' tuple."
                )

            query = build_oracle_extract_query(extract_config, where_clause)
            context.log.info(f"Oracle extract SQL:\n{query}")

            ext_table = extract_config.target_table
            _FETCH_SIZE = 10_000
            _INSERT_BATCH = 1_000

            with mssql_target_staging.get_connection() as mssql_conn:
                cursor = mssql_conn.cursor()
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS [ext].[{ext_table}]")
                    if extract_config.column_types:
                        col_defs = ", ".join(
                            f"[{c}] {extract_config.column_types[c]}" for c in columns
                        )
                    else:
                        col_defs = ", ".join(f"[{c}] NVARCHAR(MAX) NULL" for c in columns)
                    cursor.execute(f"CREATE TABLE [ext].[{ext_table}] ({col_defs})")
                    mssql_conn.commit()
                finally:
                    cursor.close()

            total_rows = 0
            placeholders = ", ".join("?" for _ in columns)
            insert_sql = (
                f"INSERT INTO [ext].[{ext_table}] "
                f"({', '.join(f'[{c}]' for c in columns)}) "
                f"VALUES ({placeholders})"
            )

            insert_method = _oracle_insert_method_from_tags(context)
            use_bulkcopy = insert_method == "bulkcopy"
            if not use_bulkcopy:
                context.log.info(
                    f"Run tag {_ORACLE_INSERT_METHOD_TAG}=executemany: "
                    "using the legacy executemany write path for this run."
                )

            # TableConfig only feeds bulkcopy_insert's target naming/logging;
            # the ext table is already created above.
            bc_config = TableConfig(
                output_name=f"ext_{ext_table}",
                source_schema=extract_config.source_schema,
                source_table=extract_config.source_table,
                target_table=ext_table,
                description=extract_config.description,
                is_full_load=True,
            )

            bulkcopy_conn = None
            open_contexts: list = []
            bc_batches = 0
            t_fetch = t_prep = t_insert = 0.0
            t_started = time.perf_counter()
            try:
                with source_resource.get_connection() as oracle_conn:
                    ora_cursor = oracle_conn.cursor()
                    # The driver default arraysize (100) costs 100 network round
                    # trips per 10k fetchmany; match it to the fetch batch.
                    # (LOB-inline + encoding bindings come from the resource's
                    # connection-level output type handler — do NOT set a
                    # cursor-level handler here, it would replace them.)
                    ora_cursor.arraysize = _FETCH_SIZE
                    ora_cursor.prefetchrows = _FETCH_SIZE + 1
                    try:
                        ora_cursor.execute(query)
                        batches = iter_rows_as_dict_batches(ora_cursor, _FETCH_SIZE, context)
                        batch_num = 0
                        while True:
                            t0 = time.perf_counter()
                            batch = next(batches, None)
                            t_fetch += time.perf_counter() - t0
                            if batch is None:
                                break
                            batch_num += 1

                            t0 = time.perf_counter()
                            params = [
                                tuple(stringify_oracle_value(row.get(c)) for c in columns)
                                for row in batch
                            ]
                            t_prep += time.perf_counter() - t0

                            t0 = time.perf_counter()
                            if use_bulkcopy:
                                if (
                                    bulkcopy_conn is not None
                                    and bc_batches > 0
                                    and bc_batches % _BULKCOPY_REFRESH_INTERVAL == 0
                                ):
                                    bulkcopy_conn = refresh_bulkcopy_conn(
                                        context,
                                        bulkcopy_conn,
                                        mssql_target_staging,
                                        label=f"[{source_label}] batch {batch_num} refresh "
                                        f"(every {_BULKCOPY_REFRESH_INTERVAL})",
                                        open_contexts=open_contexts,
                                    )
                                if bulkcopy_conn is None:
                                    ctx_mgr = mssql_target_staging.get_connection()
                                    bulkcopy_conn = ctx_mgr.__enter__()
                                    open_contexts.append(ctx_mgr)
                                # Any bulk-copy failure (beyond the split/retry
                                # machinery's own timeout recovery) fails the
                                # asset — no silent executemany degradation.
                                # expected_rows_before: fresh DROP+CREATE table,
                                # single writer -> the timeout duplicate guard
                                # in the retry helper is valid here.
                                inserted, bulkcopy_conn = bulkcopy_insert_with_retry(
                                    context,
                                    bulkcopy_conn,
                                    bc_config,
                                    list(columns),
                                    params,
                                    mssql_target_resource=mssql_target_staging,
                                    open_contexts=open_contexts,
                                    extra_columns=(),
                                    expected_rows_before=total_rows,
                                )
                                total_rows += inserted
                                bc_batches += 1
                            else:
                                _executemany_insert_batch(
                                    mssql_target_staging, insert_sql, params, _INSERT_BATCH
                                )
                                total_rows += len(params)
                            t_insert += time.perf_counter() - t0

                            if batch_num == 1 or batch_num % 10 == 0:
                                context.log.info(
                                    f"[{source_label}] streamed {total_rows:,} rows "
                                    f"({batch_num} batches of {_FETCH_SIZE}; "
                                    f"fetch={t_fetch:.1f}s insert={t_insert:.1f}s)"
                                )
                    finally:
                        ora_cursor.close()
            finally:
                for ctx_mgr in open_contexts:
                    try:
                        ctx_mgr.__exit__(None, None, None)
                    except Exception:
                        pass

            t_elapsed = time.perf_counter() - t_started
            rows_per_second = total_rows / t_elapsed if t_elapsed > 0 else 0.0
            effective_method = insert_method
            context.log.info(
                f"[{source_label}] ext insert done: {total_rows:,} rows in {t_elapsed:.1f}s "
                f"({rows_per_second:,.0f} rows/s; method={effective_method}, "
                f"fetch={t_fetch:.1f}s prep={t_prep:.1f}s insert={t_insert:.1f}s)"
            )

            if extract_config.index_keys and total_rows > 0:
                ix_cols = ", ".join(f"[{k}]" for k in extract_config.index_keys)
                ix_name = f"CIX_ext_{ext_table}"
                create_ix = (
                    f"CREATE CLUSTERED INDEX [{ix_name}] " f"ON [ext].[{ext_table}] ({ix_cols})"
                )
                context.log.info(f"Creating clustered index on ext.{ext_table} ({ix_cols})")
                with mssql_target_staging.get_connection() as conn:
                    cur = conn.cursor()
                    try:
                        cur.execute(create_ix)
                        conn.commit()
                    finally:
                        cur.close()

            context.log.info(f"Extracted {total_rows:,} rows from {source_label} → ext.{ext_table}")
            # A delta archive merge reads this row as its flow_start_key checkpoint.
            if ceiling:
                dfm.set_checkpoint(
                    *extract_config.used_ceiling_key, value=ceiling, mode=effective_mode
                )
            dfm.complete_dfm_run(run_ctx.log_id, rows_affected=total_rows)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise

    return dg.MaterializeResult(
        metadata={
            "row_count": total_rows,
            "source_table": source_label,
            "target_table": f"ext.{ext_table}",
            "mode": effective_mode,
            "checkpoint_used": str(checkpoint),
            "insert_method": effective_method,
            "rows_per_second": round(rows_per_second, 1),
            "seconds_total": round(t_elapsed, 1),
            "seconds_fetch": round(t_fetch, 1),
            "seconds_prep": round(t_prep, 1),
            "seconds_insert": round(t_insert, 1),
        }
    )


_DIALECT_REGISTRY: dict[str, dict[str, Any]] = {
    "mssql": {
        "executor": _execute_dfm_mssql_extract,
        "default_compute_kind": "mssql",
    },
    "oracle": {
        "executor": _execute_dfm_oracle_extract,
        "default_compute_kind": "oracle",
    },
}


def _get_where_builder(dialect: str) -> Callable:
    """Lazy-import the dialect-specific WHERE builder to avoid circular imports."""
    if dialect == "mssql":
        from DSI.connectors.mssql.dfm_sql_builders import build_mssql_where

        return build_mssql_where
    elif dialect == "oracle":
        from DSI.oracle.ifdb_extract import build_oracle_where

        return build_oracle_where
    else:
        raise ValueError(f"Unknown dialect {dialect!r}. Available: mssql, oracle")


def create_dfm_extraction_assets(
    configs: Sequence[DfmExtractConfig],
    source_resource_key: str,
    source_resource_type: type,
    source_system: str,
    group_name: str,
    domain_tag: str,
    dialect: str = "mssql",
    compute_kind: str | None = None,
    deps: list[str] | None = None,
    retry_policy: dg.RetryPolicy | None = None,
) -> list[Callable]:
    """Generate DFM extract assets from a config list.

    Generic factory for any SQL source dialect.  Each generated asset:
    1. Reads checkpoint via DfmManager (from config.checkpoint_from)
    2. Builds WHERE clause via the dialect-specific builder
    3. Executes data transfer via the dialect-specific executor
    4. Logs run completion/failure via DfmManager

    All assets accept RunModeConfig for ad-hoc overrides (period/ids).

    Args:
        configs: List of DfmExtractConfig — one asset generated per config.
        source_resource_key: Dagster resource key for the source database
            connection (e.g. "kmd_uddata_mssql", "ils_mssql", "ifdb_oracle").
        source_resource_type: Python type of the source resource — needed for
            Dagster's typed resource injection.
        source_system: String identifier stamped on extracted rows
            (e.g. "KMD_UdData", "ILS", "IFDB").
        group_name: Dagster asset group for all generated assets.
        domain_tag: Value for the "domain" tag on each asset.
        dialect: SQL dialect ("mssql" or "oracle").  Determines which WHERE
            builder and executor to use.
        compute_kind: Dagster compute_kind tag. Defaults to the dialect value.
        deps: Optional upstream asset dependencies.
        retry_policy: Optional Dagster retry policy for all generated assets.

    Returns:
        List of generated asset callables.
    """
    if dialect not in _DIALECT_REGISTRY:
        raise ValueError(
            f"Unknown dialect {dialect!r}. Available: {list(_DIALECT_REGISTRY.keys())}"
        )

    dialect_info = _DIALECT_REGISTRY[dialect]
    executor = dialect_info["executor"]
    effective_compute_kind = compute_kind or dialect_info["default_compute_kind"]
    where_builder = _get_where_builder(dialect)

    assets: list[Callable] = []

    for cfg in configs:
        asset_name = f"ext_{cfg.target_table}"
        # Normalize non-ASCII letters (e.g. Danish ø→o, æ→a, å→a) so the entity
        # tag satisfies Dagster's [a-zA-Z0-9_\-.] constraint.
        _raw = unicodedata.normalize("NFKD", cfg.source_table.lower())
        entity = "".join(c for c in _raw if not unicodedata.combining(c) and c.isascii())

        asset_tags = {
            "domain": domain_tag,
            "layer": "extract",
            "source": source_system.lower().replace("_", ""),
            "entity": entity,
        }

        asset_deps = [dg.AssetKey(d) for d in deps] if deps else None

        asset_kwargs: dict[str, Any] = {
            "name": asset_name,
            "group_name": group_name,
            "compute_kind": effective_compute_kind,
            "tags": asset_tags,
            "description": (
                f"DFM extract: {cfg.source_schema}.{cfg.source_table} → "
                f"ext.{cfg.target_table}. {cfg.description}"
            ),
        }
        if asset_deps:
            asset_kwargs["deps"] = asset_deps
        if retry_policy:
            asset_kwargs["retry_policy"] = retry_policy

        _asset_fn = _build_dfm_asset_fn(
            cfg,
            source_resource_key,
            source_resource_type,
            source_system,
            executor,
            where_builder,
            asset_kwargs,
        )
        assets.append(_asset_fn)

    return assets


def _build_dfm_asset_fn(
    extract_cfg: DfmExtractConfig,
    source_resource_key: str,
    source_resource_type: type,
    source_system: str,
    executor: Callable,
    where_builder: Callable,
    asset_kwargs: dict[str, Any],
) -> Callable:
    """Build a @dg.asset function with correct typed resource params.

    Uses a closure with a custom ``__signature__`` and ``__annotations__``
    so that Dagster discovers the correct parameter names (matching resource
    keys) and type annotations (for typed resource injection) without
    resorting to ``exec()``.

    Dagster calls asset functions with keyword arguments based on the
    advertised signature, so the inner function accepts ``**kwargs`` and
    extracts resources by their dynamic key name.
    """
    import inspect

    from core.dfm_config import RunModeConfig
    from core.resources import MSSQLResource

    _cfg = extract_cfg
    _source_system = source_system
    _executor = executor
    _where_builder = where_builder
    _source_key = source_resource_key

    def _asset_fn(context: dg.AssetExecutionContext, **kwargs: Any) -> dg.MaterializeResult:
        return _executor(
            context,
            kwargs[_source_key],
            kwargs["mssql_target_staging"],
            kwargs["config"],
            _cfg,
            _source_system,
            _where_builder,
        )

    _asset_fn.__signature__ = inspect.Signature(
        parameters=[
            inspect.Parameter(
                "context",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=dg.AssetExecutionContext,
            ),
            inspect.Parameter(
                source_resource_key,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=source_resource_type,
            ),
            inspect.Parameter(
                "mssql_target_staging",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=MSSQLResource,
            ),
            inspect.Parameter(
                "config",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=RunModeConfig,
            ),
        ],
        return_annotation=dg.MaterializeResult,
    )
    _asset_fn.__annotations__ = {
        "context": dg.AssetExecutionContext,
        source_resource_key: source_resource_type,
        "mssql_target_staging": MSSQLResource,
        "config": RunModeConfig,
        "return": dg.MaterializeResult,
    }

    decorated = dg.asset(**asset_kwargs)(_asset_fn)
    decorated.__name__ = asset_kwargs["name"]
    decorated.__qualname__ = asset_kwargs["name"]

    return decorated
