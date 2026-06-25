# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Generic asset factory for the DSI extraction layer.

Provides:
- `create_extraction_assets()` — partition-based factory (daily/monthly/full_load).
- `create_dfm_extraction_assets()` — DFM checkpoint-based factory (dialect-aware).

Domain-specific adapters (IFDB, ERP) call these with their own asset_builder
and resource parameters.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

import dagster as dg
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.protocol import ExtractionConfig

_DEFAULT_GROUP_SUFFIXES: dict[str, str] = {
    "daily": "_daily",
    "monthly": "_monthly",
    "full_load": "_full_load",
}


def create_extraction_assets(
    config: ExtractionConfig,
    entity_name: str,
    asset_builder: Callable[..., Callable],
    include_daily: bool = True,
    include_monthly: bool = True,
    include_full_load: bool = False,
    asset_key_prefix: list[str] | None = None,
    group_prefix: str = "extract",
    group_suffixes: dict[str, str] | None = None,
    partition_defs: dict[str, dg.PartitionsDefinition | None] | None = None,
    io_manager_key: str | None = None,
    compute_kind: str | None = None,
    tags: dict | None = None,
    append_partition_suffix: bool = True,
    deps: list[dg.AssetKey] | None = None,
    retry_policy: dg.RetryPolicy | None = None,
) -> tuple[Callable, ...]:
    """Create extraction assets for daily, monthly, and/or full_load partitions.

    The ``asset_builder`` callable receives the computed asset decorator kwargs
    and the partition_type string, and must return a decorated ``@dg.asset``
    function with the correct typed resource parameters in its signature.

    Signature: ``asset_builder(asset_kwargs, partition_type) -> Callable``

    Args:
        config: Domain-specific extraction config (satisfies ExtractionConfig protocol).
        entity_name: Name of the entity (e.g., "patient", "encounter").
        asset_builder: Domain-specific callable that builds a @dg.asset function.
        include_daily: Whether to create a daily-partitioned asset.
        include_monthly: Whether to create a monthly-partitioned asset.
        include_full_load: Whether to create a full_load-partitioned asset.
        asset_key_prefix: Prefix for nested asset keys (e.g., ["ifdb_extract"]).
            None produces flat keys.
        group_prefix: Base group name (combined with group_suffixes per partition type).
        group_suffixes: Override default suffixes per partition type.
            E.g., ``{"full_load": ""}`` gives ``"erp_extract"`` instead of
            ``"erp_extract_full_load"``.
        partition_defs: Mapping of "daily"/"monthly"/"full_load" → PartitionsDefinition.
        io_manager_key: Optional io_manager_key for asset decorator.
        compute_kind: Optional compute_kind tag.
        tags: Base tags dict merged with partition-specific tags.
        append_partition_suffix: If True, appends ``_{partition_type}`` to entity_name
            for asset key and __name__. Set to False when entity_name already contains
            the partition suffix (ERP pattern with generate_partition_configs).
        deps: Optional list of asset key dependencies.

    Returns:
        Tuple of generated asset callables (only enabled partition types).
    """
    partition_defs = partition_defs or {}
    base_tags = tags or {}
    suffixes = {**_DEFAULT_GROUP_SUFFIXES, **(group_suffixes or {})}
    assets: list[Callable] = []

    variants: list[tuple[str, bool]] = [
        ("daily", include_daily),
        ("monthly", include_monthly),
        ("full_load", include_full_load),
    ]

    for partition_type, enabled in variants:
        if not enabled:
            continue

        if append_partition_suffix:
            asset_suffix = f"{entity_name}_{partition_type}"
        else:
            asset_suffix = entity_name

        if asset_key_prefix:
            key = dg.AssetKey(asset_key_prefix + [asset_suffix])
        else:
            key = dg.AssetKey(asset_suffix)

        group_name = f"{group_prefix}{suffixes[partition_type]}"
        merged_tags = {**base_tags, "partition": partition_type}

        asset_kwargs: dict[str, Any] = {
            "key": key,
            "group_name": group_name,
            "partitions_def": partition_defs.get(partition_type),
            "tags": merged_tags,
            "description": (
                f"Extract {entity_name} data for {partition_type} partition. "
                f"{config.description}"
            ),
        }
        if io_manager_key:
            asset_kwargs["io_manager_key"] = io_manager_key
        if compute_kind:
            asset_kwargs["compute_kind"] = compute_kind
        if deps:
            asset_kwargs["deps"] = deps
        if retry_policy:
            asset_kwargs["retry_policy"] = retry_policy

        asset_fn = asset_builder(asset_kwargs, partition_type)
        asset_fn.__name__ = f"extract_{asset_suffix}"
        asset_fn.__qualname__ = f"extract_{asset_suffix}"

        assets.append(asset_fn)

    return tuple(assets)


# ---------------------------------------------------------------------------
# DFM checkpoint-based extraction factory
# ---------------------------------------------------------------------------


def _append_where_to_select_from_expr(select_from_expr: str, where_clause: str) -> str:
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
    from DSI.connectors.mssql.models import TableConfig

    dfm = DfmManager(mssql_target_staging, context)

    mode = run_config.mode if hasattr(run_config, "mode") else "delta"
    effective_mode = "full_load" if extract_config.is_full_load else mode

    run_ctx = dfm.start_dfm_run(
        "ext",
        extract_config.target_table,
        dagster_run_id=context.run_id,
        mode=effective_mode,
        checkpoint_from=extract_config.checkpoint_from,
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
                        result = extractor.custom_stream_extract_and_insert(
                            query, table_config, partition_key=None
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
    """
    from core.dfm_manager import DfmManager
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
        )
        checkpoint = run_ctx.checkpoint
        context.log.info(
            f"DFM extract [{source_label}] " f"mode={effective_mode}, checkpoint={checkpoint!r}"
        )

        try:
            upper_bound: str | None = None
            if extract_config.flow_start_from and effective_mode == "delta":
                fs_schema, fs_table = extract_config.flow_start_from
                upper_bound = dfm.get_checkpoint(fs_schema, fs_table)
                if upper_bound:
                    context.log.info(
                        f"Flow-start upper bound from {fs_schema}.{fs_table}: {upper_bound}"
                    )

            where_clause = where_builder(run_config, checkpoint, extract_config, upper_bound)
            if where_clause:
                context.log.info(f"WHERE clause: {where_clause}")

            columns = extract_config.columns
            if not columns:
                raise ValueError(
                    f"DfmExtractConfig for {extract_config.source_table}: "
                    "Oracle extracts require an explicit 'columns' tuple."
                )

            if extract_config.select_from_expr:
                query = extract_config.select_from_expr
            else:
                col_list = ", ".join(columns)
                query = f"SELECT {col_list} FROM {source_label}"
            query = _append_where_to_select_from_expr(query, where_clause)
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

            with source_resource.get_connection() as oracle_conn:
                ora_cursor = oracle_conn.cursor()
                try:
                    ora_cursor.execute(query)
                    for batch_num, batch in enumerate(
                        iter_rows_as_dict_batches(ora_cursor, _FETCH_SIZE, context), 1
                    ):
                        with mssql_target_staging.get_connection() as ins_conn:
                            ins_cursor = ins_conn.cursor()
                            try:
                                for i in range(0, len(batch), _INSERT_BATCH):
                                    chunk = batch[i : i + _INSERT_BATCH]
                                    params = [
                                        tuple(stringify_oracle_value(row.get(c)) for c in columns)
                                        for row in chunk
                                    ]
                                    ins_cursor.executemany(insert_sql, params)
                                ins_conn.commit()
                            finally:
                                ins_cursor.close()
                        total_rows += len(batch)
                        if batch_num == 1 or batch_num % 10 == 0:
                            context.log.info(
                                f"[{source_label}] streamed {total_rows:,} rows "
                                f"({batch_num} batches of {_FETCH_SIZE})"
                            )
                finally:
                    ora_cursor.close()

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
        entity = cfg.source_table.lower()

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
