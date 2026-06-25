# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Asset factory for ERP extraction assets.

Delegates to the shared core factory for asset generation while providing
ERP-specific asset builders that handle typed resource injection for each
source system (ILS, SydAX, MySupply).
"""

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any

import dagster as dg
from core.partitions import daily_partitions_erp, monthly_partitions_erp
from core.resources import (
    ILSMSSQLResource,
    KmdUddataMSSQLResource,
    MSSQLResource,
    MySupplyMSSQLResource,
    SydaxMSSQLResource,
)
from DSI.connectors.factory import create_extraction_assets as _generic_factory
from DSI.connectors.mssql.extractor import MssqlExtractor
from DSI.connectors.mssql.models import (
    TABLE_TYPE_BRIDGE,
    TABLE_TYPE_DEPENDENT,
    TableConfig,
    TableLoadResult,
)
from DSI.connectors.mssql.parallel_extract import (
    parallel_stream_extract_and_insert,
    should_run_parallel,
)


@contextmanager
def _nullcontext():
    """Yield None when no bulk-copy connection is provided."""
    yield None


def _run_extraction(
    context: dg.AssetExecutionContext,
    source_conn,
    target_conn,
    config: TableConfig,
    source_system: str,
    partition_key: str | None,
    partition_type: str,
    mssql_target_staging=None,
) -> TableLoadResult:
    """Shared extraction logic — all paths go through MssqlExtractor.

    - Bridge/dependent tables: stream via MssqlExtractor.stream_relation_extract_and_insert
      (single-pass chunked fetch+insert, no full in-memory accumulation).
    - All other tables: stream via MssqlExtractor.stream_extract_and_insert.
    """
    bc_ctx = (
        mssql_target_staging.get_connection()
        if mssql_target_staging is not None
        else _nullcontext()
    )
    with bc_ctx as bulkcopy_conn:
        extractor = MssqlExtractor(
            source_conn,
            target_conn,
            context=context,
            source_system=source_system,
            bulkcopy_conn=bulkcopy_conn,
            mssql_target_resource=mssql_target_staging,
            bcp_max_chunk_size=config.bcp_max_chunk_size,
        )

        if config.table_type in (TABLE_TYPE_DEPENDENT, TABLE_TYPE_BRIDGE):
            row_count = extractor.stream_relation_extract_and_insert(
                config, partition_key, partition_type
            )
            return TableLoadResult(
                source_table=f"{config.source_schema}.{config.source_table}",
                target_table=f"ext.{config.target_table}",
                row_count=row_count,
            )

        return extractor.stream_extract_and_insert(config, partition_key)


def run_parallel_extract_if_enabled(
    *,
    context,
    source_resource,
    target_resource,
    config,
    source_system,
    is_full_load,
    is_mssql_python,
) -> bool:
    """If the parallel gate passes, run the windowed parallel extract, emit its
    output metadata, and return True (the asset is done). Otherwise return False
    so the caller runs the serial path."""
    if not should_run_parallel(config, is_full_load=is_full_load, is_mssql_python=is_mssql_python):
        return False
    result = parallel_stream_extract_and_insert(
        context, source_resource, target_resource, config, source_system
    )
    context.add_output_metadata(result.as_metadata())
    return True


def _make_ils_builder(
    config: TableConfig, source_system: str
) -> Callable[[dict[str, Any], str], Callable]:
    def builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _extract(
            context: dg.AssetExecutionContext,
            ils_mssql: ILSMSSQLResource,
            mssql_target_staging: MSSQLResource,
        ) -> None:
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(
                    f"Running {source_system} extraction for partition: {partition_key}"
                )
            with ils_mssql.get_connection() as source_conn:
                with mssql_target_staging.get_connection() as target_conn:
                    result = _run_extraction(
                        context,
                        source_conn,
                        target_conn,
                        config,
                        source_system,
                        partition_key,
                        partition_type,
                        mssql_target_staging=mssql_target_staging,
                    )
                    context.add_output_metadata(result.as_metadata())

        return _extract

    return builder


def _make_sydax_builder(
    config: TableConfig, source_system: str
) -> Callable[[dict[str, Any], str], Callable]:
    def builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _extract(
            context: dg.AssetExecutionContext,
            sydax_mssql: SydaxMSSQLResource,
            mssql_target_staging: MSSQLResource,
        ) -> None:
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(
                    f"Running {source_system} extraction for partition: {partition_key}"
                )
            is_full_load = partition_type == "full_load"
            if run_parallel_extract_if_enabled(
                context=context,
                source_resource=sydax_mssql,
                target_resource=mssql_target_staging,
                config=config,
                source_system=source_system,
                is_full_load=is_full_load,
                is_mssql_python=True,
            ):
                return
            with sydax_mssql.get_connection() as source_conn:
                with mssql_target_staging.get_connection() as target_conn:
                    result = _run_extraction(
                        context,
                        source_conn,
                        target_conn,
                        config,
                        source_system,
                        partition_key,
                        partition_type,
                        mssql_target_staging=mssql_target_staging,
                    )
                    context.add_output_metadata(result.as_metadata())

        return _extract

    return builder


def _make_mysupply_builder(
    config: TableConfig, source_system: str
) -> Callable[[dict[str, Any], str], Callable]:
    def builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _extract(
            context: dg.AssetExecutionContext,
            mysupply_mssql: MySupplyMSSQLResource,
            mssql_target_staging: MSSQLResource,
        ) -> None:
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(
                    f"Running {source_system} extraction for partition: {partition_key}"
                )
            is_full_load = partition_type == "full_load"
            if run_parallel_extract_if_enabled(
                context=context,
                source_resource=mysupply_mssql,
                target_resource=mssql_target_staging,
                config=config,
                source_system=source_system,
                is_full_load=is_full_load,
                is_mssql_python=True,
            ):
                return
            with mysupply_mssql.get_connection() as source_conn:
                with mssql_target_staging.get_connection() as target_conn:
                    result = _run_extraction(
                        context,
                        source_conn,
                        target_conn,
                        config,
                        source_system,
                        partition_key,
                        partition_type,
                        mssql_target_staging=mssql_target_staging,
                    )
                    context.add_output_metadata(result.as_metadata())

        return _extract

    return builder


def _make_kmd_uddata_builder(
    config: TableConfig, source_system: str
) -> Callable[[dict[str, Any], str], Callable]:
    def builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _extract(
            context: dg.AssetExecutionContext,
            kmd_uddata_mssql: KmdUddataMSSQLResource,
            mssql_target_staging: MSSQLResource,
        ) -> None:
            partition_key = context.partition_key if context.has_partition_key else None
            if partition_key:
                context.log.info(
                    f"Running {source_system} extraction for partition: {partition_key}"
                )
            with kmd_uddata_mssql.get_connection() as source_conn:
                with mssql_target_staging.get_connection() as target_conn:
                    result = _run_extraction(
                        context,
                        source_conn,
                        target_conn,
                        config,
                        source_system,
                        partition_key,
                        partition_type,
                        mssql_target_staging=mssql_target_staging,
                    )
                    context.add_output_metadata(result.as_metadata())

        return _extract

    return builder


_BUILDER_FACTORIES: dict[str, Callable] = {
    "ils_mssql": _make_ils_builder,
    "sydax_mssql": _make_sydax_builder,
    "mysupply_mssql": _make_mysupply_builder,
    "kmd_uddata_mssql": _make_kmd_uddata_builder,
}


def create_erp_extraction_assets(
    config: TableConfig,
    source_system: str,
    source_resource_type: type,
    source_resource_key: str,
    include_daily: bool = True,
    include_monthly: bool = True,
    include_full_load: bool = False,
) -> tuple[Callable, ...]:
    """
    Create extraction assets for a single ERP table.

    Args:
        config: Resolved TableConfig (already has _daily/_monthly suffix if partitioned).
        source_system: "ILS", "SydAX", or "MySupply".
        source_resource_type: The typed resource class (kept for API consistency).
        source_resource_key: Dagster resource key ("ils_mssql", "sydax_mssql", "mysupply_mssql").
        include_daily: Create a daily-partitioned asset.
        include_monthly: Create a monthly-partitioned asset.
        include_full_load: Create a non-partitioned full-load asset.

    Returns:
        Tuple of generated asset functions (only the enabled ones).
    """
    make_builder = _BUILDER_FACTORIES[source_resource_key]
    asset_builder = make_builder(config, source_system)

    entity_tag = config.output_name
    for _suffix in ("_daily", "_monthly", "_full_load"):
        if entity_tag.endswith(_suffix):
            entity_tag = entity_tag[: -len(_suffix)]
            break

    assets = _generic_factory(
        config=config,
        entity_name=config.output_name,
        asset_builder=asset_builder,
        include_daily=include_daily,
        include_monthly=include_monthly,
        include_full_load=include_full_load,
        asset_key_prefix=None,
        group_prefix="erp_extract",
        # Suffixed *_full_load configs are on-demand full re-extractions and get
        # their own group so daily/monthly jobs (which select erp_extract for
        # small reference tables) never pick them up.
        group_suffixes={
            "full_load": "_full_load" if config.output_name.endswith("_full_load") else ""
        },
        partition_defs={
            "daily": daily_partitions_erp,
            "monthly": monthly_partitions_erp,
            "full_load": None,
        },
        compute_kind="mssql",
        tags={
            "layer": "extract",
            "source": "erp",
            "entity": entity_tag,
        },
        append_partition_suffix=False,
        retry_policy=dg.RetryPolicy(max_retries=2, delay=30),
    )

    for asset_fn in assets:
        asset_fn.__name__ = config.output_name
        asset_fn.__qualname__ = config.output_name

    return assets
