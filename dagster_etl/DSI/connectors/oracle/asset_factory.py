# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Asset factory for IFDB extraction assets.

Delegates to the shared core factory for asset generation while providing
the IFDB-specific asset builder with typed IFDBOracleResource injection.
"""

from collections.abc import Callable
from typing import Any

import dagster as dg
from core.extract import materialize_row_batches_to_pickle_artifact
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from core.resources import IFDBOracleResource
from DSI.connectors.factory import create_extraction_assets as _generic_factory
from DSI.connectors.oracle.extractor import OracleExtractor
from DSI.connectors.oracle.models import TableExtractionConfig


def create_extraction_assets(
    entity_name: str,
    config: TableExtractionConfig,
    source_deps: list[str] | None = None,
    custom_builder: Callable[[dict[str, Any], str], Callable] | None = None,
    include_daily: bool = True,
    include_monthly: bool = True,
    include_full_load: bool = True,
    extra_tags: dict[str, str] | None = None,
) -> tuple[Callable, ...]:
    """
    Create daily, monthly, and full_load extraction assets for an IFDB entity.

    Args:
        entity_name: Name of the entity (e.g., "patient", "encounter")
        config: Table extraction configuration
        source_deps: List of source asset names to depend on
        custom_builder: Optional builder replacing the default _ifdb_builder.
            Signature: (asset_kwargs, partition_type) -> decorated asset function.
        include_daily: Whether to create a daily-partitioned asset.
        include_monthly: Whether to create a monthly-partitioned asset.
        include_full_load: Whether to create a full_load-partitioned asset.
        extra_tags: Additional tags merged into the default tags dict.

    Returns:
        Tuple of asset functions (only for enabled partition types)
    """
    deps = [dg.AssetKey(dep) for dep in (source_deps or [])]

    def _ifdb_builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _asset_fn(
            context: dg.AssetExecutionContext,
            ifdb_oracle: IFDBOracleResource,
        ) -> Any:
            partition_key = context.partition_key if context.has_partition_key else None
            batching = config.batching
            if batching is not None:
                policy = batching.resolved(fallback_env_prefix="IFDB_EXTRACT")
                if partition_type == "daily" and policy.write_batch_sidecars:
                    context.log.info(
                        "Daily extraction for %s uses the main pickle only; "
                        "batch sidecars stay reserved for larger partitions",
                        config.table,
                    )
                    policy = policy.with_batch_sidecars(False)
                if policy.enabled:
                    context.log.info(
                        "Executing batched extraction for %s (batch_size=%d)",
                        config.table,
                        policy.fetch_size,
                    )
                    with ifdb_oracle.get_connection() as conn:
                        extractor = OracleExtractor(conn, context=context, source_system="IFDB")
                        batch_source = extractor.make_row_batch_source(
                            config,
                            partition_key,
                            partition_type,
                            policy,
                        )
                        return materialize_row_batches_to_pickle_artifact(
                            batch_source=batch_source,
                            policy=policy,
                            context=context,
                            source_label=config.table,
                        )

                context.log.info(
                    "Batched extraction disabled for %s; using legacy materialization",
                    config.table,
                )

            with ifdb_oracle.get_connection() as conn:
                extractor = OracleExtractor(conn, context=context, source_system="IFDB")
                result = extractor.extract(
                    config,
                    partition_key,
                    partition_type,
                )
            return result.data

        return _asset_fn

    builder = custom_builder if custom_builder is not None else _ifdb_builder
    io_manager_key = (
        "batched_extract_io_manager"
        if custom_builder is None and config.batching is not None
        else "io_manager"
    )

    result = _generic_factory(
        config=config,
        entity_name=entity_name,
        asset_builder=builder,
        include_daily=include_daily,
        include_monthly=include_monthly,
        include_full_load=include_full_load,
        asset_key_prefix=["ifdb_extract"],
        group_prefix="ifdb_extraction",
        partition_defs={
            "daily": daily_partitions,
            "monthly": monthly_partitions,
            "full_load": full_load_partitions,
        },
        io_manager_key=io_manager_key,
        tags={"layer": "extract", "source": "ifdb", "entity": entity_name, **(extra_tags or {})},
        deps=deps if deps else None,
    )

    return result
