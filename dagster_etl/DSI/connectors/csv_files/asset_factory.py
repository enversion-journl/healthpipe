# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Asset factory for CSV file extraction assets.

Delegates to the shared core factory for asset generation while providing
the CSV-specific asset builder with CsvExtractor.
"""

from collections.abc import Callable
from typing import Any

import dagster as dg
from core.partitions import full_load_partitions
from DSI.connectors.csv_files.extractor import CsvExtractor
from DSI.connectors.csv_files.models import CsvExtractionConfig
from DSI.connectors.factory import create_extraction_assets as _generic_factory


def create_csv_extraction_assets(
    entity_name: str,
    config: CsvExtractionConfig,
    source_system: str = "csv",
    source_deps: list[str] | None = None,
    custom_builder: Callable[[dict[str, Any], str], Callable] | None = None,
    include_full_load: bool = True,
    include_daily: bool = False,
    include_monthly: bool = False,
    group_prefix: str = "csv_extraction",
    asset_key_prefix: list[str] | None = None,
) -> tuple[Callable, ...]:
    """
    Create extraction assets for CSV files.

    CSV files are typically full-load only (the entire file is read each time),
    but daily/monthly partitions can be enabled if the CSV data supports it.

    Args:
        entity_name: Name of the entity (e.g., "manuelle_mapninger").
        config: CSV extraction configuration.
        source_system: Source system identifier for logging.
        source_deps: List of source asset names to depend on.
        custom_builder: Optional builder replacing the default _csv_builder.
            Signature: (asset_kwargs, partition_type) -> decorated asset function.
        include_full_load: Whether to create a full_load asset (default True).
        include_daily: Whether to create a daily-partitioned asset.
        include_monthly: Whether to create a monthly-partitioned asset.
        group_prefix: Base group name for the assets.
        asset_key_prefix: Prefix for nested asset keys.
            Defaults to ["csv_extract"].

    Returns:
        Tuple of asset functions (only for enabled partition types).
    """
    deps = [dg.AssetKey(dep) for dep in (source_deps or [])]

    if asset_key_prefix is None:
        asset_key_prefix = ["csv_extract"]

    def _csv_builder(asset_kwargs: dict[str, Any], partition_type: str) -> Callable:
        @dg.asset(**asset_kwargs)
        def _asset_fn(
            context: dg.AssetExecutionContext,
        ) -> list[dict[str, Any]]:
            extractor = CsvExtractor(context=context, source_system=source_system)
            result = extractor.extract(config)
            return result.data

        return _asset_fn

    builder = custom_builder if custom_builder is not None else _csv_builder

    return _generic_factory(
        config=config,
        entity_name=entity_name,
        asset_builder=builder,
        include_daily=include_daily,
        include_monthly=include_monthly,
        include_full_load=include_full_load,
        asset_key_prefix=asset_key_prefix,
        group_prefix=group_prefix,
        partition_defs={
            "full_load": full_load_partitions,
        },
        compute_kind="csv",
        tags={"layer": "extract", "source": source_system, "entity": entity_name},
        deps=deps if deps else None,
    )
