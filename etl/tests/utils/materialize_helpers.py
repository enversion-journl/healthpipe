# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Helpers for materializing Dagster assets in tests.

This module provides:
- Asset lookup utilities
- Single and multi-asset materialization
- Pipeline materialization helpers
- Materialization result retrieval
"""

import logging
from typing import Any

import dagster as dg

logger = logging.getLogger(__name__)


# =============================================================================
# Asset Lookup Utilities
# =============================================================================


def _find_asset_definition(
    defs: dg.Definitions,
    target_key: dg.AssetKey,
) -> dg.AssetsDefinition | None:
    """
    Find an asset definition by its key.

    Args:
        defs: Dagster definitions object
        target_key: The asset key to find

    Returns:
        The AssetsDefinition if found, None otherwise
    """
    for asset in defs.assets:
        if not isinstance(asset, dg.AssetsDefinition):
            continue
        if target_key in asset.keys:
            return asset
    return None


def _find_asset_definitions(
    defs: dg.Definitions,
    asset_keys: list[str | dg.AssetKey],
) -> list[dg.AssetsDefinition]:
    """
    Find multiple asset definitions by their keys.

    Args:
        defs: Dagster definitions object
        asset_keys: List of asset key strings or AssetKey objects to find

    Returns:
        List of AssetsDefinition objects

    Raises:
        ValueError: If not all assets could be found
    """
    assets_found = []
    for asset_key in asset_keys:
        # Handle both string and AssetKey inputs
        if isinstance(asset_key, dg.AssetKey):
            target_key = asset_key
        else:
            target_key = dg.AssetKey.from_user_string(asset_key)
        asset_def = _find_asset_definition(defs, target_key)
        if asset_def:
            assets_found.append(asset_def)

    if len(assets_found) != len(asset_keys):
        found_keys = [str(list(asset.keys)[0]) for asset in assets_found]
        raise ValueError(f"Could not find all assets. Requested: {asset_keys}, Found: {found_keys}")

    return assets_found


# =============================================================================
# Partition Management
# =============================================================================


def _ensure_partition_exists(
    instance: dg.DagsterInstance,
    partition: str,
) -> None:
    """
    Ensure a partition exists in the Dagster instance.

    For DynamicPartitionsDefinition, creates the partition if it doesn't exist.
    For MonthlyPartitionsDefinition, validates the partition format.

    Args:
        instance: Dagster instance
        partition: Partition key

    Raises:
        ValueError: If partition format is invalid
    """
    from core.partitions import (
        _validate_partition_key,
        monthly_partitions,
        unified_partitions,
    )

    # Handle DynamicPartitionsDefinition
    if hasattr(unified_partitions, "name") and isinstance(
        unified_partitions, dg.DynamicPartitionsDefinition
    ):
        if not _validate_partition_key(partition):
            raise ValueError(
                f"Invalid partition key format: {partition}. "
                "Must be 'full_load', 'YYYY-MM-DD' (daily), or 'YYYY-MM' (monthly)"
            )
        if not instance.has_dynamic_partition(unified_partitions.name, partition):
            instance.add_dynamic_partitions(unified_partitions.name, [partition])

    # Handle MonthlyPartitionsDefinition
    elif isinstance(monthly_partitions, dg.MonthlyPartitionsDefinition):
        if not _validate_partition_key(partition):
            raise ValueError(
                f"Invalid partition key format: {partition}. "
                "Must be 'full_load', 'YYYY-MM-DD' (daily), or 'YYYY-MM' (monthly)"
            )
        # Validate the partition is within valid range
        try:
            from datetime import datetime

            partition_date = datetime.strptime(partition, "%Y-%m")
            start_date = datetime.strptime("2015-01", "%Y-%m")
            if partition_date < start_date:
                raise ValueError(f"Partition {partition} is before the start date 2015-01")
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError(
                    f"Invalid monthly partition format: {partition}. Expected YYYY-MM"
                ) from e
            raise


# =============================================================================
# Single Asset Materialization
# =============================================================================


def materialize_asset(
    instance: dg.DagsterInstance,
    asset_key: str,
    partition: str | None = None,
    resources: dict[str, Any] | None = None,
    raise_on_error: bool = True,
) -> dg.MaterializeResult | None:
    """
    Materialize a single asset.

    Args:
        instance: Dagster instance
        asset_key: Asset key as string
        partition: Optional partition key
        resources: Dictionary of resources to use
        raise_on_error: If True, raise exception on error; if False, return None

    Returns:
        MaterializeResult from asset execution, or None on error
    """
    from definitions import defs

    target_key = dg.AssetKey.from_user_string(asset_key)
    asset_def = _find_asset_definition(defs, target_key)

    if not asset_def:
        raise ValueError(f"Asset {asset_key} not found in definitions")

    try:
        return dg.materialize(
            assets=[asset_def],
            partition_key=partition,
            resources=resources or {},
            instance=instance,
        )
    except Exception as e:
        if raise_on_error:
            raise
        logger.error(f"Error materializing asset {asset_key}: {e}")
        return None


# =============================================================================
# Multi-Asset Materialization
# =============================================================================


def materialize_assets_with_dependencies(
    instance: dg.DagsterInstance,
    asset_keys: list[str],
    partition: str | None = None,
    resources: dict[str, Any] | None = None,
    raise_on_error: bool = True,
    run_tags: dict[str, str] | None = None,
) -> dg.MaterializeResult | None:
    """
    Materialize multiple assets together to handle dependencies properly.

    Args:
        instance: Dagster instance
        asset_keys: List of asset keys as strings
        partition: Optional partition key
        resources: Dictionary of resources to use
        raise_on_error: If True, raise exception on error; if False, return None
        run_tags: Optional dictionary of tags to pass to the run (e.g., extraction mode)

    Returns:
        MaterializeResult from asset execution, or None on error
    """
    from definitions import defs

    assets_to_materialize = _find_asset_definitions(defs, asset_keys)

    try:
        if partition:
            _ensure_partition_exists(instance, partition)

        return dg.materialize(
            assets=assets_to_materialize,
            partition_key=partition,
            resources=resources or {},
            instance=instance,
            tags=run_tags or {},
        )
    except Exception as e:
        if raise_on_error:
            raise
        logger.error(f"Error materializing assets {asset_keys}: {e}")
        return None


# =============================================================================
# Pipeline Materialization Helpers
# =============================================================================


def materialize_patient_pipeline(
    instance: dg.DagsterInstance,
    partition: str,
    resources: dict[str, Any] | None = None,
    use_real_resources: bool = False,
    assets_to_materialize: list[str] | None = None,
    datasource: str = "ifdb",
    run_tags: dict[str, str] | None = None,
) -> dict[str, Any]:
    """
    Materialize entire patient pipeline with PATIENT_ID filtering.

    Args:
        instance: Dagster instance
        partition: Partition key
        resources: Dictionary of resources
        use_real_resources: If True, uses real resources with query filtering
        assets_to_materialize: List of asset keys to materialize (default: patient pipeline assets)
        datasource: Datasource to use (mssql_source, ifdb_source, or ifdb)

    Returns:
        Dictionary mapping asset keys to materialization results
    """
    from core.partitions import (
        _validate_partition_key,
        monthly_partitions,
        unified_partitions,
    )
    from definitions import defs

    # Default assets for patient pipeline based on datasource
    if assets_to_materialize is None:
        if datasource == "mssql_source":
            assets_to_materialize = [
                "transformed_fhir_patient_partitioned_mssql",
                "patient_fhir_server_partitioned_mssql",
            ]
        elif datasource == "ifdb_source":
            assets_to_materialize = [
                "transformed_fhir_patient_partitioned_ifdb",
                "patient_fhir_server_partitioned_ifdb",
            ]
        else:
            raise ValueError(f"Invalid datasource: {datasource}")

    assets_to_materialize_defs = _find_asset_definitions(defs, assets_to_materialize)

    try:
        _ensure_partition_exists(instance, partition)

        # Handle DynamicPartitionsDefinition
        if hasattr(unified_partitions, "name") and isinstance(
            unified_partitions, dg.DynamicPartitionsDefinition
        ):
            # Validate partition key format
            if not _validate_partition_key(partition):
                raise ValueError(
                    f"Invalid partition key format: {partition}. "
                    "Must be 'full_load', 'YYYY-MM-DD' (daily), or 'YYYY-MM' (monthly)"
                )
            # Check if partition exists, if not add it
            if not instance.has_dynamic_partition(unified_partitions.name, partition):
                instance.add_dynamic_partitions(unified_partitions.name, [partition])

        # Handle MonthlyPartitionsDefinition - validate that the partition key exists
        elif isinstance(monthly_partitions, dg.MonthlyPartitionsDefinition):
            # Validate partition key format
            if not _validate_partition_key(partition):
                raise ValueError(
                    f"Invalid partition key format: {partition}. "
                    "Must be 'full_load', 'YYYY-MM-DD' (daily), or 'YYYY-MM' (monthly)"
                )
            # For MonthlyPartitionsDefinition, Dagster automatically handles partition existence
            # But we should validate the partition is within the valid range
            try:
                from datetime import datetime

                partition_date = datetime.strptime(partition, "%Y-%m")
                start_date = datetime.strptime("2021-01", "%Y-%m")
                if partition_date < start_date:
                    raise ValueError(f"Partition {partition} is before the start date 2021-01")
            except ValueError as e:
                if "time data" in str(e):
                    raise ValueError(
                        f"Invalid monthly partition format: {partition}. Expected YYYY-MM"
                    ) from e
                raise

        # Materialize all assets together to handle dependencies properly
        result = dg.materialize(
            assets=assets_to_materialize_defs,
            partition_key=partition,
            resources=resources or {},
            instance=instance,
            tags=run_tags or {},
        )

        # Return results keyed by asset key string
        return {asset_key: result for asset_key in assets_to_materialize}

    except Exception as e:
        logger.error(f"Failed to materialize patient pipeline: {e}")
        return {"__pipeline_error__": str(e)}


def materialize_erp_pipeline(
    instance: dg.DagsterInstance,
    asset_keys: list[str],
    resources: dict[str, Any] | None = None,
    raise_on_error: bool = True,
) -> dg.MaterializeResult | None:
    """
    Materialize erp pipeline assets.

    This is a convenience wrapper around materialize_assets_with_dependencies
    specifically for the erp domain, providing a consistent interface
    similar to materialize_patient_pipeline() for FHIR tests.

    Args:
        instance: Dagster instance
        asset_keys: List of asset keys to materialize
        resources: Dictionary of resources to use
        raise_on_error: If True, raise exception on error; if False, return None

    Returns:
        MaterializeResult from asset execution, or None on error
    """
    return materialize_assets_with_dependencies(
        instance=instance,
        asset_keys=asset_keys,
        partition=None,  # ERP assets are not partitioned
        resources=resources,
        raise_on_error=raise_on_error,
    )


# =============================================================================
# Result Retrieval
# =============================================================================


def get_materialization_result(
    instance: dg.DagsterInstance,
    asset_key: str,
) -> dict[str, Any] | None:
    """
    Get the latest materialization result metadata for an asset.

    Args:
        instance: Dagster instance
        asset_key: Asset key string

    Returns:
        Metadata dictionary from latest materialization, or None if not found
    """
    from dagster import AssetKey
    from dagster._core.event_api import EventRecordsFilter
    from dagster._core.events import DagsterEventType

    key = AssetKey.from_user_string(asset_key)

    try:
        records = instance.get_event_records(
            EventRecordsFilter(
                asset_key=key,
                event_type=DagsterEventType.ASSET_MATERIALIZATION,
            ),
            limit=1,
        )

        if records:
            return records[
                0
            ].event_log_entry.dagster_event.event_specific_data.materialization.metadata
    except Exception as e:
        logger.warning(f"Could not get materialization result for {asset_key}: {e}")

    return None
