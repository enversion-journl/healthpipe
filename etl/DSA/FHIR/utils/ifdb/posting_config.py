# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Configuration module for FHIR posting operations.

This module provides simplified configuration for FHIR resource posting.
After benchmarking, we've standardized on:
- ID Resolution: central identifier lookup (18,000x faster than FHIR API)
- Posting: Direct DB via executemany() (4-7x faster than single inserts)

These strategies are now hardcoded - no configuration needed.
"""

from dataclasses import dataclass
from typing import Any

import dagster as dg
from core.fhir.child_resource_reconcile import DEFAULT_RETIRE_CEILING, RetireCeiling

# Tag keys for configurable parameters
TAG_CHUNK_SIZE = "fhir/chunk_size"
TAG_MAX_WORKERS = "fhir/max_workers"
TAG_ID_RESOLUTION_BATCH_SIZE = "fhir/id_resolution_batch_size"
TAG_RETIRE_CEILING_FLOOR = "fhir/retire_ceiling_floor"
TAG_RETIRE_CEILING_FRACTION = "fhir/retire_ceiling_fraction"


def _merged_context_tags(context: dg.AssetExecutionContext) -> dict[str, str]:
    """Run-tags oven på asset-tags — run-tagget vinder. Tåler attrapper i test."""
    run_tags = context.run.tags if hasattr(context, "run") and context.run else {}
    asset_tags: dict[str, str] = {}
    try:
        asset_key = context.asset_key
        asset_def = context.job_def.asset_layer.asset_graph.get(asset_key)
        asset_tags = dict(asset_def.tags) if asset_def and asset_def.tags else {}
    except Exception:
        pass
    return {**asset_tags, **run_tags}


def retire_ceiling_from_context(context: dg.AssetExecutionContext) -> RetireCeiling:
    """Pensioneringsloftet for kørslen, med run-tag-override.

    Defaults er DEFAULT_RETIRE_CEILING — bevidst u-kalibrerede sikkerhedsgrænser.
    En varslet masseoprydning køres med hævet loft via run-tags, fx
    ``fhir/retire_ceiling_fraction=1.0`` (slår loftet fra, da retire aldrig kan
    overstige de levende børn) eller et højere ``fhir/retire_ceiling_floor``.
    """
    tags = _merged_context_tags(context)
    ceiling = RetireCeiling(
        floor=int(tags.get(TAG_RETIRE_CEILING_FLOOR, DEFAULT_RETIRE_CEILING.floor)),
        fraction=float(tags.get(TAG_RETIRE_CEILING_FRACTION, DEFAULT_RETIRE_CEILING.fraction)),
    )
    if ceiling != DEFAULT_RETIRE_CEILING:
        dg.get_dagster_logger().info(
            f"Retire ceiling overridden by run tags: floor={ceiling.floor}, "
            f"fraction={ceiling.fraction}"
        )
    return ceiling


@dataclass
class FHIRPostingConfig:
    """
    Simplified configuration for FHIR posting.

    After benchmarking, we use fixed optimal settings:
    - Always use central identifier lookup for ID resolution
    - Always use direct DB posting via executemany()
    - Strict mode: any posting failure raises an exception

    Configurable options are limited to:
    - chunk_size: Number of resources per batch
    - max_workers: Parallel workers for operations
    """

    chunk_size: int
    max_workers: int
    id_resolution_batch_size: int

    @classmethod
    def from_context(
        cls,
        context: dg.AssetExecutionContext,
        defaults: dict[str, Any] | None = None,
    ) -> "FHIRPostingConfig":
        """
        Build configuration from Dagster context.

        Args:
            context: Dagster execution context
            defaults: Optional dictionary of default values to use as fallback

        Returns:
            FHIRPostingConfig instance with merged configuration
        """
        defaults = defaults or {}

        # Merge tags: run_tags override asset_tags
        merged_tags = _merged_context_tags(context)

        def get_tag(key: str, default: Any) -> str:
            """Get tag value with fallback to defaults."""
            return str(merged_tags.get(key, defaults.get(key, default)))

        # Parse numeric values
        chunk_size = int(get_tag(TAG_CHUNK_SIZE, 500))
        max_workers = int(get_tag(TAG_MAX_WORKERS, 8))
        id_resolution_batch_size = int(get_tag(TAG_ID_RESOLUTION_BATCH_SIZE, 1000))

        config = cls(
            chunk_size=chunk_size,
            max_workers=max_workers,
            id_resolution_batch_size=id_resolution_batch_size,
        )

        # Log the resolved configuration
        dg.get_dagster_logger().info(
            f"FHIR posting config: chunk_size={config.chunk_size}, "
            f"max_workers={config.max_workers}"
        )

        return config

    @classmethod
    def default(cls) -> "FHIRPostingConfig":
        """Create a default configuration without Dagster context."""
        return cls(
            chunk_size=500,
            max_workers=8,
            id_resolution_batch_size=1000,
        )
