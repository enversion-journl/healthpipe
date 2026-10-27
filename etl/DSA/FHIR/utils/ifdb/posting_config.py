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

# Tag keys for configurable parameters
TAG_CHUNK_SIZE = "fhir/chunk_size"
TAG_MAX_WORKERS = "fhir/max_workers"
TAG_ID_RESOLUTION_BATCH_SIZE = "fhir/id_resolution_batch_size"
TAG_AKTRES_CHUNK_SIZE = "fhir/aktres_chunk_size"


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


def aktivitetsresultat_chunk_size_from_context(
    context: dg.AssetExecutionContext,
    default: int,
) -> int:
    """Pipeline-chunkstørrelsen for aktivitetsresultat-posten, med run-tag-override.

    Default er miljøets ``IFDB_AKTIVITETSRESULTAT_CHUNK_SIZE``. Tagget
    ``fhir/aktres_chunk_size`` lader en målekørsel køre samme vindue med en anden
    chunkning — fx hele vinduet som én chunk eller 10.000 mod 25.000 — uden at
    røre code-serverens miljø. Ugyldige eller ikke-positive værdier ignoreres
    med en advarsel, så et tastefejl-tag aldrig kan vælte kørslen.
    """
    raw = _merged_context_tags(context).get(TAG_AKTRES_CHUNK_SIZE)
    if not isinstance(raw, (str, int)):
        return default
    try:
        value = int(str(raw).strip())
    except ValueError:
        dg.get_dagster_logger().warning(
            f"Ignoring run tag {TAG_AKTRES_CHUNK_SIZE}={raw!r}: not an integer; "
            f"using chunk size {default}"
        )
        return default
    if value < 1:
        dg.get_dagster_logger().warning(
            f"Ignoring run tag {TAG_AKTRES_CHUNK_SIZE}={value}: must be >= 1; "
            f"using chunk size {default}"
        )
        return default
    if value != default:
        dg.get_dagster_logger().info(
            f"Aktivitetsresultat chunk size overridden by run tag: {value} (default {default})"
        )
    return value


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
