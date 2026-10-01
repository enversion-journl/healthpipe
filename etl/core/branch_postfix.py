# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Branch-based asset and job postfixing for multi-deployment support.

This module enables multiple feature branches to be deployed simultaneously
to the same Dagster instance without asset/job name collisions.

Usage:
    Set the DAGSTER_BRANCH_POSTFIX environment variable to a unique identifier
    for each deployment. The postfix is ONLY applied if it starts with "feature".

    Example:
        DAGSTER_BRANCH_POSTFIX=feature-patient-validation

    This will postfix all assets and jobs with "_feature_patient_validation" to avoid collisions.

    Non-feature branches (e.g., "main", "develop", "hotfix-123") will NOT be postfixed,
    which is the default behavior for production/staging deployments.
"""

import os
import re
from collections.abc import Sequence
from typing import Any

import dagster as dg
from dagster._core.definitions.unresolved_asset_job_definition import (
    UnresolvedAssetJobDefinition,
)


def get_branch_postfix() -> str:
    """
    Get the branch postfix from environment variable.

    Only returns a postfix if the value starts with "feature" (case-insensitive).

    Returns:
        The sanitized branch postfix, or empty string if not set or not a feature branch.
    """
    raw_value = os.getenv("DAGSTER_BRANCH_POSTFIX", "").strip()

    # Only apply postfix for feature branches
    if not raw_value.lower().startswith("feature"):
        return ""

    # Sanitize: replace non-alphanumeric chars with underscore, lowercase
    postfix = re.sub(r"[^a-zA-Z0-9]+", "_", raw_value).lower().strip("_")
    return postfix


def get_postfixed_name(name: str) -> str:
    """
    Get a postfixed name for an asset or job.

    Args:
        name: The original name

    Returns:
        The postfixed name if DAGSTER_BRANCH_POSTFIX is set to a feature branch,
        otherwise the original name.
    """
    postfix = get_branch_postfix()
    if postfix:
        return f"{name}_{postfix}"
    return name


def get_postfixed_asset_key(key: dg.AssetKey | str | Sequence[str]) -> dg.AssetKey:
    """
    Get a postfixed AssetKey.

    Args:
        key: The original AssetKey, string, or sequence of strings

    Returns:
        A postfixed AssetKey if DAGSTER_BRANCH_POSTFIX is set to a feature branch.
    """
    postfix = get_branch_postfix()

    # Convert to AssetKey if needed - check AssetKey FIRST before Sequence
    if isinstance(key, dg.AssetKey):
        asset_key = key
    elif isinstance(key, str):
        asset_key = dg.AssetKey([key])
    elif isinstance(key, list | tuple):
        asset_key = dg.AssetKey(list(key))
    else:
        # Assume it's already an AssetKey-like object
        asset_key = key

    if postfix:
        # Append postfix to the last element of the key path
        path = list(asset_key.path)
        if path:
            path[-1] = f"{path[-1]}_{postfix}"
        else:
            path = [postfix]
        return dg.AssetKey(path)
    return asset_key


def postfix_asset(
    asset_def: dg.AssetsDefinition,
    all_key_replacements: dict[dg.AssetKey, dg.AssetKey] | None = None,
) -> dg.AssetsDefinition:
    """
    Apply branch postfix to an asset definition.

    This wraps an existing asset definition and applies the branch postfix
    to its key, making it unique per deployment.

    Args:
        asset_def: The original asset definition
        all_key_replacements: Optional pre-computed mapping of all asset keys to their
            postfixed versions. If provided, this is used to also update dependency keys.

    Returns:
        A new asset definition with postfixed key (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return asset_def

    # If we have the full replacement mapping, use it (includes dependency updates)
    if all_key_replacements:
        return asset_def.with_attributes(
            asset_key_replacements=all_key_replacements,
        )

    # Fallback: only replace this asset's own keys (may break deps)
    key_replacements = {}
    for key in asset_def.keys:
        new_key = get_postfixed_asset_key(key)
        key_replacements[key] = new_key

    return asset_def.with_attributes(
        asset_key_replacements=key_replacements,
    )


def build_key_replacement_map(
    assets: list[dg.AssetsDefinition | dg.SourceAsset],
) -> dict[dg.AssetKey, dg.AssetKey]:
    """
    Build a mapping of original asset keys to their postfixed versions.

    This map is used by both asset and job postfixing to ensure consistency.

    Args:
        assets: List of asset definitions

    Returns:
        Dictionary mapping original keys to postfixed keys
    """
    postfix = get_branch_postfix()
    if not postfix:
        return {}

    all_key_replacements: dict[dg.AssetKey, dg.AssetKey] = {}
    for asset in assets:
        if isinstance(asset, dg.AssetsDefinition):
            for key in asset.keys:
                all_key_replacements[key] = get_postfixed_asset_key(key)
        elif isinstance(asset, dg.SourceAsset):
            all_key_replacements[asset.key] = get_postfixed_asset_key(asset.key)

    return all_key_replacements


def postfix_assets(
    assets: list[dg.AssetsDefinition | dg.SourceAsset],
    key_replacements: dict[dg.AssetKey, dg.AssetKey] | None = None,
) -> list[dg.AssetsDefinition | dg.SourceAsset]:
    """
    Apply branch postfix to a list of asset definitions.

    This function builds a complete key replacement mapping first, then applies
    it to all assets. This ensures that dependency references are also updated.

    Args:
        assets: List of asset definitions
        key_replacements: Optional pre-computed key replacement map. If not provided,
            one will be built from the assets list.

    Returns:
        List of asset definitions with postfixed keys (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return assets

    # Use provided key replacements or build from assets
    all_key_replacements = key_replacements or build_key_replacement_map(assets)

    # Apply replacements to all assets
    result = []
    for asset in assets:
        if isinstance(asset, dg.AssetsDefinition):
            result.append(postfix_asset(asset, all_key_replacements))
        elif isinstance(asset, dg.SourceAsset):
            # SourceAssets need their key postfixed
            result.append(
                dg.SourceAsset(
                    key=get_postfixed_asset_key(asset.key),
                    description=asset.description,
                    metadata=asset.metadata,
                    io_manager_key=asset.io_manager_key,
                    partitions_def=asset.partitions_def,
                    group_name=asset.group_name,
                )
            )
        else:
            result.append(asset)
    return result


def postfix_asset_checks(
    checks: list[dg.AssetChecksDefinition],
    key_replacements: dict[dg.AssetKey, dg.AssetKey] | None = None,
) -> list[dg.AssetChecksDefinition]:
    """
    Apply branch postfix to asset check definitions.

    Asset checks reference a target asset key (e.g. ``dw_quality``). When that
    anchor asset is postfixed for a feature branch, the checks must be remapped
    to the postfixed key so Dagster can resolve them.

    Args:
        checks: List of asset check definitions.
        key_replacements: Pre-computed mapping of original asset keys to their
            postfixed versions (from ``build_key_replacement_map``).

    Returns:
        List of asset check definitions with remapped target asset keys
        (if postfix is set).
    """
    postfix = get_branch_postfix()
    if not postfix or not key_replacements:
        return checks

    return [check.with_attributes(asset_key_replacements=key_replacements) for check in checks]


def postfix_asset_selection(
    selection: dg.AssetSelection,
    key_replacements: dict[dg.AssetKey, dg.AssetKey],
) -> dg.AssetSelection:
    """
    Transform an AssetSelection to reference postfixed asset keys.

    This handles selections created with AssetSelection.assets() by
    replacing their keys with postfixed versions.

    Selections created with AssetSelection.groups() are returned unchanged
    since they resolve dynamically against the asset graph.

    Args:
        selection: The original asset selection
        key_replacements: Mapping of original keys to postfixed keys

    Returns:
        A new AssetSelection with postfixed keys (for key-based selections)
        or the original selection (for group-based selections)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return selection

    # Try to access internal keys for KeysAssetSelection
    # This handles AssetSelection.assets("key1", "key2") cases
    if hasattr(selection, "selected_keys"):
        original_keys = selection.selected_keys
        postfixed_keys = []
        for key in original_keys:
            if key in key_replacements:
                postfixed_keys.append(key_replacements[key])
            else:
                # Key not in replacement map - apply postfix directly
                postfixed_keys.append(get_postfixed_asset_key(key))
        return dg.AssetSelection.assets(*postfixed_keys)

    # Handle wrapper selections with a single child (e.g. RequiredNeighborsAssetSelection)
    if hasattr(selection, "child"):
        postfixed_child = postfix_asset_selection(selection.child, key_replacements)
        selection_type = type(selection).__name__
        if "RequiredNeighbors" in selection_type:
            return postfixed_child.required_multi_asset_neighbors()
        # Fallback: reconstruct via copy and replace the child
        import copy

        new_selection = copy.copy(selection)
        object.__setattr__(new_selection, "child", postfixed_child)
        return new_selection

    # Handle binary selections (SubtractAssetSelection, and OR/AND in some Dagster versions)
    # These use left/right instead of operands.
    if hasattr(selection, "left") and hasattr(selection, "right"):
        postfixed_left = postfix_asset_selection(selection.left, key_replacements)
        postfixed_right = postfix_asset_selection(selection.right, key_replacements)
        selection_type = type(selection).__name__
        if "Subtract" in selection_type:
            return postfixed_left - postfixed_right
        elif "Or" in selection_type:
            return postfixed_left | postfixed_right
        elif "And" in selection_type:
            return postfixed_left & postfixed_right
        import copy

        new_selection = copy.copy(selection)
        object.__setattr__(new_selection, "left", postfixed_left)
        object.__setattr__(new_selection, "right", postfixed_right)
        return new_selection

    # Handle composite selections with operands list (OR, AND in Dagster 1.11+)
    if hasattr(selection, "operands"):
        postfixed_operands = [
            postfix_asset_selection(op, key_replacements) for op in selection.operands
        ]
        selection_type = type(selection).__name__
        if "Or" in selection_type:
            result = postfixed_operands[0]
            for operand in postfixed_operands[1:]:
                result = result | operand
            return result
        elif "And" in selection_type:
            result = postfixed_operands[0]
            for operand in postfixed_operands[1:]:
                result = result & operand
            return result
        elif "Subtract" in selection_type:
            result = postfixed_operands[0]
            for operand in postfixed_operands[1:]:
                result = result - operand
            return result

    # For other selection types (groups, tags, all, etc.), return as-is
    # They resolve dynamically and will find postfixed assets
    return selection


def postfix_job(
    job_def: dg.JobDefinition | UnresolvedAssetJobDefinition,
    key_replacements: dict[dg.AssetKey, dg.AssetKey] | None = None,
) -> dg.JobDefinition | UnresolvedAssetJobDefinition:
    """
    Apply branch postfix to a job definition name and asset selection.

    Note: This creates a new job with a postfixed name and updated
    asset selection that references postfixed asset keys.

    Args:
        job_def: The original job definition
        key_replacements: Optional mapping of original keys to postfixed keys.
            Required for proper selection transformation.

    Returns:
        A new job definition with postfixed name and selection (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return job_def

    # Handle UnresolvedAssetJobDefinition (from define_asset_job)
    if isinstance(job_def, UnresolvedAssetJobDefinition):
        # Transform the selection to use postfixed keys
        postfixed_selection = job_def.selection
        if key_replacements:
            postfixed_selection = postfix_asset_selection(job_def.selection, key_replacements)

        # Create a new UnresolvedAssetJobDefinition with postfixed name and selection
        return UnresolvedAssetJobDefinition(
            name=f"{job_def.name}_{postfix}",
            selection=postfixed_selection,
            config=job_def.config,
            description=job_def.description,
            tags=job_def.tags,
            metadata=job_def.metadata,
            partitions_def=job_def.partitions_def,
            executor_def=job_def.executor_def,
            hooks=job_def.hooks,
            op_retry_policy=job_def.op_retry_policy,
            run_tags=job_def.run_tags,
        )

    # Handle regular JobDefinition
    if hasattr(job_def, "with_attributes"):
        return job_def.with_attributes(
            name=f"{job_def.name}_{postfix}",
        )

    # Fallback: return as-is if we can't modify
    return job_def


def postfix_jobs(
    jobs: list[dg.JobDefinition | UnresolvedAssetJobDefinition],
    key_replacements: dict[dg.AssetKey, dg.AssetKey] | None = None,
) -> list[dg.JobDefinition | UnresolvedAssetJobDefinition]:
    """
    Apply branch postfix to a list of job definitions.

    Args:
        jobs: List of job definitions
        key_replacements: Optional mapping of original keys to postfixed keys.
            Required for proper selection transformation in jobs that use
            AssetSelection.assets().

    Returns:
        List of job definitions with postfixed names and selections (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return jobs

    return [postfix_job(job, key_replacements) for job in jobs]


def postfix_schedule(
    schedule_def: dg.ScheduleDefinition,
    postfixed_jobs: list[dg.JobDefinition | UnresolvedAssetJobDefinition] | None = None,
) -> dg.ScheduleDefinition:
    """
    Apply branch postfix to a schedule definition.

    Args:
        schedule_def: The original schedule definition
        postfixed_jobs: Optional list of postfixed jobs. If provided, the schedule's
            job reference will be updated to point to the postfixed version.

    Returns:
        A new schedule definition with postfixed name (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return schedule_def

    # Find the postfixed version of this schedule's job
    job_to_use = schedule_def.job
    if postfixed_jobs:
        original_job_name = schedule_def.job.name
        postfixed_job_name = f"{original_job_name}_{postfix}"
        for job in postfixed_jobs:
            if job.name == postfixed_job_name:
                job_to_use = job
                break

    # Get the execution function if it exists (for custom @schedule decorators that return RunRequest)
    # This is crucial for schedules that provide partition_key - without it, the schedule
    # will revert to a simple cron schedule without partition context
    execution_fn = getattr(schedule_def, "_execution_fn", None)

    # Create a new schedule with postfixed name and job reference
    # We need to recreate the schedule since with_attributes doesn't support name changes
    return dg.ScheduleDefinition(
        name=f"{schedule_def.name}_{postfix}",
        cron_schedule=schedule_def.cron_schedule,
        job=job_to_use,
        run_config=schedule_def.run_config if hasattr(schedule_def, "run_config") else None,
        tags=schedule_def.tags,
        metadata=schedule_def.metadata,
        description=schedule_def.description,
        execution_timezone=schedule_def.execution_timezone,
        default_status=schedule_def.default_status,
        execution_fn=execution_fn,
    )


def postfix_schedules(
    schedules: list[dg.ScheduleDefinition],
    postfixed_jobs: list[dg.JobDefinition | UnresolvedAssetJobDefinition] | None = None,
) -> list[dg.ScheduleDefinition]:
    """
    Apply branch postfix to a list of schedule definitions.

    Args:
        schedules: List of schedule definitions
        postfixed_jobs: Optional list of postfixed jobs. If provided, schedule job
            references will be updated to point to the postfixed versions.

    Returns:
        List of schedule definitions with postfixed names (if postfix is set)
    """
    postfix = get_branch_postfix()
    if not postfix:
        return schedules

    return [postfix_schedule(schedule, postfixed_jobs) for schedule in schedules]


def is_feature_branch_deployment() -> bool:
    """
    Check if this is a feature branch deployment (has postfix set).

    Returns:
        True if DAGSTER_BRANCH_POSTFIX is set to a feature branch.
    """
    return bool(get_branch_postfix())


def log_branch_info(logger: Any = None) -> None:
    """
    Log information about the current branch deployment.

    Args:
        logger: Optional logger to use. If not provided, uses print.
    """
    postfix = get_branch_postfix()
    raw_value = os.getenv("DAGSTER_BRANCH_POSTFIX", "").strip()

    if postfix:
        msg = f"🌿 Feature branch deployment active: postfix='_{postfix}'"
    elif raw_value:
        msg = f"🌳 Non-feature branch deployment (no postfix applied): '{raw_value}'"
    else:
        msg = "🌳 Production deployment (no branch postfix)"

    if logger:
        logger.info(msg)
    else:
        print(msg)


# Backwards compatibility aliases (deprecated, use postfix_* functions)
get_branch_postfix = get_branch_postfix
get_prefixed_name = get_postfixed_name
get_prefixed_asset_key = get_postfixed_asset_key
prefix_asset = postfix_asset
prefix_assets = postfix_assets
prefix_job = postfix_job
prefix_jobs = postfix_jobs
prefix_schedule = postfix_schedule
prefix_schedules = postfix_schedules
is_branch_deployment = is_feature_branch_deployment
