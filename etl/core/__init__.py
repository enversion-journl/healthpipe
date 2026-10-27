# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Core utilities for Dagster ETL pipelines.

This module provides core functionality for the Dagster ETL project, including:
- Branch deployment utilities (postfix/prefix functions)
- Warning filter management for Dagster BetaWarnings
"""

from core.branch_postfix import (
    get_branch_postfix,
    get_postfixed_asset_key,
    get_postfixed_name,
    get_prefixed_name,
    is_branch_deployment,
    is_feature_branch_deployment,
    log_branch_info,
    postfix_asset,
    postfix_assets,
    postfix_job,
    postfix_jobs,
    postfix_schedule,
    postfix_schedules,
    prefix_asset,
    prefix_assets,
    prefix_job,
    prefix_jobs,
    prefix_schedule,
    prefix_schedules,
)
from core.warning_filters import apply_observable_source_asset_filter

__all__ = [
    # Warning filter functions
    "apply_observable_source_asset_filter",
    # Branch deployment utilities
    "get_branch_postfix",
    "get_postfixed_name",
    "get_postfixed_asset_key",
    "get_prefixed_name",
    "postfix_asset",
    "postfix_assets",
    "postfix_job",
    "postfix_jobs",
    "postfix_schedule",
    "postfix_schedules",
    "prefix_asset",
    "prefix_assets",
    "prefix_job",
    "prefix_jobs",
    "prefix_schedule",
    "prefix_schedules",
    "is_branch_deployment",
    "is_feature_branch_deployment",
    "log_branch_info",
]
