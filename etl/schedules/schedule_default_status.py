# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Environment-aware default schedule status for Dagster schedules.
"""

import os

import dagster as dg

AUTO_ENABLE_LOCATION_NAMES = {"development", "production", "prod"}
AUTO_ENABLE_BRANCH_PREFIXES = ("release-", "main-")


def _normalize_env_value(value: str | None) -> str:
    """Normalize env var values for consistent matching."""
    return (value or "").strip().lower()


def should_auto_enable_schedules() -> bool:
    """
    Return True when schedules should auto-start for this deployment context.

    We auto-enable for:
    - Long-lived code locations: development, production/prod
    - QA single-release and prod release-style deployments via branch postfix
      (release-* and main-*)
    """
    location_name = _normalize_env_value(os.getenv("DAGSTER_CODE_LOCATION_NAME"))
    branch_postfix = _normalize_env_value(os.getenv("DAGSTER_BRANCH_POSTFIX"))

    if location_name in AUTO_ENABLE_LOCATION_NAMES:
        return True

    if any(branch_postfix.startswith(prefix) for prefix in AUTO_ENABLE_BRANCH_PREFIXES):
        return True

    return False


def get_schedule_default_status() -> dg.DefaultScheduleStatus:
    """Get the default schedule status for the current deployment context."""
    if should_auto_enable_schedules():
        return dg.DefaultScheduleStatus.RUNNING
    return dg.DefaultScheduleStatus.STOPPED


def get_sensor_default_status() -> dg.DefaultSensorStatus:
    """Sensor analogue of ``get_schedule_default_status`` (same env gating).

    Lets sensors auto-start in long-lived / release deployments (development,
    production, release-*/main-*) and stay stopped elsewhere, so DQA automation is
    actually on after merge in dev/prod without a manual UI toggle.
    """
    if should_auto_enable_schedules():
        return dg.DefaultSensorStatus.RUNNING
    return dg.DefaultSensorStatus.STOPPED


__all__ = [
    "get_schedule_default_status",
    "get_sensor_default_status",
    "should_auto_enable_schedules",
]
