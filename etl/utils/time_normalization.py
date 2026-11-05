# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Any


def normalize_epoch_ts(
    value: Any,
    clamp_start: datetime | None = None,
    clamp_end: datetime | None = None,
) -> datetime | None:
    """
    Normalize epoch seconds/milliseconds to an aware UTC datetime.

    - If value >= 1e12 -> treat as milliseconds.
    - If value >= 1e9  -> treat as seconds.
    - Otherwise return None.
    - Clamp to a sane window to avoid absurd dates.
    """
    if clamp_start is None:
        clamp_start = datetime(1990, 1, 1, tzinfo=UTC)
    if clamp_end is None:
        clamp_end = datetime.now(tz=UTC) + timedelta(days=1)

    if value is None:
        return None

    try:
        v = int(value)
    except (TypeError, ValueError):
        return None

    if v >= 1_000_000_000_000:
        dt = datetime.fromtimestamp(v / 1000, tz=UTC)
    elif v >= 1_000_000_000:
        dt = datetime.fromtimestamp(v, tz=UTC)
    else:
        return None

    if dt < clamp_start or dt > clamp_end:
        return None

    return dt


def datetime_to_fhir_instant(dt: datetime | None) -> str | None:
    """Return FHIR instant string with Z suffix from aware/naive datetime."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.isoformat().replace("+00:00", "Z")


def datetime_to_fhir_date(dt: datetime | None) -> str | None:
    """Return FHIR date (YYYY-MM-DD)."""
    if dt is None:
        return None
    return dt.date().isoformat()


def should_normalize_epoch(
    env_var: str = "NORMALIZE_EPOCH_TS", run_tags: Mapping[str, str] | None = None
) -> bool:
    """
    Feature flag: enable epoch normalization when data stores timestamps as epoch ints.

    - Controlled via env var (default NORMALIZE_EPOCH_TS=true)
    - Or Dagster run tag "normalize_epoch" == "true"
    - Default False (production-safe)
    """
    import os

    env_flag = os.getenv(env_var, "").lower() == "true"
    tag_flag = False
    try:
        if run_tags:
            tag_flag = run_tags.get("normalize_epoch", "").lower() == "true"
    except Exception:
        tag_flag = False
    return env_flag or tag_flag
