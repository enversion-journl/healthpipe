# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Environment detection — one source of truth for "are we running against prod?".

The heuristic existed in two copies (dev assets and the verify script). This
combined function is the stricter of the two: both environment name and the
target database hosts.
"""

from __future__ import annotations

import os

_PROD_MARKER = "prod"

# Host variables pointing at a database we could end up writing to.
_HOST_ENV_VARS = (
    "MSSQL_TARGET_STAGING_HOST",
    "MSSQL_TARGET_DSO_HOST",
)


def is_prod_environment() -> bool:
    """True if the environment name or a target database host looks like prod.

    A prod host without 'prod' in its name still slips through; use an explicit
    allowlist if that risk must be closed.
    """
    if _PROD_MARKER in (os.getenv("DAGSTER_ENVIRONMENT") or "").strip().lower():
        return True
    return any(_PROD_MARKER in (os.getenv(var) or "").strip().lower() for var in _HOST_ENV_VARS)


def prod_environment_reason() -> str:
    """Which variable triggered prod detection — for error messages."""
    env = (os.getenv("DAGSTER_ENVIRONMENT") or "").strip()
    if _PROD_MARKER in env.lower():
        return f"DAGSTER_ENVIRONMENT={env!r}"
    for var in _HOST_ENV_VARS:
        val = (os.getenv(var) or "").strip()
        if _PROD_MARKER in val.lower():
            return f"{var}={val!r}"
    return "not prod"
