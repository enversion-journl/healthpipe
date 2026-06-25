# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SQL identifier validation and quoting utilities.

Used by DFM query builders and extract factories to prevent SQL injection
through operator-supplied config values (delta_column, id_filter_column, table refs).
"""

from __future__ import annotations

import re

_VALID_SQL_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_VALID_CHECKPOINT_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}([T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?)?$")
_VALID_CHECKPOINT_INT = re.compile(r"^-?\d+$")


def safe_bracket_quote(identifier: str) -> str:
    """Bracket-quote a SQL identifier after validating it is safe.

    Strips any existing brackets, then verifies the identifier contains only
    ``[A-Za-z0-9_]`` characters (starting with a letter or underscore).

    Raises:
        ValueError: If the identifier contains disallowed characters.
    """
    cleaned = identifier.replace("[", "").replace("]", "")
    if not _VALID_SQL_IDENTIFIER.match(cleaned):
        raise ValueError(
            f"Invalid SQL identifier for bracket-quoting: {identifier!r}. "
            f"Only alphanumeric characters and underscores are allowed."
        )
    return f"[{cleaned}]"


def safe_table_ref(qualified_name: str) -> str:
    """Validate and bracket-quote a ``[schema].[table]`` reference.

    Accepts ``schema.table``, ``[schema].[table]``, or a bare ``table``
    name.  Each component must be a valid SQL identifier.

    Raises:
        ValueError: If the reference has more than 2 parts or contains
            disallowed characters.
    """
    parts = qualified_name.replace("[", "").replace("]", "").split(".")
    if len(parts) not in (1, 2):
        raise ValueError(
            f"Invalid table reference: {qualified_name!r}. " f"Expected 'schema.table' or 'table'."
        )
    for part in parts:
        if not _VALID_SQL_IDENTIFIER.match(part):
            raise ValueError(
                f"Invalid identifier in table reference: {part!r} "
                f"(from {qualified_name!r}). "
                f"Only alphanumeric characters and underscores are allowed."
            )
    return ".".join(f"[{p}]" for p in parts)


def validate_checkpoint_value(value: str) -> str:
    """Validate that a checkpoint value is a safe datetime or integer string.

    Prevents SQL injection through corrupted/tampered checkpoint values
    stored in ``meta.dfm_state``.

    Accepts:
        - ISO datetime formats: ``2025-01-01``, ``2025-01-01 12:00:00``,
          ``2025-01-01T12:00:00.123``
        - Integer strings: ``0``, ``12345``, ``-1``

    Raises:
        ValueError: If the value does not match any allowed format.
    """
    if _VALID_CHECKPOINT_DATETIME.match(value) or _VALID_CHECKPOINT_INT.match(value):
        return value
    raise ValueError(
        f"Invalid checkpoint value: {value!r}. "
        f"Expected an ISO datetime (YYYY-MM-DD ...) or integer string."
    )
