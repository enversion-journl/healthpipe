# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL Server GREATEST() capability probe for legacy dev environments."""

from __future__ import annotations

from typing import Any


def detect_mssql_greatest_support(conn: Any) -> bool:
    """Return True when the connected engine exposes native GREATEST()."""
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT GREATEST(1, 2, 3) AS v")
        row = cursor.fetchone()
        return bool(row and row[0] == 3)
    except Exception:
        return False
    finally:
        cursor.close()
