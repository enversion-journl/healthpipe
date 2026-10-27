# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SOR code and date handling shared across the layers that read SOR values.

In its own module because several layers need these and none should import the
other: ``DSI/csv_files/sor_register.py`` cleans SorId on the way in from
Sundhedsdatastyrelsen's CSV, ``DSI/os2mo`` cleans the user_key values OS2mo
hands back, and both the FHIR Organization builder and the OS2mo export parse
the same SOR date shapes.
"""

from datetime import datetime
from typing import Any

__all__ = ["clean_sor_id", "parse_sor_date"]


def clean_sor_id(value: str) -> str:
    """Remove Excel formula wrapping: ="21000016006" -> 21000016006.

    The wrapper is not noise — it is what keeps a SOR id intact. A 16-digit code
    exceeds the ~15 significant digits an IEEE-754 double holds, so a spreadsheet
    that treats the value as a number silently zeroes the last digit.

    So this strips the wrapper as TEXT and never converts. Leading zeros, letters
    and length all survive; only the two wrapping characters are removed.
    """
    if not value:
        return ""
    value = value.strip()
    if value.startswith('="') and value.endswith('"'):
        value = value[2:-1]
    return value.strip()


def parse_sor_date(value: Any) -> datetime | None:
    """Parse a SOR date string (``YYYY-MM-DD[ HH:MM:SS]``) to a datetime.

    Anything else — empty, NULL, a stray placeholder — is None rather than an
    exception: a single malformed date must not fail a whole load.
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
