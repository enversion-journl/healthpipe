# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
EVENT_CODE parsing for IFDB EVENTMARKER rows.

The Oracle extraction helpers that used to live here are gone: EVENTMARKER is
now archived by the DFM pipeline (``ext.ifdb_eventmarker`` -> ``arc.ifdb_eventmarker``)
and the CarePlan transform reads the archive, so nothing queries EVENTMARKER
from a transform any more.
"""

# =============================================================================
# Constants
# =============================================================================

# EVENT_CODE parsing configuration
EVENT_CODE_PREFIX = "SKS.a.adm"
EVENT_CODE_PREFIX_LENGTH = len(EVENT_CODE_PREFIX)


# =============================================================================
# Common Parsing
# =============================================================================


def parse_event_code(event_code: str | None) -> dict[str, str | None]:
    """
    Parse EVENT_CODE to extract marker type and code components.

    EVENT_CODE format: SKS.a.adm{SKS_CODE}
    Example: SKS.a.admAFV01A1 → marker_type=AFV, marker_code=AFV01A1

    Args:
        event_code: Raw EVENT_CODE from EVENTMARKER table

    Returns:
        Dict with keys: marker_type, marker_code, marker_code_full
    """
    if not event_code or not event_code.startswith(EVENT_CODE_PREFIX):
        return {
            "marker_type": None,
            "marker_code": None,
            "marker_code_full": None,
        }

    code_without_prefix = event_code[EVENT_CODE_PREFIX_LENGTH:]
    marker_type = code_without_prefix[:3] if len(code_without_prefix) >= 3 else None

    return {
        "marker_type": marker_type,
        "marker_code": code_without_prefix,
        "marker_code_full": code_without_prefix,
    }
