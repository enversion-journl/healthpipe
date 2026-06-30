# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Helpers for building FHIR Coding lists."""

from collections.abc import Iterable, Mapping
from typing import Any

from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from fhir.resources.R4B.coding import Coding
from utils.fhir.identifier_systems import SYSTEM_SKS


def _normalize_code_for_system(system: str, code: Any) -> str | None:
    if code is None:
        return None
    code_value = str(code).strip()
    if not code_value:
        return None
    if system == SYSTEM_SKS:
        return normalize_sks_coding_code(code_value)
    return code_value


def build_unique_ordered_codings(
    *,
    system: str,
    primary_code: Any | None = None,
    primary_display: Any | None = None,
    supplementary_rows: Iterable[Mapping[str, Any]] = (),
    supplement_code_key: str,
    supplement_display_key: str,
) -> list[Coding]:
    """Build codings in source order while dropping duplicate ``(system, code)`` pairs.

    The primary code is considered first, so it wins when a supplementary row
    repeats the same code with a different display.
    """
    codings: list[Coding] = []
    seen: set[tuple[str, str]] = set()

    def append(code: Any, display: Any) -> None:
        if not code:
            return
        code_value = _normalize_code_for_system(system, code)
        if not code_value:
            return
        key = (system, code_value)
        if key in seen:
            return
        seen.add(key)
        codings.append(
            Coding(
                system=system,
                code=code_value,
                display=str(display) if display else None,
            )
        )

    append(primary_code, primary_display)

    for row in supplementary_rows:
        append(row.get(supplement_code_key), row.get(supplement_display_key))

    return codings
