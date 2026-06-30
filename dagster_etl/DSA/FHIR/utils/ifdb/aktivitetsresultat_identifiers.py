# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Identifier helpers for AKTIVITETSRESULTAT."""

from typing import Any


def _build_aktivitets_id(value: Any) -> int:
    """Convert OBJEKT_ID-like values to int for parser identity field."""
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


_SFI_ID_SEP = "#sfi#"


def _sfi_identifier_value(objekt_id_norm: str, element_nummer: int) -> str:
    """Build the per-element identifier value for an SFI Observation.

    Format: ``<OBJEKT_ID>#sfi#<element_nummer>``. Deterministic from
    (AKTIVITETSRESULTAT.OBJEKT_ID, parser-assigned element_nummer).
    """
    return f"{objekt_id_norm}{_SFI_ID_SEP}{element_nummer}"
