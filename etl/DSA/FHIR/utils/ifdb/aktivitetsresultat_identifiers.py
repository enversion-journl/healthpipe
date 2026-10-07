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


def _sfi_identifier_value(objekt_id_norm: str, element_nummer: int) -> str:
    """Build the per-element identifier value for an SFI Observation.

    Deterministic from (AKTIVITETSRESULTAT.OBJEKT_ID, parser-assigned
    element_nummer). Formatet ejes af FAMILY_SFI_OBS (child_value) — denne
    funktion er kun et bekvemt kaldested.
    """
    from DSA.FHIR.utils.ifdb.xml_child_reconcile import FAMILY_SFI_OBS

    return FAMILY_SFI_OBS.child_value(objekt_id_norm, element_nummer)
