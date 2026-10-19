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
from fhir.resources.R4B.extension import Extension
from utils.fhir.fhir_extensions import EXTENSION_CODING_SEQUENCE
from utils.fhir.identifier_systems import SYSTEM_SKS, SYSTEM_SKS_D

_SKS_NORMALIZE_SYSTEMS = {SYSTEM_SKS, SYSTEM_SKS_D}


def _normalize_code_for_system(system: str, code: Any) -> str | None:
    if code is None:
        return None
    code_value = str(code).strip()
    if not code_value:
        return None
    if system in _SKS_NORMALIZE_SYSTEMS:
        return normalize_sks_coding_code(code_value)
    return code_value


def build_unique_ordered_codings(
    *,
    system: str,
    primary_code: Any | None = None,
    primary_display: Any | None = None,
    primary_system: str | None = None,
    supplementary_rows: Iterable[Mapping[str, Any]] = (),
    supplement_code_key: str,
    supplement_display_key: str,
    supplement_sequence_key: str | None = None,
) -> list[Coding]:
    """Build codings in source order while dropping duplicate ``(system, code)`` pairs.

    The primary code is considered first, so it wins when a supplementary row
    repeats the same code with a different display. When ``primary_system`` differs
    from ``system``, a supplementary row whose normalized code matches the primary
    is also dropped (primary wins across the SKS/SKS-D split).

    ``supplement_sequence_key`` names the source column with the code's ordinal (fx
    ADM_PROCEDURE_SUPPL_CODES.IDX); it is carried on the Coding as a codingSequence
    extension so the flat child table can project it as ``idx``.
    """
    codings: list[Coding] = []
    seen: set[tuple[str, str]] = set()
    primary_effective_system = primary_system or system
    primary_normalized: str | None = None

    def append(
        code: Any,
        display: Any,
        *,
        user_selected: bool = False,
        coding_system: str | None = None,
        sequence: Any | None = None,
    ) -> None:
        nonlocal primary_normalized
        if not code:
            return
        cs = coding_system or system
        code_value = _normalize_code_for_system(system, code)
        if not code_value:
            return
        key = (cs, code_value)
        if key in seen:
            return
        seen.add(key)
        if user_selected:
            primary_normalized = code_value
        extension = None
        if sequence is not None and str(sequence).strip():
            # Kildens ordenstal er NVARCHAR i arkivet. En enkelt ikke-numerisk
            # vaerdi maa ikke vaelte hele transformen: koden er det vaesentlige,
            # raekkefoelgen er en tilgift, saa den springes over i stedet.
            try:
                ordenstal = int(float(sequence))
            except (TypeError, ValueError):
                ordenstal = None
            if ordenstal is not None:
                extension = [
                    Extension.construct(url=EXTENSION_CODING_SEQUENCE, valueInteger=ordenstal)
                ]
        codings.append(
            Coding(
                system=cs,
                code=code_value,
                display=str(display) if display else None,
                userSelected=True if user_selected else None,
                extension=extension,
            )
        )

    append(
        primary_code,
        primary_display,
        user_selected=True,
        coding_system=primary_effective_system,
    )

    for row in supplementary_rows:
        sup_code = row.get(supplement_code_key)
        if sup_code:
            sup_normalized = _normalize_code_for_system(system, sup_code)
            if primary_normalized and sup_normalized == primary_normalized:
                continue
        append(
            sup_code,
            row.get(supplement_display_key),
            sequence=row.get(supplement_sequence_key) if supplement_sequence_key else None,
        )

    return codings
