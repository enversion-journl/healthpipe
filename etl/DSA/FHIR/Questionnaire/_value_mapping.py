# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SFI datatype -> FHIR Questionnaire item.type / QR answer value[x] mapping.

Shared between Questionnaire and QuestionnaireResponse builders. Kept local
because QR answer coercion differs slightly from Observation value[x] mapping.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

SFI_DATATYPE_TO_QUESTIONNAIRE_TYPE: dict[str, str] = {
    "integer": "integer",
    "decimal": "decimal",
    "quantity": "quantity",
    "string": "string",
    "boolean": "boolean",
    "datetime": "dateTime",
    "date": "date",
    "coding": "choice",
}


def map_sfi_datatype_to_question_type(sfi_datatype: str | None) -> str:
    """Return the FHIR ``Questionnaire.item.type`` for an SFI datatype.

    Falls back to ``string`` if the datatype is unknown or empty so the
    Questionnaire still validates. Callers can flag unknown datatypes for
    review while preserving the source value.
    """
    if not sfi_datatype:
        return "string"
    return SFI_DATATYPE_TO_QUESTIONNAIRE_TYPE.get(sfi_datatype.lower(), "string")


def _parse_decimal(raw: str) -> float | None:
    s = raw.strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except ValueError:
        return None


def _parse_boolean(raw: str) -> bool | None:
    s = raw.strip().lower()
    if s in ("true", "ja", "yes", "1"):
        return True
    if s in ("false", "nej", "no", "0"):
        return False
    return None


def _parse_datetime(raw: str) -> str | None:
    s = raw.strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        return dt.isoformat()
    except ValueError:
        return None


def _parse_date(raw: str) -> str | None:
    s = raw.strip()
    if not s:
        return None
    try:
        return date.fromisoformat(s).isoformat()
    except ValueError:
        try:
            dt = datetime.fromisoformat(s)
            return dt.date().isoformat()
        except ValueError:
            return None


def coerce_answer_value(
    sfi_datatype: str | None,
    raw_value: str | None,
    element_kode: str | None = None,
    element_navn: str | None = None,
    sfi_system: str | None = None,
) -> tuple[str, Any]:
    """Coerce an XML ``Vaerdi`` string into a typed FHIR ``QR.item.answer`` field.

    Returns:
        ``(field_name, value)`` where ``field_name`` is the FHIR
        ``QuestionnaireResponseItemAnswer`` field (e.g. ``valueInteger``,
        ``valueString``, ``valueCoding``) and ``value`` is the typed value.

    If the value cannot be coerced cleanly to the declared SFI datatype, the
    raw string is returned as ``valueString`` so the source is never lost.
    Callers should compare the returned field to the expected one to detect
    datatype-mismatch events for review.
    """
    if raw_value is None:
        return "valueString", None

    dtype = (sfi_datatype or "string").lower()

    if dtype == "integer":
        try:
            return "valueInteger", int(raw_value.strip())
        except (ValueError, AttributeError):
            return "valueString", raw_value

    if dtype == "decimal":
        parsed = _parse_decimal(raw_value)
        if parsed is not None:
            return "valueDecimal", parsed
        return "valueString", raw_value

    if dtype == "boolean":
        parsed_bool = _parse_boolean(raw_value)
        if parsed_bool is not None:
            return "valueBoolean", parsed_bool
        return "valueString", raw_value

    if dtype == "datetime":
        parsed_dt = _parse_datetime(raw_value)
        if parsed_dt is not None:
            return "valueDateTime", parsed_dt
        return "valueString", raw_value

    if dtype == "date":
        parsed_d = _parse_date(raw_value)
        if parsed_d is not None:
            return "valueDate", parsed_d
        return "valueString", raw_value

    if dtype == "coding":
        # IFDB klassificering: Vaerdi like "4: Spontant" with Kode set separately.
        display = raw_value.strip() or None
        # Strip leading numeric/code prefix when of form "N: text"
        if display and ":" in display:
            left, _, right = display.partition(":")
            right_s = right.strip()
            if right_s:
                display = right_s
        coding: dict[str, Any] = {}
        if element_kode:
            coding["code"] = element_kode
        elif element_navn:
            coding["code"] = element_navn
        if sfi_system:
            coding["system"] = sfi_system
        if display:
            coding["display"] = display
        if not coding:
            return "valueString", raw_value
        return "valueCoding", coding

    return "valueString", raw_value
