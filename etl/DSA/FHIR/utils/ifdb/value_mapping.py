# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SFI datatype -> FHIR value[x] mapping for IFDB aktivitetsresultat.

Maps a raw element value (string from IFDB XML) together with its SFI datatype
to a typed FHIR ``value[x]`` field. Used by:

  - Observation builder: produces ``valueInteger`` / ``valueQuantity``
    / ``valueString`` / ``valueBoolean`` / ``valueDateTime``
    / ``valueCodeableConcept`` on each per-SFI-element Observation.
  - QuestionnaireResponse builder and SQL-on-FHIR projection consume the same
    SFI datatype contract so the SQL value
    columns line up across resource types.

Datatype mapping:

    integer       -> valueInteger        / SQL value_int
    decimal       -> valueQuantity (unitless) / SQL value_dec
    quantity      -> valueQuantity       / SQL value_quantity + value_unit
    string/text   -> valueString         / SQL value_string
    boolean       -> valueBoolean        / SQL value_bool
    datetime      -> valueDateTime       / SQL value_datetime
    coded choice  -> valueCodeableConcept (Obs) / SQL value_coding_*

When the raw value cannot be coerced to the declared SFI datatype the mapping
raises ``SfiValueTypeMismatch``. Callers should catch this per element and
skip emission for that single element only — not the whole batch.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

_TRUE_STRINGS = {"1", "true", "t", "yes", "y", "ja"}
_FALSE_STRINGS = {"0", "false", "f", "no", "n", "nej"}


class SfiValueTypeMismatch(ValueError):
    """Raised when an SFI element's raw value does not match its declared datatype.

    Captures the SFI code, datatype and raw value so the caller can log a
    row-level diagnostic without losing the whole batch.
    """

    def __init__(
        self,
        sfi_code: str,
        datatype: str,
        raw_value: Any,
        reason: str,
    ) -> None:
        self.sfi_code = sfi_code
        self.datatype = datatype
        self.raw_value = raw_value
        self.reason = reason
        super().__init__(
            f"SFI value type mismatch: code={sfi_code!r} datatype={datatype!r} "
            f"raw={raw_value!r}: {reason}"
        )


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _coerce_integer(sfi_code: str, raw: str) -> int:
    try:
        return int(raw)
    except ValueError:
        try:
            f = float(raw.replace(",", "."))
        except ValueError as exc:
            raise SfiValueTypeMismatch(sfi_code, "integer", raw, "not an integer") from exc
        if not f.is_integer():
            raise SfiValueTypeMismatch(
                sfi_code, "integer", raw, "decimal value cannot be coerced to integer"
            ) from None
        return int(f)


def _coerce_decimal(sfi_code: str, raw: str) -> float:
    try:
        return float(raw.replace(",", "."))
    except ValueError as exc:
        raise SfiValueTypeMismatch(sfi_code, "decimal", raw, "not a decimal number") from exc


def _coerce_boolean(sfi_code: str, raw: str) -> bool:
    low = raw.lower()
    if low in _TRUE_STRINGS:
        return True
    if low in _FALSE_STRINGS:
        return False
    raise SfiValueTypeMismatch(sfi_code, "boolean", raw, "not a recognised boolean literal")


def _coerce_datetime(sfi_code: str, raw: str) -> str:
    candidates = [raw]
    if " " in raw and "T" not in raw:
        candidates.append(raw.replace(" ", "T", 1))
    for cand in candidates:
        try:
            datetime.fromisoformat(cand)
            return cand
        except ValueError:
            continue
    try:
        date.fromisoformat(raw)
        return raw
    except ValueError as exc:
        raise SfiValueTypeMismatch(
            sfi_code, "datetime", raw, "not an ISO date or datetime"
        ) from exc


def map_sfi_value_to_fhir(
    sfi_code: str,
    datatype: str,
    raw_value: Any,
    unit: str | None = None,
) -> dict[str, Any] | None:
    """Map an SFI datapoint's raw value to a FHIR ``value[x]`` field dict.

    Returns a single-key dict like ``{"valueInteger": 42}`` ready to merge
    into an Observation or QuestionnaireResponse.answer element. Returns
    ``None`` when the raw value is empty (caller should drop the element).

    Raises ``SfiValueTypeMismatch`` when the value cannot be coerced to the
    declared datatype. Callers should catch and skip the element.
    """
    if not sfi_code:
        raise ValueError("sfi_code is required for SFI value mapping")

    raw = _clean(raw_value)
    if raw is None:
        return None

    dt = (datatype or "").strip().lower()

    if dt == "integer":
        return {"valueInteger": _coerce_integer(sfi_code, raw)}

    if dt == "decimal":
        dec = _coerce_decimal(sfi_code, raw)
        # FHIR R4 Observation has no valueDecimal (unlike QR.answer); use valueQuantity.
        qty: dict[str, Any] = {"value": dec}
        if unit:
            qty["unit"] = unit
        return {"valueQuantity": qty}

    if dt == "quantity":
        dec = _coerce_decimal(sfi_code, raw)
        q: dict[str, Any] = {"value": dec}
        if unit:
            q["unit"] = unit
        return {"valueQuantity": q}

    if dt == "boolean":
        return {"valueBoolean": _coerce_boolean(sfi_code, raw)}

    if dt == "datetime":
        return {"valueDateTime": _coerce_datetime(sfi_code, raw)}

    if dt in ("coding", "code", "coded"):
        return {
            "valueCodeableConcept": {
                "text": raw,
            }
        }

    if dt in ("string", "text", ""):
        return {"valueString": raw}

    return {"valueString": raw}


def project_sfi_value_to_sql(
    sfi_code: str,
    datatype: str,
    raw_value: Any,
    unit: str | None = None,
) -> dict[str, Any]:
    """Project an SFI datapoint to the SQL value-column contract.

    Returns a dict with the SQL projection keys needed to persist the same
    datapoint to ``fhir.Observation`` and
    ``fhir.QuestionnaireResponse_item`` with consistent columns. The shape
    is intentionally permissive — only the relevant ``value_*`` columns are
    populated for the matched datatype.

    Raises ``SfiValueTypeMismatch`` for type mismatches.
    """
    out: dict[str, Any] = {
        "value_type": None,
        "value_int": None,
        "value_dec": None,
        "value_string": None,
        "value_datetime": None,
        "value_bool": None,
        "value_quantity": None,
        "value_unit": unit or None,
        "value_coding_system": None,
        "value_coding_code": None,
        "value_coding_display": None,
    }

    raw = _clean(raw_value)
    if raw is None:
        return out

    dt = (datatype or "").strip().lower()

    if dt == "integer":
        out["value_type"] = "integer"
        out["value_int"] = _coerce_integer(sfi_code, raw)
    elif dt == "decimal":
        out["value_type"] = "decimal"
        out["value_dec"] = _coerce_decimal(sfi_code, raw)
    elif dt == "quantity":
        out["value_type"] = "quantity"
        out["value_quantity"] = _coerce_decimal(sfi_code, raw)
    elif dt == "boolean":
        out["value_type"] = "bool"
        out["value_bool"] = _coerce_boolean(sfi_code, raw)
    elif dt == "datetime":
        out["value_type"] = "datetime"
        out["value_datetime"] = _coerce_datetime(sfi_code, raw)
    elif dt in ("coding", "code", "coded"):
        out["value_type"] = "coding"
        out["value_coding_code"] = raw
    else:
        out["value_type"] = "string"
        out["value_string"] = raw

    return out
