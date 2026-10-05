# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SFI contract helpers for IFDB aktivitetsresultat flows.

This module is intentionally free of Dagster decorators so CodeSystem, FHIR
builders and tests can all import the same SFI rules without triggering asset
registration side effects.
"""

from __future__ import annotations

from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow

SFI_SYSTEM = "urn:dk:ifdb:sfi"
SFI_FALLBACK_PREFIX = "_fb_"
SFI_MODEL_SEPARATOR = "."


def _safe_model_token(value: str) -> str:
    """Normalize a model name into a safe SFI-code prefix token.

    Whitespace and the model-separator character are replaced with underscore
    so the resulting code roundtrips through a single rsplit('.', 1) at query
    time.
    """
    return str(value).strip().replace(" ", "_").replace(SFI_MODEL_SEPARATOR, "_")


def generate_sfi_code(
    element_id: str | None,
    element_name: str | None,
    model_name: str | None,
) -> tuple[str, str]:
    """Generate the qualified SFI code ``<model_name>.<element_key>`` + status.

    The SFI code is always qualified by ``MODELLERINGSNAVN`` (passed as
    ``model_name``) so the same XML ElementId under two different source
    models becomes two separate SFI concepts in the catalog. This is the
    explicit choice for option 2 in the SFI-top-level discussion: the form
    is the semantic context, not the version of the form.

    Status semantics:
      - ``active``  — both ``element_id`` and ``model_name`` are present;
                      the qualified code is fully governed.
      - ``review``  — either falls back to ``element_name`` or is missing
                      ``model_name``; surfaces in governance for follow-up.
    """
    if element_id and element_id.strip():
        element_key = element_id.strip()
        base_status = "active"
    elif element_name and element_name.strip():
        safe_name = element_name.strip().replace(" ", "_")
        element_key = f"{SFI_FALLBACK_PREFIX}{safe_name}"
        base_status = "review"
    else:
        return f"{SFI_FALLBACK_PREFIX}unknown", "review"

    if model_name and str(model_name).strip():
        prefix = _safe_model_token(model_name)
        return f"{prefix}{SFI_MODEL_SEPARATOR}{element_key}", base_status

    # No model context — keep the bare element key but flag for review so it
    # shows up in governance rather than silently joining concepts from
    # different forms.
    return element_key, "review"


def infer_sfi_datatype(element_type: str | None, element_value: str | None) -> str:
    """Infer the shared SFI datatype from IFDB XML type/value metadata."""
    if element_type:
        t = element_type.strip().lower()
        type_map = {
            "heltal": "integer",
            "long": "integer",
            "int": "integer",
            "decimal": "decimal",
            "float": "decimal",
            "double": "decimal",
            "dato": "datetime",
            "date": "datetime",
            "tidspunkt": "datetime",
            "datetime": "datetime",
            "fritekst": "string",
            "tekst": "string",
            "tekstfelt": "string",
            "tegn": "string",
            "string": "string",
            "boolean": "boolean",
            "bool": "boolean",
            "ja/nej": "boolean",
            "kode": "coding",
            "kodeliste": "coding",
            "kodevaerdi": "coding",
            "klassificering": "coding",
        }
        if t in type_map:
            return type_map[t]

    if element_value is not None:
        v = element_value.strip()
        if not v:
            return "string"
        try:
            int(v)
            return "integer"
        except ValueError:
            pass
        try:
            float(v.replace(",", "."))
            return "decimal"
        except ValueError:
            pass

    return "string"


def questionnaire_item_type_for_sfi_datatype(datatype: str) -> str:
    """Map shared SFI datatypes to FHIR Questionnaire.item.type."""
    return {
        "integer": "integer",
        "decimal": "decimal",
        "quantity": "quantity",
        "boolean": "boolean",
        "datetime": "dateTime",
        "date": "date",
        "coding": "choice",
        "code": "choice",
        "string": "string",
    }.get(datatype, "string")


def guess_unit_from_display(display: str, element_name: str | None) -> str:
    """Conservative unit inference used consistently across SFI builders."""
    combined = f"{display} {element_name or ''}".lower()
    unit_hints = {
        "blodtryk": "mmHg",
        "bt_sys": "mmHg",
        "bt_dia": "mmHg",
        "systolisk": "mmHg",
        "diastolisk": "mmHg",
        "puls": "bpm",
        "hjertefrekvens": "bpm",
        "resp": "/min",
        "respirationsfrekvens": "/min",
        "saturation": "%",
        "spo2": "%",
        "temperatur": "Cel",
        "temp": "Cel",
        "vaegt": "kg",
        "vægt": "kg",
        "hoejde": "cm",
        "højde": "cm",
    }
    for hint, unit in unit_hints.items():
        if hint in combined:
            return unit
    return ""


def build_sfi_path(row: AktivitetsresultatRow) -> str:
    """Build a human-readable path through the source XML structure."""
    parts: list[str] = []
    if row.hierarki:
        parts.append(row.hierarki)
    if row.klynge_navne:
        parts.append(f"kly:{row.klynge_navne}")
    if row.element_navn:
        parts.append(row.element_navn)
    return "/".join(parts) if parts else row.element_navn or "root"
