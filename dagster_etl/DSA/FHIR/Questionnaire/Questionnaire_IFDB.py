# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Questionnaire builder for IFDB AKTIVITETSRESULTAT data.

For each unique IFDB model (``MODELLERINGSNAVN`` + ``MODELLERINGSVERSION``) we
emit one versioned Questionnaire whose items carry the SFI code on
``Questionnaire.item.code[]``. QR.item entries refer back via ``linkId``
(see ``QuestionnaireResponse_IFDB.py``).

The SFI extraction and ``urn:dk:ifdb:sfi`` system constant come from the shared
SFI helper layer. The XML parser comes from
``DSA/FHIR/utils/ifdb/xml_aktivitetsresultat.py``. Routing decisions
(Observation vs QR) come from ``DSA/FHIR/utils/ifdb/routing.py``; this module
is invoked only for QR-routed aktivitetstyper.

Design choices:
  - SFI lives FHIR-native on ``Questionnaire.item.code``.
  - ``Questionnaire.item.linkId`` is the SFI code itself, so QR.item.linkId
    matches without an extra mapping.
  - ``Questionnaire.item.id`` is the stable occurrence key shared with
    ``QuestionnaireResponse.item.id``. This keeps repeated/nested source rows
    with the same SFI ``linkId`` distinct without deduplicating data away.
  - Items are deliberately flat: klynge/hierarchy/repetition context lives in
    the SFI CodeSystem properties and consumer projections, not in nested
    Questionnaire.item groups.
  - Custom item extensions are avoided by default. Stable semantics and source
    metadata live in the SFI CodeSystem.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Iterable
from typing import Any

from DSA.FHIR.Questionnaire._item_key import stable_sfi_item_id
from DSA.FHIR.Questionnaire._sfi import (
    SFI_SYSTEM,
    generate_sfi_code,
)
from DSA.FHIR.Questionnaire._value_mapping import (
    map_sfi_datatype_to_question_type,
)
from DSA.FHIR.utils.ifdb.sfi_contract import infer_sfi_datatype
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetsresultatRow,
    parse_aktivitetsresultat_xml,
)

QUESTIONNAIRE_CANONICAL_PREFIX = "urn:dk:ifdb:questionnaire"
QUESTIONNAIRE_IDENTIFIER_SYSTEM = "urn:source:ifdb:questionnaire_model"


_SAFE_TOKEN_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_token(value: object) -> str:
    """Coerce a value into a token safe for FHIR ``id`` and canonical URLs.

    Oracle returns versioned identifiers as int when the column stores numerics,
    so accept any object and coerce to str before sanitising.
    """
    if value is None or value == "":
        return "unspecified"
    s = _SAFE_TOKEN_RE.sub("-", str(value).strip())
    s = s.strip("-")
    return s or "unspecified"


def build_questionnaire_canonical(model_name: str | None) -> str:
    """Return the canonical URL for a Questionnaire keyed by model name.

    Example: ``urn:dk:ifdb:questionnaire:RM_Generelle``.
    """
    return f"{QUESTIONNAIRE_CANONICAL_PREFIX}:{_safe_token(model_name)}"


def build_questionnaire_identifier_value(model_name: str | None, model_version: str | None) -> str:
    """Return the stable source identifier value used to dedupe across runs."""
    return f"{_safe_token(model_name)}|{_safe_token(model_version)}"


def build_questionnaire_version_pinned_reference(
    model_name: str | None, model_version: str | None
) -> str:
    """Return ``<canonical>|<version>`` for ``QuestionnaireResponse.questionnaire``.

    Raises:
        ValueError: When ``model_version`` is missing.  Version-pinning is
            required by the contract and must fail loudly so that callers do not
            silently produce ambiguous QRs.
    """
    if not model_version or not str(model_version).strip():
        raise ValueError(
            "Cannot build version-pinned questionnaire reference: MODELLERINGSVERSION is required."
        )
    canonical = build_questionnaire_canonical(model_name)
    return f"{canonical}|{str(model_version).strip()}"


def _is_value_carrying_row(row: AktivitetsresultatRow) -> bool:
    """Return True if a row represents an answerable Q.item (Element/Selektion)."""
    if row.xml_status == -1:
        return False
    if row.raekke_type == "Element":
        return bool(row.element_navn)
    if row.raekke_type == "Selektion":
        return bool(row.klynge_navn)
    return False


def _row_sfi_code(row: AktivitetsresultatRow, model_name: str | None) -> tuple[str, str]:
    """Compute the SFI ``(code, status)`` for one parsed XML row."""
    if row.raekke_type == "Element":
        return generate_sfi_code(row.element_element_id, row.element_navn, model_name)
    # Selektion uses klynge name as the code basis (no ElementId on Klynge)
    return generate_sfi_code(None, row.klynge_navn, model_name)


def _infer_datatype_from_row(row: AktivitetsresultatRow) -> str:
    """Best-effort SFI datatype for one row.

    Mirrors the shared SFI datatype contract so Q.item.type stays consistent
    with the SFI CodeSystem property ``datatype``.
    """
    return infer_sfi_datatype(row.element_type, row.element_vaerdi)


def _make_questionnaire_item_dict(
    row: AktivitetsresultatRow,
    sfi_code: str,
    sfi_status: str,
) -> dict:
    """Build one flat answerable Q.item dict for an Element or Selektion row."""
    if row.raekke_type == "Selektion":
        datatype = "coding"
        display = row.klynge_ledetekst or row.klynge_navn or sfi_code
        raw_value = row.klynge_valg_ledetekst or ""
        element_kode = None
        primary_occurrence_id = row.klynge_id
        item_id_prefix = "g"
    else:
        datatype = _infer_datatype_from_row(row)
        display = row.element_ledetekst or row.element_navn or sfi_code
        raw_value = row.element_vaerdi or ""
        element_kode = row.element_kode
        primary_occurrence_id = row.element_nummer
        item_id_prefix = "e"
    q_type = map_sfi_datatype_to_question_type(datatype)
    item: dict[str, Any] = {
        "id": stable_sfi_item_id(
            item_id_prefix,
            primary_occurrence_id,
            row.aktivitets_id,
            row.raekke_type,
            sfi_code,
            raw_value,
            element_kode,
        ),
        "linkId": sfi_code,
        "type": q_type,
        "text": display,
        "code": [
            {
                "system": SFI_SYSTEM,
                "code": sfi_code,
                "display": display,
            }
        ],
    }
    # When the row sits inside a Repetition, expose that as a repeats hint.
    if row.repetition_niveau and row.repetition_niveau > 0:
        item["repeats"] = True
    if sfi_status == "review":
        # Surface fallback-generated SFI to humans without breaking schema.
        item.setdefault("_meta_review", True)
    return item


def build_questionnaire_items_dict(
    parsed_rows: Iterable[AktivitetsresultatRow],
    model_name: str | None,
) -> list[dict]:
    """Build a flat ``Questionnaire.item`` list as plain dicts.

    Each answerable source row becomes one item. The SFI code remains on
    ``linkId`` and ``code`` for concept lookup, while native ``item.id`` carries
    the occurrence key used to distinguish repeated rows with the same SFI code.
    """
    items: list[dict] = []

    for row in parsed_rows:
        if not _is_value_carrying_row(row):
            continue

        sfi_code, sfi_status = _row_sfi_code(row, model_name)
        item = _make_questionnaire_item_dict(row, sfi_code, sfi_status)

        # Strip private review flag from the persisted resource — the flag is
        # only useful inside the build pass.
        item.pop("_meta_review", None)
        items.append(item)

    return items


def build_questionnaire_dict(
    parsed_rows: list[AktivitetsresultatRow],
    *,
    model_name: str | None,
    model_version: str | None,
    resource_id: str | None = None,
    aktivitettype_kode: str | None = None,
    aktivitettype_text: str | None = None,
    overskrift: str | None = None,
) -> dict:
    """Build a FHIR Questionnaire resource dict for one IFDB model.

    The dict-first form mirrors the AKTIVITETSRESULTAT pipeline pattern
    (see ``DSA.FHIR.Aktivitetsresultat.orchestrator``)
    so the resource is cheap to post via ``post_resource_dicts_direct``.

    Raises:
        ValueError: When ``model_version`` is missing — version-pinning is
            required so QR references stay unambiguous downstream.
    """
    if not model_version or not str(model_version).strip():
        raise ValueError(
            "Cannot build Questionnaire: MODELLERINGSVERSION is required for version-pinning."
        )

    canonical = build_questionnaire_canonical(model_name)
    version = str(model_version).strip()
    title = overskrift or aktivitettype_text or model_name or canonical

    items = build_questionnaire_items_dict(parsed_rows, model_name)

    resource: dict[str, Any] = {
        "resourceType": "Questionnaire",
        "id": resource_id or str(uuid.uuid4()),
        "url": canonical,
        "version": version,
        "name": _safe_token(model_name),
        "title": title,
        "status": "active",
        "identifier": [
            {
                "system": QUESTIONNAIRE_IDENTIFIER_SYSTEM,
                "value": build_questionnaire_identifier_value(model_name, model_version),
            }
        ],
        "item": items,
    }

    if aktivitettype_kode:
        resource["code"] = [
            {
                "code": aktivitettype_kode,
                "display": aktivitettype_text or aktivitettype_kode,
            }
        ]

    return resource


def build_questionnaire_from_xml(
    xml_string: str | None,
    *,
    aktivitets_id: int,
    model_name: str | None,
    model_version: str | None,
    resource_id: str | None = None,
    aktivitettype_kode: str | None = None,
    aktivitettype_text: str | None = None,
    overskrift: str | None = None,
) -> dict:
    """Convenience wrapper: parse XML and build the Questionnaire in one call.

    Intended primarily for tests and one-off tooling. Production pipelines
    should parse XML once and feed the parsed rows into
    ``build_questionnaire_dict`` so the same parse is shared with the QR
    builder and shared SFI extraction.
    """
    parsed_rows, _ = parse_aktivitetsresultat_xml(
        xml_string=xml_string, aktivitets_id=aktivitets_id
    )
    return build_questionnaire_dict(
        parsed_rows,
        model_name=model_name,
        model_version=model_version,
        resource_id=resource_id,
        aktivitettype_kode=aktivitettype_kode,
        aktivitettype_text=aktivitettype_text,
        overskrift=overskrift,
    )
