# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR QuestionnaireResponse builder for IFDB AKTIVITETSRESULTAT data.

Each QR-routed AKTIVITETSRESULTAT row becomes one ``QuestionnaireResponse``.
The QR references its Questionnaire by version-pinned canonical URL
(``<canonical>|<version>``) so the schema in force at write-time is
deterministic.

Item linking contract:
  - ``QR.item.linkId == Questionnaire.item.linkId == <sfi_code>``.
  - ``QR.item.id == Questionnaire.item.id`` for the same source occurrence.
  - ``QR.item`` carries no native ``code`` field (FHIR R4 does not define
    one); SFI is resolved by joining ``QR.item.linkId`` back to
    ``Questionnaire.item.code`` in the SQL-on-FHIR projection, with ``id`` as
    the occurrence-level discriminator when a SFI code repeats.
  - ``QR.item.id`` is the native occurrence key used by SQL-on-FHIR to keep
    repeated items with the same ``linkId`` distinct. We avoid custom QR item
    extensions by default.
  - Items are deliberately flat: klynge/hierarchy/repetition context lives in
    the SFI CodeSystem properties, not in nested QR.item groups. Repetitions
    produce multiple flat QR.items sharing the same ``linkId`` but with
    distinct ``id`` values.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from DSA.FHIR.Questionnaire._item_key import stable_sfi_item_id
from DSA.FHIR.Questionnaire._sfi import (
    SFI_SYSTEM,
    generate_sfi_code,
)
from DSA.FHIR.Questionnaire._value_mapping import coerce_answer_value
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import (
    _infer_datatype_from_row,
    build_questionnaire_version_pinned_reference,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
    parse_aktivitetsresultat_xml,
)
from utils.fhir.fhir_extensions import EXTENSION_SFI_INSTANCE_META
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict


def _normalize_id(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _as_status(slettet: Any, kladde: Any) -> str:
    """Map IFDB lifecycle flags to ``QuestionnaireResponse.status``."""
    if _truthy(slettet):
        return "entered-in-error"
    if _truthy(kladde):
        return "in-progress"
    return "completed"


def _truthy(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    s = str(value).strip().lower()
    return s in ("1", "true", "ja", "yes", "t", "y")


def _coerce_datetime_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s).isoformat()
    except ValueError:
        return s


def build_qr_identifier_value(objekt_id: Any) -> str | None:
    """Return the source identifier value used to resolve existing QR ids."""
    return _normalize_id(objekt_id)


def _build_subject_ref(patient_id: Any) -> dict | None:
    pid = _normalize_id(patient_id)
    if pid is None:
        return None
    return {
        "identifier": {
            "system": SYSTEM_IFDB_PATIENT_ID,
            "value": pid,
        }
    }


def _build_encounter_ref(encounter_id: Any) -> dict | None:
    eid = _normalize_id(encounter_id)
    if eid is None:
        return None
    return {
        "identifier": {
            "system": SYSTEM_IFDB_OBJECT_ID,
            "value": eid,
        }
    }


def _build_author_ref(row_dict: dict[str, Any]) -> dict | None:
    """Build QuestionnaireResponse.author from UDFOERT_PERSID / UDFOERT_PERSNAVN.

    FHIR R4 QuestionnaireResponse.author is a single Reference (not a list).
    We prefer Practitioner when available; fall back to nothing.
    """
    pers_id = row_dict.get("UDFOERT_PERSID")
    pers_id_norm = _normalize_id(pers_id)
    pers_navn = row_dict.get("UDFOERT_PERSNAVN")
    if not pers_id_norm and not pers_navn:
        return None
    ref: dict[str, Any] = {"type": "Practitioner"}
    if pers_navn:
        ref["display"] = str(pers_navn)
    if pers_id_norm:
        ref["identifier"] = {
            "system": SYSTEM_IFDB_STAFF,
            "value": pers_id_norm,
        }
    return ref


def _build_qr_item_dict(
    row: AktivitetsresultatRow,
    model_name: str | None,
) -> dict | None:
    """Build one ``QuestionnaireResponse.item`` for an Element row.

    Returns ``None`` if the row carries no answer-bearing content (Tom, XML
    error, top-level container without value, etc.).
    """
    if row.xml_status == -1:
        return None
    if row.raekke_type not in ("Element", "Selektion"):
        return None
    if row.raekke_type == "Element" and not row.element_navn:
        return None
    if row.raekke_type == "Selektion" and not row.klynge_navn:
        return None

    if row.raekke_type == "Element":
        sfi_code, _ = generate_sfi_code(row.element_element_id, row.element_navn, model_name)
        display = row.element_ledetekst or row.element_navn or sfi_code
        datatype = _infer_datatype_from_row(row)
        raw_value = row.element_vaerdi or ""
        element_kode = row.element_kode
        element_navn = row.element_navn
    else:
        sfi_code, _ = generate_sfi_code(None, row.klynge_navn, model_name)
        display = row.klynge_ledetekst or row.klynge_navn or sfi_code
        datatype = "coding"
        raw_value = row.klynge_valg_ledetekst or ""
        element_kode = None
        element_navn = row.klynge_navn

    primary_occurrence_id = row.element_nummer if row.raekke_type == "Element" else row.klynge_id
    item_id_prefix = "e" if row.raekke_type == "Element" else "g"
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
        "text": display,
    }

    answer_field, answer_value = coerce_answer_value(
        sfi_datatype=datatype,
        raw_value=raw_value or None,
        element_kode=element_kode,
        element_navn=element_navn,
        sfi_system=SFI_SYSTEM,
    )
    if answer_value is not None:
        item["answer"] = [{answer_field: answer_value}]

    instance_parts: list[dict] = []
    if row.element_nummer is not None:
        instance_parts.append({"url": "elementNummer", "valueInteger": row.element_nummer})
    if row.klynge_id is not None:
        instance_parts.append({"url": "klyngeId", "valueInteger": row.klynge_id})
    if row.klynge_id_hierarki:
        instance_parts.append({"url": "klyngeIdHierarki", "valueString": row.klynge_id_hierarki})
    if row.klynge_valg_ledetekst:
        instance_parts.append(
            {
                "url": "klyngeValgLedetekst",
                "valueString": row.klynge_valg_ledetekst,
            }
        )
    if row.klynge_parent_id is not None:
        instance_parts.append({"url": "klyngeParentId", "valueInteger": row.klynge_parent_id})
    if row.repetition_id is not None:
        instance_parts.append({"url": "repetitionId", "valueInteger": row.repetition_id})
    if row.element_kode:
        instance_parts.append({"url": "elementKode", "valueString": row.element_kode})
    if row.element_original_kode:
        instance_parts.append(
            {
                "url": "elementOriginalKode",
                "valueString": row.element_original_kode,
            }
        )
    if row.element_administrativ_enhed_id:
        instance_parts.append(
            {
                "url": "elementAdminEnhedId",
                "valueString": row.element_administrativ_enhed_id,
            }
        )
    if row.repetition_id_hierarki:
        instance_parts.append(
            {
                "url": "repetitionIdHierarki",
                "valueString": row.repetition_id_hierarki,
            }
        )
    if row.opdateret_tid:
        instance_parts.append(
            {
                "url": "opdateretTid",
                "valueString": row.opdateret_tid.isoformat(),
            }
        )
    if row.xml_status and row.xml_status != 0:
        instance_parts.append({"url": "xmlStatus", "valueInteger": row.xml_status})
    if row.xml_status_text:
        instance_parts.append({"url": "xmlStatusText", "valueString": row.xml_status_text})
    if instance_parts:
        item["extension"] = [{"url": EXTENSION_SFI_INSTANCE_META, "extension": instance_parts}]

    return item


def build_questionnaire_response_items_dict(
    parsed_rows: list[AktivitetsresultatRow],
    model_name: str | None,
) -> list[dict]:
    """Build a flat ``QuestionnaireResponse.item`` list as plain dicts.

    Unlike the Questionnaire schema, QR items are NOT deduplicated: each
    answer-bearing row produces one item, so repetitions are preserved as
    multiple flat QR.item entries sharing the same ``linkId`` but with
    distinct ``id`` values. Klynge/hierarchy context lives in the SFI
    CodeSystem properties, not in nested group items.
    """
    items: list[dict] = []
    for row in parsed_rows:
        item = _build_qr_item_dict(row, model_name)
        if item is not None:
            items.append(item)
    return items


def build_questionnaire_response_dict(
    row_dict: dict[str, Any],
    parsed_rows: list[AktivitetsresultatRow],
    kontekst: AktivitetskontekstRow | None = None,
    *,
    model_name: str | None = None,
    model_version: str | None = None,
    resource_id: str | None = None,
) -> dict:
    """Build a FHIR QuestionnaireResponse resource dict for one IFDB row.

    Args:
        row_dict: The IFDB AKTIVITETSRESULTAT source row (column dict).  Must
            include ``OBJEKT_ID``, ``PATIENT_ID`` and ideally
            ``MODELLERINGSNAVN`` / ``MODELLERINGSVERSION`` so the QR can
            be version-pinned to the right Questionnaire.
        parsed_rows: XML parser output from
            ``parse_aktivitetsresultat_xml(row_dict["XML"], ...)``.
        kontekst: Parsed Kontekst node (optional).  Not embedded in this
            first implementation — provenance lineage stays on the source
            row in the database, and the extension-cleanup contract controls
            allowed FHIR extensions.  Accepted for API compatibility with the
            Observation builder.
        model_name: Override for ``MODELLERINGSNAVN``.  Falls back to the
            row dict.
        model_version: Override for ``MODELLERINGSVERSION``.  Falls back
            to the row dict.

    Raises:
        ValueError: When the resolved model_name/model_version pair cannot
            produce a version-pinned ``Questionnaire.questionnaire``
            reference. Failing here keeps the contract loud: a QR without a
            pinned Questionnaire is not analysable.
    """
    objekt_id = row_dict.get("OBJEKT_ID")
    patient_id = row_dict.get("PATIENT_ID")
    encounter_id = row_dict.get("ENCOUNTER_ID")
    title = row_dict.get("OVERSKRIFT")
    slettet = row_dict.get("SLETTET")
    kladde = row_dict.get("KLADDE")
    period_start = row_dict.get("PERIOD_START")

    resolved_model_name = model_name or row_dict.get("MODELLERINGSNAVN")
    resolved_model_version = model_version or row_dict.get("MODELLERINGSVERSION")

    questionnaire_ref = build_questionnaire_version_pinned_reference(
        resolved_model_name, resolved_model_version
    )

    items = build_questionnaire_response_items_dict(parsed_rows, resolved_model_name)

    resource: dict[str, Any] = {
        "resourceType": "QuestionnaireResponse",
        "id": resource_id or str(uuid.uuid4()),
        "status": _as_status(slettet, kladde),
        "questionnaire": questionnaire_ref,
    }
    # Sletmarkeringen som udvidelse, ikke kun som status: de flade tabellers
    # sletteflag-kolonner laeser udvidelsen, og skemasvar var den eneste af de
    # fire ressourcetyper, der ikke skrev den. Samme sandhedsfortolkning som
    # status, saa flag og status aldrig kan modsige hinanden.
    resource["extension"] = [build_is_deleted_extension_dict(1 if _truthy(slettet) else 0)]

    norm_objekt = build_qr_identifier_value(objekt_id)
    if norm_objekt:
        resource["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT,
                "value": norm_objekt,
            }
        ]

    subject = _build_subject_ref(patient_id)
    if subject:
        resource["subject"] = subject

    encounter = _build_encounter_ref(encounter_id)
    if encounter:
        resource["encounter"] = encounter

    author_ref = _build_author_ref(row_dict)
    if author_ref:
        resource["author"] = author_ref

    authored = _coerce_datetime_string(period_start)
    if authored:
        resource["authored"] = authored

    if title:
        resource["_title"] = str(title)  # internal hint; stripped below.

    if items:
        resource["item"] = items

    # Strip internal hints that should not be persisted.
    resource.pop("_title", None)

    return resource


def build_questionnaire_response_from_xml(
    row_dict: dict[str, Any],
    *,
    model_name: str | None = None,
    model_version: str | None = None,
    resource_id: str | None = None,
) -> dict:
    """Convenience wrapper: parse XML and build the QR in one call.

    Intended for tests and one-off tooling. Production pipelines should
    parse XML once and feed parsed rows into
    ``build_questionnaire_response_dict`` so the same parse can be shared
    with the Questionnaire builder and shared SFI extraction.
    """
    aktivitets_id_raw = row_dict.get("OBJEKT_ID") or 0
    try:
        aktivitets_id = int(aktivitets_id_raw) if aktivitets_id_raw else 0
    except (TypeError, ValueError):
        aktivitets_id = 0

    parsed_rows, kontekst = parse_aktivitetsresultat_xml(
        xml_string=row_dict.get("XML"), aktivitets_id=aktivitets_id
    )
    return build_questionnaire_response_dict(
        row_dict,
        parsed_rows,
        kontekst=kontekst,
        model_name=model_name,
        model_version=model_version,
        resource_id=resource_id,
    )
