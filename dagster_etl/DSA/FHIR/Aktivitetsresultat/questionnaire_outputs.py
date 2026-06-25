# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Questionnaire side-output helpers for AKTIVITETSRESULTAT."""

import json
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.Questionnaire.Questionnaire_IFDB import build_questionnaire_dict
from DSA.FHIR.QuestionnaireResponse.QuestionnaireResponse_IFDB import (
    build_questionnaire_response_dict,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
)

from .references import ResolvedRefLookup


def _merge_item_key(item: dict) -> tuple[Any, Any]:
    """Deduplicate by SFI concept plus source occurrence id."""
    return (item.get("linkId"), item.get("id") or item.get("linkId"))


def _merge_questionnaire_into_store(store: dict[str, dict], questionnaire: dict) -> bool:
    """Merge flat Questionnaire items by composite (linkId, native item id).

    Returns True when the stored Questionnaire is new or gained new items.
    """
    qid = questionnaire["id"]
    if qid not in store:
        store[qid] = questionnaire
        return True

    changed = False
    existing_items = store[qid].setdefault("item", [])
    by_item_key = {
        _merge_item_key(item): item
        for item in existing_items
        if item.get("id") or item.get("linkId")
    }
    for item in questionnaire.get("item") or []:
        if not (item.get("id") or item.get("linkId")):
            # No occurrence identity at all — cannot dedup safely, so keep the
            # item rather than risk dropping a distinct datapoint.
            existing_items.append(item)
            changed = True
            continue
        item_key = _merge_item_key(item)
        if item_key not in by_item_key:
            existing_items.append(item)
            changed = True
            by_item_key[item_key] = item
    return changed


def _merge_existing_questionnaire_items(questionnaire: dict, existing_resource: dict) -> bool:
    """Merge persisted Aidbox Questionnaire items into the outgoing resource.

    ``Questionnaire`` is model/version-level, while ``QuestionnaireResponse`` is
    activity-level. A later partition can therefore see only a subset of the
    model's item occurrences. Direct DB upsert replaces the whole JSON resource,
    so posting that subset would remove item ids needed by earlier QRs. Preserve
    any existing ``item[]`` entries that are absent from this run's resource.
    """
    existing_items = existing_resource.get("item") or []
    if not existing_items:
        return False

    changed = False
    outgoing_items = questionnaire.setdefault("item", [])
    by_item_key = {
        _merge_item_key(item): item
        for item in outgoing_items
        if item.get("id") or item.get("linkId")
    }
    for item in existing_items:
        if not (item.get("id") or item.get("linkId")):
            outgoing_items.append(item)
            changed = True
            continue
        item_key = _merge_item_key(item)
        if item_key not in by_item_key:
            outgoing_items.append(item)
            by_item_key[item_key] = item
            changed = True
    return changed


def _load_existing_questionnaires_by_id(
    fhir_database: FHIRDatabaseResource,
    qids: list[str],
) -> dict[str, dict]:
    """Load persisted Aidbox Questionnaire resources for non-destructive merges."""
    if not qids:
        return {}

    with fhir_database.get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, resource FROM questionnaire WHERE id = ANY(%s)",
                (qids,),
            )
            rows = cursor.fetchall()

    existing: dict[str, dict] = {}
    for qid, resource in rows:
        if isinstance(resource, str):
            resource = json.loads(resource)
        if isinstance(resource, dict):
            existing[str(qid)] = resource
    return existing


def _apply_resolved_lookup_refs_to_qr(
    qr: dict,
    row_dict: dict[str, Any],
    resolved: ResolvedRefLookup | None,
) -> None:
    """Upgrade QR subject/encounter refs from identifier-only to Aidbox ids."""
    if resolved is None:
        return

    patient_id = _normalize_oracle_id(row_dict.get("PATIENT_ID"))
    if patient_id:
        patient_ref = resolved.patient_ref(patient_id)
        if patient_ref and patient_ref.get("id"):
            qr["subject"] = {
                "resourceType": "Patient",
                "id": patient_ref["id"],
            }

    encounter_id = _normalize_oracle_id(row_dict.get("ENCOUNTER_ID"))
    if encounter_id:
        encounter_ref = resolved.encounter_ref(encounter_id)
        if encounter_ref and encounter_ref.get("id"):
            qr["encounter"] = {
                "resourceType": "Encounter",
                "id": encounter_ref["id"],
            }


def _append_questionnaire_outputs_for_row(
    *,
    row_dict: dict[str, Any],
    xml_rows: list[AktivitetsresultatRow],
    kontekst: AktivitetskontekstRow | None,
    resolved: ResolvedRefLookup | None,
    questionnaires_by_id: dict[str, dict],
    dirty_questionnaire_ids: list[str] | None,
    qr_dicts: list[tuple[str, dict]],
) -> None:
    questionnaire = build_questionnaire_dict(
        xml_rows,
        model_name=row_dict.get("MODELLERINGSNAVN"),
        model_version=row_dict.get("MODELLERINGSVERSION"),
        resource_id=(
            resolved.questionnaire_id(
                row_dict.get("MODELLERINGSNAVN"),
                row_dict.get("MODELLERINGSVERSION"),
            )
            if resolved
            else None
        ),
        aktivitettype_kode=row_dict.get("AKTIVITETTYPE_KODE"),
        aktivitettype_text=row_dict.get("AKTIVITETTYPE_KODETEKST"),
        overskrift=row_dict.get("OVERSKRIFT"),
    )
    if _merge_questionnaire_into_store(questionnaires_by_id, questionnaire):
        if dirty_questionnaire_ids is not None:
            dirty_questionnaire_ids.append(questionnaire["id"])

    qr = build_questionnaire_response_dict(
        row_dict,
        xml_rows,
        kontekst=kontekst,
        model_name=row_dict.get("MODELLERINGSNAVN"),
        model_version=row_dict.get("MODELLERINGSVERSION"),
        resource_id=(
            resolved.questionnaire_response_id(row_dict.get("OBJEKT_ID")) if resolved else None
        ),
    )
    _apply_resolved_lookup_refs_to_qr(qr, row_dict, resolved)
    qr_dicts.append((qr["id"], qr))


def _metadata_int(result: dg.MaterializeResult, key: str) -> int:
    value = result.metadata.get(key)
    return int(getattr(value, "value", 0))


def _sfi_side_output_result(
    *,
    context: dg.AssetExecutionContext,
    posted_count: int,
    failed_count: int,
    chunk_count: int,
    chunk_size: int,
    method: str,
) -> dg.MaterializeResult:
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(posted_count),
            "failed_count": dg.MetadataValue.int(failed_count),
            "chunk_count": dg.MetadataValue.int(chunk_count),
            "chunk_size": dg.MetadataValue.int(chunk_size),
            "partition_key": dg.MetadataValue.text(partition_key),
            "method": dg.MetadataValue.text(method),
        }
    )


def _post_dirty_questionnaires(
    *,
    context: dg.AssetExecutionContext,
    questionnaires_by_id: dict[str, dict],
    dirty_questionnaire_ids: list[str],
    fhir_database: FHIRDatabaseResource,
) -> tuple[int, int, list[str]]:
    from DSA.FHIR.utils.ifdb.fhir_posting import post_resource_dicts_direct

    if not dirty_questionnaire_ids:
        return 0, 0, []

    seen: set[str] = set()
    qids: list[str] = []
    for qid in dirty_questionnaire_ids:
        if qid not in seen and qid in questionnaires_by_id:
            seen.add(qid)
            qids.append(qid)

    if not qids:
        return 0, 0, []

    existing_by_id = _load_existing_questionnaires_by_id(fhir_database, qids)
    merged_existing_count = 0
    for qid in qids:
        existing = existing_by_id.get(qid)
        if existing and _merge_existing_questionnaire_items(questionnaires_by_id[qid], existing):
            merged_existing_count += 1

    if merged_existing_count:
        context.log.info(
            "Merged existing Aidbox Questionnaire items into %d outgoing Questionnaire resources",
            merged_existing_count,
        )

    result = post_resource_dicts_direct(
        context=context,
        resource_dicts=[(qid, questionnaires_by_id[qid]) for qid in qids],
        fhir_database=fhir_database,
        resource_type="Questionnaire",
        source_name="ifdb_oracle",
        extracted_count=len(qids),
        invalidate_cache=False,
    )
    return (
        _metadata_int(result, "posted_count"),
        _metadata_int(result, "failed_count"),
        qids,
    )
