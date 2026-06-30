# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Helpers for IFDB Questionnaire / QuestionnaireResponse asset builds.

Dagster assets are registered via
``DSA.FHIR.Aktivitetsresultat.assets`` (unified
multi_asset). This module retains routing and reference helpers for tests
and optional direct imports.
"""

from __future__ import annotations

import os
from collections.abc import Iterable
from datetime import date, datetime
from typing import Any

from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.ifdb.routing import (
    RoutingResolver,
    TargetResource,
    build_routing_resolver,
)
from DSA.FHIR.utils.ifdb.routing_fallback import (
    RoutingDecision,
    resolve_target_with_fallback,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetsresultatRow,
    parse_aktivitetsresultat_xml,
)
from utils.fhir.identifier_systems import SYSTEM_IFDB_OBJECT_ID, SYSTEM_IFDB_PATIENT_ID

_QR_CHUNK_SIZE = 500


def _sfi_mode() -> str:
    return os.environ.get("IFDB_SFI_OBSERVATION_MODE", "sfi_audit").strip().lower()


def _sfi_enabled() -> bool:
    return _sfi_mode() != "legacy"


def routing_resolver(mssql_target_staging: MSSQLResource) -> RoutingResolver:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            return build_routing_resolver(cursor=cursor)
        finally:
            cursor.close()


def _occurred_at(row: dict[str, Any]) -> date | datetime | None:
    value = row.get("PERIOD_START") or row.get("OPDATERET_TID")
    if isinstance(value, date | datetime):
        return value
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def resolve_row_route(
    row: dict[str, Any],
    resolver: RoutingResolver,
    parsed_rows: list[AktivitetsresultatRow] | None = None,
) -> tuple[RoutingDecision, list[AktivitetsresultatRow] | None, Any | None]:
    """Resolve QR/Observation routing using the shared Observation allowlist."""
    occurred_at = _occurred_at(row)
    decision = resolve_target_with_fallback(
        row,
        resolver,
        parsed_rows,
        occurred_at=occurred_at,
    )

    kontekst = None
    if parsed_rows is None and decision.target_resource == TargetResource.QUESTIONNAIRE_RESPONSE:
        parsed_rows, kontekst = parse_row(row)

    return decision, parsed_rows, kontekst


def iter_batches(rows: Any, batch_size: int) -> Iterable[list[dict[str, Any]]]:
    iter_batches_fn = getattr(rows, "iter_batches", None)
    if callable(iter_batches_fn):
        yield from iter_batches_fn(batch_size)
        return
    for start in range(0, len(rows), batch_size):
        yield rows[start : start + batch_size]


def parse_row(row: dict[str, Any]) -> tuple[list[AktivitetsresultatRow], Any]:
    raw_id = row.get("OBJEKT_ID") or 0
    try:
        aktivitets_id = int(raw_id) if raw_id else 0
    except (TypeError, ValueError):
        aktivitets_id = 0
    return parse_aktivitetsresultat_xml(row.get("XML"), aktivitets_id=aktivitets_id)


def _flat_questionnaire_item_key(item: dict) -> tuple[Any, Any]:
    return (item.get("linkId"), item.get("id") or item.get("linkId"))


def merge_flat_questionnaire_items(existing: list[dict], incoming: list[dict]) -> None:
    """Merge flat Questionnaire items by composite (linkId, native item id)."""
    by_item_key = {
        _flat_questionnaire_item_key(item): item
        for item in existing
        if item.get("id") or item.get("linkId")
    }
    for item in incoming:
        if not (item.get("id") or item.get("linkId")):
            existing.append(item)
            continue

        item_key = _flat_questionnaire_item_key(item)
        if item_key not in by_item_key:
            existing.append(item)
            by_item_key[item_key] = item


def resolve_reference_maps(
    rows: list[dict[str, Any]],
    fhir_database: FHIRDatabaseResource,
) -> tuple[dict[str, str], dict[str, str]]:
    patient_ids = sorted(
        {pid for row in rows if (pid := normalize_oracle_id(row.get("PATIENT_ID"))) is not None}
    )
    encounter_ids = sorted(
        {eid for row in rows if (eid := normalize_oracle_id(row.get("ENCOUNTER_ID"))) is not None}
    )
    patient_map = (
        fhir_database.resolve_identifier_values(
            resource_type="Patient",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=patient_ids,
        )
        if patient_ids
        else {}
    )
    encounter_map = (
        fhir_database.resolve_identifier_values(
            resource_type="Encounter",
            identifier_system=SYSTEM_IFDB_OBJECT_ID,
            values=encounter_ids,
        )
        if encounter_ids
        else {}
    )
    return patient_map, encounter_map


def apply_resolved_refs(
    qr: dict,
    row: dict[str, Any],
    patient_map: dict[str, str],
    encounter_map: dict[str, str],
) -> None:
    patient_id = normalize_oracle_id(row.get("PATIENT_ID"))
    if patient_id and patient_id in patient_map:
        qr["subject"] = {"resourceType": "Patient", "id": patient_map[patient_id]}

    encounter_id = normalize_oracle_id(row.get("ENCOUNTER_ID"))
    if encounter_id and encounter_id in encounter_map:
        qr["encounter"] = {"resourceType": "Encounter", "id": encounter_map[encounter_id]}


all_assets: list = []
