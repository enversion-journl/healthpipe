# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Runtime audit for IFDB AKTIVITETSRESULTAT routing decisions."""

from __future__ import annotations

from typing import Any

from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id


def _as_audit_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def routing_decision_record_for_row(
    row_dict: dict[str, Any],
    decision: RoutingDecision,
) -> dict[str, str | None] | None:
    """Build the idempotent audit row for one runtime routing decision."""
    ifdb_aktivitet_id = normalize_oracle_id(row_dict.get("OBJEKT_ID"))
    if not ifdb_aktivitet_id:
        return None

    return {
        "ifdb_aktivitet_id": ifdb_aktivitet_id,
        "aktivitetstype_kode": _as_audit_text(row_dict.get("AKTIVITETTYPE_KODE")),
        "target_resource": decision.target_resource.value,
        "observation_category": _as_audit_text(decision.observation_category),
        "routing_source": decision.routing_source,
        "routing_confidence": decision.routing_confidence,
        "routing_reason": decision.routing_reason,
    }


def append_routing_decision(
    sink: list[dict[str, str | None]] | None,
    row_dict: dict[str, Any],
    decision: RoutingDecision,
) -> None:
    if sink is None:
        return
    record = routing_decision_record_for_row(row_dict, decision)
    if record is not None:
        sink.append(record)
