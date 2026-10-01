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

_ROUTING_DECISION_TABLE_MISSING_HINTS = (
    "invalid object name",
    "base table or view not found",
    "no such table",
    "does not exist",
)


_ROUTING_DECISION_MERGE_SQL = """
MERGE [meta].[IFDBAktivitetsresultatRoutingDecision] AS tgt
USING (
    SELECT
        CAST(? AS NVARCHAR(255)) AS [ifdb_aktivitet_id],
        CAST(? AS NVARCHAR(255)) AS [aktivitetstype_kode],
        CAST(? AS NVARCHAR(32)) AS [target_resource],
        CAST(? AS NVARCHAR(64)) AS [observation_category],
        CAST(? AS NVARCHAR(50)) AS [routing_source],
        CAST(? AS NVARCHAR(32)) AS [routing_confidence],
        CAST(? AS NVARCHAR(MAX)) AS [routing_reason]
) AS src
ON tgt.[ifdb_aktivitet_id] = src.[ifdb_aktivitet_id]
WHEN MATCHED THEN
    UPDATE SET
        tgt.[aktivitetstype_kode] = src.[aktivitetstype_kode],
        tgt.[target_resource] = src.[target_resource],
        tgt.[observation_category] = src.[observation_category],
        tgt.[routing_source] = src.[routing_source],
        tgt.[routing_confidence] = src.[routing_confidence],
        tgt.[routing_reason] = src.[routing_reason],
        tgt.[decided_at] = CAST(SYSDATETIME() AS datetime2(0)),
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [ifdb_aktivitet_id],
        [aktivitetstype_kode],
        [target_resource],
        [observation_category],
        [routing_source],
        [routing_confidence],
        [routing_reason],
        [decided_at],
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[ifdb_aktivitet_id],
        src.[aktivitetstype_kode],
        src.[target_resource],
        src.[observation_category],
        src.[routing_source],
        src.[routing_confidence],
        src.[routing_reason],
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""


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


def upsert_routing_decisions(
    mssql_target_staging: Any,
    routing_decisions: list[dict[str, str | None]],
    logger: Any,
) -> int:
    """Persist routing decisions as an idempotent audit table.

    The primary key is the source AKTIVITETSRESULTAT id, so replaying a batch
    updates the existing decision row instead of appending a new audit resource
    every time.
    """
    if mssql_target_staging is None or not routing_decisions:
        return 0

    deduped: dict[str, dict[str, str | None]] = {}
    for record in routing_decisions:
        ifdb_aktivitet_id = record.get("ifdb_aktivitet_id")
        if ifdb_aktivitet_id:
            deduped[ifdb_aktivitet_id] = record

    if not deduped:
        return 0

    params = [
        (
            record.get("ifdb_aktivitet_id"),
            record.get("aktivitetstype_kode"),
            record.get("target_resource"),
            record.get("observation_category"),
            record.get("routing_source"),
            record.get("routing_confidence"),
            record.get("routing_reason"),
        )
        for record in deduped.values()
    ]

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True
                cursor.executemany(_ROUTING_DECISION_MERGE_SQL, params)
                conn.commit()
            finally:
                if hasattr(cursor, "close"):
                    cursor.close()
    except Exception as exc:
        message = str(exc).lower()
        if any(hint in message for hint in _ROUTING_DECISION_TABLE_MISSING_HINTS):
            logger.warning(
                "[SFI] Routing decision audit table missing; skipping %d decisions",
                len(params),
            )
            return 0
        raise

    logger.info("[SFI] Upserted %d routing decision audit rows", len(params))
    return len(params)
