# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Quality check asset for detecting deleted FHIR IFDB resources.

DELETED_RESOURCE_CHECK_CONFIGS lists resource types to reconcile. For each type we:
1. Load IFDB source identifier values and FHIR row UUIDs from PostgreSQL (FHIR store).
2. Ask Oracle IFDB which of those IDs still exist (chunked IN queries); accumulate only
   orphan source IDs (not the large "present" set).
3. Orphans (in FHIR but no longer in IFDB) get urn:fhir:isDeleted=1 via direct JSONB UPDATE.

Connection safety: Oracle errors abort the check; a combined ratio + absolute orphan guard
avoids mass false positives if IFDB returns suspiciously empty results.

No Python-side FHIR parsing -- updates run in Postgres only.
"""

from typing import Any

import dagster as dg
import oracledb
from core.fhir.deletion import mark_resources_deleted
from core.resources import FHIRDatabaseResource, IFDBOracleResource
from data_quality.models import DeletedResourceCheckConfig
from DSA.FHIR.Patient.patient_service import mark_deleted
from DSI.oracle.ifdb import ENCOUNTER_CONFIG, EPISODE_OF_CARE_CONFIG, PATIENT_CONFIG
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

DELETED_RESOURCE_CHECK_CONFIGS: list[DeletedResourceCheckConfig] = [
    DeletedResourceCheckConfig(
        ifdb_config=PATIENT_CONFIG,
        resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        id_column="PATIENT_ID",
        asset_dep="fhir_server_patient_dfm",
        result_key="patient",
    ),
    DeletedResourceCheckConfig(
        ifdb_config=ENCOUNTER_CONFIG,
        resource_type="Encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        id_column="OBJECT_ID",
        asset_dep="fhir_server_encounter_dfm",
        result_key="encounter",
    ),
    DeletedResourceCheckConfig(
        ifdb_config=EPISODE_OF_CARE_CONFIG,
        resource_type="EpisodeOfCare",
        identifier_system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        id_column="OBJECT_ID",
        asset_dep="fhir_server_eoc_dfm",
        result_key="episode_of_care",
    ),
]

_ORACLE_IN_CHUNK = 999
_PG_UPDATE_BATCH = 500
_PROGRESS_LOG_EVERY_N_CHUNKS = 100

_MAX_ORPHAN_RATIO = 0.10
_MIN_ORPHAN_ABS_THRESHOLD = 100


# -----------------------------------------------------------------------------
# Internal helpers
# -----------------------------------------------------------------------------


class _OrphanLookupResult:
    """Result of comparing FHIR source IDs against IFDB existence."""

    __slots__ = (
        "orphans_to_mark",
        "still_present_count",
        "total_checked",
        "detected_orphan_count",
        "ok_to_mark",
        "skip_reason",
    )

    def __init__(
        self,
        orphans_to_mark: list[str],
        still_present_count: int,
        total_checked: int,
        detected_orphan_count: int,
        ok_to_mark: bool,
        skip_reason: str | None = None,
    ):
        self.orphans_to_mark = orphans_to_mark
        self.still_present_count = still_present_count
        self.total_checked = total_checked
        self.detected_orphan_count = detected_orphan_count
        self.ok_to_mark = ok_to_mark
        self.skip_reason = skip_reason


def _fetch_fhir_source_id_to_row_id(
    fhir_database: FHIRDatabaseResource,
    resource_type: str,
    identifier_system: str,
) -> dict[str, str]:
    """Map IFDB source identifier value -> FHIR table primary key (UUID string)."""
    table_name = resource_type.lower()
    mapping: dict[str, str] = {}
    query = f"""
        SELECT p.id, identifier_elem->>'value' AS source_id
        FROM {table_name} p,
             jsonb_array_elements(COALESCE(p.resource->'identifier', '[]'::jsonb)) AS identifier_elem
        WHERE identifier_elem->>'system' = %s
          AND identifier_elem->>'value' IS NOT NULL
    """
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, [identifier_system])
            for row in cur.fetchall():
                fhir_id, source_id = row[0], row[1]
                if source_id:
                    mapping[str(source_id)] = str(fhir_id)
    return mapping


def _find_orphaned_ids(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    table: str,
    id_column: str,
    source_ids: list[str],
) -> _OrphanLookupResult:
    """Query IFDB for rows that exist; accumulate orphan IDs only."""
    if not source_ids:
        return _OrphanLookupResult(
            orphans_to_mark=[],
            still_present_count=0,
            total_checked=0,
            detected_orphan_count=0,
            ok_to_mark=True,
        )

    total_checked = len(source_ids)
    n_chunks = (total_checked + _ORACLE_IN_CHUNK - 1) // _ORACLE_IN_CHUNK
    orphans: set[str] = set()
    still_present_count = 0

    try:
        with ifdb_oracle.get_connection() as conn:
            cursor = conn.cursor()
            try:
                for batch_num, i in enumerate(range(0, len(source_ids), _ORACLE_IN_CHUNK), start=1):
                    chunk = source_ids[i : i + _ORACLE_IN_CHUNK]
                    chunk_set = {str(sid) for sid in chunk}
                    placeholders = ", ".join([f"'{sid}'" for sid in chunk])
                    query = (
                        f"SELECT DISTINCT {id_column} FROM {table} "
                        f"WHERE {id_column} IN ({placeholders})"
                    )
                    cursor.execute(query)
                    present_chunk: set[str] = set()
                    for row in cursor.fetchall():
                        if row[0] is not None:
                            present_chunk.add(str(row[0]))
                    still_present_count += len(present_chunk)
                    orphans.update(chunk_set - present_chunk)

                    if batch_num % _PROGRESS_LOG_EVERY_N_CHUNKS == 0:
                        context.log.info(
                            f"IFDB {table}: Oracle IN batch {batch_num}/{n_chunks}, "
                            f"{len(orphans)} orphan source IDs accumulated so far"
                        )
            finally:
                cursor.close()
    except oracledb.Error as e:
        context.log.error(f"Oracle IFDB query failed for {table}: {e}")
        return _OrphanLookupResult(
            orphans_to_mark=[],
            still_present_count=0,
            total_checked=total_checked,
            detected_orphan_count=0,
            ok_to_mark=False,
            skip_reason=f"Oracle error: {e}",
        )

    detected = len(orphans)
    if still_present_count + detected != total_checked:
        context.log.warning(
            f"IFDB {table}: identity check mismatch: present={still_present_count} "
            f"+ orphans={detected} != total={total_checked}"
        )

    if (
        detected >= _MIN_ORPHAN_ABS_THRESHOLD
        and total_checked > 0
        and (detected / total_checked) > _MAX_ORPHAN_RATIO
    ):
        ratio = detected / total_checked
        msg = (
            f"Aborting delete-mark for {table}: suspicious orphan ratio {ratio:.6f} "
            f"({detected}/{total_checked}) exceeds {_MAX_ORPHAN_RATIO} "
            f"with orphan count >= {_MIN_ORPHAN_ABS_THRESHOLD}"
        )
        context.log.error(msg)
        return _OrphanLookupResult(
            orphans_to_mark=[],
            still_present_count=still_present_count,
            total_checked=total_checked,
            detected_orphan_count=detected,
            ok_to_mark=False,
            skip_reason=msg,
        )

    context.log.info(
        f"IFDB {table}: {total_checked} IDs checked, {still_present_count} still present, "
        f"{detected} orphaned ({n_chunks} Oracle IN batches)"
    )
    return _OrphanLookupResult(
        orphans_to_mark=sorted(orphans),
        still_present_count=still_present_count,
        total_checked=total_checked,
        detected_orphan_count=detected,
        ok_to_mark=True,
    )


def _batch_mark_deleted_jsonb(context, fhir_database, resource_type, fhir_row_ids):
    """Generic reconciliation; person resources must use their domain service."""
    if resource_type == "Patient":
        raise ValueError("Use the Patient service for Patient deletion")
    return mark_resources_deleted(fhir_database, resource_type, fhir_row_ids)


def _check_and_mark_deleted_resources(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    check_config: DeletedResourceCheckConfig,
) -> dict[str, Any]:
    resource_type = check_config.resource_type

    if not check_config.enabled:
        context.log.info(
            f"Skipping quality check for {resource_type} "
            f"(disabled in DELETED_RESOURCE_CHECK_CONFIGS)"
        )
        return {
            "resource_type": resource_type,
            "source_count": 0,
            "fhir_count": 0,
            "orphaned_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
            "skip_reason": "disabled",
        }

    context.log.info(
        f"Starting quality check for {resource_type} resources "
        f"from {check_config.ifdb_config.table}"
    )

    fhir_map = _fetch_fhir_source_id_to_row_id(
        fhir_database,
        resource_type,
        check_config.identifier_system,
    )
    fhir_count = len(fhir_map)
    if fhir_count == 0:
        return {
            "resource_type": resource_type,
            "source_count": 0,
            "fhir_count": 0,
            "orphaned_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
        }

    source_ids = list(fhir_map.keys())
    lookup = _find_orphaned_ids(
        context,
        ifdb_oracle,
        check_config.ifdb_config.table,
        check_config.id_column,
        source_ids,
    )

    orphaned_count = lookup.detected_orphan_count
    source_count = lookup.still_present_count

    context.log.info(
        f"{resource_type}: {lookup.total_checked} in FHIR, {source_count} still in IFDB, "
        f"{orphaned_count} orphaned (absent from IFDB)"
    )

    if not lookup.ok_to_mark:
        return {
            "resource_type": resource_type,
            "source_count": source_count,
            "fhir_count": lookup.total_checked,
            "orphaned_count": orphaned_count,
            "updated_count": 0,
            "skipped_count": 0,
            "skip_reason": lookup.skip_reason,
        }

    if not lookup.orphans_to_mark:
        return {
            "resource_type": resource_type,
            "source_count": source_count,
            "fhir_count": lookup.total_checked,
            "orphaned_count": 0,
            "updated_count": 0,
            "skipped_count": 0,
        }

    fhir_row_ids = [fhir_map[sid] for sid in lookup.orphans_to_mark]
    if resource_type == "Patient":
        updated_count, skipped_marked = mark_deleted(
            context, fhir_database, fhir_row_ids, missing_epj_ids=lookup.orphans_to_mark
        )
    else:
        updated_count, skipped_marked = _batch_mark_deleted_jsonb(
            context, fhir_database, resource_type, fhir_row_ids
        )

    return {
        "resource_type": resource_type,
        "source_count": source_count,
        "fhir_count": lookup.total_checked,
        "orphaned_count": orphaned_count,
        "updated_count": updated_count,
        "skipped_count": skipped_marked,
    }


# -----------------------------------------------------------------------------
# Dagster Asset
# -----------------------------------------------------------------------------


@dg.asset(
    name="fhir_quality_check_deleted_resources",
    group_name="data_quality",
    deps=[c.asset_dep for c in DELETED_RESOURCE_CHECK_CONFIGS],
    tags={
        "domain": "fhir",
        "layer": "quality_check",
        "type": "reconciliation",
        "scope": "full_ifdb_vs_fhir",
    },
    description=(
        "Full-scan quality check: loads IFDB-linked identifiers from the FHIR "
        "PostgreSQL store, verifies existence in Oracle IFDB, and sets "
        "urn:fhir:isDeleted=1 on orphans via JSONB UPDATE."
    ),
)
def fhir_quality_check_deleted_resources(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Compares FHIR-stored IFDB identifiers with Oracle IFDB; hard-deleted
    source rows get urn:fhir:isDeleted=1 on the FHIR resource JSON in Postgres.
    """
    results: dict[str, dict[str, Any]] = {}

    for check_config in DELETED_RESOURCE_CHECK_CONFIGS:
        context.log.info(f"Checking {check_config.resource_type} resources...")
        result = _check_and_mark_deleted_resources(
            context, ifdb_oracle, fhir_database, check_config
        )
        results[check_config.result_key] = result

    total_orphaned = sum(r["orphaned_count"] for r in results.values())
    total_updated = sum(r["updated_count"] for r in results.values())
    total_skipped = sum(r["skipped_count"] for r in results.values())

    context.log.info(
        f"Quality check complete: {total_orphaned} orphaned resources found, "
        f"{total_updated} updated, {total_skipped} skipped (already marked)"
    )

    metadata: dict[str, dg.MetadataValue] = {}
    for key, result in results.items():
        metadata[f"{key}_orphaned"] = dg.MetadataValue.int(result["orphaned_count"])
        metadata[f"{key}_updated"] = dg.MetadataValue.int(result["updated_count"])
    metadata["total_orphaned"] = dg.MetadataValue.int(total_orphaned)
    metadata["total_updated"] = dg.MetadataValue.int(total_updated)
    metadata["total_skipped"] = dg.MetadataValue.int(total_skipped)
    metadata["results"] = dg.MetadataValue.json(results)

    return dg.MaterializeResult(metadata=metadata)
