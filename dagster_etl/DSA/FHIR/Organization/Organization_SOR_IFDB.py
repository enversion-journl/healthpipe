# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Organization asset from SOR staging.

Transforms SOR organization data (stg_organisation_sor) into FHIR Organization
resources and posts them to the FHIR database using direct DB insertion.

Data source: Sundhedsdatastyrelsen SOR (Sundhedsvæsenets Organisationsregister)
- Authoritative Danish healthcare organization registry
- Provides hierarchy (partOf), entity type, and active/closed status

Memory-efficient two-pass strategy:
  Pass 1 (chunked): Build + post Organizations without partOf, collecting SOR→FHIR ID map
  Pass 2 (SQL patch): Update partOf directly via jsonb_set on the database
"""

import gc
import uuid
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.organization import Organization
from utils.fhir.identifier_systems import SYSTEM_SOR, SYSTEM_SOR_ENTITY_TYPE

CHUNK_SIZE = 5000


def _build_organization(row: dict[str, Any]) -> Organization:
    """Build a single FHIR Organization from a SOR staging row (without partOf)."""
    sor_id = row["SORId"]
    name = row.get("EntityName") or f"Organization {sor_id}"
    active = row.get("Status") == "Active"

    identifiers = [Identifier(system=SYSTEM_SOR, value=str(sor_id))]

    org_type = None
    entity_type_name = row.get("EntityTypeName")
    if entity_type_name:
        org_type = [
            CodeableConcept(
                coding=[Coding(system=SYSTEM_SOR_ENTITY_TYPE, code=entity_type_name)],
                text=entity_type_name,
            )
        ]

    return Organization(
        identifier=identifiers,
        name=name,
        active=active,
        type=org_type,
    )


def _post_chunk_direct(
    orgs: list[Organization],
    fhir_database: FHIRDatabaseResource,
) -> tuple[int, int]:
    """Post a chunk of Organizations via batch_upsert_resources. Returns (successes, failures)."""
    successes, failures, _ = fhir_database.batch_upsert_resources(
        resources=orgs,
        resource_type="Organization",
    )
    return successes, failures


def _patch_partof_batch(
    fhir_database: FHIRDatabaseResource,
    updates: list[tuple[str, str]],
) -> int:
    """
    Patch partOf field directly in the database using jsonb_set.

    Aidbox stores references internally as {id, resourceType} (not the FHIR
    "reference" string format). This writes in that native format.

    Args:
        fhir_database: Database resource
        updates: List of (org_fhir_id, parent_fhir_id) tuples

    Returns:
        Number of rows updated
    """
    if not updates:
        return 0

    sql = """
        UPDATE organization
        SET resource = jsonb_set(
            resource,
            '{partOf}',
            jsonb_build_object('id', %s::text, 'resourceType', 'Organization')
        )
        WHERE id = %s::text
    """

    rows_updated = 0
    with fhir_database.get_connection() as conn:
        with conn.cursor() as cur:
            for org_id, parent_id in updates:
                cur.execute(sql, (parent_id, org_id))
                rows_updated += cur.rowcount
        conn.commit()

    return rows_updated


@dg.asset(
    name="fhir_organization_sor",
    deps=["stg_organisation_sor"],
    group_name="fhir_organization_sor",
    compute_kind="fhir",
    tags={"domain": "fhir", "layer": "dsa", "source": "sor"},
    description=(
        "Transform SOR organizations into FHIR Organization resources and post to FHIR DB. "
        "Two-pass: (1) chunked post without partOf, (2) SQL patch for partOf hierarchy."
    ),
)
def fhir_organization_sor(
    context: dg.AssetExecutionContext,
    stg_organisation_sor: list[dict[str, Any]],
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    total = len(stg_organisation_sor)
    context.log.info(f"Processing {total} SOR organizations")

    # Collect SOR ID → FHIR ID mapping and parent relationships
    sor_to_fhir_id: dict[str, str] = {}
    parent_map: dict[str, str] = {}  # child SOR ID → parent SOR ID

    total_successes = 0
    total_failures = 0

    # ========================================================================
    # PASS 1: Build and post Organizations in chunks (without partOf)
    # ========================================================================
    context.log.info(f"PASS 1: Posting {total} Organizations in chunks of {CHUNK_SIZE}")

    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk_rows = stg_organisation_sor[chunk_start : chunk_start + CHUNK_SIZE]
        chunk_num = chunk_start // CHUNK_SIZE + 1

        # Build Organization objects (no partOf yet)
        orgs: list[Organization] = []
        chunk_sor_ids: list[str] = []

        for row in chunk_rows:
            sor_id = row["SORId"]
            org = _build_organization(row)
            orgs.append(org)
            chunk_sor_ids.append(str(sor_id))

            parent_sor = row.get("ParentSorId")
            if parent_sor:
                parent_map[str(sor_id)] = str(parent_sor)

        # Resolve existing FHIR IDs
        orgs = prepare_resources_for_upsert(
            resources=orgs,
            fhir_database=fhir_database,
            resource_type="Organization",
            preferred_id_systems=[SYSTEM_SOR],
        )

        # Assign new UUIDs where needed
        for org in orgs:
            if not org.id:
                org.id = str(uuid.uuid4())

        # Post chunk
        successes, failures = _post_chunk_direct(orgs, fhir_database)
        total_successes += successes
        total_failures += failures

        # Collect SOR → FHIR ID mapping
        for idx, org in enumerate(orgs):
            sor_to_fhir_id[chunk_sor_ids[idx]] = org.id

        context.log.info(
            f"  Chunk {chunk_num}: posted {successes}, failed {failures} "
            f"(cumulative: {total_successes}/{total})"
        )

        # Free memory
        del orgs, chunk_sor_ids, chunk_rows
        gc.collect()

    if total_failures > 0:
        raise dg.Failure(
            description=f"Pass 1 failed: {total_failures} of {total} Organizations failed to post"
        )

    # ========================================================================
    # PASS 2: Patch partOf references via SQL
    # ========================================================================
    if parent_map:
        context.log.info(f"PASS 2: Patching partOf for {len(parent_map)} organizations via SQL")

        updates: list[tuple[str, str]] = []
        skipped = 0
        for child_sor, parent_sor in parent_map.items():
            child_fhir_id = sor_to_fhir_id.get(child_sor)
            parent_fhir_id = sor_to_fhir_id.get(parent_sor)
            if child_fhir_id and parent_fhir_id:
                updates.append((child_fhir_id, parent_fhir_id))
            else:
                skipped += 1

        context.log.info(
            f"  {len(updates)} partOf updates to apply, {skipped} skipped (parent not in dataset)"
        )

        patched = 0
        for chunk_start in range(0, len(updates), CHUNK_SIZE):
            chunk = updates[chunk_start : chunk_start + CHUNK_SIZE]
            patched += _patch_partof_batch(fhir_database, chunk)

        context.log.info(f"  Patched {patched} partOf references")
    else:
        context.log.info("PASS 2: No parent relationships to patch")

    # Free the large maps
    del sor_to_fhir_id, parent_map
    gc.collect()

    return dg.MaterializeResult(
        metadata={
            "total_organizations": total,
            "posted_count": total_successes,
            "failed_count": total_failures,
        }
    )
