# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Location asset using DFM (Data Flow Management).

Single-asset pattern: fetches distinct location master data from
arc.location_ifdb in batches, transforms to FHIR, and posts to the FHIR server.

Location is typically small (distinct SOR codes), so batching here is mainly
for consistency with the other DFM assets.

This module also owns the shared Location transform/post helpers, which lived
in Location_IFDB.py before the partitioned assets were removed.

Locations are identified solely by SOR code (LOCATION_SORCODE) from
IFDB.LOCATION, which is the same SOR registry used by BOM_IFDB.SORENHED. No
name is stored on the Location resource — source systems use different names
for the same SOR code, so the official name is obtained via the
managingOrganization reference instead.

Rows without a LOCATION_SORCODE are skipped (no SOR identity = cannot be
referenced). Both IFDB.LOCATION and BOM_IFDB.ORGENHED feed Location resources
keyed by SYSTEM_SOR. Because they share the same identifier namespace, the SOR
pipeline (Location_SOR_IFDB.py) declares an explicit dependency on this
pipeline so that it always runs second and its richer data (partOf, type,
validity period) wins on overlapping SOR codes.

No Dagster partitions — checkpoint tracking replaces partition boundaries.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.reference_resolver import (
    apply_resolved_references_to_resources,
    batch_resolve_references,
)
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location as FhirLocation
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_SOR

BATCH_SIZE = 10_000

_LOCATION_SELECT = "SELECT DISTINCT [LOCATION_SORCODE]"


# ============================================================================
# Shared Helper Functions
# ============================================================================


def _transform_locations_to_fhir(
    rows: list[dict[str, Any]], context: dg.AssetExecutionContext
) -> list[FhirLocation]:
    """
    Transform location rows to FHIR Location resources.

    Uses LOCATION_SORCODE (normalised via normalize_oracle_id) as the sole SYSTEM_SOR
    identifier. No name is set — the official name comes from managingOrganization.
    Rows without a LOCATION_SORCODE are skipped and logged.
    """
    locations: list[FhirLocation] = []
    skipped_no_sor = 0

    for row_dict in rows:
        raw_sorcode = row_dict.get("LOCATION_SORCODE")

        if not raw_sorcode:
            skipped_no_sor += 1
            continue

        sor_value = normalize_oracle_id(raw_sorcode)

        managing_org_ref = Reference(
            type="Organization",
            identifier=Identifier(system=SYSTEM_SOR, value=sor_value),
        )

        loc = FhirLocation(
            identifier=[
                Identifier(system=SYSTEM_SOR, value=sor_value),
            ],
            managingOrganization=managing_org_ref,
            status="active",
        )
        locations.append(loc)

    if skipped_no_sor:
        context.log.warning(f"Skipped {skipped_no_sor} IFDB Location rows without LOCATION_SORCODE")

    return locations


def _post_locations_to_fhir(
    context: dg.AssetExecutionContext,
    locations: list[FhirLocation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> dg.MaterializeResult:
    """
    Post Location resources to FHIR database using direct DB insertion.

    Uses executemany() for 4-7x faster performance than FHIR API.
    """
    logger = dg.get_dagster_logger()
    partition_key = context.partition_key if context.has_partition_key else "N/A"
    transformed_count = len(locations)

    if not locations:
        context.log.info(
            f"Summary[{partition_key}]: {extracted_count} ext -> 0 trans -> 0 posted (0 failed)"
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": dg.MetadataValue.int(0),
                "failed_count": dg.MetadataValue.int(0),
                "source": dg.MetadataValue.text("ifdb_oracle"),
                "partition_key": dg.MetadataValue.text(partition_key),
                "method": dg.MetadataValue.text("direct_postgres"),
            }
        )

    # Resolve Organization references for managingOrganization using memory cache
    org_identifiers: dict[str, str] = {}
    for loc in locations:
        if loc.managingOrganization and getattr(loc.managingOrganization, "identifier", None):
            org_id = str(loc.managingOrganization.identifier.value)
            org_identifiers[org_id] = ""

    resolved_orgs: dict[str, str] = {}
    if org_identifiers:
        resolved_orgs = batch_resolve_references(
            identifiers=org_identifiers,
            resource_type="Organization",
            identifier_system=SYSTEM_SOR,
            fhir_database=fhir_database,
        )

        # Apply resolved FHIR IDs for UI navigation
        if resolved_orgs:
            org_refs_updated = apply_resolved_references_to_resources(
                locations, resolved_orgs, "Organization"
            )
            logger.info(f"Applied {org_refs_updated} Organization reference IDs for UI navigation")

    # Remove unresolved Organization references to avoid validation errors
    # Organization references are optional - if the Organization doesn't exist yet,
    # we remove the reference rather than failing
    unresolved_count = 0
    for loc in locations:
        if loc.managingOrganization and getattr(loc.managingOrganization, "identifier", None):
            org_id = str(loc.managingOrganization.identifier.value)
            if org_id not in resolved_orgs:
                loc.managingOrganization = None
                unresolved_count += 1

    if unresolved_count > 0:
        logger.warning(
            f"Removed {unresolved_count} unresolved Organization references from Locations "
            f"(Organizations not found in FHIR server)"
        )

    # Resolve existing FHIR IDs before posting to enable proper upsert behavior.
    # This ensures that locations already in the database get their existing UUID
    # attached, so ON CONFLICT (id) triggers an UPDATE instead of INSERT.
    locations_before_resolve = len([loc for loc in locations if loc.id is not None])
    locations = prepare_resources_for_upsert(
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        preferred_id_systems=[SYSTEM_SOR],
        logger=logger,
    )
    locations_after_resolve = len([loc for loc in locations if loc.id is not None])
    resolved_count = locations_after_resolve - locations_before_resolve
    new_count = len(locations) - locations_after_resolve

    logger.info(
        f"ID resolution: {resolved_count} existing locations found, {new_count} new locations"
    )

    # Direct database insertion
    when = datetime.now(UTC)

    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=locations,
        resource_type="Location",
    )

    # Locations are important for Encounter references, so we use strict mode
    if failures > 0:
        raise dg.Failure(
            description=f"Failed to post {failures} of {len(locations)} Location resources. "
            f"Failed IDs: {failed_ids[:10]}"
        )

    # Log concise summary
    context.log.info(
        f"Summary[{partition_key}]: {extracted_count} ext -> {transformed_count} trans -> {successes} posted ({failures} failed)"
    )

    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(successes),
            "failed_count": dg.MetadataValue.int(failures),
            "failed_ids": dg.MetadataValue.json(failed_ids),
            "resolved_count": dg.MetadataValue.int(resolved_count),
            "new_count": dg.MetadataValue.int(new_count),
            "partition_key": dg.MetadataValue.text(partition_key),
            "source": dg.MetadataValue.text("ifdb_oracle"),
            "method": dg.MetadataValue.text("direct_postgres"),
            "decorated_at": dg.MetadataValue.text(when.isoformat()),
        }
    )


# ---------------------------------------------------------------------------
# Single DFM Asset — fetch, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_location_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=["arc_location_ifdb"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch distinct locations from arc.location_ifdb, transform to FHIR, "
        "and post to FHIR server. Memory-bounded streaming. "
        "Checkpoint owner for fhir_server.Location."
    ),
)
def fhir_server_location_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for Location resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Location",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_fhir_archive_query(
            "arc.location_ifdb",
            run_ctx.checkpoint,
            config,
            select_clause=_LOCATION_SELECT,
        )
        query += " AND [LOCATION_SORCODE] IS NOT NULL"
        context.log.info(f"Querying arc.location_ifdb (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)

                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} locations "
                        f"(total so far: {total_fetched})"
                    )

                    locations = _transform_locations_to_fhir(batch, context)

                    if locations:
                        _post_locations_to_fhir(
                            context=context,
                            locations=locations,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                        )
                        total_posted += len(locations)

            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted " f"in {batch_num} batches"
        )

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint and total_fetched > 0:
            with mssql_target_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row else None
                finally:
                    cursor.close()

            if new_checkpoint:
                dfm.set_checkpoint(
                    "fhir_server", "Location", value=new_checkpoint, mode=config.mode
                )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
