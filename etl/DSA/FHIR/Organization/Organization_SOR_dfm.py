# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Organization asset built from the SOR CodeSystem (DFM).

Organizations are derived from codesystem.SORConcept / SORProperties /
SORHierarchy (maintained by cs_sor from arc.sor_register) instead of a
separate SOR download — one shared extract for CodeSystem and Organization.

DFM single-asset pattern (checkpoint owner fhir_server.Organization):
- delta:     only codes whose codesystem rows changed since the checkpoint
             (cs_sor merges with update_only_on_change, and touches concepts
             whose properties/hierarchy rows were deleted, so
             dw_timestamp_update is a reliable change signal)
- full_load: every code in the codesystem (e.g. after an all_historical
             backfill of arc.sor_register)

Memory-efficient two-pass posting strategy (unchanged from the previous
stg-based asset):
  Pass 1 (chunked): Build + post Organizations without partOf, collecting the
                    SOR→FHIR ID map
  Pass 2 (SQL patch): Update partOf directly via jsonb_set on the database;
                    parents outside the run window are resolved from the FHIR
                    database by their SOR identifier
"""

import gc
import uuid
from datetime import datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.organization import Organization
from utils.fhir.identifier_systems import SYSTEM_SOR, SYSTEM_SOR_ENTITY_TYPE

CHUNK_SIZE = 5000


def parse_sor_date(date_str: str | None) -> datetime | None:
    """Parse a SOR date string (YYYY-MM-DD[ HH:MM:SS]) to datetime."""
    if not date_str or not str(date_str).strip():
        return None
    value = str(date_str).strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def determine_status(closing_date: str | None, now: datetime | None = None) -> str:
    """Entity status from the closingDate property.

    Rules per the SOR documentation: undefined/empty or future closing date
    means "active"; a closing date in the past means "Closed".
    """
    parsed = parse_sor_date(closing_date)
    if parsed is None:
        return "active"
    return "active" if parsed > (now or datetime.now()) else "Closed"


def build_organization_from_codesystem_row(row: dict[str, Any]) -> Organization:
    """Build a FHIR Organization (without partOf) from a pivoted codesystem row.

    Expects keys: code, display, entity_type, closing_date.
    """
    code = str(row["code"])
    name = row.get("display") or f"Organization {code}"
    active = determine_status(row.get("closing_date")) == "active"

    identifiers = [Identifier(system=SYSTEM_SOR, value=code)]

    org_type = None
    entity_type = row.get("entity_type")
    if entity_type:
        org_type = [
            CodeableConcept(
                coding=[Coding(system=SYSTEM_SOR_ENTITY_TYPE, code=entity_type)],
                text=entity_type,
            )
        ]

    return Organization(
        identifier=identifiers,
        name=name,
        active=active,
        type=org_type,
    )


def build_changed_organization_query(checkpoint: str | None, upper_bound: str, mode: str) -> str:
    """Build the pivoted codesystem query for the run window.

    delta: codes with any SORConcept/SORProperties/SORHierarchy row changed in
    (checkpoint, upper_bound]. A NULL checkpoint (first run) selects everything
    up to the bound. full_load ignores the window entirely.
    """
    if mode == "full_load":
        code_filter = ""
    else:
        lower = f"AND [dw_timestamp_update] > '{checkpoint}'" if checkpoint else ""
        code_filter = f"""
        INNER JOIN (
            SELECT DISTINCT [code] FROM (
                SELECT [code], [dw_timestamp_update] FROM [codesystem].[SORConcept]
                UNION ALL
                SELECT [code], [dw_timestamp_update] FROM [codesystem].[SORProperties]
                UNION ALL
                SELECT [child_code] AS [code], [dw_timestamp_update]
                FROM [codesystem].[SORHierarchy]
            ) changed
            WHERE [dw_timestamp_update] <= '{upper_bound}' {lower}
        ) w ON w.[code] = c.[code]"""

    return f"""
        SELECT c.[code]           AS [code],
               c.[display]        AS [display],
               p_type.[value]     AS [entity_type],
               p_close.[value]    AS [closing_date],
               h.[parent_code]    AS [parent_code]
        FROM [codesystem].[SORConcept] c{code_filter}
        LEFT JOIN [codesystem].[SORProperties] p_type
               ON p_type.[code] = c.[code] AND p_type.[property_code] = 'entityType'
        LEFT JOIN [codesystem].[SORProperties] p_close
               ON p_close.[code] = c.[code] AND p_close.[property_code] = 'closingDate'
        LEFT JOIN [codesystem].[SORHierarchy] h
               ON h.[child_code] = c.[code]
        ORDER BY c.[code]
    """


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


def _resolve_parent_ids_outside_window(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
    parent_codes: list[str],
) -> dict[str, str]:
    """Resolve FHIR ids for parent SOR codes that were not part of this run's
    window (typically unchanged parents posted in earlier runs)."""
    if not parent_codes:
        return {}

    stubs = [
        Organization(identifier=[Identifier(system=SYSTEM_SOR, value=code)])
        for code in parent_codes
    ]
    index_to_id = fhir_database.resolve_resource_ids(
        resources=stubs,
        resource_type="Organization",
        preferred_systems=[SYSTEM_SOR],
    )
    resolved = {parent_codes[idx]: fhir_id for idx, fhir_id in index_to_id.items() if fhir_id}
    context.log.info(
        f"  Resolved {len(resolved)}/{len(parent_codes)} out-of-window parents "
        f"from the FHIR database"
    )
    return resolved


@dg.asset(
    name="fhir_organization_sor",
    deps=["cs_sor"],
    group_name="fhir_organization_sor",
    compute_kind="fhir",
    tags={"domain": "fhir", "layer": "dsa", "source": "sor"},
    description=(
        "Build FHIR Organizations from codesystem.SOR* (shared SOR source) and post "
        "to the FHIR DB. DFM checkpoint owner fhir_server.Organization — delta posts "
        "only changed codes; full_load reposts the whole codesystem. Two-pass: "
        "(1) chunked post without partOf, (2) SQL patch for partOf hierarchy."
    ),
)
def fhir_organization_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    if config.mode not in ("delta", "full_load"):
        raise dg.Failure(
            description=(
                f"fhir_organization_sor supports modes 'delta' and 'full_load', "
                f"got {config.mode!r}. The codesystem source has no business-date "
                f"or id columns for period/ids runs."
            )
        )

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Organization",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        # Capture the window's upper bound BEFORE reading, so codesystem rows
        # written while we post are not silently skipped by the next delta.
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                upper_bound = cursor.fetchone()[0]
            finally:
                cursor.close()

        query = build_changed_organization_query(run_ctx.checkpoint, upper_bound, config.mode)
        context.log.info(
            f"Querying codesystem.SOR* (mode={config.mode}, "
            f"checkpoint={run_ctx.checkpoint}, upper_bound={upper_bound})"
        )

        sor_to_fhir_id: dict[str, str] = {}
        parent_map: dict[str, str] = {}  # child SOR code → parent SOR code
        total_rows = 0
        total_orgs = 0
        total_successes = 0
        chunk_num = 0

        # ====================================================================
        # PASS 1: Build and post Organizations in chunks (without partOf)
        # ====================================================================
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                pending_last_code: str | None = None
                while True:
                    raw_rows = cursor.fetchmany(CHUNK_SIZE)
                    if not raw_rows:
                        break

                    chunk_num += 1
                    total_rows += len(raw_rows)

                    orgs: list[Organization] = []
                    chunk_codes: list[str] = []
                    for raw in raw_rows:
                        row = dict(zip(columns, raw, strict=False))
                        code = str(row["code"])
                        # The hierarchy LEFT JOIN can only yield duplicate codes
                        # on (transient) multi-parent data; the query is ordered
                        # by code, so duplicates are adjacent — keep the first.
                        if code == pending_last_code:
                            continue
                        pending_last_code = code

                        orgs.append(build_organization_from_codesystem_row(row))
                        chunk_codes.append(code)
                        if row.get("parent_code"):
                            parent_map[code] = str(row["parent_code"])

                    if not orgs:
                        continue

                    # Resolve existing FHIR IDs, assign new UUIDs where needed
                    orgs = prepare_resources_for_upsert(
                        resources=orgs,
                        fhir_database=fhir_database,
                        resource_type="Organization",
                        preferred_id_systems=[SYSTEM_SOR],
                        logger=context.log,
                    )
                    for org in orgs:
                        if not org.id:
                            org.id = str(uuid.uuid4())

                    successes, failures = _post_chunk_direct(orgs, fhir_database)
                    total_successes += successes
                    total_orgs += len(orgs)

                    # Strict mode: never leave failed resources behind an
                    # advanced checkpoint — abort before advancement so the
                    # window is reprocessed next run (idempotent upsert).
                    if failures > 0:
                        raise dg.Failure(
                            description=(
                                f"Chunk {chunk_num}: {failures} of {len(orgs)} "
                                f"Organizations failed to post — checkpoint NOT advanced"
                            )
                        )

                    for idx, org in enumerate(orgs):
                        sor_to_fhir_id[chunk_codes[idx]] = org.id

                    context.log.info(
                        f"  Chunk {chunk_num}: posted {successes} "
                        f"(cumulative: {total_successes})"
                    )

                    del orgs, chunk_codes, raw_rows
                    gc.collect()
            finally:
                cursor.close()

        context.log.info(f"PASS 1 done: {total_orgs} Organizations posted from {total_rows} rows")

        # ====================================================================
        # PASS 2: Patch partOf references via SQL
        # ====================================================================
        patched = 0
        skipped = 0
        if parent_map:
            context.log.info(f"PASS 2: Patching partOf for {len(parent_map)} organizations")

            missing_parents = sorted({p for p in parent_map.values() if p not in sor_to_fhir_id})
            parent_lookup = dict(sor_to_fhir_id)
            parent_lookup.update(
                _resolve_parent_ids_outside_window(context, fhir_database, missing_parents)
            )

            updates: list[tuple[str, str]] = []
            for child_sor, parent_sor in parent_map.items():
                child_fhir_id = sor_to_fhir_id.get(child_sor)
                parent_fhir_id = parent_lookup.get(parent_sor)
                if child_fhir_id and parent_fhir_id:
                    updates.append((child_fhir_id, parent_fhir_id))
                else:
                    skipped += 1

            context.log.info(
                f"  {len(updates)} partOf updates to apply, {skipped} skipped "
                f"(parent not posted/resolvable)"
            )

            for chunk_start in range(0, len(updates), CHUNK_SIZE):
                chunk = updates[chunk_start : chunk_start + CHUNK_SIZE]
                patched += _patch_partof_batch(fhir_database, chunk)

            context.log.info(f"  Patched {patched} partOf references")
        else:
            context.log.info("PASS 2: No parent relationships to patch")

        del sor_to_fhir_id, parent_map
        gc.collect()

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fhir_server", "Organization", value=upper_bound, mode=config.mode)
            new_checkpoint = upper_bound

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_orgs,
        )

        return dg.MaterializeResult(
            metadata={
                "total_organizations": total_orgs,
                "posted_count": total_successes,
                "partof_patched": patched,
                "partof_skipped": skipped,
                "mode": config.mode,
                "checkpoint": new_checkpoint or "",
                "source_tables": "codesystem.SORConcept/SORProperties/SORHierarchy",
            }
        )

    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
