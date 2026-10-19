# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Location (sengeenhed) asset using DFM.

Reads arc.ifdb_sengeenhed, posts Location resources identified by
SYSTEM_IFDB_SENGEENHED. managingOrganization is resolved through the
ADMENHED_KODE → SOR dictionary harvested from the archive; a miss omits
the reference and logs a warning. physicalType is left unset.

Own DFM checkpoint (fhir_server / Location_WardUnit). Not a producer of
fhir.Location — that grain is SOR-only.
"""

from datetime import UTC, datetime
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import is_true_flag, normalize_oracle_id
from DSA.FHIR.utils.ifdb.ward_unit_bed import admenhed_dictionary_sql, build_admenhed_extension
from DSA.FHIR.utils.reference_resolver import (
    apply_resolved_references_to_resources,
    batch_resolve_references,
)
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location as FhirLocation
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_IFDB_SENGEENHED, SYSTEM_SOR
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000


def _load_admenhed_sor_dictionary(
    mssql: MSSQLResource,
    counters: dict[str, int] | None = None,
) -> dict[str, str]:
    """ADMENHED_KODE → unique SOR, harvested from the bed-stay archive.

    A code the source has used with more than one SOR is dropped rather than
    resolved: picking "the newest" would need an ordering key, and the only
    candidate is a registration timestamp stored as NVARCHAR in two formats.
    The number of dropped codes goes to the asset's metadata, since a rising
    count means the source has reorganised and the coupling needs a real
    ConceptMap rather than a harvest.
    """
    collected: dict[str, set[str]] = {}
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(admenhed_dictionary_sql())
            for kode, sor in cursor.fetchall():
                if kode is None or sor is None:
                    continue
                key = str(kode).strip()
                sor_value = normalize_oracle_id(sor)
                if not key or not sor_value:
                    continue
                collected.setdefault(key, set()).add(sor_value)
        finally:
            cursor.close()
    if counters is not None:
        counters["ambiguous_codes"] = sum(1 for sors in collected.values() if len(sors) > 1)
    return {kode: next(iter(sors)) for kode, sors in collected.items() if len(sors) == 1}


def _transform_ward_unit_to_fhir(
    rows: list[dict[str, Any]],
    admenhed_sor: dict[str, str],
    context: dg.AssetExecutionContext,
    counters: dict[str, int] | None = None,
) -> list[FhirLocation]:
    """Transform SENGEENHED archive rows to Location resources.

    ``counters`` accumulates the organisation-coupling coverage across batches
    (``with_code`` / ``dictionary_misses``): the coupling is derived from a
    harvested dictionary, not from an authoritative source, so its hit rate is
    a number the run has to report rather than hide in a warning.
    """
    locations: list[FhirLocation] = []
    skipped_no_id = 0
    dictionary_misses = 0

    for row in rows:
        objekt_id = normalize_oracle_id(row.get("OBJEKT_ID"))
        if not objekt_id:
            skipped_no_id += 1
            continue

        deleted = (
            1 if (is_true_flag(row.get("SLETTET")) or is_true_flag(row.get("dw_is_deleted"))) else 0
        )
        name = str(row["ADMENHED_NAVN"]).strip() if row.get("ADMENHED_NAVN") else None

        kode = row.get("ADMENHED_KODE")
        kode_key = str(kode).strip() if kode is not None else ""
        sor_value = admenhed_sor.get(kode_key) if kode_key else None
        managing_org = None
        if sor_value:
            managing_org = Reference(
                type="Organization",
                identifier=Identifier(system=SYSTEM_SOR, value=sor_value),
            )
        elif kode_key:
            dictionary_misses += 1

        extensions = [build_is_deleted_extension(deleted)]
        admenhed_ext = build_admenhed_extension(
            row.get("ADMENHED_KODE"),
            row.get("ADMENHED_NAVN"),
            row.get("ADMENHED_ORGKODE"),
        )
        if admenhed_ext is not None:
            extensions.append(admenhed_ext)

        locations.append(
            FhirLocation(
                identifier=[Identifier(system=SYSTEM_IFDB_SENGEENHED, value=objekt_id)],
                name=name or None,
                status="inactive" if deleted else "active",
                managingOrganization=managing_org,
                extension=extensions,
            )
        )

    if skipped_no_id:
        context.log.warning(f"Skipped {skipped_no_id} sengeenhed rows without OBJEKT_ID")
    if dictionary_misses:
        context.log.warning(
            f"Omitted managingOrganization for {dictionary_misses} sengeenhed "
            f"Locations — ADMENHED_KODE not in archive dictionary"
        )

    if counters is not None:
        counters["dictionary_misses"] = counters.get("dictionary_misses", 0) + dictionary_misses
        counters["resolved_organizations"] = counters.get("resolved_organizations", 0) + sum(
            1 for location in locations if location.managingOrganization is not None
        )

    return locations


def _post_ward_unit_to_fhir(
    context: dg.AssetExecutionContext,
    locations: list[FhirLocation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> dg.MaterializeResult:
    """Resolve managingOrganization, upsert sengeenhed Locations."""
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

    org_identifiers: dict[str, str] = {}
    for loc in locations:
        if loc.managingOrganization and getattr(loc.managingOrganization, "identifier", None):
            org_identifiers[str(loc.managingOrganization.identifier.value)] = ""

    resolved_orgs: dict[str, str] = {}
    if org_identifiers:
        resolved_orgs = batch_resolve_references(
            identifiers=org_identifiers,
            resource_type="Organization",
            identifier_system=SYSTEM_SOR,
            fhir_database=fhir_database,
        )
        if resolved_orgs:
            org_refs_updated = apply_resolved_references_to_resources(
                locations, resolved_orgs, "Organization"
            )
            logger.info(f"Applied {org_refs_updated} Organization reference IDs")

    unresolved_count = 0
    for loc in locations:
        if loc.managingOrganization and getattr(loc.managingOrganization, "identifier", None):
            org_id = str(loc.managingOrganization.identifier.value)
            if org_id not in resolved_orgs:
                loc.managingOrganization = None
                unresolved_count += 1

    if unresolved_count:
        logger.warning(
            f"Removed {unresolved_count} unresolved Organization references "
            f"from sengeenhed Locations"
        )

    locations_before = len([loc for loc in locations if loc.id is not None])
    locations = prepare_resources_for_upsert(
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        preferred_id_systems=[SYSTEM_IFDB_SENGEENHED],
        logger=logger,
    )
    locations_after = len([loc for loc in locations if loc.id is not None])
    resolved_count = locations_after - locations_before
    new_count = len(locations) - locations_after
    logger.info(f"ID resolution: {resolved_count} existing sengeenhed Locations, {new_count} new")

    when = datetime.now(UTC)
    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=locations,
        resource_type="Location",
    )
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(locations)} sengeenhed Location "
                f"resources. Failed IDs: {failed_ids[:10]}"
            )
        )

    context.log.info(
        f"Summary[{partition_key}]: {extracted_count} ext -> {transformed_count} trans "
        f"-> {successes} posted ({failures} failed)"
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


@dg.asset(
    name="fhir_server_location_ward_unit_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    # Korrektheden hviler paa begge: sengeenhederne selv, og sengeopholdene
    # ADMENHED_KODE -> SOR-koblingen hoestes fra.
    deps=[
        "arc_sengeenhed_ifdb",
        "arc_encounter_bed_space_history_ifdb",
    ],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch sengeenhed from arc.ifdb_sengeenhed, transform to FHIR Location, "
        "and post to FHIR server. Checkpoint owner for fhir_server.Location_WardUnit."
    ),
)
def fhir_server_location_ward_unit_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for sengeenhed Locations (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Location_WardUnit",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        coverage: dict[str, int] = {}
        admenhed_sor = _load_admenhed_sor_dictionary(mssql_target_staging, counters=coverage)
        context.log.info(
            f"ADMENHED dictionary loaded: {len(admenhed_sor)} unique codes "
            f"({coverage.get('ambiguous_codes', 0)} dropped as ambiguous)"
        )

        # managingOrganization udledes af ADMENHED_KODE -> SOR-ordbogen, som
        # aendrer sig uafhaengigt af sengeenhedernes eget delta. Et checkpoint-
        # filter ville derfor fastholde en enhed uden kobling for evigt, hvis
        # ordbogen faar hendes kode EFTER hendes sidste aendring (279 raekker,
        # der stort set aldrig aendrer sig). Derfor laeses tabellen altid fuldt,
        # uanset mode -- checkpointet foeres videre, men filtrerer ikke.
        query = build_fhir_archive_query(
            "arc.ifdb_sengeenhed",
            None,
            config.model_copy(update={"mode": "full_load"}),
            include_deleted=True,
        )
        context.log.info(
            f"Querying arc.ifdb_sengeenhed (mode={config.mode}, laest fuldt: "
            "koblingen til SOR er udledt af en ordbog uden for deltaet)"
        )

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

                    locations = _transform_ward_unit_to_fhir(
                        batch, admenhed_sor, context, counters=coverage
                    )
                    if locations:
                        _post_ward_unit_to_fhir(
                            context=context,
                            locations=locations,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                        )
                        total_posted += len(locations)
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Location_WardUnit", config, total_fetched
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )
        resolved_orgs = coverage.get("resolved_organizations", 0)
        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "dictionary_size": len(admenhed_sor),
                # Daekningsgraden for den udledte organisationskobling. Falder
                # den, er ordbogen blevet flertydig eller er holdt op med at
                # daekke et kodeomraade.
                "managing_organization_resolved": resolved_orgs,
                "managing_organization_missing": coverage.get("dictionary_misses", 0),
                "admenhed_codes_ambiguous": coverage.get("ambiguous_codes", 0),
                "managing_organization_coverage_pct": (
                    round(100.0 * resolved_orgs / total_fetched, 1) if total_fetched else 0.0
                ),
                "mode": config.mode,
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
