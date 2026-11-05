# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Location (bed) asset using DFM.

Distinct BED_ID from arc.ifdb_encounter_bed_space_history. Latest
PERIOD_START_REGISTRATION wins for BED / ROOM / LOCATION_SORCODE / LOCATION_CODE.
physicalType = bd. partOf via LOCATION_SORCODE / SYSTEM_SOR; omitted on NULL
or unresolved. ROOM and LOCATION_CODE (the ward-unit code, joinable to
WardUnit.admin_unit_code) are extensions, not Locations.

Own DFM checkpoint (fhir_server / Location_Bed). Not a producer of
fhir.Location — that grain is SOR-only.
"""

from collections.abc import Iterator
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
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.ifdb.ward_unit_bed import (
    bed_physical_type,
    bed_row_is_live,
    fold_latest_bed_rows,
)
from DSA.FHIR.utils.reference_resolver import batch_resolve_references
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location as FhirLocation
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import EXTENSION_BED_ROOM, EXTENSION_BED_SPACE_WARD_UNIT_CODE
from utils.fhir.identifier_systems import SYSTEM_IFDB_BED, SYSTEM_SOR
from utils.fhir.is_deleted_extension import build_is_deleted_extension

BATCH_SIZE = 10_000

# dw_is_deleted er med i udvalget og filtreres IKKE fra: arkivets slettesignal
# er den eneste rute til dw_is_deleted = 1 paa fhir.Bed. Filtrerede vi det bort,
# ville en pensioneret seng blot falde ud af forespoergslen og staa som levende i
# fhir.Bed for altid. fold_latest_bed_rows lader en levende raekke vinde, saa
# sengen kun markeres slettet, naar ingen levende historikraekke er tilbage.
_BED_SELECT = (
    "SELECT [BED_ID], [BED], [ROOM], [LOCATION_SORCODE], [LOCATION_CODE], "
    "[PERIOD_START_REGISTRATION], [dw_is_deleted] "
    "FROM [arc].[ifdb_encounter_bed_space_history] "
    "WHERE [BED_ID] IS NOT NULL"
)


def _transform_beds_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[FhirLocation]:
    """Transform deduplicated bed rows to Location resources."""
    locations: list[FhirLocation] = []
    skipped_no_id = 0

    for row in rows:
        bed_id = normalize_oracle_id(row.get("BED_ID"))
        if not bed_id:
            skipped_no_id += 1
            continue

        name = str(row["BED"]).strip() if row.get("BED") else None
        room = str(row["ROOM"]).strip() if row.get("ROOM") else None
        raw_sor = row.get("LOCATION_SORCODE")
        sor_value = normalize_oracle_id(raw_sor) if raw_sor else None

        deleted = 0 if bed_row_is_live(row) else 1
        extensions = [build_is_deleted_extension(deleted)]
        if room:
            extensions.append(Extension(url=EXTENSION_BED_ROOM, valueString=room))
        ward_code = str(row["LOCATION_CODE"]).strip() if row.get("LOCATION_CODE") else None
        if ward_code:
            extensions.append(
                Extension(url=EXTENSION_BED_SPACE_WARD_UNIT_CODE, valueString=ward_code)
            )

        part_of = None
        if sor_value:
            part_of = Reference(
                type="Location",
                identifier=Identifier(system=SYSTEM_SOR, value=sor_value),
            )

        locations.append(
            FhirLocation(
                identifier=[Identifier(system=SYSTEM_IFDB_BED, value=bed_id)],
                name=name or None,
                status="active",
                physicalType=bed_physical_type(),
                partOf=part_of,
                extension=extensions,
            )
        )

    if skipped_no_id:
        context.log.warning(f"Skipped {skipped_no_id} bed rows without BED_ID")
    return locations


def _resolve_part_of(
    locations: list[FhirLocation],
    fhir_database: FHIRDatabaseResource,
    context: dg.AssetExecutionContext,
) -> None:
    """Resolve partOf department Locations; omit unresolved / NULL SOR."""
    identifiers: dict[str, str] = {}
    for loc in locations:
        if loc.partOf and getattr(loc.partOf, "identifier", None):
            identifiers[str(loc.partOf.identifier.value)] = ""

    resolved: dict[str, str] = {}
    if identifiers:
        resolved = batch_resolve_references(
            identifiers=identifiers,
            resource_type="Location",
            identifier_system=SYSTEM_SOR,
            fhir_database=fhir_database,
        )

    omitted = 0
    for loc in locations:
        if not loc.partOf:
            continue
        if loc.partOf.identifier and str(loc.partOf.identifier.value) in resolved:
            loc.partOf.id = resolved[str(loc.partOf.identifier.value)]
            loc.partOf.reference = f"Location/{loc.partOf.id}"
        else:
            loc.partOf = None
            omitted += 1

    if omitted:
        context.log.warning(f"Omitted partOf on {omitted} bed Locations (NULL or unresolved SOR)")


def _post_beds_to_fhir(
    context: dg.AssetExecutionContext,
    locations: list[FhirLocation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> int:
    logger = dg.get_dagster_logger()
    if not locations:
        return 0

    _resolve_part_of(locations, fhir_database, context)

    locations = prepare_resources_for_upsert(
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        preferred_id_systems=[SYSTEM_IFDB_BED],
        logger=logger,
    )

    successes, failures, failed_ids = fhir_database.batch_upsert_resources(
        resources=locations,
        resource_type="Location",
    )
    if failures > 0:
        raise dg.Failure(
            description=(
                f"Failed to post {failures} of {len(locations)} bed Location "
                f"resources. Failed IDs: {failed_ids[:10]}"
            )
        )
    context.log.info(f"Posted {successes} bed Locations (extracted {extracted_count})")
    return successes


def _iter_bed_attribute_rows(
    mssql: MSSQLResource,
    checkpoint: str | None,
    config: RunModeConfig,
) -> Iterator[dict[str, Any]]:
    """Stream the attribute rows needed to compute latest-wins per BED_ID.

    Full load reads every row, deleted ones included — the archive flag is the
    bed's delete signal. Delta / period / ids first find the BED_IDs that moved
    in the window, then reload *all* rows for those IDs so an older period
    cannot overwrite a newer name.

    A generator, not a list: the unwindowed read covers the whole bed history
    (1,3 mio. rows, and a first delta run has no checkpoint either), which
    fold_latest_bed_rows collapses to one row per bed as it goes. Collecting
    the input first would put gigabytes in a process that shares the executor
    with every other DFM asset.
    """
    if config.mode == "full_load" or (config.mode == "delta" and not checkpoint):
        query = _BED_SELECT
    else:
        window = build_fhir_archive_query(
            "arc.ifdb_encounter_bed_space_history",
            checkpoint,
            config,
            # include_deleted: en netop soft-slettet historikraekke skal drive
            # deltaet, ellers opdateres sengens flag aldrig.
            include_deleted=True,
            select_clause="SELECT DISTINCT [BED_ID]",
        )
        query = f"{_BED_SELECT} AND [BED_ID] IN ({window})"

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            while True:
                raw_rows = cursor.fetchmany(BATCH_SIZE)
                if not raw_rows:
                    break
                for row in raw_rows:
                    yield dict(zip(columns, row, strict=False))
        finally:
            cursor.close()


@dg.asset(
    name="fhir_server_location_bed_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_encounter_bed_space_history_ifdb",
        "fhir_server_location_dfm",
        "fhir_server_location_bom_ifdb_dfm",
    ],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Deduplicate BED_ID from arc.ifdb_encounter_bed_space_history into "
        "Location resources (physicalType=bd). Checkpoint owner for "
        "fhir_server.Location_Bed."
    ),
)
def fhir_server_location_bed_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Latest-wins bed Locations from encounter bed-space history (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Location_Bed",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        scanned = 0

        def _counted() -> Iterator[dict[str, Any]]:
            nonlocal scanned
            for row in _iter_bed_attribute_rows(mssql_target_staging, run_ctx.checkpoint, config):
                scanned += 1
                yield row

        latest_rows = fold_latest_bed_rows(_counted())
        context.log.info(
            f"Bed dedup: {scanned} attribute rows -> {len(latest_rows)} distinct BED_ID"
        )

        locations = _transform_beds_to_fhir(latest_rows, context)
        posted = _post_beds_to_fhir(
            context=context,
            locations=locations,
            fhir_database=fhir_database,
            extracted_count=scanned,
        )

        new_checkpoint = advance_fhir_checkpoint(dfm, run_ctx, "Location_Bed", config, scanned)

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=posted,
        )
        return dg.MaterializeResult(
            metadata={
                "posted_count": posted,
                "fetched_count": scanned,
                "distinct_bed_id": len(latest_rows),
                "mode": config.mode,
                "decorated_at": dg.MetadataValue.text(datetime.now(UTC).isoformat()),
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
