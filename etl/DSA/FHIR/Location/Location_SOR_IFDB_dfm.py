# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Location (SOR) asset using DFM (Data Flow Management).

Single-asset pattern: fetches ORGENHED+SORENHED rows from
arc.bom_ifdb_orgenhed_sor in batches, transforms to SOR-keyed FHIR Location
resources, and posts to the FHIR server.

Parent SOR lookups are read from the archive (arc.bom_ifdb_orgenhed_sor via
MSSQL) instead of Oracle, matching the DFM convention of reading exclusively
from arc tables.

Must run AFTER fhir_server_location_dfm so that overlapping SOR codes get the
richer data from this pipeline (partOf, type, validity period).
"""

from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id, parse_clob_datetime
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location as FhirLocation
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_SOR

BATCH_SIZE = 10_000

LOCATION_SOR_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="managingOrganization",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Managing Organization (SOR-keyed, same SOR code as the Location itself).",
    ),
    ReferenceFieldConfig(
        field_name="partOf",
        target_resource_type="Location",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Parent SOR Location (via ORGENHED.PARENT_ORGENHEDOID).",
    ),
]

SYSTEM_BOM_IFDB_ORG_TYPE = "urn:source:bom_ifdb:org_type"


# =============================================================================
# Helper Functions
# =============================================================================


def _fetch_parent_sor_map_from_arc(
    parent_orgenhed_oids: set[str],
    mssql: MSSQLResource,
) -> dict[str, str]:
    """Look up SORKODE for parent ORGENHED OIDs from arc.bom_ifdb_orgenhed_sor."""
    if not parent_orgenhed_oids:
        return {}
    rows = chunked_in_fetch(
        list(parent_orgenhed_oids),
        table="arc.bom_ifdb_orgenhed_sor",
        columns=["OID", "SORKODE"],
        where_column="OID",
        mssql=mssql,
    )
    parent_to_sor: dict[str, str] = {}
    for row in rows:
        oe_oid = normalize_oracle_id(row.get("OID"))
        sorkode = row.get("SORKODE")
        if oe_oid and sorkode:
            parent_to_sor[oe_oid] = str(sorkode)
    return parent_to_sor


def _transform_locations_to_fhir(
    rows: list[dict[str, Any]],
    parent_sor_map: dict[str, str],
    context: dg.AssetExecutionContext,
) -> list[FhirLocation]:
    """Transform ORGENHED+SORENHED rows into SOR-keyed FHIR Location resources."""
    locations: list[FhirLocation] = []

    for row in rows:
        sorkode = row.get("SORKODE")
        if not sorkode:
            continue

        sor_value = str(sorkode)

        identifiers = [Identifier(system=SYSTEM_SOR, value=sor_value)]

        aktiv = row.get("AKTIV")
        try:
            aktiv_int = int(aktiv) if aktiv is not None else 0
        except (TypeError, ValueError):
            aktiv_int = 0
        status = "active" if aktiv_int == 1 else "inactive"

        org_type = row.get("ORG_TYPE")
        location_type = None
        if org_type:
            location_type = [
                CodeableConcept(
                    coding=[
                        Coding(
                            system=SYSTEM_BOM_IFDB_ORG_TYPE,
                            code=str(org_type).strip(),
                        )
                    ]
                )
            ]

        managing_org_ref = Reference(
            type="Organization",
            identifier=Identifier(system=SYSTEM_SOR, value=sor_value),
        )

        part_of_ref = None
        parent_oid = normalize_oracle_id(row.get("PARENT_ORGENHEDOID"))
        if parent_oid and parent_oid in parent_sor_map:
            part_of_ref = Reference(
                type="Location",
                identifier=Identifier(
                    system=SYSTEM_SOR,
                    value=parent_sor_map[parent_oid],
                ),
            )

        period_start = None
        period_end = None
        if row.get("GYLDIG_FRA_DATOTID"):
            parsed = parse_clob_datetime(row["GYLDIG_FRA_DATOTID"]) or row["GYLDIG_FRA_DATOTID"]
            if parsed:
                period_start = fhir_datetime(parsed)
        if row.get("GYLDIG_TIL_DATOTID"):
            parsed = parse_clob_datetime(row["GYLDIG_TIL_DATOTID"]) or row["GYLDIG_TIL_DATOTID"]
            if parsed:
                period_end = fhir_datetime(parsed)

        # R4B Location has no native validity-period field; carry it on an extension.
        extensions: list[Extension] = []
        if period_start or period_end:
            extensions.append(
                Extension(
                    url="http://regionsyddanmark.dk/fhir/StructureDefinition/locationValidityPeriod",
                    valuePeriod=Period(start=period_start, end=period_end),
                )
            )

        location = FhirLocation(
            identifier=identifiers,
            status=status,
            type=location_type,
            managingOrganization=managing_org_ref,
            partOf=part_of_ref,
            extension=extensions if extensions else None,
        )
        locations.append(location)

    return locations


def _post_locations_to_fhir(
    context: dg.AssetExecutionContext,
    locations: list[FhirLocation],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
) -> dg.MaterializeResult:
    """Post SOR-keyed Location resources with reference resolution + upsert."""
    if not locations:
        return post_resources_direct(
            context=context,
            resources=[],
            fhir_database=fhir_database,
            resource_type="Location",
            source_name="bom_ifdb_oracle",
            extracted_count=extracted_count,
        )

    logger = dg.get_dagster_logger()

    locations = prepare_resources_for_upsert(
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        preferred_id_systems=[SYSTEM_SOR],
        reference_configs=LOCATION_SOR_REFERENCE_CONFIGS,
        force_reload=True,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        source_name="bom_ifdb_oracle",
        extracted_count=extracted_count,
    )


# =============================================================================
# DFM Asset
# =============================================================================


@dg.asset(
    name="fhir_server_location_sor_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=["arc_bom_ifdb_orgenhed_sor", "fhir_server_location_dfm"],
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
    description=(
        "Fetch ORGENHED+SORENHED rows from arc.bom_ifdb_orgenhed_sor in batches, "
        "transform to SOR-keyed FHIR Location, and post to FHIR server. "
        "Checkpoint owner for fhir_server.Location_SOR."
    ),
)
def fhir_server_location_sor_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch -> transform -> post for SOR-keyed Location resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Location_SOR",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        query = build_fhir_archive_query(
            "arc.bom_ifdb_orgenhed_sor",
            run_ctx.checkpoint,
            config,
        )
        context.log.info(f"Querying arc.bom_ifdb_orgenhed_sor (mode={config.mode})")

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
                    batch: list[dict[str, Any]] = [
                        dict(zip(columns, row, strict=False)) for row in raw_rows
                    ]
                    total_fetched += len(batch)

                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} orgenhed_sor rows "
                        f"(total so far: {total_fetched})"
                    )

                    parent_oids: set[str] = set()
                    for row in batch:
                        parent_oid = normalize_oracle_id(row.get("PARENT_ORGENHEDOID"))
                        if parent_oid:
                            parent_oids.add(parent_oid)

                    parent_sor_map = _fetch_parent_sor_map_from_arc(
                        parent_oids, mssql_target_staging
                    )

                    locations = _transform_locations_to_fhir(batch, parent_sor_map, context)

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
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
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
                    "fhir_server", "Location_SOR", value=new_checkpoint, mode=config.mode
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
