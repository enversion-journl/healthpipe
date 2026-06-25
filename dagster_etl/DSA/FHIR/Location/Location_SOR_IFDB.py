# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Location assets for BOM_IFDB ORGENHED + SORENHED.

Sibling feeder to Location_IFDB.py. Produces SOR-keyed Location resources from
BOM_IFDB.ORGENHED joined to BOM_IFDB.SORENHED. These are the Location resources
that Appointment.participant.actor references via SYSTEM_SOR identifier — they
represent the *performing organisational unit* of an appointment.

Distinct from Location_IFDB.py:
- One Location per ORGENHED.OID with SORENHEDOID NOT NULL (~1,784 rows in prod)
- No name/alias — source systems disagree on names for the same SOR code;
  the official name is available via managingOrganization
- managingOrganization → the existing SOR-keyed Organization (same SORKODE)
- partOf → the parent SOR-keyed Location (via PARENT_ORGENHEDOID → ORGENHED → SORENHED)
"""

from typing import Any

import dagster as dg
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from core.resources import (
    FHIRDatabaseResource,
    IFDBOracleResource,
)
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id, parse_clob_datetime
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig
from DSI.connectors.oracle.models import fetch_rows_as_dicts
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.location import Location as FhirLocation
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import SYSTEM_SOR

# =============================================================================
# Reference Resolution Configuration
# =============================================================================

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

# Coding system for ORGENHED.ORG_TYPE classification (reg / hosp / overafd / afd / ...).
SYSTEM_BOM_IFDB_ORG_TYPE = "urn:source:bom_ifdb:org_type"


# =============================================================================
# Helper Functions
# =============================================================================


def _fetch_parent_sor_map(
    parent_orgenhed_oids: set[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """Look up SORKODE for a set of ORGENHED.OID values (for partOf resolution).

    A partition's rows may reference parent ORGENHED rows that are themselves
    unchanged in the same partition, so we resolve the SOR code via a small
    look-up. ORGENHED is ~1,800 rows so chunking the IN-list is overkill.
    """
    if not parent_orgenhed_oids:
        return {}

    oid_list = list(parent_orgenhed_oids)
    parent_to_sor: dict[str, str] = {}
    chunk_size = 999

    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for start in range(0, len(oid_list), chunk_size):
                chunk = oid_list[start : start + chunk_size]
                placeholders = ", ".join([f":p{i}" for i in range(len(chunk))])
                binds = {f"p{i}": oid for i, oid in enumerate(chunk)}
                cursor.execute(
                    f"""
                    SELECT oe.OID, se.SORKODE
                    FROM BOM_IFDB.ORGENHED oe
                    JOIN BOM_IFDB.SORENHED se ON se.OID = oe.SORENHEDOID
                    WHERE oe.OID IN ({placeholders})
                    """,
                    binds,
                )
                rows = fetch_rows_as_dicts(cursor, context)
                for row in rows:
                    oe_oid = normalize_oracle_id(row.get("OID"))
                    sorkode = row.get("SORKODE")
                    if oe_oid and sorkode:
                        parent_to_sor[oe_oid] = str(sorkode)
        finally:
            cursor.close()

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
            # Should be filtered out by the extract (INNER JOIN), but guard anyway
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

        # managingOrganization → the SOR-keyed Organization with the same SORKODE
        managing_org_ref = Reference(
            type="Organization",
            identifier=Identifier(system=SYSTEM_SOR, value=sor_value),
        )

        # partOf → parent SOR Location (when parent ORGENHED has a SORKODE too)
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

        # Validity period from ORGENHED.GYLDIG_FRA/TIL_DATOTID
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

        # R4B Location has no native validity-period field; carry it on a small
        # source-system extension so analytics can recover the window without
        # losing the data. This is the only non-standard field on this resource.
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


def _run_location_sor_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Fetch parent SOR lookups, transform, post."""
    if not extracted_data:
        return _post_locations_to_fhir(
            context=context,
            locations=[],
            fhir_database=fhir_database,
            extracted_count=0,
        )

    parent_oids: set[str] = set()
    for row in extracted_data:
        parent_oid = normalize_oracle_id(row.get("PARENT_ORGENHEDOID"))
        if parent_oid:
            parent_oids.add(parent_oid)

    parent_sor_map = _fetch_parent_sor_map(parent_oids, ifdb_oracle, context)

    locations = _transform_locations_to_fhir(extracted_data, parent_sor_map, context)

    return _post_locations_to_fhir(
        context=context,
        locations=locations,
        fhir_database=fhir_database,
        extracted_count=len(extracted_data),
    )


# =============================================================================
# Daily Partition Assets
# =============================================================================


@dg.asset(
    name="fhir_location_sor_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "orgenhed_sor_daily"])),
    },
    deps=[dg.AssetKey(["fhir_server_location_dfm"])],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "4",
        "dfm_migrated": "true",
    },
)
def fhir_location_sor_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post SOR-keyed Location resources for daily partition."""
    return _run_location_sor_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


# =============================================================================
# Monthly Partition Assets
# =============================================================================


@dg.asset(
    name="fhir_location_sor_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "orgenhed_sor_monthly"])),
    },
    deps=[dg.AssetKey(["fhir_server_location_dfm"])],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "4",
    },
)
def fhir_location_sor_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post SOR-keyed Location resources for monthly partition."""
    return _run_location_sor_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


# =============================================================================
# Full Load Partition Assets
# =============================================================================


@dg.asset(
    name="fhir_location_sor_full_load_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "orgenhed_sor_full_load"])),
    },
    deps=[dg.AssetKey(["fhir_server_location_dfm"])],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={
        TAG_CHUNK_SIZE: "500",
        TAG_MAX_WORKERS: "8",
    },
)
def fhir_location_sor_full_load_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post SOR-keyed Location resources for full load."""
    return _run_location_sor_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
