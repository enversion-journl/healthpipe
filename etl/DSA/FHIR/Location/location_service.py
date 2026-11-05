# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The write point for SOR-keyed Locations: read both archives for the codes, merge, post once."""

from collections.abc import Iterable

import dagster as dg
from core.dfm_config import RunModeConfig
from core.resources import FHIRDatabaseResource, MSSQLResource
from DSA.FHIR.Location.location_builder import (
    build,
    current_sor_rows,
    ifdb_contribution,
    sor_contribution,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.field_ownership import Contribution
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch
from DSA.FHIR.utils.ifdb.fhir_posting import post_resources_direct, prepare_resources_for_upsert
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id
from DSA.FHIR.utils.reference_resolver import ReferenceFieldConfig
from fhir.resources.R4B.location import Location
from utils.fhir.identifier_systems import SYSTEM_SOR

SOR_ARCHIVE = "arc.bom_ifdb_orgenhed_sor"
IFDB_ARCHIVE = "arc.ifdb_location"

_SOR_COLUMNS = [
    "OID",
    "SORKODE",
    "AKTIV",
    "ORG_TYPE",
    "PARENT_ORGENHEDOID",
    "GYLDIG_FRA_DATOTID",
    "GYLDIG_TIL_DATOTID",
    "dw_is_deleted",
    "dw_timestamp_update",
]

LOCATION_REFERENCE_CONFIGS = [
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


def _fetch_parent_sor_map_from_arc(
    parent_orgenhed_oids: set[str],
    mssql: MSSQLResource,
) -> dict[str, str]:
    """Look up SORKODE for parent ORGENHED OIDs from arc.bom_ifdb_orgenhed_sor."""
    if not parent_orgenhed_oids:
        return {}
    rows = chunked_in_fetch(
        list(parent_orgenhed_oids),
        table=SOR_ARCHIVE,
        columns=["OID", "SORKODE"],
        where_column="OID",
        mssql=mssql,
        include_deleted=True,
    )
    parent_to_sor: dict[str, str] = {}
    for row in rows:
        oe_oid = normalize_oracle_id(row.get("OID"))
        sorkode = row.get("SORKODE")
        if oe_oid and sorkode:
            parent_to_sor[oe_oid] = str(sorkode)
    return parent_to_sor


def build_sor_code_query(checkpoint: str | None, run_config: RunModeConfig) -> str:
    """The SORKODE of every row in arc.bom_ifdb_orgenhed_sor for the run mode, also soft-deleted."""
    return build_fhir_archive_query(
        SOR_ARCHIVE, checkpoint, run_config, include_deleted=True, select_clause="SELECT [SORKODE]"
    )


def build_ifdb_code_query(checkpoint: str | None, run_config: RunModeConfig) -> str:
    """The distinct non-NULL SOR codes in arc.ifdb_location for the run mode, also soft-deleted."""
    query = build_fhir_archive_query(
        IFDB_ARCHIVE,
        checkpoint,
        run_config,
        include_deleted=True,
        select_clause="SELECT DISTINCT [LOCATION_SORCODE]",
    )
    return query + " AND [LOCATION_SORCODE] IS NOT NULL"


def read_contributions(codes: Iterable[str], mssql: MSSQLResource) -> dict[str, list[Contribution]]:
    """Every source's contribution per SOR code; deleted SOR rows count, deleted IFDB rows do not.

    A code with only deleted IFDB rows and no SOR row maps to an empty list.
    """
    wanted = sorted({c for c in codes if c})
    if not wanted:
        return {}
    sor_rows = current_sor_rows(
        chunked_in_fetch(
            wanted,
            table=SOR_ARCHIVE,
            columns=_SOR_COLUMNS,
            where_column="SORKODE",
            mssql=mssql,
            include_deleted=True,
        )
    )
    parent_oids = {
        oid
        for row in sor_rows.values()
        if (oid := normalize_oracle_id(row.get("PARENT_ORGENHEDOID")))
    }
    parent_sor_map = _fetch_parent_sor_map_from_arc(parent_oids, mssql)
    ifdb_rows = chunked_in_fetch(
        wanted,
        table=IFDB_ARCHIVE,
        columns=["LOCATION_SORCODE", "dw_is_deleted"],
        where_column="LOCATION_SORCODE",
        mssql=mssql,
        include_deleted=True,
        distinct=True,
    )
    ifdb_known = {normalize_oracle_id(row["LOCATION_SORCODE"]) for row in ifdb_rows}
    ifdb_live = {
        normalize_oracle_id(row["LOCATION_SORCODE"])
        for row in ifdb_rows
        if not row["dw_is_deleted"]
    }

    contributions: dict[str, list[Contribution]] = {}
    for code in wanted:
        found: list[Contribution] = []
        if code in sor_rows:
            found.append(sor_contribution(sor_rows[code], parent_sor_map))
        if code in ifdb_live:
            found.append(ifdb_contribution(code))
        if found or code in ifdb_known:
            contributions[code] = found
    return contributions


def write_codes(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    codes: Iterable[str],
    *,
    source_name: str,
) -> int:
    """Rebuild and post the merged Location for each SOR code; returns the number posted."""
    wanted = sorted({c for c in codes if c})
    contributions = read_contributions(wanted, mssql)
    missing = [c for c in wanted if c not in contributions]
    if missing:
        raise dg.Failure(
            description=f"{len(missing)} changed SOR codes are in neither archive, "
            f"e.g. {missing[:5]}; the codes do not match the archive values"
        )
    unsourced = [code for code, found in contributions.items() if not found]
    if unsourced:
        dg.get_dagster_logger().warning(
            f"{len(unsourced)} changed SOR codes have only deleted IFDB rows and no SOR row; "
            f"their Locations are not rewritten, e.g. {unsourced[:5]}"
        )
    locations: list[Location] = [build(c) for c in contributions.values() if c]
    if not locations:
        return 0

    locations = prepare_resources_for_upsert(
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        preferred_id_systems=[SYSTEM_SOR],
        reference_configs=LOCATION_REFERENCE_CONFIGS,
        force_reload=True,
        logger=dg.get_dagster_logger(),
    )
    post_resources_direct(
        context=context,
        resources=locations,
        fhir_database=fhir_database,
        resource_type="Location",
        source_name=source_name,
        extracted_count=len(wanted),
    )
    return len(locations)


__all__ = [
    "LOCATION_REFERENCE_CONFIGS",
    "build_ifdb_code_query",
    "build_sor_code_query",
    "read_contributions",
    "write_codes",
]
