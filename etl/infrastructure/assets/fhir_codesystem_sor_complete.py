# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CodeSystem upload asset: SOR (content-complete)

Posts the full SOR CodeSystem resource (content: complete) to the FHIR server,
with all concepts, properties, and hierarchy inline as a recursive FHIR tree.

The FHIR server MUST run with BOX_FHIR_TERMINOLOGY_ENGINE=hybrid. In that mode
Aidbox indexes the inline concepts on PUT, enabling native $lookup and
$validate-code resolution for SOR codes directly from this resource.
"""

import json
import logging
from pathlib import Path

import dagster as dg
import yaml
from core.org_config import FHIR_PUBLISHER
from core.resources import FHIRServerResource, MSSQLResource
from infrastructure.assets.fhir_codesystem_sks_complete import (
    _build_concept_lookup,
    _build_concept_tree,
    put_content_complete_codesystem,
)

logger = logging.getLogger(__name__)

SOR_PROPERTY_DEFINITIONS = [
    {"code": "entityType", "description": "SOR entity type name", "type": "string"},
    {"code": "entityTypeId", "description": "SOR entity type identifier", "type": "string"},
    {"code": "municipality", "description": "Municipality name", "type": "string"},
    {"code": "region", "description": "Region name", "type": "string"},
    {"code": "owner", "description": "Owning organization name", "type": "string"},
    {"code": "ownerId", "description": "Owning organization SOR identifier", "type": "string"},
    {"code": "cvrNumber", "description": "cvr number", "type": "string"},
    {"code": "website", "description": "Website URL", "type": "string"},
    {"code": "email", "description": "Contact email", "type": "string"},
    {"code": "phone", "description": "Contact phone", "type": "string"},
    {"code": "address", "description": "Street address", "type": "string"},
    {"code": "postalCode", "description": "Postal code", "type": "string"},
    {"code": "city", "description": "City", "type": "string"},
    {"code": "openingDate", "description": "Opening date", "type": "dateTime"},
    {"code": "closingDate", "description": "Closing date", "type": "dateTime"},
    {
        "code": "rsdOrganization",
        "description": (
            "True when the entity belongs to Example Region's organisation tree "
            "(reachable from SOR unit 6101000016001), whether open, closed or not yet "
            "opened."
        ),
        "type": "boolean",
    },
    {
        "code": "activeOwnOrganization",
        "description": (
            "True when the entity is open, has taken effect, and belongs to "
            "Example Region's own current organisation (reachable from SOR "
            "unit 6101000016001 through a chain of open entities)."
        ),
        "type": "boolean",
    },
]


@dg.asset(
    name="fhir_codesystem_sor_complete",
    group_name="fhir_setup",
    deps=["cs_sor"],
    description=(
        "Post content-complete SOR CodeSystem (concepts with hierarchy "
        "and properties inline) to the FHIR server."
    ),
)
def fhir_codesystem_sor_complete(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    yml_path = Path(__file__).parent.parent.parent / "DSA" / "CodeSystem" / "sor" / "codesystem.yml"
    with open(yml_path) as f:
        cs_meta = yaml.safe_load(f)

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [code], [display] FROM [codesystem].[SORConcept] ORDER BY [code]"
            )
            concept_rows = cursor.fetchall()

            cursor.execute(
                "SELECT [code], [property_code], [value], [type] "
                "FROM [codesystem].[SORProperties]"
            )
            property_rows = cursor.fetchall()

            cursor.execute("SELECT [parent_code], [child_code] FROM [codesystem].[SORHierarchy]")
            hierarchy_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(
        f"Read staging tables: {len(concept_rows)} concepts, "
        f"{len(property_rows)} properties, {len(hierarchy_rows)} hierarchy entries"
    )

    if not concept_rows:
        raise dg.Failure(
            description="No concepts found in codesystem.sor_concept — cannot build CodeSystem."
        )

    lookup = _build_concept_lookup(concept_rows, property_rows)
    concept_tree = _build_concept_tree(lookup, hierarchy_rows)

    root_count = len(concept_tree)
    total_count = len(lookup)
    context.log.info(f"Built tree: {total_count} concepts, {root_count} roots")

    codesystem_dict: dict = {
        "resourceType": "CodeSystem",
        "id": "sor",
        "url": cs_meta["url"],
        "name": cs_meta["name"],
        "description": cs_meta["description"],
        "status": cs_meta["status"],
        "hierarchyMeaning": cs_meta.get("hierarchyMeaning", "is-a"),
        "publisher": cs_meta.get("publisher", FHIR_PUBLISHER),
        "content": "complete",
        "count": total_count,
        "property": SOR_PROPERTY_DEFINITIONS,
        "concept": concept_tree,
    }

    resource_json = json.dumps(codesystem_dict, ensure_ascii=False, separators=(",", ":"))
    json_size_mb = round(len(resource_json.encode("utf-8")) / (1024 * 1024), 1)
    context.log.info(f"Serialized CodeSystem JSON: {json_size_mb} MB")

    # PUT content-complete CodeSystem to Aidbox (retried; shared helper —
    # identical posting behavior to the SKS complete asset).
    put_content_complete_codesystem(
        context,
        fhir_server,
        resource_id="sor",
        resource_json=resource_json,
        json_size_mb=json_size_mb,
    )

    return dg.MaterializeResult(
        metadata={
            "status": dg.MetadataValue.text("success"),
            "url": dg.MetadataValue.text(cs_meta["url"]),
            "concept_count": dg.MetadataValue.int(total_count),
            "root_concept_count": dg.MetadataValue.int(root_count),
            "json_size_mb": dg.MetadataValue.float(json_size_mb),
            "post_method": dg.MetadataValue.text("fhir_api_put"),
        }
    )
