# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CodeSystem upload asset: SHAK (content-complete)

Posts the full SHAK CodeSystem resource (content: complete) to the FHIR server,
with all concepts, properties, and the derived hierarchy inline as a recursive
FHIR tree. Reuses the shared tree-builder and PUT helper from the SKS complete
asset.

The FHIR server MUST run with BOX_FHIR_TERMINOLOGY_ENGINE=hybrid so the inline
concepts are indexed on PUT, enabling native $lookup / $validate-code for SHAK.
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

SHAK_PROPERTY_DEFINITIONS = [
    {
        "code": "institutionTypeId",
        "description": "SHAK institution type identifier",
        "type": "string",
    },
    {"code": "institutionType", "description": "SHAK institution type name", "type": "string"},
    {"code": "ownerTypeId", "description": "SHAK owner type identifier", "type": "string"},
    {"code": "ownerType", "description": "SHAK owner type name", "type": "string"},
    {
        "code": "mainSpecialityId",
        "description": "SHAK main speciality identifier",
        "type": "string",
    },
    {"code": "mainSpeciality", "description": "SHAK main speciality name", "type": "string"},
    {"code": "speciality1Id", "description": "SHAK speciality 1 identifier", "type": "string"},
    {"code": "speciality1", "description": "SHAK speciality 1 name", "type": "string"},
    {"code": "speciality2Id", "description": "SHAK speciality 2 identifier", "type": "string"},
    {"code": "speciality2", "description": "SHAK speciality 2 name", "type": "string"},
    {"code": "speciality3Id", "description": "SHAK speciality 3 identifier", "type": "string"},
    {"code": "speciality3", "description": "SHAK speciality 3 name", "type": "string"},
    {"code": "validFrom", "description": "SHAK validity start date", "type": "dateTime"},
    {"code": "validTo", "description": "SHAK validity end date", "type": "dateTime"},
]


@dg.asset(
    name="fhir_codesystem_shak_complete",
    group_name="fhir_setup",
    deps=["cs_shak"],
    description=(
        "Post content-complete SHAK CodeSystem (concepts with derived hierarchy "
        "and properties inline) to the FHIR server."
    ),
)
def fhir_codesystem_shak_complete(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    yml_path = (
        Path(__file__).parent.parent.parent / "DSA" / "CodeSystem" / "shak" / "codesystem.yml"
    )
    with open(yml_path) as f:
        cs_meta = yaml.safe_load(f)

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [code], [display] FROM [codesystem].[SHAKConcept] ORDER BY [code]"
            )
            concept_rows = cursor.fetchall()

            cursor.execute(
                "SELECT [code], [property_code], [value], [type] FROM [codesystem].[SHAKProperties]"
            )
            property_rows = cursor.fetchall()

            cursor.execute("SELECT [parent_code], [child_code] FROM [codesystem].[SHAKHierarchy]")
            hierarchy_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(
        f"Read staging tables: {len(concept_rows)} concepts, "
        f"{len(property_rows)} properties, {len(hierarchy_rows)} hierarchy entries"
    )

    if not concept_rows:
        raise dg.Failure(
            description="No concepts found in codesystem.SHAKConcept — cannot build CodeSystem."
        )

    lookup = _build_concept_lookup(concept_rows, property_rows)
    concept_tree = _build_concept_tree(lookup, hierarchy_rows)

    root_count = len(concept_tree)
    total_count = len(lookup)
    context.log.info(f"Built tree: {total_count} concepts, {root_count} roots")

    codesystem_dict: dict = {
        "resourceType": "CodeSystem",
        "id": "shak",
        "url": cs_meta["url"],
        "name": cs_meta["name"],
        "description": cs_meta["description"],
        "status": cs_meta["status"],
        "hierarchyMeaning": cs_meta.get("hierarchyMeaning", "is-a"),
        "publisher": cs_meta.get("publisher", FHIR_PUBLISHER),
        "content": "complete",
        "count": total_count,
        "property": SHAK_PROPERTY_DEFINITIONS,
        "concept": concept_tree,
    }

    resource_json = json.dumps(codesystem_dict, ensure_ascii=False, separators=(",", ":"))
    json_size_mb = round(len(resource_json.encode("utf-8")) / (1024 * 1024), 1)
    context.log.info(f"Serialized CodeSystem JSON: {json_size_mb} MB")

    put_content_complete_codesystem(
        context,
        fhir_server,
        resource_id="shak",
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
