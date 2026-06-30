# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CodeSystem upload asset: SKS

Posts the SKS CodeSystem resource (content: not-present) to the FHIR server,
enabling $lookup resolution for SKS codes referenced in Condition, Procedure, etc.
"""

from pathlib import Path

import dagster as dg
import yaml
from core.org_config import FHIR_PUBLISHER
from core.resources import FHIRServerResource


@dg.asset(
    name="fhir_codesystem_sks",
    group_name="fhir_setup",
    deps=["cs_sks"],
    description="Post SKS CodeSystem definition to the FHIR server",
)
def fhir_codesystem_sks(fhir_server: FHIRServerResource) -> dg.MaterializeResult:
    yml_path = Path(__file__).parent.parent.parent / "DSA" / "CodeSystem" / "sks" / "codesystem.yml"
    with open(yml_path) as f:
        cs_meta = yaml.safe_load(f)

    codesystem_resource = {
        "resourceType": "CodeSystem",
        "url": cs_meta["url"],
        "name": cs_meta["name"],
        "description": cs_meta["description"],
        "status": cs_meta["status"],
        "hierarchyMeaning": cs_meta.get("hierarchyMeaning", "is-a"),
        "publisher": cs_meta.get("publisher", FHIR_PUBLISHER),
        "content": cs_meta.get("content", "not-present"),
    }

    from fhir.resources.R4B.codesystem import CodeSystem

    cs = CodeSystem(**codesystem_resource)

    responses = fhir_server.post_resource_batches(
        resources=[cs],
        chunk_size=1,
        bundle_type="transaction",
        max_retries=3,
        retry_backoff_sec=0.5,
    )

    success = all(resp.status_code == 200 for resp in responses)
    status = "success" if success else "partial_failure"

    return dg.MaterializeResult(
        metadata={
            "status": dg.MetadataValue.text(status),
            "url": dg.MetadataValue.text(cs_meta["url"]),
            "name": dg.MetadataValue.text(cs_meta["name"]),
        }
    )
