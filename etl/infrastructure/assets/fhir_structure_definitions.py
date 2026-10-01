# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

import dagster as dg
from core.resources import FHIRServerResource
from fhir.resources.R4B.structuredefinition import StructureDefinition
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_PREVIOUS_EPISODE_OF_CARE,
    EXTENSION_PREVIOUS_EPISODE_TYPE,
)


@dg.asset(
    name="fhir_structure_definitions",
    group_name="fhir_setup",
)
def fhir_structure_definitions(fhir_server: FHIRServerResource) -> dg.MaterializeResult:
    """
    Posts StructureDefinition resources for custom extensions to the FHIR server.
    Must run before any resources using these extensions.
    """
    # Previous Episode of Care Extension
    previous_episode_sd = {
        "resourceType": "StructureDefinition",
        "url": EXTENSION_PREVIOUS_EPISODE_OF_CARE,
        "name": "PreviousEpisodeOfCare",
        "title": "Previous Episode of Care Reference",
        "status": "active",
        "kind": "complex-type",
        "abstract": False,
        "context": [{"type": "element", "expression": "EpisodeOfCare"}],
        "type": "Extension",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Extension",
        "derivation": "constraint",
        "differential": {
            "element": [
                {
                    "id": "Extension",
                    "path": "Extension",
                    "short": "Previous Episode of Care Reference",
                    "definition": "Reference to a previous episode of care that this episode is related to.",
                },
                {
                    "id": "Extension.url",
                    "path": "Extension.url",
                    "fixedUri": EXTENSION_PREVIOUS_EPISODE_OF_CARE,
                },
                {
                    "id": "Extension.valueReference",
                    "path": "Extension.valueReference",
                    "short": "Reference to previous EpisodeOfCare",
                    "type": [
                        {
                            "code": "Reference",
                            "targetProfile": [
                                "http://hl7.org/fhir/StructureDefinition/EpisodeOfCare"
                            ],
                        }
                    ],
                    "mustSupport": True,
                },
            ]
        },
    }

    # Previous Episode Type Extension
    previous_episode_type_sd = {
        "resourceType": "StructureDefinition",
        "url": EXTENSION_PREVIOUS_EPISODE_TYPE,
        "name": "PreviousEpisodeOfCareType",
        "title": "Previous Episode of Care Relationship Type",
        "status": "active",
        "kind": "complex-type",
        "abstract": False,
        "context": [{"type": "element", "expression": "EpisodeOfCare"}],
        "type": "Extension",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Extension",
        "derivation": "constraint",
        "differential": {
            "element": [
                {
                    "id": "Extension",
                    "path": "Extension",
                    "short": "Previous Episode Relationship Type",
                    "definition": "Describes the type of relationship between this episode and the previous episode.",
                },
                {
                    "id": "Extension.url",
                    "path": "Extension.url",
                    "fixedUri": EXTENSION_PREVIOUS_EPISODE_TYPE,
                },
                {
                    "id": "Extension.valueCoding",
                    "path": "Extension.valueCoding",
                    "short": "Relationship type code",
                    "type": [{"code": "Coding"}],
                    "mustSupport": True,
                },
            ]
        },
    }

    # Episode of Care reference on Observation Extension
    episode_of_care_on_observation_sd = {
        "resourceType": "StructureDefinition",
        "url": EXTENSION_EPISODE_OF_CARE,
        "name": "EpisodeOfCare",
        "title": "Episode of Care Reference",
        "status": "active",
        "kind": "complex-type",
        "abstract": False,
        "context": [{"type": "element", "expression": "Observation"}],
        "type": "Extension",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Extension",
        "derivation": "constraint",
        "differential": {
            "element": [
                {
                    "id": "Extension",
                    "path": "Extension",
                    "short": "Episode of Care Reference",
                    "definition": "Reference to an episode of care that this observation is related to.",
                },
                {
                    "id": "Extension.url",
                    "path": "Extension.url",
                    "fixedUri": EXTENSION_EPISODE_OF_CARE,
                },
                {
                    "id": "Extension.valueReference",
                    "path": "Extension.valueReference",
                    "short": "Reference to EpisodeOfCare",
                    "type": [
                        {
                            "code": "Reference",
                            "targetProfile": [
                                "http://hl7.org/fhir/StructureDefinition/EpisodeOfCare"
                            ],
                        }
                    ],
                    "mustSupport": True,
                },
            ]
        },
    }

    successes = 0
    failures = 0
    failed_urls = []

    # Create all StructureDefinition resources
    structure_definitions = []
    for sd_dict in [
        previous_episode_sd,
        previous_episode_type_sd,
        episode_of_care_on_observation_sd,
    ]:
        try:
            # Create StructureDefinition resource
            sd = StructureDefinition(**sd_dict)
            structure_definitions.append(sd)
        except Exception as exc:
            failures += 1
            failed_urls.append(sd_dict.get("url", "<no-url>"))
            dg.get_dagster_logger().warning(
                f"Failed creating StructureDefinition {sd_dict.get('url')}: {exc}"
            )

    # Post all resources in batch
    if structure_definitions:
        try:
            responses = fhir_server.post_resource_batches(
                resources=structure_definitions,
                chunk_size=1000,
                bundle_type="transaction",
                max_retries=3,
                retry_backoff_sec=0.5,
            )

            total_chunks = len(responses)
            successful_chunks = sum(1 for resp in responses if resp.status_code == 200)
            failed_chunks = total_chunks - successful_chunks

            if successful_chunks > 0:
                successes = (
                    (successful_chunks * 1000)
                    if len(structure_definitions) > 1000
                    else len(structure_definitions)
                )
                failures += failed_chunks * 1000

                dg.get_dagster_logger().info(
                    f"Batch posting completed: {successful_chunks}/{total_chunks} chunks successful, "
                    f"estimated {successes} structure definitions posted successfully"
                )

                if failed_chunks > 0:
                    dg.get_dagster_logger().warning(
                        f"{failed_chunks} chunks failed during batch posting"
                    )
                    failed_urls.extend([f"chunk_{i}" for i in range(failed_chunks)])
            else:
                failures += len(structure_definitions)
                failed_urls.extend([f"chunk_{i}" for i in range(total_chunks)])

        except Exception as exc:
            dg.get_dagster_logger().error(f"Batch posting failed completely: {exc}")
            failures += len(structure_definitions)
            failed_urls.extend(
                [f"chunk_{i}" for i in range(len(responses) if "responses" in locals() else 1)]
            )

    return dg.MaterializeResult(
        metadata={
            "posted_count": successes,
            "failed_count": failures,
            "failed_urls": dg.MetadataValue.json(failed_urls),
        }
    )
