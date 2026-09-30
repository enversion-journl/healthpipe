# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Local test: PUT the SOR content-complete CodeSystem to the local FHIR server.

Exercises the exact same code path as fhir_codesystem_sor_complete, using the
dynamic timeout fix (_compute_put_timeout).
"""

import json
import logging
import os
import time
from pathlib import Path

import requests
import yaml
from core.org_config import FHIR_PUBLISHER
from core.resources import MSSQLResource
from infrastructure.assets.fhir_codesystem_sks_complete import (
    _build_concept_lookup,
    _build_concept_tree,
    _compute_put_timeout,
)
from infrastructure.assets.fhir_codesystem_sor_complete import SOR_PROPERTY_DEFINITIONS

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

LOG_PATH = "/workspaces/sydbi_basismodel/.cursor/debug-9dd847.log"


def debug_log(message, data=None):
    entry = {
        "sessionId": "9dd847",
        "timestamp": int(time.time() * 1000),
        "location": "test_sor_put_local.py",
        "message": message,
        "data": data or {},
    }
    with open(LOG_PATH, "a") as f:
        f.write(json.dumps(entry) + "\n")


def main():
    debug_log("test_start", {"phase": "building_codesystem"})

    res = MSSQLResource(
        host=os.environ["MSSQL_TARGET_STAGING_HOST"],
        port=int(os.environ["MSSQL_TARGET_STAGING_PORT"]),
        database=os.environ["MSSQL_TARGET_STAGING_DB"],
        user=os.environ["MSSQL_TARGET_STAGING_USER"],
        password=os.environ["MSSQL_TARGET_STAGING_PASSWORD"],
    )

    logger.info("Reading staging tables...")
    with res.get_connection() as conn:
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

    logger.info(
        f"Read: {len(concept_rows)} concepts, "
        f"{len(property_rows)} properties, {len(hierarchy_rows)} hierarchy entries"
    )
    debug_log(
        "staging_read",
        {
            "concepts": len(concept_rows),
            "properties": len(property_rows),
            "hierarchy": len(hierarchy_rows),
        },
    )

    logger.info("Building concept tree...")
    lookup = _build_concept_lookup(concept_rows, property_rows)
    concept_tree = _build_concept_tree(lookup, hierarchy_rows)
    logger.info(f"Built tree: {len(lookup)} concepts, {len(concept_tree)} roots")

    yml_path = Path(__file__).parent / "DSA" / "CodeSystem" / "sor" / "codesystem.yml"
    with open(yml_path) as f:
        cs_meta = yaml.safe_load(f)

    codesystem_dict = {
        "resourceType": "CodeSystem",
        "id": "sor",
        "url": cs_meta["url"],
        "name": cs_meta["name"],
        "description": cs_meta["description"],
        "status": cs_meta["status"],
        "hierarchyMeaning": cs_meta.get("hierarchyMeaning", "is-a"),
        "publisher": cs_meta.get("publisher", FHIR_PUBLISHER),
        "content": "complete",
        "count": len(lookup),
        "property": SOR_PROPERTY_DEFINITIONS,
        "concept": concept_tree,
    }

    logger.info("Serializing JSON...")
    resource_json = json.dumps(codesystem_dict, ensure_ascii=False, separators=(",", ":"))
    json_size_mb = round(len(resource_json.encode("utf-8")) / (1024 * 1024), 1)
    logger.info(f"Serialized CodeSystem JSON: {json_size_mb} MB")

    timeout_seconds = _compute_put_timeout(json_size_mb)
    debug_log(
        "pre_put",
        {"json_size_mb": json_size_mb, "timeout_seconds": timeout_seconds},
    )

    base_url = os.environ["FHIR_SERVER_URL"]
    use_fhir_api = os.environ.get("USE_FHIR_API", "false").lower() in ("true", "1", "yes")
    if use_fhir_api:
        base_url = f"{base_url.rstrip('/')}/fhir"

    url = f"{base_url}/CodeSystem/sor"
    payload = resource_json.encode("utf-8")

    session = requests.Session()
    username = os.environ.get("FHIR_CLIENT_USERNAME")
    password = os.environ.get("FHIR_CLIENT_PASSWORD")
    if username and password:
        session.auth = (username, password)

    logger.info(f"PUT {url} ({json_size_mb} MB, timeout={timeout_seconds}s)")
    debug_log("put_start", {"url": url, "size_mb": json_size_mb, "timeout_s": timeout_seconds})

    start_time = time.time()
    try:
        response = session.put(
            url,
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            timeout=timeout_seconds,
        )
        elapsed = time.time() - start_time
        debug_log(
            "put_response",
            {
                "status_code": response.status_code,
                "elapsed_seconds": round(elapsed, 1),
                "response_snippet": response.text[:500],
            },
        )
        logger.info(f"PUT completed: HTTP {response.status_code} in {elapsed:.1f}s")
        if response.status_code >= 400:
            logger.error(f"Response body: {response.text[:1000]}")
        else:
            logger.info("SUCCESS - CodeSystem/sor PUT accepted by FHIR server")
    except requests.exceptions.Timeout as exc:
        elapsed = time.time() - start_time
        debug_log("put_timeout", {"elapsed_seconds": round(elapsed, 1), "error": str(exc)})
        logger.error(f"TIMEOUT after {elapsed:.1f}s: {exc}")
    except Exception as exc:
        elapsed = time.time() - start_time
        debug_log(
            "put_error",
            {"elapsed_seconds": round(elapsed, 1), "error": str(exc), "type": type(exc).__name__},
        )
        logger.error(f"ERROR after {elapsed:.1f}s: {type(exc).__name__}: {exc}")


if __name__ == "__main__":
    main()
