# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR ConceptMap upload asset: SHAK -> SOR (content-complete)

Posts the full SHAK->SOR ConceptMap to the FHIR server with every mapping as a
group element. The mappings are read from conceptmap.SHAKToSOR (the persisted
source of truth built by cm_shak_to_sor).

This is the first FHIR ConceptMap publisher in the pipeline; it mirrors the
content-complete CodeSystem assets (build a plain resource dict, PUT with
exponential-backoff retry) rather than depending on a fhir.resources model so a
large element list serializes cheaply.
"""

import json
import logging
import time
from pathlib import Path

import dagster as dg
import requests
import yaml
from core.resources import FHIRServerResource, MSSQLResource

logger = logging.getLogger(__name__)

# Relationship vocabulary stored in conceptmap.* -> FHIR R4B ConceptMap
# element.target.equivalence codes. Our DB vocabulary already uses valid codes;
# unknown values fall back to "equivalent".
_EQUIVALENCE = {
    "equal": "equal",
    "equivalent": "equivalent",
    "wider": "wider",
    "narrower": "narrower",
    "inexact": "inexact",
    "unmatched": "unmatched",
    "disjoint": "disjoint",
}


def build_fhir_conceptmap(meta: dict, mapping_rows: list[tuple]) -> dict:
    """Build a content-complete R4B ConceptMap dict from mapping rows.

    Args:
        meta: parsed conceptmap.yml (url/name/title/description/source/target).
        mapping_rows: ``(source_code, source_display, relationship, target_code,
            target_display)`` tuples.
    """
    source_uri = meta["sourceUri"]
    target_uri = meta["targetUri"]

    elements: list[dict] = []
    for source_code, source_display, relationship, target_code, target_display in mapping_rows:
        source_code = str(source_code).strip()
        target_code = str(target_code).strip() if target_code else ""
        if not source_code or not target_code:
            continue
        target_entry: dict = {
            "code": target_code,
            "equivalence": _EQUIVALENCE.get(str(relationship or "").strip().lower(), "equivalent"),
        }
        if target_display and str(target_display).strip():
            target_entry["display"] = str(target_display).strip()

        element: dict = {"code": source_code, "target": [target_entry]}
        if source_display and str(source_display).strip():
            element["display"] = str(source_display).strip()
        elements.append(element)

    return {
        "resourceType": "ConceptMap",
        "id": meta["id"],
        "url": meta["url"],
        "name": meta["name"],
        "title": meta.get("title", meta["name"]),
        "status": meta.get("status", "active"),
        "description": meta.get("description", ""),
        "sourceUri": source_uri,
        "targetUri": target_uri,
        "group": [
            {
                "source": source_uri,
                "target": target_uri,
                "element": elements,
            }
        ],
    }


def _put_conceptmap(
    context: dg.AssetExecutionContext,
    fhir_server: FHIRServerResource,
    *,
    resource_id: str,
    resource_json: str,
    json_size_mb: float,
    max_retries: int = 3,
    base_backoff: int = 10,
):
    """PUT a ConceptMap to the FHIR server, retried with exponential backoff."""
    url = f"{fhir_server.effective_base_url}/ConceptMap/{resource_id}"
    payload = resource_json.encode("utf-8")

    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        context.log.info(
            f"PUT {url} ({json_size_mb} MB, timeout=600s) — attempt {attempt}/{max_retries}"
        )
        try:
            response = fhir_server.session.put(
                url,
                data=payload,
                headers={"Content-Type": "application/json", "Accept": "application/json"},
                timeout=600,
            )
            response.raise_for_status()
            context.log.info(f"PUT succeeded: HTTP {response.status_code}")
            return response
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.HTTPError,
        ) as exc:
            last_exc = exc
            body = getattr(getattr(exc, "response", None), "text", "")[:500]
            context.log.warning(
                f"PUT attempt {attempt}/{max_retries} failed: "
                f"{type(exc).__name__}: {exc}\nResponse body: {body}"
            )
            if attempt < max_retries:
                wait = base_backoff * (2 ** (attempt - 1))
                context.log.info(f"Retrying in {wait}s...")
                time.sleep(wait)

    body = getattr(getattr(last_exc, "response", None), "text", "")[:500]
    raise dg.Failure(
        description=(
            f"PUT {url} failed after {max_retries} attempts: "
            f"{type(last_exc).__name__}: {last_exc}\nResponse body: {body}"
        )
    ) from last_exc


@dg.asset(
    name="fhir_conceptmap_shak_to_sor",
    group_name="fhir_setup",
    deps=["cm_shak_to_sor"],
    description="Post the content-complete SHAK->SOR ConceptMap to the FHIR server.",
)
def fhir_conceptmap_shak_to_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    yml_path = (
        Path(__file__).parent.parent.parent
        / "DSA"
        / "ConceptMap"
        / "shak_to_sor"
        / "conceptmap.yml"
    )
    with open(yml_path) as f:
        cm_meta = yaml.safe_load(f)

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [source_code], [source_display], [relationship], "
                "[target_code], [target_display] "
                "FROM [conceptmap].[SHAKToSOR] ORDER BY [source_code]"
            )
            mapping_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(f"Read {len(mapping_rows)} SHAK->SOR mappings")
    if not mapping_rows:
        raise dg.Failure(description="No rows in conceptmap.SHAKToSOR — cannot build ConceptMap.")

    conceptmap_dict = build_fhir_conceptmap(cm_meta, mapping_rows)
    element_count = len(conceptmap_dict["group"][0]["element"])

    resource_json = json.dumps(conceptmap_dict, ensure_ascii=False, separators=(",", ":"))
    json_size_mb = round(len(resource_json.encode("utf-8")) / (1024 * 1024), 1)
    context.log.info(f"Serialized ConceptMap JSON: {element_count} elements, {json_size_mb} MB")

    _put_conceptmap(
        context,
        fhir_server,
        resource_id=cm_meta["id"],
        resource_json=resource_json,
        json_size_mb=json_size_mb,
    )

    return dg.MaterializeResult(
        metadata={
            "status": dg.MetadataValue.text("success"),
            "url": dg.MetadataValue.text(cm_meta["url"]),
            "element_count": dg.MetadataValue.int(element_count),
            "json_size_mb": dg.MetadataValue.float(json_size_mb),
            "post_method": dg.MetadataValue.text("fhir_api_put"),
        }
    )
