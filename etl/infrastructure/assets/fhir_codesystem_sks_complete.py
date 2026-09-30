# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR CodeSystem upload asset: SKS (content-complete)

Posts the full SKS CodeSystem resource (content: complete) to the FHIR server,
with all ~70k concepts, properties, and hierarchy inline as a recursive FHIR
tree. Concepts are clean SKS codes (classification prefix stripped); the
classification(s) and an `inactive` flag are carried as concept properties.

The FHIR server MUST run with BOX_FHIR_TERMINOLOGY_ENGINE=hybrid. In that mode
Aidbox indexes the inline concepts on PUT, enabling native $lookup and
$validate-code resolution for SKS codes directly from this resource. (In the
default/legacy engine, a PUT of a large content-complete CodeSystem returns
HTTP 500, and terminology operations do not resolve.)
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import dagster as dg
import requests
import yaml
from core.org_config import FHIR_PUBLISHER
from core.resources import FHIRServerResource, MSSQLResource

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Shared SKS property definitions (single source of truth)
# ---------------------------------------------------------------------------

SKS_PROPERTY_DEFINITIONS = [
    {
        "code": "classification",
        "description": (
            "SKS classification prefix (adm, atc, dia, opr, pro, res, "
            "spc, til, und, uly). Repeats when a code belongs to "
            "several classifications."
        ),
        "type": "string",
    },
    {
        "code": "inactive",
        "uri": "http://hl7.org/fhir/concept-properties#inactive",
        "description": (
            "True when the code's validity period has ended "
            "(newest version's valid-to date is in the past)."
        ),
        "type": "boolean",
    },
]


# ---------------------------------------------------------------------------
# Private helpers: build FHIR concept tree from flat staging tables
# ---------------------------------------------------------------------------


def _to_fhir_datetime(value: object) -> str | None:
    """Normalize a SQL date/datetime value to a valid FHIR date, or ``None``.

    MSSQL returns date values as ``YYYY-MM-DD HH:MM:SS`` (space separator, no
    timezone), which is NOT a valid FHIR ``dateTime`` — Aidbox rejects it with
    "invalid time separator" / "no timezone" and fails the *whole* CodeSystem
    PUT. These source values are date-only (midnight padding), so we emit FHIR
    date precision (``YYYY-MM-DD``), which is itself a valid ``dateTime``.

    Returns ``None`` for empty or unparseable input so the caller can skip the
    property rather than emit invalid FHIR (one bad row otherwise fails the
    entire PUT). A non-midnight time component is dropped with a warning.

    Kept self-contained (rather than reusing ``DSA.FHIR.utils.fhir_date``) to
    avoid an infrastructure→DSA/fhir.resources import dependency in this asset.
    """
    raw = "" if value is None else str(value).strip()
    if not raw:
        return None
    date_part = raw.split(" ")[0].split("T")[0]
    try:
        normalized = datetime.strptime(date_part, "%Y-%m-%d").date().isoformat()
    except ValueError:
        logger.warning("Skipping date property with unparseable value %r", raw)
        return None
    remainder = raw[len(date_part) :].lstrip("T ").strip()
    if remainder and not remainder.startswith("00:00:00"):
        logger.warning(
            "Discarding non-midnight time from date value %r (emitting date %s)",
            raw,
            normalized,
        )
    return normalized


def _build_concept_lookup(
    concept_rows: list[tuple],
    property_rows: list[tuple],
) -> dict[str, dict]:
    """
    Build a flat lookup {code: {code, display, property[]}} from staging rows.

    concept_rows:  [(code, display), ...]
    property_rows: [(code, property_code, value, type), ...] — a code may carry
                   several rows for the same property_code (e.g. one
                   classification per SKS category it belongs to).

    Type dispatch:
      - Boolean/boolean        -> valueBoolean (bool)
      - Date/date/dateTime/DateTime -> valueDateTime, normalized to FHIR date
        precision via _to_fhir_datetime; empty/unparseable values are skipped
      - anything else          -> valueString
    """
    _DATETIME_TYPES = {"date", "datetime"}

    # Index properties by code
    props_by_code: dict[str, list[dict]] = {}
    for code, prop_code, value, prop_type in property_rows:
        code = str(code).strip()
        if not code:
            continue
        type_norm = str(prop_type or "").strip().lower()
        if type_norm == "boolean":
            prop: dict = {
                "code": str(prop_code).strip(),
                "valueBoolean": str(value).strip().lower() == "true",
            }
        elif type_norm in _DATETIME_TYPES:
            fhir_dt = _to_fhir_datetime(value)
            if fhir_dt is None:
                # Empty/unparseable date — skip rather than emit invalid FHIR.
                continue
            prop = {
                "code": str(prop_code).strip(),
                "valueDateTime": fhir_dt,
            }
        else:
            prop = {"code": str(prop_code).strip(), "valueString": str(value).strip()}
        props_by_code.setdefault(code, []).append(prop)

    lookup: dict[str, dict] = {}
    for code, display in concept_rows:
        code = str(code).strip()
        if not code:
            continue
        display = str(display).strip() if display else code
        entry: dict = {"code": code, "display": display}
        if code in props_by_code:
            entry["property"] = props_by_code[code]
        lookup[code] = entry

    return lookup


def _build_concept_tree(
    lookup: dict[str, dict],
    hierarchy_rows: list[tuple],
) -> list[dict]:
    """
    Build a recursive FHIR concept tree from a flat lookup and hierarchy rows.

    hierarchy_rows: [(parent_code, child_code), ...]

    Returns a sorted list of root concepts (codes not appearing as children),
    each with nested ``concept[]`` children.
    """
    children_of: dict[str, list[str]] = {}
    parent_of: dict[str, str] = {}
    multi_parent: set[str] = set()

    for parent_code, child_code in hierarchy_rows:
        parent_code = str(parent_code).strip()
        child_code = str(child_code).strip()
        if not parent_code or not child_code:
            continue
        # Only include if both codes exist in the lookup
        if parent_code not in lookup or child_code not in lookup:
            continue
        existing = parent_of.get(child_code)
        if existing is not None:
            # A FHIR concept tree is a tree, not a DAG: a code may appear under
            # only ONE parent, else it is emitted more than once and Aidbox's
            # csd-1 invariant ("all codes SHALL be unique") rejects the whole
            # CodeSystem (HTTP 422). Keep the first parent; flag a genuine
            # second parent. A duplicate (parent, child) row is harmless.
            if existing != parent_code:
                multi_parent.add(child_code)
            continue
        parent_of[child_code] = parent_code
        children_of.setdefault(parent_code, []).append(child_code)

    has_parent: set[str] = set(parent_of)

    if multi_parent:
        logger.warning(
            "%d concept(s) have multiple parents in the hierarchy; each is "
            "placed under its first parent only (a FHIR concept tree is a tree, "
            "not a DAG). Sample: %s",
            len(multi_parent),
            sorted(multi_parent)[:5],
        )

    def _attach_children(code: str, visited: set[str] | None = None) -> dict:
        if visited is None:
            visited = set()
        node = dict(lookup[code])  # shallow copy
        kids = children_of.get(code, [])
        if kids:
            children_nodes = []
            for c in sorted(kids):
                if c in visited:
                    logger.warning(
                        "Cycle detected in CodeSystem hierarchy: code %r is already an "
                        "ancestor of itself (via %r). Skipping to prevent infinite recursion.",
                        c,
                        code,
                    )
                    continue
                children_nodes.append(_attach_children(c, visited | {code}))
            if children_nodes:
                node["concept"] = children_nodes
        return node

    # Root concepts = codes in lookup that are NOT children of anything
    roots = sorted(code for code in lookup if code not in has_parent)
    tree = [_attach_children(r) for r in roots]

    # Warn if any concepts were dropped (e.g. all paths to them were cyclic)
    def _count_tree(nodes: list[dict]) -> int:
        total = 0
        for n in nodes:
            total += 1
            total += _count_tree(n.get("concept", []))
        return total

    emitted = _count_tree(tree)
    if emitted < len(lookup):
        logger.warning(
            "Concept tree emitted %d concepts but lookup has %d — "
            "%d concept(s) unreachable (possible cycle or orphaned subtree).",
            emitted,
            len(lookup),
            len(lookup) - emitted,
        )

    return tree


# ---------------------------------------------------------------------------
# Shared content-complete PUT (used by the SKS and SOR complete assets)
# ---------------------------------------------------------------------------


def _compute_put_timeout(json_size_mb: float) -> int:
    """Compute timeout in seconds for a content-complete CodeSystem PUT.

    Aidbox indexes inline concepts on PUT; processing time scales non-linearly
    with payload size. Small payloads (<10 MB) use the baseline 600s. Larger
    payloads get 45s per additional MB to accommodate server-side indexing.
    """
    baseline = 600
    if json_size_mb <= 10.0:
        return baseline
    return baseline + int((json_size_mb - 10.0) * 45)


def put_content_complete_codesystem(
    context: dg.AssetExecutionContext,
    fhir_server: FHIRServerResource,
    *,
    resource_id: str,
    resource_json: str,
    json_size_mb: float,
    max_retries: int = 3,
    base_backoff: int = 10,
):
    """PUT a content-complete CodeSystem to Aidbox, retried with exponential backoff.

    In hybrid terminology-engine mode Aidbox indexes the inline concepts on PUT,
    which is what makes native ``$lookup`` / ``$validate-code`` resolve. A PUT
    failure must be loud: a direct-PG write would persist the resource row
    WITHOUT the FAR/terminology indexing, silently leaving ``$lookup`` broken.
    The PUT is retried with exponential backoff because the FHIR server may drop
    the connection for large payloads on transient issues.

    Timeout scales with payload size (see ``_compute_put_timeout``).

    Returns the successful response; raises ``dg.Failure`` once retries are
    exhausted.
    """
    url = f"{fhir_server.effective_base_url}/CodeSystem/{resource_id}"
    payload = resource_json.encode("utf-8")
    timeout_seconds = _compute_put_timeout(json_size_mb)

    last_exc: BaseException | None = None
    for attempt in range(1, max_retries + 1):
        context.log.info(
            f"PUT {url} ({json_size_mb} MB, timeout={timeout_seconds}s) "
            f"— attempt {attempt}/{max_retries}"
        )
        try:
            response = fhir_server.session.put(
                url,
                data=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=timeout_seconds,
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
            f"{type(last_exc).__name__}: {last_exc}\n"
            f"Response body: {body}\n"
            "Confirm the FHIR server runs with "
            "BOX_FHIR_TERMINOLOGY_ENGINE=hybrid — a content-complete "
            "CodeSystem PUT returns HTTP 500 in the default/legacy engine."
        )
    ) from last_exc


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_codesystem_sks_complete",
    group_name="fhir_setup",
    deps=["cs_sks"],
    description=(
        "Post content-complete SKS CodeSystem (~70k concepts with hierarchy "
        "and properties inline) to the FHIR server."
    ),
)
def fhir_codesystem_sks_complete(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_server: FHIRServerResource,
) -> dg.MaterializeResult:
    # ------------------------------------------------------------------
    # 1. Read YAML metadata
    # ------------------------------------------------------------------
    yml_path = Path(__file__).parent.parent.parent / "DSA" / "CodeSystem" / "sks" / "codesystem.yml"
    with open(yml_path) as f:
        cs_meta = yaml.safe_load(f)

    # ------------------------------------------------------------------
    # 2. Read the three staging tables
    # ------------------------------------------------------------------
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [code], [display] FROM [codesystem].[SKSConcept] ORDER BY [code]"
            )
            concept_rows = cursor.fetchall()

            cursor.execute(
                "SELECT [code], [property_code], [value], [type] "
                "FROM [codesystem].[SKSProperties] "
                "ORDER BY [code], [property_code], [value]"
            )
            property_rows = cursor.fetchall()

            cursor.execute("SELECT [parent_code], [child_code] FROM [codesystem].[SKSHierarchy]")
            hierarchy_rows = cursor.fetchall()
        finally:
            cursor.close()

    context.log.info(
        f"Read staging tables: {len(concept_rows)} concepts, "
        f"{len(property_rows)} properties, {len(hierarchy_rows)} hierarchy entries"
    )

    if not concept_rows:
        raise dg.Failure(
            description="No concepts found in codesystem.sks_concept — cannot build CodeSystem."
        )

    # ------------------------------------------------------------------
    # 3. Build concept lookup + tree
    # ------------------------------------------------------------------
    lookup = _build_concept_lookup(concept_rows, property_rows)
    concept_tree = _build_concept_tree(lookup, hierarchy_rows)

    root_count = len(concept_tree)
    total_count = len(lookup)
    context.log.info(f"Built tree: {total_count} concepts, {root_count} roots")

    # ------------------------------------------------------------------
    # 4. Assemble full CodeSystem dict
    # ------------------------------------------------------------------
    codesystem_dict: dict = {
        "resourceType": "CodeSystem",
        "id": "sks",
        "url": cs_meta["url"],
        "name": cs_meta["name"],
        "description": cs_meta["description"],
        "status": cs_meta["status"],
        "hierarchyMeaning": cs_meta.get("hierarchyMeaning", "is-a"),
        "publisher": cs_meta.get("publisher", FHIR_PUBLISHER),
        "content": "complete",
        "count": total_count,
        "property": SKS_PROPERTY_DEFINITIONS,
        "concept": concept_tree,
    }

    # ------------------------------------------------------------------
    # 5. Serialize to JSON
    # ------------------------------------------------------------------
    resource_json = json.dumps(codesystem_dict, ensure_ascii=False, separators=(",", ":"))
    json_size_mb = round(len(resource_json.encode("utf-8")) / (1024 * 1024), 1)
    context.log.info(f"Serialized CodeSystem JSON: {json_size_mb} MB")

    # ------------------------------------------------------------------
    # 6. PUT content-complete CodeSystem to Aidbox (retried; shared helper).
    # ------------------------------------------------------------------
    put_content_complete_codesystem(
        context,
        fhir_server,
        resource_id="sks",
        resource_json=resource_json,
        json_size_mb=json_size_mb,
    )

    # ------------------------------------------------------------------
    # 7. Return metadata
    # ------------------------------------------------------------------
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
