# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: Okosted to SOR

Reads okosted and SOR codesystem data from codesystem.* tables,
applies fuzzy matching and manual overrides, and MERGEs mappings
into persistent table: conceptmap.okosted_to_sor.

Matching strategy:
1. Top-level: RSD -> Region Syddanmark (6101000016001) via manual CSV
2. Top-level okosted codes (direct children of RSD): fuzzy match to SOR
3. Lower-level codes: fuzzy match, fallback to parent's target with "narrower"
"""

from collections import defaultdict
from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import (
    determine_relationship,
    extract_keywords,
    find_all_descendants,
    find_best_match,
    manual_mappings_from_os2mo_rows,
    merge_conceptmap_table,
    rows_to_concept_dict,
    rows_to_hierarchy_dict,
    rows_to_properties_dict,
    update_conceptmap_dw_ek,
)

# ---------------------------------------------------------------------------
# Matching helpers (okosted-specific root resolution + shared utils imports)
# ---------------------------------------------------------------------------


def _extract_kw(text: str) -> set[str]:
    return extract_keywords(
        text,
        extra_stop_words={"vs", "dki", "fat", "open", "master"},
    )


def _build_child_to_parent_map(hierarchy: dict[str, list[str]]) -> dict[str, str | None]:
    child_to_parent: dict[str, str | None] = {}
    for parent, children in hierarchy.items():
        for child in children:
            child_to_parent[child] = parent
    return child_to_parent


def _get_hierarchy_depth(
    code: str,
    root_code: str,
    child_to_parent: dict[str, str | None],
    depth_cache: dict[str, int],
) -> int:
    if code == root_code:
        return 0
    if code in depth_cache:
        return depth_cache[code]

    depth = 0
    current = code
    visited: set[str] = set()
    while current and current != root_code:
        if current in visited:
            break
        visited.add(current)
        if current in depth_cache:
            depth += depth_cache[current]
            break
        parent = child_to_parent.get(current)
        if parent:
            depth += 1
            current = parent
        else:
            break

    depth_cache[code] = depth
    return depth


def _find_root_code(hierarchy: dict[str, list[str]], concepts: dict[str, str]) -> str | None:
    all_children: set[str] = set()
    for children in hierarchy.values():
        all_children.update(children)
    roots = set(concepts.keys()) - all_children
    if "RSD" in concepts:
        if "RSD" in hierarchy.get("RSD", []):
            return "RSD"
        if "RSD" in roots:
            return "RSD"
    for root in roots:
        if "region" in concepts[root].lower():
            return root
    return next(iter(roots)) if roots else None


# ---------------------------------------------------------------------------
# Build mappings (replaces the temp-CSV + JSON roundtrip)
# ---------------------------------------------------------------------------


def build_okosted_to_sor_mappings(
    source_concepts_rows: list[tuple],
    source_properties_rows: list[tuple],
    source_hierarchy_rows: list[tuple],
    sor_concepts_rows: list[tuple],
    sor_properties_rows: list[tuple],
    sor_hierarchy_rows: list[tuple],
    manual_mappings: dict[str, tuple[str, str]],
) -> list[tuple]:
    """Build okosted-to-SOR mapping rows directly from DB data.

    Returns list of (SourceCode, SourceDisplay, Relationship, TargetCode, TargetDisplay).
    """
    okosted_concepts = rows_to_concept_dict(source_concepts_rows)
    rows_to_properties_dict(source_properties_rows)
    okosted_hierarchy = rows_to_hierarchy_dict(source_hierarchy_rows)
    sor_concepts = rows_to_concept_dict(sor_concepts_rows)
    sor_properties = rows_to_properties_dict(sor_properties_rows)
    sor_hierarchy = rows_to_hierarchy_dict(sor_hierarchy_rows)

    okosted_root = _find_root_code(okosted_hierarchy, okosted_concepts)
    sor_root = "6101000016001"

    sor_root_descendants = find_all_descendants(sor_root, sor_hierarchy)
    sor_root_concepts = {c: sor_concepts[c] for c in sor_root_descendants if c in sor_concepts}

    sor_keywords_cache: dict[str, set[str]] = {
        c: _extract_kw(d) for c, d in sor_root_concepts.items()
    }
    okosted_keywords_cache: dict[str, set[str]] = {
        c: _extract_kw(d) for c, d in okosted_concepts.items() if c != okosted_root
    }

    okosted_child_to_parent = _build_child_to_parent_map(okosted_hierarchy)
    depth_cache: dict[str, int] = {}

    code_mappings: dict[str, str] = {}
    results: list[tuple] = []

    def _append(src_code: str, tgt_code: str, rel: str):
        code_mappings[src_code] = tgt_code
        results.append(
            (
                src_code,
                okosted_concepts.get(src_code) or None,
                rel,
                tgt_code,
                sor_concepts.get(tgt_code) or None,
            )
        )

    # Root mapping
    if okosted_root:
        if okosted_root in manual_mappings:
            tgt, rel = manual_mappings[okosted_root]
            _append(okosted_root, tgt, rel)
        else:
            _append(okosted_root, sor_root, "equal")

    # Group remaining codes by depth
    codes_by_depth: dict[int, list[str]] = defaultdict(list)
    for code in okosted_concepts:
        if code == okosted_root:
            continue
        depth = _get_hierarchy_depth(code, okosted_root or "", okosted_child_to_parent, depth_cache)
        codes_by_depth[depth].append(code)

    for depth in sorted(codes_by_depth):
        for okosted_code in codes_by_depth[depth]:
            okosted_display = okosted_concepts[okosted_code]
            parent_code = okosted_child_to_parent.get(okosted_code)

            if okosted_code in manual_mappings:
                tgt, rel = manual_mappings[okosted_code]
                _append(okosted_code, tgt, rel)
                continue

            target_code: str | None = None
            min_sim = 0.5 if depth == 1 else 0.6
            score_threshold = 0.7 if depth == 1 else 0.8

            match = find_best_match(
                okosted_display,
                sor_root_concepts,
                sor_properties,
                target_keywords_cache=sor_keywords_cache,
                source_keywords=okosted_keywords_cache.get(okosted_code),
                min_similarity=min_sim,
                extract_kw=_extract_kw,
            )
            if match:
                matched_code, score = match
                if score >= score_threshold:
                    target_code = matched_code
                    rel = determine_relationship(score)
                    _append(okosted_code, target_code, rel)
                    continue

            if parent_code and parent_code in code_mappings:
                target_code = code_mappings[parent_code]
            else:
                target_code = sor_root
            _append(okosted_code, target_code, "narrower")

    return results


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    name="cm_okosted_to_sor",
    deps=["cs_okosted", "cs_sor"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "okosted_to_sor"},
    description="Build okosted-to-sor mappings and MERGE into conceptmap.okosted_to_sor",
)
def cm_okosted_to_sor(
    context: dg.AssetExecutionContext,
    ext_os2mo_okosted_to_sor: list[dict[str, Any]],
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[OkostedConcept]")
            source_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[OkostedProperties]")
            source_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[OkostedHierarchy]")
            source_hierarchy = cursor.fetchall()

            cursor.execute("SELECT Code, Display FROM [codesystem].[SORConcept]")
            sor_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[SORProperties]")
            sor_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[SORHierarchy]")
            sor_hierarchy = cursor.fetchall()

            context.log.info(
                f"Read {len(source_concepts)} okosted concepts, "
                f"{len(sor_concepts)} SOR concepts"
            )

            manual = manual_mappings_from_os2mo_rows(ext_os2mo_okosted_to_sor)
            rows = build_okosted_to_sor_mappings(
                source_concepts,
                source_properties,
                source_hierarchy,
                sor_concepts,
                sor_properties,
                sor_hierarchy,
                manual,
            )
            context.log.info(f"Built {len(rows)} mappings")

            result = merge_conceptmap_table(cursor, "OkostedToSOR", rows)
            update_conceptmap_dw_ek(cursor, "OkostedToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.okosted_to_sor: {result['inserted']} inserted, " f"{result['updated']} updated"
    )
    return dg.MaterializeResult(metadata=result)
