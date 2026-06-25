# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: EAN to SOR

Reads EAN and SOR codesystem data from codesystem.* tables,
applies fuzzy matching and manual overrides, and MERGEs mappings
into persistent table: conceptmap.ean_to_sor.

Matching strategy:
1. Extract known mappings from SOR properties where KEY='eanCode'
2. Apply any manual overrides from the DSI ext_os2mo_ean_to_sor extract
3. Fuzzy match remaining EAN codes against SOR concepts under the Region Syddanmark
   hierarchy root (scoped subtree), using a keyword index to avoid scanning every pair.
"""

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import (
    determine_relationship,
    extract_keywords,
    find_all_descendants,
    manual_mappings_from_os2mo_rows,
    merge_conceptmap_table,
    rows_to_concept_dict,
    rows_to_hierarchy_dict,
    rows_to_properties_dict,
    semantic_similarity,
    update_conceptmap_dw_ek,
)

# ---------------------------------------------------------------------------
# Matching helpers (inlined from former matching.py)
# ---------------------------------------------------------------------------

_SHORT_DISPLAY_MAX = 10


def _extract_kw_ean(text: str) -> set[str]:
    return extract_keywords(
        text,
        extra_stop_words={"vs", "dki", "fat", "master"},
        important_acronyms={"fam", "shs", "ouh", "slb", "svs", "rsd", "open"},
    )


def _similarity(a: str, b: str) -> float:
    return semantic_similarity(a.lower(), b.lower(), extract_kw=_extract_kw_ean)


def _build_keyword_index(sor_keywords_cache: dict[str, set[str]]) -> dict[str, set[str]]:
    """Map each keyword to SOR codes whose display contains that keyword."""
    index: dict[str, set[str]] = defaultdict(set)
    for code, kws in sor_keywords_cache.items():
        for kw in kws:
            index[kw].add(code)
    return dict(index)


def _candidate_codes_for_ean(
    source_keywords: set[str],
    source_display: str,
    keyword_index: dict[str, set[str]],
    all_scoped_codes: set[str],
) -> set[str]:
    """
    Restrict fuzzy-match targets using keyword overlap.

    Short displays keep the full scoped set so behaviour stays aligned with the legacy
    path that still runs SequenceMatcher when both strings are short and share no token.
    """
    if len(source_display.strip()) <= _SHORT_DISPLAY_MAX:
        return all_scoped_codes
    if not source_keywords:
        return all_scoped_codes
    candidates: set[str] = set()
    for kw in source_keywords:
        candidates |= keyword_index.get(kw, set())
    return candidates if candidates else all_scoped_codes


def _sort_candidates_by_keyword_overlap(
    source_keywords: set[str],
    candidate_codes: set[str],
    sor_keywords_cache: dict[str, set[str]],
    sor_scoped_concepts: dict[str, str],
) -> list[tuple[str, str]]:
    """Highest keyword overlap first so we tend to hit strong matches and exit early."""
    pairs: list[tuple[str, str]] = []
    for code in candidate_codes:
        display = sor_scoped_concepts.get(code, "")
        tgt_kw = sor_keywords_cache.get(code, set())
        overlap = len(source_keywords & tgt_kw)
        pairs.append((code, display, overlap))
    pairs.sort(key=lambda x: -x[2])
    return [(c, d) for c, d, _ in pairs]


def _find_best_match(
    source_display: str,
    target_concepts: dict[str, str],
    target_keywords_cache: dict[str, set[str]] | None = None,
    source_keywords: set[str] | None = None,
    min_similarity: float = 0.3,
    *,
    ordered_pairs: list[tuple[str, str]] | None = None,
) -> tuple[str, float] | None:
    best_match: tuple[str, float] | None = None
    best_score = 0.0
    if source_keywords is None:
        source_keywords = _extract_kw_ean(source_display)
    source_lower = source_display.lower()

    iterator: Iterable[tuple[str, str]]
    if ordered_pairs is not None:
        iterator = ordered_pairs
    else:
        iterator = target_concepts.items()

    for target_code, target_display in iterator:
        if best_score >= 0.95:
            break
        target_lower = target_display.lower()
        if source_lower == target_lower:
            return (target_code, 1.0)

        if target_keywords_cache and target_code in target_keywords_cache:
            target_kw = target_keywords_cache[target_code]
        else:
            target_kw = _extract_kw_ean(target_display)
            if target_keywords_cache is not None:
                target_keywords_cache[target_code] = target_kw

        if source_keywords and target_kw:
            if len(source_keywords & target_kw) == 0 and (
                len(source_lower) > 10 or len(target_lower) > 10
            ):
                continue

        score = semantic_similarity(
            source_lower,
            target_lower,
            source_keywords,
            target_kw,
            extract_kw=_extract_kw_ean,
        )
        if score > best_score and score >= min_similarity:
            best_score = score
            best_match = (target_code, score)
    return best_match


def _extract_known_ean_mappings(
    sor_properties: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Extract known EAN-to-SOR mappings from SOR properties where KEY='eanCode'."""
    ean_to_sor: dict[str, str] = {}
    for sor_code, props in sor_properties.items():
        ean_value = props.get("eanCode", "")
        if ean_value:
            ean_value = ean_value.replace('="', "").replace('"', "").strip()
            if ean_value:
                ean_to_sor[ean_value] = sor_code
    return ean_to_sor


# ---------------------------------------------------------------------------
# Build mappings (replaces the temp-CSV + JSON roundtrip)
# ---------------------------------------------------------------------------

_SOR_ROOT = "6101000016001"


def build_ean_to_sor_mappings(
    source_concepts_rows: list[tuple],
    source_properties_rows: list[tuple],
    source_hierarchy_rows: list[tuple],
    sor_concepts_rows: list[tuple],
    sor_properties_rows: list[tuple],
    sor_hierarchy_rows: list[tuple],
    manual_mappings: dict[str, tuple[str, str]],
) -> list[tuple]:
    ean_concepts = rows_to_concept_dict(source_concepts_rows)
    rows_to_hierarchy_dict(source_hierarchy_rows)
    sor_concepts = rows_to_concept_dict(sor_concepts_rows)
    sor_properties = rows_to_properties_dict(sor_properties_rows)
    sor_hierarchy = rows_to_hierarchy_dict(sor_hierarchy_rows)

    sor_root_descendants = find_all_descendants(_SOR_ROOT, sor_hierarchy)
    sor_scoped_concepts = {c: sor_concepts[c] for c in sor_root_descendants if c in sor_concepts}

    csv_mappings = manual_mappings

    known_mappings = _extract_known_ean_mappings(sor_properties)
    for ean_code, (sor_code, _) in csv_mappings.items():
        known_mappings[ean_code] = sor_code

    sor_keywords_cache: dict[str, set[str]] = {
        c: _extract_kw_ean(d) for c, d in sor_scoped_concepts.items()
    }
    keyword_index = _build_keyword_index(sor_keywords_cache)
    scoped_codes = set(sor_scoped_concepts.keys())

    known_in_system = {ean: sor for ean, sor in known_mappings.items() if ean in ean_concepts}
    ean_to_match = [(c, d) for c, d in ean_concepts.items() if c not in known_in_system]

    results: list[tuple] = []

    # Known mappings
    for ean_code, target_code in known_in_system.items():
        ean_display = ean_concepts[ean_code]
        tgt_display = sor_concepts.get(target_code, "")
        score = _similarity(ean_display, tgt_display) if tgt_display else 0.95
        if ean_code in csv_mappings:
            _, rel = csv_mappings[ean_code]
        else:
            rel = determine_relationship(score, is_known_mapping=True)
        results.append(
            (
                ean_code,
                ean_display or None,
                rel,
                target_code,
                tgt_display or None,
            )
        )

    # Fuzzy matching for remaining EAN codes — scoped to Region Syddanmark subtree
    ean_kw_cache: dict[str, set[str]] = {c: _extract_kw_ean(d) for c, d in ean_to_match}
    for ean_code, ean_display in ean_to_match:
        src_kw = ean_kw_cache[ean_code]
        block = _candidate_codes_for_ean(src_kw, ean_display, keyword_index, scoped_codes)
        ordered = _sort_candidates_by_keyword_overlap(
            src_kw, block, sor_keywords_cache, sor_scoped_concepts
        )
        match = _find_best_match(
            ean_display,
            sor_scoped_concepts,
            target_keywords_cache=sor_keywords_cache,
            source_keywords=src_kw,
            min_similarity=0.5,
            ordered_pairs=ordered,
        )
        if match:
            tgt_code, score = match
            rel = determine_relationship(score)
            results.append(
                (
                    ean_code,
                    ean_display or None,
                    rel,
                    tgt_code,
                    sor_concepts.get(tgt_code) or None,
                )
            )

    return results


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    name="cm_ean_to_sor",
    deps=["cs_ean", "cs_sor"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "ean_to_sor"},
    description="Build ean-to-sor mappings and MERGE into conceptmap.ean_to_sor",
)
def cm_ean_to_sor(
    context: dg.AssetExecutionContext,
    ext_os2mo_ean_to_sor: list[dict[str, Any]],
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[EANConcept]")
            source_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[EANProperties]")
            source_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[EANHierarchy]")
            source_hierarchy = cursor.fetchall()

            cursor.execute("SELECT Code, Display FROM [codesystem].[SORConcept]")
            sor_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[SORProperties]")
            sor_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[SORHierarchy]")
            sor_hierarchy = cursor.fetchall()

            context.log.info(
                f"Read {len(source_concepts)} EAN concepts, {len(sor_concepts)} SOR concepts"
            )

            manual = manual_mappings_from_os2mo_rows(ext_os2mo_ean_to_sor)
            rows = build_ean_to_sor_mappings(
                source_concepts,
                source_properties,
                source_hierarchy,
                sor_concepts,
                sor_properties,
                sor_hierarchy,
                manual,
            )
            context.log.info(f"Built {len(rows)} mappings")

            result = merge_conceptmap_table(cursor, "EANToSOR", rows)
            update_conceptmap_dw_ek(cursor, "EANToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.ean_to_sor: {result['inserted']} inserted, " f"{result['updated']} updated"
    )
    return dg.MaterializeResult(metadata=result)
