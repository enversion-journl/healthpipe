# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: Lager to SOR

Reads lager and SOR codesystem data from codesystem.* tables,
applies fuzzy matching and manual overrides, and MERGEs mappings
into persistent table: conceptmap.lager_to_sor.

Matching strategy:
1. Top-level: REGION_SYD -> Region Syddanmark (6101000016001) - "equal"
2. Hospital level: SGH_* -> SOR hospitals using manual CSV mappings
3. Lager level: LGR_* -> specific SOR codes or parent hospital with "narrower"
"""

from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import (
    determine_relationship,
    extract_keywords,
    find_all_descendants,
    find_best_match,
    find_root_code_region_syd,
    manual_mappings_from_os2mo_rows,
    merge_conceptmap_table,
    rows_to_concept_dict,
    rows_to_hierarchy_dict,
    rows_to_properties_dict,
    update_conceptmap_dw_ek,
)

# ---------------------------------------------------------------------------
# Domain-specific keyword sets for semantic penalty
# ---------------------------------------------------------------------------

WAREHOUSE_KEYWORDS = {
    "lager",
    "depot",
    "indkøb",
    "indkob",
    "steril",
    "sterillager",
    "centrallager",
    "fremskudt",
    "skaffevarer",
    "sikkerhedslager",
    "bygningsdrift",
    "service",
    "medicoteknik",
    "utensilier",
    "praksis",
    "stamdata",
    "lagerhotel",
    "værnemidler",
    "behandlingskritiske",
    "genbehandling",
    "fødevarer",
    "kontorartikler",
    "laboratorie",
    "laboratorietorvet",
    "teknisk",
    "papirdepot",
    "linneddepot",
}

NON_WAREHOUSE_KEYWORDS = {
    "ambulatorium",
    "amb",
    "klinik",
    "afdeling",
    "afsnit",
    "ambulatorie",
    "behandling",
    "patient",
    "sygehus",
    "direktion",
    "forskning",
    "forskningsenhed",
    "demensklinik",
    "geriatrisk",
    "patologisk",
    "anæstesi",
    "intensiv",
}


# ---------------------------------------------------------------------------
# Matching helpers (domain-specific keyword tuning + shared utils imports)
# ---------------------------------------------------------------------------

_LAGER_STOP_WORDS = {
    "og",
    "af",
    "i",
    "på",
    "til",
    "for",
    "med",
    "den",
    "det",
    "de",
    "en",
    "et",
    "ouh",
    "shs",
    "slb",
    "svs",
    "rsd",
}


def _extract_kw(text: str) -> set[str]:
    return extract_keywords(text, stop_words=_LAGER_STOP_WORDS)


def _lager_scoring_adjuster(source_lower: str, target_lower: str) -> float:
    src_wh = any(kw in source_lower for kw in WAREHOUSE_KEYWORDS)
    src_non_wh = any(kw in source_lower for kw in NON_WAREHOUSE_KEYWORDS)
    tgt_wh = any(kw in target_lower for kw in WAREHOUSE_KEYWORDS)
    tgt_non_wh = any(kw in target_lower for kw in NON_WAREHOUSE_KEYWORDS)

    penalty = 0.0
    if src_wh and tgt_non_wh:
        penalty = 0.5
    elif src_non_wh and tgt_wh:
        penalty = 0.5
    elif src_wh and not tgt_wh and not tgt_non_wh:
        penalty = 0.3
    elif tgt_wh and not src_wh and not src_non_wh:
        penalty = 0.3

    return -penalty


# ---------------------------------------------------------------------------
# Build mappings (replaces the temp-CSV + JSON roundtrip)
# ---------------------------------------------------------------------------


def build_lager_to_sor_mappings(
    source_concepts_rows: list[tuple],
    source_properties_rows: list[tuple],
    source_hierarchy_rows: list[tuple],
    sor_concepts_rows: list[tuple],
    sor_properties_rows: list[tuple],
    sor_hierarchy_rows: list[tuple],
    manual_mappings: dict[str, tuple[str, str]],
) -> list[tuple]:
    lager_concepts = rows_to_concept_dict(source_concepts_rows)
    lager_properties = rows_to_properties_dict(source_properties_rows)
    lager_hierarchy = rows_to_hierarchy_dict(source_hierarchy_rows)
    sor_concepts = rows_to_concept_dict(sor_concepts_rows)
    sor_properties = rows_to_properties_dict(sor_properties_rows)
    sor_hierarchy = rows_to_hierarchy_dict(sor_hierarchy_rows)

    csv_mappings = manual_mappings

    lager_root = find_root_code_region_syd(lager_hierarchy, lager_concepts)
    sor_root = "6101000016001"

    sor_keywords_cache: dict[str, set[str]] = {c: _extract_kw(d) for c, d in sor_concepts.items()}
    lager_keywords_cache: dict[str, set[str]] = {
        c: _extract_kw(d) for c, d in lager_concepts.items()
    }

    results: list[tuple] = []

    def _append(src: str, tgt: str, rel: str):
        results.append(
            (
                src,
                lager_concepts.get(src) or None,
                rel,
                tgt,
                sor_concepts.get(tgt) or None,
            )
        )

    # 1. Root
    if lager_root:
        _append(lager_root, sor_root, "equal")

    # 2. Hospitals (entityType=sygehus)
    hospital_mapping: dict[str, str] = {}
    sgh_codes = [
        c for c in lager_concepts if lager_properties.get(c, {}).get("entityType") == "sygehus"
    ]

    for sgh_code in sgh_codes:
        sgh_display = lager_concepts[sgh_code]
        if sgh_code in csv_mappings:
            tgt, rel = csv_mappings[sgh_code]
            hospital_mapping[sgh_code] = tgt
            _append(sgh_code, tgt, rel)
        else:
            match = find_best_match(
                sgh_display,
                sor_concepts,
                sor_properties,
                target_keywords_cache=sor_keywords_cache,
                source_keywords=lager_keywords_cache.get(sgh_code),
                entity_type_filter="hospital",
                min_similarity=0.5,
                extract_kw=_extract_kw,
                scoring_adjuster=_lager_scoring_adjuster,
                clamp_upper=False,
            )
            if match:
                tgt, score = match
                hospital_mapping[sgh_code] = tgt
                _append(sgh_code, tgt, determine_relationship(score))
            else:
                hospital_mapping[sgh_code] = sor_root
                _append(sgh_code, sor_root, "narrower")

    # 3. Lager codes (entityType=lager)
    lgr_codes = [
        c for c in lager_concepts if lager_properties.get(c, {}).get("entityType") == "lager"
    ]

    for lgr_code in lgr_codes:
        lgr_display = lager_concepts[lgr_code]

        if lgr_code in csv_mappings:
            tgt, rel = csv_mappings[lgr_code]
            _append(lgr_code, tgt, rel)
            continue

        parent_sgh_num = lager_properties.get(lgr_code, {}).get("sgh")
        parent_sgh = f"SGH_{parent_sgh_num}" if parent_sgh_num else None
        target_code: str | None = None

        if parent_sgh and parent_sgh in hospital_mapping:
            parent_sor = hospital_mapping[parent_sgh]
            parent_descendants = find_all_descendants(parent_sor, sor_hierarchy)
            parent_sor_concepts = {
                c: sor_concepts[c] for c in parent_descendants if c in sor_concepts
            }
            match = find_best_match(
                lgr_display,
                parent_sor_concepts,
                sor_properties,
                target_keywords_cache=sor_keywords_cache,
                source_keywords=lager_keywords_cache.get(lgr_code),
                min_similarity=0.5,
                extract_kw=_extract_kw,
                scoring_adjuster=_lager_scoring_adjuster,
                clamp_upper=False,
            )
            if match:
                best_code, best_score = match
                if best_score >= 0.7:
                    target_code = best_code
                    rel = determine_relationship(best_score)
                    if rel == "inexact" and best_score >= 0.75:
                        rel = "narrower"
                    _append(lgr_code, target_code, rel)
                    continue

            target_code = parent_sor
        else:
            target_code = sor_root
        _append(lgr_code, target_code, "narrower")

    return results


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    name="cm_lager_to_sor",
    deps=["cs_lager", "cs_sor"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "lager_to_sor"},
    description="Build lager-to-sor mappings and MERGE into conceptmap.lager_to_sor",
)
def cm_lager_to_sor(
    context: dg.AssetExecutionContext,
    ext_os2mo_lager_to_sor: list[dict[str, Any]],
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[LagerConcept]")
            source_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[LagerProperties]")
            source_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[LagerHierarchy]")
            source_hierarchy = cursor.fetchall()

            cursor.execute("SELECT Code, Display FROM [codesystem].[SORConcept]")
            sor_concepts = cursor.fetchall()

            cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[SORProperties]")
            sor_properties = cursor.fetchall()

            cursor.execute("SELECT ParentCode, ChildCode FROM [codesystem].[SORHierarchy]")
            sor_hierarchy = cursor.fetchall()

            context.log.info(
                f"Read {len(source_concepts)} lager concepts, " f"{len(sor_concepts)} SOR concepts"
            )

            manual = manual_mappings_from_os2mo_rows(ext_os2mo_lager_to_sor)
            rows = build_lager_to_sor_mappings(
                source_concepts,
                source_properties,
                source_hierarchy,
                sor_concepts,
                sor_properties,
                sor_hierarchy,
                manual,
            )
            context.log.info(f"Built {len(rows)} mappings")

            result = merge_conceptmap_table(cursor, "LagerToSOR", rows)
            update_conceptmap_dw_ek(cursor, "LagerToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.lager_to_sor: {result['inserted']} inserted, " f"{result['updated']} updated"
    )
    return dg.MaterializeResult(metadata=result)
