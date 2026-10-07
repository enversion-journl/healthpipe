# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: Rekvirent to SOR

Reads rekvirent and SOR codesystem data from codesystem.* tables,
applies fuzzy matching and manual overrides, and MERGEs mappings
into persistent table: conceptmap.rekvirent_to_sor.

Matching strategy:
1. Top-level: REGION_SYD -> Example Region (6101000016001) - "equal"
2. Hospital level: SGH_* -> SOR hospitals using manual CSV mappings
3. rgrp level: fuzzy match within parent hospital's SOR descendants
4. rtyp / rnr: inherit parent's target with "narrower"
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
    get_parent_code,
    manual_mappings_from_os2mo_rows,
    merge_conceptmap_table,
    rows_to_concept_dict,
    rows_to_hierarchy_dict,
    rows_to_properties_dict,
    update_conceptmap_dw_ek,
)

# ---------------------------------------------------------------------------
# Domain-specific keyword sets for semantic penalty / boost
# ---------------------------------------------------------------------------

DEPARTMENT_KEYWORDS = {
    "afdeling",
    "afsnit",
    "ambulatorium",
    "amb",
    "klinik",
    "ambulatorie",
    "kirurgi",
    "kirurgisk",
    "medicinsk",
    "laboratorie",
    "laboratorietorvet",
    "røntgen",
    "patologi",
    "anæstesi",
    "intensiv",
    "geriatrisk",
    "demensklinik",
    "onkologi",
    "kræft",
    "ortopædi",
    "ortopædisk",
    "venekirurgi",
    "organ",
    "genetisk",
    "øjenlæge",
    "fodterapeut",
    "psyk",
    "psykiatrisk",
    "urologisk",
    "ergoterapi",
    "fysioterapi",
    "diætist",
    "diabetes",
    "hormon",
    "hjerte",
    "nyre",
    "lunge",
    "mave",
    "tarm",
    "bryst",
    "plastik",
    "karkirurgi",
    "kvindesygdomme",
    "børn",
    "unge",
    "nyfødt",
    "familie",
    "graviditet",
    "kæbekirurgi",
    "øre",
    "næse",
    "hals",
    "høre",
    "stroke",
    "sklerose",
    "gigt",
    "bindevæv",
    "dialyse",
    "lindende",
    "behandling",
}

ADMIN_KEYWORDS = {
    "logistik",
    "indkøb",
    "forvaltning",
    "it",
    "edb",
    "kvalitet",
    "tilsyn",
    "klinisk it",
    "sundhedsstab",
    "direktion",
    "service",
    "servicecenter",
    "køkken",
    "forskning",
    "uddannelse",
    "innovation",
    "regulering",
    "forberedelse",
    "flytning",
    "diagnostik",
    "konti",
    "projekt",
    "master",
    "utensilier",
    "praksis",
    "sterilcentral",
    "steril",
    "akutmodtagelse",
    "akut",
    "modtagelse",
    "fælles",
}

NON_DEPARTMENT_KEYWORDS = {
    "lager",
    "depot",
    "centrallager",
    "sterillager",
    "sikkerhedslager",
    "skaffevarer",
    "fødevarer",
    "kontorartikler",
    "værnemidler",
    "behandlingskritiske",
    "stamdata",
    "lagerhotel",
}


# ---------------------------------------------------------------------------
# Matching helpers (domain-specific keyword tuning + shared utils imports)
# ---------------------------------------------------------------------------


def _extract_kw(text: str) -> set[str]:
    return extract_keywords(
        text,
        extra_stop_words={"rsd", "vs", "fam", "dki", "fat", "open", "master"},
    )


def _rekvirent_scoring_adjuster(source_lower: str, target_lower: str) -> float:
    source_is_dept = any(kw in source_lower for kw in DEPARTMENT_KEYWORDS)
    source_is_admin = any(kw in source_lower for kw in ADMIN_KEYWORDS)
    source_is_non_dept = any(kw in source_lower for kw in NON_DEPARTMENT_KEYWORDS)
    target_is_dept = any(kw in target_lower for kw in DEPARTMENT_KEYWORDS)
    target_is_admin = any(kw in target_lower for kw in ADMIN_KEYWORDS)
    target_is_non_dept = any(kw in target_lower for kw in NON_DEPARTMENT_KEYWORDS)

    penalty = 0.0
    if source_is_non_dept and (target_is_dept or target_is_admin):
        penalty = 0.5
    elif (source_is_dept or source_is_admin) and target_is_non_dept:
        penalty = 0.5

    boost = 0.0
    if source_is_dept and target_is_dept:
        boost = 0.1
    elif source_is_admin and target_is_admin:
        boost = 0.05

    return -penalty + boost


# ---------------------------------------------------------------------------
# Build mappings (replaces the temp-CSV + JSON roundtrip)
# ---------------------------------------------------------------------------


def build_rekvirent_to_sor_mappings(
    source_concepts_rows: list[tuple],
    source_properties_rows: list[tuple],
    source_hierarchy_rows: list[tuple],
    sor_concepts_rows: list[tuple],
    sor_properties_rows: list[tuple],
    sor_hierarchy_rows: list[tuple],
    manual_mappings: dict[str, tuple[str, str]],
) -> list[tuple]:
    rekv_concepts = rows_to_concept_dict(source_concepts_rows)
    rows_to_properties_dict(source_properties_rows)
    rekv_hierarchy = rows_to_hierarchy_dict(source_hierarchy_rows)
    sor_concepts = rows_to_concept_dict(sor_concepts_rows)
    sor_properties = rows_to_properties_dict(sor_properties_rows)
    sor_hierarchy = rows_to_hierarchy_dict(sor_hierarchy_rows)

    csv_mappings = manual_mappings

    rekv_root = find_root_code_region_syd(rekv_hierarchy, rekv_concepts)
    sor_root = "6101000016001"

    sor_keywords_cache: dict[str, set[str]] = {c: _extract_kw(d) for c, d in sor_concepts.items()}
    rekv_keywords_cache: dict[str, set[str]] = {c: _extract_kw(d) for c, d in rekv_concepts.items()}

    results: list[tuple] = []

    def _append(src: str, tgt: str, rel: str):
        results.append(
            (
                src,
                rekv_concepts.get(src) or None,
                rel,
                tgt,
                sor_concepts.get(tgt) or None,
            )
        )

    # 1. Root
    if rekv_root:
        _append(rekv_root, sor_root, "equal")

    # 2. Hospitals (SGH_*)
    hospital_mapping: dict[str, str] = {}
    sgh_codes = [c for c in rekv_concepts if c.startswith("SGH_")]

    for sgh_code in sgh_codes:
        sgh_display = rekv_concepts[sgh_code]
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
                source_keywords=rekv_keywords_cache.get(sgh_code),
                entity_type_filter="hospital",
                min_similarity=0.5,
                extract_kw=_extract_kw,
                scoring_adjuster=_rekvirent_scoring_adjuster,
                clamp_upper=True,
            )
            if match:
                tgt, score = match
                hospital_mapping[sgh_code] = tgt
                _append(sgh_code, tgt, determine_relationship(score))
            else:
                hospital_mapping[sgh_code] = sor_root
                _append(sgh_code, sor_root, "narrower")

    # 3. rgrp codes
    rgrp_codes = [c for c in rekv_concepts if c.startswith("RGRP_")]
    for rgrp_code in rgrp_codes:
        rgrp_display = rekv_concepts[rgrp_code]

        if rgrp_code in csv_mappings:
            tgt, rel = csv_mappings[rgrp_code]
            _append(rgrp_code, tgt, rel)
            continue

        parent_sgh = get_parent_code(rgrp_code, rekv_hierarchy)
        target_code: str | None = None

        if parent_sgh and parent_sgh in hospital_mapping:
            parent_sor = hospital_mapping[parent_sgh]
            parent_descendants = find_all_descendants(parent_sor, sor_hierarchy)
            parent_sor_concepts = {
                c: sor_concepts[c] for c in parent_descendants if c in sor_concepts
            }
            match = find_best_match(
                rgrp_display,
                parent_sor_concepts,
                sor_properties,
                target_keywords_cache=sor_keywords_cache,
                source_keywords=rekv_keywords_cache.get(rgrp_code),
                min_similarity=0.5,
                extract_kw=_extract_kw,
                scoring_adjuster=_rekvirent_scoring_adjuster,
                clamp_upper=True,
            )
            if match:
                best_code, best_score = match
                if best_score >= 0.7:
                    target_code = best_code
                    rel = determine_relationship(best_score)
                    if rel == "inexact" and best_score >= 0.75:
                        rel = "narrower"
                    _append(rgrp_code, target_code, rel)
                    continue

            target_code = parent_sor
        else:
            target_code = sor_root
        _append(rgrp_code, target_code, "narrower")

    # 4. rtyp codes — inherit parent rgrp target
    rtyp_codes = [c for c in rekv_concepts if c.startswith("RTYP_")]
    element_map = {r[0]: r[3] for r in results}  # src -> tgt for lookup

    for rtyp_code in rtyp_codes:
        parent = get_parent_code(rtyp_code, rekv_hierarchy)
        tgt = element_map.get(parent, sor_root) if parent else sor_root
        _append(rtyp_code, tgt, "narrower")
        element_map[rtyp_code] = tgt

    # 5. rnr codes — inherit parent target
    rnr_codes = [c for c in rekv_concepts if c.startswith("RNR_")]
    for rnr_code in rnr_codes:
        parent = get_parent_code(rnr_code, rekv_hierarchy)
        tgt = element_map.get(parent, sor_root) if parent else sor_root
        _append(rnr_code, tgt, "narrower")
        element_map[rnr_code] = tgt

    return results


# ---------------------------------------------------------------------------
# Dagster asset
# ---------------------------------------------------------------------------


@dg.asset(
    name="cm_rekvirent_to_sor",
    deps=["cs_rekvirent", "cs_sor"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "rekvirent_to_sor"},
    description="Build rekvirent-to-sor mappings and MERGE into conceptmap.rekvirent_to_sor",
)
def cm_rekvirent_to_sor(
    context: dg.AssetExecutionContext,
    ext_os2mo_rekvirent_to_sor: list[dict[str, Any]],
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT code, display FROM [codesystem].[RekvirentConcept]")
            source_concepts = cursor.fetchall()

            cursor.execute(
                "SELECT code, property_code, value FROM [codesystem].[RekvirentProperties]"
            )
            source_properties = cursor.fetchall()

            cursor.execute("SELECT parent_code, child_code FROM [codesystem].[RekvirentHierarchy]")
            source_hierarchy = cursor.fetchall()

            cursor.execute("SELECT code, display FROM [codesystem].[SORConcept]")
            sor_concepts = cursor.fetchall()

            cursor.execute("SELECT code, property_code, value FROM [codesystem].[SORProperties]")
            sor_properties = cursor.fetchall()

            cursor.execute("SELECT parent_code, child_code FROM [codesystem].[SORHierarchy]")
            sor_hierarchy = cursor.fetchall()

            context.log.info(
                f"Read {len(source_concepts)} rekvirent concepts, "
                f"{len(sor_concepts)} SOR concepts"
            )

            manual = manual_mappings_from_os2mo_rows(ext_os2mo_rekvirent_to_sor)
            rows = build_rekvirent_to_sor_mappings(
                source_concepts,
                source_properties,
                source_hierarchy,
                sor_concepts,
                sor_properties,
                sor_hierarchy,
                manual,
            )
            context.log.info(f"Built {len(rows)} mappings")

            result = merge_conceptmap_table(cursor, "RekvirentToSOR", rows)
            update_conceptmap_dw_ek(cursor, "RekvirentToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.rekvirent_to_sor: {result['inserted']} inserted, "
        f"{result['updated']} updated"
    )
    return dg.MaterializeResult(metadata=result)
