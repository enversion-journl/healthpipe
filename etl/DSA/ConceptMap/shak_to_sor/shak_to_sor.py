# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: SHAK to SOR

Unlike the fuzzy-matched ERP conceptmaps, the SHAK->SOR mapping is authoritative:
SHAKSORMap.csv already carries the newest direct SHAK->SOR mapping per SHAK code
(selected in extract_shak_register). This builder reads that mapping straight
from ext.shaksor_map — including the SOR organisation name (SorDisplay) — so the
result has exactly one row per SHAK code.

Target display comes from ext.shaksor_map (SorEntityName), NOT from the SOR
codesystem: ~56% of SHAK codes map to CLOSED SOR entities that are absent from
the daily-loaded codesystem.SOR* / ref.Organisation. dw_ek_Organisation_target
still resolves against ref.Organisation for active orgs (NULL -> -1 for closed
ones); the dedup view keeps those rows so no SHAK code is dropped.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import merge_conceptmap_table, update_conceptmap_dw_ek

# Direct mappings are the authoritative crosswalk; the rare indirect fallback
# (a SHAK code with no direct candidate at all) is downgraded to 'inexact' so
# consumers can distinguish it.
_RELATIONSHIP_DIRECT = "equivalent"
_RELATIONSHIP_INDIRECT = "inexact"


def build_shak_to_sor_mappings(shak_map_rows: list[tuple]) -> list[tuple]:
    """Build SHAK->SOR mapping rows.

    Args:
        shak_map_rows: ``(Code, display, SorCode, SorDisplay, isDirect)`` tuples
            from ext.shaksor_map.

    Returns:
        ``(source_code, source_display, relationship, target_code, target_display)``
        tuples — one per SHAK code with a SOR target.
    """
    results: list[tuple] = []
    for code, display, sor_code, sor_display, is_direct in shak_map_rows:
        source_code = str(code).strip()
        target_code = str(sor_code).strip() if sor_code else ""
        if not source_code or not target_code:
            continue
        direct = str(is_direct or "").strip().lower() == "true"
        results.append(
            (
                source_code,
                (str(display).strip() if display else None) or None,
                _RELATIONSHIP_DIRECT if direct else _RELATIONSHIP_INDIRECT,
                target_code,
                (str(sor_display).strip() if sor_display else None) or None,
            )
        )
    return results


@dg.asset(
    name="cm_shak_to_sor",
    deps=["extract_shak_register", "map_SHAK"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "shak_to_sor"},
    description="Build authoritative SHAK-to-SOR mappings and MERGE into conceptmap.SHAKToSOR",
)
def cm_shak_to_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT Code, display, SorCode, SorDisplay, isDirect FROM [ext].[shaksor_map]"
            )
            shak_map_rows = cursor.fetchall()
            context.log.info(f"Read {len(shak_map_rows)} SHAK mappings")

            rows = build_shak_to_sor_mappings(shak_map_rows)
            context.log.info(f"Built {len(rows)} mappings")

            # Full snapshot each run: delete_stale removes an old
            # (source_code, target_code) pair when a SHAK code changes SOR target,
            # so the raw table (read by the FHIR publisher) stays 1:1 per SHAK.
            result = merge_conceptmap_table(cursor, "SHAKToSOR", rows, delete_stale=True)
            update_conceptmap_dw_ek(cursor, "SHAKToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.shak_to_sor: {result['inserted']} inserted, {result['updated']} updated"
    )
    return dg.MaterializeResult(metadata=result)
