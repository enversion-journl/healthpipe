# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ConceptMap Asset: SHAK-a-like to SOR

Authoritative mapping from EPJI location.external_id to the location's
SorIdentifier (location_identifier.value). No fuzzy matching.

A full snapshot each run plus delete_stale MERGE replaces (source, old SOR)
when the identifier value is updated in place without a source timestamp.

Reads arc.epji_location (live rows only, dw_is_deleted = 0) — the same
persistent archive cs_shakalike reads — rather than ext.epji_location
directly, so there is exactly one entry point into the deletion semantics.
Unlike SHAK, which has no archive layer and therefore genuinely has only
ext as a source, SHAK-a-like's archive is the intended source of truth.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.ConceptMap.utils import merge_conceptmap_table, update_conceptmap_dw_ek

_RELATIONSHIP = "equivalent"


def build_shakalike_to_sor_mappings(map_rows: list[tuple]) -> list[tuple]:
    """Build SHAK-a-like -> SOR mapping rows.

    Args:
        map_rows: ``(code, display, sor_code)`` tuples from arc.epji_location
            (live rows only).

    Returns:
        ``(source_code, source_display, relationship, target_code, target_display)``
        tuples — one per code that has a SOR target. target_display is None;
        the official name comes from ref.Organisation / codesystem.SOR when
        dw_ek resolution runs.
    """
    results: list[tuple] = []
    for code, display, sor_code in map_rows:
        source_code = str(code).strip()
        target_code = str(sor_code).strip() if sor_code else ""
        if not source_code or not target_code:
            continue
        results.append(
            (
                source_code,
                (str(display).strip() if display else None) or None,
                _RELATIONSHIP,
                target_code,
                None,
            )
        )
    return results


@dg.asset(
    name="cm_shakalike_to_sor",
    deps=["arc_epji_shakalike", "map_SHAKAlike"],
    group_name="conceptmap",
    compute_kind="mssql",
    tags={"domain": "conceptmap", "layer": "conceptmap", "module": "shakalike_to_sor"},
    description=(
        "Build SHAK-a-like-to-SOR mappings and MERGE into conceptmap.SHAKAlikeToSOR "
        "with delete_stale so in-place SOR identifier updates replace the old target."
    ),
)
def cm_shakalike_to_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT code, display, sor_code FROM [arc].[epji_location] "
                "WHERE dw_is_deleted = 0"
            )
            map_rows = cursor.fetchall()
            context.log.info(f"Read {len(map_rows)} SHAK-a-like rows")

            rows = build_shakalike_to_sor_mappings(map_rows)
            context.log.info(f"Built {len(rows)} mappings")

            result = merge_conceptmap_table(cursor, "SHAKAlikeToSOR", rows, delete_stale=True)
            update_conceptmap_dw_ek(cursor, "SHAKAlikeToSOR")
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"conceptmap.shakalike_to_sor: {result['inserted']} inserted, "
        f"{result['updated']} updated, {result.get('deleted', 0)} deleted"
    )
    return dg.MaterializeResult(metadata=result)
