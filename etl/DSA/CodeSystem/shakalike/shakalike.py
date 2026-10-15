# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SHAK-a-like (EPJI location.external_id)

Reads arc.epji_location (the persistent archive, MERGEd from the
ext_epji_location full snapshot by arc_epji_shakalike) and MERGEs into
codesystem.SHAKAlikeConcept / _Properties / _Hierarchy. Only live rows
(dw_is_deleted = 0) are read — a location soft-deleted in the archive stops
feeding new concepts/properties, though its existing codesystem entry is
left in place (see below).

SHAK-a-like and real SHAK are separate code systems that happen to model
the same kind of hospital/department/section hierarchy — EPJI's own
identifiers, not SHAK identifiers. SHAK's codes concatenate digits
implicitly (``8026`` hospital -> ``802601`` department -> ``80260101``
section); SHAK-a-like's are EPJI's own dot-separated levels
(``01.02.01.4.1``). The two code spaces are NOT comparable and must never
be cross-matched — the only bridge between them is the SOR code, resolved
separately by ``cm_shakalike_to_sor`` / ``conceptmap.SHAKAlikeToSOR``, which
this hierarchy derivation does not touch (not every SHAK-a-like location
even has a SOR match).

Hierarchy is derived from the code's own dot-separated levels — a code's
parent is the longest strictly-shorter dot-prefix of itself that also
exists in the set (walking up past any missing intermediate levels).
EPJI's LOCATION.PART_OF_GUID models the same parent relationship
authoritatively; deriving from the code string avoids widening the extract
to carry that column, and matches PART_OF_GUID exactly for every case
checked against real EPJI data. A code with no existing ancestor at any
prefix length becomes a root and is logged as a warning, since a gap in
EPJI's location tree would otherwise silently reset that whole subtree's
level/parent in stg_SHAKAlike / dim_SHAKAlike. Concepts are
UPSERTed (stale codes kept so map/dim references stay intact); properties
and hierarchy are refreshed snapshot-style so an in-place SOR identifier
change does not leave a stale property next to the new value.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables

_NON_PROPERTY_COLUMNS = {
    "code",
    "display",
    "sor_code",
    "dw_timestamp_insert",
    "dw_timestamp_update",
}


def derive_shakalike_hierarchy(codes: set[str]) -> list[dict]:
    """Derive parent/child edges from dot-hierarchical SHAK-a-like codes.

    A code's parent is the longest strictly-shorter dot-prefix of itself that
    also exists in ``codes`` (e.g. ``01.02.01.4.1`` walks ``01.02.01.4``,
    ``01.02.01``, ``01.02``, ``01`` in that order and stops at the first
    match). Codes with no shorter existing ancestor at any prefix length are
    left as roots.
    """
    hierarchy: list[dict] = []
    for code in codes:
        segments = code.split(".")
        for i in range(len(segments) - 1, 0, -1):
            parent = ".".join(segments[:i])
            if parent in codes:
                hierarchy.append({"CODE": code, "PARENT_CODE": parent})
                break
    return hierarchy


def build_shakalike_codesystem(
    rows: list,
    columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build (concepts, properties, hierarchy) from ext.epji_location rows."""
    if columns is not None:
        data = [dict(zip(columns, row, strict=False)) for row in rows]
    else:
        data = list(rows)

    property_columns = [
        col
        for col in (columns or (data[0].keys() if data else []))
        if col not in _NON_PROPERTY_COLUMNS
    ]

    concepts: list[dict] = []
    properties: list[dict] = []
    codes: set[str] = set()

    for row in data:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        codes.add(code)
        display = str(row.get("display", "")).strip() if row.get("display") else ""
        concepts.append({"CODE": code, "DISPLAY": display or code})

        for key in property_columns:
            value = row.get(key)
            if value is None or not str(value).strip():
                continue
            properties.append(
                {
                    "CODE": code,
                    "KEY": key,
                    "VALUE": str(value).strip(),
                    "TYPE": "String",
                }
            )

    hierarchy = derive_shakalike_hierarchy(codes)
    return concepts, properties, hierarchy


@dg.asset(
    name="cs_shakalike",
    deps=["arc_epji_shakalike", "staging_infra_codesystem"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "shakalike"},
    description=(
        "Build the SHAK-a-like CodeSystem from arc.epji_location (live rows only). "
        "Concepts are UPSERTed; properties and hierarchy are snapshot-refreshed."
    ),
)
def cs_shakalike(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT [code], [location_pk], [display], [sor_code], [identifier_system] "
                "FROM [arc].[epji_location] WHERE [dw_is_deleted] = 0"
            )
            rows = cursor.fetchall()
            cols = [col[0] for col in cursor.description]
            context.log.info(f"Read {len(rows)} live rows from arc.epji_location")

            concepts, properties, hierarchy = build_shakalike_codesystem(rows, cols)
            context.log.info(
                f"Built {len(concepts)} concepts, {len(properties)} properties, "
                f"{len(hierarchy)} hierarchy entries"
            )

            multi_segment_codes = {c["CODE"] for c in concepts if "." in c["CODE"]}
            codes_with_parent = {h["CODE"] for h in hierarchy}
            orphan_codes = multi_segment_codes - codes_with_parent
            if orphan_codes:
                context.log.warning(
                    f"{len(orphan_codes)} non-root SHAK-a-like code(s) have no "
                    f"ancestor anywhere in the archive and became roots: "
                    f"{sorted(orphan_codes)[:20]}"
                )

            result = merge_codesystem_tables(
                cursor,
                "shakalike",
                concepts,
                properties,
                hierarchy,
                delete_stale_properties=True,
                delete_stale_hierarchy=True,
            )
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.shakalike: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u)"
    )
    return dg.MaterializeResult(metadata={**result, "orphan_hierarchy_codes": len(orphan_codes)})
