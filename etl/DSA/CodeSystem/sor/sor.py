# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CodeSystem Asset: SOR

Reads the CURRENT version per SOR code from the persistent arc.sor_register
archive (one row per entity version, maintained by arc_sor_register), builds
the codesystem structure, and MERGEs into persistent tables:
codesystem.SORConcept, SORProperties, SORHierarchy.

Because arc is cumulative (codes are never deleted from it), the source is a
complete snapshot of every code ever seen — so stale PROPERTIES and HIERARCHY
edges are deleted on merge (a cleared to_date reopens an entity; a moved
entity loses its old parent edge), while concepts are never deleted.

The codesystem tables are the shared source for fhir_organization_sor (and
from there dim.Organisation); update_only_on_change keeps dw_timestamp_update
usable as its delta signal.
"""

from collections.abc import Iterator
from datetime import datetime

import dagster as dg
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables

__all__ = [
    "build_sor_codesystem",
    "build_sor_codesystem_streaming",
    "cs_sor",
    "KNOWN_PROPERTY_KEYS",
    "SOR_ROOT",
    "ACTIVE_RSD_PROPERTY_KEY",
]

KNOWN_PROPERTY_KEYS = [
    "entityType",
    "entityTypeId",
    "municipality",
    "region",
    "owner",
    "ownerId",
    "cvrNumber",
    "website",
    "email",
    "phone",
    "address",
    "postalCode",
    "city",
    "openingDate",
    "closingDate",
    "activeRsdOrganization",
]

SKIP_COLUMNS = {"code", "display", "parent_code", "dw_timestamp_insert", "dw_timestamp_update"}

# Region Syddanmark's rod-enhed i SOR — samme konvention som conceptmap-modulerne
# (ean_to_sor._SOR_ROOT, rekvirent_to_sor/lager_to_sor/okosted_to_sor sor_root).
SOR_ROOT = "6101000016001"

ACTIVE_RSD_PROPERTY_KEY = "activeRsdOrganization"


def build_sor_codesystem(
    sor_rows: list,
    sor_columns: list[str] | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Build codesystem structure from SOR register rows (current version per code).

    Handles two formats:
    1. Wide format (each row has Code, display, ParentCode, + property columns)
    2. Pre-normalised format (separate concept/property/hierarchy rows already merged
       into the source table with CODE, KEY, VALUE, TYPE columns)

    Returns:
        (concepts, properties, hierarchy) triple
    """
    sor_data = _rows_to_dicts(sor_rows, sor_columns) if sor_columns else _ensure_dicts(sor_rows)

    if not sor_data:
        return [], [], []

    sample = sor_data[0]
    if "KEY" in sample and "VALUE" in sample:
        concepts, properties, hierarchy = _build_from_normalised(sor_data)
    else:
        concepts, properties, hierarchy = _build_from_wide(sor_data)

    _append_active_rsd_property(concepts, properties, hierarchy)
    return concepts, properties, hierarchy


def _append_active_rsd_property(
    concepts: list[dict],
    properties: list[dict],
    hierarchy: list[dict],
    now: datetime | None = None,
) -> None:
    """Append an ``activeRsdOrganization`` Boolean property to every concept.

    True when the entity is part of Region Syddanmark's egen, aktuelle
    organisation: den er åben (ingen closingDate), er trådt i kraft
    (openingDate <= nu) og kan nås fra rod-enheden ``SOR_ROOT`` gennem en
    kæde af ligeledes åbne enheder. Praksissektoren, kommuner og andre
    aktører i regionen hænger aldrig under rod-enheden og bliver false.

    Emitted som true/false for ALLE koder — ikke kun true — så værdien
    flipper på plads under MERGE'ens Code+PropertyCode-nøgle, når en enhed
    lukker eller flyttes (cs_sor kører uden delete_stale, så en kun-true
    property ville efterlade forældede true-rækker).
    """
    now_str = (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    inactive: set[str] = set()
    for prop in properties:
        if prop["KEY"] == "closingDate" and prop["VALUE"]:
            inactive.add(prop["CODE"])
        elif prop["KEY"] == "openingDate" and prop["VALUE"] > now_str:
            # ISO-formaterede datoer (YYYY-MM-DD ...) sammenlignes leksikografisk.
            inactive.add(prop["CODE"])

    children: dict[str, list[str]] = {}
    for h in hierarchy:
        children.setdefault(h["PARENT_CODE"], []).append(h["CODE"])

    all_codes = {c["CODE"] for c in concepts}
    reachable = _compute_reachable(all_codes, children, inactive)

    for concept in concepts:
        properties.append(
            {
                "CODE": concept["CODE"],
                "KEY": ACTIVE_RSD_PROPERTY_KEY,
                "VALUE": "true" if concept["CODE"] in reachable else "false",
                "TYPE": "Boolean",
            }
        )


def _compute_reachable(
    all_codes: set[str], children: dict[str, list[str]], inactive: set[str]
) -> set[str]:
    """Codes reachable from SOR_ROOT through a chain of open (non-inactive) units."""
    reachable: set[str] = set()
    if SOR_ROOT in all_codes and SOR_ROOT not in inactive:
        stack = [SOR_ROOT]
        while stack:
            code = stack.pop()
            if code in reachable:
                continue
            reachable.add(code)
            for child in children.get(code, []):
                if child in all_codes and child not in inactive:
                    stack.append(child)
    return reachable


def _build_from_wide(sor_data: list[dict]) -> tuple[list[dict], list[dict], list[dict]]:
    """Build triple from wide-format rows (Code, display, ParentCode, + property columns)."""
    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    property_columns: list[str] = []
    if sor_data:
        property_columns = [k for k in sor_data[0] if k not in SKIP_COLUMNS]

    for row in sor_data:
        code = str(row.get("code", "")).strip()
        if not code:
            continue
        display = str(row.get("display", "")).strip() if row.get("display") else ""

        concepts.append({"CODE": code, "DISPLAY": display or code})

        for prop_key in property_columns:
            value = row.get(prop_key)
            if value is not None and str(value).strip():
                prop_type = "String"
                if prop_key in ("openingDate", "closingDate"):
                    prop_type = "Date"
                properties.append(
                    {
                        "CODE": code,
                        "KEY": prop_key,
                        "VALUE": str(value).strip(),
                        "TYPE": prop_type,
                    }
                )

        parent_code = str(row.get("parent_code", "")).strip() if row.get("parent_code") else ""
        if parent_code:
            hierarchy.append({"CODE": code, "PARENT_CODE": parent_code})

    return concepts, properties, hierarchy


def _build_from_normalised(
    sor_data: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build triple from pre-normalised rows (CODE, KEY, VALUE, TYPE)."""
    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    concept_set: set[str] = set()
    display_lookup: dict[str, str] = {}

    for row in sor_data:
        code = str(row.get("CODE", "")).strip()
        key = str(row.get("KEY", "")).strip()
        value = str(row.get("VALUE", "")).strip() if row.get("VALUE") else ""
        row_type = str(row.get("TYPE", "String")).strip()

        if not code or not key:
            continue

        if key == "display":
            display_lookup[code] = value
            concept_set.add(code)
        elif key == "parentCode":
            if value:
                hierarchy.append({"CODE": code, "PARENT_CODE": value})
            concept_set.add(code)
        else:
            if value:
                properties.append({"CODE": code, "KEY": key, "VALUE": value, "TYPE": row_type})
            concept_set.add(code)

    for code in concept_set:
        display = display_lookup.get(code, code)
        concepts.append({"CODE": code, "DISPLAY": display})

    return concepts, properties, hierarchy


def build_sor_codesystem_streaming(
    sor_rows: list,
    sor_columns: list[str],
    now: datetime | None = None,
) -> tuple[list[dict], Iterator[dict], list[dict]]:
    """Memory-lean wide-format build for large sources (used by the cs_sor asset).

    Same output as build_sor_codesystem, but the ~15-per-code property rows are
    YIELDED instead of materialized — at full SOR scale (165k codes, 2.4M
    properties) the property list alone costs gigabytes and has OOM-killed the
    run container. Only the concept/hierarchy lists and the reachability sets
    are held in memory.

    Each property is emitted exactly once per (code, property_code) — sor_rows
    must be unique per code (the arc current-version query guarantees this) —
    so the result is safe for merge_codesystem_tables(assume_unique_keys=True).
    """
    col = {name: idx for idx, name in enumerate(sor_columns)}
    property_columns = [c for c in sor_columns if c not in SKIP_COLUMNS]
    now_str = (now or datetime.now()).strftime("%Y-%m-%d %H:%M:%S")

    def _cell(row, name: str) -> str:
        value = row[col[name]]
        return str(value).strip() if value is not None else ""

    concepts: list[dict] = []
    hierarchy: list[dict] = []
    children: dict[str, list[str]] = {}
    inactive: set[str] = set()

    for row in sor_rows:
        code = _cell(row, "code")
        if not code:
            continue
        display = _cell(row, "display")
        concepts.append({"CODE": code, "DISPLAY": display or code})

        parent_code = _cell(row, "parent_code")
        if parent_code:
            hierarchy.append({"CODE": code, "PARENT_CODE": parent_code})
            children.setdefault(parent_code, []).append(code)

        # Same inactive rules as _append_active_rsd_property: any closingDate,
        # or an openingDate still in the future (ISO strings compare lexically).
        if _cell(row, "closingDate"):
            inactive.add(code)
        elif _cell(row, "openingDate") > now_str:
            inactive.add(code)

    all_codes = {c["CODE"] for c in concepts}
    reachable = _compute_reachable(all_codes, children, inactive)

    def _properties() -> Iterator[dict]:
        for row in sor_rows:
            code = _cell(row, "code")
            if not code:
                continue
            for prop_key in property_columns:
                value = _cell(row, prop_key)
                if value:
                    prop_type = "Date" if prop_key in ("openingDate", "closingDate") else "String"
                    yield {"CODE": code, "KEY": prop_key, "VALUE": value, "TYPE": prop_type}
            yield {
                "CODE": code,
                "KEY": ACTIVE_RSD_PROPERTY_KEY,
                "VALUE": "true" if code in reachable else "false",
                "TYPE": "Boolean",
            }

    return concepts, _properties(), hierarchy


def _rows_to_dicts(rows: list[tuple], columns: list[str]) -> list[dict]:
    return [dict(zip(columns, row, strict=False)) for row in rows]


def _ensure_dicts(rows: list) -> list[dict]:
    if rows and isinstance(rows[0], dict):
        return rows
    return rows


# Current version per code from the version-grained archive. to_date of the
# current version is the entity's closing date (openingDate = FirstFromDate
# comes straight from the source as its own column).
_CURRENT_VERSION_QUERY = """
    SELECT [code], [display], [parent_code],
           [entityType], [entityTypeId], [municipality], [region],
           [owner], [ownerId], [cvrNumber], [website], [email], [phone],
           [address], [postalCode], [city],
           [openingDate], [to_date] AS [closingDate]
    FROM (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY [code] ORDER BY [from_date] DESC
        ) AS _rn
        FROM [arc].[sor_register]
    ) v
    WHERE _rn = 1
"""

_TOUCH_CHUNK_SIZE = 1000


def _touch_concepts(cursor, codes: list[str]) -> None:
    """Bump SORConcept.dw_timestamp_update for codes whose properties or
    hierarchy edges were DELETED — deletions leave no timestamp on surviving
    rows, so without this the downstream delta consumer would never see them."""
    for i in range(0, len(codes), _TOUCH_CHUNK_SIZE):
        chunk = codes[i : i + _TOUCH_CHUNK_SIZE]
        placeholders = ", ".join(["?"] * len(chunk))
        cursor.execute(
            f"UPDATE [codesystem].[SORConcept] "
            f"SET [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0)) "
            f"WHERE [code] IN ({placeholders})",
            chunk,
        )


@dg.asset(
    name="cs_sor",
    deps=["arc_sor_register"],
    group_name="codesystem",
    compute_kind="mssql",
    tags={"domain": "codesystem", "layer": "codesystem", "module": "sor"},
    description=(
        "Build SOR codesystem from the current version per code in "
        "arc.sor_register and MERGE into codesystem.SOR*"
    ),
)
def cs_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(_CURRENT_VERSION_QUERY)
            sor_rows = cursor.fetchall()
            sor_cols = [col[0] for col in cursor.description]

            context.log.info(f"Read from arc.sor_register (current versions): {len(sor_rows)} rows")

            # Streaming build: properties are yielded straight into the merge's
            # chunked temp-table inserts — the full property list (2.4M rows at
            # full SOR scale) never exists in memory.
            concepts, properties, hierarchy = build_sor_codesystem_streaming(sor_rows, sor_cols)

            context.log.info(
                f"Built {len(concepts)} concepts, {len(hierarchy)} hierarchy entries "
                f"(properties are streamed)"
            )

            result = merge_codesystem_tables(
                cursor,
                "sor",
                concepts,
                properties,
                hierarchy,
                # arc is cumulative, so the built snapshot covers every code
                # ever seen — stale properties (e.g. a cleared closingDate)
                # and stale hierarchy edges can safely be deleted. Concepts
                # are never deleted.
                delete_stale_properties=True,
                delete_stale_hierarchy=True,
                update_only_on_change=True,
                collect_deleted_codes=True,
                # The wide-format build emits exactly one row per merge key
                # (one arc row per code), so the dedup dicts — gigabytes at
                # 2.4M properties — are skipped.
                assume_unique_keys=True,
            )
            deleted_codes = result.pop("deleted_codes", [])
            if deleted_codes:
                _touch_concepts(cursor, deleted_codes)
                result["concepts_touched_for_deletes"] = len(deleted_codes)
            conn.commit()
        finally:
            cursor.close()

    context.log.info(
        f"codesystem.sor: "
        f"concepts({result['concepts_inserted']}i/{result['concepts_updated']}u), "
        f"properties({result['properties_inserted']}i/{result['properties_updated']}u"
        f"/{result.get('properties_deleted', 0)}d), "
        f"hierarchy({result['hierarchy_inserted']}i/{result['hierarchy_updated']}u"
        f"/{result.get('hierarchy_deleted', 0)}d)"
    )

    return dg.MaterializeResult(metadata=result)
