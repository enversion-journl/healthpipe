# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared utilities for CodeSystem assets.

Provides the merge_codesystem_tables function that handles the temp-table + MERGE
pattern for all three codesystem table types (concept, properties, hierarchy).
"""

from collections.abc import Iterable

# Canonical PascalCase prefix per codesystem (acronyms stay UPPER, words Title-cased).
CODESYSTEM_PASCAL = {
    "sks": "SKS",
    "sor": "SOR",
    "sfi": "SFI",
    "ean": "EAN",
    "shak": "SHAK",
    "shakalike": "SHAKAlike",
    "rekvirent": "Rekvirent",
    "lager": "Lager",
    "okosted": "Okosted",
    "ifdb": "IFDB",
}

_KIND_PASCAL = {"concept": "Concept", "properties": "Properties", "hierarchy": "Hierarchy"}

_INSERT_CHUNK_SIZE = 1000


def codesystem_table_name(codesystem_name: str, kind: str) -> str:
    """Return the PascalCase table name, e.g. ('sks', 'concept') -> 'SKSConcept'.

    Raises KeyError for an unknown codesystem, ValueError for an unknown kind.
    """
    prefix = CODESYSTEM_PASCAL[codesystem_name]
    if kind not in _KIND_PASCAL:
        raise ValueError(f"Unknown codesystem kind={kind!r}. Allowed: {sorted(_KIND_PASCAL)}")
    return f"{prefix}{_KIND_PASCAL[kind]}"


_TOUCH_CHUNK_SIZE = 1000


def touch_concepts(cursor, codesystem_name: str, codes: list[str]) -> None:
    """Bump the concept's dw_timestamp_update for codes whose properties or
    hierarchy edges were DELETED — deletions leave no timestamp on surviving
    rows, so without this a delta consumer would never see them."""
    table = codesystem_table_name(codesystem_name, "concept")
    for i in range(0, len(codes), _TOUCH_CHUNK_SIZE):
        chunk = codes[i : i + _TOUCH_CHUNK_SIZE]
        placeholders = ", ".join(["?"] * len(chunk))
        cursor.execute(
            f"UPDATE [codesystem].[{table}] "
            f"SET [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0)) "
            f"WHERE [code] IN ({placeholders})",
            chunk,
        )


def _insert_chunked(cursor, insert_sql: str, rows: Iterable[tuple]) -> None:
    """executemany in fixed-size chunks without materializing the full row list."""
    buffer: list[tuple] = []
    for row in rows:
        buffer.append(row)
        if len(buffer) >= _INSERT_CHUNK_SIZE:
            cursor.executemany(insert_sql, buffer)
            buffer.clear()
    if buffer:
        cursor.executemany(insert_sql, buffer)


def _run_merge_with_action_counts(
    cursor,
    merge_sql: str,
    *,
    code_max_length: int,
    collect_deleted: bool,
) -> tuple[int, int, int, list[str]]:
    """Run a MERGE whose OUTPUT is aggregated server-side.

    The MERGE's per-row ``$action, code`` output goes into a table variable and
    only a counts row (plus, when requested, the distinct DELETEd codes) is
    returned to the client — never one row per merged row. At full SOR scale
    the old ``OUTPUT $action`` + fetchall() pulled millions of rows into the
    step process and OOM-killed the run container.

    ``merge_sql`` must end with ``OUTPUT $action, <code expr> INTO
    @_actions([action], [code]);``.
    """
    deleted_select = (
        "SELECT DISTINCT [code] FROM @_actions WHERE [action] = 'DELETE' AND [code] IS NOT NULL;"
        if collect_deleted
        else ""
    )
    batch = f"""
        DECLARE @_actions TABLE ([action] NVARCHAR(10), [code] NVARCHAR({code_max_length}));
        {merge_sql}
        SELECT
            SUM(CASE WHEN [action] = 'INSERT' THEN 1 ELSE 0 END),
            SUM(CASE WHEN [action] = 'UPDATE' THEN 1 ELSE 0 END),
            SUM(CASE WHEN [action] = 'DELETE' THEN 1 ELSE 0 END)
        FROM @_actions;
        {deleted_select}
    """
    cursor.execute(batch)
    cursor.nextset()
    counts = cursor.fetchone()
    inserted = counts[0] or 0
    updated = counts[1] or 0
    deleted = counts[2] or 0

    deleted_codes: list[str] = []
    if collect_deleted:
        cursor.nextset()
        deleted_codes = [row[0] for row in cursor.fetchall()]

    return inserted, updated, deleted, deleted_codes


# Binary collation for the change guard. The database collation is
# case-insensitive, and `=` ignores trailing spaces under ANY collation, so a
# plain comparison would call "kardiologi" and "Kardiologi " unchanged — the
# guard would then skip the UPDATE and the corrected value would never land.
_EXACT = "COLLATE Latin1_General_BIN2"


def _null_safe_diff(column: str) -> str:
    """SQL that is true when target and source differ in ``column`` — exactly.

    NULL-safe both ways, case- and accent-sensitive (binary collation), and
    sensitive to trailing spaces via DATALENGTH.
    """
    target = f"target.[{column}]"
    source = f"source.[{column}]"
    return (
        f"ISNULL(NULLIF({target} {_EXACT}, {source} {_EXACT}), "
        f"NULLIF({source} {_EXACT}, {target} {_EXACT})) IS NOT NULL "
        f"OR DATALENGTH({target}) <> DATALENGTH({source})"
    )


def merge_codesystem_tables(
    cursor,
    codesystem_name: str,
    concepts: list[dict],
    properties: Iterable[dict],
    hierarchy: list[dict],
    *,
    code_max_length: int = 100,
    multi_value_properties: bool = False,
    delete_stale: bool = False,
    delete_stale_properties: bool | None = None,
    delete_stale_hierarchy: bool | None = None,
    update_only_on_change: bool = False,
    collect_deleted_codes: bool = False,
    assume_unique_keys: bool = False,
) -> dict:
    """
    MERGE concepts, properties, and hierarchy into codesystem.{name}_* tables.

    Uses temp tables + MERGE; per-row actions are aggregated server-side into
    insert/update/delete counts (no per-row OUTPUT rows reach the client).

    Args:
        code_max_length: NVARCHAR width for Code columns in temp tables. Must match
            the permanent table width (e.g. 255 for SFI).
        multi_value_properties: When True, a code may carry several values for
            the same PropertyCode (rows are keyed on Code+PropertyCode+value
            instead of Code+PropertyCode). Requires the target properties
            table to have [value] in its primary key.
        delete_stale: When True, rows in the target tables that are absent
            from the source are deleted (full-snapshot semantics). Only use
            for codesystems where the extract is a complete snapshot.
        delete_stale_properties: Per-table override of ``delete_stale`` for the
            properties table (None inherits ``delete_stale``). Lets a codesystem
            keep stale CONCEPTS (so surrogate-key references stay intact) while
            still refreshing properties snapshot-style.
        delete_stale_hierarchy: Same override for the hierarchy table — without
            it, a changed parent leaves the old edge behind and the concept
            gets two parents in the FHIR tree.
        update_only_on_change: When True, matched rows are only UPDATEd (and
            dw_timestamp_update only bumped) when a non-key column actually
            changed. This makes dw_timestamp_update usable as a delta signal
            for downstream DFM consumers (e.g. fhir_organization_sor).
        collect_deleted_codes: When True, the result includes ``deleted_codes``
            — the distinct concept codes affected by property/hierarchy
            deletions — so the caller can propagate a change signal for them
            (deletions leave no timestamp on surviving rows). Opt-in because
            the result dict is commonly passed straight into asset metadata.
        assume_unique_keys: When True, the caller guarantees the inputs are
            already unique on their merge keys (concepts: code; properties:
            code+property_code[+value]; hierarchy: parent+child) and the
            client-side dedup dicts are skipped — rows stream straight into
            the temp tables. At millions of properties the dedup dicts cost
            gigabytes; a builder that emits one row per key (like cs_sor's
            wide-format build) does not need them. A duplicate key with this
            flag set fails the MERGE instead of being silently collapsed.

    Returns:
        Dict with insert/update (and delete, when stale deletion is on) counts
        for each table type, plus ``deleted_codes`` when requested.
    """
    result: dict = {}
    deleted_codes: set[str] = set()
    delete_stale_props = (
        delete_stale if delete_stale_properties is None else delete_stale_properties
    )
    delete_stale_hier = delete_stale if delete_stale_hierarchy is None else delete_stale_hierarchy
    delete_clause = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale else ""
    delete_clause_props = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale_props else ""
    delete_clause_hier = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale_hier else ""

    if hasattr(cursor, "fast_executemany"):
        cursor.fast_executemany = True

    # --- CONCEPTS ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_concept;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_concept (
            [code] NVARCHAR({code_max_length}) COLLATE DATABASE_DEFAULT NOT NULL PRIMARY KEY,
            [display] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL
        );
    """
    )
    if assume_unique_keys:
        concept_rows: Iterable[tuple] = ((c["CODE"], c.get("DISPLAY") or None) for c in concepts)
    else:
        seen: dict[str, tuple[str, str | None]] = {}
        for c in concepts:
            key = c["CODE"].upper()
            seen[key] = (c["CODE"], c.get("DISPLAY") or None)
        concept_rows = seen.values()
    _insert_chunked(
        cursor, "INSERT INTO #tmp_concept ([code], [display]) VALUES (?, ?)", concept_rows
    )

    concept_change_guard = ""
    if update_only_on_change:
        concept_change_guard = f" AND ({_null_safe_diff('display')})"
    concept_merge = f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "concept")}] AS target
        USING #tmp_concept AS source
        ON target.[code] = source.[code]
        WHEN MATCHED{concept_change_guard} THEN
            UPDATE SET
                target.[display] = source.[display],
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([code], [display], [dw_timestamp_insert], [dw_timestamp_update])
            VALUES (source.[code], source.[display],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action, ISNULL(inserted.[code], deleted.[code]) INTO @_actions([action], [code]);
    """
    inserted, updated, deleted, _ = _run_merge_with_action_counts(
        cursor, concept_merge, code_max_length=code_max_length, collect_deleted=False
    )
    result["concepts_inserted"] = inserted
    result["concepts_updated"] = updated
    if delete_stale:
        result["concepts_deleted"] = deleted

    # --- PROPERTIES ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_properties;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_properties (
            [code] NVARCHAR({code_max_length}) COLLATE DATABASE_DEFAULT NOT NULL,
            [property_code] NVARCHAR(100) COLLATE DATABASE_DEFAULT NOT NULL,
            [value] NVARCHAR(500) COLLATE DATABASE_DEFAULT NULL,
            [type] NVARCHAR(50) COLLATE DATABASE_DEFAULT NULL
        );
    """
    )
    if assume_unique_keys:
        property_rows: Iterable[tuple] = (
            (p["CODE"], p["KEY"], p.get("VALUE"), p.get("TYPE")) for p in properties
        )
    else:
        seen_props: dict[tuple, tuple[str, str, str | None, str | None]] = {}
        for p in properties:
            if multi_value_properties:
                key = (p["CODE"].upper(), p["KEY"].upper(), (p.get("VALUE") or "").upper())
            else:
                key = (p["CODE"].upper(), p["KEY"].upper())
            seen_props[key] = (p["CODE"], p["KEY"], p.get("VALUE"), p.get("TYPE"))
        property_rows = seen_props.values()
    _insert_chunked(
        cursor,
        "INSERT INTO #tmp_properties ([code], [property_code], [value], [type]) "
        "VALUES (?, ?, ?, ?)",
        property_rows,
    )

    if multi_value_properties:
        properties_on = """
        ON target.[code] = source.[code]
           AND target.[property_code] = source.[property_code]
           AND ISNULL(target.[value], N'') = ISNULL(source.[value], N'')"""
        properties_update = """
            UPDATE SET
                target.[type] = source.[type],
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))"""
        properties_change_guard = f" AND ({_null_safe_diff('type')})"
    else:
        properties_on = """
        ON target.[code] = source.[code]
           AND target.[property_code] = source.[property_code]"""
        properties_update = """
            UPDATE SET
                target.[value] = source.[value],
                target.[type] = source.[type],
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))"""
        properties_change_guard = f" AND ({_null_safe_diff('value')} OR {_null_safe_diff('type')})"
    if not update_only_on_change:
        properties_change_guard = ""

    properties_merge = f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "properties")}] AS target
        USING #tmp_properties AS source
        {properties_on}
        WHEN MATCHED{properties_change_guard} THEN
            {properties_update}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([code], [property_code], [value], [type],
                    [dw_timestamp_insert], [dw_timestamp_update])
            VALUES (source.[code], source.[property_code], source.[value], source.[type],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause_props}
        OUTPUT $action, ISNULL(inserted.[code], deleted.[code]) INTO @_actions([action], [code]);
    """
    inserted, updated, deleted, prop_deleted_codes = _run_merge_with_action_counts(
        cursor,
        properties_merge,
        code_max_length=code_max_length,
        collect_deleted=collect_deleted_codes and delete_stale_props,
    )
    result["properties_inserted"] = inserted
    result["properties_updated"] = updated
    if delete_stale_props:
        result["properties_deleted"] = deleted
        deleted_codes.update(prop_deleted_codes)

    # --- HIERARCHY ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_hierarchy;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_hierarchy (
            [parent_code] NVARCHAR({code_max_length}) COLLATE DATABASE_DEFAULT NOT NULL,
            [child_code] NVARCHAR({code_max_length}) COLLATE DATABASE_DEFAULT NOT NULL,
            PRIMARY KEY ([parent_code], [child_code])
        );
    """
    )
    valid_edges = (h for h in hierarchy if h.get("PARENT_CODE") and h["PARENT_CODE"] != h["CODE"])
    if assume_unique_keys:
        hierarchy_rows: Iterable[tuple] = ((h["PARENT_CODE"], h["CODE"]) for h in valid_edges)
    else:
        seen_hier: dict[tuple, tuple[str, str]] = {}
        for h in valid_edges:
            key = (h["PARENT_CODE"].upper(), h["CODE"].upper())
            seen_hier[key] = (h["PARENT_CODE"], h["CODE"])
        hierarchy_rows = seen_hier.values()
    _insert_chunked(
        cursor,
        "INSERT INTO #tmp_hierarchy ([parent_code], [child_code]) VALUES (?, ?)",
        hierarchy_rows,
    )

    # Hierarchy rows have no non-key columns — a matched row is by definition
    # unchanged, so with update_only_on_change the MATCHED branch is dropped
    # instead of bumping timestamps on every edge.
    hierarchy_matched_clause = ""
    if not update_only_on_change:
        hierarchy_matched_clause = """WHEN MATCHED THEN
            UPDATE SET
                target.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
        """

    hierarchy_merge = f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "hierarchy")}] AS target
        USING #tmp_hierarchy AS source
        ON target.[parent_code] = source.[parent_code]
           AND target.[child_code] = source.[child_code]
        {hierarchy_matched_clause}WHEN NOT MATCHED BY TARGET THEN
            INSERT ([parent_code], [child_code],
                    [dw_timestamp_insert], [dw_timestamp_update])
            VALUES (source.[parent_code], source.[child_code],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause_hier}
        OUTPUT $action, ISNULL(inserted.[child_code], deleted.[child_code])
            INTO @_actions([action], [code]);
    """
    inserted, updated, deleted, hier_deleted_codes = _run_merge_with_action_counts(
        cursor,
        hierarchy_merge,
        code_max_length=code_max_length,
        collect_deleted=collect_deleted_codes and delete_stale_hier,
    )
    result["hierarchy_inserted"] = inserted
    result["hierarchy_updated"] = updated
    if delete_stale_hier:
        result["hierarchy_deleted"] = deleted
        deleted_codes.update(hier_deleted_codes)

    cursor.execute("DROP TABLE IF EXISTS #tmp_concept;")
    cursor.execute("DROP TABLE IF EXISTS #tmp_properties;")
    cursor.execute("DROP TABLE IF EXISTS #tmp_hierarchy;")

    if collect_deleted_codes:
        result["deleted_codes"] = sorted(deleted_codes)

    return result
