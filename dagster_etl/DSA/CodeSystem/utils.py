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

# Canonical PascalCase prefix per codesystem (acronyms stay UPPER, words Title-cased).
CODESYSTEM_PASCAL = {
    "sks": "SKS",
    "sor": "SOR",
    "sfi": "SFI",
    "ean": "EAN",
    "rekvirent": "Rekvirent",
    "lager": "Lager",
    "okosted": "Okosted",
}

_KIND_PASCAL = {"concept": "Concept", "properties": "Properties", "hierarchy": "Hierarchy"}


def codesystem_table_name(codesystem_name: str, kind: str) -> str:
    """Return the PascalCase table name, e.g. ('sks', 'concept') -> 'SKSConcept'.

    Raises KeyError for an unknown codesystem, ValueError for an unknown kind.
    """
    prefix = CODESYSTEM_PASCAL[codesystem_name]
    if kind not in _KIND_PASCAL:
        raise ValueError(f"Unknown codesystem kind={kind!r}. Allowed: {sorted(_KIND_PASCAL)}")
    return f"{prefix}{_KIND_PASCAL[kind]}"


def merge_codesystem_tables(
    cursor,
    codesystem_name: str,
    concepts: list[dict],
    properties: list[dict],
    hierarchy: list[dict],
    *,
    code_max_length: int = 100,
    multi_value_properties: bool = False,
    delete_stale: bool = False,
) -> dict:
    """
    MERGE concepts, properties, and hierarchy into codesystem.{name}_* tables.

    Uses temp tables + MERGE with OUTPUT $action to count inserts/updates.

    Args:
        code_max_length: NVARCHAR width for Code columns in temp tables. Must match
            the permanent table width (e.g. 255 for SFI).
        multi_value_properties: When True, a code may carry several values for
            the same PropertyCode (rows are keyed on Code+PropertyCode+Value
            instead of Code+PropertyCode). Requires the target properties
            table to have [Value] in its primary key.
        delete_stale: When True, rows in the target tables that are absent
            from the source are deleted (full-snapshot semantics). Only use
            for codesystems where the extract is a complete snapshot.

    Returns:
        Dict with insert/update (and delete, when delete_stale) counts for
        each table type.
    """
    result: dict = {}
    delete_clause = "WHEN NOT MATCHED BY SOURCE THEN DELETE" if delete_stale else ""

    # --- CONCEPTS ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_concept;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_concept (
            [Code] NVARCHAR({code_max_length}) NOT NULL PRIMARY KEY,
            [Display] NVARCHAR(500) NULL
        );
    """
    )
    if concepts:
        if hasattr(cursor, "fast_executemany"):
            cursor.fast_executemany = True
        seen: dict[str, tuple[str, str | None]] = {}
        for c in concepts:
            key = c["CODE"].upper()
            seen[key] = (c["CODE"], c.get("DISPLAY") or None)
        rows = list(seen.values())
        for i in range(0, len(rows), 1000):
            cursor.executemany(
                "INSERT INTO #tmp_concept ([Code], [Display]) VALUES (?, ?)",
                rows[i : i + 1000],
            )

    cursor.execute(
        f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "concept")}] AS target
        USING #tmp_concept AS source
        ON target.[Code] = source.[Code]
        WHEN MATCHED THEN
            UPDATE SET
                target.[Display] = source.[Display],
                target.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([Code], [Display], [DW_Timestamp_Insert], [DW_Timestamp_Update])
            VALUES (source.[Code], source.[Display],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action;
    """
    )
    concept_results = cursor.fetchall()
    result["concepts_inserted"] = sum(1 for r in concept_results if r[0] == "INSERT")
    result["concepts_updated"] = sum(1 for r in concept_results if r[0] == "UPDATE")
    if delete_stale:
        result["concepts_deleted"] = sum(1 for r in concept_results if r[0] == "DELETE")

    # --- PROPERTIES ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_properties;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_properties (
            [Code] NVARCHAR({code_max_length}) NOT NULL,
            [PropertyCode] NVARCHAR(100) NOT NULL,
            [Value] NVARCHAR(500) NULL,
            [Type] NVARCHAR(50) NULL
        );
    """
    )
    if properties:
        seen_props: dict[tuple, tuple[str, str, str | None, str | None]] = {}
        for p in properties:
            if multi_value_properties:
                key = (p["CODE"].upper(), p["KEY"].upper(), (p.get("VALUE") or "").upper())
            else:
                key = (p["CODE"].upper(), p["KEY"].upper())
            seen_props[key] = (p["CODE"], p["KEY"], p.get("VALUE"), p.get("TYPE"))
        rows = list(seen_props.values())
        for i in range(0, len(rows), 1000):
            cursor.executemany(
                "INSERT INTO #tmp_properties ([Code], [PropertyCode], [Value], [Type]) "
                "VALUES (?, ?, ?, ?)",
                rows[i : i + 1000],
            )

    if multi_value_properties:
        properties_on = """
        ON target.[Code] = source.[Code]
           AND target.[PropertyCode] = source.[PropertyCode]
           AND ISNULL(target.[Value], N'') = ISNULL(source.[Value], N'')"""
        properties_update = """
            UPDATE SET
                target.[Type] = source.[Type],
                target.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))"""
    else:
        properties_on = """
        ON target.[Code] = source.[Code]
           AND target.[PropertyCode] = source.[PropertyCode]"""
        properties_update = """
            UPDATE SET
                target.[Value] = source.[Value],
                target.[Type] = source.[Type],
                target.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))"""

    cursor.execute(
        f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "properties")}] AS target
        USING #tmp_properties AS source
        {properties_on}
        WHEN MATCHED THEN
            {properties_update}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([Code], [PropertyCode], [Value], [Type],
                    [DW_Timestamp_Insert], [DW_Timestamp_Update])
            VALUES (source.[Code], source.[PropertyCode], source.[Value], source.[Type],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action;
    """
    )
    prop_results = cursor.fetchall()
    result["properties_inserted"] = sum(1 for r in prop_results if r[0] == "INSERT")
    result["properties_updated"] = sum(1 for r in prop_results if r[0] == "UPDATE")
    if delete_stale:
        result["properties_deleted"] = sum(1 for r in prop_results if r[0] == "DELETE")

    # --- HIERARCHY ---
    cursor.execute("DROP TABLE IF EXISTS #tmp_hierarchy;")
    cursor.execute(
        f"""
        CREATE TABLE #tmp_hierarchy (
            [ParentCode] NVARCHAR({code_max_length}) NOT NULL,
            [ChildCode] NVARCHAR({code_max_length}) NOT NULL,
            PRIMARY KEY ([ParentCode], [ChildCode])
        );
    """
    )
    if hierarchy:
        seen_hier: dict[str, tuple[str, str]] = {}
        for h in hierarchy:
            if h.get("PARENT_CODE") and h["PARENT_CODE"] != h["CODE"]:
                key = (h["PARENT_CODE"].upper(), h["CODE"].upper())
                seen_hier[key] = (h["PARENT_CODE"], h["CODE"])
        rows = list(seen_hier.values())
        for i in range(0, len(rows), 1000):
            cursor.executemany(
                "INSERT INTO #tmp_hierarchy ([ParentCode], [ChildCode]) VALUES (?, ?)",
                rows[i : i + 1000],
            )

    cursor.execute(
        f"""
        MERGE INTO [codesystem].[{codesystem_table_name(codesystem_name, "hierarchy")}] AS target
        USING #tmp_hierarchy AS source
        ON target.[ParentCode] = source.[ParentCode]
           AND target.[ChildCode] = source.[ChildCode]
        WHEN MATCHED THEN
            UPDATE SET
                target.[DW_Timestamp_Update] = CAST(SYSDATETIME() AS datetime2(0))
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ([ParentCode], [ChildCode],
                    [DW_Timestamp_Insert], [DW_Timestamp_Update])
            VALUES (source.[ParentCode], source.[ChildCode],
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0)))
        {delete_clause}
        OUTPUT $action;
    """
    )
    hier_results = cursor.fetchall()
    result["hierarchy_inserted"] = sum(1 for r in hier_results if r[0] == "INSERT")
    result["hierarchy_updated"] = sum(1 for r in hier_results if r[0] == "UPDATE")
    if delete_stale:
        result["hierarchy_deleted"] = sum(1 for r in hier_results if r[0] == "DELETE")

    cursor.execute("DROP TABLE IF EXISTS #tmp_concept;")
    cursor.execute("DROP TABLE IF EXISTS #tmp_properties;")
    cursor.execute("DROP TABLE IF EXISTS #tmp_hierarchy;")

    return result
