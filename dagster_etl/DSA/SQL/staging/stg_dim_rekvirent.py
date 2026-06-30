# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: dim_rekvirent from ILS Rekvirent codesystem.

Reads the Rekvirent codesystem tables (codesystem.rekvirent_concept, _properties, _hierarchy)
and the rekvirent-to-sor ConceptMap table, pivots selected property keys into columns,
and writes the result to stg.dim_rekvirent in the staging database.

Property-to-column mapping is configured via PROPERTY_COLUMN_MAP. To add more
properties as columns, simply extend the dict.
"""

import dagster as dg
from core.resources import MSSQLResource

PROPERTY_COLUMN_MAP: dict[str, str] = {
    "entityType": "EntityType",
    "sgh": "SGH",
    "rgrp": "RGRP",
    "rtyp": "RTYP",
    "rnr": "RNR",
}


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    """Build child_code -> parent_code dict from codesystem.rekvirent_hierarchy."""
    cursor.execute("SELECT ChildCode, ParentCode FROM [codesystem].[RekvirentHierarchy]")
    return {str(row[0]).strip(): str(row[1]).strip() for row in cursor.fetchall()}


def _compute_levels(hierarchy_lookup: dict[str, str]) -> dict[str, int]:
    """Compute hierarchy depth for each code (root=1)."""
    levels: dict[str, int] = {}
    all_codes = set(hierarchy_lookup.keys()) | set(hierarchy_lookup.values())

    for code in all_codes:
        depth = 1
        current = code
        visited: set[str] = set()
        while current in hierarchy_lookup and current not in visited:
            visited.add(current)
            current = hierarchy_lookup[current]
            depth += 1
        levels[code] = depth

    return levels


def _build_properties_lookup(cursor) -> dict[str, dict[str, str]]:
    """Build code -> {propertyCode: value} dict from codesystem.rekvirent_properties."""
    cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[RekvirentProperties]")
    lookup: dict[str, dict[str, str]] = {}
    for row in cursor.fetchall():
        code = str(row[0]).strip()
        key = str(row[1]).strip()
        value = str(row[2]).strip() if row[2] else ""
        lookup.setdefault(code, {})[key] = value
    return lookup


def _load_sor_mapping(cursor) -> dict[str, tuple[str, str]]:
    """Load SOR mapping from conceptmap.rekvirent_to_sor."""
    cursor.execute("SELECT SourceCode, TargetCode, Relationship FROM [conceptmap].[RekvirentToSOR]")
    return {
        str(row[0]).strip(): (str(row[1]).strip(), str(row[2]).strip()) for row in cursor.fetchall()
    }


@dg.asset(
    name="stg_dim_rekvirent",
    deps=["cs_rekvirent", "cm_rekvirent_to_sor"],
    group_name="erp_staging",
    compute_kind="mssql",
    description=(
        "Stage Rekvirent dimension from codesystem + ConceptMap tables. "
        "Reads codesystem.rekvirent_* and conceptmap.rekvirent_to_sor, "
        "then writes stg.dim_rekvirent with pivoted property columns."
    ),
)
def stg_dim_rekvirent(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    prop_columns = list(PROPERTY_COLUMN_MAP.values())
    prop_keys = list(PROPERTY_COLUMN_MAP.keys())

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[RekvirentConcept]")
            concepts = [(str(r[0]).strip(), r[1]) for r in cursor.fetchall()]
            context.log.info(f"Loaded {len(concepts)} concepts from codesystem.rekvirent_concept")

            hierarchy_lookup = _build_hierarchy_lookup(cursor)
            levels = _compute_levels(hierarchy_lookup)
            properties_lookup = _build_properties_lookup(cursor)
            sor_mapping = _load_sor_mapping(cursor)
            context.log.info(f"Loaded SOR mapping with {len(sor_mapping)} entries")

            def _get_prop(code: str, key: str) -> str | None:
                return properties_lookup.get(code, {}).get(key) or None

            def build_cleancode(code: str) -> str:
                sgh = _get_prop(code, "sgh")
                rnr = _get_prop(code, "rnr")
                if not sgh and not rnr:
                    return code
                parts = []
                if sgh:
                    parts.append(f"SGH_{sgh}")
                if rnr:
                    rnr_padded = str(rnr).zfill(4) if rnr else None
                    if rnr_padded:
                        parts.append(f"RNR_{rnr_padded}")
                if parts and len(parts) == 1 and parts[0].startswith("SGH_"):
                    return code
                return "_".join(parts) if parts else code

            row_candidates: list[dict] = []
            for code, display in concepts:
                display_val = display if display else None
                parent_code = hierarchy_lookup.get(code)
                level = levels.get(code, 1)
                prop_values = [_get_prop(code, k) for k in prop_keys]

                sor_entry = sor_mapping.get(code)
                code_sor = sor_entry[0] if sor_entry else None
                sor_relationship = sor_entry[1] if sor_entry else None

                clean_code = build_cleancode(code)

                row_candidates.append(
                    {
                        "clean_code": clean_code,
                        "code": code,
                        "display": display_val,
                        "parent_code": parent_code,
                        "level": level,
                        "prop_values": prop_values,
                        "code_sor": code_sor,
                        "sor_relationship": sor_relationship,
                    }
                )

            context.log.info(f"Built {len(row_candidates)} staging row candidates")

            def _row_sort_key(row: dict) -> tuple[int, int, int, str, str]:
                code = str(row["code"])
                return (
                    0 if row["code_sor"] is not None else 1,
                    -int(row["level"]),
                    0 if row["display"] else 1,
                    code.upper(),
                    code,
                )

            deduped_by_clean_code: dict[str, dict] = {}
            duplicate_clean_codes_removed = 0
            duplicate_clean_codes_replaced = 0

            for row in row_candidates:
                normalized_clean_code = str(row["clean_code"]).upper()
                existing = deduped_by_clean_code.get(normalized_clean_code)
                if existing is None:
                    deduped_by_clean_code[normalized_clean_code] = row
                    continue
                duplicate_clean_codes_removed += 1
                if _row_sort_key(row) < _row_sort_key(existing):
                    deduped_by_clean_code[normalized_clean_code] = row
                    duplicate_clean_codes_replaced += 1

            rows: list[tuple] = []
            for row in deduped_by_clean_code.values():
                rows.append(
                    (
                        row["clean_code"],
                        row["code"],
                        row["display"],
                        row["parent_code"],
                        row["level"],
                        *row["prop_values"],
                        row["code_sor"],
                        row["sor_relationship"],
                    )
                )

            context.log.info(
                "Deduplicated Rekvirent rows: %s kept, %s duplicates removed (%s replacements)",
                len(rows),
                duplicate_clean_codes_removed,
                duplicate_clean_codes_replaced,
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_rekvirent];")

            prop_col_defs = ", ".join(f"[{col}] NVARCHAR(200)" for col in prop_columns)

            cursor.execute(
                f"""
                CREATE TABLE [stg].[dim_rekvirent] (
                    [CleanCode_Rekvirent] VARCHAR(100) NOT NULL,
                    [Code] VARCHAR(100) NOT NULL,
                    [Display] NVARCHAR(500),
                    [ParentCode] VARCHAR(100),
                    [Level] INT,
                    {prop_col_defs},
                    [CodeSOR] VARCHAR(50),
                    [SORRelationship] VARCHAR(50)
                );
            """
            )

            num_cols = 5 + len(prop_columns) + 2
            placeholders = ", ".join(["?"] * num_cols)
            insert_sql = f"INSERT INTO [stg].[dim_rekvirent] VALUES ({placeholders})"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                cursor.executemany(insert_sql, batch)

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_rekvirent_CleanCode
                ON [stg].[dim_rekvirent]([CleanCode_Rekvirent]);
            """
            )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_rekvirent]")
            row_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    mapped_count = sum(1 for r in rows if r[-2] is not None)
    context.log.info(f"stg.dim_rekvirent: {row_count} rows ({mapped_count} mapped to SOR)")

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_rekvirent",
            "row_count": row_count,
            "raw_row_count": len(row_candidates),
            "sor_mapped": mapped_count,
            "property_columns": prop_columns,
            "duplicate_clean_codes_removed": duplicate_clean_codes_removed,
            "duplicate_clean_codes_replaced": duplicate_clean_codes_replaced,
        }
    )
