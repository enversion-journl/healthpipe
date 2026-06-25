# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: dim_lager from ILS Lager codesystem.

Reads the Lager codesystem tables (codesystem.lager_concept, _properties, _hierarchy)
and the lager-to-sor ConceptMap table, pivots selected property keys into columns,
and writes the result to stg.dim_lager in the staging database.

Property-to-column mapping is configured via PROPERTY_COLUMN_MAP. To add more
properties as columns, simply extend the dict.
"""

import dagster as dg
from core.resources import MSSQLResource

PROPERTY_COLUMN_MAP: dict[str, str] = {
    "entityType": "EntityType",
    "sgh": "SGH",
    "lgr": "LGR",
    "adr": "Adr",
    "land": "Land",
    "postdi": "Postdi",
    "tlf": "Tlf",
}


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    """Build child_code -> parent_code dict from codesystem.lager_hierarchy."""
    cursor.execute("SELECT ChildCode, ParentCode FROM [codesystem].[LagerHierarchy]")
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
    """Build code -> {propertyCode: value} dict from codesystem.lager_properties."""
    cursor.execute("SELECT Code, PropertyCode, Value FROM [codesystem].[LagerProperties]")
    lookup: dict[str, dict[str, str]] = {}
    for row in cursor.fetchall():
        code = str(row[0]).strip()
        key = str(row[1]).strip()
        value = str(row[2]).strip() if row[2] else ""
        lookup.setdefault(code, {})[key] = value
    return lookup


def _load_sor_mapping(cursor) -> dict[str, tuple[str, str]]:
    """Load SOR mapping from conceptmap.lager_to_sor."""
    cursor.execute("SELECT SourceCode, TargetCode, Relationship FROM [conceptmap].[LagerToSOR]")
    return {
        str(row[0]).strip(): (str(row[1]).strip(), str(row[2]).strip()) for row in cursor.fetchall()
    }


@dg.asset(
    name="stg_dim_lager",
    deps=["cs_lager", "cm_lager_to_sor"],
    group_name="erp_staging",
    compute_kind="mssql",
    description=(
        "Stage Lager dimension from codesystem + ConceptMap tables. "
        "Reads codesystem.lager_* and conceptmap.lager_to_sor, "
        "then writes stg.dim_lager with pivoted property columns."
    ),
)
def stg_dim_lager(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    prop_columns = list(PROPERTY_COLUMN_MAP.values())
    prop_keys = list(PROPERTY_COLUMN_MAP.keys())

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[LagerConcept]")
            concepts = [(str(r[0]).strip(), r[1]) for r in cursor.fetchall()]
            context.log.info(f"Loaded {len(concepts)} concepts from codesystem.lager_concept")

            hierarchy_lookup = _build_hierarchy_lookup(cursor)
            levels = _compute_levels(hierarchy_lookup)
            properties_lookup = _build_properties_lookup(cursor)
            sor_mapping = _load_sor_mapping(cursor)
            context.log.info(f"Loaded SOR mapping with {len(sor_mapping)} entries")

            def _get_prop(code: str, key: str) -> str | None:
                return properties_lookup.get(code, {}).get(key) or None

            rows: list[tuple] = []
            for code, display in concepts:
                display_val = display if display else None
                parent_code = hierarchy_lookup.get(code)
                level = levels.get(code, 1)
                prop_values = [_get_prop(code, k) for k in prop_keys]

                sor_entry = sor_mapping.get(code)
                code_sor = sor_entry[0] if sor_entry else None
                sor_relationship = sor_entry[1] if sor_entry else None

                rows.append(
                    (
                        code,
                        code,
                        display_val,
                        parent_code,
                        level,
                        *prop_values,
                        code_sor,
                        sor_relationship,
                    )
                )

            context.log.info(f"Built {len(rows)} staging rows")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_lager];")

            prop_col_defs = ", ".join(f"[{col}] NVARCHAR(200)" for col in prop_columns)

            cursor.execute(
                f"""
                CREATE TABLE [stg].[dim_lager] (
                    [CleanCode_Lager] VARCHAR(100) NOT NULL,
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
            insert_sql = f"INSERT INTO [stg].[dim_lager] VALUES ({placeholders})"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                cursor.executemany(insert_sql, batch)

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_lager_CleanCode
                ON [stg].[dim_lager]([CleanCode_Lager]);
            """
            )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_lager]")
            row_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    mapped_count = sum(1 for r in rows if r[-2] is not None)
    context.log.info(f"stg.dim_lager: {row_count} rows ({mapped_count} mapped to SOR)")

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_lager",
            "row_count": row_count,
            "sor_mapped": mapped_count,
            "property_columns": prop_columns,
        }
    )
