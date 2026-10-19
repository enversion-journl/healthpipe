# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: dim_Lager from ILS Lager codesystem.

Reads the Lager codesystem tables (codesystem.lager_concept, _properties, _hierarchy)
and the lager-to-sor ConceptMap table, pivots selected property keys into columns,
and writes the result to stg.dim_Lager in the staging database.

Property-to-column mapping is configured via PROPERTY_COLUMN_MAP. To add more
properties as columns, simply extend the dict.
"""

from collections.abc import Sequence

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.ils_clean_code_shared import sql_norm_code
from DSA.SQL.common.lager_adresse import LAGER_ADRESSE_COLUMNS

# Property key in codesystem.LagerProperties -> column in stg/dim.Lager.
#
# Every key here MUST be written by cs_lager (_LGR_ATTRIBUTE_PROPERTIES in
# DSA/CodeSystem/lager/lager.py). A key that is pivoted without being written
# yields a column that is always NULL — that is how dim.Lager.adr/land/postdi/tlf
# came about. Enforced by test_every_pivoted_lager_property_is_actually_written.
PROPERTY_COLUMN_MAP: dict[str, str] = {
    "entityType": "entity_type",
    "sgh": "sgh",
    "lgr": "lgr",
    "lagerejerRekvirentnummer": "lagerejer_rekvirentnummer",
    "svindRekvirentnummer": "svind_rekvirentnummer",
    "standardLeveringsdage": "standard_leveringsdage",
    "aktiv": "aktiv",
}

# Base columns written by the bulk insert.
_BASE_COLUMNS: tuple[str, ...] = ("clean_code_Lager", "code", "display", "parent_code", "level")
_TRAILING_COLUMNS: tuple[str, ...] = ("code_sor", "sor_relationship")


def build_adresse_column_ddl() -> str:
    """CREATE TABLE fragment for the joined address columns."""
    return ", ".join(f"[{name}] {sql_type}" for name, _, sql_type in LAGER_ADRESSE_COLUMNS)


def build_staging_insert_sql(prop_columns: Sequence[str] | None = None) -> str:
    """Bulk INSERT with an explicit column list.

    The address columns are filled in by ``build_adresse_update_sql`` afterwards,
    so an ``INSERT ... VALUES`` without column names would miscount the moment the
    table gained more columns than the tuple has values.
    """
    if prop_columns is None:
        prop_columns = list(PROPERTY_COLUMN_MAP.values())
    columns = (*_BASE_COLUMNS, *prop_columns, *_TRAILING_COLUMNS)
    column_list = ", ".join(f"[{col}]" for col in columns)
    placeholders = ", ".join(["?"] * len(columns))
    return f"INSERT INTO [stg].[dim_Lager] ({column_list}) VALUES ({placeholders})"


def build_adresse_update_sql() -> str:
    """UPDATE that pulls the warehouse address details in from arc.ils_lgr.

    Address fields are populated outside the CodeSystem — see
    DSA/SQL/common/lager_adresse.py.

    ``arc.ils_lgr`` has prrowid as its PK but (sgh, lgr) as the logical key, so a
    ROW_NUMBER picks the row deterministically; without it ``UPDATE ... FROM``
    would grab an arbitrary row on duplicates without failing.

    INNER JOIN: concepts with no dbo.lgr row (region, hospital) keep NULL, which
    is exactly the LEFT JOIN result.
    """
    sgh_stg = sql_norm_code("stg.[sgh]")
    lgr_stg = sql_norm_code("stg.[lgr]")
    sgh_lgr = sql_norm_code("[sgh]")
    lgr_lgr = sql_norm_code("[lgr]")

    source_columns = ",\n            ".join(f"[{src}]" for _, src, _ in LAGER_ADRESSE_COLUMNS)
    set_clauses = ",\n        ".join(
        f"stg.[{name}] = NULLIF(LTRIM(RTRIM(r.[{src}])), '')"
        for name, src, _ in LAGER_ADRESSE_COLUMNS
    )

    return f"""
    WITH lgr_dedup AS (
        SELECT
            {sgh_lgr} AS sgh_key,
            {lgr_lgr} AS lgr_key,
            {source_columns},
            ROW_NUMBER() OVER (
                PARTITION BY {sgh_lgr}, {lgr_lgr}
                ORDER BY [pro2modified] DESC, [prrowid] ASC
            ) AS rn
        FROM [arc].[ils_lgr]
    )
    UPDATE stg
    SET
        {set_clauses}
    FROM [stg].[dim_Lager] stg
    INNER JOIN lgr_dedup r
        ON r.rn = 1
       AND r.sgh_key = {sgh_stg}
       AND r.lgr_key = {lgr_stg};
"""


def _build_hierarchy_lookup(cursor) -> dict[str, str]:
    """Build child_code -> parent_code dict from codesystem.lager_hierarchy."""
    cursor.execute("SELECT child_code, parent_code FROM [codesystem].[LagerHierarchy]")
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
    cursor.execute("SELECT code, property_code, value FROM [codesystem].[LagerProperties]")
    lookup: dict[str, dict[str, str]] = {}
    for row in cursor.fetchall():
        code = str(row[0]).strip()
        key = str(row[1]).strip()
        value = str(row[2]).strip() if row[2] else ""
        lookup.setdefault(code, {})[key] = value
    return lookup


def _load_sor_mapping(cursor) -> dict[str, tuple[str, str]]:
    """Load SOR mapping from conceptmap.lager_to_sor."""
    cursor.execute("SELECT source_code, target_code, relationship FROM [conceptmap].[LagerToSOR]")
    return {
        str(row[0]).strip(): (str(row[1]).strip(), str(row[2]).strip()) for row in cursor.fetchall()
    }


@dg.asset(
    name="stg_dim_Lager",
    deps=["cs_lager", "cm_lager_to_sor", "arc_ils_lgr"],
    group_name="erp_staging",
    compute_kind="mssql",
    description=(
        "Stage Lager dimension from codesystem + ConceptMap tables. "
        "Reads codesystem.lager_* and conceptmap.lager_to_sor, "
        "then writes stg.dim_Lager with pivoted property columns."
    ),
)
def stg_dim_Lager(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    prop_columns = list(PROPERTY_COLUMN_MAP.values())
    prop_keys = list(PROPERTY_COLUMN_MAP.keys())

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("stg", "dim_Lager", dagster_run_id=context.run_id, mode=config.mode)

    row_count = 0
    mapped_count = 0
    adresse_rows = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT Code, display FROM [codesystem].[LagerConcept]")
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

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Lager];")

                prop_col_defs = ", ".join(f"[{col}] NVARCHAR(200)" for col in prop_columns)

                cursor.execute(
                    f"""
                    CREATE TABLE [stg].[dim_Lager] (
                        [clean_code_Lager] VARCHAR(100) NOT NULL,
                        [code] VARCHAR(100) NOT NULL,
                        [display] NVARCHAR(500),
                        [parent_code] VARCHAR(100),
                        [level] INT,
                        {prop_col_defs},
                        [code_sor] VARCHAR(50),
                        [sor_relationship] VARCHAR(50),
                        {build_adresse_column_ddl()}
                    );
                """
                )

                insert_sql = build_staging_insert_sql(prop_columns)

                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True

                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    cursor.executemany(insert_sql, batch)

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_lager_CleanCode
                    ON [stg].[dim_Lager]([clean_code_Lager]);
                """
                )

                # Address details from the archive.
                cursor.execute(build_adresse_update_sql())
                adresse_rows = cursor.rowcount

                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Lager]")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        mapped_count = sum(1 for r in rows if r[-2] is not None)
        context.log.info(f"stg.dim_Lager: {row_count} rows ({mapped_count} mapped to SOR)")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_Lager",
            "row_count": row_count,
            "sor_mapped": mapped_count,
            "property_columns": prop_columns,
            "adresse_columns": [name for name, _, _ in LAGER_ADRESSE_COLUMNS],
            "adresse_rows_enriched": adresse_rows,
            "mode": config.mode,
        }
    )
