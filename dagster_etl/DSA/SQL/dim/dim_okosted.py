# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for Okosted (Omkostningssted).

Populates dim.Okosted with Type 1 SCD.
Resolves parent DW_EK from the self-referencing hierarchy.
"""

import dagster as dg
from core.resources import MSSQLResource
from DSA.SQL.map.map_okosted import map_okosted
from DSA.SQL.staging.stg_okosted import stg_okosted


@dg.asset(
    name="dim_okosted",
    deps=["staging_infra_erp", map_okosted, stg_okosted],
    group_name="dwh_okosted",
    compute_kind="mssql",
    description="Populate dim.Okosted (Type 1 SCD)",
)
def dim_okosted(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    stg_okosted: list[dict],
) -> dg.MaterializeResult:
    """Populate dim.Okosted (Type 1 SCD)."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT CleanCode_Okosted, DW_EK_Okosted FROM [map].[Okosted]")
            clean_code_to_ek = {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()

    context.log.info(f"Loaded {len(clean_code_to_ek)} CleanCode_Okosted -> DW_EK_Okosted mappings")

    dim_rows = []
    for entry in stg_okosted:
        clean_code = entry["code"]
        dw_ek = clean_code_to_ek.get(clean_code)

        if not dw_ek:
            context.log.warning(f"No DW_EK found for {clean_code}")
            continue

        dw_ek_parent = -1
        if entry.get("parent_code"):
            parent_clean_code = entry["parent_code"]
            looked_up_ek = clean_code_to_ek.get(parent_clean_code)
            if looked_up_ek is not None:
                dw_ek_parent = looked_up_ek

        dim_rows.append(
            {
                "dw_ek": dw_ek,
                "code_okosted": entry["code"],
                "name": entry.get("display"),
                "level": entry.get("level"),
                "dw_ek_parent": dw_ek_parent,
                "parent_code": entry.get("parent_code"),
                "active": entry["aktiv"],
            }
        )

    context.log.info(f"Processing {len(dim_rows)} rows for dim.Okosted")

    dim_tuples = [
        (
            row["dw_ek"],
            row["code_okosted"],
            row["name"],
            row["level"],
            row["dw_ek_parent"],
            row["parent_code"],
            row["active"],
        )
        for row in dim_rows
    ]

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            is_mssql_python = type(conn).__module__.startswith("mssql_python")
            placeholder = "?" if is_mssql_python else "%s"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            cursor.execute(
                """
                CREATE TABLE #staging_dim_okosted (
                    DW_EK_Okosted INT NOT NULL,
                    CodeOkosted VARCHAR(50) NOT NULL,
                    Name NVARCHAR(500),
                    [Level] INT,
                    DW_EK_Okosted_Parent INT,
                    ParentCode VARCHAR(50),
                    Active BIT NOT NULL
                )
            """
            )

            batch_size = 1000
            total_inserted = 0

            placeholders = ", ".join([placeholder] * 7)
            insert_sql = (
                f"INSERT INTO #staging_dim_okosted ("
                f"DW_EK_Okosted, CodeOkosted, Name, [Level], "
                f"DW_EK_Okosted_Parent, ParentCode, Active"
                f") VALUES ({placeholders})"
            )

            for i in range(0, len(dim_tuples), batch_size):
                batch = dim_tuples[i : i + batch_size]
                cursor.executemany(insert_sql, batch)
                total_inserted += len(batch)

            context.log.info(f"Staged all {total_inserted} dimension rows in temp table")

            cursor.execute(
                """
                MERGE INTO [dim].[Okosted] AS target
                USING #staging_dim_okosted AS source
                ON target.DW_EK_Okosted = source.DW_EK_Okosted
                WHEN MATCHED THEN
                    UPDATE SET
                        CodeOkosted = source.CodeOkosted,
                        Name = source.Name,
                        [Level] = source.[Level],
                        DW_EK_Okosted_Parent = source.DW_EK_Okosted_Parent,
                        ParentCode = source.ParentCode,
                        Active = source.Active,
                        DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
                WHEN NOT MATCHED THEN
                    INSERT (DW_EK_Okosted, CodeOkosted, Name, [Level],
                            DW_EK_Okosted_Parent, ParentCode, Active,
                            DW_Timestamp_Insert)
                    VALUES (source.DW_EK_Okosted, source.CodeOkosted,
                            source.Name, source.[Level],
                            source.DW_EK_Okosted_Parent, source.ParentCode,
                            source.Active,
                            CAST(SYSDATETIME() AS datetime2(0)))
                OUTPUT $action;
            """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()
        finally:
            cursor.close()

    context.log.info(f"dim.Okosted: {inserted} inserted, {updated} updated")

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total": len(dim_rows),
            "method": "bulk_insert_with_merge",
        }
    )
