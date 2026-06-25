# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Finanskladde

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial journal headers.

Uses:
- map.Finanskladde: DW_EK_Finanskladde assignment via IDENTITY
- stg.dim_finanskladde_sydax: Deduplicated finanskladde attributes at CleanCode_Finanskladde grain

Business key: bk_ref_LedgerjournaltableId
Descriptive attributes: Kladdenummer, Kladdetekst, Kladdenavn, ErLukket, DataAreaId, Maskinnummer
Date keys: DW_EK_Dato_Oprettet (CreatedOn), DW_EK_Dato_Aendret (Modified)
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="dim_finanskladde",
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        "map_finanskladde",
        AssetKey(["erp", "stg_dim_finanskladde_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=("Sync dim.Finanskladde from map + add contextual attributes from SydAX staging"),
)
def dim_finanskladde(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Finanskladde dimension sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladde]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Finanskladde entries to sync")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_finanskladde_sydax] fs
            LEFT JOIN [map].[Finanskladde] m
                ON m.CleanCode_Finanskladde = fs.CleanCode_Finanskladde
            WHERE m.DW_EK_Finanskladde IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(
                    f"Found {unmapped_rows} stg.dim_finanskladde_sydax rows without map match"
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Finanskladde_Attributes;

            SELECT
                m.DW_EK_Finanskladde,
                fs.bk_ref_LedgerjournaltableId,
                fs.Kladdenummer,
                fs.Kladdetekst,
                fs.Kladdenavn,
                fs.ErLukket,
                fs.DataAreaId,
                fs.Maskinnummer,
                COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, fs.CreatedOn)), -1) AS DW_EK_Dato_Oprettet,
                COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, fs.Modified)), -1) AS DW_EK_Dato_Aendret
            INTO #Finanskladde_Attributes
            FROM [stg].[dim_finanskladde_sydax] fs
            LEFT JOIN [map].[Finanskladde] m
                ON m.CleanCode_Finanskladde = fs.CleanCode_Finanskladde
            WHERE m.DW_EK_Finanskladde IS NOT NULL;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladde_Attributes
            ON #Finanskladde_Attributes (DW_EK_Finanskladde)
            """
            )
            context.log.info("Temp table #Finanskladde_Attributes created")

            cursor.execute(
                """
            MERGE [dim].[Finanskladde] AS tgt
            USING #Finanskladde_Attributes AS src
            ON tgt.DW_EK_Finanskladde = src.DW_EK_Finanskladde

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.bk_ref_LedgerjournaltableId = src.bk_ref_LedgerjournaltableId,
                    tgt.Kladdenummer = src.Kladdenummer,
                    tgt.Kladdetekst = src.Kladdetekst,
                    tgt.Kladdenavn = src.Kladdenavn,
                    tgt.ErLukket = src.ErLukket,
                    tgt.DataAreaId = src.DataAreaId,
                    tgt.Maskinnummer = src.Maskinnummer,
                    tgt.DW_EK_Dato_Oprettet = src.DW_EK_Dato_Oprettet,
                    tgt.DW_EK_Dato_Aendret = src.DW_EK_Dato_Aendret,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Finanskladde,
                    bk_ref_LedgerjournaltableId,
                    Kladdenummer,
                    Kladdetekst,
                    Kladdenavn,
                    ErLukket,
                    DataAreaId,
                    Maskinnummer,
                    DW_EK_Dato_Oprettet,
                    DW_EK_Dato_Aendret,
                    DW_Timestamp_Insert,
                    DW_Timestamp_Update
                )
                VALUES (
                    src.DW_EK_Finanskladde,
                    src.bk_ref_LedgerjournaltableId,
                    src.Kladdenummer,
                    src.Kladdetekst,
                    src.Kladdenavn,
                    src.ErLukket,
                    src.DataAreaId,
                    src.Maskinnummer,
                    src.DW_EK_Dato_Oprettet,
                    src.DW_EK_Dato_Aendret,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Finanskladde_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Finanskladde]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                COUNT(Kladdenavn) AS has_kladdenavn,
                COUNT(Kladdetekst) AS has_kladdetekst
            FROM [dim].[Finanskladde]
            WHERE DW_EK_Finanskladde <> -1
        """
            )
            stats = cursor.fetchone()
            total_dim_rows = stats[0]
            has_kladdenavn = stats[1]
            has_kladdetekst = stats[2]

            context.log.info(
                f"Dimension sync complete: {total_dim_rows} total rows in dim.Finanskladde — "
                f"coverage: Kladdenavn={has_kladdenavn}/{total_dim_rows}, "
                f"Kladdetekst={has_kladdetekst}/{total_dim_rows}"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "has_kladdenavn": has_kladdenavn,
            "has_kladdetekst": has_kladdetekst,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Finanskladde",
            "source_tables": "stg.dim_finanskladde_sydax + map.Finanskladde",
            "scd_type": "Type 1 (overwrite)",
        }
    )
