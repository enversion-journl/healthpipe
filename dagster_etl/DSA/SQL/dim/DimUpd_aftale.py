# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Aftale

Type 2 SCD maintenance for dim.Aftale.
Tracks changes in agreement attributes over time.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_aftale"]),
        AssetKey(["erp", "stg_dim_aftale_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type2",
    },
    description="Type 2 SCD sync for dim.Aftale using map.Aftale + stg.dim_aftale_ils",
)
def dim_aftale(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Type 2 SCD for aftaler."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Aftale Type 2 SCD sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Aftale]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Aftale entries to sync")

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM [stg].[dim_aftale_ils] s
                LEFT JOIN [map].[Aftale] m
                    ON m.CleanCode_Aftale = s.CleanCode_Aftale
                WHERE m.DW_EK_Aftale IS NULL
            """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(
                    f"Found {unmapped_rows} stg.dim_aftale_ils rows without map match"
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Aftale_Source;
            DROP TABLE IF EXISTS #Changed_Current;
            DROP TABLE IF EXISTS #Missing_Source;

            SELECT
                m.DW_EK_Aftale,
                s.CleanCode_Aftale,
                s.AftaleNummer,
                s.AftaleType,
                s.Aftaletekst,
                s.HovedStart,
                s.HovedSlut
            INTO #Aftale_Source
            FROM [stg].[dim_aftale_ils] s
            LEFT JOIN [map].[Aftale] m
                ON m.CleanCode_Aftale = s.CleanCode_Aftale
            WHERE m.DW_EK_Aftale IS NOT NULL;

            -- Current active rows that have attribute changes in source
            SELECT
                d.DW_EK_Aftale
            INTO #Changed_Current
            FROM [dim].[Aftale] d
            INNER JOIN #Aftale_Source s
                ON s.DW_EK_Aftale = d.DW_EK_Aftale
            WHERE d.DW_ValidTo = '9999-12-31'
              AND d.DW_IsDeleted = 0
              AND (
                  ISNULL(d.CleanCode_Aftale, '') <> ISNULL(s.CleanCode_Aftale, '')
                  OR ISNULL(d.AftaleNummer, '') <> ISNULL(s.AftaleNummer, '')
                  OR ISNULL(d.AftaleType, '') <> ISNULL(s.AftaleType, '')
                  OR ISNULL(d.Aftaletekst, '') <> ISNULL(s.Aftaletekst, '')
                  OR ISNULL(CAST(d.HovedStart AS datetime2(0)), '1900-01-01') <>
                     ISNULL(CAST(s.HovedStart AS datetime2(0)), '1900-01-01')
                  OR ISNULL(CAST(d.HovedSlut AS datetime2(0)), '1900-01-01') <>
                     ISNULL(CAST(s.HovedSlut AS datetime2(0)), '1900-01-01')
              );

            -- Current active rows missing from source (soft-delete candidates)
            SELECT
                d.DW_EK_Aftale
            INTO #Missing_Source
            FROM [dim].[Aftale] d
            WHERE d.DW_ValidTo = '9999-12-31'
              AND d.DW_IsDeleted = 0
              AND d.DW_EK_Aftale <> -1
              AND NOT EXISTS (
                  SELECT 1
                  FROM #Aftale_Source s
                  WHERE s.DW_EK_Aftale = d.DW_EK_Aftale
              );
        """
            )

            # Close changed current rows
            cursor.execute(
                """
            UPDATE d
            SET
                d.DW_ValidTo = CAST(SYSDATETIME() AS datetime2(0)),
                d.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            FROM [dim].[Aftale] d
            INNER JOIN #Changed_Current c
                ON c.DW_EK_Aftale = d.DW_EK_Aftale
            WHERE d.DW_ValidTo = '9999-12-31'
              AND d.DW_IsDeleted = 0;
        """
            )
            changed_closed = cursor.rowcount

            # Insert new versions for changed rows
            cursor.execute(
                """
            INSERT INTO [dim].[Aftale] (
                DW_EK_Aftale,
                CleanCode_Aftale,
                AftaleNummer,
                AftaleType,
                Aftaletekst,
                HovedStart,
                HovedSlut,
                DW_ValidFrom,
                DW_ValidTo,
                DW_IsDeleted,
                DW_Timestamp_Insert
            )
            SELECT
                s.DW_EK_Aftale,
                s.CleanCode_Aftale,
                s.AftaleNummer,
                s.AftaleType,
                s.Aftaletekst,
                s.HovedStart,
                s.HovedSlut,
                CAST(SYSDATETIME() AS datetime2(0)),
                '9999-12-31',
                0,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM #Aftale_Source s
            INNER JOIN #Changed_Current c
                ON c.DW_EK_Aftale = s.DW_EK_Aftale;
        """
            )
            changed_inserted = cursor.rowcount

            # Initial inserts (first ever version for new DW_EK_Aftale)
            cursor.execute(
                """
            INSERT INTO [dim].[Aftale] (
                DW_EK_Aftale,
                CleanCode_Aftale,
                AftaleNummer,
                AftaleType,
                Aftaletekst,
                HovedStart,
                HovedSlut,
                DW_ValidFrom,
                DW_ValidTo,
                DW_IsDeleted,
                DW_Timestamp_Insert
            )
            SELECT
                s.DW_EK_Aftale,
                s.CleanCode_Aftale,
                s.AftaleNummer,
                s.AftaleType,
                s.Aftaletekst,
                s.HovedStart,
                s.HovedSlut,
                '1900-01-01',
                '9999-12-31',
                0,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM #Aftale_Source s
            WHERE NOT EXISTS (
                SELECT 1
                FROM [dim].[Aftale] d
                WHERE d.DW_EK_Aftale = s.DW_EK_Aftale
            );
        """
            )
            initial_inserted = cursor.rowcount

            # Reactivation: source exists but no current row for DW_EK_Aftale
            cursor.execute(
                """
            INSERT INTO [dim].[Aftale] (
                DW_EK_Aftale,
                CleanCode_Aftale,
                AftaleNummer,
                AftaleType,
                Aftaletekst,
                HovedStart,
                HovedSlut,
                DW_ValidFrom,
                DW_ValidTo,
                DW_IsDeleted,
                DW_Timestamp_Insert
            )
            SELECT
                s.DW_EK_Aftale,
                s.CleanCode_Aftale,
                s.AftaleNummer,
                s.AftaleType,
                s.Aftaletekst,
                s.HovedStart,
                s.HovedSlut,
                CAST(SYSDATETIME() AS datetime2(0)),
                '9999-12-31',
                0,
                CAST(SYSDATETIME() AS datetime2(0))
            FROM #Aftale_Source s
            WHERE EXISTS (
                SELECT 1
                FROM [dim].[Aftale] d
                WHERE d.DW_EK_Aftale = s.DW_EK_Aftale
            )
              AND NOT EXISTS (
                  SELECT 1
                  FROM [dim].[Aftale] d
                  WHERE d.DW_EK_Aftale = s.DW_EK_Aftale
                    AND d.DW_ValidTo = '9999-12-31'
              );
        """
            )
            reactivated_inserted = cursor.rowcount

            # Soft-delete aftaler missing from source
            cursor.execute(
                """
            UPDATE d
            SET
                d.DW_IsDeleted = 1,
                d.DW_ValidTo = CAST(SYSDATETIME() AS datetime2(0)),
                d.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
            FROM [dim].[Aftale] d
            INNER JOIN #Missing_Source m
                ON m.DW_EK_Aftale = d.DW_EK_Aftale
            WHERE d.DW_ValidTo = '9999-12-31'
              AND d.DW_IsDeleted = 0;
        """
            )
            soft_deleted = cursor.rowcount

            cursor.execute("DROP TABLE IF EXISTS #Aftale_Source;")
            cursor.execute("DROP TABLE IF EXISTS #Changed_Current;")
            cursor.execute("DROP TABLE IF EXISTS #Missing_Source;")

            conn.commit()

            cursor.execute("UPDATE STATISTICS [dim].[Aftale]")

            cursor.execute("SELECT COUNT(*) FROM [dim].[Aftale]")
            total_rows = cursor.fetchone()[0]

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [dim].[Aftale]
            WHERE DW_ValidTo = '9999-12-31' AND DW_IsDeleted = 0;
        """
            )
            active_rows = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM [dim].[Aftale] WHERE DW_IsDeleted = 1")
            deleted_rows = cursor.fetchone()[0]

            context.log.info(
                "Aftale Type 2 sync complete: "
                f"changed_closed={changed_closed}, changed_inserted={changed_inserted}, "
                f"initial_inserted={initial_inserted}, reactivated_inserted={reactivated_inserted}, "
                f"soft_deleted={soft_deleted}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "dim.Aftale",
            "changed_closed": changed_closed,
            "changed_inserted": changed_inserted,
            "initial_inserted": initial_inserted,
            "reactivated_inserted": reactivated_inserted,
            "soft_deleted": soft_deleted,
            "unmapped_stg_rows": unmapped_rows,
            "total_rows": total_rows,
            "active_rows": active_rows,
            "deleted_rows": deleted_rows,
            "scd_type": "Type 2",
            "initial_valid_from": "1900-01-01",
            "source_table": "stg.dim_aftale_ils",
        }
    )
