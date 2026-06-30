# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Aftale from ILS (Dimension Layer)

Aggregate ILS agreement data at aftale grain.
Derived from pre_stg_vareaftaler_ils with deterministic deduplication.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_aftale_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_vareaftaler_ils"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "aftale",
    },
    description="Aggregate ILS aftale data at aftale grain using latest start-date deduplication",
)
def stg_dim_aftale_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Create staging table for aftale dimension with one row per aftale."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_aftale_ils from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_aftale_ils];")

            cursor.execute(
                """
                SELECT
                    CleanCode_Aftale,
                    aftale AS AftaleNummer,
                    afttype AS AftaleType,
                    aftaletekst AS Aftaletekst,
                    hoved_start AS HovedStart,
                    hoved_slut AS HovedSlut,
                    DW_LoadTimestamp
                INTO [stg].[dim_aftale_ils]
                FROM (
                    SELECT
                        CleanCode_Aftale,
                        aftale,
                        afttype,
                        aftaletekst,
                        hoved_start,
                        hoved_slut,
                        DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY aftale
                            ORDER BY
                                COALESCE(hoved_start, linje_start) DESC,
                                DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_vareaftaler_ils]
                    WHERE aftale IS NOT NULL
                      AND LTRIM(RTRIM(aftale)) <> ''
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_aftale_ils with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_aftale_ils_CleanCode_Aftale
                ON [stg].[dim_aftale_ils](CleanCode_Aftale);
            """
            )

            cursor.execute(
                """
                SELECT COUNT(*) AS inconsistent_txt_aftaler
                FROM (
                    SELECT
                        aftale
                    FROM [stg].[pre_vareaftaler_ils]
                    WHERE aftale IS NOT NULL
                      AND LTRIM(RTRIM(aftale)) <> ''
                    GROUP BY aftale
                    HAVING COUNT(DISTINCT COALESCE(LTRIM(RTRIM(aftaletekst)), '')) > 1
                ) diff_txt;
            """
            )
            inconsistent_txt_aftaler = cursor.fetchone()[0]
            context.log.info(
                "Aftaler with inconsistent aftaletekst across lines: " f"{inconsistent_txt_aftaler}"
            )

            conn.commit()
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_aftale_ils",
            "row_count": row_count,
            "source_table": "stg.pre_vareaftaler_ils",
            "grain": "aftale",
            "deduplication": "ROW_NUMBER() by aftale ordered on COALESCE(hoved_start, linje_start) DESC",
            "inconsistent_aftaletekst_aftaler": inconsistent_txt_aftaler,
            "layer": "dim (Layer 1)",
        }
    )
