# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Dimension Layer)

Deduplicate SydAX finanskladdelinje data at CleanCode_Finanskladdelinje level.
Derived from pre_stg_finanskladdelinje_sydax.

Grain: ref_LedgertransId via CleanCode_Finanskladdelinje
Business key: bk_ref_LedgertransId
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_finanskladdelinje_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_finanskladdelinje_sydax"]),
        "map_finanskladdelinje",
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "finanskladdelinje",
    },
    description=(
        "Deduplicate SydAX finanskladdelinje at CleanCode_Finanskladdelinje level "
        "for dim.Finanskladdelinje"
    ),
)
def stg_dim_finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX finanskladdelinje dimension data.

    Deduplicates pre_finanskladdelinje_sydax at CleanCode_Finanskladdelinje grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info(
                "Creating staging table stg.dim_finanskladdelinje_sydax from base layer..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_finanskladdelinje_sydax];")

            cursor.execute(
                """
                SELECT
                    CleanCode_Finanskladdelinje,
                    bk_ref_LedgertransId,
                    Beskrivelse,
                    Linjenummer,
                    Ekspeditionsnummer,
                    Transaktionstypenummer,
                    Transaktionstypetekst,
                    Maskinnummer
                INTO [stg].[dim_finanskladdelinje_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Finanskladdelinje
                            ORDER BY DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_finanskladdelinje_sydax]
                    WHERE CleanCode_Finanskladdelinje IS NOT NULL
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_finanskladdelinje_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_finanskladdelinje_sydax_CleanCode
                ON [stg].[dim_finanskladdelinje_sydax](CleanCode_Finanskladdelinje);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Beskrivelse) AS has_beskrivelse,
                    COUNT(Transaktionstypetekst) AS has_transaktionstypetekst
                FROM [stg].[dim_finanskladdelinje_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Beskrivelse, "
                f"{stats[2]}/{stats[0]} with Transaktionstypetekst"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_finanskladdelinje_sydax",
            "target_table": "stg.dim_finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_finanskladdelinje_sydax",
            "deduplication": "ROW_NUMBER() on CleanCode_Finanskladdelinje",
            "grain": "ref_LedgertransId via CleanCode_Finanskladdelinje",
            "has_beskrivelse": stats[1],
            "has_transaktionstypetekst": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
