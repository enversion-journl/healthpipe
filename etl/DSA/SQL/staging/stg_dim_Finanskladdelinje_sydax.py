# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Dimension Layer)

Deduplicate SydAX finanskladdelinje data at clean_code_Finanskladdelinje level.
Derived from pre_stg_Finanskladdelinje_sydax.

Grain: ref_ledgertrans_id via clean_code_Finanskladdelinje
Business key: bk_ref_ledgertrans_id
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Finanskladdelinje_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Finanskladdelinje_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "finanskladdelinje",
    },
    description=(
        "Deduplicate SydAX finanskladdelinje at clean_code_Finanskladdelinje level "
        "for dim.Finanskladdelinje"
    ),
)
def stg_dim_Finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX finanskladdelinje dimension data.

    Deduplicates pre_Finanskladdelinje_sydax at clean_code_Finanskladdelinje grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info(
                "Creating staging table stg.dim_Finanskladdelinje_sydax from base layer..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Finanskladdelinje_sydax];")

            cursor.execute(
                """
                SELECT
                    clean_code_Finanskladdelinje,
                    bk_ref_ledgertrans_id,
                    beskrivelse,
                    linjenummer,
                    ekspeditionsnummer,
                    transaktionstypenummer,
                    transaktionstypetekst,
                    maskinnummer
                INTO [stg].[dim_Finanskladdelinje_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_Finanskladdelinje
                            ORDER BY dw_timestamp_update DESC
                        ) AS rn
                    FROM [stg].[pre_Finanskladdelinje_sydax]
                    WHERE clean_code_Finanskladdelinje IS NOT NULL
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Finanskladdelinje_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_finanskladdelinje_sydax_CleanCode
                ON [stg].[dim_Finanskladdelinje_sydax](clean_code_Finanskladdelinje);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(beskrivelse) AS has_beskrivelse,
                    COUNT(transaktionstypetekst) AS has_transaktionstypetekst
                FROM [stg].[dim_Finanskladdelinje_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with beskrivelse, "
                f"{stats[2]}/{stats[0]} with transaktionstypetekst"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Finanskladdelinje_sydax",
            "target_table": "stg.dim_Finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Finanskladdelinje_sydax",
            "deduplication": "ROW_NUMBER() on clean_code_Finanskladdelinje",
            "grain": "ref_ledgertrans_id via clean_code_Finanskladdelinje",
            "has_beskrivelse": stats[1],
            "has_transaktionstypetekst": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
