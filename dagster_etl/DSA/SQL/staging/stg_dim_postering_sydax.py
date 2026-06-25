# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering from SydAX (Dimension Layer)

Deduplicate SydAX postering data at CleanCode_Postering level.
Derived from pre_stg_postering_sydax.

Grain: ref_LedgertransId via CleanCode_Postering (KILDESYSTEM_ref_LedgertransId)
Business key: bk_ref_LedgertransId
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_postering_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "postering",
    },
    description="Deduplicate SydAX postering at CleanCode_Postering level for dim.Postering",
)
def stg_dim_postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX postering dimension data.

    Deduplicates pre_stg_postering_sydax at CleanCode_Postering grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_postering_sydax from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_postering_sydax];")

            cursor.execute(
                """
                SELECT
                    CleanCode_Postering,
                    ref_LedgertransId,
                    bk_ref_LedgertransId,
                    ref_VendTransId,
                    DokumentId,
                    Kreditornummer,
                    Kreditornavn,
                    JuridiskEnhed,
                    Kontonummer,
                    FakturaNr,
                    SidstUdlignetBilagsnummer,
                    Bilagsnummer_Trans,
                    Bilagsnummer_Postering,
                    Valutakode_Trans,
                    Valutakode_Postering,
                    Transaktionstekst,
                    Posteringstekst
                INTO [stg].[dim_postering_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Postering
                            ORDER BY DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_postering_sydax]
                    WHERE CleanCode_Postering IS NOT NULL
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_postering_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_postering_sydax_CleanCode
                ON [stg].[dim_postering_sydax](CleanCode_Postering);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(Kreditornavn) AS has_navn,
                    COUNT(FakturaNr) AS has_faktura_nr
                FROM [stg].[dim_postering_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with Kreditornavn, "
                f"{stats[2]}/{stats[0]} with FakturaNr"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_postering_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_postering_sydax",
            "deduplication": "ROW_NUMBER() on CleanCode_Postering",
            "grain": "ref_LedgertransId via CleanCode_Postering",
            "has_kreditornavn": stats[1],
            "has_faktura_nr": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
