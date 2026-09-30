# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering from SydAX (Dimension Layer)

Deduplicate SydAX postering data at clean_code_Postering level.
Derived from pre_stg_Postering_sydax.

Grain: ref_ledgertrans_id via clean_code_Postering (KILDESYSTEM_ref_LedgertransId)
Business key: bk_ref_ledgertrans_id
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Postering_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "postering",
    },
    description="Deduplicate SydAX postering at clean_code_Postering level for dim.Postering",
)
def stg_dim_Postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX postering dimension data.

    Deduplicates pre_stg_Postering_sydax at clean_code_Postering grain,
    keeping the most recent row per CleanCode.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_Postering_sydax from base layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Postering_sydax];")

            cursor.execute(
                """
                SELECT
                    clean_code_Postering,
                    ref_ledgertrans_id,
                    bk_ref_ledgertrans_id,
                    ref_vend_trans_id,
                    dokument_id,
                    kreditornummer,
                    kreditornavn,
                    juridisk_enhed,
                    kontonummer,
                    faktura_nr,
                    sidst_udlignet_bilagsnummer,
                    bilagsnummer_trans,
                    bilagsnummer_postering,
                    valutakode_trans,
                    valutakode_postering,
                    transaktionstekst,
                    posteringstekst
                INTO [stg].[dim_Postering_sydax]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY clean_code_Postering
                            ORDER BY dw_timestamp_update DESC
                        ) AS rn
                    FROM [stg].[pre_Postering_sydax]
                    WHERE clean_code_Postering IS NOT NULL
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Postering_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_postering_sydax_CleanCode
                ON [stg].[dim_Postering_sydax](clean_code_Postering);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(kreditornavn) AS has_navn,
                    COUNT(faktura_nr) AS has_faktura_nr
                FROM [stg].[dim_Postering_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with kreditornavn, "
                f"{stats[2]}/{stats[0]} with faktura_nr"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Postering_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Postering_sydax",
            "deduplication": "ROW_NUMBER() on clean_code_Postering",
            "grain": "ref_ledgertrans_id via clean_code_Postering",
            "has_kreditornavn": stats[1],
            "has_faktura_nr": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
