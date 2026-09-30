# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Fact Layer)

Select columns needed for fct_Finanskladdelinje from pre_stg_Finanskladdelinje_sydax.
Intermediate staging table for the fact table load.

Grain contract: one row per ref_ledgertrans_id (deterministic dedup applied here).
Business key: bk_ref_ledgertrans_id
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

from etl.DSA.SQL.common.dato_dimension import dato_key, dato_tid


@asset(
    key=AssetKey(["erp", "stg_fct_Finanskladdelinje_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Finanskladdelinje_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "finanskladdelinje",
    },
    description=(
        "Select columns needed for fct_Finanskladdelinje from "
        "pre_stg_Finanskladdelinje_sydax. Grain: ref_ledgertrans_id."
    ),
)
def stg_fct_Finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX finanskladdelinje fact table.

    Enforces grain (one row per ref_ledgertrans_id) via deterministic dedup and
    performs measure and date key conversions for downstream MERGE.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_Finanskladdelinje_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Finanskladdelinje_sydax];")

            cursor.execute(
                f"""
                ;WITH grain_dedup AS (
                    SELECT
                        p.ref_ledgertrans_id,
                        p.bk_ref_ledgertrans_id,
                        p.clean_code_Finanskladdelinje,
                        p.clean_code_Finanskladde,
                        p.clean_code_Kontoplan,
                        p.omkostningsstednummer,
                        p.debet,
                        p.kredit,
                        p.Dato,
                        p.Linje_CreatedOn,
                        p.dw_timestamp_update,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.ref_ledgertrans_id
                            ORDER BY p.dw_timestamp_update DESC
                        ) AS grain_rn
                    FROM [stg].[pre_Finanskladdelinje_sydax] p
                    WHERE p.ref_ledgertrans_id IS NOT NULL
                )

                SELECT
                    g.ref_ledgertrans_id,
                    g.bk_ref_ledgertrans_id,
                    g.clean_code_Finanskladdelinje,
                    g.clean_code_Finanskladde,
                    g.clean_code_Kontoplan,
                    g.omkostningsstednummer,

                    TRY_CONVERT(DECIMAL(30,12), g.debet) AS debet,
                    TRY_CONVERT(DECIMAL(30,12), g.kredit) AS kredit,

                    {dato_key('g.Dato')} AS dw_ek_Dato_linje,
                    {dato_tid('g.Dato')} AS dato_tid_linje,
                    {dato_key('g.Linje_CreatedOn')} AS dw_ek_Dato_oprettet,
                    {dato_tid('g.Linje_CreatedOn')} AS dato_tid_oprettet,

                    TRY_CONVERT(DATETIME2, g.Linje_CreatedOn) AS dw_haendelsestidspunkt
                INTO [stg].[fct_Finanskladdelinje_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_Finanskladdelinje_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [stg].[pre_Finanskladdelinje_sydax]
                WHERE ref_ledgertrans_id IS NOT NULL
            """
            )
            pre_stg_count = cursor.fetchone()[0]
            duplicates_removed = pre_stg_count - row_count
            if duplicates_removed > 0:
                context.log.warning(
                    f"Removed {duplicates_removed} duplicate ref_ledgertrans_id rows "
                    "during grain enforcement (kept latest by dw_timestamp_update)"
                )

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_fct_finanskladdelinje_sydax_PK
                ON [stg].[fct_Finanskladdelinje_sydax](ref_ledgertrans_id);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Finanskladdelinje
                ON [stg].[fct_Finanskladdelinje_sydax](clean_code_Finanskladdelinje);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Finanskladde
                ON [stg].[fct_Finanskladdelinje_sydax](clean_code_Finanskladde);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_finanskladdelinje_sydax_Kontoplan
                ON [stg].[fct_Finanskladdelinje_sydax](clean_code_Kontoplan);
            """
            )

            context.log.info("Created indexes for grain and map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(clean_code_Finanskladdelinje) AS has_linje,
                    COUNT(clean_code_Finanskladde) AS has_kladde,
                    COUNT(clean_code_Kontoplan) AS has_kontoplan
                FROM [stg].[fct_Finanskladdelinje_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} clean_code_Finanskladdelinje, "
                f"{stats[2]}/{stats[0]} clean_code_Finanskladde, "
                f"{stats[3]}/{stats[0]} clean_code_Kontoplan"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Dato_linje <> -1 THEN 1 ELSE 0 END) AS has_linje_dato,
                    SUM(CASE WHEN dw_ek_Dato_oprettet <> -1 THEN 1 ELSE 0 END) AS has_oprettet,
                    SUM(CASE WHEN dw_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_Finanskladdelinje_sydax]
            """
            )
            date_stats = cursor.fetchone()
            context.log.info(
                f"Date key coverage (non -1): Linje={date_stats[1]}/{date_stats[0]}, "
                f"Oprettet={date_stats[2]}/{date_stats[0]}, "
                f"Haendelse={date_stats[3]}/{date_stats[0]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Finanskladdelinje_sydax",
            "target_table": "stg.fct_Finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Finanskladdelinje_sydax",
            "grain": "ref_ledgertrans_id",
            "duplicates_removed": duplicates_removed,
            "has_cleancode_finanskladdelinje": stats[1],
            "has_cleancode_finanskladde": stats[2],
            "has_cleancode_kontoplan": stats[3],
            "layer": "stg_fct (Layer 1)",
        }
    )
