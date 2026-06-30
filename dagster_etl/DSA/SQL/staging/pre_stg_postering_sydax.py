# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering from SydAX (Base Layer)

Pass-through from ext.Finanspostering_enriched into stg.pre_postering_sydax.
The enrichment logic (joins, CleanCodes, hierarchy lookups) now lives in the
DSI layer (DSI/mssql/sydax_enriched.py) which writes ext.Finanspostering_enriched.

Grain: ref_LedgertransId (unique BIGINT from SydAX ledger)
Business key: bk_ref_LedgertransId
Partition column (upstream): Oprettelsestidspunkt
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "pre_stg_postering_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_finanspostering_enriched_sydax_daily"]),
        AssetKey(["ext_finanspostering_enriched_sydax_monthly"]),
        AssetKey(["ext_finanspostering_enriched_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "postering",
    },
    description=(
        "Pass-through from ext.Finanspostering_enriched to stg.pre_postering_sydax. "
        "Enrichment logic now lives in DSI layer."
    ),
)
def pre_stg_postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Copy ext.Finanspostering_enriched into stg.pre_postering_sydax."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Creating stg.pre_postering_sydax from ext.Finanspostering_enriched..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[pre_postering_sydax];")

            cursor.execute(
                """
                SELECT *,
                    KreditorId AS bk_KreditorId
                INTO [stg].[pre_postering_sydax]
                FROM [ext].[Finanspostering_enriched];
                """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.pre_postering_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_pre_postering_sydax_PK
                ON [stg].[pre_postering_sydax](ref_LedgertransId);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_postering_sydax_CleanCode
                ON [stg].[pre_postering_sydax](CleanCode_Postering);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_postering_sydax_CVR
                ON [stg].[pre_postering_sydax](CleanCode_Kreditor)
                INCLUDE (KreditorId, Kreditornummer, Kreditornavn, JuridiskEnhed);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_postering_sydax_Kontoplan
                ON [stg].[pre_postering_sydax](CleanCode_Kontoplan)
                INCLUDE (bk_FinanskontoId, Kontonummer);
                """
            )

            context.log.info("Created indexes for entity aggregation")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kreditor) AS has_cleancode_kreditor,
                    COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(DISTINCT CleanCode_Kreditor) AS unique_cleancode_kreditor,
                    COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cleancode_kontoplan,
                    COUNT(ref_VendTransId) AS has_ref_vendtransid,
                    COUNT(Kreditornummer) AS has_kreditornummer,
                    COUNT(Kreditornavn) AS has_kreditornavn,
                    COUNT(FakturaNr) AS has_fakturanr,
                    COUNT(Bogfoeringsdato) AS has_bogfoeringsdato,
                    COUNT(Post_Oprettelsestidspunkt) AS has_post_oprettelsestidspunkt,
                    COUNT(GodkendtDato) AS has_godkendtdato,
                    COUNT(Posteringsbeloeb) AS has_posteringsbeloeb,
                    COUNT(Transaktionsbeloeb) AS has_transaktionsbeloeb
                FROM [stg].[pre_postering_sydax]
                """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                f"{stats[2]}/{stats[0]} with CleanCode_Kontoplan. "
                f"Unique CleanCode_Kreditor: {stats[3]}, unique CleanCode_Kontoplan: {stats[4]}"
            )
            context.log.info(
                f"Bridge/trans/kred coverage: "
                f"ref_VendTransId={stats[5]}/{stats[0]}, "
                f"Kreditornummer={stats[6]}/{stats[0]}, "
                f"Kreditornavn={stats[7]}/{stats[0]}, "
                f"FakturaNr={stats[8]}/{stats[0]}"
            )
            context.log.info(
                f"Date coverage: "
                f"Bogfoeringsdato={stats[9]}/{stats[0]}, "
                f"Post_Oprettelsestidspunkt={stats[10]}/{stats[0]}, "
                f"GodkendtDato={stats[11]}/{stats[0]}"
            )
            context.log.info(
                f"Measure coverage: "
                f"Posteringsbeloeb={stats[12]}/{stats[0]}, "
                f"Transaktionsbeloeb={stats[13]}/{stats[0]}"
            )

            cursor.execute(
                """
                SELECT
                    c.name AS column_name,
                    t.name AS data_type,
                    c.max_length,
                    c.precision,
                    c.scale
                FROM sys.columns c
                JOIN sys.types t ON c.user_type_id = t.user_type_id
                WHERE c.object_id = OBJECT_ID('[stg].[pre_postering_sydax]')
                  AND c.name IN (
                      'Bogfoeringsdato', 'Post_Oprettelsestidspunkt',
                      'GodkendtDato', 'Transaktionsdato',
                      'Posteringsbeloeb', 'Transaktionsbeloeb',
                      'ref_VendTransId', 'ref_LedgertransId'
                  )
                ORDER BY c.column_id
                """
            )
            type_rows = cursor.fetchall()
            for tr in type_rows:
                context.log.info(
                    f"  Column type: {tr[0]} -> {tr[1]}"
                    f"(max_length={tr[2]}, precision={tr[3]}, scale={tr[4]})"
                )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.pre_postering_sydax",
            "row_count": row_count,
            "source_table": "ext.Finanspostering_enriched",
            "grain": "ref_LedgertransId",
            "unique_cvrs": stats[3],
            "unique_kontoplaner": stats[4],
            "has_ref_vendtransid": stats[5],
            "has_kreditornummer": stats[6],
            "has_bogfoeringsdato": stats[9],
            "has_post_oprettelsestidspunkt": stats[10],
            "has_posteringsbeloeb": stats[12],
            "has_transaktionsbeloeb": stats[13],
            "layer": "stg_fct (Layer 0)",
        }
    )
