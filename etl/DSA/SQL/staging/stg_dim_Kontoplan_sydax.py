# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Kontoplan from SydAX (Dimension Layer)

Aggregate SydAX kontoplan data at clean_code_Kontoplan level with full hierarchy
attributes, built directly from the arc.sydax_* konto/hierarchy archives.

The hierarchy joins (finanskonto -> hovedkonto/gruppering/art/funktion/dranst +
finanskonto_omkostningssted -> omkostningssted_hierarki) are all keyed on
FinanskontoId, so the chart of accounts is reconstructed at account grain
without going through the postering pre-staging. This also enriches accounts
that have no postings (previously NULL hierarchy).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

STG_SQL = """
    ;WITH konto_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [FinanskontoId]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_finanskonto]
        WHERE [FinanskontoId] IS NOT NULL
    ),
    omkost_link_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [FinanskontoOmkostningsstedid]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_finanskonto_omkostningssted]
    ),
    omk_hier_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [OmkostningsstedHierarkiId]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_omkostningssted_hierarki]
    ),
    hk_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [ref_RECID]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_hovedkonto]
        WHERE [ref_RECID] IS NOT NULL
    ),
    grp_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [RECID]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_gruppering]
        WHERE [RECID] IS NOT NULL
    ),
    grp_aut_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [ref_RECID]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_gruppering_autoriseret]
    ),
    art_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [RECID]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_art]
        WHERE [RECID] IS NOT NULL
    ),
    funk_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [F1], [F2], [F3]
                   ORDER BY [Modified] DESC, [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_funktion]
    ),
    dr_dedup AS (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY [ref_RECID]
                   ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
               ) AS _rn
        FROM [arc].[sydax_dranst]
    )

    SELECT
        clean_code_Kontoplan,
        finanskonto_id,
        kontonummer,
        f1, f2, f3, d1, g1, g2,
        s1, s2, s3, s4, s5, s6, s7,
        a1, a2,
        tekst_s1, tekst_s2, tekst_s3, tekst_s4, tekst_s5, tekst_s6, tekst_s7,
        tekst_g1, tekst_g2, tekst_a1, tekst_a2,
        tekst_f1, tekst_f2, tekst_f3, tekst_d1,
        omkostningsstednummer, omkostningssted_niveau,
        dw_timestamp_update
    INTO [stg].[dim_Kontoplan_sydax]
    FROM (
        SELECT
            enriched.*,
            ROW_NUMBER() OVER (
                PARTITION BY clean_code_Kontoplan
                ORDER BY
                    CASE WHEN omkostningsstednummer IS NOT NULL THEN 0 ELSE 1 END,
                    dw_timestamp_update DESC
            ) AS rn
        FROM (
            SELECT
                'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                    CAST(konto.[FinanskontoId] AS NVARCHAR(100)), ' ', '')
                ))) AS clean_code_Kontoplan,
                CAST(konto.[FinanskontoId] AS BIGINT) AS finanskonto_id,
                konto.[Kontonummer] AS kontonummer,

                CAST(hk.[F1] AS NVARCHAR(50)) AS f1,
                CAST(hk.[F2] AS NVARCHAR(50)) AS f2,
                CAST(hk.[F3] AS NVARCHAR(50)) AS f3,
                CAST(hk.[D1] AS NVARCHAR(50)) AS d1,
                CAST(hk.[G1] AS NVARCHAR(50)) AS g1,
                CAST(grp.[Grupperingsnummer] AS NVARCHAR(50)) AS g2,

                CAST(omk_hier.[nummer1] AS NVARCHAR(50)) AS s1,
                CAST(omk_hier.[nummer2] AS NVARCHAR(50)) AS s2,
                CAST(omk_hier.[nummer3] AS NVARCHAR(50)) AS s3,
                CAST(omk_hier.[nummer4] AS NVARCHAR(50)) AS s4,
                CAST(omk_hier.[nummer5] AS NVARCHAR(50)) AS s5,
                CAST(omk_hier.[nummer6] AS NVARCHAR(50)) AS s6,
                CAST(omk_hier.[nummer7] AS NVARCHAR(50)) AS s7,

                CAST(art.[A1] AS NVARCHAR(50)) AS a1,
                CAST(art.[A2] AS NVARCHAR(50)) AS a2,

                CAST(omk_hier.[tekst1] AS NVARCHAR(255)) AS tekst_s1,
                CAST(omk_hier.[tekst2] AS NVARCHAR(255)) AS tekst_s2,
                CAST(omk_hier.[tekst3] AS NVARCHAR(255)) AS tekst_s3,
                CAST(omk_hier.[tekst4] AS NVARCHAR(255)) AS tekst_s4,
                CAST(omk_hier.[tekst5] AS NVARCHAR(255)) AS tekst_s5,
                CAST(omk_hier.[tekst6] AS NVARCHAR(255)) AS tekst_s6,
                CAST(omk_hier.[tekst7] AS NVARCHAR(255)) AS tekst_s7,

                COALESCE(
                    CAST(grp_aut.[G1Tekst] AS NVARCHAR(255)),
                    CAST(grp.[Grupperingstekst] AS NVARCHAR(255))
                ) AS tekst_g1,
                CAST(grp.[Grupperingstekst] AS NVARCHAR(255)) AS tekst_g2,
                CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_a1,
                CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_a2,
                CAST(funk_f1.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f1,
                CAST(funk_f2.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f2,
                CAST(funk_f3.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f3,
                CAST(dr.[D1Tekst] AS NVARCHAR(255)) AS tekst_d1,

                COALESCE(
                    CAST(omk_hier.[nummer20] AS NVARCHAR(50)), CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer18] AS NVARCHAR(50)), CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer16] AS NVARCHAR(50)), CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer14] AS NVARCHAR(50)), CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer12] AS NVARCHAR(50)), CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer10] AS NVARCHAR(50)), CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer8] AS NVARCHAR(50)), CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer6] AS NVARCHAR(50)), CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer4] AS NVARCHAR(50)), CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer2] AS NVARCHAR(50)), CAST(omk_hier.[nummer1] AS NVARCHAR(50))
                ) AS omkostningsstednummer,
                CASE
                    WHEN omk_hier.[nummer20] IS NOT NULL THEN 20 WHEN omk_hier.[nummer19] IS NOT NULL THEN 19
                    WHEN omk_hier.[nummer18] IS NOT NULL THEN 18 WHEN omk_hier.[nummer17] IS NOT NULL THEN 17
                    WHEN omk_hier.[nummer16] IS NOT NULL THEN 16 WHEN omk_hier.[nummer15] IS NOT NULL THEN 15
                    WHEN omk_hier.[nummer14] IS NOT NULL THEN 14 WHEN omk_hier.[nummer13] IS NOT NULL THEN 13
                    WHEN omk_hier.[nummer12] IS NOT NULL THEN 12 WHEN omk_hier.[nummer11] IS NOT NULL THEN 11
                    WHEN omk_hier.[nummer10] IS NOT NULL THEN 10 WHEN omk_hier.[nummer9] IS NOT NULL THEN 9
                    WHEN omk_hier.[nummer8] IS NOT NULL THEN 8 WHEN omk_hier.[nummer7] IS NOT NULL THEN 7
                    WHEN omk_hier.[nummer6] IS NOT NULL THEN 6 WHEN omk_hier.[nummer5] IS NOT NULL THEN 5
                    WHEN omk_hier.[nummer4] IS NOT NULL THEN 4 WHEN omk_hier.[nummer3] IS NOT NULL THEN 3
                    WHEN omk_hier.[nummer2] IS NOT NULL THEN 2 WHEN omk_hier.[nummer1] IS NOT NULL THEN 1
                    ELSE NULL
                END AS omkostningssted_niveau,

                konto.[dw_timestamp_update]

            FROM konto_dedup konto

            LEFT JOIN omkost_link_dedup omkost_link
                ON omkost_link.[FinanskontoId] = konto.[FinanskontoId]
                AND omkost_link._rn = 1

            LEFT JOIN omk_hier_dedup omk_hier
                ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
                   CAST(omkost_link.[Omkostningsstedid] AS BIGINT)
                AND omk_hier._rn = 1

            LEFT JOIN hk_dedup hk
                ON hk.[ref_RECID] = konto.[hk_RECID] AND hk._rn = 1
            LEFT JOIN grp_dedup grp
                ON grp.[RECID] = konto.[grp_RECID] AND grp._rn = 1
            LEFT JOIN grp_aut_dedup grp_aut
                ON grp_aut.[ref_RECID] = konto.[grp_RECID] AND grp_aut._rn = 1
            LEFT JOIN art_dedup art
                ON art.[RECID] = konto.[art_RECID] AND art._rn = 1
            LEFT JOIN funk_dedup funk_f1
                ON funk_f1.[F1] = hk.[F1] AND funk_f1.[F2] IS NULL AND funk_f1.[F3] IS NULL AND funk_f1._rn = 1
            LEFT JOIN funk_dedup funk_f2
                ON funk_f2.[F1] = hk.[F1] AND funk_f2.[F2] = hk.[F2] AND funk_f2.[F3] IS NULL AND funk_f2._rn = 1
            LEFT JOIN funk_dedup funk_f3
                ON funk_f3.[F1] = hk.[F1] AND funk_f3.[F2] = hk.[F2] AND funk_f3.[F3] = hk.[F3] AND funk_f3._rn = 1
            LEFT JOIN dr_dedup dr
                ON dr.[ref_RECID] = TRY_CAST(konto.[d1_RECID] AS BIGINT) AND dr._rn = 1

            WHERE konto._rn = 1
        ) enriched
    ) ranked
    WHERE rn = 1;
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Kontoplan_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        "arc_sydax_finanskonto",
        "arc_sydax_finanskonto_omkostningssted",
        "arc_sydax_omkostningssted_hierarki",
        "arc_sydax_hovedkonto",
        "arc_sydax_gruppering",
        "arc_sydax_gruppering_autoriseret",
        "arc_sydax_art",
        "arc_sydax_funktion",
        "arc_sydax_dranst",
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "sydax",
        "entity": "kontoplan",
    },
    description="Aggregate SydAX kontoplan at clean_code_Kontoplan level with full hierarchy attributes from arc.sydax_* konto archives",
)
def stg_dim_Kontoplan_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX kontoplan (chart of accounts) data with hierarchy enrichment.

    Reconstructs the account hierarchy directly from arc.sydax_finanskonto and
    its konto-keyed lookup archives (hovedkonto, gruppering, art, funktion,
    dranst, finanskonto_omkostningssted -> omkostningssted_hierarki). The
    hierarchy CTEs are the same proven current-version collapses used in
    pre_stg_Postering_sydax / pre_stg_Finanskladdelinje_sydax.

    Deduplicates at clean_code_Kontoplan level.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_Kontoplan_sydax from arc layer...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Kontoplan_sydax];")

            cursor.execute(STG_SQL)

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_Kontoplan_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_kontoplan_sydax_CleanCode
                ON [stg].[dim_Kontoplan_sydax](clean_code_Kontoplan);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(kontonummer) AS has_kontonummer,
                    COUNT(s1) AS has_s1,
                    COUNT(f1) AS has_f1,
                    COUNT(omkostningsstednummer) AS has_omkost
                FROM [stg].[dim_Kontoplan_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with kontonummer, "
                f"{stats[2]}/{stats[0]} with s1, {stats[3]}/{stats[0]} with f1, "
                f"{stats[4]}/{stats[0]} with omkostningsstednummer"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Kontoplan_sydax",
            "row_count": row_count,
            "source_tables": "arc.sydax_finanskonto + konto-keyed hierarchy archives",
            "deduplication": "ROW_NUMBER() on clean_code_Kontoplan (prefer hierarchy-enriched rows)",
            "has_kontonummer": stats[1],
            "has_s1": stats[2],
            "has_f1": stats[3],
            "has_omkost": stats[4],
            "layer": "dim (Layer 1)",
        }
    )
