# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Base Layer)

Transforms ext.finanskladdelinje_enriched (raw joined columns from DSI) into
stg.pre_finanskladdelinje_sydax by applying:
- CleanCode generation (CleanCode_Finanskladdelinje, CleanCode_Finanskladde, CleanCode_Kontoplan)
- Type casts (BilagsnummerID/DokumentId → BIGINT, Debet/Kredit → DECIMAL,
  hierarchy columns → NVARCHAR(50/255))
- COALESCE for hierarchy tekst columns (tekst_G1 from GrupperingAutoriseret/Gruppering)
- Omkostningsstednummer / Omkostningssted_niveau resolution
- Business key aliases (bk_ref_LedgertransId, bk_ref_LedgerjournaltableId, bk_FinanskontoId)

All source-side joins (Finanskonto hierarchy, Hovedkonto, Gruppering, Art,
Funktion, Dranst, OmkostningsstedHierarki, etc.) are performed in the DSI
enriched extraction (sydax_enriched_finanskladde.py) which outputs raw values.

Downstream consumers:
- stg_dim_finanskladdelinje_sydax -> dim.Finanskladdelinje
- stg_fct_finanskladdelinje_sydax -> fct.Finanskladdelinje

Grain: ref_LedgertransId (unique BIGINT from SydAX ledger)
Business key: bk_ref_LedgertransId
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "pre_stg_finanskladdelinje_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["ext_finanskladdelinje_enriched_sydax_daily"]),
        AssetKey(["ext_finanskladdelinje_enriched_sydax_monthly"]),
        AssetKey(["ext_finanskladdelinje_enriched_sydax_full_load"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "finanskladdelinje",
    },
    description=(
        "Transform ext.finanskladdelinje_enriched (raw joined columns) into "
        "stg.pre_finanskladdelinje_sydax: CleanCode generation, type casts, "
        "COALESCE hierarchy texts, Omkostningsstednummer resolution. "
        "Feeds dim.Finanskladdelinje and fct.Finanskladdelinje "
        "(via stg_dim_/stg_fct_finanskladdelinje_sydax)."
    ),
)
def pre_stg_finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Transform ext.finanskladdelinje_enriched into stg.pre_finanskladdelinje_sydax."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Creating stg.pre_finanskladdelinje_sydax from ext.finanskladdelinje_enriched..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[pre_finanskladdelinje_sydax];")

            cursor.execute(
                """
                SELECT
                    -- Line grain and business key
                    e.[ref_LedgertransId],
                    e.[ref_LedgertransId] AS bk_ref_LedgertransId,
                    e.[ref_RecId],

                    -- CleanCode for the line (fact + dim grain)
                    'FKLINJE_' + LTRIM(RTRIM(
                        CAST(e.[ref_LedgertransId] AS NVARCHAR(100))
                    )) AS CleanCode_Finanskladdelinje,

                    -- Header columns
                    e.[ref_LedgerjournaltableId],
                    e.[ref_LedgerjournaltableId] AS bk_ref_LedgerjournaltableId,
                    'FINANSKLADDE_' + LTRIM(RTRIM(
                        CAST(e.[ref_LedgerjournaltableId] AS NVARCHAR(100))
                    )) AS CleanCode_Finanskladde,
                    e.[Kladdenummer],
                    e.[Kladdetekst],
                    e.[Kladdenavn],
                    e.[ErLukket],
                    e.[DataAreaId],
                    e.[Maskinnummer_Kladde],

                    -- Transaction type (raw from enriched join)
                    e.[Transaktionstypenummer],
                    e.[Transaktionstypetekst],

                    -- Line descriptive columns
                    e.[Linjenummer],
                    e.[dato],
                    e.[Beskrivelse],
                    e.[Ekspeditionsnummer],
                    e.[Maskinnummer],
                    CAST(e.[BilagsnummerID] AS BIGINT) AS BilagsnummerID,
                    CAST(e.[DokumentId] AS BIGINT) AS DokumentId,

                    -- Measures
                    TRY_CONVERT(DECIMAL(30,12), e.[Debet]) AS Debet,
                    TRY_CONVERT(DECIMAL(30,12), e.[Kredit]) AS Kredit,

                    -- Account chain
                    'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                        CAST(e.[FinanskontoId] AS NVARCHAR(100)), ' ', '')
                    ))) AS CleanCode_Kontoplan,
                    CAST(e.[FinanskontoId] AS BIGINT) AS FinanskontoId,
                    e.[FinanskontoId] AS bk_FinanskontoId,
                    e.[Kontonummer],
                    e.[hk_RECID],
                    e.[grp_RECID],
                    e.[art_RECID],
                    e.[d1_RECID],

                    -- Hierarchy nummer columns (cast to NVARCHAR(50))
                    CAST(e.[F1] AS NVARCHAR(50)) AS F1,
                    CAST(e.[F2] AS NVARCHAR(50)) AS F2,
                    CAST(e.[F3] AS NVARCHAR(50)) AS F3,
                    CAST(e.[D1] AS NVARCHAR(50)) AS D1,
                    CAST(e.[G1] AS NVARCHAR(50)) AS G1,

                    CAST(e.[nummer1] AS NVARCHAR(50)) AS S1,
                    CAST(e.[nummer2] AS NVARCHAR(50)) AS S2,
                    CAST(e.[nummer3] AS NVARCHAR(50)) AS S3,
                    CAST(e.[nummer4] AS NVARCHAR(50)) AS S4,
                    CAST(e.[nummer5] AS NVARCHAR(50)) AS S5,
                    CAST(e.[nummer6] AS NVARCHAR(50)) AS S6,
                    CAST(e.[nummer7] AS NVARCHAR(50)) AS S7,
                    CAST(e.[nummer8] AS NVARCHAR(50)) AS S8,
                    CAST(e.[nummer9] AS NVARCHAR(50)) AS S9,
                    CAST(e.[nummer10] AS NVARCHAR(50)) AS S10,
                    CAST(e.[nummer11] AS NVARCHAR(50)) AS S11,
                    CAST(e.[nummer12] AS NVARCHAR(50)) AS S12,
                    CAST(e.[nummer13] AS NVARCHAR(50)) AS S13,
                    CAST(e.[nummer14] AS NVARCHAR(50)) AS S14,
                    CAST(e.[nummer15] AS NVARCHAR(50)) AS S15,
                    CAST(e.[nummer16] AS NVARCHAR(50)) AS S16,
                    CAST(e.[nummer17] AS NVARCHAR(50)) AS S17,
                    CAST(e.[nummer18] AS NVARCHAR(50)) AS S18,
                    CAST(e.[nummer19] AS NVARCHAR(50)) AS S19,
                    CAST(e.[nummer20] AS NVARCHAR(50)) AS S20,

                    CAST(e.[Grupperingsnummer] AS NVARCHAR(50)) AS G2,
                    CAST(e.[A1] AS NVARCHAR(50)) AS A1,
                    CAST(e.[A2] AS NVARCHAR(50)) AS A2,

                    -- Hierarchy tekst columns (cast to NVARCHAR(255))
                    CAST(e.[tekst1] AS NVARCHAR(255)) AS tekst_S1,
                    CAST(e.[tekst2] AS NVARCHAR(255)) AS tekst_S2,
                    CAST(e.[tekst3] AS NVARCHAR(255)) AS tekst_S3,
                    CAST(e.[tekst4] AS NVARCHAR(255)) AS tekst_S4,
                    CAST(e.[tekst5] AS NVARCHAR(255)) AS tekst_S5,
                    CAST(e.[tekst6] AS NVARCHAR(255)) AS tekst_S6,
                    CAST(e.[tekst7] AS NVARCHAR(255)) AS tekst_S7,
                    CAST(e.[tekst8] AS NVARCHAR(255)) AS tekst_S8,
                    CAST(e.[tekst9] AS NVARCHAR(255)) AS tekst_S9,
                    CAST(e.[tekst10] AS NVARCHAR(255)) AS tekst_S10,
                    CAST(e.[tekst11] AS NVARCHAR(255)) AS tekst_S11,
                    CAST(e.[tekst12] AS NVARCHAR(255)) AS tekst_S12,
                    CAST(e.[tekst13] AS NVARCHAR(255)) AS tekst_S13,
                    CAST(e.[tekst14] AS NVARCHAR(255)) AS tekst_S14,
                    CAST(e.[tekst15] AS NVARCHAR(255)) AS tekst_S15,
                    CAST(e.[tekst16] AS NVARCHAR(255)) AS tekst_S16,
                    CAST(e.[tekst17] AS NVARCHAR(255)) AS tekst_S17,
                    CAST(e.[tekst18] AS NVARCHAR(255)) AS tekst_S18,
                    CAST(e.[tekst19] AS NVARCHAR(255)) AS tekst_S19,
                    CAST(e.[tekst20] AS NVARCHAR(255)) AS tekst_S20,

                    COALESCE(
                        CAST(e.[G1Tekst] AS NVARCHAR(255)),
                        CAST(e.[Grupperingstekst] AS NVARCHAR(255))
                    ) AS tekst_G1,
                    CAST(e.[Grupperingstekst] AS NVARCHAR(255)) AS tekst_G2,
                    CAST(e.[Artstekst] AS NVARCHAR(255)) AS tekst_A1,
                    CAST(e.[Artstekst] AS NVARCHAR(255)) AS tekst_A2,
                    CAST(e.[FunktionTekst_F1] AS NVARCHAR(255)) AS tekst_F1,
                    CAST(e.[FunktionTekst_F2] AS NVARCHAR(255)) AS tekst_F2,
                    CAST(e.[FunktionTekst_F3] AS NVARCHAR(255)) AS tekst_F3,
                    CAST(e.[D1Tekst] AS NVARCHAR(255)) AS tekst_D1,

                    -- Omkostningssted resolved
                    CAST(e.[Omkostningsstedid] AS BIGINT) AS Omkostningsstedid,
                    COALESCE(
                        CAST(e.[nummer20] AS NVARCHAR(50)),
                        CAST(e.[nummer19] AS NVARCHAR(50)),
                        CAST(e.[nummer18] AS NVARCHAR(50)),
                        CAST(e.[nummer17] AS NVARCHAR(50)),
                        CAST(e.[nummer16] AS NVARCHAR(50)),
                        CAST(e.[nummer15] AS NVARCHAR(50)),
                        CAST(e.[nummer14] AS NVARCHAR(50)),
                        CAST(e.[nummer13] AS NVARCHAR(50)),
                        CAST(e.[nummer12] AS NVARCHAR(50)),
                        CAST(e.[nummer11] AS NVARCHAR(50)),
                        CAST(e.[nummer10] AS NVARCHAR(50)),
                        CAST(e.[nummer9] AS NVARCHAR(50)),
                        CAST(e.[nummer8] AS NVARCHAR(50)),
                        CAST(e.[nummer7] AS NVARCHAR(50)),
                        CAST(e.[nummer6] AS NVARCHAR(50)),
                        CAST(e.[nummer5] AS NVARCHAR(50)),
                        CAST(e.[nummer4] AS NVARCHAR(50)),
                        CAST(e.[nummer3] AS NVARCHAR(50)),
                        CAST(e.[nummer2] AS NVARCHAR(50)),
                        CAST(e.[nummer1] AS NVARCHAR(50))
                    ) AS Omkostningsstednummer,
                    CASE
                        WHEN e.[nummer20] IS NOT NULL THEN 20
                        WHEN e.[nummer19] IS NOT NULL THEN 19
                        WHEN e.[nummer18] IS NOT NULL THEN 18
                        WHEN e.[nummer17] IS NOT NULL THEN 17
                        WHEN e.[nummer16] IS NOT NULL THEN 16
                        WHEN e.[nummer15] IS NOT NULL THEN 15
                        WHEN e.[nummer14] IS NOT NULL THEN 14
                        WHEN e.[nummer13] IS NOT NULL THEN 13
                        WHEN e.[nummer12] IS NOT NULL THEN 12
                        WHEN e.[nummer11] IS NOT NULL THEN 11
                        WHEN e.[nummer10] IS NOT NULL THEN 10
                        WHEN e.[nummer9] IS NOT NULL THEN 9
                        WHEN e.[nummer8] IS NOT NULL THEN 8
                        WHEN e.[nummer7] IS NOT NULL THEN 7
                        WHEN e.[nummer6] IS NOT NULL THEN 6
                        WHEN e.[nummer5] IS NOT NULL THEN 5
                        WHEN e.[nummer4] IS NOT NULL THEN 4
                        WHEN e.[nummer3] IS NOT NULL THEN 3
                        WHEN e.[nummer2] IS NOT NULL THEN 2
                        WHEN e.[nummer1] IS NOT NULL THEN 1
                        ELSE NULL
                    END AS Omkostningssted_niveau,

                    -- Dates
                    TRY_CONVERT(DATE, e.[dato]) AS dato_converted,
                    TRY_CONVERT(DATETIME2, e.[Linje_CreatedOn]) AS Linje_CreatedOn,
                    TRY_CONVERT(DATETIME2, e.[Kladde_CreatedOn]) AS Kladde_CreatedOn,
                    TRY_CONVERT(DATETIME2, e.[Kladde_Modified]) AS Kladde_Modified,

                    -- Audit
                    e.[DW_LoadTimestamp]

                INTO [stg].[pre_finanskladdelinje_sydax]
                FROM [ext].[finanskladdelinje_enriched] e;
                """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.pre_finanskladdelinje_sydax with {row_count} rows")

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_pre_finanskladdelinje_sydax_PK
                ON [stg].[pre_finanskladdelinje_sydax](ref_LedgertransId);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Linje
                ON [stg].[pre_finanskladdelinje_sydax](CleanCode_Finanskladdelinje);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Kladde
                ON [stg].[pre_finanskladdelinje_sydax](CleanCode_Finanskladde);
                """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Kontoplan
                ON [stg].[pre_finanskladdelinje_sydax](CleanCode_Kontoplan);
                """
            )

            context.log.info("Created indexes for pre_finanskladdelinje_sydax")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Finanskladdelinje) AS has_cc_linje,
                    COUNT(CleanCode_Finanskladde) AS has_cc_kladde,
                    COUNT(CleanCode_Kontoplan) AS has_cc_kontoplan,
                    COUNT(DISTINCT CleanCode_Finanskladde) AS unique_cc_kladde,
                    COUNT(DISTINCT CleanCode_Kontoplan) AS unique_cc_kontoplan,
                    COUNT(ref_LedgerjournaltableId) AS has_header_ref,
                    COUNT(Kontonummer) AS has_kontonummer,
                    COUNT(Omkostningsstednummer) AS has_omkostningssted,
                    COUNT(Transaktionstypenummer) AS has_transtype
                FROM [stg].[pre_finanskladdelinje_sydax]
                """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Finanskladdelinje, "
                f"{stats[2]}/{stats[0]} with CleanCode_Finanskladde, "
                f"{stats[3]}/{stats[0]} with CleanCode_Kontoplan. "
                f"Unique CC_Kladde: {stats[4]}, unique CC_Kontoplan: {stats[5]}"
            )
            context.log.info(
                f"Enrichment coverage: "
                f"header_ref={stats[6]}/{stats[0]}, "
                f"Kontonummer={stats[7]}/{stats[0]}, "
                f"Omkostningssted={stats[8]}/{stats[0]}, "
                f"Transaktionstype={stats[9]}/{stats[0]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.pre_finanskladdelinje_sydax",
            "target_table": "stg.pre_finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "ext.finanskladdelinje_enriched",
            "grain": "ref_LedgertransId",
            "unique_cc_kladde": stats[4],
            "unique_cc_kontoplan": stats[5],
            "has_header_ref": stats[6],
            "has_kontonummer": stats[7],
            "layer": "stg_fct (Layer 0)",
        }
    )
