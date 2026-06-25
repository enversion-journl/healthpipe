# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering from SydAX (Fact Layer)

Select columns needed for fct_postering from pre_stg_postering_sydax.
Intermediate staging table for the fact table load.

Grain contract: one row per ref_LedgertransId (deterministic dedup applied here).
Business key: bk_ref_LedgertransId
All type conversions (date keys, measures, text casts) are performed in this layer
so the downstream fact asset only needs map lookups + MERGE.

Includes Omkostningsstednummer for direct link to OKOSTED organization.

Descriptive attributes (Bilagsnummer_*, Valutakode_*, Transaktionstekst,
Posteringstekst, SidstUdlignetBilagsnummer, DokumentId) live exclusively on
dim.Postering and flow through stg_dim_postering_sydax — NOT through this
fact staging path. FakturaNr stays here (degenerate dim, used by the
ref.Faktura_To_Postering bridge).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_fct_postering_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "postering",
    },
    description=(
        "Select columns needed for fct_postering from pre_stg_postering_sydax. "
        "Grain: ref_LedgertransId."
    ),
)
def stg_fct_postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX postering fact table.

    Enforces grain (one row per ref_LedgertransId) via deterministic dedup and
    performs all type conversions so the fact layer stays focused on map lookups.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_postering_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_postering_sydax];")

            cursor.execute(
                """
                ;WITH grain_dedup AS (
                    SELECT
                        p.ref_LedgertransId,
                        p.bk_ref_LedgertransId,
                        p.CleanCode_Postering,
                        p.CleanCode_Kreditor,
                        p.CleanCode_Kontoplan,
                        p.CleanCode_Faktura,
                        p.bk_FinanskontoId,
                        p.Omkostningsstednummer,
                        p.GodkendtDato,
                        p.LukketDato,
                        p.Trans_Oprettelsestidspunkt,
                        p.Forfaldsdato,
                        p.Trans_Fakturadato,
                        p.SidstUdlignetDato,
                        p.Transaktionsdato,
                        p.Bogfoeringsdato,
                        p.Post_Oprettelsestidspunkt,
                        p.TransaktionsbeloebValuta,
                        p.Transaktionsbeloeb,
                        p.UdlignetBeloebValuta,
                        p.UdlignetBeloeb,
                        p.Posteringsbeloeb,
                        p.FakturaNr,
                        p.DW_LoadTimestamp,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.ref_LedgertransId
                            ORDER BY p.DW_LoadTimestamp DESC
                        ) AS grain_rn
                    FROM [stg].[pre_postering_sydax] p
                    WHERE p.ref_LedgertransId IS NOT NULL
                )

                SELECT
                    g.ref_LedgertransId,
                    g.bk_ref_LedgertransId,
                    g.CleanCode_Postering,

                    g.CleanCode_Kreditor,
                    g.CleanCode_Kontoplan,
                    g.CleanCode_Faktura AS bk_Faktura,
                    g.CleanCode_Kreditor AS bk_Kreditor,
                    g.bk_FinanskontoId,
                    g.Omkostningsstednummer,

                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.GodkendtDato)), -1) AS DW_EK_Dato_Godkendelse,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.LukketDato)), -1) AS DW_EK_Dato_Lukket,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Trans_Oprettelsestidspunkt)), -1) AS DW_EK_Dato_TransOprettet,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Forfaldsdato)), -1) AS DW_EK_Dato_Forfald,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Trans_Fakturadato)), -1) AS DW_EK_Dato_JournalFaktura,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.SidstUdlignetDato)), -1) AS DW_EK_Dato_SidstUdlignet,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Transaktionsdato)), -1) AS DW_EK_Dato_Transaktion,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Bogfoeringsdato)), -1) AS DW_EK_Dato_Bogfoering,
                    COALESCE(dbo.fn_DateTime_To_DateKey(TRY_CONVERT(DATETIME2, g.Post_Oprettelsestidspunkt)), -1) AS DW_EK_Dato_PostOprettet,

                    TRY_CONVERT(DECIMAL(30,12), g.TransaktionsbeloebValuta) AS TransaktionsbeloebValuta,
                    TRY_CONVERT(DECIMAL(30,12), g.Transaktionsbeloeb) AS Transaktionsbeloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.UdlignetBeloebValuta) AS UdlignetBeloebValuta,
                    TRY_CONVERT(DECIMAL(30,12), g.UdlignetBeloeb) AS UdlignetBeloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.Posteringsbeloeb) AS Posteringsbeloeb,

                    g.FakturaNr,

                    TRY_CONVERT(DATETIME2, g.Post_Oprettelsestidspunkt) AS DW_haendelsestidspunkt,

                    'FKLINJE_' + LTRIM(RTRIM(CAST(g.ref_LedgertransId AS NVARCHAR(100)))) AS CleanCode_Finanskladdelinje

                INTO [stg].[fct_postering_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_postering_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [stg].[pre_postering_sydax]
                WHERE ref_LedgertransId IS NOT NULL
            """
            )
            pre_stg_count = cursor.fetchone()[0]
            duplicates_removed = pre_stg_count - row_count
            if duplicates_removed > 0:
                context.log.warning(
                    f"Removed {duplicates_removed} duplicate ref_LedgertransId rows "
                    "during grain enforcement (kept latest by DW_LoadTimestamp)"
                )

            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_fct_postering_sydax_PK
                ON [stg].[fct_postering_sydax](ref_LedgertransId);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CVR
                ON [stg].[fct_postering_sydax](CleanCode_Kreditor);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_Kontoplan
                ON [stg].[fct_postering_sydax](bk_FinanskontoId);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CleanCode_Postering
                ON [stg].[fct_postering_sydax](CleanCode_Postering);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CleanCode_Finanskladdelinje
                ON [stg].[fct_postering_sydax](CleanCode_Finanskladdelinje);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kreditor) AS has_cleancode_kreditor,
                    COUNT(CleanCode_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(Omkostningsstednummer) AS has_okosted
                FROM [stg].[fct_postering_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                f"{stats[2]}/{stats[0]} with CleanCode_Kontoplan, "
                f"{stats[3]}/{stats[0]} with Omkostningsstednummer (OKOSTED)"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN DW_EK_Dato_Bogfoering <> -1 THEN 1 ELSE 0 END) AS has_bogfoering,
                    SUM(CASE WHEN DW_EK_Dato_PostOprettet <> -1 THEN 1 ELSE 0 END) AS has_post_oprettet,
                    SUM(CASE WHEN DW_EK_Dato_Godkendelse <> -1 THEN 1 ELSE 0 END) AS has_godkendt,
                    SUM(CASE WHEN DW_EK_Dato_Transaktion <> -1 THEN 1 ELSE 0 END) AS has_transaktion,
                    SUM(CASE WHEN DW_EK_Dato_Forfald <> -1 THEN 1 ELSE 0 END) AS has_forfald,
                    SUM(CASE WHEN DW_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_postering_sydax]
                """
            )
            date_stats = cursor.fetchone()
            context.log.info(
                f"Date key coverage (non -1): "
                f"Bogfoering={date_stats[1]}/{date_stats[0]}, "
                f"PostOprettet={date_stats[2]}/{date_stats[0]}, "
                f"Godkendt={date_stats[3]}/{date_stats[0]}, "
                f"Transaktion={date_stats[4]}/{date_stats[0]}, "
                f"Forfald={date_stats[5]}/{date_stats[0]}, "
                f"Haendelse={date_stats[6]}/{date_stats[0]}"
            )
            if date_stats[1] == 0 and date_stats[2] == 0:
                context.log.error(
                    "ALL date keys are -1 including Bogfoeringsdato and "
                    "Post_Oprettelsestidspunkt (from posting table itself). "
                    "Check if dbo.fn_DateTime_To_DateKey exists and if date columns "
                    "have correct types in pre_stg."
                )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_postering_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_postering_sydax",
            "grain": "ref_LedgertransId",
            "duplicates_removed": duplicates_removed,
            "has_cvr_clean": stats[1],
            "has_kontoplan_clean": stats[2],
            "has_okosted": stats[3],
            "date_columns": 9,
            "measure_columns": 5,
            "layer": "stg_fct (Layer 1)",
        }
    )
