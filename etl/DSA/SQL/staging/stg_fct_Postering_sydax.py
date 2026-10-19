# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering from SydAX (Fact Layer)

Select columns needed for fct_Postering from pre_stg_Postering_sydax.
Intermediate staging table for the fact table load.

Grain contract: one row per ref_ledgertrans_id (deterministic dedup applied here).
Business key: bk_ref_ledgertrans_id
All type conversions (date keys, measures, text casts) are performed in this layer
so the downstream fact asset only needs map lookups + MERGE.

Includes omkostningsstednummer for direct link to OKOSTED organization.

Descriptive attributes (Bilagsnummer_*, Valutakode_*, transaktionstekst,
posteringstekst, sidst_udlignet_bilagsnummer, dokument_id) live exclusively on
dim.Postering and flow through stg_dim_Postering_sydax — NOT through this
fact staging path. faktura_nr stays here (degenerate dim, used by the
ref.Faktura_To_Postering bridge).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key, dato_tid


@asset(
    key=AssetKey(["erp", "stg_fct_Postering_sydax"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "postering",
    },
    description=(
        "Select columns needed for fct_Postering from pre_stg_Postering_sydax. "
        "Grain: ref_ledgertrans_id."
    ),
)
def stg_fct_Postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for SydAX postering fact table.

    Enforces grain (one row per ref_ledgertrans_id) via deterministic dedup and
    performs all type conversions so the fact layer stays focused on map lookups.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.fct_Postering_sydax...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[fct_Postering_sydax];")

            cursor.execute(
                f"""
                ;WITH grain_dedup AS (
                    SELECT
                        p.ref_ledgertrans_id,
                        p.bk_ref_ledgertrans_id,
                        p.clean_code_Postering,
                        p.clean_code_Kreditor,
                        p.clean_code_Kontoplan,
                        p.clean_code_Faktura,
                        p.bk_finanskonto_id,
                        p.omkostningsstednummer,
                        p.GodkendtDato,
                        p.LukketDato,
                        p.Trans_Oprettelsestidspunkt,
                        p.Forfaldsdato,
                        p.Trans_Fakturadato,
                        p.SidstUdlignetDato,
                        p.Transaktionsdato,
                        p.Bogfoeringsdato,
                        p.Post_Oprettelsestidspunkt,
                        p.transaktionsbeloeb_valuta,
                        p.transaktionsbeloeb,
                        p.udlignet_beloeb_valuta,
                        p.udlignet_beloeb,
                        p.posteringsbeloeb,
                        p.faktura_nr,
                        p.dw_timestamp_update,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.ref_ledgertrans_id
                            ORDER BY p.dw_timestamp_update DESC
                        ) AS grain_rn
                    FROM [stg].[pre_Postering_sydax] p
                    WHERE p.ref_ledgertrans_id IS NOT NULL
                )

                SELECT
                    g.ref_ledgertrans_id,
                    g.bk_ref_ledgertrans_id,
                    g.clean_code_Postering,

                    g.clean_code_Kreditor,
                    g.clean_code_Kontoplan,
                    g.clean_code_Faktura AS bk_faktura,
                    g.clean_code_Kreditor AS bk_kreditor,
                    g.bk_finanskonto_id,
                    g.omkostningsstednummer,

                    {dato_key('g.GodkendtDato')} AS dw_ek_Dato_godkendelse,
                    {dato_tid('g.GodkendtDato')} AS dato_tid_godkendelse,
                    {dato_key('g.LukketDato')} AS dw_ek_Dato_lukket,
                    {dato_tid('g.LukketDato')} AS dato_tid_lukket,
                    {dato_key('g.Trans_Oprettelsestidspunkt')} AS dw_ek_Dato_trans_oprettet,
                    {dato_tid('g.Trans_Oprettelsestidspunkt')} AS dato_tid_trans_oprettet,
                    {dato_key('g.Forfaldsdato')} AS dw_ek_Dato_forfald,
                    {dato_tid('g.Forfaldsdato')} AS dato_tid_forfald,
                    {dato_key('g.Trans_Fakturadato')} AS dw_ek_Dato_journal_faktura,
                    {dato_tid('g.Trans_Fakturadato')} AS dato_tid_journal_faktura,
                    {dato_key('g.SidstUdlignetDato')} AS dw_ek_Dato_sidst_udlignet,
                    {dato_tid('g.SidstUdlignetDato')} AS dato_tid_sidst_udlignet,
                    {dato_key('g.Transaktionsdato')} AS dw_ek_Dato_transaktion,
                    {dato_tid('g.Transaktionsdato')} AS dato_tid_transaktion,
                    {dato_key('g.Bogfoeringsdato')} AS dw_ek_Dato_bogfoering,
                    {dato_tid('g.Bogfoeringsdato')} AS dato_tid_bogfoering,
                    {dato_key('g.Post_Oprettelsestidspunkt')} AS dw_ek_Dato_post_oprettet,
                    {dato_tid('g.Post_Oprettelsestidspunkt')} AS dato_tid_post_oprettet,

                    TRY_CONVERT(DECIMAL(30,12), g.transaktionsbeloeb_valuta) AS transaktionsbeloeb_valuta,
                    TRY_CONVERT(DECIMAL(30,12), g.transaktionsbeloeb) AS transaktionsbeloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.udlignet_beloeb_valuta) AS udlignet_beloeb_valuta,
                    TRY_CONVERT(DECIMAL(30,12), g.udlignet_beloeb) AS udlignet_beloeb,
                    TRY_CONVERT(DECIMAL(30,12), g.posteringsbeloeb) AS posteringsbeloeb,

                    g.faktura_nr,

                    TRY_CONVERT(DATETIME2, g.Post_Oprettelsestidspunkt) AS dw_haendelsestidspunkt,

                    'FKLINJE_' + LTRIM(RTRIM(CAST(g.ref_ledgertrans_id AS NVARCHAR(100)))) AS clean_code_Finanskladdelinje

                INTO [stg].[fct_Postering_sydax]
                FROM grain_dedup g
                WHERE g.grain_rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.fct_Postering_sydax with {row_count} rows")

            cursor.execute(
                """
                SELECT COUNT(*) FROM [stg].[pre_Postering_sydax]
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
                CREATE CLUSTERED INDEX IX_fct_postering_sydax_PK
                ON [stg].[fct_Postering_sydax](ref_ledgertrans_id);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CVR
                ON [stg].[fct_Postering_sydax](clean_code_Kreditor);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_Kontoplan
                ON [stg].[fct_Postering_sydax](bk_finanskonto_id);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CleanCode_Postering
                ON [stg].[fct_Postering_sydax](clean_code_Postering);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_fct_postering_sydax_CleanCode_Finanskladdelinje
                ON [stg].[fct_Postering_sydax](clean_code_Finanskladdelinje);
            """
            )

            context.log.info("Created indexes for map lookups")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(clean_code_Kreditor) AS has_cleancode_kreditor,
                    COUNT(clean_code_Kontoplan) AS has_cleancode_kontoplan,
                    COUNT(omkostningsstednummer) AS has_okosted
                FROM [stg].[fct_Postering_sydax]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with clean_code_Kreditor, "
                f"{stats[2]}/{stats[0]} with clean_code_Kontoplan, "
                f"{stats[3]}/{stats[0]} with omkostningsstednummer (OKOSTED)"
            )

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN dw_ek_Dato_bogfoering <> -1 THEN 1 ELSE 0 END) AS has_bogfoering,
                    SUM(CASE WHEN dw_ek_Dato_post_oprettet <> -1 THEN 1 ELSE 0 END) AS has_post_oprettet,
                    SUM(CASE WHEN dw_ek_Dato_godkendelse <> -1 THEN 1 ELSE 0 END) AS has_godkendt,
                    SUM(CASE WHEN dw_ek_Dato_transaktion <> -1 THEN 1 ELSE 0 END) AS has_transaktion,
                    SUM(CASE WHEN dw_ek_Dato_forfald <> -1 THEN 1 ELSE 0 END) AS has_forfald,
                    SUM(CASE WHEN dw_haendelsestidspunkt IS NOT NULL THEN 1 ELSE 0 END) AS has_haendelse
                FROM [stg].[fct_Postering_sydax]
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
                    "Check if date columns have correct types in pre_stg."
                )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.fct_Postering_sydax",
            "row_count": row_count,
            "source_table": "stg.pre_Postering_sydax",
            "grain": "ref_ledgertrans_id",
            "duplicates_removed": duplicates_removed,
            "has_cvr_clean": stats[1],
            "has_kontoplan_clean": stats[2],
            "has_okosted": stats[3],
            "date_columns": 9,
            "measure_columns": 5,
            "layer": "stg_fct (Layer 1)",
        }
    )
