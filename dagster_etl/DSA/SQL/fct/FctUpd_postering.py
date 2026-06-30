# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Assets for ERP: Postering

Fact table for financial postings with DW_EK lookups from maps.
Grain: ref_LedgertransId (unique BIGINT from SydAX ledger)

Reads from stg.fct_postering_sydax (Layer 1 staging) which contains
precomputed date keys, typed measures, and the grain contract
(one row per ref_LedgertransId).

This asset is responsible only for map lookups + MERGE + diagnostics.
Includes DW_EK_Postering lookup via map.Postering (CleanCode_Postering),
DW_EK_Okosted lookup via map.Okosted (CleanCode_Okosted = Omkostningsstednummer),
and DW_EK_Finanskladdelinje lookup via shared ref_LedgertransId.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_facts",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_fct_postering_sydax"]),
        "map_kreditor",
        "map_kontoplan",
        "map_okosted",
        "map_postering",
        "map_finanskladdelinje",
        "dim_kreditor",
        "dim_kontoplan",
        "dim_finanskladdelinje",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "posting",
    },
    description=(
        "Fact table for financial postings - reads precomputed staging columns "
        "and looks up DW_EK_Postering, DW_EK_Kreditor, DW_EK_Kontoplan, DW_EK_Faktura, "
        "DW_EK_Okosted, DW_EK_Finanskladdelinje from maps. Grain: ref_LedgertransId."
    ),
)
def fct_postering(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Map lookups (incl. DW_EK_Postering) + MERGE into fct.Postering. All conversions done in staging."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting fact table load with DW_EK lookups...")

            cursor.execute("SELECT COUNT(*) FROM [stg].[fct_postering_sydax]")
            staging_count = cursor.fetchone()[0]
            context.log.info(f"Staging count: {staging_count}")

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Postering_Facts;

            SELECT
                src.ref_LedgertransId,
                src.CleanCode_Postering,
                COALESCE(post_map.DW_EK_Postering, -1) AS DW_EK_Postering,

                COALESCE(kred_map.DW_EK_Kreditor, -1) AS DW_EK_Kreditor,
                COALESCE(konto_map.DW_EK_Kontoplan, -1) AS DW_EK_Kontoplan,
                CAST(-1 AS INT) AS DW_EK_Faktura,
                COALESCE(oko_map.DW_EK_Okosted, -1) AS DW_EK_Okosted,
                COALESCE(fkl_map.DW_EK_Finanskladdelinje, -1) AS DW_EK_Finanskladdelinje,

                src.DW_EK_Dato_Godkendelse,
                src.DW_EK_Dato_Lukket,
                src.DW_EK_Dato_TransOprettet,
                src.DW_EK_Dato_Forfald,
                src.DW_EK_Dato_JournalFaktura,
                src.DW_EK_Dato_SidstUdlignet,
                src.DW_EK_Dato_Transaktion,
                src.DW_EK_Dato_Bogfoering,
                src.DW_EK_Dato_PostOprettet,
                src.TransaktionsbeloebValuta,
                src.Transaktionsbeloeb,
                src.UdlignetBeloebValuta,
                src.UdlignetBeloeb,
                src.Posteringsbeloeb,
                src.FakturaNr,
                src.DW_haendelsestidspunkt

            INTO #Postering_Facts
            FROM [stg].[fct_postering_sydax] src

            LEFT JOIN [map].[Kreditor] kred_map
                ON kred_map.CleanCode_Kreditor = src.CleanCode_Kreditor
                AND src.CleanCode_Kreditor IS NOT NULL
                AND src.CleanCode_Kreditor <> ''

            LEFT JOIN [map].[Kontoplan] konto_map
                ON konto_map.bk_FinanskontoId = src.bk_FinanskontoId

            LEFT JOIN [map].[Okosted] oko_map
                ON oko_map.CleanCode_Okosted = src.Omkostningsstednummer
               AND src.Omkostningsstednummer IS NOT NULL
               AND src.Omkostningsstednummer <> ''

            LEFT JOIN [map].[Postering] post_map
                ON post_map.CleanCode_Postering = src.CleanCode_Postering
               AND src.CleanCode_Postering IS NOT NULL
               AND src.CleanCode_Postering <> ''

            LEFT JOIN [map].[Finanskladdelinje] fkl_map
                ON fkl_map.CleanCode_Finanskladdelinje = src.CleanCode_Finanskladdelinje
               AND src.CleanCode_Finanskladdelinje IS NOT NULL
               AND src.CleanCode_Finanskladdelinje <> '';
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Postering_Facts
            ON #Postering_Facts (ref_LedgertransId)
            """
            )

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_source_rows,
                COUNT(DISTINCT ref_LedgertransId) AS distinct_ids
            FROM #Postering_Facts
        """
            )
            source_stats = cursor.fetchone()
            duplicate_source_rows = source_stats[0] - source_stats[1]
            if duplicate_source_rows > 0:
                message = (
                    "Duplicate grain violation in fct_postering: "
                    f"{duplicate_source_rows} duplicate source rows found in #Postering_Facts. "
                    "Expected one row per ref_LedgertransId from pre_stg/stg assets."
                )
                context.log.error(message)
                raise ValueError(message)

            cursor.execute(
                """
            MERGE [fct].[Postering] AS tgt
            USING #Postering_Facts AS src
            ON tgt.ref_LedgertransId = src.ref_LedgertransId

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.DW_EK_Postering = src.DW_EK_Postering,
                    tgt.CleanCode_Postering = src.CleanCode_Postering,
                    tgt.DW_EK_Kreditor = src.DW_EK_Kreditor,
                    tgt.DW_EK_Kontoplan = src.DW_EK_Kontoplan,
                    tgt.DW_EK_Okosted = src.DW_EK_Okosted,
                    tgt.DW_EK_Finanskladdelinje = src.DW_EK_Finanskladdelinje,
                    tgt.DW_EK_Dato_Godkendelse = src.DW_EK_Dato_Godkendelse,
                    tgt.DW_EK_Dato_Lukket = src.DW_EK_Dato_Lukket,
                    tgt.DW_EK_Dato_TransOprettet = src.DW_EK_Dato_TransOprettet,
                    tgt.DW_EK_Dato_Forfald = src.DW_EK_Dato_Forfald,
                    tgt.DW_EK_Dato_JournalFaktura = src.DW_EK_Dato_JournalFaktura,
                    tgt.DW_EK_Dato_SidstUdlignet = src.DW_EK_Dato_SidstUdlignet,
                    tgt.DW_EK_Dato_Transaktion = src.DW_EK_Dato_Transaktion,
                    tgt.DW_EK_Dato_Bogfoering = src.DW_EK_Dato_Bogfoering,
                    tgt.DW_EK_Dato_PostOprettet = src.DW_EK_Dato_PostOprettet,
                    tgt.TransaktionsbeloebValuta = src.TransaktionsbeloebValuta,
                    tgt.Transaktionsbeloeb = src.Transaktionsbeloeb,
                    tgt.UdlignetBeloebValuta = src.UdlignetBeloebValuta,
                    tgt.UdlignetBeloeb = src.UdlignetBeloeb,
                    tgt.Posteringsbeloeb = src.Posteringsbeloeb,
                    tgt.FakturaNr = src.FakturaNr,
                    tgt.DW_haendelsestidspunkt = src.DW_haendelsestidspunkt,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    ref_LedgertransId, DW_EK_Postering, CleanCode_Postering,
                    DW_EK_Kreditor, DW_EK_Kontoplan, DW_EK_Faktura, DW_EK_Okosted,
                    DW_EK_Finanskladdelinje,
                    DW_EK_Dato_Godkendelse, DW_EK_Dato_Lukket, DW_EK_Dato_TransOprettet,
                    DW_EK_Dato_Forfald, DW_EK_Dato_JournalFaktura, DW_EK_Dato_SidstUdlignet,
                    DW_EK_Dato_Transaktion, DW_EK_Dato_Bogfoering, DW_EK_Dato_PostOprettet,
                    TransaktionsbeloebValuta, Transaktionsbeloeb, UdlignetBeloebValuta, UdlignetBeloeb,
                    Posteringsbeloeb, FakturaNr, DW_haendelsestidspunkt,
                    DW_Timestamp_Insert, DW_Timestamp_Update
                )
                VALUES (
                    src.ref_LedgertransId, src.DW_EK_Postering, src.CleanCode_Postering,
                    src.DW_EK_Kreditor, src.DW_EK_Kontoplan, src.DW_EK_Faktura, src.DW_EK_Okosted,
                    src.DW_EK_Finanskladdelinje,
                    src.DW_EK_Dato_Godkendelse, src.DW_EK_Dato_Lukket, src.DW_EK_Dato_TransOprettet,
                    src.DW_EK_Dato_Forfald, src.DW_EK_Dato_JournalFaktura, src.DW_EK_Dato_SidstUdlignet,
                    src.DW_EK_Dato_Transaktion, src.DW_EK_Dato_Bogfoering, src.DW_EK_Dato_PostOprettet,
                    src.TransaktionsbeloebValuta, src.Transaktionsbeloeb, src.UdlignetBeloebValuta, src.UdlignetBeloeb,
                    src.Posteringsbeloeb, src.FakturaNr, src.DW_haendelsestidspunkt,
                    CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_facts,
                SUM(CASE WHEN DW_EK_Postering = -1 THEN 1 ELSE 0 END) AS missing_postering_fk,
                SUM(CASE WHEN DW_EK_Kreditor = -1 THEN 1 ELSE 0 END) AS missing_kreditor_fk,
                SUM(CASE WHEN DW_EK_Kontoplan = -1 THEN 1 ELSE 0 END) AS missing_kontoplan_fk,
                SUM(CASE WHEN DW_EK_Faktura = -1 THEN 1 ELSE 0 END) AS missing_faktura_fk,
                SUM(CASE WHEN DW_EK_Okosted = -1 THEN 1 ELSE 0 END) AS missing_okosted_fk,
                SUM(CASE WHEN DW_EK_Finanskladdelinje = -1 THEN 1 ELSE 0 END) AS missing_finanskladdelinje_fk
            FROM [fct].[Postering]
        """
            )
            diag = cursor.fetchone()

            context.log.info(
                f"FK Matching: Total={diag[0]}, Missing Postering={diag[1]}, "
                f"Missing Kreditor={diag[2]}, Missing Kontoplan={diag[3]}, "
                f"Missing Faktura={diag[4]}, Missing Okosted={diag[5]}, "
                f"Missing Finanskladdelinje={diag[6]}"
            )

            cursor.execute("DROP TABLE IF EXISTS #Postering_Facts")
            cursor.execute("UPDATE STATISTICS [fct].[Postering]")

            context.log.info(f"Fact table load complete: {rows_affected} rows processed")
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "table": "fct.Postering",
            "grain": "ref_LedgertransId",
            "staging_source": "stg.fct_postering_sydax (Layer 1)",
            "dw_ek_lookups": "Postering, Kreditor, Kontoplan, Faktura, Okosted, Finanskladdelinje",
            "duplicate_source_rows": duplicate_source_rows,
        }
    )
