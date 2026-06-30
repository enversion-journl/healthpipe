# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Postering

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial postings.

Uses:
- map.Postering: DW_EK_Postering assignment via IDENTITY
- stg.dim_postering_sydax: Deduplicated postering attributes at CleanCode_Postering grain

Business key: bk_ref_LedgertransId
Descriptive attributes: ref_LedgertransId (grain/PK), ref_VendTransId, DokumentId,
  Kreditornummer, Kreditornavn, JuridiskEnhed, Kontonummer, FakturaNr,
  SidstUdlignetBilagsnummer, Bilagsnummer_Trans, Bilagsnummer_Postering,
  Valutakode_Trans, Valutakode_Postering, Transaktionstekst, Posteringstekst

dim.Postering is the EXCLUSIVE home of these descriptors — fct.Postering keeps
only keys, date keys, measures, DW_haendelsestidspunkt and FakturaNr
(degenerate dim for the Faktura_To_Postering bridge).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_postering"]),
        AssetKey(["erp", "stg_dim_postering_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="Sync dim.Postering from map + add contextual attributes from SydAX staging",
)
def dim_postering(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Postering dimension sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Postering]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Postering entries to sync")

            cursor.execute(
                """
            SELECT COUNT(*)
            FROM [stg].[dim_postering_sydax] ps
            LEFT JOIN [map].[Postering] m
                ON m.CleanCode_Postering = ps.CleanCode_Postering
            WHERE m.DW_EK_Postering IS NULL
        """
            )
            unmapped_rows = cursor.fetchone()[0]
            if unmapped_rows > 0:
                context.log.warning(
                    f"Found {unmapped_rows} stg.dim_postering_sydax rows without map match"
                )

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Postering_Attributes;

            SELECT
                m.DW_EK_Postering,
                ps.ref_LedgertransId,
                ps.bk_ref_LedgertransId,
                ps.ref_VendTransId,
                ps.DokumentId,
                ps.Kreditornummer,
                ps.Kreditornavn,
                ps.JuridiskEnhed,
                ps.Kontonummer,
                ps.FakturaNr,
                ps.SidstUdlignetBilagsnummer,
                ps.Bilagsnummer_Trans,
                ps.Bilagsnummer_Postering,
                ps.Valutakode_Trans,
                ps.Valutakode_Postering,
                ps.Transaktionstekst,
                ps.Posteringstekst
            INTO #Postering_Attributes
            FROM [stg].[dim_postering_sydax] ps
            LEFT JOIN [map].[Postering] m
                ON m.CleanCode_Postering = ps.CleanCode_Postering
            WHERE m.DW_EK_Postering IS NOT NULL;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Postering_Attributes
            ON #Postering_Attributes (DW_EK_Postering)
            """
            )
            context.log.info("Temp table #Postering_Attributes created")

            cursor.execute(
                """
            MERGE [dim].[Postering] AS tgt
            USING #Postering_Attributes AS src
            ON tgt.DW_EK_Postering = src.DW_EK_Postering

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.ref_LedgertransId = src.ref_LedgertransId,
                    tgt.bk_ref_LedgertransId = src.bk_ref_LedgertransId,
                    tgt.ref_VendTransId = src.ref_VendTransId,
                    tgt.DokumentId = src.DokumentId,
                    tgt.Kreditornummer = src.Kreditornummer,
                    tgt.Kreditornavn = src.Kreditornavn,
                    tgt.JuridiskEnhed = src.JuridiskEnhed,
                    tgt.Kontonummer = src.Kontonummer,
                    tgt.FakturaNr = src.FakturaNr,
                    tgt.SidstUdlignetBilagsnummer = src.SidstUdlignetBilagsnummer,
                    tgt.Bilagsnummer_Trans = src.Bilagsnummer_Trans,
                    tgt.Bilagsnummer_Postering = src.Bilagsnummer_Postering,
                    tgt.Valutakode_Trans = src.Valutakode_Trans,
                    tgt.Valutakode_Postering = src.Valutakode_Postering,
                    tgt.Transaktionstekst = src.Transaktionstekst,
                    tgt.Posteringstekst = src.Posteringstekst,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Postering,
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
                    Posteringstekst,
                    DW_Timestamp_Insert,
                    DW_Timestamp_Update
                )
                VALUES (
                    src.DW_EK_Postering,
                    src.ref_LedgertransId,
                    src.bk_ref_LedgertransId,
                    src.ref_VendTransId,
                    src.DokumentId,
                    src.Kreditornummer,
                    src.Kreditornavn,
                    src.JuridiskEnhed,
                    src.Kontonummer,
                    src.FakturaNr,
                    src.SidstUdlignetBilagsnummer,
                    src.Bilagsnummer_Trans,
                    src.Bilagsnummer_Postering,
                    src.Valutakode_Trans,
                    src.Valutakode_Postering,
                    src.Transaktionstekst,
                    src.Posteringstekst,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Postering_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Postering]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                SUM(CASE WHEN DW_Timestamp_Update = DW_Timestamp_Insert THEN 1 ELSE 0 END) AS inserts_only,
                SUM(CASE WHEN DW_Timestamp_Update > DW_Timestamp_Insert THEN 1 ELSE 0 END) AS updated,
                COUNT(Kreditornavn) AS has_kreditornavn,
                COUNT(FakturaNr) AS has_faktura_nr
            FROM [dim].[Postering]
            WHERE DW_EK_Postering <> -1
        """
            )
            stats = cursor.fetchone()
            total_dim_rows = stats[0]
            inserts_only = stats[1]
            updated_count = stats[2]
            has_kreditornavn = stats[3]
            has_faktura_nr = stats[4]

            context.log.info(
                f"Dimension sync complete: {total_dim_rows} total rows "
                f"({inserts_only} insert-only, {updated_count} updated)"
            )
            context.log.info(
                f"Coverage: {has_kreditornavn}/{total_dim_rows} with Kreditornavn, "
                f"{has_faktura_nr}/{total_dim_rows} with FakturaNr"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "inserts_only": inserts_only,
            "updated": updated_count,
            "has_kreditornavn": has_kreditornavn,
            "has_faktura_nr": has_faktura_nr,
            "unmapped_stg_rows": unmapped_rows,
            "table": "dim.Postering",
            "source_tables": "stg.dim_postering_sydax + map.Postering",
            "scd_type": "Type 1 (overwrite)",
        }
    )
