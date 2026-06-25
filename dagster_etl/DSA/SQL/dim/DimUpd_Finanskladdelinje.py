# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Finanskladdelinje

Sync dimension from map + add contextual attributes from SydAX staging.
Logic: Type 1 SCD - update textual information for financial journal lines.

Uses:
- map.Finanskladdelinje: DW_EK_Finanskladdelinje assignment via IDENTITY
- stg.dim_finanskladdelinje_sydax: Deduplicated attributes at CleanCode_Finanskladdelinje grain

Business key: bk_ref_LedgertransId
Descriptive attributes: Beskrivelse, Linjenummer, Ekspeditionsnummer,
  Transaktionstypenummer, Transaktionstypetekst, Maskinnummer
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="dim_finanskladdelinje",
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_finanskladdelinje_sydax"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description=(
        "Sync dim.Finanskladdelinje from map + add contextual attributes from SydAX staging"
    ),
)
def dim_finanskladdelinje(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    """Dimension table maintenance: Sync from map + add contextual attributes from SydAX."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Starting Finanskladdelinje dimension sync...")

            cursor.execute("SELECT COUNT(*) FROM [map].[Finanskladdelinje]")
            map_count = cursor.fetchone()[0]
            context.log.info(f"Map has {map_count} Finanskladdelinje entries to sync")

            cursor.execute(
                """
            DROP TABLE IF EXISTS #Finanskladdelinje_Attributes;

            SELECT
                m.DW_EK_Finanskladdelinje,
                ls.bk_ref_LedgertransId,
                ls.Beskrivelse,
                ls.Linjenummer,
                ls.Ekspeditionsnummer,
                ls.Transaktionstypenummer,
                ls.Transaktionstypetekst,
                ls.Maskinnummer
            INTO #Finanskladdelinje_Attributes
            FROM [stg].[dim_finanskladdelinje_sydax] ls
            LEFT JOIN [map].[Finanskladdelinje] m
                ON m.CleanCode_Finanskladdelinje = ls.CleanCode_Finanskladdelinje
            WHERE m.DW_EK_Finanskladdelinje IS NOT NULL;
        """
            )

            cursor.execute(
                """
            CREATE UNIQUE CLUSTERED INDEX IX_tmp_Finanskladdelinje_Attributes
            ON #Finanskladdelinje_Attributes (DW_EK_Finanskladdelinje)
            """
            )
            context.log.info("Temp table #Finanskladdelinje_Attributes created")

            cursor.execute(
                """
            MERGE [dim].[Finanskladdelinje] AS tgt
            USING #Finanskladdelinje_Attributes AS src
            ON tgt.DW_EK_Finanskladdelinje = src.DW_EK_Finanskladdelinje

            WHEN MATCHED THEN
                UPDATE SET
                    tgt.bk_ref_LedgertransId = src.bk_ref_LedgertransId,
                    tgt.Beskrivelse = src.Beskrivelse,
                    tgt.Linjenummer = src.Linjenummer,
                    tgt.Ekspeditionsnummer = src.Ekspeditionsnummer,
                    tgt.Transaktionstypenummer = src.Transaktionstypenummer,
                    tgt.Transaktionstypetekst = src.Transaktionstypetekst,
                    tgt.Maskinnummer = src.Maskinnummer,
                    tgt.DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    DW_EK_Finanskladdelinje,
                    bk_ref_LedgertransId,
                    Beskrivelse,
                    Linjenummer,
                    Ekspeditionsnummer,
                    Transaktionstypenummer,
                    Transaktionstypetekst,
                    Maskinnummer,
                    DW_Timestamp_Insert,
                    DW_Timestamp_Update
                )
                VALUES (
                    src.DW_EK_Finanskladdelinje,
                    src.bk_ref_LedgertransId,
                    src.Beskrivelse,
                    src.Linjenummer,
                    src.Ekspeditionsnummer,
                    src.Transaktionstypenummer,
                    src.Transaktionstypetekst,
                    src.Maskinnummer,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    CAST(SYSDATETIME() AS datetime2(0))
                );
        """
            )

            rows_affected = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #Finanskladdelinje_Attributes")

            cursor.execute("UPDATE STATISTICS [dim].[Finanskladdelinje]")

            cursor.execute(
                """
            SELECT
                COUNT(*) AS total_dim_rows,
                COUNT(Beskrivelse) AS has_beskrivelse,
                COUNT(Transaktionstypetekst) AS has_transaktionstypetekst
            FROM [dim].[Finanskladdelinje]
            WHERE DW_EK_Finanskladdelinje <> -1
        """
            )
            stats = cursor.fetchone()
            total_dim_rows = stats[0]
            has_beskrivelse = stats[1]
            has_transaktionstypetekst = stats[2]

            context.log.info(
                f"Dimension sync complete: {total_dim_rows} total rows in "
                f"dim.Finanskladdelinje — coverage: "
                f"Beskrivelse={has_beskrivelse}/{total_dim_rows}, "
                f"Transaktionstypetekst={has_transaktionstypetekst}/{total_dim_rows}"
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "total_dim_rows": total_dim_rows,
            "has_beskrivelse": has_beskrivelse,
            "has_transaktionstypetekst": has_transaktionstypetekst,
            "table": "dim.Finanskladdelinje",
            "source_tables": "stg.dim_finanskladdelinje_sydax + map.Finanskladdelinje",
            "scd_type": "Type 1 (overwrite)",
        }
    )
