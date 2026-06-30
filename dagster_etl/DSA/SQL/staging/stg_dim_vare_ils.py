# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vare from ILS (Dimension Layer)

Aggregate ILS vare data at (CVR_CleanCode, Leverandoer_Varenummer_CleanCode) level.
Derived from pre_stg_vareaftaler_ils + pre_stg_bridge_vare_ils.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_vare_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_vareaftaler_ils"]),
        AssetKey(["erp", "pre_stg_bridge_vare_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "vare",
    },
    description="Aggregate ILS vare data at (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) level - from agreements + bridge tables",
)
def stg_dim_vare_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Staging for ILS vare (product/item) data.

    UNIONs agreement-based data (pre_stg_vareaftaler_ils) with expanded bridge
    data (pre_stg_bridge_vare_ils) and deduplicates at composite key level.
    Agreement data is preferred when both sources provide the same key.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating staging table stg.dim_vare_ils from agreements + bridges...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_vare_ils];")

            cursor.execute(
                """
                SELECT
                    CleanCode_Kreditor,
                    CleanCode_LeverandoerVarenummer,
                    lnr,
                    vnr,
                    aftale,
                    lvnr1,
                    maerke,
                    navn,
                    faelvnr,
                    ean_vnr,
                    varestr,
                    navn_conc,
                    basisenh,
                    vareart,
                    varegrp,
                    varetype,
                    pakstr1,
                    pakstr2,
                    pakstr3,
                    vareart_navn,
                    varegrp_navn,
                    bridge_source,
                    DW_LoadTimestamp
                INTO [stg].[dim_vare_ils]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Kreditor, CleanCode_LeverandoerVarenummer
                            ORDER BY
                                source_priority ASC,
                                CASE WHEN lvnr1 IS NOT NULL AND lvnr1 <> '' THEN 0 ELSE 1 END,
                                CASE WHEN navn IS NOT NULL AND navn <> '' THEN 0 ELSE 1 END,
                                DW_LoadTimestamp DESC
                        ) AS rn
                    FROM (
                        -- Source 1: Agreement data (highest priority)
                        SELECT
                            CleanCode_Kreditor,
                            CleanCode_LeverandoerVarenummer,
                            COALESCE(laftd_lnr, lafth_lnr) AS lnr,
                            vnr,
                            aftale,
                            lvnr1,
                            maerke,
                            vare_navn AS navn,
                            faelvnr,
                            ean_vnr,
                            varestr,
                            navn_conc,
                            basisenh,
                            vareart,
                            varegrp,
                            varetype,
                            pakstr1,
                            pakstr2,
                            pakstr3,
                            vareart_navn,
                            varegrp_navn,
                            'agreement' AS bridge_source,
                            0 AS source_priority,
                            DW_LoadTimestamp
                        FROM [stg].[pre_vareaftaler_ils]
                        WHERE CleanCode_Kreditor IS NOT NULL AND CleanCode_Kreditor <> ''
                          AND CleanCode_LeverandoerVarenummer IS NOT NULL
                          AND CleanCode_LeverandoerVarenummer <> ''

                        UNION ALL

                        -- Source 2: Bridge table data (fills gaps)
                        SELECT
                            CleanCode_Kreditor,
                            CleanCode_LeverandoerVarenummer,
                            lnr,
                            vnr,
                            NULL AS aftale,
                            NULL AS lvnr1,
                            NULL AS maerke,
                            vare_navn AS navn,
                            faelvnr,
                            ean_vnr,
                            varestr,
                            navn_conc,
                            basisenh,
                            vareart,
                            varegrp,
                            varetype,
                            NULL AS pakstr1,
                            NULL AS pakstr2,
                            NULL AS pakstr3,
                            NULL AS vareart_navn,
                            NULL AS varegrp_navn,
                            bridge_source,
                            bridge_priority AS source_priority,
                            DW_LoadTimestamp
                        FROM [stg].[pre_bridge_vare_ils]
                        WHERE CleanCode_Kreditor IS NOT NULL AND CleanCode_Kreditor <> ''
                          AND CleanCode_LeverandoerVarenummer IS NOT NULL
                          AND CleanCode_LeverandoerVarenummer <> ''
                    ) combined
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_vare_ils with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_vare_ils_CompositeKey
                ON [stg].[dim_vare_ils](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer);
            """
            )
            context.log.info("Created unique clustered index")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(navn) AS has_navn,
                    COUNT(maerke) AS has_maerke,
                    COUNT(ean_vnr) AS has_ean,
                    COUNT(vnr) AS has_vnr,
                    SUM(CASE WHEN bridge_source = 'agreement' THEN 1 ELSE 0 END) AS from_agreement,
                    SUM(CASE WHEN bridge_source <> 'agreement' THEN 1 ELSE 0 END) AS from_bridge
                FROM [stg].[dim_vare_ils]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with navn, "
                f"{stats[2]}/{stats[0]} with maerke, {stats[3]}/{stats[0]} with EAN, "
                f"{stats[4]}/{stats[0]} with vnr. "
                f"Sources: {stats[5]} from agreement, {stats[6]} from bridge"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_vare_ils",
            "row_count": row_count,
            "source_tables": "stg.pre_vareaftaler_ils + stg.pre_bridge_vare_ils",
            "composite_key": "CleanCode_Kreditor + CleanCode_LeverandoerVarenummer",
            "has_navn": stats[1],
            "has_vnr": stats[4],
            "from_agreement": stats[5],
            "from_bridge": stats[6],
            "layer": "dim (Layer 1)",
        }
    )
