# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Unified Vare Dimension (Layer 1)

Single unified staging table at (CVR_CleanCode, Leverandoer_Varenummer_CleanCode) grain,
combining MySupply invoice data, ILS bridge data, and ILS agreement data.

Used by both MapUpd_vare and DimUpd_vare as the sole source of truth.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_vare"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "stg_dim_vare_mysupply"]),
        AssetKey(["erp", "pre_stg_bridge_vare_ils"]),
        AssetKey(["erp", "pre_stg_vareaftaler_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "combined",
        "entity": "vare",
    },
    description="Unified vare staging at (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) grain - combines MySupply + ILS bridge + ILS agreements",
)
def stg_dim_vare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Unified vare dimension staging combining all sources.

    Collects all (CleanCode_Kreditor, CleanCode_LeverandoerVarenummer) keys from:
    - stg.dim_vare_mysupply (MySupply invoice lines)
    - stg.pre_bridge_vare_ils (ILS bridge cascade: laftdstam, lgrvare, laftd, pmdm)
    - stg.pre_vareaftaler_ils (ILS agreement/contract data)

    LEFT JOINs each source for attributes, coalesces names, and deduplicates
    agreement data by latest start date.
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating unified staging table stg.dim_vare...")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_vare];")

            cursor.execute(
                """
                ;WITH all_keys AS (
                    SELECT DISTINCT
                        CleanCode_Kreditor,
                        CleanCode_LeverandoerVarenummer
                    FROM [stg].[dim_vare_mysupply]
                    WHERE CleanCode_Kreditor IS NOT NULL
                      AND CleanCode_LeverandoerVarenummer IS NOT NULL

                    UNION

                    SELECT DISTINCT
                        CleanCode_Kreditor,
                        CleanCode_LeverandoerVarenummer
                    FROM [stg].[pre_bridge_vare_ils]
                    WHERE CleanCode_Kreditor IS NOT NULL AND CleanCode_Kreditor <> ''
                      AND CleanCode_LeverandoerVarenummer IS NOT NULL
                      AND CleanCode_LeverandoerVarenummer <> ''

                    UNION

                    SELECT DISTINCT
                        CleanCode_Kreditor,
                        CleanCode_LeverandoerVarenummer
                    FROM [stg].[pre_vareaftaler_ils]
                    WHERE CleanCode_Kreditor IS NOT NULL AND CleanCode_Kreditor <> ''
                      AND CleanCode_LeverandoerVarenummer IS NOT NULL
                      AND CleanCode_LeverandoerVarenummer <> ''
                )

                SELECT
                    k.CleanCode_Kreditor,
                    k.CleanCode_LeverandoerVarenummer,
                    k.CleanCode_Kreditor + '_' + k.CleanCode_LeverandoerVarenummer AS CleanCode_Vare,
                    k.CleanCode_Kreditor AS bk_Kreditor,
                    k.CleanCode_LeverandoerVarenummer AS bk_LeverandoerVarenummer,

                    -- Relationship columns from ILS
                    COALESCE(bridge.vnr, aftaler.vnr) AS vnr,
                    COALESCE(bridge.lnr, aftaler.kreditornummer) AS lnr,

                    -- MySupply attributes
                    mys.Item_FakturaUdstederVareNavn,
                    mys.Item_FakturaUdstederVareNr,

                    -- ILS product attributes (bridge > agreement fallback)
                    COALESCE(bridge.vare_navn, aftaler.vare_navn) AS ILS_Varenavn,
                    COALESCE(bridge.faelvnr, aftaler.faelvnr) AS faelvnr,
                    COALESCE(bridge.ean_vnr, aftaler.ean_vnr) AS ean_vnr,
                    COALESCE(bridge.varestr, aftaler.varestr) AS varestr,
                    COALESCE(bridge.navn_conc, aftaler.navn_conc) AS navn_conc,
                    COALESCE(bridge.basisenh, aftaler.basisenh) AS basisenh,
                    COALESCE(bridge.vareart, aftaler.vareart) AS vareart,
                    COALESCE(bridge.varegrp, aftaler.varegrp) AS varegrp,
                    COALESCE(bridge.varetype, aftaler.varetype) AS varetype,

                    -- Agreement-specific attributes
                    aftaler.aftale,
                    aftaler.afttype,
                    aftaler.aftaletekst,
                    aftaler.lvnr1,
                    aftaler.lvnr2,
                    aftaler.lvnr3,
                    aftaler.maerke,
                    aftaler.pakstr1,
                    aftaler.pakstr2,
                    aftaler.pakstr3,
                    aftaler.pris1,
                    aftaler.pris2,
                    aftaler.pris3,
                    aftaler.momskd,
                    aftaler.afgiftkd,
                    aftaler.vareart_navn,
                    aftaler.varegrp_navn,
                    aftaler.ils_dsa_aftale_varenummer,
                    aftaler.kreditornummer,
                    aftaler.CleanCode_Aftale,

                    -- Derived: best available name
                    COALESCE(
                        mys.Item_FakturaUdstederVareNavn,
                        bridge.vare_navn,
                        aftaler.vare_navn,
                        'UNKNOWN'
                    ) AS Final_Varenavn,

                    -- Source tracking
                    CASE
                        WHEN mys.CleanCode_Kreditor IS NOT NULL AND bridge.CleanCode_Kreditor IS NOT NULL
                            THEN 'mysupply+' + bridge.bridge_source
                        WHEN mys.CleanCode_Kreditor IS NOT NULL THEN 'mysupply'
                        WHEN bridge.CleanCode_Kreditor IS NOT NULL THEN bridge.bridge_source
                        WHEN aftaler.CleanCode_Kreditor IS NOT NULL THEN 'agreement'
                        ELSE 'unknown'
                    END AS source,

                    COALESCE(mys.DW_LoadTimestamp, bridge.DW_LoadTimestamp, aftaler.DW_LoadTimestamp)
                        AS DW_LoadTimestamp

                INTO [stg].[dim_vare]
                FROM all_keys k

                -- MySupply attributes
                LEFT JOIN [stg].[dim_vare_mysupply] mys
                    ON mys.CleanCode_Kreditor = k.CleanCode_Kreditor
                   AND mys.CleanCode_LeverandoerVarenummer = k.CleanCode_LeverandoerVarenummer

                -- ILS bridge attributes (already deduplicated by bridge priority)
                LEFT JOIN [stg].[pre_bridge_vare_ils] bridge
                    ON bridge.CleanCode_Kreditor = k.CleanCode_Kreditor
                   AND bridge.CleanCode_LeverandoerVarenummer = k.CleanCode_LeverandoerVarenummer

                -- Agreement attributes (deduplicated by latest start date)
                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            CleanCode_Kreditor,
                            CleanCode_LeverandoerVarenummer,
                            vnr,
                            COALESCE(laftd_lnr, lafth_lnr) AS kreditornummer,
                            vare_navn,
                            faelvnr,
                            ean_vnr,
                            varestr,
                            navn_conc,
                            basisenh,
                            vareart,
                            varegrp,
                            varetype,
                            aftale,
                            afttype,
                            aftaletekst,
                            lvnr1,
                            lvnr2,
                            lvnr3,
                            maerke,
                            pakstr1,
                            pakstr2,
                            pakstr3,
                            pris1,
                            pris2,
                            pris3,
                            momskd,
                            afgiftkd,
                            vareart_navn,
                            varegrp_navn,
                            vnr AS ils_dsa_aftale_varenummer,
                            CleanCode_Aftale,
                            DW_LoadTimestamp,
                            ROW_NUMBER() OVER (
                                PARTITION BY CleanCode_Kreditor, CleanCode_LeverandoerVarenummer
                                ORDER BY COALESCE(hoved_start, linje_start) DESC
                            ) AS rn
                        FROM [stg].[pre_vareaftaler_ils]
                        WHERE CleanCode_Kreditor IS NOT NULL
                          AND CleanCode_LeverandoerVarenummer IS NOT NULL
                    ) ranked
                    WHERE rn = 1
                ) aftaler
                    ON aftaler.CleanCode_Kreditor = k.CleanCode_Kreditor
                   AND aftaler.CleanCode_LeverandoerVarenummer = k.CleanCode_LeverandoerVarenummer;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_vare with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_vare_CompositeKey
                ON [stg].[dim_vare](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer);
            """
            )
            context.log.info("Created unique clustered index")
            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_dim_vare_CleanCode_Vare
                ON [stg].[dim_vare](CleanCode_Vare);
            """
            )

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(vnr) AS has_vnr,
                    COUNT(Item_FakturaUdstederVareNavn) AS has_mysupply_navn,
                    COUNT(ILS_Varenavn) AS has_ils_navn,
                    COUNT(aftale) AS has_aftale,
                    COUNT(ean_vnr) AS has_ean,
                    SUM(CASE WHEN source LIKE 'mysupply%' THEN 1 ELSE 0 END) AS from_mysupply,
                    SUM(CASE WHEN source NOT LIKE 'mysupply%' THEN 1 ELSE 0 END) AS from_ils_only
                FROM [stg].[dim_vare]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with vnr, "
                f"{stats[2]}/{stats[0]} with MySupply navn, "
                f"{stats[3]}/{stats[0]} with ILS navn, "
                f"{stats[4]}/{stats[0]} with aftale, "
                f"{stats[5]}/{stats[0]} with EAN. "
                f"Sources: {stats[6]} from MySupply, {stats[7]} ILS-only"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_vare",
            "row_count": row_count,
            "source_tables": "stg.dim_vare_mysupply + stg.pre_bridge_vare_ils + stg.pre_vareaftaler_ils",
            "composite_key": "CleanCode_Vare",
            "has_vnr": stats[1],
            "has_mysupply_navn": stats[2],
            "has_ils_navn": stats[3],
            "has_aftale": stats[4],
            "from_mysupply": stats[6],
            "from_ils_only": stats[7],
            "layer": "dim (Layer 1)",
        }
    )
