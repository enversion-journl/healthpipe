# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: ILS aftale-grundlag.

Base staging for ILS - joins all contract/agreement tables.
Foundation for dim_kreditor_ils and dim_vare_ils.
"""

from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)


@asset(
    key=AssetKey(["erp", "pre_stg_vareaftaler_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["ext_lafth_ils"]),  # Full load (small table)
        # Daily partitioned ILS assets
        AssetKey(["ext_laftd_ils_daily"]),  # Daily partitioned
        AssetKey(["ext_laftdstam_ils_daily"]),  # Daily partitioned
        AssetKey(["ext_laftdpris_ils_daily"]),  # Daily partitioned
        # Monthly partitioned ILS assets
        AssetKey(["ext_laftd_ils_monthly"]),
        AssetKey(["ext_laftdstam_ils_monthly"]),
        AssetKey(["ext_laftdpris_ils_monthly"]),
        # Full-load ILS assets
        AssetKey(["ext_lev_ils"]),  # Full load
        AssetKey(["ext_vare_ils"]),  # Full load
        AssetKey(["ext_vareart_ils"]),  # Full load (small table)
        AssetKey(["ext_varegrp_ils"]),  # Full load (small table)
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "vareaftaler",
    },
    description="Base staging for ILS - joins all contract/agreement tables. Foundation for dim_kreditor_ils and dim_vare_ils.",
)
def pre_stg_vareaftaler_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Base staging for ILS contract/agreement data.

    Performs all heavy joins ONCE:
    - lafth_ils: Contract headers
    - laftd_ils: Contract lines with maerke (brand)
    - laftdstam_ils: Supplier item numbers (Leverandoer_Varenummer_CleanCode)
    - laftdpris_ils: Pricing and package sizes
    - lev_ils: Kreditor master with CleanCode_Kreditor
    - vare_ils: Item master data
    - vareart_ils, varegrp_ils: Reference data

    This table is the foundation for:
    - dim_kreditor_ils (aggregated kreditor view)
    - dim_vare_ils (aggregated vare view)
    - dim_aftale_ils (agreement dimension staging)
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info("Creating base staging table stg.pre_vareaftaler_ils...")

            # Drop existing table
            cursor.execute("DROP TABLE IF EXISTS [stg].[pre_vareaftaler_ils];")

            # Create base staging table with all joins
            cursor.execute(
                """
                SELECT
                    -- Primary identifiers
                    lafth.aftale,
                    laftd.vnr,
                    laftd.lnr AS laftd_lnr,
                    lafth.lnr AS lafth_lnr,

                    -- Clean codes (generated inline from raw ext columns)
                    CASE
                        WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                        THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                        ELSE NULL
                    END AS CleanCode_Kreditor,
                    CASE
                        WHEN stam.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(stam.[lvnr##1])) <> ''
                        THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(stam.[lvnr##1], ' ', ''))))
                        ELSE NULL
                    END AS CleanCode_LeverandoerVarenummer,
                    CASE
                        WHEN laftd.aftale IS NOT NULL AND laftd.vnr IS NOT NULL
                        THEN 'AFTALE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(laftd.aftale AS NVARCHAR(100)), ' ', '')))) + '_VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(laftd.vnr AS NVARCHAR(100)), ' ', ''))))
                        ELSE NULL
                    END AS CleanCode_Vareaftale,
                    'AFTALE_' + UPPER(LTRIM(RTRIM(lafth.aftale))) AS CleanCode_Aftale,

                    -- Kreditor attributes (for dim_kreditor_ils)
                    lev.se_nr,
                    lev.navn AS kreditor_navn,
                    lev.adr,
                    lev.adr2,
                    lev.postdi,
                    lev.kontakt,
                    lev.cvr_nr,
                    lev.oprdto,

                    -- Vare attributes (for dim_vare_ils)
                    stam.[lvnr##1] AS lvnr1,
                    laftd.maerke,
                    vare.navn AS vare_navn,
                    vare.faelvnr,
                    vare.ean_vnr,
                    vare.varestr,
                    vare.navn_conc,
                    vare.basisenh,
                    vare.vareart,
                    vare.varegrp,
                    vare.varetype,
                    vare.momskd,
                    vare.afgiftkd,
                    vareart.navn AS vareart_navn,
                    varegrp.navn AS varegrp_navn,

                    -- Agreement header data
                    lafth.afttype,
                    lafth.txt AS aftaletekst,
                    lafth.aftdto,
                    lafth.[start_] AS hoved_start,
                    lafth.slut AS hoved_slut,

                    -- Agreement line data
                    laftd.[start_] AS linje_start,
                    laftd.slut AS linje_slut,
                    laftd.[lvnr##2] AS lvnr2,
                    laftd.[lvnr##3] AS lvnr3,

                    -- Pricing data
                    laftdpris.[pakstr##1] AS pakstr1,
                    laftdpris.[pakstr##2] AS pakstr2,
                    laftdpris.[pakstr##3] AS pakstr3,
                    laftdpris.[pris##1] AS pris1,
                    laftdpris.[pris##2] AS pris2,
                    laftdpris.[pris##3] AS pris3,

                    -- Metadata
                    laftd.DW_LoadTimestamp

                INTO [stg].[pre_vareaftaler_ils]
                FROM [ext].[laftd_ils] laftd

                INNER JOIN [ext].[lafth_ils] lafth
                    ON lafth.aftale = laftd.aftale

                LEFT JOIN [ext].[lev_ils] lev
                    ON lev.lnr = COALESCE(laftd.lnr, lafth.lnr)

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            s.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY s.aftale, s.vnr
                                ORDER BY
                                    CASE WHEN s.[lvnr##1] IS NOT NULL AND s.[lvnr##1] <> '' THEN 0 ELSE 1 END,
                                    s.start_ DESC
                            ) AS rn
                        FROM [ext].[laftdstam_ils] s
                    ) ranked
                    WHERE rn = 1
                ) stam ON stam.aftale = laftd.aftale AND stam.vnr = laftd.vnr

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            p.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY p.aftale, p.vnr
                                ORDER BY
                                p.aktiv DESC,
                                p.start_ DESC
                            ) AS rn
                        FROM [ext].[laftdpris_ils] p
                    ) ranked
                    WHERE rn = 1
                ) laftdpris ON laftdpris.aftale = laftd.aftale AND laftdpris.vnr = laftd.vnr

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            v.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY v.vnr
                                ORDER BY v.aktiv DESC, v.oprdto DESC
                            ) AS rn
                        FROM [ext].[vare_ils] v
                    ) ranked
                    WHERE rn = 1
                ) vare ON vare.vnr = laftd.vnr

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            va.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY va.vareart
                                ORDER BY va.aktiv DESC, va.pro2modified DESC
                            ) AS rn
                        FROM [ext].[vareart_ils] va
                    ) ranked
                    WHERE rn = 1
                ) vareart ON vareart.vareart = vare.vareart

                LEFT JOIN (
                    SELECT *
                    FROM (
                        SELECT
                            vg.*,
                            ROW_NUMBER() OVER (
                                PARTITION BY vg.varegrp
                                ORDER BY vg.aktiv DESC, vg.pro2modified DESC
                            ) AS rn
                        FROM [ext].[varegrp_ils] vg
                    ) ranked
                    WHERE rn = 1
                ) varegrp ON varegrp.varegrp = vare.varegrp;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.pre_vareaftaler_ils with {row_count} rows")

            # Create clustered index on natural key
            cursor.execute(
                """
                CREATE CLUSTERED INDEX IX_pre_vareaftaler_ils_PK
                ON [stg].[pre_vareaftaler_ils](aftale, vnr);
            """
            )

            # Create non-clustered indexes for entity aggregation
            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_vareaftaler_ils_CVR
                ON [stg].[pre_vareaftaler_ils](CleanCode_Kreditor)
                INCLUDE (se_nr, kreditor_navn, adr, adr2, postdi, kontakt, oprdto);
            """
            )

            cursor.execute(
                """
                CREATE NONCLUSTERED INDEX IX_pre_vareaftaler_ils_Vare
                ON [stg].[pre_vareaftaler_ils](CleanCode_Kreditor, CleanCode_LeverandoerVarenummer)
                INCLUDE (
                    vnr,
                    lvnr1,
                    maerke,
                    vare_navn,
                    faelvnr,
                    ean_vnr,
                    vareart,
                    varegrp,
                    CleanCode_Vareaftale,
                    CleanCode_Aftale
                );
            """
            )

            context.log.info("Created indexes for entity aggregation")

            conn.commit()

            # Diagnostic: Check coverage
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(CleanCode_Kreditor) AS has_cvr_clean,
                    COUNT(CleanCode_LeverandoerVarenummer) AS has_vare_clean,
                    COUNT(CleanCode_Vareaftale) AS has_aftale_clean,
                    COUNT(DISTINCT CleanCode_Kreditor) AS unique_cvrs,
                    COUNT(DISTINCT CleanCode_LeverandoerVarenummer) AS unique_varer
                FROM [stg].[pre_vareaftaler_ils]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with CleanCode_Kreditor, "
                f"{stats[2]}/{stats[0]} with CleanCode_LeverandoerVarenummer, "
                f"{stats[3]}/{stats[0]} with CleanCode_Vareaftale. "
                f"Unique CVRs: {stats[4]}, unique varer: {stats[5]}"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.pre_vareaftaler_ils",
            "row_count": row_count,
            "source_tables": "lafth_ils + laftd_ils + laftdstam_ils + laftdpris_ils + lev_ils + vare_ils + vareart_ils + varegrp_ils",
            "grain": "aftale + vnr",
            "unique_cvrs": stats[4],
            "unique_varer": stats[5],
            "has_aftale_clean": stats[3],
            "layer": "stg_fct (Layer 0)",
        }
    )
