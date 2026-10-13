# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: ILS aftale-grundlag.

Base staging for ILS - joins all contract/agreement tables.
Foundation for stg_dim_Leverandoervare and stg_dim_Aftale_ils.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import (
    AssetExecutionContext,
    AssetKey,
    MaterializeResult,
    RetryPolicy,
    asset,
)


@asset(
    key=AssetKey(["erp", "pre_stg_Vareaftaler_ils"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        # Archive ILS assets (full load via arc, replacing ext)
        AssetKey(["arc_ils_lafth"]),
        AssetKey(["arc_ils_laftd"]),
        AssetKey(["arc_ils_laftdstam"]),
        AssetKey(["arc_ils_laftdpris"]),
        AssetKey(["arc_ils_lev"]),
        AssetKey(["arc_ils_vare"]),
        AssetKey(["arc_ils_vareart"]),
        AssetKey(["arc_ils_varegrp"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "vareaftaler",
    },
    description="Base staging for ILS - joins all contract/agreement tables. Foundation for stg_dim_Leverandoervare and stg_dim_Aftale_ils.",
)
def pre_stg_Vareaftaler_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Base staging for ILS contract/agreement data.

    Performs all heavy joins ONCE:
    - lafth_ils: Contract headers (deduped to one row per (lnr, aftale); the
      newest archive row wins, since arc upserts on prrowid alone)
    - laftd_ils: Contract lines with maerke (brand)
    - laftdstam_ils: Supplier item numbers (Leverandoer_Varenummer_CleanCode)
    - laftdpris_ils: Pricing and package sizes
    - lev_ils: joined only to generate clean_code_Kreditor (kreditor
      attributes are enriched from arc.ils_lev in dim_Kreditor)
    - vare_ils: Item master data
    - vareart_ils, varegrp_ils: Reference data

    This table is the foundation for:
    - dim_Leverandoervare_ils (aggregated vare view)
    - dim_Aftale_ils (agreement dimension staging)

    Partition filter decision: the original code was a pure full rebuild (no
    partition_key, no incremental filter). This asset reads arc tables that are
    fully populated, so it continues as a full rebuild on every run.
    No checkpoint filter is applied here — dim_Aftale owns the checkpoint.
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_vareaftaler_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0

    try:
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
                        laftd.lnr,
                        laftd.vnr,

                        -- Clean codes (generated inline from raw ext columns)
                        CASE
                            WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                            THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                            ELSE NULL
                        END AS clean_code_Kreditor,
                        CASE
                            WHEN stam.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(stam.[lvnr##1])) <> ''
                            THEN 'LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(stam.[lvnr##1], ' ', ''))))
                            ELSE NULL
                        END AS clean_code_LeverandoerVarenummer,
                        'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(laftd.lnr AS NVARCHAR(100))))) + '_'
                            + UPPER(LTRIM(RTRIM(REPLACE(lafth.aftale, ' ', '')))) AS clean_code_Aftale,

                        -- Vare attributes (for dim_Leverandoervare_ils)
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
                        lafth.ansvar,
                        lafth.[start_] AS hoved_start,
                        lafth.slut AS hoved_slut,

                        -- Agreement line data
                        laftd.[start_] AS linje_start,
                        laftd.slut AS linje_slut,
                        laftd.[lvnr##2] AS lvnr2,
                        laftd.[lvnr##3] AS lvnr3,

                        -- Pricing data (nyeste aktive pris — dim-lag; fuld historik ligger i stg.fct_AftaleVarePris_ils)
                        laftdpris.[pakstr##1] AS pakstr1,
                        laftdpris.[pakstr##2] AS pakstr2,
                        laftdpris.[pakstr##3] AS pakstr3,
                        laftdpris.[pris##1] AS pris1,
                        laftdpris.[pris##2] AS pris2,
                        laftdpris.[pris##3] AS pris3,

                        -- Metadata
                        laftd.dw_timestamp_update

                    INTO [stg].[pre_vareaftaler_ils]
                    FROM [arc].[ils_laftd] laftd

                    -- arc.ils_lafth upserts on prrowid, so several rowids can sit on
                    -- the same (lnr, aftale) — notably when Progress recreates a
                    -- record under a new rowid and the old row is left behind
                    -- (detect_deletes=False). The dimension's business key is
                    -- (lnr, aftale), and here the header's VALUES matter (unlike the
                    -- fct chain, where only its existence counts), so the newest
                    -- archive row wins. prrowid DESC is only a deterministic
                    -- tiebreaker for when the timestamps are equal.
                    INNER JOIN (
                        SELECT *
                        FROM (
                            SELECT
                                h.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY h.lnr, h.aftale
                                    ORDER BY h.dw_timestamp_update DESC, h.prrowid DESC
                                ) AS rn
                            FROM [arc].[ils_lafth] h
                        ) ranked
                        WHERE rn = 1
                    ) lafth
                        ON lafth.aftale = laftd.aftale
                       AND lafth.lnr = laftd.lnr

                    LEFT JOIN [arc].[ils_lev] lev
                        ON lev.lnr = laftd.lnr

                    LEFT JOIN (
                        SELECT *
                        FROM (
                            SELECT
                                s.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY s.lnr, s.aftale, s.vnr
                                    ORDER BY
                                        CASE WHEN s.[lvnr##1] IS NOT NULL AND s.[lvnr##1] <> '' THEN 0 ELSE 1 END,
                                        s.start_ DESC
                                ) AS rn
                            FROM [arc].[ils_laftdstam] s
                        ) ranked
                        WHERE rn = 1
                    ) stam ON stam.lnr = laftd.lnr AND stam.aftale = laftd.aftale AND stam.vnr = laftd.vnr

                    LEFT JOIN (
                        SELECT *
                        FROM (
                            SELECT
                                p.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY p.lnr, p.aftale, p.vnr
                                    ORDER BY
                                    p.aktiv DESC,
                                    p.start_ DESC
                                ) AS rn
                            FROM [arc].[ils_laftdpris] p
                        ) ranked
                        WHERE rn = 1
                    ) laftdpris ON laftdpris.lnr = laftd.lnr AND laftdpris.aftale = laftd.aftale AND laftdpris.vnr = laftd.vnr

                    LEFT JOIN (
                        SELECT *
                        FROM (
                            SELECT
                                v.*,
                                ROW_NUMBER() OVER (
                                    PARTITION BY v.vnr
                                    ORDER BY v.aktiv DESC, v.oprdto DESC
                                ) AS rn
                            FROM [arc].[ils_vare] v
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
                            FROM [arc].[ils_vareart] va
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
                            FROM [arc].[ils_varegrp] vg
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
                    ON [stg].[pre_vareaftaler_ils](aftale, lnr, vnr);
                """
                )

                # Create non-clustered indexes for entity aggregation
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_vareaftaler_ils_Vare
                    ON [stg].[pre_vareaftaler_ils](clean_code_Kreditor, clean_code_LeverandoerVarenummer)
                    INCLUDE (
                        vnr,
                        lvnr1,
                        maerke,
                        vare_navn,
                        faelvnr,
                        ean_vnr,
                        vareart,
                        varegrp,
                        clean_code_Aftale
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
                        COUNT(clean_code_Kreditor) AS has_cvr_clean,
                        COUNT(clean_code_LeverandoerVarenummer) AS has_vare_clean,
                        COUNT(DISTINCT clean_code_Kreditor) AS unique_cvrs,
                        COUNT(DISTINCT clean_code_LeverandoerVarenummer) AS unique_varer
                    FROM [stg].[pre_vareaftaler_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with clean_code_Kreditor, "
                    f"{stats[2]}/{stats[0]} with clean_code_LeverandoerVarenummer. "
                    f"Unique CVRs: {stats[3]}, unique varer: {stats[4]}"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_vareaftaler_ils",
            "row_count": row_count,
            "source_tables": "arc.ils_lafth + arc.ils_laftd + arc.ils_laftdstam + arc.ils_laftdpris + arc.ils_lev + arc.ils_vare + arc.ils_vareart + arc.ils_varegrp",
            "grain": "aftale + lnr + vnr",
            "unique_cvrs": stats[3],
            "unique_varer": stats[4],
            "layer": "stg_fct (Layer 0)",
            "mode": config.mode,
        }
    )
