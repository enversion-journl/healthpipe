# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vare (Dimension Layer, delta-driven)

Delta of (clean_code_Kreditor, clean_code_LeverandoerVarenummer) entities from
the entity-delta hub (stg.pre_Leverandoervare_ILSVare_delta_entiteter), enriched with MySupply
invoice attributes read directly from arc.

dim.Leverandoervare carries no ILS item-master attributes: names on vnr grain live on
dim.ILSVare and are reached via bridge.LeverandoervareILSVare. The ILS side only
contributes entity existence (dw_source_ils).

Per-source flags (dw_source_mysupply/dw_source_ils) are derived from EXISTS
over the full current archive picture, not from which delta branch produced
the row — so a flag is correct regardless of why the row was staged.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from utils.sql.leverandoervare_ilsvare_sql import (
    LAFTDSTAM_ALT_LVNR_SLOTS,
    sql_lev_current_cte,
)
from utils.sql.leverandoervare_ilsvare_sql import LVNR_CC as _LVNR_CC
from utils.sql.tax_id_sql import sql_cvr_cleancode_from_column


def _alt_lvnr_dim_branches() -> str:
    """ILS-eksistens-branches for laftdstams alternative lvnr-slots."""
    branches = []
    for slot in LAFTDSTAM_ALT_LVNR_SLOTS:
        col = f"alt_s.[{slot}]"
        branches.append(
            f"""
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col=col)}
        FROM [arc].[ils_laftdstam] alt_s
        INNER JOIN lev_current lev ON lev.lnr = alt_s.lnr
        WHERE {col} IS NOT NULL AND LTRIM(RTRIM({col})) <> ''"""
        )
    return "".join(branches)


def _build_stg_sql(checkpoint: str | None) -> str:
    """Delta staging: hub entities (Vare + ILSVare-expansion) enriched from arc.

    checkpoint None => full hub picture (no timestamp filter).

    The delta entity set is materialized first (#delta_vare); the MySupply
    ranking is semi-joined to it BEFORE the window function, so the sort
    scales with the delta — not with the full invoice archive.
    """
    cp_filter = "" if checkpoint is None else f" AND h.dw_timestamp_update > '{checkpoint}'"
    mysupply_cvr = f"CAST({sql_cvr_cleancode_from_column('m.LeverandorCVR')} AS NVARCHAR(200))"

    return f"""
    DROP TABLE IF EXISTS #delta_vare;
    DROP TABLE IF EXISTS #mysupply_delta;
    DROP TABLE IF EXISTS #ils_pairs;
    DROP TABLE IF EXISTS [stg].[dim_Leverandoervare];

    -- 1) Delta-entiteterne. Hub'ens Vare-delta er komplet paa tvaers af
    --    kilder (inkl. varestamdata-aendringer, som hub'en selv propagerer
    --    til refererende vare-entiteter). Begge noegler kraeves udfyldt —
    --    (kreditor, leverandoervarenummer) ER entitetens business key, saa
    --    kravet haandhaeves her uafhaengigt af hub'ens egne guards.
    SELECT h.clean_code_Kreditor, h.clean_code_LeverandoerVarenummer
    INTO #delta_vare
    FROM [stg].[pre_Leverandoervare_ILSVare_delta_entiteter] h
    WHERE h.clean_code_Kreditor IS NOT NULL
      AND h.clean_code_LeverandoerVarenummer IS NOT NULL{cp_filter};

    CREATE UNIQUE CLUSTERED INDEX IX_tmp_delta_vare
    ON #delta_vare (clean_code_Kreditor, clean_code_LeverandoerVarenummer);

    -- 2) MySupply-attributter kun for delta-entiteterne: semi-join FOER
    --    ROW_NUMBER (partitionsnoeglerne ER join-noeglerne, saa vinderen pr.
    --    entitet er uaendret). Kun rn=1 gemmes — en raekke her er samtidig
    --    mysupply-eksistens for entiteten.
    SELECT clean_code_Kreditor, clean_code_LeverandoerVarenummer, Item_FakturaUdstederVareNavn
    INTO #mysupply_delta
    FROM (
        SELECT
            j.clean_code_Kreditor,
            j.clean_code_LeverandoerVarenummer,
            j.Item_FakturaUdstederVareNavn,
            ROW_NUMBER() OVER (
                PARTITION BY j.clean_code_Kreditor, j.clean_code_LeverandoerVarenummer
                ORDER BY
                    CASE WHEN j.Item_FakturaUdstederVareNavn IS NOT NULL
                         AND j.Item_FakturaUdstederVareNavn <> '' THEN 0 ELSE 1 END,
                    j.FakturaDato DESC
            ) AS rn
        FROM (
            SELECT
                {mysupply_cvr} AS clean_code_Kreditor,
                {_LVNR_CC.format(col="l.Item_FakturaUdstederVareNr")} AS clean_code_LeverandoerVarenummer,
                l.Item_FakturaUdstederVareNavn,
                h.FakturaDato
            FROM [arc].[mysupply_statistic_invoice_line] l
            INNER JOIN [arc].[mysupply_statistic_invoice_header] h ON h.id = l.statistic_invoice_header_id
            INNER JOIN [arc].[mysupply_metadata] m ON m.id = h.metadata_id
            WHERE l.Item_FakturaUdstederVareNr IS NOT NULL
              AND LTRIM(RTRIM(l.Item_FakturaUdstederVareNr)) <> ''
        ) j
        INNER JOIN #delta_vare dv
            ON dv.clean_code_Kreditor = j.clean_code_Kreditor
           AND dv.clean_code_LeverandoerVarenummer = j.clean_code_LeverandoerVarenummer
    ) ranked
    WHERE rn = 1;

    -- 2b) ILS-par-billedet materialiseres EEN gang til en indekseret temp-tabel,
    --     semi-joinet til delta-entiteterne (output probes kun med #delta_vare-
    --     noegler) og dedupliceret til par-grain. Det erstatter en korreleret
    --     EXISTS mod den ikke-materialiserbare ils_pairs-CTE pr. output-raekke,
    --     som tvang hele det 5-vejs ILS-arkiv-union (+ lev_current) til at blive
    --     gen-scannet for HVER raekke i #delta_vare. Ingen timestamp-filtrering
    --     her: eksistens er fuld-tilstand for entiteten (kun entitets-eksistens
    --     — attributter paa vnr-grain bor i dim.ILSVare og naas via bridgen).
    ;WITH{sql_lev_current_cte()},
    ils_pairs AS (
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="s.[lvnr##1]")} AS clean_code_LeverandoerVarenummer
        FROM [arc].[ils_laftdstam] s
        INNER JOIN lev_current lev ON lev.lnr = s.lnr
        WHERE s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="lg.lvnr")}
        FROM [arc].[ils_lgrvare] lg
        INNER JOIN lev_current lev ON lev.lnr = lg.lnr
        WHERE lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="ik.levvarenr")}
        FROM [arc].[ils_indkfak] ik
        INNER JOIN lev_current lev ON lev.lnr = ik.lnr
        WHERE ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
          AND ik.levvarenr <> '-'
          AND LEN(LTRIM(RTRIM(ik.levvarenr))) >= 3
          AND ik.levvarenr LIKE '%[a-zA-Z0-9]%'
          AND ik.vnr IS NOT NULL
        UNION ALL
        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="p.lvnr")}
        FROM [arc].[ils_pmdm] p
        INNER JOIN lev_current lev ON lev.lnr = p.lnr
        WHERE p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''{_alt_lvnr_dim_branches()}
    )
    SELECT DISTINCT ip.clean_code_Kreditor, ip.clean_code_LeverandoerVarenummer
    INTO #ils_pairs
    FROM ils_pairs ip
    INNER JOIN #delta_vare dv
        ON dv.clean_code_Kreditor = ip.clean_code_Kreditor
       AND dv.clean_code_LeverandoerVarenummer = ip.clean_code_LeverandoerVarenummer;

    CREATE UNIQUE CLUSTERED INDEX IX_tmp_ils_pairs
    ON #ils_pairs (clean_code_Kreditor, clean_code_LeverandoerVarenummer);

    -- 3) Output. Flags er fuld-tilstand for entiteten: mysupply = raekke i det
    --    delta-scopede fulde fakturabillede; ils = par i det materialiserede
    --    #ils_pairs (indekseret seek pr. raekke i stedet for gen-scanning af
    --    hele arkivet). Begge lookups kan ikke fanne ud: #mysupply_delta er
    --    rn=1 pr. par og #ils_pairs er DISTINCT paa par-grain, mens #delta_vare
    --    har unikke noegler — raekketallet bevares.
    SELECT
        d.clean_code_Kreditor,
        d.clean_code_LeverandoerVarenummer,
        d.clean_code_Kreditor + '_' + d.clean_code_LeverandoerVarenummer AS clean_code_Leverandoervare,
        d.clean_code_Kreditor AS bk_kreditor,
        d.clean_code_LeverandoerVarenummer AS bk_leverandoer_varenummer,
        mys.Item_FakturaUdstederVareNavn AS varenavn_mysupply,
        COALESCE(NULLIF(mys.Item_FakturaUdstederVareNavn, ''), 'UNKNOWN') AS varenavn,
        CAST(CASE WHEN mys.clean_code_Kreditor IS NOT NULL
             THEN 1 ELSE 0 END AS BIT) AS dw_source_mysupply,
        CAST(CASE WHEN ip.clean_code_Kreditor IS NOT NULL
             THEN 1 ELSE 0 END AS BIT) AS dw_source_ils,
        CAST(SYSDATETIME() AS datetime2(0)) AS dw_timestamp_update
    INTO [stg].[dim_Leverandoervare]
    FROM #delta_vare d
    LEFT JOIN #mysupply_delta mys
        ON mys.clean_code_Kreditor = d.clean_code_Kreditor
       AND mys.clean_code_LeverandoerVarenummer = d.clean_code_LeverandoerVarenummer
    LEFT JOIN #ils_pairs ip
        ON ip.clean_code_Kreditor = d.clean_code_Kreditor
       AND ip.clean_code_LeverandoerVarenummer = d.clean_code_LeverandoerVarenummer;

    DROP TABLE IF EXISTS #delta_vare;
    DROP TABLE IF EXISTS #mysupply_delta;
    DROP TABLE IF EXISTS #ils_pairs;
"""


@asset(
    key=AssetKey(["erp", "stg_dim_Leverandoervare"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_Leverandoervare_ILSVare_delta_entiteter"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "combined",
        "entity": "vare",
    },
    description=(
        "Delta staging at (clean_code_Kreditor, clean_code_LeverandoerVarenummer) grain: "
        "changed hub entities enriched from MySupply arc, with per-source presence "
        "flags (ILS contributes existence only). Feeds map.Leverandoervare."
    ),
)
def stg_dim_Leverandoervare(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Delta-driven Leverandoervare staging: hub entities enriched from arc, with EXISTS-derived source flags."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_Leverandoervare", dagster_run_id=context.run_id, mode=config.mode
    )
    row_count = 0
    try:
        cp = dfm.get_checkpoint("dim", "Leverandoervare")
        if config.mode == "full_load":
            cp = None

        if cp is not None:
            context.log.info(f"Delta mode: staging hub changes since {cp}")
        else:
            context.log.info("No checkpoint — staging the full hub picture")

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Creating staging table stg.dim_Leverandoervare from hub + arc enrichment..."
                )

                cursor.execute(_build_stg_sql(cp))

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_Leverandoervare]")
                row_count = cursor.fetchone()[0]
                context.log.info(f"Created stg.dim_Leverandoervare with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE NONCLUSTERED INDEX IX_dim_leverandoervare_CompositeKey
                    ON [stg].[dim_Leverandoervare](clean_code_Kreditor, clean_code_LeverandoerVarenummer);
                """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_dim_leverandoervare_CleanCode_Leverandoervare
                    ON [stg].[dim_Leverandoervare](clean_code_Leverandoervare);
                """
                )

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CAST(dw_source_mysupply AS INT)) AS from_mysupply,
                        SUM(CAST(dw_source_ils AS INT)) AS from_ils
                    FROM [stg].[dim_Leverandoervare]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[0]} changed entities — "
                    f"mysupply={stats[1]}, ils={stats[2]}"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Leverandoervare",
            "row_count": row_count,
            "source_tables": "stg.pre_Leverandoervare_ILSVare_delta_entiteter + arc.mysupply_* + arc.ils_*",
            "composite_key": "clean_code_Leverandoervare",
            "checkpoint_filter": cp if cp else "none (full hub picture)",
            "from_mysupply": stats[1],
            "from_ils": stats[2],
            "layer": "dim (Layer 1)",
            "mode": config.mode,
        }
    )
