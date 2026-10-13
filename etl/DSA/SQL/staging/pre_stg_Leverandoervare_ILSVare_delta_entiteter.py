# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Pre-staging: entity-delta hub for the Vare/ILSVare/Bridge flows.

Extracts CHANGED ENTITIES (not pairs, not attributes) from the archive since
the relevant consumer checkpoints:
  - Vare entities (kreditor + leverandoervarenummer): from the five ILS bridge
    tables and MySupply, filtered by checkpoint_Vare =
    MIN(CP dim.Leverandoervare, CP bridge.LeverandoervareILSVare). laftdstam
    and laftd contribute ALL of their lvnr-slots (##1 the primary plus the
    alternates ##2-4/##2-3, spec 2026-08-27-alternate-lvnr-rows), not just
    lvnr##1. dim.Leverandoervare carries no copy of
    ILS item-master attributes (names live on dim.ILSVare, reached via the
    bridge), so ils_vare changes only flow through the ILSVare axis.
  - ILSVare entities (vnr): from ils_vare (incl. its lookup tables — vareart,
    varegrp, varetype, moms, zz_oper, unspsc — so a lookup-row change restages
    every ils_vare row referencing it), lgrbeh/afdlgrbeh, and the five ILS
    bridge tables (a vnr can appear in the pair sources without an item-master
    row and must still be bridgeable). Filtered by checkpoint_ILSVare =
    MIN(CP dim.ILSVare, CP bridge.LeverandoervareILSVare).

ILSVare rows carry BOTH the natural key (raw vnr, the join key used by
downstream ILS-side lookups) and the derived clean_code_ILSVare (the map key).
Vare rows carry only cleancodes — their natural key is cross-system and only
unified through the cleancode pair.

Checkpoints are never NULL: a consumer checkpoint missing from DFM resolves
to BEGINNING_OF_TIME, so the filter selects the full archive picture. The hub
only READS checkpoints (checkpoint_after=None).
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
from utils.sql.leverandoervare_ilsvare_sql import (
    LAFTD_ALT_LVNR_SLOTS,
    LAFTDSTAM_ALT_LVNR_SLOTS,
    sql_lev_current_cte,
)
from utils.sql.leverandoervare_ilsvare_sql import LVNR_CC as _LVNR_CC
from utils.sql.leverandoervare_ilsvare_sql import VNR_CC as _VNR_CC
from utils.sql.tax_id_sql import sql_cvr_cleancode_from_column

BEGINNING_OF_TIME = "0001-01-01"


def _alt_lvnr_hub_branches() -> str:
    """UNION ALL-branches for de alternative lvnr-slots (spec 2026-08-27).

    Samme form som laftdstam/laftd-branches for lvnr##1: normaliseret via
    LVNR_CC, tom/NULL filtreret. Et alternativ identisk med lvnr##1 kollapser
    til samme CleanCode og dedupliceres af hub'ens GROUP BY.
    """
    branches = []
    for table, alias, slots in (
        ("ils_laftdstam", "alt_s", LAFTDSTAM_ALT_LVNR_SLOTS),
        ("ils_laftd", "alt_d", LAFTD_ALT_LVNR_SLOTS),
    ):
        for slot in slots:
            col = f"{alias}.[{slot}]"
            branches.append(
                f"""
                        UNION ALL
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col=col)},
                               {alias}.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[{table}] {alias}
                        INNER JOIN lev_current lev ON lev.lnr = {alias}.lnr
                        WHERE {col} IS NOT NULL AND LTRIM(RTRIM({col})) <> ''"""
            )
    return "".join(branches)


def get_checkpoints(dfm: DfmManager, context: AssetExecutionContext) -> tuple[str, str]:
    """Resolve the hub's two axis checkpoints from the consumer checkpoints in DFM.

    Returns (checkpoint_ILSVare, checkpoint_Vare):
      - checkpoint_ILSVare = MIN(CP dim.ILSVare, CP bridge.LeverandoervareILSVare)
      - checkpoint_Vare    = MIN(CP dim.Leverandoervare,    CP bridge.LeverandoervareILSVare)

    A consumer checkpoint missing from DFM counts as BEGINNING_OF_TIME, so the
    returned checkpoints are never None — a missing consumer forces its axis
    to the full archive picture. Checkpoints are ISO-8601 strings, so min() is
    chronological.
    """
    cp_dim_vare = dfm.get_checkpoint("dim", "Leverandoervare")
    cp_dim_ilsvare = dfm.get_checkpoint("dim", "ILSVare")
    cp_bridge = dfm.get_checkpoint("bridge", "LeverandoervareILSVare")
    context.log.info(
        "DFM-checkpoints: dim.Leverandoervare=%s, dim.ILSVare=%s, bridge.LeverandoervareILSVare=%s",
        cp_dim_vare,
        cp_dim_ilsvare,
        cp_bridge,
    )

    checkpoint_ilsvare = min(
        cp_dim_ilsvare or BEGINNING_OF_TIME,
        cp_bridge or BEGINNING_OF_TIME,
    )
    checkpoint_vare = min(
        cp_dim_vare or BEGINNING_OF_TIME,
        cp_bridge or BEGINNING_OF_TIME,
    )
    context.log.info(
        "Hub-akser: checkpoint_ILSVare=%s, checkpoint_Vare=%s",
        checkpoint_ilsvare,
        checkpoint_vare,
    )
    return checkpoint_ilsvare, checkpoint_vare


def _build_hub_sql(checkpoint_Vare: str, checkpoint_ILSVare: str) -> str:
    """Build the full hub SQL. Pure function — unit-testable without a DB.

    Both checkpoints must be non-NULL ISO date strings (BEGINNING_OF_TIME for
    the full picture); every source branch filters unconditionally on
    dw_timestamp_update > checkpoint.
    """
    mysupply_cvr = f"CAST({sql_cvr_cleancode_from_column('m.LeverandorCVR')} AS NVARCHAR(200))"
    return f"""
                    DROP TABLE IF EXISTS [stg].[pre_Leverandoervare_ILSVare_delta_entiteter];

                    ;WITH{sql_lev_current_cte()},
                    ils_pairs AS (
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="s.[lvnr##1]")} AS clean_code_LeverandoerVarenummer,
                               s.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[ils_laftdstam] s
                        INNER JOIN lev_current lev ON lev.lnr = s.lnr
                        WHERE s.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(s.[lvnr##1])) <> ''
                        UNION ALL
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="lg.lvnr")},
                               lg.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[ils_lgrvare] lg
                        INNER JOIN lev_current lev ON lev.lnr = lg.lnr
                        WHERE lg.lvnr IS NOT NULL AND LTRIM(RTRIM(lg.lvnr)) <> ''
                        UNION ALL
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="ik.levvarenr")},
                               ik.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[ils_indkfak] ik
                        INNER JOIN lev_current lev ON lev.lnr = ik.lnr
                        WHERE ik.levvarenr IS NOT NULL AND LTRIM(RTRIM(ik.levvarenr)) <> ''
                          AND ik.levvarenr <> '-'
                          AND LEN(LTRIM(RTRIM(ik.levvarenr))) >= 3
                          AND ik.levvarenr LIKE '%[a-zA-Z0-9]%'
                          AND ik.vnr IS NOT NULL
                        UNION ALL
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="ld.[lvnr##1]")},
                               ld.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[ils_laftd] ld
                        INNER JOIN lev_current lev ON lev.lnr = ld.lnr
                        WHERE ld.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(ld.[lvnr##1])) <> ''
                        UNION ALL
                        SELECT lev.clean_code_Kreditor, {_LVNR_CC.format(col="p.lvnr")},
                               p.dw_timestamp_update, lev.lev_dw_timestamp_update
                        FROM [arc].[ils_pmdm] p
                        INNER JOIN lev_current lev ON lev.lnr = p.lnr
                        WHERE p.lvnr IS NOT NULL AND LTRIM(RTRIM(p.lvnr)) <> ''{_alt_lvnr_hub_branches()}
                    ),
                    vare_entities AS (
                        -- ILS: par-raekken ELLER leverandoeren aendret (et lev-skift,
                        -- fx nyt CVR, restager ALLE leverandoerens entiteter)
                        SELECT clean_code_Kreditor,
                               clean_code_LeverandoerVarenummer,
                               CASE
                                   WHEN lev_dw_timestamp_update IS NULL
                                     OR dw_timestamp_update >= lev_dw_timestamp_update
                                   THEN dw_timestamp_update
                                   ELSE lev_dw_timestamp_update
                               END AS dw_timestamp_update
                        FROM ils_pairs
                        WHERE dw_timestamp_update > '{checkpoint_Vare}'
                           OR lev_dw_timestamp_update > '{checkpoint_Vare}'
                        UNION ALL
                        -- MySupply: linje-, header- eller metadata-aendring
                        -- (metadata baerer CVR — et flyt restager entiteten);
                        -- vandmaerket er den nyeste af de tre timestamps
                        SELECT {mysupply_cvr},
                               {_LVNR_CC.format(col="l.Item_FakturaUdstederVareNr")},
                               (SELECT MAX(ts) FROM (VALUES
                                   (l.dw_timestamp_update),
                                   (h.dw_timestamp_update),
                                   (m.dw_timestamp_update)
                               ) t(ts))
                        FROM [arc].[mysupply_statistic_invoice_line] l
                        INNER JOIN [arc].[mysupply_statistic_invoice_header] h ON h.id = l.statistic_invoice_header_id
                        INNER JOIN [arc].[mysupply_metadata] m ON m.id = h.metadata_id
                        WHERE l.Item_FakturaUdstederVareNr IS NOT NULL
                          AND LTRIM(RTRIM(l.Item_FakturaUdstederVareNr)) <> ''
                          AND (l.dw_timestamp_update > '{checkpoint_Vare}'
                            OR h.dw_timestamp_update > '{checkpoint_Vare}'
                            OR m.dw_timestamp_update > '{checkpoint_Vare}')
                    ),
                    ilsvare_entities AS (
                        -- Varestammen inkl. opslagstabellerne i EEN scanning: en
                        -- opslags-aendring (fx nyt vareart-navn) skal restage alle
                        -- refererende vnr, saa navnene naar dim.ILSVare; vandmaerket
                        -- er den nyeste af raekkens og opslagenes timestamps
                        SELECT CAST(v.vnr AS NVARCHAR(100)) AS vnr,
                               (SELECT MAX(ts) FROM (VALUES
                                   (v.dw_timestamp_update),
                                   (va.dw_timestamp_update),
                                   (vg.dw_timestamp_update),
                                   (vt.dw_timestamp_update),
                                   (mo.dw_timestamp_update),
                                   (zz.dw_timestamp_update),
                                   (u1.dw_timestamp_update),
                                   (u2.dw_timestamp_update)
                               ) t(ts)) AS dw_timestamp_update
                        FROM [arc].[ils_vare] v
                        LEFT JOIN [arc].[ils_vareart]  va ON va.vareart  = v.vareart
                        LEFT JOIN [arc].[ils_varegrp]  vg ON vg.varegrp  = v.varegrp
                        LEFT JOIN [arc].[ils_varetype] vt ON vt.varetype = v.varetype
                        LEFT JOIN [arc].[ils_moms]     mo ON mo.momskd   = v.momskd
                        LEFT JOIN [arc].[ils_zz_oper]  zz ON zz.bd_init  = v.oprbruger
                        LEFT JOIN [arc].[ils_unspsc]   u1 ON u1.unspsc   = v.unspc
                        LEFT JOIN [arc].[ils_unspsc]   u2 ON u2.unspsc   = v.unspsc2
                        WHERE NULLIF(LTRIM(RTRIM(CAST(v.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND (v.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR va.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR vg.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR vt.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR mo.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR zz.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR u1.dw_timestamp_update > '{checkpoint_ILSVare}'
                            OR u2.dw_timestamp_update > '{checkpoint_ILSVare}')
                        UNION ALL
                        SELECT CAST(b.vnr AS NVARCHAR(100)), b.dw_timestamp_update
                        FROM [arc].[ils_lgrbeh] b
                        WHERE NULLIF(LTRIM(RTRIM(CAST(b.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND b.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        SELECT CAST(a.vnr AS NVARCHAR(100)), a.dw_timestamp_update
                        FROM [arc].[ils_afdlgrbeh] a
                        WHERE NULLIF(LTRIM(RTRIM(CAST(a.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND a.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        -- Par-tabellerne genererer ogsaa ILSVare-entiteter (symmetri
                        -- med Vare-aksen): et vnr kan optraede i par-kilderne uden
                        -- varestamme-raekke og skal stadig kunne bridges
                        SELECT CAST(s.vnr AS NVARCHAR(100)), s.dw_timestamp_update
                        FROM [arc].[ils_laftdstam] s
                        WHERE NULLIF(LTRIM(RTRIM(CAST(s.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND s.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        SELECT CAST(lg.vnr AS NVARCHAR(100)), lg.dw_timestamp_update
                        FROM [arc].[ils_lgrvare] lg
                        WHERE NULLIF(LTRIM(RTRIM(CAST(lg.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND lg.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        SELECT CAST(ik.vnr AS NVARCHAR(100)), ik.dw_timestamp_update
                        FROM [arc].[ils_indkfak] ik
                        WHERE NULLIF(LTRIM(RTRIM(CAST(ik.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND ik.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        SELECT CAST(ld.vnr AS NVARCHAR(100)), ld.dw_timestamp_update
                        FROM [arc].[ils_laftd] ld
                        WHERE NULLIF(LTRIM(RTRIM(CAST(ld.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND ld.dw_timestamp_update > '{checkpoint_ILSVare}'
                        UNION ALL
                        SELECT CAST(p.vnr AS NVARCHAR(100)), p.dw_timestamp_update
                        FROM [arc].[ils_pmdm] p
                        WHERE NULLIF(LTRIM(RTRIM(CAST(p.vnr AS NVARCHAR(100)))), '') IS NOT NULL
                          AND p.dw_timestamp_update > '{checkpoint_ILSVare}'
                    )
                    SELECT *
                    INTO [stg].[pre_Leverandoervare_ILSVare_delta_entiteter]
                    FROM (
                        SELECT clean_code_Kreditor,
                               clean_code_LeverandoerVarenummer,
                               CAST(NULL AS NVARCHAR(120)) AS clean_code_ILSVare,
                               CAST(NULL AS NVARCHAR(100)) AS vnr,
                               MAX(dw_timestamp_update) AS dw_timestamp_update
                        FROM vare_entities
                        WHERE clean_code_Kreditor IS NOT NULL AND clean_code_Kreditor <> ''
                          AND clean_code_LeverandoerVarenummer IS NOT NULL AND clean_code_LeverandoerVarenummer <> ''
                        GROUP BY clean_code_Kreditor, clean_code_LeverandoerVarenummer
                        UNION ALL
                        SELECT CAST(NULL AS NVARCHAR(200)),
                               CAST(NULL AS NVARCHAR(500)),
                               {_VNR_CC.format(col="vnr")},
                               vnr,
                               MAX(dw_timestamp_update)
                        FROM ilsvare_entities
                        GROUP BY vnr
                    ) u;
    """


@asset(
    key=AssetKey(["erp", "pre_stg_Leverandoervare_ILSVare_delta_entiteter"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["arc_ils_laftdstam"]),
        AssetKey(["arc_ils_laftd"]),
        AssetKey(["arc_ils_lgrvare"]),
        AssetKey(["arc_ils_indkfak"]),
        AssetKey(["arc_ils_pmdm"]),
        AssetKey(["arc_ils_lev"]),
        AssetKey(["arc_ils_vare"]),
        AssetKey(["arc_ils_lgrbeh"]),
        AssetKey(["arc_ils_afdlgrbeh"]),
        AssetKey(["arc_ils_vareart"]),
        AssetKey(["arc_ils_varegrp"]),
        AssetKey(["arc_ils_varetype"]),
        AssetKey(["arc_ils_moms"]),
        AssetKey(["arc_ils_unspsc"]),
        AssetKey(["arc_ils_zz_oper"]),
        AssetKey(["arc_mysupply_statistic_invoice_line"]),
        AssetKey(["arc_mysupply_statistic_invoice_header"]),
        AssetKey(["arc_mysupply_metadata"]),
    ],
    tags={"domain": "erp", "layer": "pre_staging", "entity": "leverandoervare_ilsvare"},
    description=(
        "Entity-delta hub: changed Vare entities (kreditor+lvnr; ILS bridge tables "
        "+ MySupply, filtered by checkpoint_Vare) and changed ILSVare entities "
        "(vnr; item master, stock and bridge tables, filtered by "
        "checkpoint_ILSVare). One row per changed entity."
    ),
)
def pre_stg_Leverandoervare_ILSVare_delta_entiteter(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_Leverandoervare_ILSVare_delta_entiteter",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    row_count = 0
    try:
        if config.mode == "full_load":
            checkpoint_ilsvare = checkpoint_vare = BEGINNING_OF_TIME
            context.log.info("full_load: begge hub-akser saettes til BEGINNING_OF_TIME")
        else:
            checkpoint_ilsvare, checkpoint_vare = get_checkpoints(dfm, context)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(_build_hub_sql(checkpoint_vare, checkpoint_ilsvare))
                cursor.execute(
                    "SELECT COUNT(*) FROM [stg].[pre_Leverandoervare_ILSVare_delta_entiteter]"
                )
                row_count = cursor.fetchone()[0]
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_leverandoervare_ilsvare_delta_ent
                    ON [stg].[pre_Leverandoervare_ILSVare_delta_entiteter](
                        clean_code_Kreditor, clean_code_LeverandoerVarenummer, clean_code_ILSVare, vnr
                    );
                """
                )
                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Leverandoervare_ILSVare_delta_entiteter",
            "row_count": row_count,
            "grain": "one row per changed entity (Vare XOR ILSVare)",
            "checkpoint_Vare": checkpoint_vare,
            "checkpoint_ILSVare": checkpoint_ilsvare,
            "mode": config.mode,
        }
    )
