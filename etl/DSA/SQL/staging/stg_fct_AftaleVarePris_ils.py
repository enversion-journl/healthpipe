# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: AftaleVarePris fact staging (Layer 1) — delta-drevet.

Alle prisversioner fra ILS laftdpris — én række pr. (lnr, aftale, vnr, start_, startklk),
unikt identificeret ved bk_prrowid. INGEN pris-dedup: kildens historik bevares.
dw_valid_from/dw_valid_to kommer fra kildens start_/startklk/slut.

Delta-kontrakt: delta-enheden er AFTALELINJEN (lnr, aftale, vnr). Checkpointet
laeses fra ejeren ("fct", "AftaleVarePris") via DfmManager. #delta_lines er
unionen af 7 signaler:
  1) laftdpris-aendringer (backbone)
  2) laftd-aendringer (maerke, lvnr##2/3, linje_start/slut + sent ankommet forael)
  3) laftdstam-aendringer (kreditorvarenummer1, clean_code_Leverandoervare)
  4) lev-aendringer via lnr (CVR -> clean_code_Kreditor)
  5) vare-aendringer via vnr (momskode)
  6) lafth-aendringer (sent ankommet hoved aabner INNER JOIN-ekskluderede prisraekker)
  7) time-flip: prisversion traadt i kraft siden sidste checkpoint
     (start_ > checkpoint-dato AND start_ <= i dag) -> dw_is_current skal flippe

Checkpointet saettes ved fct-completion (SYSDATETIME), saa arc-raekker skrevet
mellem staging-laesning og checkpoint-avance kan foerst samles op af naeste
koersel — samme tradeoff som Budget/Postering-moenstret.

ALLE prisversioner for en beroert linje medtages, saa dw_is_current-vinduet
(nyeste ikrafttraadte version pr. linje) beregnes korrekt inden for linjen.

full_load-mode (RunModeConfig) ignorerer checkpointet og genopbygger alt;
manglende checkpoint (nyt miljoe) giver ligeledes fuld genopbygning.
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

# Delta-linje-detektion: DISTINCT (lnr, aftale, vnr) over de 7 signaler.
# Checkpointet bindes som parameter (?) — aldrig interpoleret.
DELTA_LINES_SQL = """
DECLARE @cp DATETIME2 = CAST(? AS DATETIME2);
-- 1 dags overlap: checkpointet stemples ved fct-completion og kan ligge
-- efter midnat ift. staging-koerslens GETDATE(); uden overlap kan en
-- version med start_ = checkpoint-datoen aldrig time-flippe.
DECLARE @cp_date DATE = CAST(DATEADD(DAY, -1, @cp) AS DATE);
DECLARE @today DATE = CAST(GETDATE() AS DATE);

DROP TABLE IF EXISTS #delta_lines;

SELECT DISTINCT x.lnr, x.aftale, x.vnr
INTO #delta_lines
FROM (
    -- 1) Backbone: nye/aendrede prisversioner
    SELECT p.lnr, p.aftale, p.vnr
    FROM [arc].[ils_laftdpris] p
    WHERE p.[dw_timestamp_update] > @cp

    UNION ALL
    -- 2) Linje-drift: maerke, lvnr##2/3, linje_start/slut + sent ankommet laftd
    SELECT d.lnr, d.aftale, d.vnr
    FROM [arc].[ils_laftd] d
    WHERE d.[dw_timestamp_update] > @cp

    UNION ALL
    -- 3) Stam-drift: kreditorvarenummer1, clean_code_Leverandoervare
    SELECT s.lnr, s.aftale, s.vnr
    FROM [arc].[ils_laftdstam] s
    WHERE s.[dw_timestamp_update] > @cp

    UNION ALL
    -- 4) Leverandoer-drift: CVR -> clean_code_Kreditor / clean_code_Leverandoervare
    SELECT d.lnr, d.aftale, d.vnr
    FROM [arc].[ils_laftd] d
    INNER JOIN [arc].[ils_lev] lev ON lev.lnr = d.lnr
    WHERE lev.[dw_timestamp_update] > @cp

    UNION ALL
    -- 5) Vare-drift: momskode
    SELECT d.lnr, d.aftale, d.vnr
    FROM [arc].[ils_laftd] d
    INNER JOIN [arc].[ils_vare] v ON v.vnr = d.vnr
    WHERE v.[dw_timestamp_update] > @cp

    UNION ALL
    -- 6) Header-drift: sent ankommet lafth aabner INNER JOIN-ekskluderede prisraekker
    SELECT d.lnr, d.aftale, d.vnr
    FROM [arc].[ils_laftd] d
    INNER JOIN [arc].[ils_lafth] h ON h.lnr = d.lnr AND h.aftale = d.aftale
    WHERE h.[dw_timestamp_update] > @cp

    UNION ALL
    -- 7) Time-flip: fremtidsdateret prisversion traadt i kraft siden sidste
    -- checkpoint (NULL start_ = 1900-sentinel og kan aldrig krydse ind i kraft)
    SELECT p.lnr, p.aftale, p.vnr
    FROM [arc].[ils_laftdpris] p
    WHERE CAST(p.[start_] AS DATE) > @cp_date
      AND CAST(p.[start_] AS DATE) <= @today
) x;

CREATE CLUSTERED INDEX IX_delta_lines ON #delta_lines(lnr, aftale, vnr);
"""


def _build_staging_sql(delta_join: str) -> str:
    """Hoved-SELECT — uaendret transformation; delta_join gates backbone i delta-mode."""
    return f"""
        SELECT
            -- Forretningsnoegler
            p.prrowid AS bk_prrowid,
            UPPER(LTRIM(RTRIM(CAST(p.lnr AS NVARCHAR(100))))) AS bk_leverandoernummer,
            UPPER(LTRIM(RTRIM(REPLACE(CAST(p.aftale AS NVARCHAR(100)), ' ', '')))) AS bk_aftale_nummer,
            UPPER(LTRIM(RTRIM(REPLACE(CAST(p.vnr AS NVARCHAR(100)), ' ', '')))) AS bk_ils_varenummer,

            -- CleanCodes til map-opslag
            'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(p.lnr AS NVARCHAR(100))))) + '_'
                + UPPER(LTRIM(RTRIM(REPLACE(CAST(p.aftale AS NVARCHAR(100)), ' ', '')))) AS clean_code_Aftale,
            'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(p.lnr AS NVARCHAR(100))))) + '_'
                + UPPER(LTRIM(RTRIM(REPLACE(CAST(p.aftale AS NVARCHAR(100)), ' ', ''))))
                + '_VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(p.vnr AS NVARCHAR(100)), ' ', '')))) AS clean_code_AftaleVarePris,
            CASE
                WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                ELSE NULL
            END AS clean_code_Kreditor,
            -- Vare-opslag: clean_code_Leverandoervare = {{CVR-clean}}_{{LEVVARE-clean}} (map.Leverandoervare PK)
            CASE
                WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                     AND stam.[lvnr##1] IS NOT NULL AND LTRIM(RTRIM(stam.[lvnr##1])) <> ''
                THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                     + '_LEVVARE_' + UPPER(LTRIM(RTRIM(REPLACE(stam.[lvnr##1], ' ', ''))))
                ELSE NULL
            END AS clean_code_Leverandoervare,
            'ILSVARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(p.vnr AS NVARCHAR(100)), ' ', '')))) AS IlsVare_CleanCode,

            -- Priser (alle 4 trin)
            p.[pris##1] AS pris1,
            p.[pris##2] AS pris2,
            p.[pris##3] AS pris3,
            p.[pris##4] AS pris4,
            p.[pakstr##1] AS pakkestoerrelse1,
            p.[pakstr##2] AS pakkestoerrelse2,
            p.[pakstr##3] AS pakkestoerrelse3,
            p.[pakstr##4] AS pakkestoerrelse4,
            vare.momskd AS momskode,

            -- Linjeattributter (flyttet fra dim.Leverandoervare)
            stam.[lvnr##1] AS kreditorvarenummer1,
            d.[lvnr##2] AS kreditorvarenummer2,
            d.[lvnr##3] AS kreditorvarenummer3,
            d.maerke AS maerke,
            d.[start_] AS linje_start,
            d.slut AS linje_slut,

            -- Kildeleveret gyldighed (startklk er sekvens-tiebreaker samme dag).
            -- NULL start_/slut i kilden falder tilbage til 1900-01-01/9999-12-31
            -- (kolonnerne er NOT NULL i fct.AftaleVarePris).
            DATEADD(SECOND, COALESCE(p.startklk, 0),
                CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0))) AS dw_valid_from,
            CAST(COALESCE(p.slut, '9999-12-31') AS DATETIME2(0)) AS dw_valid_to,
            CAST(p.aktiv AS BIT) AS source_aktiv,

            -- dw_is_current: nyeste ikrafttraadte prisversion pr. aftalelinje
            -- (NULL start_ behandles som 1900-01-01, jf. dw_valid_from-fallback).
            -- Korrekt i delta-mode fordi ALLE linjens versioner medtages via #delta_lines.
            CASE
                WHEN CAST(COALESCE(p.[start_], '1900-01-01') AS DATE) <= CAST(GETDATE() AS DATE)
                     AND ROW_NUMBER() OVER (
                         PARTITION BY
                             p.lnr, p.aftale, p.vnr,
                             CASE WHEN CAST(COALESCE(p.[start_], '1900-01-01') AS DATE) <= CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END
                         ORDER BY COALESCE(p.[start_], '1900-01-01') DESC, p.startklk DESC
                     ) = 1
                THEN 1
                ELSE 0
            END AS dw_is_current

        INTO [stg].[fct_AftaleVarePris_ils]
        FROM [arc].[ils_laftdpris] p
        {delta_join}

        -- arc.ils_laftd upsertes paa prrowid (Oracle-rowid), IKKE paa
        -- (lnr, aftale, vnr) — samme aftalelinje kan derfor optraede med flere
        -- rowids naar kilden har flere seqnr-versioner af linjen. seqnr er ILS'
        -- interne versionstaeller PR. TABEL (ikke en delt join-noegle med
        -- laftdpris: en hoejere seqnr er ikke noedvendigvis nyere), saa vi maa
        -- IKKE matche paa seqnr — vi dedup'er til én linje pr. (lnr, aftale, vnr).
        -- Attributterne er ens paa tvaers af versionerne; vi vaelger den senest
        -- kildeopdaterede (sysdato DESC), prrowid som deterministisk tiebreaker.
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    d.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.lnr, d.aftale, d.vnr
                        ORDER BY d.sysdato DESC, d.prrowid DESC
                    ) AS rn
                FROM [arc].[ils_laftd] d
            ) ranked
            WHERE rn = 1
        ) d
            ON d.lnr = p.lnr AND d.aftale = p.aftale AND d.vnr = p.vnr

        -- INNER JOIN med vilje: prisraekker uden aftale-hoved (lafth)
        -- er foraeldreloese og maa ikke naa map/dim-kaeden. arc.ils_lafth
        -- upsertes paa prrowid, saa flere rowids pr. (lnr, aftale) skal dedup'es
        -- (rn=1) — ingen kolonner laeses fra h, saa join'et er ren eksistens-gate
        -- og tiebreakeren (prrowid DESC) paavirker ikke output, kun fan-out.
        INNER JOIN (
            SELECT *
            FROM (
                SELECT
                    h.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY h.lnr, h.aftale
                        ORDER BY h.prrowid DESC
                    ) AS rn
                FROM [arc].[ils_lafth] h
            ) ranked
            WHERE rn = 1
        ) h
            ON h.lnr = p.lnr AND h.aftale = p.aftale

        -- arc.ils_lev upsertes paa prrowid, ikke lnr — dedup til én
        -- leverandoerraekke pr. lnr (foretraek non-tom se_nr, prrowid DESC
        -- som deterministisk tiebreaker) saa se_nr-opslaget ikke fan-out'er.
        LEFT JOIN (
            SELECT *
            FROM (
                SELECT
                    lev.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY lev.lnr
                        ORDER BY
                            CASE WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> '' THEN 0 ELSE 1 END,
                            lev.prrowid DESC
                    ) AS rn
                FROM [arc].[ils_lev] lev
            ) ranked
            WHERE rn = 1
        ) lev
            ON lev.lnr = p.lnr

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
        ) stam ON stam.lnr = p.lnr AND stam.aftale = p.aftale AND stam.vnr = p.vnr

        LEFT JOIN (
            SELECT *
            FROM (
                SELECT
                    v.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY v.vnr
                        ORDER BY v.oprdto DESC
                    ) AS rn
                FROM [arc].[ils_vare] v
            ) ranked
            WHERE rn = 1
        ) vare ON vare.vnr = p.vnr;
    """


@asset(
    key=AssetKey(["erp", "stg_fct_AftaleVarePris_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[
        AssetKey(["arc_ils_laftdpris"]),
        AssetKey(["arc_ils_laftd"]),
        AssetKey(["arc_ils_lafth"]),
        AssetKey(["arc_ils_laftdstam"]),
        AssetKey(["arc_ils_lev"]),
        AssetKey(["arc_ils_vare"]),
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "ils",
        "entity": "aftalevarepris",
    },
    description=(
        "Prisversioner pr. aftalelinje fra ILS laftdpris — delta-drevet paa "
        "aftalelinje-granularitet via fct.AftaleVarePris-checkpointet, ingen pris-dedup"
    ),
)
def stg_fct_AftaleVarePris_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Genopbyg stg.fct_AftaleVarePris_ils — delta (beroerte aftalelinjer) eller fuld."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "fct_AftaleVarePris_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
        checkpoint_from=("fct", "AftaleVarePris"),
    )

    checkpoint = run_ctx.checkpoint
    if config.mode == "full_load":
        checkpoint = None

    row_count = 0
    delta_line_count = None
    stats = (0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Creating staging table stg.fct_AftaleVarePris_ils...")

                if checkpoint is not None:
                    context.log.info(
                        f"Delta-mode: bygger #delta_lines med checkpoint > {checkpoint}"
                    )
                    delta_join = (
                        "INNER JOIN #delta_lines dl "
                        "ON dl.lnr = p.lnr AND dl.aftale = p.aftale AND dl.vnr = p.vnr"
                    )
                    # #delta_lines er en session-scoped temp-tabel: den maa
                    # oprettes og forbruges i SAMME batch, ellers droppes den
                    # foer hoved-SELECT'en kan laese den ("Invalid object
                    # name '#delta_lines'"). Derfor bygges ÉN samlet
                    # SQL-streng og udfoeres med ét cursor.execute-kald;
                    # checkpointet forbliver et bundet parameter (?), aldrig
                    # f-string-interpoleret.
                    combined_sql = (
                        "SET NOCOUNT ON;\n"
                        + DELTA_LINES_SQL
                        + "\nDROP TABLE IF EXISTS [stg].[fct_AftaleVarePris_ils];\n"
                        + _build_staging_sql(delta_join)
                        + "\nSELECT (SELECT COUNT(*) FROM #delta_lines) AS delta_lines, "
                        "(SELECT COUNT(*) FROM [stg].[fct_AftaleVarePris_ils]) AS row_count;"
                    )
                    cursor.execute(combined_sql, (checkpoint,))
                    delta_line_count, row_count = cursor.fetchone()
                    context.log.info(f"{delta_line_count} beroerte aftalelinjer i delta")
                else:
                    context.log.info("Intet checkpoint-filter — fuld genopbygning fra arkiverne")
                    delta_join = ""
                    cursor.execute("DROP TABLE IF EXISTS [stg].[fct_AftaleVarePris_ils];")
                    cursor.execute(_build_staging_sql(delta_join))
                    row_count = cursor.rowcount

                context.log.info(
                    f"Created stg.fct_AftaleVarePris_ils with {row_count} price version rows"
                )

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_fct_aftalevarepris_ils_bk_prrowid
                    ON [stg].[fct_AftaleVarePris_ils](bk_prrowid);
                """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_fct_aftalevarepris_ils_CleanCodes
                    ON [stg].[fct_AftaleVarePris_ils](clean_code_AftaleVarePris)
                    INCLUDE (clean_code_Aftale, clean_code_Kreditor, clean_code_Leverandoervare, IlsVare_CleanCode);
                """
                )

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        SUM(CAST(dw_is_current AS INT)) AS current_rows,
                        COUNT(DISTINCT clean_code_AftaleVarePris) AS distinct_aftalelinjer,
                        COUNT(clean_code_Leverandoervare) AS has_vare_cleancode
                    FROM [stg].[fct_AftaleVarePris_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Prisversioner: {stats[0]} total, {stats[1]} dw_is_current, "
                    f"{stats[2]} distinkte aftalelinjer, {stats[3]} med vare-CleanCode"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.fct_AftaleVarePris_ils",
            "row_count": row_count,
            "source_tables": "arc.ils_laftdpris + arc.ils_laftd + arc.ils_lafth + arc.ils_laftdstam + arc.ils_lev + arc.ils_vare",
            "grain": "lnr + aftale + vnr + start_ + startklk (bk_prrowid)",
            "checkpoint_filter": checkpoint if checkpoint else "none (full load)",
            "delta_lines": delta_line_count if delta_line_count is not None else "n/a (full load)",
            "current_rows": stats[1],
            "distinct_aftalelinjer": stats[2],
            "layer": "stg_fct (Layer 1)",
            "mode": config.mode,
        }
    )
