# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: AftaleVarePris fact staging (Layer 1) — delta-drevet.

Backbone (spec 2026-08-31-prisloese-linjer-og-aftalelager): arc.ils_laftd
(vinder-dedup'et pr. (lnr, aftale, vnr)) LEFT JOIN arc.ils_laftdpris paa
linjenoeglerne — prisloese aftalelinjer (UdbAft med items, laftdstam-
alternativer, men INGEN laftdpris-raekker) faar dermed ogsaa en raekke,
med NULL priser og har_prisversion = 0 ("syntetisk" linje-daekningsraekke).
Linjer MED prisversioner faar én raekke pr. prisversion som foer
(har_prisversion = 1). bk_prrowid baerer kildepraefiks saa de to typer aldrig
kolliderer paa tvaers af tabeller: 'LAFTDPRIS_<prrowid>' (rigtig prisversion)
/ 'LAFTD_<prrowid>' (syntetisk raekke). arc.ils_laftdpris.prrowid og
arc.ils_laftd.prrowid er begge varchar(36) (Oracle-rowid som tekst) —
praefikset er derfor plain strengkonkatenering, ingen CONVERT noedvendig.

Alle prisversioner fra ILS laftdpris, fan'et ud pr. distinkt lvnr-CleanCode
(spec 2026-08-27-alternate-lvnr-rows): grain er (bk_prrowid, clean_code_AftaleVarePris)
— prisversion x lvnr (eller syntetisk-linje x lvnr for prisloese linjer).
Linjens lvnr-slots (laftdstam ##1-4 + laftd ##2-3) unpivotes via OUTER APPLY
paa prisversions-raekken, saa hver distinkt lvnr faar sin egen raekke med
provenans i kilder; linjer uden lvnr bevares som ÉN raekke med NULL clean_lvnr
(basisformat-CleanCode). INGEN pris-dedup: kildens historik bevares.
dw_valid_from kommer fra kildens start_/startklk (prisversion) hhv.
COALESCE(linje_start, '1900-01-01') (syntetisk raekke); dw_valid_to er
LEAD-kaedet til naeste prisversions dw_valid_from (kildens slut kun som
fallback paa sidste version) hhv. '9999-12-31' (syntetisk raekke) — ILS
lukker ikke slut naar en nyere version afloeser. dw_is_current er altid 1
paa en syntetisk raekke (linjen har ingen konkurrerende versioner).

Delta-kontrakt: delta-enheden er AFTALELINJEN (lnr, aftale, vnr). Checkpointet
laeses fra ejeren ("fct", "AftaleVarePris") via DfmManager. #delta_lines er
unionen af 7 signaler:
  1) laftdpris-aendringer (prisversioner)
  2) laftd-aendringer (backbone: maerke, lvnr##2/3, linje_start/slut + sent ankommet forael)
  3) laftdstam-aendringer (lvnr##1-4, clean_code_Leverandoervare)
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
from utils.sql.leverandoervare_ilsvare_sql import (
    LAFTD_ALT_LVNR_SLOTS,
    LAFTDSTAM_ALT_LVNR_SLOTS,
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
    -- 3) Stam-drift: lvnr##1-4, clean_code_Leverandoervare
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


def _lvnr_slot_values() -> str:
    """VALUES-raekker til lvnr-unpivot'en: (kilde-label, prioritet, raa kolonne).

    laftdstam_1 er den primaere (samme vinder-raekke som i dag); alternativerne
    foelger i slot-orden, laftd-slots til sidst. Prioriteten styrer kun
    STRING_AGG-ordenen i kilder.

    HOLDES I SYNC: pv.stam_lvnr{i}/pv.laftd_lvnr{i}-aliaserne genereret her
    refererer til den haandkodede pv-kolonneliste (stam_lvnr1-4, laftd_lvnr2-3)
    i pv-SELECT'en nedenfor — den er haandkodet 4+2, IKKE afledt af
    LAFTDSTAM_ALT_LVNR_SLOTS/LAFTD_ALT_LVNR_SLOTS. Udvides slot-listerne, skal
    pv-kolonnerne (og aliaserne) opdateres tilsvarende.
    """
    rows = ["('laftdstam_1', 1, pv.stam_lvnr1)"]
    for i, _slot in enumerate(LAFTDSTAM_ALT_LVNR_SLOTS, start=2):
        rows.append(f"('laftdstam_{i}', {i}, pv.stam_lvnr{i})")
    offset = 1 + len(LAFTDSTAM_ALT_LVNR_SLOTS)
    for i, _slot in enumerate(LAFTD_ALT_LVNR_SLOTS, start=2):
        rows.append(f"('laftd_{i}', {offset + i - 1}, pv.laftd_lvnr{i})")
    return ",\n                ".join(rows)


def _build_staging_sql(delta_join: str) -> str:
    """Hoved-SELECT i to lag (spec 2026-08-31-prisloese-linjer-og-aftalelager,
    bygger paa 2026-08-27-alternate-lvnr-rows):

    pv  — linje/prisversions-aksen: backbone er arc.ils_laftd (vinder-dedup'et
          pr. (lnr, aftale, vnr)) LEFT JOIN arc.ils_laftdpris paa linjenoeglerne
          — prisloese linjer faar dermed ogsaa en raekke (har_prisversion = 0).
          ALLE window-funktioner (dw_is_current, LEAD-kaedet dw_valid_to) ligger
          her, guardet eksplicit paa p.prrowid IS NULL/IS NOT NULL. Vinduerne
          SKAL ligge her: efter fan-out'en ville LEAD pege paa samme versions
          soester-lvnr-raekke og dw_is_current kun ramme EN lvnr-raekke pr. linje.
    lv  — lvnr-aksen: OUTER APPLY unpivoter linjens slots (stam ##1-4 +
          laftd ##2-3), normaliserer, dedupper pr. distinkt lvnr og aggregerer
          kilder. OUTER APPLY bevarer no-lvnr-linjer som EN raekke med
          NULL clean_lvnr (basisformat-CleanCode, dw_ek_Leverandoervare -1).

    Grain: (bk_prrowid, clean_code_AftaleVarePris) — (prisversion ELLER
    syntetisk linje-daekningsraekke) x lvnr. bk_prrowid baerer kildepraefiks:
    'LAFTDPRIS_<prrowid>' (rigtig prisversion) / 'LAFTD_<prrowid>' (syntetisk).
    delta_join gates backbone (laftd-siden, alias d) i delta-mode.
    """
    return f"""
        SELECT
            -- Forretningsnoegler
            pv.bk_prrowid,
            pv.har_prisversion,
            pv.bk_leverandoernummer,
            pv.bk_aftale_nummer,
            pv.bk_ils_varenummer,

            -- CleanCodes til map-opslag. Entiteten er (aftalelinje x lvnr):
            -- _LEVVARE_-suffikset holder dw_ek_AftaleVarePris 1:1 med
            -- (aftalelinje x lvnr)-entiteten (ingen versionskomponent i cc);
            -- no-lvnr-raekken beholder basisformatet.
            pv.clean_code_Aftale,
            pv.clean_code_AftaleVarePris_base
                + CASE WHEN lv.clean_lvnr IS NOT NULL
                       THEN '_LEVVARE_' + lv.clean_lvnr
                       ELSE '' END AS clean_code_AftaleVarePris,
            pv.clean_code_Kreditor,
            CASE
                WHEN pv.clean_code_Kreditor IS NOT NULL AND lv.clean_lvnr IS NOT NULL
                THEN pv.clean_code_Kreditor + '_LEVVARE_' + lv.clean_lvnr
                ELSE NULL
            END AS clean_code_Leverandoervare,
            pv.IlsVare_CleanCode,

            -- Slot-provenans for raekkens lvnr (NULL paa no-lvnr-raekken).
            -- laftdstam_1 markerer den primaere — er_primaer-flag er bevidst
            -- fravalgt som redundant med kilder (jf. spec).
            lv.kilder,

            -- Priser (alle 4 trin)
            pv.pris1, pv.pris2, pv.pris3, pv.pris4,
            pv.pakkestoerrelse1, pv.pakkestoerrelse2, pv.pakkestoerrelse3, pv.pakkestoerrelse4,
            pv.momskode,

            -- Linjeattributter
            pv.maerke,
            pv.linje_start,
            pv.linje_slut,

            -- Gyldighed (beregnet paa prisversions-aksen i pv)
            pv.dw_valid_from,
            pv.dw_valid_to,
            pv.source_aktiv,
            pv.dw_is_current

        INTO [stg].[fct_AftaleVarePris_ils]
        FROM (
        SELECT
            CASE WHEN p.prrowid IS NOT NULL
                 THEN 'LAFTDPRIS_' + p.prrowid
                 ELSE 'LAFTD_' + d.prrowid
            END AS bk_prrowid,
            CAST(CASE WHEN p.prrowid IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS har_prisversion,
            UPPER(LTRIM(RTRIM(CAST(d.lnr AS NVARCHAR(100))))) AS bk_leverandoernummer,
            UPPER(LTRIM(RTRIM(REPLACE(CAST(d.aftale AS NVARCHAR(100)), ' ', '')))) AS bk_aftale_nummer,
            UPPER(LTRIM(RTRIM(REPLACE(CAST(d.vnr AS NVARCHAR(100)), ' ', '')))) AS bk_ils_varenummer,

            'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(d.lnr AS NVARCHAR(100))))) + '_'
                + UPPER(LTRIM(RTRIM(REPLACE(CAST(d.aftale AS NVARCHAR(100)), ' ', '')))) AS clean_code_Aftale,
            'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(d.lnr AS NVARCHAR(100))))) + '_'
                + UPPER(LTRIM(RTRIM(REPLACE(CAST(d.aftale AS NVARCHAR(100)), ' ', ''))))
                + '_VARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(d.vnr AS NVARCHAR(100)), ' ', '')))) AS clean_code_AftaleVarePris_base,
            CASE
                WHEN lev.se_nr IS NOT NULL AND LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))) <> ''
                THEN 'CVR_' + CASE WHEN REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') LIKE 'DK[0-9]%' THEN SUBSTRING(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''), 3, LEN(REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', ''))) ELSE REPLACE(LTRIM(RTRIM(CAST(lev.se_nr AS NVARCHAR(100)))), ' ', '') END
                ELSE NULL
            END AS clean_code_Kreditor,
            'ILSVARE_' + UPPER(LTRIM(RTRIM(REPLACE(CAST(d.vnr AS NVARCHAR(100)), ' ', '')))) AS IlsVare_CleanCode,

            -- Priser (alle 4 trin) — NULL paa syntetisk raekke (p er LEFT JOIN'et)
            p.[pris##1] AS pris1,
            p.[pris##2] AS pris2,
            p.[pris##3] AS pris3,
            p.[pris##4] AS pris4,
            p.[pakstr##1] AS pakkestoerrelse1,
            p.[pakstr##2] AS pakkestoerrelse2,
            p.[pakstr##3] AS pakkestoerrelse3,
            p.[pakstr##4] AS pakkestoerrelse4,
            vare.momskd AS momskode,

            -- Raa lvnr-slots til fan-out'en (vinder-raekkerne; unpivotes i lv)
            stam.[lvnr##1] AS stam_lvnr1,
            stam.[lvnr##2] AS stam_lvnr2,
            stam.[lvnr##3] AS stam_lvnr3,
            stam.[lvnr##4] AS stam_lvnr4,
            d.[lvnr##2] AS laftd_lvnr2,
            d.[lvnr##3] AS laftd_lvnr3,

            d.maerke AS maerke,
            d.[start_] AS linje_start,
            d.slut AS linje_slut,

            -- dw_valid_from: kildeleveret start (startklk er sekvens-tiebreaker
            -- samme dag) naar linjen har en prisversion; ellers linjens egen
            -- start (spec 2026-08-31) — syntetisk raekke har intet startklk.
            CASE
                WHEN p.prrowid IS NOT NULL THEN
                    DATEADD(SECOND, COALESCE(p.startklk, 0),
                        CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0)))
                ELSE CAST(COALESCE(d.[start_], '1900-01-01') AS DATETIME2(0))
            END AS dw_valid_from,

            -- dw_valid_to: KAEDET, ikke kildeleveret. ILS lukker ikke slut naar
            -- en nyere prisversion afloeser — LEAD til naeste versions start
            -- vinder ubetinget; kildens slut kun som fallback paa sidste
            -- version. Lukket-aabent interval: >= from AND < to. Syntetisk
            -- raekke (ingen prisversion) faar aabent interval ('9999-12-31') —
            -- LEAD-vinduet nedenfor gaelder KUN linjer med prisversioner (en
            -- linje har enten prisversioner ELLER en syntetisk raekke, aldrig
            -- begge, saa NULL-partitionen paa tvaers af syntetiske raekker
            -- paavirker aldrig de rigtige prisversioners LEAD-vaerdi).
            CASE
                WHEN p.prrowid IS NULL THEN CAST('9999-12-31' AS DATETIME2(0))
                ELSE COALESCE(
                LEAD(DATEADD(SECOND, COALESCE(p.startklk, 0),
                    CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0)))) OVER (
                    PARTITION BY p.lnr, p.aftale, p.vnr
                    ORDER BY DATEADD(SECOND, COALESCE(p.startklk, 0),
                        CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0))), p.prrowid
                ),
                CAST(COALESCE(p.slut, '9999-12-31') AS DATETIME2(0))
            )
            END AS dw_valid_to,
            -- NULL paa syntetiske raekker med vilje: kolonnen betyder
            -- prisversions-aktiv, ikke linje-aktiv (p er LEFT JOIN'et).
            CAST(p.aktiv AS BIT) AS source_aktiv,

            -- dw_is_current: nyeste ikrafttraadte prisversion pr. aftalelinje
            -- (NULL start_ behandles som 1900-01-01, jf. dw_valid_from-fallback).
            -- Korrekt i delta-mode fordi ALLE linjens versioner medtages via #delta_lines.
            -- Syntetisk raekke (ingen prisversion) er altid dw_is_current = 1 —
            -- linjen har ingen konkurrerende versioner at tabe til.
            -- ORDER BY er BEVIDST identisk med LEAD-kaeden (modsat retning: DESC
            -- her vs. ASC i LEAD) + prrowid-tiebreaker (ikke startklk) — de to
            -- vinduer maa aldrig kunne uenes om hvilken version der er sidst.
            CASE
                WHEN p.prrowid IS NULL THEN 1
                WHEN CAST(COALESCE(p.[start_], '1900-01-01') AS DATE) <= CAST(GETDATE() AS DATE)
                     AND ROW_NUMBER() OVER (
                         PARTITION BY
                             p.lnr, p.aftale, p.vnr,
                             CASE WHEN CAST(COALESCE(p.[start_], '1900-01-01') AS DATE) <= CAST(GETDATE() AS DATE) THEN 1 ELSE 0 END
                         ORDER BY DATEADD(SECOND, COALESCE(p.startklk, 0),
                             CAST(COALESCE(p.[start_], '1900-01-01') AS DATETIME2(0))) DESC, p.prrowid DESC
                     ) = 1
                THEN 1
                ELSE 0
            END AS dw_is_current

        -- NULL-noegle-vagt (review-fund efter backbone-flip): lnr/aftale/vnr er
        -- NULL-able i arc.ils_laftd. Den gamle backbone (p INNER JOIN d) udelukkede
        -- strukturelt NULL-noegle-linjer, fordi de aldrig kunne matche en
        -- laftdpris-raekke. Nu hvor d er venstre side, skal filteret genindfoeres
        -- eksplicit — ellers giver en NULL-vnr-linje en syntetisk raekke med NULL
        -- clean_code_AftaleVarePris, som map-opslaget dropper og applierens
        -- fail-fast derefter fejler paa. Placeres FOER ROW_NUMBER: en NULL-noegle-
        -- raekke kan aldrig vaere en legitim vinder af noget.
        FROM (
            SELECT *
            FROM (
                SELECT
                    d.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.lnr, d.aftale, d.vnr
                        ORDER BY d.sysdato DESC, d.prrowid DESC
                    ) AS rn
                FROM [arc].[ils_laftd] d
                WHERE d.lnr IS NOT NULL AND d.aftale IS NOT NULL AND d.vnr IS NOT NULL
            ) ranked
            WHERE rn = 1
        ) d
        {delta_join}

        -- Backbone flippet (spec 2026-08-31): laftd er nu venstre side, saa
        -- prisloese linjer (ingen laftdpris-raekker) faar ogsaa en raekke.
        -- p.* er NULL paa disse raekker — alle p.*-udtryk ovenfor er derfor
        -- eksplicit CASE-guardet paa p.prrowid.
        LEFT JOIN [arc].[ils_laftdpris] p
            ON p.lnr = d.lnr AND p.aftale = d.aftale AND p.vnr = d.vnr

        -- INNER JOIN med vilje: aftalelinjer uden aftale-hoved (lafth)
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
            ON h.lnr = d.lnr AND h.aftale = d.aftale

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
            ON lev.lnr = d.lnr

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
        ) stam ON stam.lnr = d.lnr AND stam.aftale = d.aftale AND stam.vnr = d.vnr

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
        ) vare ON vare.vnr = d.vnr
        ) pv

        -- lvnr-fan-out: unpivot linjens slots, normaliser (LVNR_CC-reglen),
        -- dedup pr. distinkt lvnr og aggreger kilder. OUTER APPLY bevarer
        -- no-lvnr-linjer som EN raekke med NULL clean_lvnr.
        OUTER APPLY (
            SELECT norm.clean_lvnr,
                   STRING_AGG(slots.kilde, ',') WITHIN GROUP (ORDER BY slots.prioritet) AS kilder
            FROM (VALUES
                {_lvnr_slot_values()}
            ) slots(kilde, prioritet, lvnr_raw)
            CROSS APPLY (SELECT UPPER(LTRIM(RTRIM(REPLACE(slots.lvnr_raw, ' ', '')))) AS clean_lvnr) norm
            WHERE slots.lvnr_raw IS NOT NULL AND LTRIM(RTRIM(slots.lvnr_raw)) <> ''
            GROUP BY norm.clean_lvnr
        ) lv;
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
        "Prisversioner (+ prisloese linjers syntetiske daekningsraekker) pr. aftalelinje "
        "fra ILS laftd LEFT JOIN laftdpris — delta-drevet paa aftalelinje-granularitet "
        "via fct.AftaleVarePris-checkpointet, ingen pris-dedup"
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
    stats = (0, 0, 0, 0, 0)

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
                        "ON dl.lnr = d.lnr AND dl.aftale = d.aftale AND dl.vnr = d.vnr"
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
                    f"Created stg.fct_AftaleVarePris_ils with {row_count} rows "
                    "(prisversion x lvnr-fan-out)"
                )

                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_fct_aftalevarepris_ils_prrowid
                    ON [stg].[fct_AftaleVarePris_ils](bk_prrowid);
                """
                )
                # Grain-vagt: unikt pr. (prisversion x lvnr). NONCLUSTERED fordi
                # NVARCHAR(500)-noeglen overskrider clustered-graensen paa 900 bytes.
                cursor.execute(
                    """
                    CREATE UNIQUE NONCLUSTERED INDEX IX_fct_aftalevarepris_ils_grain
                    ON [stg].[fct_AftaleVarePris_ils](bk_prrowid, clean_code_AftaleVarePris);
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
                        COUNT(DISTINCT clean_code_AftaleVarePris) AS distinct_fanout_entiteter,
                        COUNT(clean_code_Leverandoervare) AS has_vare_cleancode,
                        SUM(CASE WHEN har_prisversion = 0 THEN 1 ELSE 0 END) AS synthetic_rows
                    FROM [stg].[fct_AftaleVarePris_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Prisversion x lvnr-raekker: {stats[0]} total, {stats[1]} dw_is_current, "
                    f"{stats[2]} distinkte (aftalelinje x lvnr)-entiteter, "
                    f"{stats[3]} med vare-CleanCode, {stats[4]} syntetiske (prisloese) raekker"
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
            "source_tables": "arc.ils_laftd + arc.ils_laftdpris + arc.ils_lafth + arc.ils_laftdstam + arc.ils_lev + arc.ils_vare",
            "grain": "bk_prrowid x clean_code_AftaleVarePris (prisversion-eller-syntetisk-linje x lvnr)",
            "backbone": "arc.ils_laftd LEFT JOIN arc.ils_laftdpris",
            "checkpoint_filter": checkpoint if checkpoint else "none (full load)",
            "delta_lines": delta_line_count if delta_line_count is not None else "n/a (full load)",
            "current_rows": stats[1],
            "distinct_fanout_entiteter": stats[2],
            "synthetic_rows": stats[4],
            "layer": "stg_fct (Layer 1)",
            "mode": config.mode,
        }
    )
