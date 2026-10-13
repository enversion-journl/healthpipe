# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: bridge_AftaleLager (aftale x lager/sygehus-styring)

Full rebuild hver koersel fra [arc].[ils_lafthsgh] ("Benyttes af" i ILS) — ingen
checkpoint, lafthsgh er en lille full-load styringstabel og delta-maskineri
er ikke besvaeret vaerd (spec 2026-08-31-prisloese-linjer-og-aftalelager).

Grain: en raekke pr. (lnr, aftale, sgh), deduplikeret paa nyeste
sysdato/prrowid DESC (partitioneret paa de NORMALISEREDE noegler — samme
UPPER+REPLACE-space-normalisering som den gemte clean_code_Aftale/sgh_raw
under CI-collation, ikke blot LTRIM/RTRIM — saa dedup-graenserne matcher de
gemte noeglers faktiske ligheds-graenser: to raekker der kun adskiller sig
ved case eller indlejrede mellemrum maa ikke begge vinde rn=1). `sgh` er
kildens sammensatte sygehus+lager-noegle og splittes her i sygehus (2 tegn
ved praefiks '55', ellers 4 tegn) + lager (NULL = hele sygehuset deltager,
ingen lager-specifik styring — clean_code_Lager falder da tilbage til
codesystemets SGH_{sygehus}-koncept, saa dw_ek_Lager ogsaa resolver paa
sygehus-niveau).

Kendt begraensning: arc.ils_lafthsgh er upsert-only (detect_deletes=False,
se DSI/mssql/archive/ils_archive_configs.py's _full_load_config). En raekke
der fjernes i ILS forsvinder derfor IKKE af sig selv fra arc — og dermed
heller ikke fra bridge.AftaleLager eller fra dim.Aftale.alle_maa_benytte
(som forbliver 0). Kraever manuel arc-oprydning eller delete-detektion paa
arkivet for at rette op paa ophaevet styring.
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


def _build_staging_sql() -> str:
    """Full rebuild af stg.bridge_AftaleLager fra arc.ils_lafthsgh.

    sgh-split (spec Section B, verbatim): sygehus = 2 tegn naar praefiks
    '55', ellers 4 tegn; lager = resten naar LEN(sgh) > 4, ellers NULL.
    clean_code_Aftale bruger samme udtryk som stg_fct_AftaleVarePris_ils.

    clean_code_Lager: den KANONISKE map.Lager-noegle er IKKE
    pre_stg_Vareudlevering_ils's bare 'LGR_'-udtryk (det er dead code) —
    den er 'SGH_{sgh}_LGR_{lgr}'-kompositten som Lager-codesystemet bygger
    (DSA/CodeSystem/lager/lager.py's build_lager_hierarchy, ~linje 69:
    `lgr_code = f"SGH_{sgh}_LGR_{lgr}"`) og som stg_dim_Lager.py saetter
    UAENDRET som clean_code_Lager (kolonnen 'code' fra
    codesystem.LagerConcept, som IGEN er byggekildens 'CODE'-vaerdi).
    Normaliseringen i lager.py's _safe_strip() er KUN str(value).strip() —
    INGEN UPPER, INGEN zero-padding. Derfor bygges kompositten her fra de
    allerede LTRIM/RTRIM'ede sygehus/lager-splitvaerdier UDEN yderligere
    case- eller padding-aendring, for byte-for-byte at matche
    codesystemets noeglerum. En lager uden sgh-parent (skippes i
    lager.py naar der ikke findes en sgh_codes[sgh]-foraelder) resolver
    naturligt til -1 via applierens LEFT JOIN-fallback.
    """
    return """
    -- Grain: en raekke pr. (lnr, aftale, sgh), deduplikeret paa nyeste
    -- sysdato/prrowid DESC. PARTITION BY matcher den NORMALISEREDE noegle
    -- (UPPER + REPLACE-space for lnr/aftale, jf. clean_code_Aftale-udtrykket
    -- nedenfor; UPPER+trim for sgh, jf. sgh_raw) under CI-collation — ikke
    -- blot LTRIM/RTRIM. To raekker der kun adskiller sig ved case eller
    -- indlejrede mellemrum (fx 'AB 12' vs 'ab12') maa ikke begge vinde rn=1:
    -- de ville begge mappe til samme gemte, normaliserede noegle og kollidere
    -- i applierens unikke temp-indeks.
    DROP TABLE IF EXISTS [stg].[bridge_AftaleLager];

    SELECT
        clean_code_Aftale,
        sgh_raw,
        sygehus,
        lager,
        -- Sygehus-niveau-fallback: naar sgh ikke baerer et lager-led, resolves
        -- mod codesystemets SGH_{sygehus}-koncept (findes i map.Lager som
        -- hierarki-foraelder). dw_ek_Lager er dermed blandet granularitet —
        -- lager IS NULL paa raekken markerer sygehus-niveauet eksplicit.
        CASE WHEN lager IS NOT NULL
             THEN 'SGH_' + sygehus + '_LGR_' + lager
             ELSE 'SGH_' + sygehus
        END AS clean_code_Lager,
        maa_benyttes,
        foerste_prioritet
    INTO [stg].[bridge_AftaleLager]
    FROM (
        SELECT
            'AFTALE_' + UPPER(LTRIM(RTRIM(CAST(lnr AS NVARCHAR(100))))) + '_'
                + UPPER(LTRIM(RTRIM(REPLACE(CAST(aftale AS NVARCHAR(100)), ' ', '')))) AS clean_code_Aftale,
            LTRIM(RTRIM(sgh)) AS sgh_raw,
            CASE WHEN LEFT(LTRIM(RTRIM(sgh)), 2) = '55' THEN '55'
                 ELSE LEFT(LTRIM(RTRIM(sgh)), 4) END AS sygehus,
            CASE WHEN LEN(LTRIM(RTRIM(sgh))) > 4
                 THEN CASE WHEN LEFT(LTRIM(RTRIM(sgh)), 2) = '55'
                           THEN SUBSTRING(LTRIM(RTRIM(sgh)), 3, 20)
                           ELSE SUBSTRING(LTRIM(RTRIM(sgh)), 5, 20) END
                 ELSE NULL END AS lager,
            sw_benyt AS maa_benyttes,
            sw_prioritet AS foerste_prioritet,
            -- PARTITION BY matcher clean_code_Aftale/sgh_raw's faktiske
            -- normalisering byte-for-byte (ikke blot LTRIM/RTRIM): lnr faar
            -- UPPER+trim (INGEN REPLACE-space, ligesom clean_code_Aftale's
            -- lnr-led ovenfor), aftale faar UPPER+trim+REPLACE-space (ligesom
            -- clean_code_Aftale's aftale-led), sgh faar UPPER+trim (ligesom
            -- sgh_raw under CI-collation). At bruge REPLACE paa lnr her ville
            -- kollapse to raekker med forskellig lnr-whitespace til SAMME
            -- partition, selvom de mapper til FORSKELLIGE gemte
            -- clean_code_Aftale-noegler — og fejlagtigt droppe den ene.
            ROW_NUMBER() OVER (
                PARTITION BY
                    UPPER(LTRIM(RTRIM(CAST(lnr AS NVARCHAR(100))))),
                    UPPER(LTRIM(RTRIM(REPLACE(CAST(aftale AS NVARCHAR(100)), ' ', '')))),
                    UPPER(LTRIM(RTRIM(sgh)))
                ORDER BY sysdato DESC, prrowid DESC
            ) AS rn
        FROM [arc].[ils_lafthsgh]
        WHERE lnr IS NOT NULL AND LTRIM(RTRIM(lnr)) <> ''
          AND aftale IS NOT NULL AND LTRIM(RTRIM(aftale)) <> ''
          AND sgh IS NOT NULL AND LTRIM(RTRIM(sgh)) <> ''
    ) ranked
    WHERE rn = 1;
"""


@asset(
    key=AssetKey(["erp", "stg_bridge_AftaleLager"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_ils_lafthsgh"])],
    tags={
        "domain": "erp",
        "layer": "bridge",
        "source": "ils",
        "entity": "aftalelager",
    },
    description=(
        "Bridge staging: full rebuild af stg.bridge_AftaleLager fra arc.ils_lafthsgh "
        "(aftale x lager/sygehus-styring, 'Benyttes af'). Feeder bridge.AftaleLager."
    ),
)
def stg_bridge_AftaleLager(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Full-rebuild staging: dedupliceret lafthsgh-styring med sgh-split."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "bridge_AftaleLager", dagster_run_id=context.run_id, mode=config.mode
    )
    row_count = 0
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Building stg.bridge_AftaleLager (full rebuild)...")
                cursor.execute(_build_staging_sql())
                cursor.execute("SELECT COUNT(*) FROM [stg].[bridge_AftaleLager]")
                row_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_stg_bridge_AftaleLager
                    ON [stg].[bridge_AftaleLager](clean_code_Aftale, sgh_raw);
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
            "table": "stg.bridge_AftaleLager",
            "row_count": row_count,
            "source_table": "arc.ils_lafthsgh",
            "grain": "one row per (lnr, aftale, sgh), deduped on newest sysdato/prrowid DESC",
            "mode": config.mode,
        }
    )
