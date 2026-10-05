# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Postering (Finans) from SydAX (Base Layer)

Reconstructs the enriched Finanspostering row set directly from the
``arc.sydax_*`` archive tables — replacing the previous dependency on the
DSI-built ``ext.Finanspostering_enriched`` JOIN. The multi-table enrichment
(Kreditortransaktion via the relation bridge, Kreditor, and the full
Finanskonto → hierarchy chain) is performed here, then the same transforms the
old enrichment applied (CleanCode generation, type casts, COALESCE hierarchy
texts, Omkostningssted resolution, business-key aliases) are layered on top so
the output schema of ``stg.pre_Postering_sydax`` stays byte-identical.

Archive dedup:
    SydAX hierarchy archives are SCD2-style versioned (StartDateTime/EndDateTime,
    no dw_is_deleted). Each is collapsed to its current version per business key
    via ROW_NUMBER OVER (PARTITION BY <bk> ORDER BY [EndDateTime] DESC,
    [StartDateTime] DESC, [dw_timestamp_update] DESC) = 1 — matching the legacy
    enrichment's EndDateTime DESC version selector. The fact archive
    (arc.sydax_finanspostering) carries one row per (Kildesystem,
    ref_ledgertrans_id) and is only delta-filtered.

Delta filter:
    The fact archive is delta-filtered by the MIN of the downstream dim/fct
    checkpoints (DfmManager). full_load mode ignores the filter and fully
    reconstructs the table. pre_stg is NOT a checkpoint owner.

Grain: ref_ledgertrans_id (unique BIGINT). Business key: bk_ref_ledgertrans_id.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

POSTERING_SYDAX_KILDESYSTEM = "SydAX"


@asset(
    key=AssetKey(["erp", "pre_stg_Postering_sydax"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[
        "arc_sydax_finanspostering",
        "arc_sydax_finanspostering_kreditortrans_relation",
        "arc_sydax_kreditortransaktion",
        "arc_sydax_kreditor",
        "arc_sydax_finanskonto",
        "arc_sydax_finanskonto_omkostningssted",
        "arc_sydax_omkostningssted_hierarki",
        "arc_sydax_hovedkonto",
        "arc_sydax_gruppering",
        "arc_sydax_gruppering_autoriseret",
        "arc_sydax_art",
        "arc_sydax_funktion",
        "arc_sydax_dranst",
    ],
    tags={
        "domain": "erp",
        "layer": "stg_fct",
        "source": "sydax",
        "entity": "postering",
    },
    description=(
        "Reconstruct enriched postering from arc.sydax_* archives (replaces the "
        "DSI ext.Finanspostering_enriched JOIN). Delta-filtered by downstream "
        "dim/fct checkpoints. Feeds dim.Postering and fct.Postering."
    ),
)
def pre_stg_Postering_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Reconstruct stg.pre_Postering_sydax from arc.sydax_* archives."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_Postering_sydax",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        cps = [
            dfm.get_checkpoint("dim", "Postering"),
            dfm.get_checkpoint("fct", "Postering"),
        ]
        known = [c for c in cps if c]
        min_checkpoint = min(known) if known else None
        if config.mode == "full_load":
            min_checkpoint = None

        post_filter = (
            f"AND post.[dw_timestamp_update] > '{min_checkpoint}'" if min_checkpoint else ""
        )
        if min_checkpoint is not None:
            context.log.info(f"Checkpoint filter: post.dw_timestamp_update > {min_checkpoint}")
        else:
            context.log.info("No checkpoint filter — full reconstruction from archives")

        build_sql = _build_reconstruction_sql(post_filter)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Reconstructing stg.pre_Postering_sydax from arc.sydax_* archives..."
                )
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Postering_sydax];")
                cursor.execute(build_sql)
                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_pre_postering_sydax_PK
                    ON [stg].[pre_Postering_sydax](ref_ledgertrans_id);
                    """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_postering_sydax_CC_Postering
                    ON [stg].[pre_Postering_sydax](clean_code_Postering);
                    """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_postering_sydax_CC_Kontoplan
                    ON [stg].[pre_Postering_sydax](clean_code_Kontoplan);
                    """
                )
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[pre_Postering_sydax];")
                row_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(clean_code_Postering) AS has_cc_postering,
                        COUNT(clean_code_Kontoplan) AS has_cc_kontoplan,
                        COUNT(clean_code_Kreditor) AS has_cc_kreditor,
                        COUNT(kontonummer) AS has_kontonummer,
                        COUNT(omkostningsstednummer) AS has_omkostningssted
                    FROM [stg].[pre_Postering_sydax]
                    """
                )
                stats = cursor.fetchone()
            finally:
                cursor.close()

        context.log.info(
            f"Coverage: {stats[1]}/{stats[0]} CC_Postering, "
            f"{stats[2]}/{stats[0]} CC_Kontoplan, {stats[3]}/{stats[0]} CC_Kreditor, "
            f"{stats[4]}/{stats[0]} kontonummer, {stats[5]}/{stats[0]} Omkostningssted"
        )

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Postering_sydax",
            "target_table": "stg.pre_Postering_sydax",
            "row_count": row_count,
            "source_table": "arc.sydax_finanspostering (+12 enrichment archives)",
            "grain": "ref_ledgertrans_id",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
            "layer": "stg_fct (Layer 0)",
        }
    )


def _build_reconstruction_sql(post_filter: str) -> str:
    """Reconstruct the enriched postering set from arc.sydax_* archives.

    Backbone (delta-filtered, no version-collapse): arc.sydax_finanspostering ->
    relation bridge -> arc.sydax_kreditortransaktion. Kreditor + the full
    Finanskonto -> hierarchy chain are SCD-collapsed to their current version
    (EndDateTime DESC) — the hierarchy CTEs are identical to the proven
    pre_stg_Finanskladdelinje_sydax reconstruction.
    """
    return f"""
        ;WITH post_base AS (
            -- Fact grain: one row per (Kildesystem, ref_ledgertrans_id); delta-filtered.
            SELECT *
            FROM [arc].[sydax_finanspostering] post
            WHERE post.[Kildesystem] = '{POSTERING_SYDAX_KILDESYSTEM}'
              {post_filter}
        ),
        rel_dedup AS (
            -- Relation bridge keyed (Kildesystem, ref_ledgertrans_id, KreditortransaktionId).
            -- Already one row per combo; pick current version defensively.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [Kildesystem], [ref_LedgertransId], [KreditortransaktionId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_finanspostering_kreditortrans_relation]
        ),
        trans_dedup AS (
            -- Joined on KreditortransaktionId (non-key in arc); collapse to current.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [KreditortransaktionId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_kreditortransaktion]
            WHERE [KreditortransaktionId] IS NOT NULL
        ),
        kred_dedup AS (
            -- SCD-versioned; collapse to current version per kreditor_id.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [KreditorId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_kreditor]
        ),
        konto_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanskontoId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_finanskonto]
        ),
        omkost_link_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanskontoOmkostningsstedid]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_finanskonto_omkostningssted]
        ),
        omk_hier_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [OmkostningsstedHierarkiId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_omkostningssted_hierarki]
        ),
        hk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_hovedkonto]
            WHERE [ref_RECID] IS NOT NULL
        ),
        grp_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_gruppering]
            WHERE [RECID] IS NOT NULL
        ),
        grp_aut_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_gruppering_autoriseret]
        ),
        art_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_art]
            WHERE [RECID] IS NOT NULL
        ),
        funk_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [F1], [F2], [F3]
                       ORDER BY [Modified] DESC, [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_funktion]
        ),
        dr_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_dranst]
        )

        SELECT
            -- Grain + business key
            CAST(post.[ref_LedgertransId] AS BIGINT) AS ref_ledgertrans_id,
            CAST(post.[ref_LedgertransId] AS BIGINT) AS bk_ref_ledgertrans_id,

            -- clean_code_Postering: 'SYDAX_' + ref_ledgertrans_id (matches legacy)
            CASE
                WHEN post.[Kildesystem] IS NOT NULL AND post.[ref_LedgertransId] IS NOT NULL
                THEN UPPER(LTRIM(RTRIM(REPLACE(CAST(post.[Kildesystem] AS NVARCHAR(100)), ' ', '')))) + '_' +
                     CAST(post.[ref_LedgertransId] AS NVARCHAR(50))
                WHEN post.[ref_LedgertransId] IS NOT NULL
                THEN CAST(post.[ref_LedgertransId] AS NVARCHAR(50))
                ELSE NULL
            END AS clean_code_Postering,

            CAST(trans.[ref_VendTransId] AS BIGINT) AS ref_vend_trans_id,

            -- clean_code_Kreditor: 'CVR_' + juridisk_enhed (strip DK prefix)
            CASE
                WHEN kred.[JuridiskEnhed] IS NOT NULL AND LTRIM(RTRIM(kred.[JuridiskEnhed])) <> ''
                THEN 'CVR_' + CASE WHEN LTRIM(RTRIM(kred.[JuridiskEnhed])) LIKE 'DK[0-9]%'
                                   THEN SUBSTRING(LTRIM(RTRIM(kred.[JuridiskEnhed])), 3, LEN(LTRIM(RTRIM(kred.[JuridiskEnhed]))))
                                   ELSE LTRIM(RTRIM(kred.[JuridiskEnhed])) END
                ELSE NULL
            END AS clean_code_Kreditor,

            -- clean_code_Kontoplan from the posting's own finanskonto_id
            'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                CAST(post.[FinanskontoId] AS NVARCHAR(100)), ' ', '')
            ))) AS clean_code_Kontoplan,

            -- clean_code_Faktura: 'FAKTURA_' + Fakturanummer
            CASE
                WHEN trans.[Fakturanummer] IS NOT NULL AND LTRIM(RTRIM(trans.[Fakturanummer])) <> ''
                THEN 'FAKTURA_' + UPPER(LTRIM(RTRIM(REPLACE(trans.[Fakturanummer], ' ', ''))))
                ELSE NULL
            END AS clean_code_Faktura,

            CAST(kred.[KreditorId] AS BIGINT) AS kreditor_id,
            CAST(kred.[KreditorId] AS BIGINT) AS bk_kreditor_id,
            kred.[Kreditornummer] AS kreditornummer,
            kred.[Kreditornavn] AS kreditornavn,
            kred.[JuridiskEnhed] AS juridisk_enhed,

            CAST(post.[FinanskontoId] AS BIGINT) AS finanskonto_id,
            post.[FinanskontoId] AS bk_finanskonto_id,
            konto.[Kontonummer] AS kontonummer,

            CAST(hk.[F1] AS NVARCHAR(50)) AS f1,
            CAST(hk.[F2] AS NVARCHAR(50)) AS f2,
            CAST(hk.[F3] AS NVARCHAR(50)) AS f3,
            CAST(hk.[D1] AS NVARCHAR(50)) AS d1,
            CAST(hk.[G1] AS NVARCHAR(50)) AS g1,

            CAST(omk_hier.[nummer1] AS NVARCHAR(50)) AS s1,
            CAST(omk_hier.[nummer2] AS NVARCHAR(50)) AS s2,
            CAST(omk_hier.[nummer3] AS NVARCHAR(50)) AS s3,
            CAST(omk_hier.[nummer4] AS NVARCHAR(50)) AS s4,
            CAST(omk_hier.[nummer5] AS NVARCHAR(50)) AS s5,
            CAST(omk_hier.[nummer6] AS NVARCHAR(50)) AS s6,
            CAST(omk_hier.[nummer7] AS NVARCHAR(50)) AS s7,
            CAST(omk_hier.[nummer8] AS NVARCHAR(50)) AS S8,
            CAST(omk_hier.[nummer9] AS NVARCHAR(50)) AS S9,
            CAST(omk_hier.[nummer10] AS NVARCHAR(50)) AS S10,
            CAST(omk_hier.[nummer11] AS NVARCHAR(50)) AS S11,
            CAST(omk_hier.[nummer12] AS NVARCHAR(50)) AS S12,
            CAST(omk_hier.[nummer13] AS NVARCHAR(50)) AS S13,
            CAST(omk_hier.[nummer14] AS NVARCHAR(50)) AS S14,
            CAST(omk_hier.[nummer15] AS NVARCHAR(50)) AS S15,
            CAST(omk_hier.[nummer16] AS NVARCHAR(50)) AS S16,
            CAST(omk_hier.[nummer17] AS NVARCHAR(50)) AS S17,
            CAST(omk_hier.[nummer18] AS NVARCHAR(50)) AS S18,
            CAST(omk_hier.[nummer19] AS NVARCHAR(50)) AS S19,
            CAST(omk_hier.[nummer20] AS NVARCHAR(50)) AS S20,

            CAST(grp.[Grupperingsnummer] AS NVARCHAR(50)) AS g2,
            CAST(art.[A1] AS NVARCHAR(50)) AS a1,
            CAST(art.[A2] AS NVARCHAR(50)) AS a2,

            CAST(omk_hier.[tekst1] AS NVARCHAR(255)) AS tekst_s1,
            CAST(omk_hier.[tekst2] AS NVARCHAR(255)) AS tekst_s2,
            CAST(omk_hier.[tekst3] AS NVARCHAR(255)) AS tekst_s3,
            CAST(omk_hier.[tekst4] AS NVARCHAR(255)) AS tekst_s4,
            CAST(omk_hier.[tekst5] AS NVARCHAR(255)) AS tekst_s5,
            CAST(omk_hier.[tekst6] AS NVARCHAR(255)) AS tekst_s6,
            CAST(omk_hier.[tekst7] AS NVARCHAR(255)) AS tekst_s7,
            CAST(omk_hier.[tekst8] AS NVARCHAR(255)) AS tekst_S8,
            CAST(omk_hier.[tekst9] AS NVARCHAR(255)) AS tekst_S9,
            CAST(omk_hier.[tekst10] AS NVARCHAR(255)) AS tekst_S10,
            CAST(omk_hier.[tekst11] AS NVARCHAR(255)) AS tekst_S11,
            CAST(omk_hier.[tekst12] AS NVARCHAR(255)) AS tekst_S12,
            CAST(omk_hier.[tekst13] AS NVARCHAR(255)) AS tekst_S13,
            CAST(omk_hier.[tekst14] AS NVARCHAR(255)) AS tekst_S14,
            CAST(omk_hier.[tekst15] AS NVARCHAR(255)) AS tekst_S15,
            CAST(omk_hier.[tekst16] AS NVARCHAR(255)) AS tekst_S16,
            CAST(omk_hier.[tekst17] AS NVARCHAR(255)) AS tekst_S17,
            CAST(omk_hier.[tekst18] AS NVARCHAR(255)) AS tekst_S18,
            CAST(omk_hier.[tekst19] AS NVARCHAR(255)) AS tekst_S19,
            CAST(omk_hier.[tekst20] AS NVARCHAR(255)) AS tekst_S20,

            COALESCE(
                CAST(grp_aut.[G1Tekst] AS NVARCHAR(255)),
                CAST(grp.[Grupperingstekst] AS NVARCHAR(255))
            ) AS tekst_g1,
            CAST(grp.[Grupperingstekst] AS NVARCHAR(255)) AS tekst_g2,
            CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_a1,
            CAST(art.[Artstekst] AS NVARCHAR(255)) AS tekst_a2,
            CAST(funk_f1.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f1,
            CAST(funk_f2.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f2,
            CAST(funk_f3.[FunktionTekst] AS NVARCHAR(255)) AS tekst_f3,
            CAST(dr.[D1Tekst] AS NVARCHAR(255)) AS tekst_d1,

            CAST(omkost_link.[Omkostningsstedid] AS BIGINT) AS Omkostningsstedid,
            COALESCE(
                CAST(omk_hier.[nummer20] AS NVARCHAR(50)), CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer18] AS NVARCHAR(50)), CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer16] AS NVARCHAR(50)), CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer14] AS NVARCHAR(50)), CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer12] AS NVARCHAR(50)), CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer10] AS NVARCHAR(50)), CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer8] AS NVARCHAR(50)), CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer6] AS NVARCHAR(50)), CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer4] AS NVARCHAR(50)), CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer2] AS NVARCHAR(50)), CAST(omk_hier.[nummer1] AS NVARCHAR(50))
            ) AS omkostningsstednummer,
            CASE
                WHEN omk_hier.[nummer20] IS NOT NULL THEN 20 WHEN omk_hier.[nummer19] IS NOT NULL THEN 19
                WHEN omk_hier.[nummer18] IS NOT NULL THEN 18 WHEN omk_hier.[nummer17] IS NOT NULL THEN 17
                WHEN omk_hier.[nummer16] IS NOT NULL THEN 16 WHEN omk_hier.[nummer15] IS NOT NULL THEN 15
                WHEN omk_hier.[nummer14] IS NOT NULL THEN 14 WHEN omk_hier.[nummer13] IS NOT NULL THEN 13
                WHEN omk_hier.[nummer12] IS NOT NULL THEN 12 WHEN omk_hier.[nummer11] IS NOT NULL THEN 11
                WHEN omk_hier.[nummer10] IS NOT NULL THEN 10 WHEN omk_hier.[nummer9] IS NOT NULL THEN 9
                WHEN omk_hier.[nummer8] IS NOT NULL THEN 8 WHEN omk_hier.[nummer7] IS NOT NULL THEN 7
                WHEN omk_hier.[nummer6] IS NOT NULL THEN 6 WHEN omk_hier.[nummer5] IS NOT NULL THEN 5
                WHEN omk_hier.[nummer4] IS NOT NULL THEN 4 WHEN omk_hier.[nummer3] IS NOT NULL THEN 3
                WHEN omk_hier.[nummer2] IS NOT NULL THEN 2 WHEN omk_hier.[nummer1] IS NOT NULL THEN 1
                ELSE NULL
            END AS omkostningssted_niveau,

            TRY_CONVERT(DATE, trans.[GodkendtDato]) AS GodkendtDato,
            TRY_CONVERT(DATE, trans.[LukketDato]) AS LukketDato,
            TRY_CONVERT(DATETIME2, trans.[Oprettelsestidspunkt]) AS Trans_Oprettelsestidspunkt,
            TRY_CONVERT(DATE, trans.[Forfaldsdato]) AS Forfaldsdato,
            TRY_CONVERT(DATE, trans.[Fakturadato]) AS Trans_Fakturadato,
            TRY_CONVERT(DATE, trans.[SidstUdlignetDato]) AS SidstUdlignetDato,
            TRY_CONVERT(DATE, trans.[Transaktionsdato]) AS Transaktionsdato,
            TRY_CONVERT(DATE, post.[Bogfoeringsdato]) AS Bogfoeringsdato,
            TRY_CONVERT(DATETIME2, post.[Oprettelsestidspunkt]) AS Post_Oprettelsestidspunkt,

            TRY_CONVERT(DECIMAL(30,12), trans.[TransaktionsbeloebValuta]) AS transaktionsbeloeb_valuta,
            TRY_CONVERT(DECIMAL(30,12), trans.[Transaktionsbeloeb]) AS transaktionsbeloeb,
            TRY_CONVERT(DECIMAL(30,12), trans.[UdlignetBeloebValuta]) AS udlignet_beloeb_valuta,
            TRY_CONVERT(DECIMAL(30,12), trans.[UdlignetBeloeb]) AS udlignet_beloeb,
            TRY_CONVERT(DECIMAL(30,12), post.[Posteringsbeloeb]) AS posteringsbeloeb,

            trans.[Fakturanummer] AS faktura_nr,
            trans.[SidstUdlignetBilagsnummer] AS sidst_udlignet_bilagsnummer,
            trans.[Bilagsnummer] AS bilagsnummer_trans,
            post.[Bilagsnummer] AS bilagsnummer_postering,
            TRY_CONVERT(BIGINT, trans.[DokumentId]) AS dokument_id,
            trans.[Valutakode] AS valutakode_trans,
            post.[Valutakode] AS valutakode_postering,
            trans.[Transaktionstekst] AS transaktionstekst,
            post.[Posteringstekst] AS posteringstekst,

            -- Audit (latest-update timestamp; the downstream stg_dim_/stg_fct
            -- assets use dw_timestamp_update as the recency tiebreaker)
            post.[dw_timestamp_update]

        INTO [stg].[pre_Postering_sydax]
        FROM post_base post

        LEFT JOIN rel_dedup rel
            ON rel.[Kildesystem] = post.[Kildesystem]
            AND rel.[ref_LedgertransId] = post.[ref_LedgertransId]
            AND rel._rn = 1

        LEFT JOIN trans_dedup trans
            ON trans.[KreditortransaktionId] = rel.[KreditortransaktionId]
            AND trans._rn = 1

        LEFT JOIN kred_dedup kred
            ON kred.[KreditorId] = trans.[KreditorId]
            AND kred._rn = 1

        LEFT JOIN konto_dedup konto
            ON konto.[FinanskontoId] = post.[FinanskontoId]
            AND post.[FinanskontoId] IS NOT NULL
            AND konto._rn = 1

        LEFT JOIN omkost_link_dedup omkost_link
            ON omkost_link.[FinanskontoId] = post.[FinanskontoId]
            AND omkost_link._rn = 1

        LEFT JOIN omk_hier_dedup omk_hier
            ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
               CAST(omkost_link.[Omkostningsstedid] AS BIGINT)
            AND omk_hier._rn = 1

        LEFT JOIN hk_dedup hk
            ON hk.[ref_RECID] = konto.[hk_RECID] AND hk._rn = 1
        LEFT JOIN grp_dedup grp
            ON grp.[RECID] = konto.[grp_RECID] AND grp._rn = 1
        LEFT JOIN grp_aut_dedup grp_aut
            ON grp_aut.[ref_RECID] = konto.[grp_RECID] AND grp_aut._rn = 1
        LEFT JOIN art_dedup art
            ON art.[RECID] = konto.[art_RECID] AND art._rn = 1
        LEFT JOIN funk_dedup funk_f1
            ON funk_f1.[F1] = hk.[F1] AND funk_f1.[F2] IS NULL AND funk_f1.[F3] IS NULL AND funk_f1._rn = 1
        LEFT JOIN funk_dedup funk_f2
            ON funk_f2.[F1] = hk.[F1] AND funk_f2.[F2] = hk.[F2] AND funk_f2.[F3] IS NULL AND funk_f2._rn = 1
        LEFT JOIN funk_dedup funk_f3
            ON funk_f3.[F1] = hk.[F1] AND funk_f3.[F2] = hk.[F2] AND funk_f3.[F3] = hk.[F3] AND funk_f3._rn = 1
        LEFT JOIN dr_dedup dr
            ON dr.[ref_RECID] = TRY_CAST(konto.[d1_RECID] AS BIGINT) AND dr._rn = 1;
    """
