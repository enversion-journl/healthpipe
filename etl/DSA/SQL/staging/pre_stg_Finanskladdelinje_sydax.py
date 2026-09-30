# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Finanskladdelinje from SydAX (Base Layer)

Reconstructs the enriched Finanskladdelinje row set directly from the
``arc.sydax_*`` archive tables — replacing the previous dependency on the
DSI-built ``ext.finanskladdelinje_enriched`` JOIN.  The multi-table enrichment
(Finanskladde header, Finanstransaktionstype, and the full Finanskonto →
hierarchy chain) is performed here, then the same transforms the old asset
applied (CleanCode generation, type casts, COALESCE hierarchy texts,
Omkostningssted resolution, business-key aliases) are layered on top so the
output schema of ``stg.pre_Finanskladdelinje_sydax`` stays byte-identical.

Archive dedup:
    The SydAX archives are SCD2-style versioned (``StartDateTime`` /
    ``EndDateTime``) with NO soft-delete flag (there is no ``dw_is_deleted``
    column on any ``arc.sydax_*`` table).  Each hierarchy archive is therefore
    collapsed to its latest (current) version per business key via
    ``ROW_NUMBER() OVER (PARTITION BY <bk> ORDER BY [EndDateTime] DESC,
    [StartDateTime] DESC, [dw_timestamp_update] DESC) = 1`` — matching the
    legacy ``sydax_enriched_finanskladde`` enrichment, which selected the
    current SCD version by ``[EndDateTime] DESC`` (EndDateTime is NOT NULL; the
    open version carries a far-future sentinel, so plain DESC sorts it first).
    StartDateTime / dw_timestamp_update are deterministic tiebreakers.  Funktion
    follows the legacy ``[Modified] DESC`` version selector with the same
    tiebreakers appended.

Delta filter:
    The line (fact) archive is delta-filtered by the MIN of the downstream
    dim/fct checkpoints (DfmManager).  full_load mode ignores the filter and
    fully reconstructs the table.  pre_stg is NOT a checkpoint owner, so it
    never calls set_checkpoint / advances a checkpoint.

Downstream consumers:
- stg_dim_Finanskladdelinje_sydax -> dim.Finanskladdelinje
- stg_fct_Finanskladdelinje_sydax -> fct.Finanskladdelinje
- stg_dim_Finanskladde_sydax      -> dim.Finanskladde

Grain: ref_ledgertrans_id (unique BIGINT from SydAX ledger)
Business key: bk_ref_ledgertrans_id
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

FINANSKLADDE_SYDAX_KILDESYSTEM = "SydAX"


@asset(
    key=AssetKey(["erp", "pre_stg_Finanskladdelinje_sydax"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[
        "arc_sydax_finanskladdelinje",
        "arc_sydax_finanskladde",
        "arc_sydax_finanstransaktionstype",
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
        "entity": "finanskladdelinje",
    },
    description=(
        "Reconstruct enriched finanskladdelinje from arc.sydax_* archives (replaces the "
        "DSI ext.finanskladdelinje_enriched JOIN). Delta-filtered by downstream dim/fct "
        "checkpoints. Feeds dim.Finanskladdelinje and fct.Finanskladdelinje."
    ),
)
def pre_stg_Finanskladdelinje_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Reconstruct stg.pre_Finanskladdelinje_sydax from arc.sydax_* archives."""

    dfm = DfmManager(mssql_target_staging, context)

    run_ctx = dfm.start_dfm_run(
        "pre_stg",
        "pre_Finanskladdelinje_sydax",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    try:
        # ------------------------------------------------------------------
        # Delta filter from downstream checkpoints (MIN of dim/fct owners).
        # full_load mode rebuilds everything; first run (no checkpoints) too.
        # ------------------------------------------------------------------
        cps = [
            dfm.get_checkpoint("dim", "Finanskladdelinje"),
            dfm.get_checkpoint("fct", "Finanskladdelinje"),
            dfm.get_checkpoint("dim", "Finanskladde"),
        ]
        known = [c for c in cps if c]
        min_checkpoint = min(known) if known else None
        if config.mode == "full_load":
            min_checkpoint = None  # full rebuild ignores checkpoint

        line_filter = (
            f"AND fkl.[dw_timestamp_update] > '{min_checkpoint}'" if min_checkpoint else ""
        )

        if min_checkpoint is not None:
            context.log.info(f"Checkpoint filter: fkl.dw_timestamp_update > {min_checkpoint}")
        else:
            context.log.info("No checkpoint filter — full reconstruction from archives")

        build_sql = _build_reconstruction_sql(line_filter)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Reconstructing stg.pre_Finanskladdelinje_sydax from arc.sydax_* archives..."
                )
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Finanskladdelinje_sydax];")
                cursor.execute(build_sql)

                cursor.execute(
                    """
                    CREATE CLUSTERED INDEX IX_pre_finanskladdelinje_sydax_PK
                    ON [stg].[pre_Finanskladdelinje_sydax](ref_ledgertrans_id);
                    """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Linje
                    ON [stg].[pre_Finanskladdelinje_sydax](clean_code_Finanskladdelinje);
                    """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Kladde
                    ON [stg].[pre_Finanskladdelinje_sydax](clean_code_Finanskladde);
                    """
                )
                cursor.execute(
                    """
                    CREATE NONCLUSTERED INDEX IX_pre_finanskladdelinje_sydax_CC_Kontoplan
                    ON [stg].[pre_Finanskladdelinje_sydax](clean_code_Kontoplan);
                    """
                )
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[pre_Finanskladdelinje_sydax];")
                row_count = cursor.fetchone()[0]

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(clean_code_Finanskladdelinje) AS has_cc_linje,
                        COUNT(clean_code_Finanskladde) AS has_cc_kladde,
                        COUNT(clean_code_Kontoplan) AS has_cc_kontoplan,
                        COUNT(DISTINCT clean_code_Finanskladde) AS unique_cc_kladde,
                        COUNT(DISTINCT clean_code_Kontoplan) AS unique_cc_kontoplan,
                        COUNT(ref_LedgerjournaltableId) AS has_header_ref,
                        COUNT(kontonummer) AS has_kontonummer,
                        COUNT(omkostningsstednummer) AS has_omkostningssted,
                        COUNT(transaktionstypenummer) AS has_transtype
                    FROM [stg].[pre_Finanskladdelinje_sydax]
                    """
                )
                stats = cursor.fetchone()
            finally:
                cursor.close()

        context.log.info(
            f"Coverage: {stats[1]}/{stats[0]} with clean_code_Finanskladdelinje, "
            f"{stats[2]}/{stats[0]} with clean_code_Finanskladde, "
            f"{stats[3]}/{stats[0]} with clean_code_Kontoplan. "
            f"Unique CC_Kladde: {stats[4]}, unique CC_Kontoplan: {stats[5]}"
        )
        context.log.info(
            f"Enrichment coverage: "
            f"header_ref={stats[6]}/{stats[0]}, "
            f"kontonummer={stats[7]}/{stats[0]}, "
            f"Omkostningssted={stats[8]}/{stats[0]}, "
            f"Transaktionstype={stats[9]}/{stats[0]}"
        )

        # pre_stg is NOT a checkpoint owner; it does not call set_checkpoint.
        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Finanskladdelinje_sydax",
            "target_table": "stg.pre_Finanskladdelinje_sydax",
            "row_count": row_count,
            "source_table": "arc.sydax_finanskladdelinje (+11 hierarchy archives)",
            "grain": "ref_ledgertrans_id",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
            "unique_cc_kladde": stats[4],
            "unique_cc_kontoplan": stats[5],
            "has_header_ref": stats[6],
            "has_kontonummer": stats[7],
            "layer": "stg_fct (Layer 0)",
        }
    )


def _build_reconstruction_sql(line_filter: str) -> str:
    """Build the SELECT ... INTO that reconstructs the enriched line set.

    Reproduces the multi-table enrichment from the old DSI
    ``sydax_enriched_finanskladde.py`` query (every output column and the
    Finanskonto → hierarchy join chain) directly against ``arc.sydax_*``,
    then applies the downstream transforms (CleanCodes, casts, COALESCE,
    Omkostningssted resolution) the previous pre_stg asset applied — so the
    output schema is byte-identical.

    Each hierarchy archive is collapsed to its latest (current) version per
    business key (no dw_is_deleted exists on these tables; the current version is
    discriminated by EndDateTime DESC, matching the legacy enrichment). The fact
    archive (arc.sydax_finanskladdelinje) carries one row per
    (Kildesystem, ref_ledgertrans_id) and is only delta-filtered.
    """
    return f"""
        ;WITH fkl_dedup AS (
            -- Fact line: one row per (Kildesystem, ref_ledgertrans_id); delta-filtered.
            SELECT *
            FROM [arc].[sydax_finanskladdelinje]
            WHERE [Kildesystem] = '{FINANSKLADDE_SYDAX_KILDESYSTEM}'
        ),
        fk_dedup AS (
            -- Legacy enrichment selected the current SCD version by EndDateTime DESC
            -- (EndDateTime is NOT NULL; the open version carries a far-future
            -- sentinel so plain DESC sorts it first). StartDateTime / dw_timestamp_update
            -- are deterministic tiebreakers.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanskladdeId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_finanskladde]
        ),
        tt_dedup AS (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [FinanstransaktionstypeId]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_finanstransaktionstype]
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
            -- Legacy hk_dedup: ORDER BY [EndDateTime] DESC.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [ref_RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_hovedkonto]
            WHERE [ref_RECID] IS NOT NULL
        ),
        grp_dedup AS (
            -- Legacy grp_dedup: ORDER BY [EndDateTime] DESC.
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
            -- Legacy art_dedup: ORDER BY [EndDateTime] DESC.
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY [RECID]
                       ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC
                   ) AS _rn
            FROM [arc].[sydax_art]
            WHERE [RECID] IS NOT NULL
        ),
        funk_dedup AS (
            -- Legacy funk_dedup: ORDER BY [Modified] DESC (its version selector).
            -- EndDateTime / StartDateTime / dw_timestamp_update are deterministic
            -- tiebreakers under the same Modified.
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
        ),

        enriched AS (
            SELECT
                -- Line grain
                fkl.[ref_LedgertransId] AS ref_ledgertrans_id,
                fkl.[ref_RecId],

                -- Header columns (denormalized from Finanskladde)
                fk.[ref_LedgerjournaltableId],
                fk.[Kladdenummer] AS kladdenummer,
                fk.[Kladdetekst] AS kladdetekst,
                fk.[Kladdenavn] AS kladdenavn,
                fk.[ErLukket] AS er_lukket,
                fk.[DataAreaId] AS data_area_id,
                fk.[Maskinnummer] AS Maskinnummer_Kladde,

                -- Transaction type lookup
                tt.[Transaktionstypenummer] AS transaktionstypenummer,
                tt.[Transaktionstypetekst] AS transaktionstypetekst,

                -- Line descriptive columns
                fkl.[Linjenummer] AS linjenummer,
                fkl.[dato] AS Dato,
                fkl.[Beskrivelse] AS beskrivelse,
                fkl.[Ekspeditionsnummer] AS ekspeditionsnummer,
                fkl.[Maskinnummer] AS maskinnummer,
                fkl.[BilagsnummerID],
                fkl.[DokumentId] AS dokument_id,
                fkl.[Debet] AS debet,
                fkl.[Kredit] AS kredit,

                -- Account chain (raw FKs and values from Finanskonto)
                fkl.[FinanskontoId] AS finanskonto_id,
                konto.[Kontonummer] AS kontonummer,
                konto.[hk_RECID],
                konto.[grp_RECID],
                konto.[art_RECID],
                konto.[d1_RECID],

                -- Hierarchy raw values from Hovedkonto
                hk.[F1] AS f1,
                hk.[F2] AS f2,
                hk.[F3] AS f3,
                hk.[D1] AS d1,
                hk.[G1] AS g1,

                -- Omkostningssted hierarchy raw values
                omk_hier.[nummer1],
                omk_hier.[nummer2],
                omk_hier.[nummer3],
                omk_hier.[nummer4],
                omk_hier.[nummer5],
                omk_hier.[nummer6],
                omk_hier.[nummer7],
                omk_hier.[nummer8],
                omk_hier.[nummer9],
                omk_hier.[nummer10],
                omk_hier.[nummer11],
                omk_hier.[nummer12],
                omk_hier.[nummer13],
                omk_hier.[nummer14],
                omk_hier.[nummer15],
                omk_hier.[nummer16],
                omk_hier.[nummer17],
                omk_hier.[nummer18],
                omk_hier.[nummer19],
                omk_hier.[nummer20],
                omk_hier.[tekst1],
                omk_hier.[tekst2],
                omk_hier.[tekst3],
                omk_hier.[tekst4],
                omk_hier.[tekst5],
                omk_hier.[tekst6],
                omk_hier.[tekst7],
                omk_hier.[tekst8],
                omk_hier.[tekst9],
                omk_hier.[tekst10],
                omk_hier.[tekst11],
                omk_hier.[tekst12],
                omk_hier.[tekst13],
                omk_hier.[tekst14],
                omk_hier.[tekst15],
                omk_hier.[tekst16],
                omk_hier.[tekst17],
                omk_hier.[tekst18],
                omk_hier.[tekst19],
                omk_hier.[tekst20],

                -- gruppering raw values
                grp.[Grupperingsnummer],
                grp.[Grupperingstekst],

                -- GrupperingAutoriseret raw values
                grp_aut.[G1Tekst],

                -- art raw values
                art.[A1] AS a1,
                art.[A2] AS a2,
                art.[Artstekst],

                -- Funktion raw tekst values
                funk_f1.[FunktionTekst] AS FunktionTekst_F1,
                funk_f2.[FunktionTekst] AS FunktionTekst_F2,
                funk_f3.[FunktionTekst] AS FunktionTekst_F3,

                -- Dranst raw value
                dr.[D1Tekst],

                -- Omkostningssted link
                omkost_link.[Omkostningsstedid],

                -- Dates from the line/header
                fkl.[CreatedOn] AS Linje_CreatedOn,
                fk.[CreatedOn] AS Kladde_CreatedOn,
                fk.[Modified] AS Kladde_Modified,

                -- Audit (latest-update timestamp; the downstream stg_dim_/stg_fct
                -- assets use dw_timestamp_update as the recency tiebreaker)
                fkl.[dw_timestamp_update]

            FROM fkl_dedup fkl

            LEFT JOIN fk_dedup fk
                ON fk.[FinanskladdeId] = fkl.[FinanskladdeId]
                AND fk._rn = 1

            LEFT JOIN tt_dedup tt
                ON tt.[FinanstransaktionstypeId] = fkl.[FinanstransaktionstypeId]
                AND tt._rn = 1

            LEFT JOIN konto_dedup konto
                ON konto.[FinanskontoId] = fkl.[FinanskontoId]
                AND konto.[FinanskontoId] IS NOT NULL
                AND konto._rn = 1

            LEFT JOIN omkost_link_dedup omkost_link
                ON omkost_link.[FinanskontoId] = fkl.[FinanskontoId]
                AND omkost_link._rn = 1

            LEFT JOIN omk_hier_dedup omk_hier
                ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
                   CAST(omkost_link.[Omkostningsstedid] AS BIGINT)
                AND omk_hier._rn = 1

            LEFT JOIN hk_dedup hk
                ON hk.[ref_RECID] = konto.[hk_RECID]
                AND hk._rn = 1

            LEFT JOIN grp_dedup grp
                ON grp.[RECID] = konto.[grp_RECID]
                AND grp._rn = 1

            LEFT JOIN grp_aut_dedup grp_aut
                ON grp_aut.[ref_RECID] = konto.[grp_RECID]
                AND grp_aut._rn = 1

            LEFT JOIN art_dedup art
                ON art.[RECID] = konto.[art_RECID]
                AND art._rn = 1

            LEFT JOIN funk_dedup funk_f1
                ON funk_f1.[F1] = hk.[F1]
                AND funk_f1.[F2] IS NULL
                AND funk_f1.[F3] IS NULL
                AND funk_f1._rn = 1

            LEFT JOIN funk_dedup funk_f2
                ON funk_f2.[F1] = hk.[F1]
                AND funk_f2.[F2] = hk.[F2]
                AND funk_f2.[F3] IS NULL
                AND funk_f2._rn = 1

            LEFT JOIN funk_dedup funk_f3
                ON funk_f3.[F1] = hk.[F1]
                AND funk_f3.[F2] = hk.[F2]
                AND funk_f3.[F3] = hk.[F3]
                AND funk_f3._rn = 1

            LEFT JOIN dr_dedup dr
                ON dr.[ref_RECID] = TRY_CAST(konto.[d1_RECID] AS BIGINT)
                AND dr._rn = 1

            WHERE fkl.[Kildesystem] = '{FINANSKLADDE_SYDAX_KILDESYSTEM}'
              {line_filter}
        )

        SELECT
            -- Line grain and business key
            e.[ref_ledgertrans_id] AS ref_ledgertrans_id,
            e.[ref_ledgertrans_id] AS bk_ref_ledgertrans_id,
            e.[ref_RecId],

            -- CleanCode for the line (fact + dim grain)
            'FKLINJE_' + LTRIM(RTRIM(
                CAST(e.[ref_ledgertrans_id] AS NVARCHAR(100))
            )) AS clean_code_Finanskladdelinje,

            -- Header columns
            e.[ref_LedgerjournaltableId],
            e.[ref_LedgerjournaltableId] AS bk_ref_ledgerjournaltable_id,
            'FINANSKLADDE_' + LTRIM(RTRIM(
                CAST(e.[ref_LedgerjournaltableId] AS NVARCHAR(100))
            )) AS clean_code_Finanskladde,
            e.[kladdenummer] AS kladdenummer,
            e.[kladdetekst] AS kladdetekst,
            e.[kladdenavn] AS kladdenavn,
            e.[er_lukket] AS er_lukket,
            e.[data_area_id] AS data_area_id,
            e.[Maskinnummer_Kladde],

            -- Transaction type (raw from enriched join)
            e.[transaktionstypenummer] AS transaktionstypenummer,
            e.[transaktionstypetekst] AS transaktionstypetekst,

            -- Line descriptive columns
            e.[linjenummer] AS linjenummer,
            e.[Dato] AS Dato,
            e.[beskrivelse] AS beskrivelse,
            e.[ekspeditionsnummer] AS ekspeditionsnummer,
            e.[maskinnummer] AS maskinnummer,
            CAST(e.[BilagsnummerID] AS BIGINT) AS BilagsnummerID,
            CAST(e.[dokument_id] AS BIGINT) AS dokument_id,

            -- Measures
            TRY_CONVERT(DECIMAL(30,12), e.[debet]) AS debet,
            TRY_CONVERT(DECIMAL(30,12), e.[kredit]) AS kredit,

            -- Account chain
            'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                CAST(e.[finanskonto_id] AS NVARCHAR(100)), ' ', '')
            ))) AS clean_code_Kontoplan,
            CAST(e.[finanskonto_id] AS BIGINT) AS finanskonto_id,
            e.[finanskonto_id] AS bk_finanskonto_id,
            e.[kontonummer] AS kontonummer,
            e.[hk_RECID],
            e.[grp_RECID],
            e.[art_RECID],
            e.[d1_RECID],

            -- Hierarchy nummer columns (cast to NVARCHAR(50))
            CAST(e.[f1] AS NVARCHAR(50)) AS f1,
            CAST(e.[f2] AS NVARCHAR(50)) AS f2,
            CAST(e.[f3] AS NVARCHAR(50)) AS f3,
            CAST(e.[d1] AS NVARCHAR(50)) AS d1,
            CAST(e.[g1] AS NVARCHAR(50)) AS g1,

            CAST(e.[nummer1] AS NVARCHAR(50)) AS s1,
            CAST(e.[nummer2] AS NVARCHAR(50)) AS s2,
            CAST(e.[nummer3] AS NVARCHAR(50)) AS s3,
            CAST(e.[nummer4] AS NVARCHAR(50)) AS s4,
            CAST(e.[nummer5] AS NVARCHAR(50)) AS s5,
            CAST(e.[nummer6] AS NVARCHAR(50)) AS s6,
            CAST(e.[nummer7] AS NVARCHAR(50)) AS s7,
            CAST(e.[nummer8] AS NVARCHAR(50)) AS S8,
            CAST(e.[nummer9] AS NVARCHAR(50)) AS S9,
            CAST(e.[nummer10] AS NVARCHAR(50)) AS S10,
            CAST(e.[nummer11] AS NVARCHAR(50)) AS S11,
            CAST(e.[nummer12] AS NVARCHAR(50)) AS S12,
            CAST(e.[nummer13] AS NVARCHAR(50)) AS S13,
            CAST(e.[nummer14] AS NVARCHAR(50)) AS S14,
            CAST(e.[nummer15] AS NVARCHAR(50)) AS S15,
            CAST(e.[nummer16] AS NVARCHAR(50)) AS S16,
            CAST(e.[nummer17] AS NVARCHAR(50)) AS S17,
            CAST(e.[nummer18] AS NVARCHAR(50)) AS S18,
            CAST(e.[nummer19] AS NVARCHAR(50)) AS S19,
            CAST(e.[nummer20] AS NVARCHAR(50)) AS S20,

            CAST(e.[Grupperingsnummer] AS NVARCHAR(50)) AS g2,
            CAST(e.[a1] AS NVARCHAR(50)) AS a1,
            CAST(e.[a2] AS NVARCHAR(50)) AS a2,

            -- Hierarchy tekst columns (cast to NVARCHAR(255))
            CAST(e.[tekst1] AS NVARCHAR(255)) AS tekst_s1,
            CAST(e.[tekst2] AS NVARCHAR(255)) AS tekst_s2,
            CAST(e.[tekst3] AS NVARCHAR(255)) AS tekst_s3,
            CAST(e.[tekst4] AS NVARCHAR(255)) AS tekst_s4,
            CAST(e.[tekst5] AS NVARCHAR(255)) AS tekst_s5,
            CAST(e.[tekst6] AS NVARCHAR(255)) AS tekst_s6,
            CAST(e.[tekst7] AS NVARCHAR(255)) AS tekst_s7,
            CAST(e.[tekst8] AS NVARCHAR(255)) AS tekst_S8,
            CAST(e.[tekst9] AS NVARCHAR(255)) AS tekst_S9,
            CAST(e.[tekst10] AS NVARCHAR(255)) AS tekst_S10,
            CAST(e.[tekst11] AS NVARCHAR(255)) AS tekst_S11,
            CAST(e.[tekst12] AS NVARCHAR(255)) AS tekst_S12,
            CAST(e.[tekst13] AS NVARCHAR(255)) AS tekst_S13,
            CAST(e.[tekst14] AS NVARCHAR(255)) AS tekst_S14,
            CAST(e.[tekst15] AS NVARCHAR(255)) AS tekst_S15,
            CAST(e.[tekst16] AS NVARCHAR(255)) AS tekst_S16,
            CAST(e.[tekst17] AS NVARCHAR(255)) AS tekst_S17,
            CAST(e.[tekst18] AS NVARCHAR(255)) AS tekst_S18,
            CAST(e.[tekst19] AS NVARCHAR(255)) AS tekst_S19,
            CAST(e.[tekst20] AS NVARCHAR(255)) AS tekst_S20,

            COALESCE(
                CAST(e.[G1Tekst] AS NVARCHAR(255)),
                CAST(e.[Grupperingstekst] AS NVARCHAR(255))
            ) AS tekst_g1,
            CAST(e.[Grupperingstekst] AS NVARCHAR(255)) AS tekst_g2,
            CAST(e.[Artstekst] AS NVARCHAR(255)) AS tekst_a1,
            CAST(e.[Artstekst] AS NVARCHAR(255)) AS tekst_a2,
            CAST(e.[FunktionTekst_F1] AS NVARCHAR(255)) AS tekst_f1,
            CAST(e.[FunktionTekst_F2] AS NVARCHAR(255)) AS tekst_f2,
            CAST(e.[FunktionTekst_F3] AS NVARCHAR(255)) AS tekst_f3,
            CAST(e.[D1Tekst] AS NVARCHAR(255)) AS tekst_d1,

            -- Omkostningssted resolved
            CAST(e.[Omkostningsstedid] AS BIGINT) AS Omkostningsstedid,
            COALESCE(
                CAST(e.[nummer20] AS NVARCHAR(50)),
                CAST(e.[nummer19] AS NVARCHAR(50)),
                CAST(e.[nummer18] AS NVARCHAR(50)),
                CAST(e.[nummer17] AS NVARCHAR(50)),
                CAST(e.[nummer16] AS NVARCHAR(50)),
                CAST(e.[nummer15] AS NVARCHAR(50)),
                CAST(e.[nummer14] AS NVARCHAR(50)),
                CAST(e.[nummer13] AS NVARCHAR(50)),
                CAST(e.[nummer12] AS NVARCHAR(50)),
                CAST(e.[nummer11] AS NVARCHAR(50)),
                CAST(e.[nummer10] AS NVARCHAR(50)),
                CAST(e.[nummer9] AS NVARCHAR(50)),
                CAST(e.[nummer8] AS NVARCHAR(50)),
                CAST(e.[nummer7] AS NVARCHAR(50)),
                CAST(e.[nummer6] AS NVARCHAR(50)),
                CAST(e.[nummer5] AS NVARCHAR(50)),
                CAST(e.[nummer4] AS NVARCHAR(50)),
                CAST(e.[nummer3] AS NVARCHAR(50)),
                CAST(e.[nummer2] AS NVARCHAR(50)),
                CAST(e.[nummer1] AS NVARCHAR(50))
            ) AS omkostningsstednummer,
            CASE
                WHEN e.[nummer20] IS NOT NULL THEN 20
                WHEN e.[nummer19] IS NOT NULL THEN 19
                WHEN e.[nummer18] IS NOT NULL THEN 18
                WHEN e.[nummer17] IS NOT NULL THEN 17
                WHEN e.[nummer16] IS NOT NULL THEN 16
                WHEN e.[nummer15] IS NOT NULL THEN 15
                WHEN e.[nummer14] IS NOT NULL THEN 14
                WHEN e.[nummer13] IS NOT NULL THEN 13
                WHEN e.[nummer12] IS NOT NULL THEN 12
                WHEN e.[nummer11] IS NOT NULL THEN 11
                WHEN e.[nummer10] IS NOT NULL THEN 10
                WHEN e.[nummer9] IS NOT NULL THEN 9
                WHEN e.[nummer8] IS NOT NULL THEN 8
                WHEN e.[nummer7] IS NOT NULL THEN 7
                WHEN e.[nummer6] IS NOT NULL THEN 6
                WHEN e.[nummer5] IS NOT NULL THEN 5
                WHEN e.[nummer4] IS NOT NULL THEN 4
                WHEN e.[nummer3] IS NOT NULL THEN 3
                WHEN e.[nummer2] IS NOT NULL THEN 2
                WHEN e.[nummer1] IS NOT NULL THEN 1
                ELSE NULL
            END AS omkostningssted_niveau,

            -- Dates
            TRY_CONVERT(DATE, e.[Dato]) AS dato_converted,
            TRY_CONVERT(DATETIME2, e.[Linje_CreatedOn]) AS Linje_CreatedOn,
            TRY_CONVERT(DATETIME2, e.[Kladde_CreatedOn]) AS Kladde_CreatedOn,
            TRY_CONVERT(DATETIME2, e.[Kladde_Modified]) AS Kladde_Modified,

            -- Audit
            e.[dw_timestamp_update]

        INTO [stg].[pre_Finanskladdelinje_sydax]
        FROM enriched e;
    """
