# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Budget (Finans) from SydAX (Base Layer)

Reconstructs the enriched Budgetpost row set directly from the arc.sydax_*
archive tables — replacing the previous dependency on the DSI-built
ext.budgetpost_enriched JOIN. Reads 7 archives: the budgetpost grain plus
budgetmodel / budgetpoststatus / budgetbeloebtype lookups and the
finanskonto -> omkostningssted hierarchy chain (only the columns the two
downstream budget stg assets consume — the F/G/A/D hierarchy texts are not
consumed by budget, so those archives are intentionally omitted).

Archive dedup: lookup/hierarchy archives are SCD2-versioned; each is collapsed
to its current version per business key via
ROW_NUMBER OVER (PARTITION BY <bk> ORDER BY [EndDateTime] DESC, [StartDateTime]
DESC, [dw_timestamp_update] DESC) = 1. The budgetpost grain archive carries one
row per (Kildesystem, ref_LedgerbudgetId) and is only delta-filtered.

Delta filter: budgetpost is delta-filtered by the MIN of the downstream
dim.BudgetEgenskaber / fct.Budget checkpoints (DfmManager). full_load mode
ignores the filter. pre_stg is NOT a checkpoint owner.

Grain: ref_LedgerbudgetId.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

BUDGET_SYDAX_KILDESYSTEM = "SydAX"


@asset(
    key=AssetKey(["erp", "pre_stg_Budget_sydax"]),
    group_name="erp_pre_staging",
    compute_kind="mssql",
    deps=[
        "arc_budgetpost_sydax",
        "arc_budgetmodel_sydax",
        "arc_budgetpoststatus_sydax",
        "arc_budgetbeloebtype_sydax",
        "arc_finanskonto_sydax",
        "arc_finanskonto_omkostningssted_sydax",
        "arc_omkostningssted_hierarki_sydax",
    ],
    tags={"domain": "erp", "layer": "stg_fct", "source": "sydax", "entity": "budget"},
    description=(
        "Reconstruct enriched budget from arc.sydax_* archives (replaces the DSI "
        "ext.budgetpost_enriched JOIN). Delta-filtered by downstream dim/fct "
        "checkpoints. Feeds stg.fct_Budget_sydax and stg.dim_BudgetEgenskaber_sydax."
    ),
)
def pre_stg_Budget_sydax(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Reconstruct stg.pre_Budget_sydax from arc.sydax_* archives."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "pre_stg", "pre_Budget_sydax", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        cps = [
            dfm.get_checkpoint("dim", "BudgetEgenskaber"),
            dfm.get_checkpoint("fct", "Budget"),
        ]
        known = [c for c in cps if c]
        min_checkpoint = min(known) if known else None
        if config.mode == "full_load":
            min_checkpoint = None

        bp_filter = f"AND bp.[dw_timestamp_update] > '{min_checkpoint}'" if min_checkpoint else ""
        if min_checkpoint is not None:
            context.log.info(f"Checkpoint filter: bp.dw_timestamp_update > {min_checkpoint}")
        else:
            context.log.info("No checkpoint filter — full reconstruction from archives")

        build_sql = _build_reconstruction_sql(bp_filter)

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Reconstructing stg.pre_Budget_sydax from arc.sydax_* archives...")
                cursor.execute("DROP TABLE IF EXISTS [stg].[pre_Budget_sydax];")
                cursor.execute(build_sql)
                cursor.execute(
                    "CREATE CLUSTERED INDEX IX_pre_budget_sydax_PK "
                    "ON [stg].[pre_Budget_sydax](ref_LedgerbudgetId);"
                )
                cursor.execute(
                    "CREATE NONCLUSTERED INDEX IX_pre_budget_sydax_CC_Budget "
                    "ON [stg].[pre_Budget_sydax](clean_code_Budget);"
                )
                cursor.execute(
                    "CREATE NONCLUSTERED INDEX IX_pre_budget_sydax_CC_Egenskaber "
                    "ON [stg].[pre_Budget_sydax](clean_code_BudgetEgenskaber);"
                )
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[pre_Budget_sydax];")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"Reconstructed {row_count} rows into stg.pre_Budget_sydax")
        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.pre_Budget_sydax",
            "target_table": "stg.pre_Budget_sydax",
            "row_count": row_count,
            "source_table": "arc.sydax_budgetpost (+6 enrichment archives)",
            "grain": "ref_LedgerbudgetId",
            "checkpoint_filter": min_checkpoint if min_checkpoint else "none (full load)",
            "mode": config.mode,
            "layer": "stg_fct (Layer 0)",
        }
    )


def _build_reconstruction_sql(bp_filter: str) -> str:
    """Reconstruct the enriched budget set from arc.sydax_* archives.

    Backbone (delta-filtered, no version-collapse): arc.sydax_budgetpost.
    Lookup archives (budgetmodel, budgetpoststatus, budgetbeloebtype) and the
    finanskonto -> omkostningssted hierarchy chain are SCD-collapsed to their
    current version (EndDateTime DESC) — matching the original enrichment in
    DSI/mssql/sydax_enriched_budget.py.

    CleanCode expressions ported verbatim from sydax_enriched_budget.py lines
    166-172 (clean_code_Budget), 174-209 (clean_code_BudgetEgenskaber), 216-218
    (clean_code_Kontoplan), and 289-310 (omkostningsstednummer), with source-table
    alias swaps: bp.->bp_base/bp, bm.->bm_dedup/bm, konto.->konto_dedup/konto,
    omk_hier.->omk_hier_dedup/omk_hier, bps.->bps_dedup/bps, bbt.->bbt_dedup/bbt.
    """
    return f"""
        ;WITH bp_base AS (
            SELECT * FROM [arc].[sydax_budgetpost] bp
            WHERE bp.[Kildesystem] = '{BUDGET_SYDAX_KILDESYSTEM}'
              {bp_filter}
        ),
        bm_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [BudgetmodelId]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_budgetmodel] WHERE [Kildesystem] = '{BUDGET_SYDAX_KILDESYSTEM}'
        ),
        bps_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [BudgetpostStatusId]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_budgetpoststatus]
        ),
        bbt_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [BudgetbeløbTypeId]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_budgetbeloebtype]
        ),
        konto_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [FinanskontoId]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_finanskonto]
        ),
        omkost_link_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [FinanskontoOmkostningsstedid]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_finanskonto_omkostningssted]
        ),
        omk_hier_dedup AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY [OmkostningsstedHierarkiId]
                ORDER BY [EndDateTime] DESC, [StartDateTime] DESC, [dw_timestamp_update] DESC) AS _rn
            FROM [arc].[sydax_omkostningssted_hierarki]
        )

        SELECT
            CAST(bp.[ref_LedgerbudgetId] AS BIGINT) AS ref_LedgerbudgetId,

            -- clean_code_Budget: ported verbatim from sydax_enriched_budget.py lines 166-172
            CASE
                WHEN bp.[ref_LedgerbudgetId] IS NOT NULL
                THEN 'BUDGET_' + UPPER(LTRIM(RTRIM(REPLACE(
                         CAST(bp.[ref_LedgerbudgetId] AS NVARCHAR(100)), ' ', '')
                     )))
                ELSE NULL
            END AS clean_code_Budget,

            -- clean_code_BudgetEgenskaber: ported verbatim from sydax_enriched_budget.py lines 174-209
            'BUDGETEGENSKABER'
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(REPLACE(
                    CAST(bm.[ref_BudgetmodelId] AS NVARCHAR(100)), ' ', '')))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bm.[Budgetmodelnummer] AS NVARCHAR(100))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bm.[Budgetmodeltype] AS NVARCHAR(100))))), '')
                + '_' + ISNULL(CAST(bm.[ErLukket] AS NVARCHAR(10)), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Valutakode]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[DebetKredit]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Budgetkode]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[Ændringstype]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(bp.[ÆndringstypeBeskrivelse]))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bp.[BeloebstypeId] AS NVARCHAR(50))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(CAST(bps.[BudgetpostStatusId] AS NVARCHAR(50))))), '')
                + '_' + ISNULL(UPPER(LTRIM(RTRIM(COALESCE(
                    CAST(omk_hier.[nummer20] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer18] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer16] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer14] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer12] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer10] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer8] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer6] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer4] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer2] AS NVARCHAR(50)),
                    CAST(omk_hier.[nummer1] AS NVARCHAR(50))
                )))), '')
            AS clean_code_BudgetEgenskaber,

            -- clean_code_Kontoplan: ported verbatim from sydax_enriched_budget.py lines 216-218
            -- Uses konto.[FinanskontoId] (the joined finanskonto), matching original parity
            'KONTOPLAN_' + UPPER(LTRIM(RTRIM(REPLACE(
                CAST(konto.[FinanskontoId] AS NVARCHAR(100)), ' ', '')
            ))) AS clean_code_Kontoplan,

            CAST(bm.[ref_BudgetmodelId] AS NVARCHAR(100)) AS ref_BudgetmodelId,
            CAST(bm.[Budgetmodelnummer] AS NVARCHAR(100)) AS budgetmodelnummer,
            CAST(bm.[Budgetmodeltekst] AS NVARCHAR(255)) AS budgetmodeltekst,
            CAST(bm.[Budgetmodeltype] AS NVARCHAR(100)) AS budgetmodeltype,
            bm.[ErLukket] AS ErLukket_Budgetmodel,

            CAST(bp.[FinanskontoId] AS NVARCHAR(100)) AS bk_finanskonto_id,

            -- omkostningsstednummer: ported verbatim from sydax_enriched_budget.py lines 289-310
            COALESCE(
                CAST(omk_hier.[nummer20] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer19] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer18] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer17] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer16] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer15] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer14] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer13] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer12] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer11] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer10] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer9] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer8] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer7] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer6] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer5] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer4] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer3] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer2] AS NVARCHAR(50)),
                CAST(omk_hier.[nummer1] AS NVARCHAR(50))
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

            TRY_CONVERT(DATE, bp.[Budgetdato]) AS Budgetdato,
            TRY_CONVERT(DATE, bp.[BudgetAdoptedDate]) AS BudgetAdoptedDate,
            TRY_CONVERT(DATETIME2, bp.[Oprettelsestidspunkt]) AS Oprettelsestidspunkt,
            TRY_CONVERT(DATETIME2, bp.[SidstAendretTidspunkt]) AS SidstAendretTidspunkt,

            TRY_CONVERT(DECIMAL(30,12), bp.[Budgetbeloeb]) AS budgetbeloeb,
            TRY_CONVERT(DECIMAL(30,12), bp.[BudgetbeloebValuta]) AS budgetbeloeb_valuta,
            bp.[Valutakode] AS valutakode,
            bp.[DebetKredit] AS debet_kredit,
            bp.[Budgetkode] AS budgetkode,
            bp.[Ændringstype] AS aendringstype,
            bp.[ÆndringstypeBeskrivelse] AS aendringstype_beskrivelse,
            CAST(bp.[BeloebstypeId] AS NVARCHAR(50)) AS beloebstype_id,
            CAST(bbt.[BudgetbeløbTypetekst] AS NVARCHAR(1000)) AS budgetbeloeb_typetekst,
            CAST(bps.[BudgetpostStatusId] AS NVARCHAR(50)) AS budgetpost_status_id,
            CAST(bps.[BudgetpostStatustekst] AS NVARCHAR(255)) AS budgetpost_status_tekst,

            bp.[dw_timestamp_update]

        INTO [stg].[pre_Budget_sydax]
        FROM bp_base bp
        LEFT JOIN bm_dedup bm
            ON bm.[BudgetmodelId] = bp.[BudgetmodelId] AND bm._rn = 1
        LEFT JOIN konto_dedup konto
            ON konto.[FinanskontoId] = bp.[FinanskontoId] AND konto._rn = 1
        LEFT JOIN omkost_link_dedup omkost_link
            ON omkost_link.[FinanskontoId] = bp.[FinanskontoId] AND omkost_link._rn = 1
        LEFT JOIN omk_hier_dedup omk_hier
            ON CAST(CAST(omk_hier.[max_OmkostningsstedId] AS FLOAT) AS BIGINT) =
               CAST(omkost_link.[Omkostningsstedid] AS BIGINT) AND omk_hier._rn = 1
        LEFT JOIN bps_dedup bps
            ON bps.[BudgetpostStatusId] = bp.[BudgetpostStatusId] AND bps._rn = 1
        LEFT JOIN bbt_dedup bbt
            ON bbt.[BudgetbeløbTypeId] = bp.[BeloebstypeId] AND bbt._rn = 1;
    """
