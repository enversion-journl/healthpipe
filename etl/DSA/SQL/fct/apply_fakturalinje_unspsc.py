# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
fct_apply_fakturalinje_unspsc — apply-pass for dw_ek_UNSPSC on fct.Fakturalinje.

After ref.KMDFakturalinjeUNSPSCMatch and fct.Fakturalinje are materialised,
this pass resolves tiered UNSPSC matches and stamps dw_ek_UNSPSC on changed rows.

Delta (see build_apply_delta_sql): driven ENTIRELY off ref.FakturalinjeMatchKeys —
  - match-key rows changed since the apply checkpoint, plus
  - match-key rows whose KMD keys appear in ref rows changed since the checkpoint.

Scope: only fakturalinjer with faktura_dato >= APPLY_PASS_MIN_FAKTURA_DATE.
MySupply goes further back than KMD UdData posting data. The scope column is
denormalised onto ref.FakturalinjeMatchKeys so the delta never touches the fact.

Only rows where dw_ek_UNSPSC actually changes are touched (watermark-safe). There is
no separate demote pass: a line whose match was soft-deleted re-enters the delta via
the ref-change branch, resolves to -1 through the dw_is_deleted = 0 tier joins, and
REFRESH writes that -1 back.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.unspsc_match_shared import sql_match_currency

# Apply-pass scope: earliest faktura_dato (invoice date) to consider for UNSPSC resolution.
# KMD UdData spend/posting coverage starts around 2024; MySupply fakturalinjer can go
# much further back. Rows before this date can never get a KMD match — skip them in the
# afterburner delta scan. Adjust here when KMD coverage window changes.
APPLY_PASS_MIN_FAKTURA_DATE = "2023-01-01"

# Checkpoint stamp for the NEXT run's delta.
#
# Both watermark columns this pass compares against — ref.FakturalinjeMatchKeys and
# ref.KMDFakturalinjeUNSPSCMatch — are datetime2(0), written as
# CAST(SYSDATETIME() AS datetime2(0)), which ROUNDS: a write at 17:20:18.4 is stored
# as 17:20:18. A full-precision checkpoint would sit above such a value and skip it
# permanently, and there is no longer a fact-side watermark or demote pass to catch it
# on a later run. Floor to the second and keep one second of margin; the cost is one
# second of re-processed delta per run.
RUN_STARTED_AT_SQL = (
    "SELECT CONVERT(NVARCHAR(50), CAST(DATEADD(SECOND, -1, SYSDATETIME()) AS datetime2(0)), 121)"
)

REFRESH_SQL = """
UPDATE f
SET f.[dw_ek_UNSPSC] = r.[dw_ek_UNSPSC],
    f.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM [fct].[Fakturalinje] f
INNER JOIN #UnspscResolved r
    ON r.[dw_ek_Fakturalinje] = f.[dw_ek_Fakturalinje]
WHERE f.[dw_ek_UNSPSC] <> r.[dw_ek_UNSPSC];
"""


def build_unspsc_resolve_expression(*, match_alias: str = "kmd") -> str:
    """COALESCE tier 1→3 for primary clean_code_kreditor branch."""
    return f"""COALESCE(
        {match_alias}1.[dw_ek_UNSPSC],
        {match_alias}2.[dw_ek_UNSPSC],
        {match_alias}3.[dw_ek_UNSPSC],
        -1
    )"""


def build_apply_scope_predicate(*, alias: str = "mk") -> str:
    """APPLY_PASS_MIN_FAKTURA_DATE scope on a denormalised dw_ek_Dato_faktura.

    -1 is dim.Dato's UNKNOWN sentinel (source date NULL or unparseable) and is kept
    in scope: an unknown date must not silently exclude a line from resolution.
    """
    min_key = (
        f"CAST(CONVERT(CHAR(8), CAST('{APPLY_PASS_MIN_FAKTURA_DATE}' AS datetime2), 112) AS INT)"
    )
    return f"""(
        {alias}.[dw_ek_Dato_faktura] >= {min_key}
        OR {alias}.[dw_ek_Dato_faktura] = -1
    )"""


def build_apply_delta_sql(*, checkpoint_param: str) -> str:
    """Materialise the delta match-key rows into #delta.

    Two temp tables, four seeks, no full scan of either base table:

    #RefChangedKeys — the distinct (kreditor, faktura) keys of ref rows changed since
    the checkpoint. Deliberately NOT filtered on dw_is_deleted: a soft-deletion must
    trigger a re-resolve so the tier joins in build_apply_resolve_sql (which DO filter
    dw_is_deleted = 0) can fall the line back to -1. That is exactly why none of the
    table's filtered (`WHERE dw_is_deleted = 0`) indexes can serve this predicate — a
    filtered index is only usable when its predicate is implied by the query's. It
    seeks IX_ref_KMDFakturalinjeUNSPSCMatch_delta instead, which is unfiltered and
    INCLUDEs both clean_code columns for this query. Without that index the only access
    path is the clustered PK: the *result* would still be bounded by the ref delta, but
    the *work* would be the whole ref table, on every run.

    #delta — the match-key rows to re-resolve. The three candidate branches are kept
    as separate UNION ALL legs rather than one disjunctive predicate, so each gets its
    own index seek: the watermark leg uses IX_ref_fakturalinje_matchkeys_timestamp, and
    the two ref-change legs use IX_ref_fakturalinje_matchkeys_primary /
    IX_ref_fakturalinje_matchkeys_header. The header leg correlates on
    clean_code_Kreditor_header against the SAME ref key pair, mirroring the kmdh* tier
    joins. The invoice-date scope is applied once on the outer row.
    """
    scope = build_apply_scope_predicate(alias="mk")

    return f"""
DROP TABLE IF EXISTS #RefChangedKeys;
DROP TABLE IF EXISTS #delta;

SELECT DISTINCT
    r.[clean_code_kreditor],
    r.[clean_code_faktura]
INTO #RefChangedKeys
FROM [ref].[KMDFakturalinjeUNSPSCMatch] r
WHERE r.[dw_timestamp_update] > '{checkpoint_param}';

SELECT
    mk.[dw_ek_Fakturalinje],
    mk.[clean_code_Kreditor],
    mk.[clean_code_Kreditor_header],
    mk.[clean_code_Faktura],
    mk.[linje_beloeb],
    mk.[enheds_pris],
    mk.[antal_enheder_faktureret],
    mk.[faktura_total]
INTO #delta
FROM [ref].[FakturalinjeMatchKeys] mk
WHERE mk.[dw_ek_Fakturalinje] IN (
        SELECT w.[dw_ek_Fakturalinje]
        FROM [ref].[FakturalinjeMatchKeys] w
        WHERE w.[dw_timestamp_update] > '{checkpoint_param}'
    UNION ALL
        SELECT p.[dw_ek_Fakturalinje]
        FROM #RefChangedKeys c
        INNER JOIN [ref].[FakturalinjeMatchKeys] p
            ON p.[clean_code_Kreditor] = c.[clean_code_kreditor]
           AND p.[clean_code_Faktura] = c.[clean_code_faktura]
    UNION ALL
        SELECT h.[dw_ek_Fakturalinje]
        FROM #RefChangedKeys c
        INNER JOIN [ref].[FakturalinjeMatchKeys] h
            ON h.[clean_code_Kreditor_header] = c.[clean_code_kreditor]
           AND h.[clean_code_Faktura] = c.[clean_code_faktura]
)
AND {scope};

CREATE UNIQUE CLUSTERED INDEX [IX_delta_Fakturalinje] ON #delta ([dw_ek_Fakturalinje]);
"""


def build_apply_resolve_sql() -> str:
    """Resolve dw_ek_UNSPSC for every #delta row via the tiered ref match.

    Reads #delta (built by build_apply_delta_sql) instead of joining
    ref.FakturalinjeMatchKeys to fct.Fakturalinje: the delta is already scoped and
    keyed, so the fact table is not touched until REFRESH_SQL. Takes no checkpoint —
    the whole delta definition lives in build_apply_delta_sql.

    Every tier join keeps dw_is_deleted = 0, so a line only ever resolves to a
    still-live match; a line whose match was soft-deleted resolves to -1 here and is
    reset by REFRESH_SQL. That is what replaces the old separate demote pass.
    """
    resolve_primary = build_unspsc_resolve_expression(match_alias="kmd")
    resolve_header = build_unspsc_resolve_expression(match_alias="kmdh")
    mk_linje = sql_match_currency("mk.[linje_beloeb]")
    mk_enhed = sql_match_currency("mk.[enheds_pris]")
    mk_total = sql_match_currency("mk.[faktura_total]")

    return f"""
DROP TABLE IF EXISTS #UnspscResolved;

SELECT
    mk.[dw_ek_Fakturalinje],
    COALESCE(
        CASE WHEN {resolve_primary} <> -1 THEN {resolve_primary} END,
        CASE WHEN {resolve_header} <> -1 THEN {resolve_header} END,
        -1
    ) AS dw_ek_UNSPSC
INTO #UnspscResolved
FROM #delta mk
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd1
    ON kmd1.[match_tier] = 1
   AND kmd1.[dw_is_deleted] = 0
   AND kmd1.[clean_code_kreditor] = mk.[clean_code_Kreditor]
   AND kmd1.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmd1.[line_amount_sum] = {mk_linje}
   AND kmd1.[invoiced_quantity] = TRY_CONVERT(DECIMAL(18, 4), mk.[antal_enheder_faktureret])
   AND kmd1.[unit_price] = {mk_enhed}
   AND mk.[clean_code_Kreditor] IS NOT NULL AND mk.[clean_code_Kreditor] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> ''
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd2
    ON kmd2.[match_tier] = 2
   AND kmd2.[dw_is_deleted] = 0
   AND kmd2.[clean_code_kreditor] = mk.[clean_code_Kreditor]
   AND kmd2.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmd2.[line_amount_sum] = {mk_linje}
   AND kmd2.[invoiced_quantity] = TRY_CONVERT(DECIMAL(18, 4), mk.[antal_enheder_faktureret])
   AND mk.[clean_code_Kreditor] IS NOT NULL AND mk.[clean_code_Kreditor] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> ''
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmd3
    ON kmd3.[match_tier] = 3
   AND kmd3.[dw_is_deleted] = 0
   AND kmd3.[clean_code_kreditor] = mk.[clean_code_Kreditor]
   AND kmd3.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmd3.[invoice_total] = {mk_total}
   AND mk.[linje_beloeb] IS NULL
   AND mk.[enheds_pris] IS NULL
   AND mk.[clean_code_Kreditor] IS NOT NULL AND mk.[clean_code_Kreditor] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> ''
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmdh1
    ON kmdh1.[match_tier] = 1
   AND kmdh1.[dw_is_deleted] = 0
   AND kmdh1.[clean_code_kreditor] = mk.[clean_code_Kreditor_header]
   AND kmdh1.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmdh1.[line_amount_sum] = {mk_linje}
   AND kmdh1.[invoiced_quantity] = TRY_CONVERT(DECIMAL(18, 4), mk.[antal_enheder_faktureret])
   AND kmdh1.[unit_price] = {mk_enhed}
   AND mk.[clean_code_Kreditor_header] IS NOT NULL AND mk.[clean_code_Kreditor_header] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> ''
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmdh2
    ON kmdh2.[match_tier] = 2
   AND kmdh2.[dw_is_deleted] = 0
   AND kmdh2.[clean_code_kreditor] = mk.[clean_code_Kreditor_header]
   AND kmdh2.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmdh2.[line_amount_sum] = {mk_linje}
   AND kmdh2.[invoiced_quantity] = TRY_CONVERT(DECIMAL(18, 4), mk.[antal_enheder_faktureret])
   AND mk.[clean_code_Kreditor_header] IS NOT NULL AND mk.[clean_code_Kreditor_header] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> ''
LEFT JOIN [ref].[KMDFakturalinjeUNSPSCMatch] kmdh3
    ON kmdh3.[match_tier] = 3
   AND kmdh3.[dw_is_deleted] = 0
   AND kmdh3.[clean_code_kreditor] = mk.[clean_code_Kreditor_header]
   AND kmdh3.[clean_code_faktura] = mk.[clean_code_Faktura]
   AND kmdh3.[invoice_total] = {mk_total}
   AND mk.[linje_beloeb] IS NULL
   AND mk.[enheds_pris] IS NULL
   AND mk.[clean_code_Kreditor_header] IS NOT NULL AND mk.[clean_code_Kreditor_header] <> ''
   AND mk.[clean_code_Faktura] IS NOT NULL AND mk.[clean_code_Faktura] <> '';

CREATE CLUSTERED INDEX [IX_UnspscResolved_Fakturalinje]
    ON #UnspscResolved ([dw_ek_Fakturalinje]);
"""


@asset(
    name="fct_apply_fakturalinje_unspsc",
    group_name="erp_ref",
    compute_kind="mssql",
    deps=[
        AssetKey(["fct_Fakturalinje"]),
        AssetKey(["erp", "ref_kmd_fakturalinje_unspsc_match"]),
        AssetKey(["erp", "ref_fakturalinje_matchkeys"]),
    ],
    tags={"layer": "fct", "module": "erp", "domain": "erp"},
    description=(
        "Apply-pass: stamp dw_ek_UNSPSC on fct.Fakturalinje from "
        "ref.KMDFakturalinjeUNSPSCMatch (delta from ref.FakturalinjeMatchKeys)."
    ),
)
def fct_apply_fakturalinje_unspsc(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Refresh fct.Fakturalinje.dw_ek_UNSPSC from the KMD ref match table."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dfm",
        "fakturalinje_unspsc_apply",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint
    cp_param = checkpoint or "1900-01-01"

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(RUN_STARTED_AT_SQL)
                run_started_at = cursor.fetchone()[0]

                cursor.execute(build_apply_delta_sql(checkpoint_param=cp_param))
                cursor.execute("SELECT COUNT(*) FROM #delta")
                delta_rows = cursor.fetchone()[0]

                cursor.execute(build_apply_resolve_sql())
                cursor.execute("SELECT COUNT(*) FROM #UnspscResolved WHERE [dw_ek_UNSPSC] <> -1")
                candidate_hits = cursor.fetchone()[0]

                cursor.execute(REFRESH_SQL)
                refreshed = cursor.rowcount

                conn.commit()

                context.log.info(
                    f"UNSPSC apply-pass: {delta_rows} delta match-key rows, "
                    f"{candidate_hits} resolved candidates, {refreshed} refreshed "
                    f"(checkpoint {checkpoint})"
                )
            finally:
                cursor.close()

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint(
                "dfm",
                "fakturalinje_unspsc_apply",
                value=run_started_at,
                mode=config.mode,
            )
            new_checkpoint = dfm.get_checkpoint("dfm", "fakturalinje_unspsc_apply")

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=refreshed,
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "delta_rows": delta_rows,
            "candidate_hits": candidate_hits,
            "rows_refreshed": refreshed,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "min_faktura_date": APPLY_PASS_MIN_FAKTURA_DATE,
            "table": "fct.Fakturalinje (dw_ek_UNSPSC apply)",
        }
    )
