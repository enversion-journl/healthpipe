# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact Update: UdredningsretMarkoer.

Grain: one row per delivered marker — the archive's own merge key
(markoer_noegle, loebenummer) carried through staging as
clean_code_UdredningsretMarkoer and minted in map.UdredningsretMarkoer. The
fact row and the archive row are therefore 1:1 by construction, and SDS'
OBJEKTID_FORLOBSMARKOR is not part of the key, so a re-issued marker UUID
updates markoer_uuid on the existing row instead of minting a second one.

This is the child event fact of fct.Udredningsforloeb. The parent key
dw_ek_Udredningsforloeb is resolved through map.Udredningsforloeb: a staged
(non-NULL) parent clean_code without a map member FAILS the run rather than
landing silently on the sentinel, so the fact can never hold an orphan
("0 foraeldreloese"). -1 means the marker carries no F_ID at all and therefore
belongs to no forloeb — design, not a failed lookup (decision U5, spec 4.2),
which is why that join is a LEFT JOIN with COALESCE(-1) and the DQA sentinel
cap for the role is set with headroom. The blank marker code gets the same
treatment: staging leaves clean_code_Forloebsmarkoer NULL for it. A soft-deleted
marker whose code no longer appears on any live archive row also lands on -1,
because map.Forloebsmarkoer is minted from the live rows only — see the UNMAPPED
comment below, which is why that one guard is restricted to the live rows.

No retirement step, unlike fct.Udredningsforloeb — and deliberately so. A
forloeb is a GROUPING that can vanish without any archive row vanishing (its
markers regroup under another start), so that fact has to retire what is no
longer staged. A marker has no such freedom: every marker row exists 1:1 in
arc.udredningsret_mont, staging carries the soft-deleted rows inside its own
scope (spec 3.5), and the archive never hard-deletes. A marker can therefore
only retire through its own staged dw_is_deleted, which the MERGE writes from
the source on every matched row — so it retires when the delivery drops it and
revives when the delivery brings it back.

Control total (built in): the live fact must equal the live archive — the row
count against the live rows of arc.udredningsret_mont, and
SUM(antal_kilderaekker) against the archive's own sum over those rows, which
reproduces the delivery's raw row count before duplicate reduction. The
comparison spans the WHOLE fact against the WHOLE archive, so it holds across
delta batches — it presupposes the fact was bootstrapped by a full-load
staging pass, which the deployment order guarantees. A deviation FAILS the run.

The counter control is metadata only and never fails the run: decision U6
keeps antal_markoerer in pre_stg (built from the archive, not from this fact),
and US-5 verifies instead that SUM(antal_markoerer) over the live forloeb fact
agrees with the live marker rows that have a forloeb. US-6 turns it into a DQA
subject. Reading the forloeb fact is why this asset declares a dep on it: both
facts live in group udredningsret_fact, so without that edge Dagster may build
this one first and the counter would compare against the previous run.

GDPR: no CPR reaches this layer — dw_ek_Borger arrives resolved from pre_stg,
and only counts are logged and reported.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.staging.udredningsret_kolonner import (
    FCT_FORLOEB_DWH_TABLE,
    FCT_MARKOER_DWH_TABLE,
    FCT_MARKOER_TABLE,
    MAP_FORLOEB_TABLE,
    MAP_FORLOEBSMARKOER_TABLE,
    MAP_HF_TABLE,
    MAP_MARKOER_TABLE,
)
from DSA.SQL.staging.udredningsret_staging_sql import ARC_TABLE

TEMP_TABLE = "#UdredningsretMarkoer_Facts"
GRAIN_COLUMN = "clean_code_UdredningsretMarkoer"

# Columns carried 1:1 from stg.fct_UdredningsretMarkoer into the fact. The
# staged table also holds borger_opslag, the raw SOR code, f_id, pk and its own
# dw_timestamp_update — diagnostics and staging bookkeeping the fact does not
# carry (the audit stamps are set server-side here) — plus markoer_kode and
# markoer_dato, which the fact deliberately drops: the code is
# dim.Forloebsmarkoer via dw_ek_Forloebsmarkoer and the date is
# dw_ek_Dato_markoer plus dw_haendelsestidspunkt. No degenerate copies.
STAGED_COLUMNS: tuple[str, ...] = (
    "dw_ek_Borger",
    "dw_ek_Organisation_forloeb",
    "dw_ek_Dato_markoer",
    "helbredsforloeb_id",
    "forloebselement_uuid",
    "markoer_uuid",
    "indgaar_i_indikator_H",
    "indgaar_i_indikator_L_N",
    "sygehusregion",
    "bopaelsregion",
    "antal_kilderaekker",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
)

# All non-PK fact columns, in fact-DDL order: the three map-resolved keys come
# first and the staged ones follow unbroken. Held to the DDL by a unit test.
FACT_COLUMNS: tuple[str, ...] = (
    "clean_code_UdredningsretMarkoer",
    "dw_ek_Udredningsforloeb",
    "dw_ek_Helbredsforloeb",
    "dw_ek_Forloebsmarkoer",
    *STAGED_COLUMNS,
)

_staged_selects = ",\n    ".join(f"src.[{c}]" for c in STAGED_COLUMNS)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in FACT_COLUMNS)
_insert_cols = ",\n        ".join(f"[{c}]" for c in FACT_COLUMNS)
_cmp_src = ",\n        ".join(f"src.[{c}]" for c in FACT_COLUMNS)
_cmp_tgt = ",\n        ".join(f"tgt.[{c}]" for c in FACT_COLUMNS)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in FACT_COLUMNS)

# Four counts in one round trip.
#   (a) and (b) are the INNER JOINs of RESOLVE_SQL below, so an unmapped
#       clean_code would silently DROP the row rather than resolve to -1.
#   (c) and (d) are the LEFT JOINs, where NULL is the legitimate sentinel case
#       (no F_ID / blank marker code) — so they count only the staged rows that
#       DO carry a clean_code. A non-NULL parent without a map member is the
#       "0 foraeldreloese" invariant: it must fail, never resolve quietly to -1.
#   (d) counts only the LIVE staged rows on top of that, because that is the
#       whole population map.Forloebsmarkoer can ever cover: the marker-code
#       map is fed from stg.dim_Forloebsmarkoer, built over the LIVE archive
#       rows alone (spec 3.4), while this staging deliberately carries live AND
#       soft-deleted rows (spec 3.5). A code that only ever appears on
#       soft-deleted rows can therefore never be minted, and without this
#       restriction the run would hard fail on a state no map run could repair
#       (the map is append-only, so a retry changes nothing). Such a retired
#       marker lands on COALESCE(-1) instead — the row is deleted anyway — and
#       the guard bites in full again the moment the code is delivered live.
#       The other three need no such restriction: map.UdredningsretMarkoer is
#       fed from this very staging table, map.Helbredsforloeb from
#       stg.pre_Helbredsforloeb (which also stages a helbredsforloeb with no
#       live rows at all), and (c)'s clean_code is read from
#       stg.pre_Udredningsforloeb, the same table map.Udredningsforloeb is
#       minted from — so a non-NULL value there always has a member.
UNMAPPED_SQL = f"""
SELECT
    (SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} s
     LEFT JOIN {MAP_MARKOER_TABLE} m
         ON m.[clean_code_UdredningsretMarkoer] = s.[clean_code_UdredningsretMarkoer]
     WHERE m.[dw_ek_UdredningsretMarkoer] IS NULL),
    (SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} s
     LEFT JOIN {MAP_HF_TABLE} m
         ON m.[clean_code_Helbredsforloeb] = s.[clean_code_Helbredsforloeb]
     WHERE m.[dw_ek_Helbredsforloeb] IS NULL),
    (SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} s
     LEFT JOIN {MAP_FORLOEB_TABLE} m
         ON m.[clean_code_Udredningsforloeb] = s.[clean_code_Udredningsforloeb]
     WHERE s.[clean_code_Udredningsforloeb] IS NOT NULL
       AND m.[dw_ek_Udredningsforloeb] IS NULL),
    (SELECT COUNT(*) FROM {FCT_MARKOER_TABLE} s
     LEFT JOIN {MAP_FORLOEBSMARKOER_TABLE} m
         ON m.[clean_code_Forloebsmarkoer] = s.[clean_code_Forloebsmarkoer]
     WHERE s.[clean_code_Forloebsmarkoer] IS NOT NULL
       AND s.[dw_is_deleted] = 0
       AND m.[dw_ek_Forloebsmarkoer] IS NULL);
"""

# The two LEFT JOINs never match on a NULL staged clean_code (NULL = anything is
# unknown in SQL Server), so COALESCE turns exactly the legitimate NULL cases
# into the sentinel — and only those, because a non-NULL clean_code without a
# map member has already failed the guard above.
RESOLVE_SQL = f"""
DROP TABLE IF EXISTS {TEMP_TABLE};

SELECT
    m.[dw_ek_UdredningsretMarkoer],
    src.[clean_code_UdredningsretMarkoer],
    COALESCE(u.[dw_ek_Udredningsforloeb], -1) AS [dw_ek_Udredningsforloeb],
    hf.[dw_ek_Helbredsforloeb],
    COALESCE(fm.[dw_ek_Forloebsmarkoer], -1) AS [dw_ek_Forloebsmarkoer],
    {_staged_selects}

INTO {TEMP_TABLE}
FROM {FCT_MARKOER_TABLE} src

INNER JOIN {MAP_MARKOER_TABLE} m
    ON m.[clean_code_UdredningsretMarkoer] = src.[clean_code_UdredningsretMarkoer]

INNER JOIN {MAP_HF_TABLE} hf
    ON hf.[clean_code_Helbredsforloeb] = src.[clean_code_Helbredsforloeb]

LEFT JOIN {MAP_FORLOEB_TABLE} u
    ON u.[clean_code_Udredningsforloeb] = src.[clean_code_Udredningsforloeb]

LEFT JOIN {MAP_FORLOEBSMARKOER_TABLE} fm
    ON fm.[clean_code_Forloebsmarkoer] = src.[clean_code_Forloebsmarkoer];

-- Clustered but NOT unique on purpose: a duplicated staging row must reach the
-- grain check below as a counted ValueError, not die as a failed index build.
CREATE CLUSTERED INDEX [IX_UdredningsretMarkoer_Facts]
    ON {TEMP_TABLE}([dw_ek_UdredningsretMarkoer]);
"""

GRAIN_CHECK_SQL = f"SELECT COUNT(*), COUNT(DISTINCT [{GRAIN_COLUMN}]) FROM {TEMP_TABLE}"

# Facts carry no sentinel row, so any row at all means the MERGE path.
TARGET_HAS_ROWS_SQL = (
    f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {FCT_MARKOER_DWH_TABLE}) THEN 1 ELSE 0 END"
)

# WHEN MATCHED writes dw_is_deleted from the source, so a marker the delivery
# dropped retires and one it brings back revives — the only retirement path
# this fact has (see the module docstring).
MERGE_SQL = f"""
MERGE {FCT_MARKOER_DWH_TABLE} AS tgt
USING {TEMP_TABLE} AS src
ON tgt.[dw_ek_UdredningsretMarkoer] = src.[dw_ek_UdredningsretMarkoer]

WHEN MATCHED AND EXISTS (
    SELECT
        {_cmp_src}
    EXCEPT
    SELECT
        {_cmp_tgt}
) THEN
    UPDATE SET
        {_updates},
        tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))

WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        [dw_ek_UdredningsretMarkoer],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_UdredningsretMarkoer],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO {FCT_MARKOER_DWH_TABLE} WITH (TABLOCK) (
    [dw_ek_UdredningsretMarkoer],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_UdredningsretMarkoer],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM {TEMP_TABLE} src;
"""

# Whole live fact vs whole live archive, so the total holds across delta
# batches. The grain is 1:1 with the archive row, so the row count needs no
# grouping — and antal_kilderaekker carries the delivery's raw row count, so its
# sum must match too: a marker the load dropped would move the first number, a
# duplicate-reduction that drifted would move the second. The sums are cast to
# BIGINT because SUM over INT overflows at 2^31 on a big enough delivery.
KONTROLTOTAL_SQL = f"""
WITH arkiv AS (
    SELECT
        COUNT(*) AS [antal],
        ISNULL(SUM(CAST(a.[antal_kilderaekker] AS BIGINT)), 0) AS [kilderaekker]
    FROM {ARC_TABLE} AS a
    WHERE a.[dw_is_deleted] = 0
),
fakta AS (
    SELECT
        COUNT(*) AS [antal],
        ISNULL(SUM(CAST([antal_kilderaekker] AS BIGINT)), 0) AS [kilderaekker]
    FROM {FCT_MARKOER_DWH_TABLE}
    WHERE [dw_is_deleted] = 0
)
SELECT
    f.[antal] AS [fct_antal],
    a.[antal] AS [arc_antal],
    f.[kilderaekker] AS [fct_kilderaekker],
    a.[kilderaekker] AS [arc_kilderaekker]
FROM fakta AS f
CROSS JOIN arkiv AS a;
"""

# Fact FK roles whose -1 share is reported per run, in DDL order. dw_ek_Borger
# also has the -2 blank sentinel (structurally absent CPR), counted separately;
# dw_ek_Udredningsforloeb's -1 is reported under its own name as well, because
# it is design (a marker without F_ID, decision U5) and not an unresolved key.
FK_ROLES: tuple[str, ...] = (
    "dw_ek_Udredningsforloeb",
    "dw_ek_Helbredsforloeb",
    "dw_ek_Forloebsmarkoer",
    "dw_ek_Borger",
    "dw_ek_Organisation_forloeb",
    "dw_ek_Dato_markoer",
)

_fk_selects = ",\n    ".join(
    f"SUM(CASE WHEN [{c}] = -1 THEN 1 ELSE 0 END) AS [{c}]" for c in FK_ROLES
)
FK_DIAGNOSTIK_SQL = f"""
SELECT
    COUNT(*) AS [total],
    SUM(CASE WHEN [dw_ek_Borger] = -2 THEN 1 ELSE 0 END) AS [borger_tom],
    {_fk_selects}
FROM {FCT_MARKOER_DWH_TABLE}
WHERE [dw_is_deleted] = 0;
"""

# Metadata only — NEVER a hard fail. The two numbers are built from different
# sources on purpose (decision U6): antal_markoerer is counted in pre_stg
# straight from the archive, so the forloeb fact never has to read this one.
# Agreement is the evidence that both readings of "the markers of a forloeb"
# describe the same rows; the E2E asserts equality on synthetic data and US-6
# turns it into a DQA subject.
TAELLER_KONTROL_SQL = f"""
SELECT
    (SELECT ISNULL(SUM(CAST([antal_markoerer] AS BIGINT)), 0)
     FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0),
    (SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE}
     WHERE [dw_is_deleted] = 0 AND [dw_ek_Udredningsforloeb] <> -1);
"""


@asset(
    group_name="udredningsret_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["udredningsret", "stg_fct_UdredningsretMarkoer"]),
        AssetKey(["map_UdredningsretMarkoer"]),
        AssetKey(["map_Helbredsforloeb"]),
        AssetKey(["map_Udredningsforloeb"]),
        AssetKey(["map_Forloebsmarkoer"]),
        # The control total reads the archive directly (live rows).
        AssetKey(["arc_udredningsret_mont"]),
        # The counter control reads fct.Udredningsforloeb, so this fact must run
        # AFTER it: without the edge both facts sit in group udredningsret_fact
        # with nothing ordering them, and the counter would compare this run's
        # marker rows against the PREVIOUS run's antal_markoerer. No cycle — the
        # forloeb fact reads stg, pre_stg, its maps, arc and its own target,
        # never this fact.
        AssetKey(["fct_Udredningsforloeb"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "markoer",
    },
    description=(
        "Event fact for udredningsret markers — one row per delivered marker, "
        "map lookups on materialised clean_codes, MERGE on the "
        "redelivery-stable dw_ek_UdredningsretMarkoer, soft-delete carried "
        "straight through from the archive, control total against the archive"
    ),
)
def fct_UdredningsretMarkoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct", "UdredningsretMarkoer", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {FCT_MARKOER_TABLE}")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging batch: {staging_count:,} rows")

                cursor.execute(UNMAPPED_SQL)
                markoer_unmapped, hf_unmapped, forloeb_unmapped, kode_unmapped = (
                    int(v or 0) for v in cursor.fetchone()
                )
                if markoer_unmapped + hf_unmapped + forloeb_unmapped + kode_unmapped > 0:
                    raise ValueError(
                        f"fct_UdredningsretMarkoer: {markoer_unmapped} staging rows have no "
                        f"match in map.UdredningsretMarkoer, {hf_unmapped} none in "
                        f"map.Helbredsforloeb and {kode_unmapped} none in "
                        "map.Forloebsmarkoer. Run map_UdredningsretMarkoer, "
                        "map_Helbredsforloeb and map_Forloebsmarkoer first. "
                        f"{forloeb_unmapped} rows carry a forloeb key that "
                        "map.Udredningsforloeb does not know: a staged parent MUST exist "
                        "(0 foraeldreloese) — run map_Udredningsforloeb first, and if it "
                        "is already current, the staged forloeb itself is missing and the "
                        "marker must not be resolved to -1, which means 'no F_ID at all'."
                    )

                cursor.execute(RESOLVE_SQL)

                cursor.execute(GRAIN_CHECK_SQL)
                total_rows, distinct_codes = cursor.fetchone()
                if total_rows != distinct_codes:
                    raise ValueError(
                        f"fct_UdredningsretMarkoer: grain violated — "
                        f"{total_rows - distinct_codes} duplicate rows per {GRAIN_COLUMN} "
                        "in the resolve set. Expected one row per delivered marker."
                    )

                cursor.execute(TARGET_HAS_ROWS_SQL)
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")
                cursor.execute(f"UPDATE STATISTICS {FCT_MARKOER_DWH_TABLE}")
                # UPDATE STATISTICS is transactional in SQL Server, and the
                # connection is not autocommit (get_connection only closes it),
                # so without this commit the refreshed statistics are rolled
                # back on close and never persist.
                conn.commit()

                cursor.execute(KONTROLTOTAL_SQL)
                fct_antal, arc_antal, fct_kilderaekker, arc_kilderaekker = (
                    int(v or 0) for v in cursor.fetchone()
                )

                cursor.execute(FK_DIAGNOSTIK_SQL)
                diag_row = cursor.fetchone()

                cursor.execute(TAELLER_KONTROL_SQL)
                forloeb_taeller, markoerer_med_forloeb = (int(v or 0) for v in cursor.fetchone())
            finally:
                cursor.close()

        total = diag_row[0] or 0
        borger_tom = diag_row[1] or 0
        fk_ukendt = {col: (diag_row[i + 2] or 0) for i, col in enumerate(FK_ROLES)}
        uden_forloeb = fk_ukendt["dw_ek_Udredningsforloeb"]
        taeller_afvigelse = forloeb_taeller - markoerer_med_forloeb
        context.log.info(
            f"fct.UdredningsretMarkoer: {rows_affected:,} rows processed, {total:,} live, "
            f"{uden_forloeb:,} without a forloeb (design), "
            f"{fct_kilderaekker:,} source rows; counter control: "
            f"{forloeb_taeller:,} antal_markoerer vs {markoerer_med_forloeb:,} marker rows "
            f"with a forloeb (difference {taeller_afvigelse:,}); "
            "-1 pr. rolle: " + ", ".join(f"{k}={v:,}" for k, v in fk_ukendt.items())
        )

        if fct_antal != arc_antal or fct_kilderaekker != arc_kilderaekker:
            dfm.fail_dfm_run(run_ctx.log_id, error_message="kontroltotal afviger")
            raise dg.Failure(
                description=(
                    "fct_UdredningsretMarkoer: kontroltotalen holder ikke — facten har "
                    f"{fct_antal:,} levende raekker mod arkivets {arc_antal:,}, og "
                    f"{fct_kilderaekker:,} kilderaekker mod arkivets {arc_kilderaekker:,}."
                ),
                metadata={
                    "fct_antal": fct_antal,
                    "arc_antal": arc_antal,
                    "fct_kilderaekker": fct_kilderaekker,
                    "arc_kilderaekker": arc_kilderaekker,
                },
            )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "UdredningsretMarkoer", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "UdredningsretMarkoer")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except dg.Failure:
        raise
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "rows_affected": rows_affected,
            "staging_batch": staging_count,
            "total_live_rows": total,
            "sum_antal_kilderaekker": fct_kilderaekker,
            # Design, not an unresolved key: the marker carries no F_ID at all
            # (decision U5), which is why it is named apart from the -1 counts.
            "markoerer_uden_forloeb": uden_forloeb,
            # Constant by construction: a deviation raises dg.Failure above, so
            # this key only ever reports the green run. It is a marker for the
            # run log, never a measurement to assert on.
            "kontroltotal_afvigelser": arc_antal - fct_antal,
            # Metadata only (decision U6) — reported, never enforced here.
            "antal_markoerer_forloebsfact": forloeb_taeller,
            "markoerer_med_forloeb": markoerer_med_forloeb,
            "taeller_afvigelse": taeller_afvigelse,
            "borger_tom_cpr": borger_tom,
            **{f"ukendt_{k}": v for k, v in fk_ukendt.items()},
            "table": "fct.UdredningsretMarkoer",
            "grain": "markoer via clean_code_UdredningsretMarkoer",
            "staging_source": "stg.fct_UdredningsretMarkoer",
            "mode": config.mode,
        }
    )
