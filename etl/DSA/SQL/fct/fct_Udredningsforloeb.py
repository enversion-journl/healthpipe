# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact Update: Udredningsforloeb.

Grain: (helbredsforloeb, udredningsforloeb) — one row per forloeb built in
staging from the live markers of one helbredsforloeb. The merge key is
dw_ek_Udredningsforloeb, minted in map.Udredningsforloeb on a clean_code that
never carries SDS' F_ID/PK (both are renumbered per delivery), so a redelivery
updates the same row instead of re-keying it.

Reads stg.fct_Udredningsforloeb (typed measures, resolved Borger/Organisation/
Dato keys, materialised clean_codes) and resolves the three remaining keys from
map.Udredningsforloeb, map.Helbredsforloeb and map.UdredningsretKlassifikation.
dim.UdredningsretEgenskaber shares this PK, so the fact carries no
dw_ek_UdredningsretEgenskaber column.

Retirement (spec section 6): a forloeb that vanishes from a delivery — every
marker soft-deleted, or the markers regrouped under another start — keeps its
row and is marked dw_is_deleted = 1. The retirement is scoped to the
helbredsforloeb staged in this run (stg.pre_Helbredsforloeb through
map.Helbredsforloeb), so a delta never retires forloeb outside its own scope,
and it runs on BOTH load paths. A clean_code that comes back is revived by the
MERGE, which writes dw_is_deleted from the source. Rows are never deleted.

Control total (built in): the live fact must equal the live archive — the row
count against the number of (helbredsforloeb_noegle, F_ID) pairs over live
arc rows with a non-blank F_ID (exactly the grain stg.pre_Udredningsforloeb
builds), and SUM(indgaar_i_indikator) against the number of those pairs
carrying both a henvist and a non-henvist marker with MRK_H = '1'. The
comparison spans the WHOLE fact against the WHOLE archive, so it holds across
delta batches — it presupposes the fact was bootstrapped by a full-load
staging pass, which the deployment order guarantees. A deviation FAILS the run.
It compares two scalars, though, so it is a tripwire for the population as a
whole and not a per-helbredsforloeb reconciliation: a split and a merge of
forloeb in the same window cancel out and the run goes green with both wrong
(decision U13). The cause-agnostic reconciliation — a FULL OUTER JOIN on
helbredsforloeb_noegle — is a planned US-6 DQA subject.

GDPR: no CPR reaches this layer — dw_ek_Borger arrives resolved from pre_stg,
and only counts are logged and reported.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import DATO_BLANK_KEY
from DSA.SQL.staging.udredningsret_kolonner import (
    FCT_FORLOEB_DWH_TABLE,
    FCT_FORLOEB_TABLE,
    MAP_FORLOEB_TABLE,
    MAP_HF_TABLE,
    MAP_KLASSIFIKATION_TABLE,
    PRE_HF_TABLE,
    henvist_predicate_sql,
)
from DSA.SQL.staging.udredningsret_staging_sql import ARC_TABLE

TEMP_TABLE = "#Udredningsforloeb_Facts"
GRAIN_COLUMN = "clean_code_Udredningsforloeb"

# Columns carried 1:1 from stg.fct_Udredningsforloeb into the fact. The staged
# table also holds borger_opslag, the raw SOR codes and its own
# dw_timestamp_update — diagnostics and staging bookkeeping that the fact does
# not carry (the audit stamps are set server-side here).
STAGED_COLUMNS: tuple[str, ...] = (
    "dw_ek_Borger",
    "dw_ek_Organisation_forloeb",
    "dw_ek_Organisation_kontakt",
    "dw_ek_Dato_start",
    "dw_ek_Dato_slut",
    "dw_ek_Dato_opgoerelse",
    "udredningstid_dage",
    "indgaar_i_indikator",
    "overholdt",
    "inden_30_dage",
    "alder_ved_henvisning",
    "aktionsdiagnose_kode",
    "diagnosekontakt_uuid",
    "helbredsforloeb_id",
    "antal_markoerer",
    "antal_udredningsplaner",
    "antal_afbrydelser",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
)

# All non-PK fact columns, in fact-DDL order (the map-resolved keys are
# interleaved with the staged ones, so the order is spelled out rather than
# concatenated). Held to the DDL by a unit test.
FACT_COLUMNS: tuple[str, ...] = (
    "clean_code_Udredningsforloeb",
    "dw_ek_Helbredsforloeb",
    "dw_ek_Borger",
    "dw_ek_Organisation_forloeb",
    "dw_ek_Organisation_kontakt",
    "dw_ek_UdredningsretKlassifikation",
    "dw_ek_Dato_start",
    "dw_ek_Dato_slut",
    "dw_ek_Dato_opgoerelse",
    "udredningstid_dage",
    "indgaar_i_indikator",
    "overholdt",
    "inden_30_dage",
    "alder_ved_henvisning",
    "aktionsdiagnose_kode",
    "diagnosekontakt_uuid",
    "helbredsforloeb_id",
    "antal_markoerer",
    "antal_udredningsplaner",
    "antal_afbrydelser",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
)

_staged_selects = ",\n    ".join(f"src.[{c}]" for c in STAGED_COLUMNS)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in FACT_COLUMNS)
_insert_cols = ",\n        ".join(f"[{c}]" for c in FACT_COLUMNS)
_cmp_src = ",\n        ".join(f"src.[{c}]" for c in FACT_COLUMNS)
_cmp_tgt = ",\n        ".join(f"tgt.[{c}]" for c in FACT_COLUMNS)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in FACT_COLUMNS)

# Three counts in one round trip: staged clean_codes without a map member.
# Every one of them is an INNER JOIN in RESOLVE_SQL below, so an unmapped
# clean_code would silently DROP the row rather than resolve to -1 — hence the
# hard fail naming the map to run first.
UNMAPPED_SQL = f"""
SELECT
    (SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} s
     LEFT JOIN {MAP_FORLOEB_TABLE} m
         ON m.[clean_code_Udredningsforloeb] = s.[clean_code_Udredningsforloeb]
     WHERE m.[dw_ek_Udredningsforloeb] IS NULL),
    (SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} s
     LEFT JOIN {MAP_HF_TABLE} m
         ON m.[clean_code_Helbredsforloeb] = s.[clean_code_Helbredsforloeb]
     WHERE m.[dw_ek_Helbredsforloeb] IS NULL),
    (SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE} s
     LEFT JOIN {MAP_KLASSIFIKATION_TABLE} m
         ON m.[clean_code_UdredningsretKlassifikation]
             = s.[clean_code_UdredningsretKlassifikation]
     WHERE m.[dw_ek_UdredningsretKlassifikation] IS NULL);
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS {TEMP_TABLE};

SELECT
    m.[dw_ek_Udredningsforloeb],
    src.[clean_code_Udredningsforloeb],
    hf.[dw_ek_Helbredsforloeb],
    klass.[dw_ek_UdredningsretKlassifikation],
    {_staged_selects}

INTO {TEMP_TABLE}
FROM {FCT_FORLOEB_TABLE} src

INNER JOIN {MAP_FORLOEB_TABLE} m
    ON m.[clean_code_Udredningsforloeb] = src.[clean_code_Udredningsforloeb]

INNER JOIN {MAP_HF_TABLE} hf
    ON hf.[clean_code_Helbredsforloeb] = src.[clean_code_Helbredsforloeb]

INNER JOIN {MAP_KLASSIFIKATION_TABLE} klass
    ON klass.[clean_code_UdredningsretKlassifikation]
        = src.[clean_code_UdredningsretKlassifikation];

-- Clustered but NOT unique on purpose: a duplicated staging row must reach the
-- grain check below as a counted ValueError, not die as a failed index build.
CREATE CLUSTERED INDEX [IX_Udredningsforloeb_Facts]
    ON {TEMP_TABLE}([dw_ek_Udredningsforloeb]);
"""

GRAIN_CHECK_SQL = f"SELECT COUNT(*), COUNT(DISTINCT [{GRAIN_COLUMN}]) FROM {TEMP_TABLE}"

# Facts carry no sentinel row, so any row at all means the MERGE path.
TARGET_HAS_ROWS_SQL = (
    f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {FCT_FORLOEB_DWH_TABLE}) THEN 1 ELSE 0 END"
)

MERGE_SQL = f"""
MERGE {FCT_FORLOEB_DWH_TABLE} AS tgt
USING {TEMP_TABLE} AS src
ON tgt.[dw_ek_Udredningsforloeb] = src.[dw_ek_Udredningsforloeb]

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
        [dw_ek_Udredningsforloeb],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_Udredningsforloeb],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO {FCT_FORLOEB_DWH_TABLE} WITH (TABLOCK) (
    [dw_ek_Udredningsforloeb],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_Udredningsforloeb],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM {TEMP_TABLE} src;
"""

# Spec section 6: retire the live fact rows of the helbredsforloeb staged this
# run whose forloeb is NOT in the batch — the forloeb vanished (all markers
# soft-deleted) or was regrouped under another start and re-keyed. Scoping
# through stg.pre_Helbredsforloeb keeps a delta from touching anything outside
# its own window; the MERGE above revives a row that comes back.
RETIRE_SQL = f"""
UPDATE f
SET f.[dw_is_deleted] = 1,
    f.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM {FCT_FORLOEB_DWH_TABLE} AS f
INNER JOIN {MAP_HF_TABLE} AS m
    ON m.[dw_ek_Helbredsforloeb] = f.[dw_ek_Helbredsforloeb]
INNER JOIN {PRE_HF_TABLE} AS h
    ON h.[clean_code_Helbredsforloeb] = m.[clean_code_Helbredsforloeb]
WHERE f.[dw_is_deleted] = 0
  AND NOT EXISTS (
      SELECT 1 FROM {TEMP_TABLE} AS s
      WHERE s.[dw_ek_Udredningsforloeb] = f.[dw_ek_Udredningsforloeb]
  );
"""

# Whole live fact vs whole live archive, so the total holds across delta
# batches. The archive is grouped on (helbredsforloeb_noegle, F_ID) — the grain
# stg.pre_Udredningsforloeb builds — rather than on F_ID alone, which SDS
# renumbers per delivery and which can repeat across helbredsforloeb. The
# indicator rule is the pre_stg expression shape verbatim: mrk_h_1 and
# er_henvist are CASE ... ELSE 0, so a NULL marker code falls in the ELSE and
# counts as not-henvist exactly as the marker group does in staging.
_MRK_H_1 = "CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END"
_ER_HENVIST = (
    f"CASE WHEN {henvist_predicate_sql('LTRIM(RTRIM(a.[FORLOEBSMARKOER]))')} THEN 1 ELSE 0 END"
)

KONTROLTOTAL_SQL = f"""
WITH arkiv_markoerer AS (
    SELECT
        a.[helbredsforloeb_noegle],
        LTRIM(RTRIM(a.[F_ID])) AS [f_id],
        {_MRK_H_1} AS [mrk_h_1],
        {_ER_HENVIST} AS [er_henvist]
    FROM {ARC_TABLE} AS a
    WHERE a.[dw_is_deleted] = 0
      AND a.[F_ID] IS NOT NULL AND LTRIM(RTRIM(a.[F_ID])) <> ''
),
arkiv_forloeb AS (
    SELECT
        MAX(CASE WHEN m.[mrk_h_1] = 1 AND m.[er_henvist] = 1 THEN 1 ELSE 0 END) AS [har_henvist],
        MAX(CASE WHEN m.[mrk_h_1] = 1 AND m.[er_henvist] = 0 THEN 1 ELSE 0 END) AS [har_slut]
    FROM arkiv_markoerer AS m
    GROUP BY m.[helbredsforloeb_noegle], m.[f_id]
),
arkiv AS (
    SELECT
        COUNT(*) AS [antal],
        SUM(CASE WHEN g.[har_henvist] = 1 AND g.[har_slut] = 1 THEN 1 ELSE 0 END) AS [indikator]
    FROM arkiv_forloeb AS g
),
fakta AS (
    SELECT
        COUNT(*) AS [antal],
        ISNULL(SUM(CAST([indgaar_i_indikator] AS INT)), 0) AS [indikator]
    FROM {FCT_FORLOEB_DWH_TABLE}
    WHERE [dw_is_deleted] = 0
)
SELECT
    f.[antal] AS [fct_antal],
    ISNULL(a.[antal], 0) AS [arc_antal],
    f.[indikator] AS [fct_indikator],
    ISNULL(a.[indikator], 0) AS [arc_indikator]
FROM fakta AS f
CROSS JOIN arkiv AS a;
"""

# Fact FK roles whose -1 share is reported per run. dw_ek_Borger also has the
# -2 blank sentinel (structurally absent CPR), counted separately; the two
# dates that legitimately land on BLANK (no end marker / no reporting quarter,
# decision U2) are counted on their own key rather than as -1.
FK_ROLES: tuple[str, ...] = (
    "dw_ek_Borger",
    "dw_ek_Helbredsforloeb",
    "dw_ek_Organisation_forloeb",
    "dw_ek_Organisation_kontakt",
    "dw_ek_UdredningsretKlassifikation",
    "dw_ek_Dato_start",
    "dw_ek_Dato_slut",
    "dw_ek_Dato_opgoerelse",
)

_fk_selects = ",\n    ".join(
    f"SUM(CASE WHEN [{c}] = -1 THEN 1 ELSE 0 END) AS [{c}]" for c in FK_ROLES
)
FK_DIAGNOSTIK_SQL = f"""
SELECT
    COUNT(*) AS [total],
    SUM(CASE WHEN [dw_ek_Borger] = -2 THEN 1 ELSE 0 END) AS [borger_tom],
    {_fk_selects},
    SUM(CASE WHEN [dw_ek_Dato_slut] = {DATO_BLANK_KEY} THEN 1 ELSE 0 END) AS [dato_slut_blank],
    SUM(CASE WHEN [dw_ek_Dato_opgoerelse] = {DATO_BLANK_KEY} THEN 1 ELSE 0 END)
        AS [dato_opgoerelse_blank]
FROM {FCT_FORLOEB_DWH_TABLE}
WHERE [dw_is_deleted] = 0;
"""


@asset(
    group_name="udredningsret_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["udredningsret", "stg_fct_Udredningsforloeb"]),
        # The retirement scope is the staged helbredsforloeb of this run.
        AssetKey(["udredningsret", "pre_stg_Helbredsforloeb"]),
        AssetKey(["map_Udredningsforloeb"]),
        AssetKey(["map_Helbredsforloeb"]),
        AssetKey(["map_UdredningsretKlassifikation"]),
        # The control total reads the archive directly (live rows with F_ID).
        AssetKey(["arc_udredningsret_mont"]),
    ],
    tags={
        "domain": "udredningsret",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "helbredsforloeb_x_udredningsforloeb",
    },
    description=(
        "Fact table for udredningsforloeb — map lookups on materialised "
        "clean_codes, MERGE on the redelivery-stable dw_ek_Udredningsforloeb, "
        "retirement of vanished forloeb in scope, control total against the archive"
    ),
)
def fct_Udredningsforloeb(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct", "Udredningsforloeb", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {FCT_FORLOEB_TABLE}")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging batch: {staging_count:,} rows")

                cursor.execute(UNMAPPED_SQL)
                forloeb_unmapped, hf_unmapped, klass_unmapped = (
                    int(v or 0) for v in cursor.fetchone()
                )
                if forloeb_unmapped + hf_unmapped + klass_unmapped > 0:
                    raise ValueError(
                        f"fct_Udredningsforloeb: {forloeb_unmapped} staging rows have no match "
                        f"in map.Udredningsforloeb, {hf_unmapped} none in map.Helbredsforloeb "
                        f"and {klass_unmapped} none in map.UdredningsretKlassifikation. Run "
                        "map_Udredningsforloeb, map_Helbredsforloeb and "
                        "map_UdredningsretKlassifikation first."
                    )

                cursor.execute(RESOLVE_SQL)

                cursor.execute(GRAIN_CHECK_SQL)
                total_rows, distinct_codes = cursor.fetchone()
                if total_rows != distinct_codes:
                    raise ValueError(
                        f"fct_Udredningsforloeb: grain violated — {total_rows - distinct_codes} "
                        f"duplicate rows per {GRAIN_COLUMN} in the resolve set. Expected one "
                        "row per (helbredsforloeb, udredningsforloeb)."
                    )

                cursor.execute(TARGET_HAS_ROWS_SQL)
                if cursor.fetchone()[0]:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount

                # Always, on both load paths: the bulk INSERT only runs against
                # an empty target (nothing to retire), but a rebuild over a
                # populated target takes the MERGE path and still has to retire
                # the forloeb that no longer exist.
                cursor.execute(RETIRE_SQL)
                retireret = max(cursor.rowcount, 0)
                conn.commit()

                cursor.execute(f"DROP TABLE IF EXISTS {TEMP_TABLE}")
                cursor.execute(f"UPDATE STATISTICS {FCT_FORLOEB_DWH_TABLE}")
                # UPDATE STATISTICS is transactional in SQL Server, and the
                # connection is not autocommit (get_connection only closes it),
                # so without this commit the refreshed statistics are rolled
                # back on close and never persist.
                conn.commit()

                cursor.execute(KONTROLTOTAL_SQL)
                fct_antal, arc_antal, fct_indikator, arc_indikator = (
                    int(v or 0) for v in cursor.fetchone()
                )

                cursor.execute(FK_DIAGNOSTIK_SQL)
                diag_row = cursor.fetchone()
            finally:
                cursor.close()

        total = diag_row[0] or 0
        borger_tom = diag_row[1] or 0
        fk_ukendt = {col: (diag_row[i + 2] or 0) for i, col in enumerate(FK_ROLES)}
        dato_slut_blank = diag_row[2 + len(FK_ROLES)] or 0
        dato_opgoerelse_blank = diag_row[3 + len(FK_ROLES)] or 0
        context.log.info(
            f"fct.Udredningsforloeb: {rows_affected:,} rows processed, {total:,} live, "
            f"{retireret:,} retired, {fct_indikator:,} in the indicator; BLANK dato: "
            f"slut={dato_slut_blank:,}, opgoerelse={dato_opgoerelse_blank:,}; "
            "-1 pr. rolle: " + ", ".join(f"{k}={v:,}" for k, v in fk_ukendt.items())
        )

        if fct_antal != arc_antal or fct_indikator != arc_indikator:
            dfm.fail_dfm_run(run_ctx.log_id, error_message="kontroltotal afviger")
            raise dg.Failure(
                description=(
                    "fct_Udredningsforloeb: kontroltotalen holder ikke — facten har "
                    f"{fct_antal:,} levende raekker mod arkivets {arc_antal:,} "
                    f"(helbredsforloeb, F_ID)-par, og {fct_indikator:,} indikatorforloeb "
                    f"mod arkivets {arc_indikator:,}. Hyppigste aarsag: SDS har omgrupperet "
                    "markoerer mellem F_ID uden anden indholdsaendring (U1). Afhjaelpning: "
                    "koer udredningsret_publish_job med mode=full_load."
                ),
                metadata={
                    "fct_antal": fct_antal,
                    "arc_antal": arc_antal,
                    "fct_indikator": fct_indikator,
                    "arc_indikator": arc_indikator,
                },
            )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Udredningsforloeb", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Udredningsforloeb")
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
            "retireret": retireret,
            "indikatorforloeb": fct_indikator,
            # The measured difference, not a literal: a deviation raises
            # dg.Failure above, so a run that gets here reports 0 — but the 0
            # now comes from the two counts the check actually compared, so the
            # key cannot silently disagree with them if the check is ever
            # softened or its grain changes (U13).
            "kontroltotal_afvigelser": arc_antal - fct_antal,
            "borger_tom_cpr": borger_tom,
            **{f"ukendt_{k}": v for k, v in fk_ukendt.items()},
            "dato_slut_blank": dato_slut_blank,
            "dato_opgoerelse_blank": dato_opgoerelse_blank,
            "table": "fct.Udredningsforloeb",
            "grain": "(helbredsforloeb, udredningsforloeb) via clean_code_Udredningsforloeb",
            "staging_source": "stg.fct_Udredningsforloeb",
            "mode": config.mode,
        }
    )
