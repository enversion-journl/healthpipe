# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Fact Update: DRGKontakt.

Grain: (leverancetype, takstsystem, kontakt) — every combination of contact
and takstsystem is its own row. The merge key is dw_ek_DRGKontakt, minted in
map.DRGKontakt on the uuid-based clean_code, so year revisions and
redeliveries update in place and never re-key.

Reads stg.fct_DRGKontakt (typed measures, resolved Borger/Organisation/Dato
keys, materialised dimension clean_codes) and resolves the remaining keys
from the maps. Soft-deleted contacts are staged through and retire their
fact row via dw_is_deleted — rows are never deleted.

Control total (built in): live row counts and SUM(totalpris_drg) in the fact
must equal the live archive per (leverancetype, takstsystem, aktivitetsaar);
a deviation FAILS the run. The comparison spans the WHOLE fact against the
WHOLE archive, so it holds across delta batches — it presupposes the fact
was bootstrapped by a full-load staging pass, which the deployment order
guarantees.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Columns carried 1:1 from stg.fct_DRGKontakt into the fact.
STAGED_COLUMNS: tuple[str, ...] = (
    "dw_ek_Borger",
    "dw_ek_Organisation_sygehus",
    "dw_ek_Organisation_kontaktansvar",
    "dw_ek_Organisation_forloebselement",
    "dw_ek_Organisation_henvisende",
    "dw_ek_Dato_ind",
    "dato_tid_ind",
    "dw_ek_Dato_ud",
    "dato_tid_ud",
    "dw_ek_Dato_henvisning",
    "pris_drg",
    "langliggerpris",
    "totalpris_drg",
    "langliggerdage",
    "liggedage",
    "kontaktdage",
    "varighed_time",
    "forloeb_totalpris",
    "forloeb_pris_drg",
    "forloeb_langliggerpris",
    "forloeb_loensumafgift_pris",
    "forloeb_loensumafgift_langliggerpris",
    "forloeb_antal_kontakter",
    "forloeb_mrk_naerhed_1",
    "forloeb_mrk_naerhed_2",
    "forloeb_mrk_naerhed_4",
    "forloeb_mrk_kirurgi",
    "forloeb_pris_psykiatriforloeb",
    "forloeb_dtd_justeret_takst",
    "forloeb_loensumafgift_pris_psyk",
    "forloeb_liggedage",
    "forloeb_antal_kontakter_psyk",
    "tungeste_drgkontakt",
    "udfaldsgivende_kontakt",
    "dw_haendelsestidspunkt",
    "dw_is_deleted",
)

# All non-PK fact columns, in fact-DDL order.
FACT_COLUMNS: tuple[str, ...] = (
    "clean_code_DRGKontakt",
    "leverancetype",
    "takstsystem",
    "aktivitetsaar",
    "dw_ek_DRGKontaktKlassifikation",
    "dw_ek_DRGDiagnoseGruppe",
    "dw_ek_DRGProcedureGruppe",
    *STAGED_COLUMNS,
)

# The two code-set group FKs are owned SOLELY by the per-type refresh below,
# not by the MERGE: a delta that changes only one code type (or the header)
# leaves the contact out of the OTHER type's group staging, so letting the
# MERGE write it would clobber the still-correct key to the -1 empty group.
# They are still set on INSERT (a brand-new row must carry a value).
GROUP_FK_COLUMNS: tuple[str, ...] = ("dw_ek_DRGDiagnoseGruppe", "dw_ek_DRGProcedureGruppe")
MERGE_COLUMNS: tuple[str, ...] = tuple(c for c in FACT_COLUMNS if c not in GROUP_FK_COLUMNS)

_staged_selects = ",\n    ".join(f"src.[{c}]" for c in STAGED_COLUMNS)
_src_cols = ",\n        ".join(f"src.[{c}]" for c in FACT_COLUMNS)
_insert_cols = ",\n        ".join(f"[{c}]" for c in FACT_COLUMNS)
_cmp_src = ",\n        ".join(f"src.[{c}]" for c in MERGE_COLUMNS)
_cmp_tgt = ",\n        ".join(f"tgt.[{c}]" for c in MERGE_COLUMNS)
_updates = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in MERGE_COLUMNS)

UNMAPPED_SQL = """
SELECT COUNT(*)
FROM [stg].[fct_DRGKontakt] src
LEFT JOIN [map].[DRGKontakt] m
    ON m.[clean_code_DRGKontakt] = src.[clean_code_DRGKontakt]
WHERE m.[dw_ek_DRGKontakt] IS NULL;
"""

# A staged group hash that the group map doesn't know is a hard error (run the
# group maps first) — without this the RESOLVE COALESCE would silently fold it
# into the -1 empty group, indistinguishable from a contact with no codes.
GROUP_UNMAPPED_SQL = """
SELECT
    (SELECT COUNT(*) FROM [stg].[DRGKontaktDiagnoseGruppe] s
     LEFT JOIN [map].[DRGDiagnoseGruppe] m
         ON m.[clean_code_DRGDiagnoseGruppe] = s.[clean_code_DRGDiagnoseGruppe]
     WHERE m.[dw_ek_DRGDiagnoseGruppe] IS NULL),
    (SELECT COUNT(*) FROM [stg].[DRGKontaktProcedureGruppe] s
     LEFT JOIN [map].[DRGProcedureGruppe] m
         ON m.[clean_code_DRGProcedureGruppe] = s.[clean_code_DRGProcedureGruppe]
     WHERE m.[dw_ek_DRGProcedureGruppe] IS NULL);
"""

RESOLVE_SQL = f"""
DROP TABLE IF EXISTS #DRGKontakt_Facts;

SELECT
    m.[dw_ek_DRGKontakt],
    src.[clean_code_DRGKontakt],
    src.[leverancetype],
    src.[takstsystem],
    TRY_CONVERT(SMALLINT, src.[aktivitetsaar]) AS [aktivitetsaar],
    COALESCE(klass_map.[dw_ek_DRGKontaktKlassifikation], -1)
        AS [dw_ek_DRGKontaktKlassifikation],
    -- Code-set group FKs: a contact with no diagnoses/procedures has no group
    -- staging row and resolves to the -1 empty group.
    COALESCE(diag_grp_map.[dw_ek_DRGDiagnoseGruppe], -1) AS [dw_ek_DRGDiagnoseGruppe],
    COALESCE(proc_grp_map.[dw_ek_DRGProcedureGruppe], -1) AS [dw_ek_DRGProcedureGruppe],
    {_staged_selects}

INTO #DRGKontakt_Facts
FROM [stg].[fct_DRGKontakt] src

INNER JOIN [map].[DRGKontakt] m
    ON m.[clean_code_DRGKontakt] = src.[clean_code_DRGKontakt]

LEFT JOIN [map].[DRGKontaktKlassifikation] klass_map
    ON klass_map.[clean_code_DRGKontaktKlassifikation]
        = src.[clean_code_DRGKontaktKlassifikation]
   AND src.[clean_code_DRGKontaktKlassifikation] IS NOT NULL

LEFT JOIN [stg].[DRGKontaktDiagnoseGruppe] diag_grp
    ON diag_grp.[clean_code_DRGKontakt] = src.[clean_code_DRGKontakt]
LEFT JOIN [map].[DRGDiagnoseGruppe] diag_grp_map
    ON diag_grp_map.[clean_code_DRGDiagnoseGruppe] = diag_grp.[clean_code_DRGDiagnoseGruppe]

LEFT JOIN [stg].[DRGKontaktProcedureGruppe] proc_grp
    ON proc_grp.[clean_code_DRGKontakt] = src.[clean_code_DRGKontakt]
LEFT JOIN [map].[DRGProcedureGruppe] proc_grp_map
    ON proc_grp_map.[clean_code_DRGProcedureGruppe] = proc_grp.[clean_code_DRGProcedureGruppe];
"""

# The MERGE never writes the group FKs (see GROUP_FK_COLUMNS). These refreshes
# own them: for every contact in the per-type code scope (which includes ones
# whose registrations were all retracted -> back to the -1 empty group), set the
# group FK from its current set. Scoped per code type, so a change to one type
# never touches the other type's key. Run whenever the MERGE path is taken (a
# non-empty target); the empty-target INSERT already carries the resolved FKs.
REFRESH_DIAGNOSE_GRUPPE_SQL = """
UPDATE f
SET f.[dw_ek_DRGDiagnoseGruppe] = COALESCE(mg.[dw_ek_DRGDiagnoseGruppe], -1),
    f.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM [fct].[DRGKontakt] f
INNER JOIN [stg].[bridge_DRGKontaktDiagnose_scope] sc
    ON sc.[clean_code_DRGKontakt] = f.[clean_code_DRGKontakt]
LEFT JOIN [stg].[DRGKontaktDiagnoseGruppe] g
    ON g.[clean_code_DRGKontakt] = f.[clean_code_DRGKontakt]
LEFT JOIN [map].[DRGDiagnoseGruppe] mg
    ON mg.[clean_code_DRGDiagnoseGruppe] = g.[clean_code_DRGDiagnoseGruppe]
WHERE f.[dw_ek_DRGDiagnoseGruppe] <> COALESCE(mg.[dw_ek_DRGDiagnoseGruppe], -1);
"""

REFRESH_PROCEDURE_GRUPPE_SQL = """
UPDATE f
SET f.[dw_ek_DRGProcedureGruppe] = COALESCE(mg.[dw_ek_DRGProcedureGruppe], -1),
    f.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
FROM [fct].[DRGKontakt] f
INNER JOIN [stg].[bridge_DRGKontaktProcedure_scope] sc
    ON sc.[clean_code_DRGKontakt] = f.[clean_code_DRGKontakt]
LEFT JOIN [stg].[DRGKontaktProcedureGruppe] g
    ON g.[clean_code_DRGKontakt] = f.[clean_code_DRGKontakt]
LEFT JOIN [map].[DRGProcedureGruppe] mg
    ON mg.[clean_code_DRGProcedureGruppe] = g.[clean_code_DRGProcedureGruppe]
WHERE f.[dw_ek_DRGProcedureGruppe] <> COALESCE(mg.[dw_ek_DRGProcedureGruppe], -1);
"""

MERGE_SQL = f"""
MERGE [fct].[DRGKontakt] AS tgt
USING #DRGKontakt_Facts AS src
ON tgt.[dw_ek_DRGKontakt] = src.[dw_ek_DRGKontakt]

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
        [dw_ek_DRGKontakt],
        {_insert_cols},
        [dw_timestamp_insert],
        [dw_timestamp_update]
    )
    VALUES (
        src.[dw_ek_DRGKontakt],
        {_src_cols},
        CAST(SYSDATETIME() AS datetime2(0)),
        CAST(SYSDATETIME() AS datetime2(0))
    );
"""

INSERT_SQL = f"""
INSERT INTO [fct].[DRGKontakt] WITH (TABLOCK) (
    [dw_ek_DRGKontakt],
    {_insert_cols},
    [dw_timestamp_insert],
    [dw_timestamp_update]
)
SELECT
    src.[dw_ek_DRGKontakt],
    {_src_cols},
    CAST(SYSDATETIME() AS datetime2(0)),
    CAST(SYSDATETIME() AS datetime2(0))
FROM #DRGKontakt_Facts src;
"""

# Live fact vs live archive per (leverancetype, takstsystem, aktivitetsaar).
# The archive's TOTALPRIS_DRG converts exactly like staging did, so the sums
# are comparable to the decimal; psyk has no totalpris and compares as 0=0.
KONTROLTOTAL_SQL = """
WITH fakta AS (
    SELECT [leverancetype], [takstsystem], [aktivitetsaar],
           COUNT(*) AS [antal],
           ISNULL(SUM([totalpris_drg]), 0) AS [totalpris]
    FROM [fct].[DRGKontakt]
    WHERE [dw_is_deleted] = 0
    GROUP BY [leverancetype], [takstsystem], [aktivitetsaar]
),
arkiv AS (
    SELECT 'DRG' AS [leverancetype], [takstsystem],
           TRY_CONVERT(SMALLINT, [AKTIVITETSAAR]) AS [aktivitetsaar],
           COUNT(*) AS [antal],
           ISNULL(SUM(TRY_CONVERT(DECIMAL(19,4), [TOTALPRIS_DRG])), 0) AS [totalpris]
    FROM [arc].[drg_kontakt]
    WHERE [dw_is_deleted] = 0
    GROUP BY [takstsystem], TRY_CONVERT(SMALLINT, [AKTIVITETSAAR])
    UNION ALL
    -- psyk contacts carry no activity year (the column is absent from the
    -- delivery), so their slice is (takstsystem) alone with a NULL year
    SELECT 'DRGPSYK', [takstsystem],
           CAST(NULL AS SMALLINT),
           COUNT(*),
           0
    FROM [arc].[drgpsyk_kontakt]
    WHERE [dw_is_deleted] = 0
    GROUP BY [takstsystem]
)
SELECT
    COALESCE(f.[leverancetype], a.[leverancetype]) AS [leverancetype],
    COALESCE(f.[takstsystem], a.[takstsystem]) AS [takstsystem],
    COALESCE(f.[aktivitetsaar], a.[aktivitetsaar]) AS [aktivitetsaar],
    ISNULL(f.[antal], 0) AS [fct_antal],
    ISNULL(a.[antal], 0) AS [arc_antal],
    ISNULL(f.[totalpris], 0) AS [fct_totalpris],
    ISNULL(a.[totalpris], 0) AS [arc_totalpris]
FROM fakta f
FULL OUTER JOIN arkiv a
    ON a.[leverancetype] = f.[leverancetype]
   AND a.[takstsystem] = f.[takstsystem]
   AND ISNULL(a.[aktivitetsaar], -1) = ISNULL(f.[aktivitetsaar], -1)
WHERE ISNULL(f.[antal], 0) <> ISNULL(a.[antal], 0)
   OR ISNULL(f.[totalpris], 0) <> ISNULL(a.[totalpris], 0);
"""

# Fact FK roles whose -1 share is reported per run. dw_ek_Borger also has the
# -2 blank sentinel (structurally absent CPR), counted separately.
FK_ROLES: tuple[str, ...] = (
    "dw_ek_Borger",
    "dw_ek_DRGKontaktKlassifikation",
    "dw_ek_Organisation_sygehus",
    "dw_ek_Organisation_kontaktansvar",
    "dw_ek_Organisation_forloebselement",
    "dw_ek_Organisation_henvisende",
    "dw_ek_Dato_ind",
    "dw_ek_Dato_ud",
    "dw_ek_Dato_henvisning",
)

_fk_selects = ",\n    ".join(
    f"SUM(CASE WHEN [{c}] = -1 THEN 1 ELSE 0 END) AS [{c}]" for c in FK_ROLES
)
FK_DIAGNOSTIK_SQL = f"""
SELECT
    COUNT(*) AS [total],
    SUM(CASE WHEN [dw_ek_Borger] = -2 THEN 1 ELSE 0 END) AS [borger_tom],
    {_fk_selects},
    SUM(CASE WHEN [dw_ek_DRGDiagnoseGruppe] = -1 THEN 1 ELSE 0 END) AS [diagnose_gruppe_tom],
    SUM(CASE WHEN [dw_ek_DRGProcedureGruppe] = -1 THEN 1 ELSE 0 END) AS [procedure_gruppe_tom]
FROM [fct].[DRGKontakt]
WHERE [dw_is_deleted] = 0;
"""


@asset(
    group_name="drg_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["drg", "stg_fct_DRGKontakt"]),
        AssetKey(["map_DRGKontakt"]),
        AssetKey(["map_DRGKontaktKlassifikation"]),
        # Group-FK resolution + the refresh read the group staging, the group
        # maps, and the per-type scope tables (built by stg_bridge_*).
        AssetKey(["drg", "stg_bridge_DRGKontaktDiagnose"]),
        AssetKey(["drg", "stg_bridge_DRGKontaktProcedure"]),
        AssetKey(["drg", "stg_DRGDiagnoseGruppe"]),
        AssetKey(["drg", "stg_DRGProcedureGruppe"]),
        AssetKey(["map_DRGDiagnoseGruppe"]),
        AssetKey(["map_DRGProcedureGruppe"]),
    ],
    tags={
        "domain": "drg",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "kontakt_x_takstsystem",
    },
    description=(
        "Fact table for DRG-grouped contacts — map lookups on materialised "
        "clean_codes, MERGE on the redelivery-stable dw_ek_DRGKontakt, "
        "control total against the archive"
    ),
)
def fct_DRGKontakt(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fct", "DRGKontakt", dagster_run_id=context.run_id, mode=config.mode
    )

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT COUNT(*) FROM [stg].[fct_DRGKontakt]")
                staging_count = cursor.fetchone()[0]
                context.log.info(f"Staging batch: {staging_count:,} rows")

                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"fct_DRGKontakt: {unmapped_rows} staging rows have no match in "
                        "map.DRGKontakt. Run map_DRGKontakt first."
                    )

                cursor.execute(GROUP_UNMAPPED_SQL)
                diag_grp_unmapped, proc_grp_unmapped = cursor.fetchone()
                if (diag_grp_unmapped or 0) + (proc_grp_unmapped or 0) > 0:
                    raise ValueError(
                        f"fct_DRGKontakt: {diag_grp_unmapped or 0} diagnose- and "
                        f"{proc_grp_unmapped or 0} procedure-group hashes are staged but not in "
                        "map.DRG*Gruppe. Run the group maps first; the group FKs must not "
                        "silently fold into the -1 empty group."
                    )

                cursor.execute(RESOLVE_SQL)

                cursor.execute(
                    "SELECT COUNT(*), COUNT(DISTINCT [clean_code_DRGKontakt]) "
                    "FROM #DRGKontakt_Facts"
                )
                total_rows, distinct_codes = cursor.fetchone()
                if total_rows != distinct_codes:
                    raise ValueError(
                        f"fct_DRGKontakt: grain violated — {total_rows - distinct_codes} "
                        "duplicate rows per clean_code_DRGKontakt in the resolve set. "
                        "Expected one row per (leverancetype, takstsystem, kontakt)."
                    )

                # Short-circuit: an empty target takes a minimally logged bulk
                # INSERT; facts carry no sentinel row, so any row means MERGE.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[DRGKontakt]) "
                    "THEN 1 ELSE 0 END"
                )
                used_merge = bool(cursor.fetchone()[0])
                if used_merge:
                    context.log.info("Target has existing rows - MERGE upsert path")
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)

                rows_affected = cursor.rowcount
                conn.commit()

                # The MERGE never writes the group FKs, so the per-type refresh
                # owns them whenever the MERGE path ran (every delta, and a
                # full_load over a non-empty target). The empty-target INSERT
                # already carried the resolved FKs, so it needs no refresh.
                if used_merge:
                    cursor.execute(REFRESH_DIAGNOSE_GRUPPE_SQL)
                    cursor.execute(REFRESH_PROCEDURE_GRUPPE_SQL)
                    conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #DRGKontakt_Facts")
                cursor.execute("UPDATE STATISTICS [fct].[DRGKontakt]")
                # UPDATE STATISTICS is transactional in SQL Server, and the
                # connection is not autocommit (get_connection only closes it),
                # so without this commit the refreshed statistics are rolled
                # back on close and never persist.
                conn.commit()

                cursor.execute(KONTROLTOTAL_SQL)
                afvigelser = cursor.fetchall()

                cursor.execute(FK_DIAGNOSTIK_SQL)
                diag_row = cursor.fetchone()
            finally:
                cursor.close()

        total = diag_row[0] or 0
        borger_tom = diag_row[1] or 0
        fk_ukendt = {col: (diag_row[i + 2] or 0) for i, col in enumerate(FK_ROLES)}
        # -1 here is the legitimate empty group (a contact with no codes), not an
        # unresolved key — reported separately so its share stays visible.
        diagnose_gruppe_tom = diag_row[2 + len(FK_ROLES)] or 0
        procedure_gruppe_tom = diag_row[3 + len(FK_ROLES)] or 0
        context.log.info(
            f"fct.DRGKontakt: {rows_affected:,} rows processed, {total:,} live; "
            f"tom gruppe: diagnose={diagnose_gruppe_tom:,}, procedure={procedure_gruppe_tom:,}; "
            f"-1 pr. rolle: " + ", ".join(f"{k}={v:,}" for k, v in fk_ukendt.items())
        )

        if afvigelser:
            preview = [
                f"({lt}, {ts}, {aar}): fct {fa:,}/{ft} vs arc {aa:,}/{at}"
                for lt, ts, aar, fa, aa, ft, at in afvigelser[:5]
            ]
            dfm.fail_dfm_run(run_ctx.log_id, error_message="kontroltotal afviger")
            raise dg.Failure(
                description=(
                    f"fct_DRGKontakt: kontroltotalen holder ikke — {len(afvigelser)} "
                    f"(leverancetype, takstsystem, aktivitetsaar)-skiver afviger "
                    f"mellem fct og arc. Foerste: {'; '.join(preview)}"
                ),
                metadata={"afvigende_skiver": len(afvigelser)},
            )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "DRGKontakt", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "DRGKontakt")
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
            "kontroltotal_afvigelser": 0,
            "borger_tom_cpr": borger_tom,
            **{f"ukendt_{k}": v for k, v in fk_ukendt.items()},
            "tom_diagnose_gruppe": diagnose_gruppe_tom,
            "tom_procedure_gruppe": procedure_gruppe_tom,
            "table": "fct.DRGKontakt",
            "grain": "(leverancetype, takstsystem, kontakt) via clean_code_DRGKontakt",
            "staging_source": "stg.fct_DRGKontakt",
        }
    )
