# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: ILSLeverandoer from ILS (Dimension Layer)

One row per ILS supplier number (dbo.lev.lnr) from arc.ils_lev, full picture
every run (no checkpoint filter), like stg_dim_Aftale_ils.

Three candidate CVR codes are staged for the dim.Kreditor outrigger, one per
matchable lev column (role suffix = the column): clean_code_Kreditor_se_nr,
clean_code_Kreditor_cvr_nr and clean_code_Kreditor_lnr. dim_ILSLeverandoer
resolves them in that order. Rows with giro = 'Person' get no candidates: their
se_nr holds birth dates or CPR numbers, not a CVR.

arc.ils_lev upserts on prrowid, so several archive rows can sit on the same lnr
(Progress recreates a record under a new rowid and the old row stays,
detect_deletes=False). The newest archive row wins; sysdato and prrowid DESC
are deterministic tiebreakers.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.ils_leverandoer_clean_code import (
    sql_clean_code_ils_leverandoer,
    sql_kreditor_clean_code_from_lnr,
)
from utils.sql.tax_id_sql import sql_cvr_cleancode_from_se_nr

CLEAN_CODE_SQL = sql_clean_code_ils_leverandoer("lev.lnr")


def _unless_person(candidate: str) -> str:
    """NULL for giro = 'Person' rows, else the candidate CVR code."""
    return f"CASE WHEN lev.giro = 'Person' THEN NULL ELSE {candidate} END"


def _text(column: str) -> str:
    """Trimmed text attribute; blank collapses to NULL."""
    return f"NULLIF(LTRIM(RTRIM({column})), '')"


def _dato_tid(dato: str, klk: str) -> str:
    """ILS date + seconds-after-midnight as one DATETIME2 (date only when no time)."""
    return f"""CASE
                WHEN {dato} IS NOT NULL AND {klk} IS NOT NULL
                THEN DATEADD(SECOND, {klk}, CAST({dato} AS DATETIME2))
                WHEN {dato} IS NOT NULL
                THEN CAST({dato} AS DATETIME2)
                ELSE NULL
            END"""


STG_SQL = f"""
    SELECT
        CAST(clean_code_ILSLeverandoer AS NVARCHAR(255)) AS clean_code_ILSLeverandoer,
        leverandoernummer,
        CAST(clean_code_Kreditor_se_nr AS NVARCHAR(255)) AS clean_code_Kreditor_se_nr,
        CAST(clean_code_Kreditor_cvr_nr AS NVARCHAR(255)) AS clean_code_Kreditor_cvr_nr,
        CAST(clean_code_Kreditor_lnr AS NVARCHAR(255)) AS clean_code_Kreditor_lnr,
        leverandoernavn,
        adresse,
        adresse2,
        land,
        postnummer,
        giro,
        aktiv,
        standard_leveringsdage,
        dato_tid_oprettet,
        opr_bruger,
        dato_tid_ajourfoert,
        ajourfoert_bruger
    INTO [stg].[dim_ILSLeverandoer]
    FROM (
        SELECT
            {CLEAN_CODE_SQL} AS clean_code_ILSLeverandoer,
            CAST(UPPER(LTRIM(RTRIM(lev.lnr))) AS NVARCHAR(100)) AS leverandoernummer,
            {_unless_person(sql_cvr_cleancode_from_se_nr("lev.se_nr"))}
                AS clean_code_Kreditor_se_nr,
            {_unless_person(sql_cvr_cleancode_from_se_nr("lev.cvr_nr"))}
                AS clean_code_Kreditor_cvr_nr,
            {_unless_person(sql_kreditor_clean_code_from_lnr("lev.lnr", "lev.land"))}
                AS clean_code_Kreditor_lnr,
            {_text("lev.navn")} AS leverandoernavn,
            {_text("lev.adr")} AS adresse,
            {_text("lev.adr2")} AS adresse2,
            {_text("lev.land")} AS land,
            {_text("lev.postdi")} AS postnummer,
            -- giro is kept verbatim: the datamarts' Person masking matches
            -- giro = 'Person' exactly, so it must not be reshaped here.
            NULLIF(lev.giro, '') AS giro,
            lev.aktiv,
            -- The supplier's default delivery time; not lafth.deflevdage (the
            -- agreement's) nor lgr.deflevdage (dim.Lager.standard_leveringsdage).
            lev.deflevdage AS standard_leveringsdage,
            {_dato_tid("lev.oprdto", "lev.oprtid")} AS dato_tid_oprettet,
            UPPER({_text("lev.oprbruger")}) AS opr_bruger,
            {_dato_tid("lev.sysdato", "lev.sysklk")} AS dato_tid_ajourfoert,
            UPPER({_text("lev.bruger")}) AS ajourfoert_bruger,
            ROW_NUMBER() OVER (
                PARTITION BY {CLEAN_CODE_SQL}
                ORDER BY lev.dw_timestamp_update DESC, lev.sysdato DESC, lev.prrowid DESC
            ) AS rn
        FROM [arc].[ils_lev] lev
    ) ranked
    WHERE rn = 1
      AND clean_code_ILSLeverandoer IS NOT NULL;
"""


@asset(
    key=AssetKey(["erp", "stg_dim_ILSLeverandoer"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["arc_ils_lev"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "ilsleverandoer",
    },
    description="Stage ILS suppliers at lnr grain from arc.ils_lev (newest archive row per lnr)",
)
def stg_dim_ILSLeverandoer(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Create staging table for the ILS supplier dimension with one row per lnr."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_ILSLeverandoer",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    stats = (0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    "Creating staging table stg.dim_ILSLeverandoer from arc.ils_lev..."
                )

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_ILSLeverandoer];")
                cursor.execute(STG_SQL)

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_ILSLeverandoer with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_ILSLeverandoer_CleanCode
                    ON [stg].[dim_ILSLeverandoer](clean_code_ILSLeverandoer);
                """
                )

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COALESCE(SUM(CASE WHEN clean_code_Kreditor_se_nr IS NULL
                                           AND clean_code_Kreditor_cvr_nr IS NULL
                                           AND clean_code_Kreditor_lnr IS NULL
                                          THEN 1 ELSE 0 END), 0) AS uden_cvr_kandidat,
                        COALESCE(SUM(CASE WHEN giro = 'Person' THEN 1 ELSE 0 END), 0) AS personer,
                        COALESCE(SUM(CASE WHEN aktiv = 1 THEN 1 ELSE 0 END), 0) AS aktive
                    FROM [stg].[dim_ILSLeverandoer]
                """
                )
                stats = tuple(v or 0 for v in cursor.fetchone())
                context.log.info(
                    f"Coverage: {stats[0]} lnr — uden CVR-kandidat={stats[1]}, "
                    f"giro='Person'={stats[2]}, aktive={stats[3]}"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_ILSLeverandoer",
            "row_count": row_count,
            "uden_cvr_kandidat": stats[1],
            "personer": stats[2],
            "aktive": stats[3],
            "source_table": "arc.ils_lev",
            "grain": "lnr (clean_code_ILSLeverandoer)",
            "deduplication": "ROW_NUMBER() per lnr: newest dw_timestamp_update, then sysdato, prrowid",
            "layer": "dim (Layer 1)",
            "mode": config.mode,
        }
    )
