# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: ILSBruger (Dimension Layer, full rebuild)

The ILS user master (dbo.zz_oper) staged at one row per user initials. zz_oper
is a small full-load reference table, so the whole archive is restaged on every
run; no checkpoint.

Grain: bd_init, the key the user columns of the other ILS tables refer to.
bd_oprid, the general user ID across systems, is carried twice: as bruger_id
(upper-cased and trimmed, for lookups such as ordh.bd_oprid) and as
identifier_staff (long-form staff codes reduced to their short form). bd_primafd is the user's primary hospital as an ILS
sgh code; its name comes from dbo.sgh.

A user without initials is not staged. Should two rows share initials, the
active one wins, then the latest sysdato, then pro2modified.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset
from DSA.SQL.common.ils_clean_code_shared import sql_norm_code
from DSA.SQL.common.staff_identifier_sql import sql_staff_identifier

SOURCE_TABLE = "ils_zz_oper"


def _initialer(column: str) -> str:
    """The initials as the grain keys them; DROPPED_SQL counts with the same rule."""
    return f"UPPER({sql_norm_code(column)})"


BUILD_SQL = f"""
    DROP TABLE IF EXISTS [stg].[dim_ILSBruger];

    ;WITH src AS (
        SELECT
            {_initialer("z.bd_init")} AS initialer,
            UPPER({sql_norm_code("z.bd_oprid")}) AS bruger_id,
            {sql_staff_identifier("z.bd_oprid")} AS identifier_staff,
            NULLIF(LTRIM(RTRIM(z.bd_navn)), '') AS name_text,
            z.bd_aktiv AS active,
            {sql_norm_code("z.bd_primafd")} AS primaer_sygehus,
            z.sysdato,
            z.pro2modified,
            z.prrowid
        FROM [arc].[ils_zz_oper] z
    ),
    ranked AS (
        SELECT
            src.*,
            ROW_NUMBER() OVER (
                PARTITION BY initialer
                ORDER BY CASE WHEN active = 1 THEN 0 ELSE 1 END,
                         sysdato DESC,
                         pro2modified DESC,
                         prrowid ASC
            ) AS rn
        FROM src
        WHERE initialer IS NOT NULL
    )
    SELECT
        CAST('ILSBRUGER_' + initialer AS NVARCHAR(100)) AS clean_code_ILSBruger,
        CAST(initialer AS NVARCHAR(6)) AS initialer,
        CAST(bruger_id AS NVARCHAR(20)) AS bruger_id,
        CAST(identifier_staff AS NVARCHAR(50)) AS identifier_staff,
        CAST(name_text AS NVARCHAR(60)) AS name_text,
        active,
        CAST(primaer_sygehus AS NVARCHAR(16)) AS primaer_sygehus,
        CAST(sgh.navn AS NVARCHAR(80)) AS primaer_sygehus_navn,
        CAST(N'{SOURCE_TABLE}' AS NVARCHAR(100)) AS source_table
    INTO [stg].[dim_ILSBruger]
    FROM ranked
    -- arc.ils_sgh is keyed on prrowid, so pick one row per sgh deterministically.
    OUTER APPLY (
        SELECT TOP 1 NULLIF(LTRIM(RTRIM(s.navn)), '') AS navn
        FROM [arc].[ils_sgh] s
        WHERE {sql_norm_code("s.sgh")} = ranked.primaer_sygehus
        ORDER BY s.sysdato DESC, s.pro2modified DESC, s.prrowid ASC
    ) sgh
    WHERE ranked.rn = 1;
"""

INDEX_SQL = """
    CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_ILSBruger_CleanCode
    ON [stg].[dim_ILSBruger](clean_code_ILSBruger);
"""

# Rows the grain drops: no initials at all, or a second row for the same initials.
DROPPED_SQL = f"""
    SELECT
        SUM(CASE WHEN {sql_norm_code("bd_init")} IS NULL THEN 1 ELSE 0 END),
        COUNT({sql_norm_code("bd_init")})
            - COUNT(DISTINCT {_initialer("bd_init")})
    FROM [arc].[ils_zz_oper];
"""

# Lossy or missing lookups, counted so they do not pass silently: hospital codes
# with no dbo.sgh name (e.g. '*'), long-form staff codes reduced to their short
# form, and users without bd_oprid.
LOOKUP_GAPS_SQL = """
    SELECT
        SUM(CASE WHEN primaer_sygehus IS NOT NULL AND primaer_sygehus_navn IS NULL
                 THEN 1 ELSE 0 END),
        SUM(CASE WHEN identifier_staff <> bruger_id THEN 1 ELSE 0 END),
        SUM(CASE WHEN bruger_id IS NULL THEN 1 ELSE 0 END)
    FROM [stg].[dim_ILSBruger];
"""


@asset(
    key=AssetKey(["erp", "stg_dim_ILSBruger"]),
    group_name="erp_staging",
    compute_kind="mssql",
    retry_policy=RetryPolicy(max_retries=2, delay=30),
    deps=[AssetKey(["arc_ils_zz_oper"]), AssetKey(["arc_ils_sgh"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "ilsbruger",
    },
    description=(
        "Dimension staging for the ILS user master (arc.ils_zz_oper), full rebuild "
        "at one row per initials, with bd_oprid as identifier_staff in short "
        "staff-code form. Feeds map.ILSBruger and dim.ILSBruger."
    ),
)
def stg_dim_ILSBruger(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Full rebuild of stg.dim_ILSBruger from arc.ils_zz_oper."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg", "dim_ILSBruger", dagster_run_id=context.run_id, mode=config.mode
    )

    row_count = 0
    without_initials = 0
    duplicate_initials = 0
    sygehus_without_name = 0
    identifier_staff_reduced = 0
    without_bruger_id = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(BUILD_SQL)
                cursor.execute(INDEX_SQL)
                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ILSBruger]")
                row_count = cursor.fetchone()[0]

                cursor.execute(DROPPED_SQL)
                dropped = cursor.fetchone()
                without_initials = dropped[0] or 0
                duplicate_initials = dropped[1] or 0

                cursor.execute(LOOKUP_GAPS_SQL)
                gaps = cursor.fetchone()
                sygehus_without_name = gaps[0] or 0
                identifier_staff_reduced = gaps[1] or 0
                without_bruger_id = gaps[2] or 0
            finally:
                cursor.close()

        context.log.info(
            f"stg.dim_ILSBruger: {row_count} rows "
            f"({without_initials} source rows without initials, "
            f"{duplicate_initials} duplicate initials collapsed, "
            f"{sygehus_without_name} with a primaer_sygehus code not in arc.ils_sgh, "
            f"{identifier_staff_reduced} long-form staff codes reduced, "
            f"{without_bruger_id} without bd_oprid)"
        )
        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_ILSBruger",
            "row_count": row_count,
            "source_rows_without_initials": without_initials,
            "duplicate_initials_collapsed": duplicate_initials,
            "primaer_sygehus_without_name": sygehus_without_name,
            "identifier_staff_reduced": identifier_staff_reduced,
            "without_bruger_id": without_bruger_id,
            "source_tables": "arc.ils_zz_oper, arc.ils_sgh",
            "grain": "bd_init (one row per ILS user initials)",
            "mode": config.mode,
        }
    )
