# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fact Update: Ordre

Header-grain fact for the ILS order headers (dbo.ordh): one row per order
(sgh, lgr, ordre), carrying the amounts that belong to the order as a whole —
freight and fee. On the order lines they would be counted once per line.

Transaction dimension: dw_ek_Ordre is the shared PK with dim.Ordre, resolved from
map.Ordre. Reads stg.fct_Ordre_ils, which holds the precomputed date key and
typed measures; this asset only resolves CleanCodes to DW_EKs and MERGEs.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Everything the MERGE writes apart from the key.
FACT_COLUMNS: tuple[str, ...] = (
    "clean_code_Ordre",
    "bk_prrowid",
    "dw_ek_Lager",
    "dw_ek_ILSLeverandoer",
    "dw_ek_Dato_ordre",
    "dato_tid_ordre",
    "fragt_beloeb",
    "gebyr_beloeb",
    "dw_haendelsestidspunkt",
)

RESOLVE_SQL = """
    DROP TABLE IF EXISTS #Ordre_Facts;

    SELECT
        ordre_map.dw_ek_Ordre,
        src.clean_code_Ordre,
        src.bk_prrowid,
        COALESCE(lager_map.dw_ek_Lager, -1) AS dw_ek_Lager,
        COALESCE(lev_map.dw_ek_ILSLeverandoer, -1) AS dw_ek_ILSLeverandoer,
        src.dw_ek_Dato_ordre,
        src.dato_tid_ordre,
        src.fragt_beloeb,
        src.gebyr_beloeb,
        src.dw_haendelsestidspunkt
    INTO #Ordre_Facts
    FROM [stg].[fct_Ordre_ils] src
    -- Shared PK with dim.Ordre
    INNER JOIN [map].[Ordre] ordre_map
        ON ordre_map.clean_code_Ordre = src.clean_code_Ordre
    -- SGH_{sgh}_LGR_{lgr}, or SGH_{sgh} when lgr is not stated
    LEFT JOIN [map].[Lager] lager_map
        ON lager_map.clean_code_Lager = src.clean_code_Lager
    -- ILSLEV_{lnr}
    LEFT JOIN [map].[ILSLeverandoer] lev_map
        ON lev_map.clean_code_ILSLeverandoer = src.clean_code_ILSLeverandoer;

    CREATE UNIQUE CLUSTERED INDEX IX_Ordre_Facts_EK
    ON #Ordre_Facts(dw_ek_Ordre);
"""

UNMAPPED_SQL = """
    SELECT COUNT(*)
    FROM [stg].[fct_Ordre_ils] src
    LEFT JOIN [map].[Ordre] m
        ON m.clean_code_Ordre = src.clean_code_Ordre
    WHERE m.dw_ek_Ordre IS NULL;
"""


def _merge_sql() -> str:
    src_cols = ", ".join(f"src.{c}" for c in FACT_COLUMNS)
    tgt_cols = ", ".join(f"tgt.{c}" for c in FACT_COLUMNS)
    update_set = "".join(f"        tgt.{c} = src.{c},\n" for c in FACT_COLUMNS)
    insert_cols = ", ".join(FACT_COLUMNS)

    return f"""
    MERGE INTO [fct].[Ordre] AS tgt
    USING #Ordre_Facts AS src
        ON tgt.dw_ek_Ordre = src.dw_ek_Ordre
    WHEN MATCHED AND EXISTS (
        SELECT {src_cols}
        EXCEPT
        SELECT {tgt_cols}
    ) THEN UPDATE SET
{update_set}        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
    WHEN NOT MATCHED THEN INSERT (
        dw_ek_Ordre, {insert_cols},
        dw_timestamp_insert, dw_timestamp_update
    ) VALUES (
        src.dw_ek_Ordre, {src_cols},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    );
"""


def _insert_sql() -> str:
    cols = ", ".join(FACT_COLUMNS)
    src_cols = ", ".join(f"src.{c}" for c in FACT_COLUMNS)
    return f"""
    INSERT INTO [fct].[Ordre] WITH (TABLOCK) (
        dw_ek_Ordre, {cols},
        dw_timestamp_insert, dw_timestamp_update
    )
    SELECT
        src.dw_ek_Ordre, {src_cols},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    FROM #Ordre_Facts src;
"""


MERGE_SQL = _merge_sql()
INSERT_SQL = _insert_sql()

FK_STATS_SQL = """
    SELECT
        COUNT(*),
        SUM(CASE WHEN dw_ek_Lager = -1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN dw_ek_ILSLeverandoer = -1 THEN 1 ELSE 0 END),
        SUM(CASE WHEN dw_ek_Dato_ordre = -1 THEN 1 ELSE 0 END)
    FROM [fct].[Ordre];
"""


@asset(
    group_name="erp_fact",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_fct_Ordre_ils"]),
        "map_Ordre",
        "map_Lager",
        "map_ILSLeverandoer",
    ],
    tags={
        "domain": "erp",
        "layer": "facts",
        "module": "datawarehouse",
        "grain": "order_header",
    },
    description=(
        "Header-grain fact for ILS orders (freight, fee) - reads precomputed "
        "staging columns and looks up DW_EK from maps via CleanCodes"
    ),
)
def fct_Ordre(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Map lookups + MERGE into fct.Ordre. All conversions done in staging."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("fct", "Ordre", dagster_run_id=context.run_id, mode=config.mode)

    rows_affected = 0
    diag: tuple[int, ...] = (0, 0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"fct_Ordre: {unmapped_rows} staging rows have no match in map.Ordre. "
                        "Ensure map_Ordre runs before fct_Ordre."
                    )

                cursor.execute(RESOLVE_SQL)

                # On an empty target a bulk INSERT WITH (TABLOCK) can be minimally
                # logged; MERGE is always fully logged. Facts carry no -1 sentinel.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [fct].[Ordre]) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target empty - bulk INSERT WITH (TABLOCK)")
                    cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Ordre_Facts;")
                cursor.execute("UPDATE STATISTICS [fct].[Ordre];")
                conn.commit()

                cursor.execute(FK_STATS_SQL)
                diag = tuple(v or 0 for v in cursor.fetchone())
            finally:
                cursor.close()

        context.log.info(
            f"fct.Ordre: {rows_affected} merged, {diag[0]} total "
            f"(-1 keys: Lager={diag[1]}, ILSLeverandoer={diag[2]}, Dato_ordre={diag[3]})"
        )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("fct", "Ordre", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("fct", "Ordre")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "fct.Ordre",
            "rows_affected": rows_affected,
            "total_rows": diag[0],
            "missing_lager_fk": diag[1],
            "missing_ilsleverandoer_fk": diag[2],
            "missing_dato_ordre_fk": diag[3],
            "grain": "clean_code_Ordre (sgh, lgr, ordre)",
            "staging_source": "stg.fct_Ordre_ils (Layer 1)",
            "mode": config.mode,
        }
    )
