# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Ordre

Transaction dimension for the ILS order headers (dbo.ordh): dw_ek_Ordre is the
shared PK with fct.Ordre, resolved from map.Ordre (same pattern as
dim.Vareudlevering/fct.Vareudlevering).

Type 1 SCD — overwrite on change. The MERGE has no WHEN NOT MATCHED BY SOURCE
clause: in delta mode staging holds only the restaged orders, so a delete would
remove every other order, and the archive does not detect deletes anyway.

dim.ILSBruger is an outrigger: stg_dim_Ordre_ils resolves the creator to a
clean_code_ILSBruger from the archive, and dw_ek_ILSBruger_oprettet_af is its key
in map.ILSBruger, -1 when the creator names no ILS user. Initials are reused over
time and dim.ILSBruger only knows today's holder, so a match on the initials is
less certain; ilsbruger_match_kilde says which match was used. A delta run
resolves only the orders it restages, so an order whose user appears in ILS later
is resolved on its next change or by a full_load.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset
from DSA.SQL.common.dato_dimension import dato_key

# Everything the MERGE writes apart from the key.
ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "clean_code_Ordre",
    "bk_prrowid",
    "ordrenummer",
    "sygehus",
    "lager",
    "leverandoernummer",
    "ordredato",
    "ordrestatus",
    "ordre_oprettet_af",
    "dw_ek_ILSBruger_oprettet_af",
    "ilsbruger_match_kilde",
    "dw_ek_Dato_ajourfoert",
    "dato_tid_ajourfoert",
    "ajourfoert_bruger",
)

RESOLVE_SQL = f"""
    DROP TABLE IF EXISTS #Ordre_Resolved;

    SELECT
        m.dw_ek_Ordre,
        s.clean_code_Ordre,
        s.bk_prrowid,
        s.ordrenummer,
        s.sygehus,
        s.lager,
        s.leverandoernummer,
        s.ordredato,
        s.ordrestatus,
        s.ordre_oprettet_af,
        COALESCE(mb.dw_ek_ILSBruger, -1) AS dw_ek_ILSBruger_oprettet_af,
        CASE WHEN mb.dw_ek_ILSBruger IS NOT NULL THEN s.ilsbruger_match_kilde END AS ilsbruger_match_kilde,
        {dato_key("s.dato_tid_ajourfoert")} AS dw_ek_Dato_ajourfoert,
        s.dato_tid_ajourfoert,
        s.ajourfoert_bruger
    INTO #Ordre_Resolved
    FROM [stg].[dim_Ordre_ils] s
    INNER JOIN [map].[Ordre] m
        ON m.clean_code_Ordre = s.clean_code_Ordre
    LEFT JOIN [map].[ILSBruger] mb
        ON mb.clean_code_ILSBruger = s.clean_code_ILSBruger_oprettet_af;

    CREATE UNIQUE CLUSTERED INDEX IX_Ordre_Resolved_EK
    ON #Ordre_Resolved(dw_ek_Ordre);
"""

UNMAPPED_SQL = """
    SELECT COUNT(*)
    FROM [stg].[dim_Ordre_ils] s
    LEFT JOIN [map].[Ordre] m
        ON m.clean_code_Ordre = s.clean_code_Ordre
    WHERE m.dw_ek_Ordre IS NULL;
"""


def _merge_sql() -> str:
    src_cols = ", ".join(f"src.{c}" for c in ATTRIBUTE_COLUMNS)
    tgt_cols = ", ".join(f"tgt.{c}" for c in ATTRIBUTE_COLUMNS)
    update_set = "".join(f"        tgt.{c} = src.{c},\n" for c in ATTRIBUTE_COLUMNS)
    insert_cols = ", ".join(ATTRIBUTE_COLUMNS)

    return f"""
    MERGE INTO [dim].[Ordre] AS tgt
    USING #Ordre_Resolved AS src
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
    cols = ", ".join(ATTRIBUTE_COLUMNS)
    src_cols = ", ".join(f"src.{c}" for c in ATTRIBUTE_COLUMNS)
    return f"""
    INSERT INTO [dim].[Ordre] WITH (TABLOCK) (
        dw_ek_Ordre, {cols},
        dw_timestamp_insert, dw_timestamp_update
    )
    SELECT
        src.dw_ek_Ordre, {src_cols},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    FROM #Ordre_Resolved src;
"""


MERGE_SQL = _merge_sql()
INSERT_SQL = _insert_sql()

STATS_SQL = """
    SELECT
        COUNT(*),
        SUM(CASE WHEN ordre_oprettet_af IS NOT NULL AND dw_ek_ILSBruger_oprettet_af = -1
                 THEN 1 ELSE 0 END),
        SUM(CASE WHEN ilsbruger_match_kilde = 'initialer' THEN 1 ELSE 0 END)
    FROM [dim].[Ordre]
    WHERE dw_ek_Ordre <> -1;
"""


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_Ordre_ils"]),
        AssetKey(["map_Ordre"]),
        AssetKey(["map_ILSBruger"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="MERGE dim.Ordre (Type 1 SCD) from stg.dim_Ordre_ils via map.Ordre",
)
def dim_Ordre(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension maintenance: MERGE ILS order header attributes."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "Ordre", dagster_run_id=context.run_id, mode=config.mode)

    rows_affected = 0
    total = 0
    opretter_unresolved = 0
    opretter_via_initialer = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(UNMAPPED_SQL)
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_Ordre: {unmapped_rows} staging rows have no match in map.Ordre. "
                        "Ensure map_Ordre runs before dim_Ordre."
                    )

                cursor.execute(RESOLVE_SQL)

                # On a fresh or rebuilt table (only the -1 sentinel) a bulk INSERT
                # WITH (TABLOCK) can be minimally logged; MERGE is always fully
                # logged. map.Ordre's IDENTITY starts at 1, so it never hits -1.
                cursor.execute(
                    "SELECT CASE WHEN EXISTS (SELECT 1 FROM [dim].[Ordre] "
                    "WHERE dw_ek_Ordre <> -1) THEN 1 ELSE 0 END"
                )
                if cursor.fetchone()[0]:
                    cursor.execute(MERGE_SQL)
                else:
                    context.log.info("Target holds only the -1 sentinel - bulk INSERT")
                    cursor.execute(INSERT_SQL)
                rows_affected = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #Ordre_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[Ordre];")
                # UPDATE STATISTICS is transactional and the connection is not
                # autocommit, so without this commit it is rolled back on close.
                conn.commit()

                cursor.execute(STATS_SQL)
                stats = cursor.fetchone()
                total = stats[0] or 0
                opretter_unresolved = stats[1] or 0
                opretter_via_initialer = stats[2] or 0
            finally:
                cursor.close()

        context.log.info(
            f"dim.Ordre: {rows_affected} merged, {total} total "
            f"({opretter_via_initialer} creators matched on initials, "
            f"{opretter_unresolved} with ordre_oprettet_af but no ILS user)"
        )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Ordre", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Ordre")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "dim.Ordre",
            "rows_affected": rows_affected,
            "total_rows": total,
            "ordre_oprettet_af_unresolved": opretter_unresolved,
            "ordre_oprettet_af_via_initialer": opretter_via_initialer,
            "source_tables": "stg.dim_Ordre_ils + map.Ordre + map.ILSBruger",
            "pattern": "MERGE (Type 1 SCD)",
            "mode": config.mode,
        }
    )
