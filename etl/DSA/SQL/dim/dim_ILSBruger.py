# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: ILSBruger

Type 1 SCD -- MERGE from stg.dim_ILSBruger via map.ILSBruger for DW_EK resolution.

Staging holds the full zz_oper picture, so a dimension row that is no longer
staged (the user's initials changed or were cleared) is kept but set to
``active = 0``. The archive does not detect deletes, so a user deleted in ILS
keeps the last ``bd_aktiv`` the archive saw.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Everything the MERGE writes apart from the key.
ATTRIBUTE_COLUMNS: tuple[str, ...] = (
    "clean_code_ILSBruger",
    "initialer",
    "bruger_id",
    "identifier_staff",
    "name_text",
    "active",
    "primaer_sygehus",
    "primaer_sygehus_navn",
    "source_table",
)

RESOLVE_SQL = """
    DROP TABLE IF EXISTS #ILSBruger_Resolved;

    -- COLLATE DATABASE_DEFAULT: a #table otherwise takes tempdb's collation, and
    -- the MERGE's EXCEPT against dim.ILSBruger fails on the collation conflict.
    CREATE TABLE #ILSBruger_Resolved (
        dw_ek_ILSBruger      INT            NOT NULL PRIMARY KEY CLUSTERED,
        clean_code_ILSBruger NVARCHAR(100)  COLLATE DATABASE_DEFAULT NOT NULL,
        initialer            NVARCHAR(6)    COLLATE DATABASE_DEFAULT NULL,
        bruger_id            NVARCHAR(20)   COLLATE DATABASE_DEFAULT NULL,
        identifier_staff     NVARCHAR(50)   COLLATE DATABASE_DEFAULT NULL,
        name_text            NVARCHAR(60)   COLLATE DATABASE_DEFAULT NULL,
        active               BIT            NULL,
        primaer_sygehus      NVARCHAR(16)   COLLATE DATABASE_DEFAULT NULL,
        primaer_sygehus_navn NVARCHAR(80)   COLLATE DATABASE_DEFAULT NULL,
        source_table         NVARCHAR(100)  COLLATE DATABASE_DEFAULT NULL
    );

    INSERT INTO #ILSBruger_Resolved (
        dw_ek_ILSBruger, clean_code_ILSBruger, initialer, bruger_id, identifier_staff,
        name_text, active, primaer_sygehus, primaer_sygehus_navn, source_table
    )
    SELECT
        m.dw_ek_ILSBruger,
        stg.clean_code_ILSBruger,
        stg.initialer,
        stg.bruger_id,
        stg.identifier_staff,
        stg.name_text,
        stg.active,
        stg.primaer_sygehus,
        stg.primaer_sygehus_navn,
        stg.source_table
    FROM [stg].[dim_ILSBruger] stg
    INNER JOIN [map].[ILSBruger] m
        ON m.clean_code_ILSBruger = stg.clean_code_ILSBruger;
"""


# The change check compares text case-sensitively, so a case-only correction of a
# name updates the row; the database collation is case-insensitive.
_TEXT_COMPARE_COLLATION = "Latin1_General_100_BIN2"


def _compared(alias: str, column: str) -> str:
    if column == "active":
        return f"{alias}.{column}"
    return f"{alias}.{column} COLLATE {_TEXT_COMPARE_COLLATION}"


def _merge_sql() -> str:
    src_cols = ", ".join(f"src.{c}" for c in ATTRIBUTE_COLUMNS)
    src_compared = ", ".join(_compared("src", c) for c in ATTRIBUTE_COLUMNS)
    tgt_compared = ", ".join(_compared("tgt", c) for c in ATTRIBUTE_COLUMNS)
    update_set = "".join(f"        tgt.{c} = src.{c},\n" for c in ATTRIBUTE_COLUMNS)
    insert_cols = ", ".join(ATTRIBUTE_COLUMNS)

    return f"""
    MERGE INTO [dim].[ILSBruger] AS tgt
    USING #ILSBruger_Resolved AS src
        ON tgt.dw_ek_ILSBruger = src.dw_ek_ILSBruger
    WHEN MATCHED AND EXISTS (
        SELECT {src_compared}
        EXCEPT
        SELECT {tgt_compared}
    ) THEN UPDATE SET
{update_set}        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
    WHEN NOT MATCHED THEN INSERT (
        dw_ek_ILSBruger, {insert_cols},
        dw_timestamp_insert, dw_timestamp_update
    ) VALUES (
        src.dw_ek_ILSBruger, {src_cols},
        CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
    )
    WHEN NOT MATCHED BY SOURCE
        AND tgt.dw_ek_ILSBruger <> -1
        AND (tgt.active IS NULL OR tgt.active <> 0)
    THEN UPDATE SET
        tgt.active = 0,
        tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0));
"""


MERGE_SQL = _merge_sql()


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["erp", "stg_dim_ILSBruger"]),
        AssetKey(["map_ILSBruger"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="MERGE dim.ILSBruger (Type 1 SCD) from stg.dim_ILSBruger",
)
def dim_ILSBruger(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension maintenance: MERGE ILS user attributes."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("dim", "ILSBruger", dagster_run_id=context.run_id, mode=config.mode)

    merge_count = 0
    total = 0
    active = 0
    with_identifier = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                # An empty staging table would deactivate every user via
                # WHEN NOT MATCHED BY SOURCE.
                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ILSBruger]")
                if cursor.fetchone()[0] == 0:
                    raise ValueError(
                        "dim_ILSBruger: stg.dim_ILSBruger is empty. Refusing to MERGE, "
                        "because it would set every user inactive."
                    )

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [stg].[dim_ILSBruger] stg
                    LEFT JOIN [map].[ILSBruger] m
                        ON m.clean_code_ILSBruger = stg.clean_code_ILSBruger
                    WHERE m.dw_ek_ILSBruger IS NULL
                    """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    raise ValueError(
                        f"dim_ILSBruger: {unmapped_rows} staging rows have no match in "
                        "map.ILSBruger. Ensure map_ILSBruger runs before dim_ILSBruger."
                    )

                cursor.execute(RESOLVE_SQL)
                cursor.execute(MERGE_SQL)
                merge_count = cursor.rowcount
                conn.commit()

                cursor.execute("DROP TABLE IF EXISTS #ILSBruger_Resolved;")
                cursor.execute("UPDATE STATISTICS [dim].[ILSBruger];")
                # UPDATE STATISTICS is transactional and the connection is not
                # autocommit, so without this commit it is rolled back on close.
                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*),
                        SUM(CASE WHEN active = 1 THEN 1 ELSE 0 END),
                        SUM(CASE WHEN identifier_staff IS NOT NULL THEN 1 ELSE 0 END)
                    FROM [dim].[ILSBruger]
                    WHERE dw_ek_ILSBruger <> -1
                    """
                )
                stats = cursor.fetchone()
                total = stats[0] or 0
                active = stats[1] or 0
                with_identifier = stats[2] or 0
            finally:
                cursor.close()

        context.log.info(
            f"dim.ILSBruger: {merge_count} merged, {total} total "
            f"({active} active, {with_identifier} with identifier_staff)"
        )

        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "ILSBruger", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "ILSBruger")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=merge_count
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "dim.ILSBruger",
            "merged_rows": merge_count,
            "total_rows": total,
            "active_rows": active,
            "rows_with_identifier_staff": with_identifier,
            "pattern": "MERGE (Type 1 SCD)",
            "mode": config.mode,
        }
    )
