# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Aftale

Type 2 SCD maintenance for dim.Aftale.
Tracks changes in agreement attributes over time.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    group_name="erp_dimension",
    compute_kind="mssql",
    deps=[
        AssetKey(["map_Aftale"]),
        AssetKey(["map_Kreditor"]),
        AssetKey(["erp", "stg_dim_Aftale_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type2",
    },
    description="Type 2 SCD sync for dim.Aftale using map.Aftale + stg.dim_Aftale_ils",
)
def dim_Aftale(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Type 2 SCD for aftaler."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "Aftale",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    changed_closed = 0
    changed_inserted = 0
    initial_inserted = 0
    reactivated_inserted = 0
    soft_deleted = 0
    unmapped_rows = 0
    total_rows = 0
    active_rows = 0
    deleted_rows = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Aftale Type 2 SCD sync...")

                cursor.execute("SELECT COUNT(*) FROM [map].[Aftale]")
                map_count = cursor.fetchone()[0]
                context.log.info(f"Map has {map_count} Aftale entries to sync")

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM [stg].[dim_Aftale_ils] s
                    LEFT JOIN [map].[Aftale] m
                        ON m.clean_code_Aftale = s.clean_code_Aftale
                    WHERE m.dw_ek_Aftale IS NULL
                """
                )
                unmapped_rows = cursor.fetchone()[0]
                if unmapped_rows > 0:
                    context.log.warning(
                        f"Found {unmapped_rows} stg.dim_Aftale_ils rows without map match"
                    )

                cursor.execute(
                    """
                DROP TABLE IF EXISTS #Aftale_Source;
                DROP TABLE IF EXISTS #Changed_Current;
                DROP TABLE IF EXISTS #Missing_Source;

                SELECT
                    m.dw_ek_Aftale,
                    s.clean_code_Aftale,
                    s.aftale_nummer,
                    s.leverandoernummer,
                    COALESCE(mk.dw_ek_Kreditor, -1) AS dw_ek_Kreditor,
                    s.aftaletype,
                    s.aftaletekst,
                    s.aftale_dato,
                    s.aftale_ansvarlig,
                    s.hoved_start,
                    s.hoved_slut
                INTO #Aftale_Source
                FROM [stg].[dim_Aftale_ils] s
                LEFT JOIN [map].[Aftale] m
                    ON m.clean_code_Aftale = s.clean_code_Aftale
                LEFT JOIN [map].[Kreditor] mk
                    ON mk.clean_code_Kreditor = s.clean_code_Kreditor
                WHERE m.dw_ek_Aftale IS NOT NULL;

                -- Current active rows that have attribute changes in source
                SELECT
                    d.dw_ek_Aftale
                INTO #Changed_Current
                FROM [dim].[Aftale] d
                INNER JOIN #Aftale_Source s
                    ON s.dw_ek_Aftale = d.dw_ek_Aftale
                WHERE d.dw_valid_to = '9999-12-31'
                  AND d.dw_is_deleted = 0
                  AND (
                      ISNULL(d.clean_code_Aftale, '') <> ISNULL(s.clean_code_Aftale, '')
                      OR ISNULL(d.aftale_nummer, '') <> ISNULL(s.aftale_nummer, '')
                      OR ISNULL(d.leverandoernummer, '') <> ISNULL(s.leverandoernummer, '')
                      OR ISNULL(d.dw_ek_Kreditor, -1) <> ISNULL(s.dw_ek_Kreditor, -1)
                      OR ISNULL(d.aftale_ansvarlig, '') <> ISNULL(s.aftale_ansvarlig, '')
                      OR ISNULL(CAST(d.aftale_dato AS datetime2(0)), '1900-01-01') <>
                         ISNULL(CAST(s.aftale_dato AS datetime2(0)), '1900-01-01')
                      OR ISNULL(d.aftaletype, '') <> ISNULL(s.aftaletype, '')
                      OR ISNULL(d.aftaletekst, '') <> ISNULL(s.aftaletekst, '')
                      OR ISNULL(CAST(d.hoved_start AS datetime2(0)), '1900-01-01') <>
                         ISNULL(CAST(s.hoved_start AS datetime2(0)), '1900-01-01')
                      OR ISNULL(CAST(d.hoved_slut AS datetime2(0)), '1900-01-01') <>
                         ISNULL(CAST(s.hoved_slut AS datetime2(0)), '1900-01-01')
                  );

                -- Current active rows missing from source (soft-delete candidates)
                SELECT
                    d.dw_ek_Aftale
                INTO #Missing_Source
                FROM [dim].[Aftale] d
                WHERE d.dw_valid_to = '9999-12-31'
                  AND d.dw_is_deleted = 0
                  AND d.dw_ek_Aftale <> -1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM #Aftale_Source s
                      WHERE s.dw_ek_Aftale = d.dw_ek_Aftale
                  );
            """
                )

                # Close changed current rows
                cursor.execute(
                    """
                UPDATE d
                SET
                    d.dw_valid_to = CAST(SYSDATETIME() AS datetime2(0)),
                    d.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [dim].[Aftale] d
                INNER JOIN #Changed_Current c
                    ON c.dw_ek_Aftale = d.dw_ek_Aftale
                WHERE d.dw_valid_to = '9999-12-31'
                  AND d.dw_is_deleted = 0;
            """
                )
                changed_closed = cursor.rowcount

                # Insert new versions for changed rows
                cursor.execute(
                    """
                INSERT INTO [dim].[Aftale] (
                    dw_ek_Aftale,
                    clean_code_Aftale,
                    aftale_nummer,
                    leverandoernummer,
                    dw_ek_Kreditor,
                    aftale_dato,
                    aftale_ansvarlig,
                    aftaletype,
                    aftaletekst,
                    hoved_start,
                    hoved_slut,
                    dw_valid_from,
                    dw_valid_to,
                    dw_is_deleted,
                    dw_timestamp_insert
                )
                SELECT
                    s.dw_ek_Aftale,
                    s.clean_code_Aftale,
                    s.aftale_nummer,
                    s.leverandoernummer,
                    s.dw_ek_Kreditor,
                    s.aftale_dato,
                    s.aftale_ansvarlig,
                    s.aftaletype,
                    s.aftaletekst,
                    s.hoved_start,
                    s.hoved_slut,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    '9999-12-31',
                    0,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #Aftale_Source s
                INNER JOIN #Changed_Current c
                    ON c.dw_ek_Aftale = s.dw_ek_Aftale;
            """
                )
                changed_inserted = cursor.rowcount

                # Initial inserts (first ever version for new dw_ek_Aftale)
                cursor.execute(
                    """
                INSERT INTO [dim].[Aftale] (
                    dw_ek_Aftale,
                    clean_code_Aftale,
                    aftale_nummer,
                    leverandoernummer,
                    dw_ek_Kreditor,
                    aftale_dato,
                    aftale_ansvarlig,
                    aftaletype,
                    aftaletekst,
                    hoved_start,
                    hoved_slut,
                    dw_valid_from,
                    dw_valid_to,
                    dw_is_deleted,
                    dw_timestamp_insert
                )
                SELECT
                    s.dw_ek_Aftale,
                    s.clean_code_Aftale,
                    s.aftale_nummer,
                    s.leverandoernummer,
                    s.dw_ek_Kreditor,
                    s.aftale_dato,
                    s.aftale_ansvarlig,
                    s.aftaletype,
                    s.aftaletekst,
                    s.hoved_start,
                    s.hoved_slut,
                    '1900-01-01',
                    '9999-12-31',
                    0,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #Aftale_Source s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [dim].[Aftale] d
                    WHERE d.dw_ek_Aftale = s.dw_ek_Aftale
                );
            """
                )
                initial_inserted = cursor.rowcount

                # Reactivation: source exists but no current row for dw_ek_Aftale
                cursor.execute(
                    """
                INSERT INTO [dim].[Aftale] (
                    dw_ek_Aftale,
                    clean_code_Aftale,
                    aftale_nummer,
                    leverandoernummer,
                    dw_ek_Kreditor,
                    aftale_dato,
                    aftale_ansvarlig,
                    aftaletype,
                    aftaletekst,
                    hoved_start,
                    hoved_slut,
                    dw_valid_from,
                    dw_valid_to,
                    dw_is_deleted,
                    dw_timestamp_insert
                )
                SELECT
                    s.dw_ek_Aftale,
                    s.clean_code_Aftale,
                    s.aftale_nummer,
                    s.leverandoernummer,
                    s.dw_ek_Kreditor,
                    s.aftale_dato,
                    s.aftale_ansvarlig,
                    s.aftaletype,
                    s.aftaletekst,
                    s.hoved_start,
                    s.hoved_slut,
                    CAST(SYSDATETIME() AS datetime2(0)),
                    '9999-12-31',
                    0,
                    CAST(SYSDATETIME() AS datetime2(0))
                FROM #Aftale_Source s
                WHERE EXISTS (
                    SELECT 1
                    FROM [dim].[Aftale] d
                    WHERE d.dw_ek_Aftale = s.dw_ek_Aftale
                )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [dim].[Aftale] d
                      WHERE d.dw_ek_Aftale = s.dw_ek_Aftale
                        AND d.dw_valid_to = '9999-12-31'
                  );
            """
                )
                reactivated_inserted = cursor.rowcount

                # Soft-delete aftaler missing from source
                cursor.execute(
                    """
                UPDATE d
                SET
                    d.dw_is_deleted = 1,
                    d.dw_valid_to = CAST(SYSDATETIME() AS datetime2(0)),
                    d.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [dim].[Aftale] d
                INNER JOIN #Missing_Source m
                    ON m.dw_ek_Aftale = d.dw_ek_Aftale
                WHERE d.dw_valid_to = '9999-12-31'
                  AND d.dw_is_deleted = 0;
            """
                )
                soft_deleted = cursor.rowcount

                cursor.execute("DROP TABLE IF EXISTS #Aftale_Source;")
                cursor.execute("DROP TABLE IF EXISTS #Changed_Current;")
                cursor.execute("DROP TABLE IF EXISTS #Missing_Source;")

                conn.commit()

                cursor.execute("UPDATE STATISTICS [dim].[Aftale]")

                cursor.execute("SELECT COUNT(*) FROM [dim].[Aftale]")
                total_rows = cursor.fetchone()[0]

                cursor.execute(
                    """
                SELECT COUNT(*)
                FROM [dim].[Aftale]
                WHERE dw_valid_to = '9999-12-31' AND dw_is_deleted = 0;
            """
                )
                active_rows = cursor.fetchone()[0]

                cursor.execute("SELECT COUNT(*) FROM [dim].[Aftale] WHERE dw_is_deleted = 1")
                deleted_rows = cursor.fetchone()[0]

                context.log.info(
                    "Aftale Type 2 sync complete: "
                    f"changed_closed={changed_closed}, changed_inserted={changed_inserted}, "
                    f"initial_inserted={initial_inserted}, reactivated_inserted={reactivated_inserted}, "
                    f"soft_deleted={soft_deleted}"
                )
            finally:
                cursor.close()

        rows_affected = (
            changed_closed
            + changed_inserted
            + initial_inserted
            + reactivated_inserted
            + soft_deleted
        )
        logged_checkpoint = None
        if config.should_advance_checkpoint:
            dfm.set_checkpoint("dim", "Aftale", mode=config.mode)
            logged_checkpoint = dfm.get_checkpoint("dim", "Aftale")
        dfm.complete_dfm_run(
            run_ctx.log_id, checkpoint_after=logged_checkpoint, rows_affected=rows_affected
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "dim.Aftale",
            "changed_closed": changed_closed,
            "changed_inserted": changed_inserted,
            "initial_inserted": initial_inserted,
            "reactivated_inserted": reactivated_inserted,
            "soft_deleted": soft_deleted,
            "unmapped_stg_rows": unmapped_rows,
            "total_rows": total_rows,
            "active_rows": active_rows,
            "deleted_rows": deleted_rows,
            "scd_type": "Type 2",
            "initial_valid_from": "1900-01-01",
            "source_table": "stg.dim_Aftale_ils",
            "mode": config.mode,
        }
    )
