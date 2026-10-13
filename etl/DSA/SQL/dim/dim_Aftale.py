# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: Aftale

Type 1 SCD maintenance for dim.Aftale — one row per dw_ek_Aftale.

The dimension carries active, expired and deleted agreements alike. The source
removes an agreement once it expires, and the archive keeps it
(detect_deletes=False) because historical purchases reference it. There is
therefore no soft delete here: an expired agreement is simply an agreement
whose hoved_slut lies in the past.

The business key is (lnr, aftale). Where the archive holds several rows for
that key — arc.ils_lafth upserts on prrowid alone — the newest archive row
wins. That choice is made in pre_stg_Vareaftaler_ils, so staging already
delivers one row per agreement.

alle_maa_benytte: 1 = no lafthsgh steering ("Benyttes af" blank in ILS) = every
hospital/warehouse may buy on the agreement; 0 = steered, see bridge.AftaleLager.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

# Attributes that constitute a change. Without a change guard the MERGE would
# update every row on every run, and dw_timestamp_update would stop being a
# change signal.
_CHANGE_CHECK = """
                   ISNULL(tgt.clean_code_Aftale, '') <> ISNULL(src.clean_code_Aftale, '')
                OR ISNULL(tgt.aftale_nummer, '') <> ISNULL(src.aftale_nummer, '')
                OR ISNULL(tgt.leverandoernummer, '') <> ISNULL(src.leverandoernummer, '')
                OR ISNULL(tgt.dw_ek_Kreditor, -1) <> ISNULL(src.dw_ek_Kreditor, -1)
                OR ISNULL(tgt.aftale_ansvarlig, '') <> ISNULL(src.aftale_ansvarlig, '')
                OR ISNULL(CAST(tgt.aftale_dato AS datetime2(0)), '1900-01-01') <>
                   ISNULL(CAST(src.aftale_dato AS datetime2(0)), '1900-01-01')
                OR ISNULL(tgt.aftaletype, '') <> ISNULL(src.aftaletype, '')
                OR ISNULL(tgt.aftaletekst, '') <> ISNULL(src.aftaletekst, '')
                OR ISNULL(CAST(tgt.hoved_start AS datetime2(0)), '1900-01-01') <>
                   ISNULL(CAST(src.hoved_start AS datetime2(0)), '1900-01-01')
                OR ISNULL(CAST(tgt.hoved_slut AS datetime2(0)), '1900-01-01') <>
                   ISNULL(CAST(src.hoved_slut AS datetime2(0)), '1900-01-01')
                OR ISNULL(tgt.alle_maa_benytte, 1) <> ISNULL(src.alle_maa_benytte, 1)
"""

MERGE_SQL = f"""
    MERGE [dim].[Aftale] AS tgt
    USING #Aftale_Source AS src
        ON tgt.dw_ek_Aftale = src.dw_ek_Aftale

    WHEN MATCHED AND (
        {_CHANGE_CHECK.strip()}
    ) THEN
        UPDATE SET
            tgt.clean_code_Aftale = src.clean_code_Aftale,
            tgt.aftale_nummer = src.aftale_nummer,
            tgt.leverandoernummer = src.leverandoernummer,
            tgt.dw_ek_Kreditor = src.dw_ek_Kreditor,
            tgt.aftale_dato = src.aftale_dato,
            tgt.aftale_ansvarlig = src.aftale_ansvarlig,
            tgt.aftaletype = src.aftaletype,
            tgt.aftaletekst = src.aftaletekst,
            tgt.hoved_start = src.hoved_start,
            tgt.hoved_slut = src.hoved_slut,
            tgt.alle_maa_benytte = src.alle_maa_benytte,
            tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
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
            alle_maa_benytte,
            dw_timestamp_insert,
            dw_timestamp_update
        )
        VALUES (
            src.dw_ek_Aftale,
            src.clean_code_Aftale,
            src.aftale_nummer,
            src.leverandoernummer,
            src.dw_ek_Kreditor,
            src.aftale_dato,
            src.aftale_ansvarlig,
            src.aftaletype,
            src.aftaletekst,
            src.hoved_start,
            src.hoved_slut,
            src.alle_maa_benytte,
            CAST(SYSDATETIME() AS datetime2(0)),
            CAST(SYSDATETIME() AS datetime2(0))
        )
    OUTPUT $action;
"""


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
        "scd": "type1",
    },
    description="Type 1 SCD sync for dim.Aftale using map.Aftale + stg.dim_Aftale_ils",
)
def dim_Aftale(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Dimension table maintenance: Type 1 upsert for aftaler."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "Aftale",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    inserted = 0
    updated = 0
    unmapped_rows = 0
    total_rows = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info("Starting Aftale Type 1 sync...")

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
                    s.hoved_slut,
                    s.alle_maa_benytte
                INTO #Aftale_Source
                FROM [stg].[dim_Aftale_ils] s
                LEFT JOIN [map].[Aftale] m
                    ON m.clean_code_Aftale = s.clean_code_Aftale
                LEFT JOIN [map].[Kreditor] mk
                    ON mk.clean_code_Kreditor = s.clean_code_Kreditor
                WHERE m.dw_ek_Aftale IS NOT NULL;

                CREATE UNIQUE CLUSTERED INDEX IX_Aftale_Source_EK
                ON #Aftale_Source(dw_ek_Aftale);
            """
                )

                cursor.execute(MERGE_SQL)
                actions = cursor.fetchall()
                inserted = sum(1 for row in actions if row[0] == "INSERT")
                updated = sum(1 for row in actions if row[0] == "UPDATE")

                cursor.execute("DROP TABLE IF EXISTS #Aftale_Source;")

                conn.commit()

                cursor.execute("UPDATE STATISTICS [dim].[Aftale]")

                cursor.execute("SELECT COUNT(*) FROM [dim].[Aftale]")
                total_rows = cursor.fetchone()[0]

                context.log.info(
                    "Aftale Type 1 sync complete: "
                    f"inserted={inserted}, updated={updated}, total={total_rows}"
                )
            finally:
                cursor.close()

        rows_affected = inserted + updated
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
            "inserted": inserted,
            "updated": updated,
            "unmapped_stg_rows": unmapped_rows,
            "total_rows": total_rows,
            "scd_type": "Type 1",
            "grain": "one row per dw_ek_Aftale (lnr + aftale)",
            "source_table": "stg.dim_Aftale_ils",
            "mode": config.mode,
        }
    )
