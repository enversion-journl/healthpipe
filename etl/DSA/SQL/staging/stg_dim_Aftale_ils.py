# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Aftale from ILS (Dimension Layer)

Aggregate ILS agreement data at aftale grain.
Derived from pre_stg_Vareaftaler_ils with deterministic deduplication.
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Aftale_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[AssetKey(["erp", "pre_stg_Vareaftaler_ils"])],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "aftale",
    },
    description="Aggregate ILS aftale data at aftale grain using latest start-date deduplication",
)
def stg_dim_Aftale_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """Create staging table for aftale dimension with one row per aftale."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_Aftale_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    inconsistent_txt_aftaler = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info("Creating staging table stg.dim_Aftale_ils from base layer...")

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Aftale_ils];")

                cursor.execute(
                    """
                    SELECT
                        clean_code_Aftale,
                        aftale AS aftale_nummer,
                        lnr AS leverandoernummer,
                        clean_code_Kreditor,
                        afttype AS aftaletype,
                        aftaletekst AS aftaletekst,
                        aftdto AS aftale_dato,
                        ansvar AS aftale_ansvarlig,
                        hoved_start AS hoved_start,
                        hoved_slut AS hoved_slut,
                        dw_timestamp_update
                    INTO [stg].[dim_Aftale_ils]
                    FROM (
                        SELECT
                            clean_code_Aftale,
                            aftale,
                            lnr,
                            clean_code_Kreditor,
                            afttype,
                            aftaletekst,
                            aftdto,
                            ansvar,
                            hoved_start,
                            hoved_slut,
                            dw_timestamp_update,
                            ROW_NUMBER() OVER (
                                PARTITION BY clean_code_Aftale
                                ORDER BY
                                    COALESCE(hoved_start, linje_start) DESC,
                                    dw_timestamp_update DESC
                            ) AS rn
                        FROM [stg].[pre_vareaftaler_ils]
                        WHERE aftale IS NOT NULL
                          AND LTRIM(RTRIM(aftale)) <> ''
                    ) ranked
                    WHERE rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_Aftale_ils with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_aftale_ils_CleanCode_Aftale
                    ON [stg].[dim_Aftale_ils](clean_code_Aftale);
                """
                )

                cursor.execute(
                    """
                    SELECT COUNT(*) AS inconsistent_txt_aftaler
                    FROM (
                        SELECT
                            clean_code_Aftale
                        FROM [stg].[pre_vareaftaler_ils]
                        WHERE aftale IS NOT NULL
                          AND LTRIM(RTRIM(aftale)) <> ''
                        GROUP BY clean_code_Aftale
                        HAVING COUNT(DISTINCT COALESCE(LTRIM(RTRIM(aftaletekst)), '')) > 1
                    ) diff_txt;
                """
                )
                inconsistent_txt_aftaler = cursor.fetchone()[0]
                context.log.info(
                    "Aftaler with inconsistent aftaletekst across lines: "
                    f"{inconsistent_txt_aftaler}"
                )

                conn.commit()
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, checkpoint_after=None, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Aftale_ils",
            "row_count": row_count,
            "source_table": "stg.pre_vareaftaler_ils",
            "grain": "lnr + aftale (clean_code_Aftale)",
            "deduplication": "ROW_NUMBER() by clean_code_Aftale ordered on COALESCE(hoved_start, linje_start) DESC",
            "inconsistent_aftaletekst_aftaler": inconsistent_txt_aftaler,
            "layer": "dim (Layer 1)",
            "mode": config.mode,
        }
    )
