# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vareudlevering from ILS (Dimension Layer)

Selects dimension-relevant columns from pre_stg_Vareudlevering_ils
for map.Vareudlevering and dim.Vareudlevering downstream consumers.

Grain: one row per clean_code_Vareudlevering (deduplicated by prrowid).
"""

from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_Vareudlevering_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_Vareudlevering_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description="Dimension staging for ILS delivery data — identifiers and descriptive attributes for map/dim Vareudlevering",
)
def stg_dim_Vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> MaterializeResult:
    """
    Layer 1 dimension staging for ILS udlevering data.

    Selects identifier and descriptive columns from pre_stg_Vareudlevering_ils,
    deduplicated at clean_code_Vareudlevering grain. This feeds MapUpd_vareudlevering
    (which only needs CleanCode + prrowid) and DimUpd_vareudlevering (which needs
    the full set of descriptive attributes).
    """

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "stg",
        "dim_Vareudlevering_ils",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    row_count = 0
    stats = (0, 0, 0)

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:

                context.log.info(
                    "Creating staging table stg.dim_Vareudlevering_ils from pre-staging..."
                )

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_Vareudlevering_ils];")

                cursor.execute(
                    """
                    SELECT
                        clean_code_Vareudlevering,
                        prrowid,

                        rvis AS rekvisitionsnummer,
                        CAST(flgsednr AS NVARCHAR(100)) AS foelgeseddelnummer,
                        CAST(brik AS NVARCHAR(100)) AS brik,
                        CAST(lnr AS NVARCHAR(100)) AS leverandoernummer,
                        CAST(ordre AS NVARCHAR(100)) AS ordrenummer,
                        CAST(ordlinie AS NVARCHAR(100)) AS ordrelinje,
                        CAST(sw_direkte AS NVARCHAR(10)) AS sw_direkte_udlevering,
                        CAST(sw_paksdl AS NVARCHAR(10)) AS sw_pakkeseddel,
                        CAST(sw_flgsdl AS NVARCHAR(10)) AS sw_foelgeseddel,
                        CAST(sw_fly AS NVARCHAR(10)) AS sw_fly,
                        CAST(sw_haster AS NVARCHAR(10)) AS sw_haster,
                        CAST(sw_erstat AS NVARCHAR(10)) AS sw_erstat,
                        CAST(projnr AS NVARCHAR(100)) AS projektnummer,
                        CAST(kto AS NVARCHAR(100)) AS kontonummer_kode,
                        CAST(bruger AS NVARCHAR(100)) AS bruger,
                        CAST(id AS NVARCHAR(100)) AS bk_prrowid,
                        CAST(refnr AS NVARCHAR(100)) AS referencenummer,
                        CAST(reflinie AS NVARCHAR(100)) AS referencelinje,
                        CAST(salgsordre AS NVARCHAR(100)) AS salgsordre,
                        CAST(salgsordreint AS NVARCHAR(100)) AS salgsordre_id,
                        CAST(salgsfaknr AS NVARCHAR(100)) AS salgsfakturanummer,
                        CAST(rvisvareid AS NVARCHAR(100)) AS rekvisitionsvare_id,
                        CAST(plukvareid AS NVARCHAR(100)) AS plukvarer_id,
                        CAST(aflevoper AS NVARCHAR(100)) AS afleveringsoperatoer,
                        CAST(aflevtxt AS NVARCHAR(100)) AS afleveringstekst,
                        CAST(aflevstreg AS NVARCHAR(100)) AS afleveringsstregkode,
                        CAST(vognnummer AS NVARCHAR(100)) AS vognnummer

                    INTO [stg].[dim_Vareudlevering_ils]
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY clean_code_Vareudlevering
                                ORDER BY dw_timestamp_update DESC
                            ) AS rn
                        FROM [stg].[pre_Vareudlevering_ils]
                        WHERE clean_code_Vareudlevering IS NOT NULL
                    ) ranked
                    WHERE rn = 1;
                """
                )

                row_count = cursor.rowcount
                context.log.info(f"Created stg.dim_Vareudlevering_ils with {row_count} rows")

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_dim_vareudlevering_ils_CleanCode
                    ON [stg].[dim_Vareudlevering_ils](clean_code_Vareudlevering);
                """
                )
                context.log.info("Created unique clustered index on clean_code_Vareudlevering")

                conn.commit()

                cursor.execute(
                    """
                    SELECT
                        COUNT(*) AS total,
                        COUNT(prrowid) AS has_prrowid,
                        COUNT(foelgeseddelnummer) AS has_foelgeseddel
                    FROM [stg].[dim_Vareudlevering_ils]
                """
                )
                stats = cursor.fetchone()
                context.log.info(
                    f"Coverage: {stats[1]}/{stats[0]} with prrowid, "
                    f"{stats[2]}/{stats[0]} with foelgeseddelnummer"
                )
            finally:
                cursor.close()

        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return MaterializeResult(
        metadata={
            "table": "stg.dim_Vareudlevering_ils",
            "row_count": row_count,
            "source_table": "stg.pre_Vareudlevering_ils",
            "deduplication": "ROW_NUMBER() on clean_code_Vareudlevering",
            "has_prrowid": stats[1],
            "has_foelgeseddel": stats[2],
            "mode": config.mode,
            "layer": "dim (Layer 1)",
        }
    )
