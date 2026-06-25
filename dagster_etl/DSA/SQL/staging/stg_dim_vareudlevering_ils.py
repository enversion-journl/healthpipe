# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: Vareudlevering from ILS (Dimension Layer)

Selects dimension-relevant columns from pre_stg_vareudlevering_ils
for map.Vareudlevering and dim.Vareudlevering downstream consumers.

Grain: one row per CleanCode_Vareudlevering (deduplicated by prrowid).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset


@asset(
    key=AssetKey(["erp", "stg_dim_vareudlevering_ils"]),
    group_name="erp_staging",
    compute_kind="mssql",
    deps=[
        AssetKey(["erp", "pre_stg_vareudlevering_ils"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dim",
        "source": "ils",
        "entity": "vareudlevering",
    },
    description="Dimension staging for ILS delivery data — identifiers and descriptive attributes for map/dim Vareudlevering",
)
def stg_dim_vareudlevering_ils(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """
    Layer 1 dimension staging for ILS udlevering data.

    Selects identifier and descriptive columns from pre_stg_vareudlevering_ils,
    deduplicated at CleanCode_Vareudlevering grain. This feeds MapUpd_vareudlevering
    (which only needs CleanCode + prrowid) and DimUpd_vareudlevering (which needs
    the full set of descriptive attributes).
    """

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:

            context.log.info(
                "Creating staging table stg.dim_vareudlevering_ils from pre-staging..."
            )

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_vareudlevering_ils];")

            cursor.execute(
                """
                SELECT
                    CleanCode_Vareudlevering,
                    prrowid,

                    rvis AS Revisionsnummer,
                    CAST(flgsednr AS NVARCHAR(100)) AS Foelgeseddelnummer,
                    CAST(brik AS NVARCHAR(100)) AS Brik,
                    CAST(lnr AS NVARCHAR(100)) AS Leverandoernummer,
                    CAST(ordre AS NVARCHAR(100)) AS Ordrenummer,
                    CAST(ordlinie AS NVARCHAR(100)) AS Ordrelinje,
                    CAST(sw_direkte AS NVARCHAR(10)) AS sw_direkte_udlevering,
                    CAST(sw_paksdl AS NVARCHAR(10)) AS sw_pakkeseddel,
                    CAST(sw_flgsdl AS NVARCHAR(10)) AS sw_foelgeseddel,
                    CAST(sw_fly AS NVARCHAR(10)) AS sw_fly,
                    CAST(sw_haster AS NVARCHAR(10)) AS sw_haster,
                    CAST(sw_erstat AS NVARCHAR(10)) AS sw_erstat,
                    CAST(projnr AS NVARCHAR(100)) AS Projektnummer,
                    CAST(kto AS NVARCHAR(100)) AS Kontonummer_kode,
                    CAST(bruger AS NVARCHAR(100)) AS Bruger,
                    CAST(id AS NVARCHAR(100)) AS bk_prrowid,
                    CAST(refnr AS NVARCHAR(100)) AS Referencenummer,
                    CAST(reflinie AS NVARCHAR(100)) AS Referencelinje,
                    CAST(salgsordre AS NVARCHAR(100)) AS Salgsordre,
                    CAST(salgsordreint AS NVARCHAR(100)) AS SalgsordreId,
                    CAST(salgsfaknr AS NVARCHAR(100)) AS Salgsfakturanummer,
                    CAST(rvisvareid AS NVARCHAR(100)) AS RevisionsvarerId,
                    CAST(plukvareid AS NVARCHAR(100)) AS PlukvarerId,
                    CAST(aflevoper AS NVARCHAR(100)) AS Afleveringsoperatoer,
                    CAST(aflevtxt AS NVARCHAR(100)) AS Afleveringstekst,
                    CAST(aflevstreg AS NVARCHAR(100)) AS Afleveringsstregkode,
                    CAST(vognnummer AS NVARCHAR(100)) AS Vognnummer

                INTO [stg].[dim_vareudlevering_ils]
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY CleanCode_Vareudlevering
                            ORDER BY DW_LoadTimestamp DESC
                        ) AS rn
                    FROM [stg].[pre_vareudlevering_ils]
                    WHERE CleanCode_Vareudlevering IS NOT NULL
                ) ranked
                WHERE rn = 1;
            """
            )

            row_count = cursor.rowcount
            context.log.info(f"Created stg.dim_vareudlevering_ils with {row_count} rows")

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_dim_vareudlevering_ils_CleanCode
                ON [stg].[dim_vareudlevering_ils](CleanCode_Vareudlevering);
            """
            )
            context.log.info("Created unique clustered index on CleanCode_Vareudlevering")

            conn.commit()

            cursor.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    COUNT(prrowid) AS has_prrowid,
                    COUNT(Foelgeseddelnummer) AS has_foelgeseddel
                FROM [stg].[dim_vareudlevering_ils]
            """
            )
            stats = cursor.fetchone()
            context.log.info(
                f"Coverage: {stats[1]}/{stats[0]} with prrowid, "
                f"{stats[2]}/{stats[0]} with Foelgeseddelnummer"
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "table": "stg.dim_vareudlevering_ils",
            "row_count": row_count,
            "source_table": "stg.pre_vareudlevering_ils",
            "deduplication": "ROW_NUMBER() on CleanCode_Vareudlevering",
            "has_prrowid": stats[1],
            "has_foelgeseddel": stats[2],
            "layer": "dim (Layer 1)",
        }
    )
