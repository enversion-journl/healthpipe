# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension Update: IlsVare

Type 1 SCD -- MERGE from ext.vare_enriched_ils via map.IlsVare for DW_EK resolution.
DW_EK_UNSPSC and DW_EK_UNSPSC_Intern resolved here (at dim-time) via map.UNSPSC.
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, AssetKey, MaterializeResult, asset

RESOLVE_SQL = """
    DROP TABLE IF EXISTS #IlsVare_Resolved;

    SELECT
        m.DW_EK_IlsVare,
        stg.CleanCode_IlsVare,
        stg.vnr AS Varenummer,
        stg.navn AS Varenavn,
        stg.navn2 AS Varenavn2,
        stg.varenavn_concat AS Varenavn_Concat,
        stg.varestr AS Varestr,
        stg.basisenh AS Basisenhed,
        stg.vareart AS Vareart,
        stg.vareart_navn AS Vareart_Navn,
        stg.varegrp AS Varegruppe,
        stg.varegrp_navn AS Varegruppe_Navn,
        stg.varetype AS Varetype,
        stg.varetype_navn AS Varetype_Navn,
        stg.momskd AS Momskode,
        stg.momskode_navn AS Momskode_Navn,
        stg.momspct AS Momsprocent,
        COALESCE(
            dbo.fn_DateTime_To_DateKey(stg.oprdto), -1
        ) AS DW_EK_Dato_Oprettet,
        CASE
            WHEN stg.oprdto IS NOT NULL AND stg.oprtid IS NOT NULL
            THEN DATEADD(SECOND, stg.oprtid, CAST(stg.oprdto AS DATETIME2))
            WHEN stg.oprdto IS NOT NULL
            THEN CAST(stg.oprdto AS DATETIME2)
            ELSE NULL
        END AS Datotid_Oprettet,
        COALESCE(um_primary.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC,
        COALESCE(um_intern.[DW_EK_UNSPSC], -1) AS DW_EK_UNSPSC_Intern,
        stg.aktiv AS Aktiv
    INTO #IlsVare_Resolved
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY CleanCode_IlsVare
                ORDER BY pro2modified DESC, DW_LoadTimestamp DESC
            ) AS rn
        FROM [ext].[vare_enriched_ils]
        WHERE CleanCode_IlsVare IS NOT NULL
          AND CleanCode_IlsVare <> ''
    ) stg
    INNER JOIN [map].[IlsVare] m
        ON m.CleanCode_IlsVare = stg.CleanCode_IlsVare
    LEFT JOIN [map].[UNSPSC] um_primary
        ON um_primary.[CleanCode_UNSPSC] = NULLIF(LTRIM(RTRIM(stg.unspc)), '')
    LEFT JOIN [map].[UNSPSC] um_intern
        ON um_intern.[CleanCode_UNSPSC] = CASE
            WHEN stg.unspsc2 LIKE '%.0'
            THEN LEFT(LTRIM(RTRIM(stg.unspsc2)), LEN(LTRIM(RTRIM(stg.unspsc2))) - 2)
            ELSE NULLIF(LTRIM(RTRIM(stg.unspsc2)), '')
        END
    WHERE stg.rn = 1;
"""

MERGE_SQL = """
    MERGE INTO [dim].[IlsVare] AS tgt
    USING #IlsVare_Resolved AS src
        ON tgt.DW_EK_IlsVare = src.DW_EK_IlsVare
    WHEN MATCHED THEN UPDATE SET
        tgt.CleanCode_IlsVare = src.CleanCode_IlsVare,
        tgt.Varenummer = src.Varenummer,
        tgt.Varenavn = src.Varenavn,
        tgt.Varenavn2 = src.Varenavn2,
        tgt.Varenavn_Concat = src.Varenavn_Concat,
        tgt.Varestr = src.Varestr,
        tgt.Basisenhed = src.Basisenhed,
        tgt.Vareart = src.Vareart,
        tgt.Vareart_Navn = src.Vareart_Navn,
        tgt.Varegruppe = src.Varegruppe,
        tgt.Varegruppe_Navn = src.Varegruppe_Navn,
        tgt.Varetype = src.Varetype,
        tgt.Varetype_Navn = src.Varetype_Navn,
        tgt.Momskode = src.Momskode,
        tgt.Momskode_Navn = src.Momskode_Navn,
        tgt.Momsprocent = src.Momsprocent,
        tgt.DW_EK_Dato_Oprettet = src.DW_EK_Dato_Oprettet,
        tgt.Datotid_Oprettet = src.Datotid_Oprettet,
        tgt.DW_EK_UNSPSC = src.DW_EK_UNSPSC,
        tgt.DW_EK_UNSPSC_Intern = src.DW_EK_UNSPSC_Intern,
        tgt.Aktiv = src.Aktiv,
        tgt.DW_Timestamp_Update = SYSDATETIME()
    WHEN NOT MATCHED THEN INSERT (
        DW_EK_IlsVare, CleanCode_IlsVare, Varenummer,
        Varenavn, Varenavn2, Varenavn_Concat,
        Varestr, Basisenhed,
        Vareart, Vareart_Navn, Varegruppe, Varegruppe_Navn,
        Varetype, Varetype_Navn,
        Momskode, Momskode_Navn, Momsprocent,
        DW_EK_Dato_Oprettet, Datotid_Oprettet,
        DW_EK_UNSPSC, DW_EK_UNSPSC_Intern,
        Aktiv, DW_Timestamp_Insert, DW_Timestamp_Update
    ) VALUES (
        src.DW_EK_IlsVare, src.CleanCode_IlsVare, src.Varenummer,
        src.Varenavn, src.Varenavn2, src.Varenavn_Concat,
        src.Varestr, src.Basisenhed,
        src.Vareart, src.Vareart_Navn, src.Varegruppe, src.Varegruppe_Navn,
        src.Varetype, src.Varetype_Navn, src.Momskode, src.Momskode_Navn,
        src.Momsprocent,
        src.DW_EK_Dato_Oprettet, src.Datotid_Oprettet,
        src.DW_EK_UNSPSC, src.DW_EK_UNSPSC_Intern,
        src.Aktiv, SYSDATETIME(), SYSDATETIME()
    );
"""


@asset(
    group_name="erp_dimensions",
    compute_kind="mssql",
    deps=[
        AssetKey(["staging_infra_erp"]),
        AssetKey(["ext_vare_enriched_ils_daily"]),
        AssetKey(["ext_vare_enriched_ils_monthly"]),
        "map_ilsvare",
        AssetKey(["map_UNSPSC"]),
    ],
    tags={
        "domain": "erp",
        "layer": "dimensions",
        "module": "datawarehouse",
        "scd": "type1",
    },
    description="MERGE dim.IlsVare (Type 1 SCD) from ext.vare_enriched_ils",
)
def dim_ilsvare(
    context: AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> MaterializeResult:
    """Dimension maintenance: MERGE ILS item attributes."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(RESOLVE_SQL)
            resolved_count = cursor.rowcount
            context.log.info(f"Resolved {resolved_count} rows via map")

            cursor.execute(MERGE_SQL)
            merge_count = cursor.rowcount
            conn.commit()

            cursor.execute("DROP TABLE IF EXISTS #IlsVare_Resolved;")
            cursor.execute("UPDATE STATISTICS [dim].[IlsVare];")

            cursor.execute("SELECT COUNT(*) FROM [dim].[IlsVare]")
            total = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"dim.IlsVare: {merge_count} merged, {total} total")

    return MaterializeResult(
        metadata={
            "table": "dim.IlsVare",
            "merged_rows": merge_count,
            "total_rows": total,
            "resolved_from_map": resolved_count,
            "pattern": "MERGE (Type 1 SCD)",
        }
    )
