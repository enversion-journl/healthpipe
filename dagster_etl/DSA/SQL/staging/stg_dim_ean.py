# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: dim_ean from MySupply invoice data enriched with NemHandelsregisteret.

Reads distinct EAN numbers from stg.pre_fakturalinje_mysupply (CleanCode_EAN_Regningsmodtager)
and enriches with participant names from the EAN codesystem table (codesystem.ean_concept).
Writes the result to stg.dim_ean in the staging database.

Flat structure (no hierarchy, no SOR ConceptMap).
"""

import dagster as dg
from core.resources import MSSQLResource


@dg.asset(
    name="stg_dim_ean",
    group_name="erp_staging",
    compute_kind="mssql",
    description=(
        "Stage EAN dimension from pre_stg invoice data enriched with NemHandelsregisteret. "
        "Reads distinct CleanCode_EAN_Regningsmodtager from stg.pre_fakturalinje_mysupply, "
        "LEFT JOINs codesystem.ean_concept for Navn_Nemhandelsregister."
    ),
    deps=[
        "cs_ean",
        dg.AssetKey(["erp", "pre_stg_fakturalinje_mysupply"]),
    ],
)
def stg_dim_ean(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT Code, Display FROM [codesystem].[EANConcept]")
            name_lookup = {str(r[0]).strip(): r[1] for r in cursor.fetchall() if r[1]}
            context.log.info(f"Loaded {len(name_lookup)} NemHandel name mappings")

            cursor.execute(
                """
                SELECT DISTINCT CleanCode_EAN_Regningsmodtager AS CleanCode_EAN
                FROM [stg].[pre_fakturalinje_mysupply]
                WHERE CleanCode_EAN_Regningsmodtager IS NOT NULL
                  AND CleanCode_EAN_Regningsmodtager <> ''
            """
            )
            ean_codes = [row[0] for row in cursor.fetchall()]
            context.log.info(f"Found {len(ean_codes)} distinct EAN codes from pre_stg")

            rows = []
            matched = 0
            for code in ean_codes:
                navn = name_lookup.get(code)
                if navn:
                    matched += 1
                rows.append((code, code, navn))

            context.log.info(f"NemHandel enrichment: {matched}/{len(rows)} EANs matched to names")

            cursor.execute("DROP TABLE IF EXISTS [stg].[dim_ean];")

            cursor.execute(
                """
                CREATE TABLE [stg].[dim_ean] (
                    [CleanCode_EAN] NVARCHAR(50) NOT NULL,
                    [EAN] NVARCHAR(50) NOT NULL,
                    [Navn_Nemhandelsregister] NVARCHAR(512)
                );
            """
            )

            placeholders = ", ".join(["?"] * 3)
            insert_sql = f"INSERT INTO [stg].[dim_ean] VALUES ({placeholders})"

            if hasattr(cursor, "fast_executemany"):
                cursor.fast_executemany = True

            batch_size = 1000
            for i in range(0, len(rows), batch_size):
                batch = rows[i : i + batch_size]
                cursor.executemany(insert_sql, batch)

            cursor.execute(
                """
                CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_ean_CleanCode
                ON [stg].[dim_ean]([CleanCode_EAN]);
            """
            )

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [stg].[dim_ean]")
            row_count = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(f"stg.dim_ean: {row_count} rows")

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_ean",
            "row_count": row_count,
            "nemhandel_matched": matched,
        }
    )
