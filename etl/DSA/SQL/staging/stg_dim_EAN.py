# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging Asset: dim_EAN from MySupply invoice data enriched with NemHandelsregisteret.

Reads distinct ean numbers straight from the MySupply invoice-header archive
(arc.mysupply_statistic_invoice_header.RegningsModtagerID) and enriches with
participant names from the ean codesystem table (codesystem.ean_concept).
Writes the result to stg.dim_EAN in the staging database.

The ean natural key (clean_code_EAN) is the plain cast of RegningsModtagerID.
This matches clean_code_EAN_Regningsmodtager on the fact side, so the
fct.Faktura / fct.Fakturalinje FK -> dim.ean (via map.ean) resolves.

The set of distinct RegningsModtagerID on the header archive is a superset of
the EANs that reach the facts, so map.ean holds every ean a fact can reference.

Flat structure (no hierarchy, no SOR ConceptMap).
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource


@dg.asset(
    name="stg_dim_EAN",
    group_name="erp_staging",
    compute_kind="mssql",
    description=(
        "Stage ean dimension from the MySupply invoice-header archive enriched with "
        "NemHandelsregisteret. Reads distinct RegningsModtagerID from "
        "arc.mysupply_statistic_invoice_header, LEFT JOINs codesystem.ean_concept "
        "for navn_nemhandelsregister."
    ),
    deps=[
        "cs_ean",
        dg.AssetKey(["arc_mysupply_statistic_invoice_header"]),
    ],
)
def stg_dim_EAN(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run("stg", "dim_EAN", dagster_run_id=context.run_id, mode=config.mode)

    row_count = 0
    matched = 0

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT Code, display FROM [codesystem].[EANConcept]")
                name_lookup = {str(r[0]).strip(): r[1] for r in cursor.fetchall() if r[1]}
                context.log.info(f"Loaded {len(name_lookup)} NemHandel name mappings")

                # clean_code_EAN is the plain cast of RegningsModtagerID, byte-identical
                # to clean_code_EAN_Regningsmodtager on the fact side, so the keys align
                # when fct.Faktura/fct.Fakturalinje resolve against map.ean.
                cursor.execute(
                    """
                    SELECT DISTINCT CAST(RegningsModtagerID AS NVARCHAR(200)) AS clean_code_EAN
                    FROM [arc].[mysupply_statistic_invoice_header]
                    WHERE RegningsModtagerID IS NOT NULL
                      AND CAST(RegningsModtagerID AS NVARCHAR(200)) <> ''
                """
                )
                ean_codes = [row[0] for row in cursor.fetchall()]
                context.log.info(
                    f"Found {len(ean_codes)} distinct ean codes from arc invoice header"
                )

                rows = []
                for code in ean_codes:
                    navn = name_lookup.get(code)
                    if navn:
                        matched += 1
                    rows.append((code, code, navn))

                context.log.info(
                    f"NemHandel enrichment: {matched}/{len(rows)} EANs matched to names"
                )

                cursor.execute("DROP TABLE IF EXISTS [stg].[dim_EAN];")

                cursor.execute(
                    """
                    CREATE TABLE [stg].[dim_EAN] (
                        [clean_code_EAN] NVARCHAR(50) NOT NULL,
                        [ean] NVARCHAR(50) NOT NULL,
                        [navn_nemhandelsregister] NVARCHAR(512)
                    );
                """
                )

                placeholders = ", ".join(["?"] * 3)
                insert_sql = f"INSERT INTO [stg].[dim_EAN] VALUES ({placeholders})"

                if hasattr(cursor, "fast_executemany"):
                    cursor.fast_executemany = True

                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i : i + batch_size]
                    cursor.executemany(insert_sql, batch)

                cursor.execute(
                    """
                    CREATE UNIQUE CLUSTERED INDEX IX_stg_dim_ean_CleanCode
                    ON [stg].[dim_EAN]([clean_code_EAN]);
                """
                )

                conn.commit()

                cursor.execute("SELECT COUNT(*) FROM [stg].[dim_EAN]")
                row_count = cursor.fetchone()[0]
            finally:
                cursor.close()

        context.log.info(f"stg.dim_EAN: {row_count} rows")
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=row_count)
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "table": "stg.dim_EAN",
            "row_count": row_count,
            "nemhandel_matched": matched,
        }
    )
