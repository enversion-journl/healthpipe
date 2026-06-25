# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Staging for SydAx Organisation data from Finans.Afdeling.

This module extracts organisation hierarchy data from the SydAx financial system.
SydAx organisations use their own code system and are treated as "other" type codes
that can potentially be mapped to SOR via the Terminology Server.

Code system: SydAx
CleanCode format: SydAx_{Afdelingsnummer}
Parent derivation: Remove last segment after '.' (e.g., "SHS.57.7.6" → parent="SHS.57.7")
"""

import dagster as dg
from core.resources import MSSQLResource
from dagster import AssetKey


@dg.asset(
    name="stg_organisation_sydax",
    deps=[AssetKey(["ext_afdeling_sydax_daily"])],
    group_name="shared_organisation_staging",
    description="SydAx organisation staging from ext_afdeling_sydax. Extracts latest valid codes with parent hierarchy.",
)
def stg_organisation_sydax(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> list[dict]:
    """
    Extract organisation data from [ext].[afdeling_sydax] (populated by ext_afdeling_sydax_daily).

    Uses ROW_NUMBER() to get the latest valid code per Afdelingsnummer,
    and derives parent_code by removing the last segment after '.'.

    Output fields per entry:
    - code: Afdelingsnummer (e.g., "SHS.57.7.6")
    - display: Afdelingstekst (organisation name)
    - parent_code: Derived parent (e.g., "SHS.57.7" for code "SHS.57.7.6")
    """
    query = """
        WITH LatestCodes AS (
            SELECT
                Afdelingsnummer,
                Afdelingstekst,
                StartDateTime,
                EndDateTime,
                ROW_NUMBER() OVER (PARTITION BY Afdelingsnummer ORDER BY StartDateTime DESC) AS rn
            FROM [ext].[afdeling_sydax]
        )
        SELECT
            Afdelingsnummer AS code,
            Afdelingstekst AS display,
            CASE
                WHEN CHARINDEX('.', Afdelingsnummer) > 0
                THEN LEFT(Afdelingsnummer, LEN(Afdelingsnummer) - CHARINDEX('.', REVERSE(Afdelingsnummer)))
                ELSE NULL
            END AS parent_code
        FROM LatestCodes
        WHERE rn = 1
    """

    context.log.info("Querying [ext].[afdeling_sydax] for organisation hierarchy")

    entries = []
    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)

            # Get column names from cursor description
            columns = [col[0] for col in cursor.description]

            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row, strict=False))

                code = row_dict.get("code")
                display = row_dict.get("display")
                parent_code = row_dict.get("parent_code")

                if not code:
                    continue

                # Clean up values
                code = str(code).strip() if code else None
                display = str(display).strip() if display else None
                parent_code = str(parent_code).strip() if parent_code else None

                if code:
                    entries.append(
                        {
                            "code": code,
                            "display": display,
                            "parent_code": parent_code,
                        }
                    )
        finally:
            cursor.close()

    # Log summary
    with_parent = sum(1 for e in entries if e["parent_code"])
    context.log.info(
        f"Extracted {len(entries)} SydAx organisations "
        f"({with_parent} with parent codes, {len(entries) - with_parent} root level)"
    )

    # Log a few examples for debugging
    if entries:
        context.log.debug("Example SydAx entries (first 5):")
        for entry in entries[:5]:
            display_preview = entry["display"][:50] if entry["display"] else "N/A"
            context.log.debug(
                f"  code={entry['code']}, parent={entry['parent_code']}, "
                f"display={display_preview}..."
            )

    return entries
