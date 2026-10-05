# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
fct_apply_faktura_postering_relations — refresh fct.Postering.dw_ek_Faktura from bridge.

After ref.FakturaToPostering is (re)built each run, this pass copies the
"newest" faktura per posting onto the posting fact:

  fct.Postering.dw_ek_Faktura  ← newest faktura header for the postering

The reverse link — the newest posting/account for an invoice — is NOT stored on
fct.Faktura (finding 2.2: no fact-to-fact FK on the invoice facts). It is exposed
at query time via vw_fct.Faktura / vw_fct.Fakturalinje, which join the newest
posting through this same bridge (on dw_ek_Faktura) and surface
DW_EK_Kontoplan_Postering. Account-weighted measures aggregate fct.Postering.

Two-part:
  1. REFRESH — set dw_ek_Faktura to the bridge's current newest where it differs.
  2. DEMOTE  — reset dw_ek_Faktura to -1 when no live (dw_is_deleted=0) newest bridge
     row exists for it anymore (the match disappeared / was soft-deleted), so a
     retracted pairing does not leave a stale link behind.

Only rows where the value actually changes are touched, so the DSO sync
watermark (dw_timestamp_update) bumps only on real change (idempotent).
"""

from core.resources import MSSQLResource
from dagster import AssetExecutionContext, MaterializeResult, asset


@asset(
    group_name="erp_ref",
    compute_kind="mssql",
    deps=["ref_faktura_to_postering"],
    tags={"layer": "fct", "module": "erp", "domain": "erp"},
    description=(
        "Apply-pass: copy the newest faktura per posting from "
        "ref.FakturaToPostering into fct.Postering.dw_ek_Faktura."
    ),
)
def fct_apply_faktura_postering_relations(
    context: AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> MaterializeResult:
    """Refresh fct.Postering.dw_ek_Faktura from the bridge."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                """
                UPDATE p
                SET p.dw_ek_Faktura      = b.dw_ek_Faktura,
                    p.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [fct].[Postering] p
                JOIN [ref].[FakturaToPostering] b
                  ON b.dw_ek_Postering = p.dw_ek_Postering
                 AND b.is_newest_faktura_for_postering = 1
                 AND b.dw_is_deleted = 0
                WHERE p.dw_ek_Faktura <> b.dw_ek_Faktura;
            """
            )
            postering_refreshed = cursor.rowcount

            # DEMOTE: reset to -1 when the match no longer exists in the bridge.
            cursor.execute(
                """
                UPDATE p
                SET p.dw_ek_Faktura      = -1,
                    p.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [fct].[Postering] p
                WHERE p.dw_ek_Faktura <> -1
                  AND NOT EXISTS (
                      SELECT 1 FROM [ref].[FakturaToPostering] b
                      WHERE b.dw_ek_Postering = p.dw_ek_Postering
                        AND b.is_newest_faktura_for_postering = 1
                        AND b.dw_is_deleted = 0
                  );
            """
            )
            postering_demoted = cursor.rowcount

            conn.commit()

            context.log.info(
                f"Apply-pass: fct.Postering {postering_refreshed} refreshed / "
                f"{postering_demoted} demoted (dw_ek_Faktura)."
            )
        finally:
            cursor.close()

    return MaterializeResult(
        metadata={
            "postering_rows_refreshed": postering_refreshed,
            "postering_rows_demoted": postering_demoted,
            "table": "fct.Postering (dw_ek_Faktura apply)",
        }
    )
