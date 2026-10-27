# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.Organisation — entity-mapping between fhir.Organization.organization_id and
the INT surrogate dw_ek_Organisation used by dim.Organisation and every fact /
dim that FKs to it.

Replaces map.Organisation as the source of dw_ek_Organisation. Read directly
from fhir.Organization (SOR). Every DELETION runs before the MERGE, and every
RESURRECTION happens inside it:
  1. fhir says deleted        -> soft-delete (frees that row's live SOR code)
  2. fhir lost its SOR id     -> soft-delete (sor_code can no longer be trusted,
                                 and a live row would hold the code hostage:
                                 it is invisible to the MERGE, which filters
                                 identifier_value_sor IS NOT NULL)
  3. UUID gone from fhir      -> soft-delete (preserves DW_EK for fact FKs)
  4. MERGE                    -> INSERT new organization_ids (IDENTITY assigns
                                 dw_ek_Organisation), and for matched rows set
                                 sor_code and dw_is_deleted TOGETHER

Order matters for UQ_Ref_Organisation_SorCode_Live. After steps 1-3 every live
ref row is present in the MERGE source, whose live rows are unique per SOR code
(UQ_FHIR_Organization_IdentifierValueSor_Live), so the MERGE cannot leave two
live rows on one code. Resurrecting in a separate statement ahead of the MERGE
would break that: it would clear dw_is_deleted without also correcting a stale
sor_code, and the freed code may already be live on another UUID.

Full-scan (no DFM checkpoint): SOR is a small dimension and sof_organization
is itself a full refresh, so the delta machinery used by ref.PatientToBorger
buys nothing here.
"""

import dagster as dg
from core.resources import MSSQLResource


@dg.asset(
    name="ref_organisation",
    group_name="ref",
    compute_kind="mssql",
    deps=["sof_organization"],
    tags={"layer": "ref", "module": "dwh_organisation", "domain": "ref"},
    description=(
        "ref.Organisation — assign INT IDENTITY dw_ek_Organisation per "
        "organization_id from fhir.Organization. Soft-deletes first (fhir flag, "
        "missing SOR id, vanished UUID), then MERGE inserts and resurrects."
    ),
)
def ref_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Build / refresh ref.Organisation from fhir.Organization."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info("Merging identifiers from fhir.Organization into ref.Organisation...")

            # (1) fhir says deleted. Deletions only — the MERGE resurrects, so that
            # a cleared flag and a corrected sor_code land in the same statement.
            cursor.execute(
                """
                UPDATE tgt
                SET dw_is_deleted        = 1,
                    dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [ref].[Organisation] tgt
                INNER JOIN [fhir].[Organization] f
                    ON f.organization_id = tgt.organization_id
                WHERE tgt.dw_is_deleted = 0
                  AND f.dw_is_deleted = 1;
                """
            )
            flag_deletes = cursor.rowcount

            # (2) fhir row lost its SOR identifier. It drops out of the MERGE source,
            # so a live ref row here would keep a stale sor_code forever and block
            # any other UUID that later takes that code.
            cursor.execute(
                """
                UPDATE tgt
                SET dw_is_deleted        = 1,
                    dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [ref].[Organisation] tgt
                INNER JOIN [fhir].[Organization] f
                    ON f.organization_id = tgt.organization_id
                WHERE tgt.dw_is_deleted = 0
                  AND f.identifier_value_sor IS NULL;
                """
            )
            unidentified_deletes = cursor.rowcount

            # (3) UUID gone from fhir entirely.
            cursor.execute(
                """
                UPDATE tgt
                SET dw_is_deleted        = 1,
                    dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [ref].[Organisation] tgt
                WHERE tgt.dw_is_deleted = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM [fhir].[Organization] f
                      WHERE f.organization_id = tgt.organization_id
                  );
            """
            )
            anti_join_deleted = cursor.rowcount

            # (4) INSERT new UUIDs, and for matched rows set sor_code and
            # dw_is_deleted together — this is the only statement that resurrects.
            cursor.execute(
                """
                MERGE INTO [ref].[Organisation] AS tgt
                USING (
                    SELECT
                        f.organization_id,
                        f.identifier_value_sor AS sor_code,
                        CAST(CASE WHEN f.dw_is_deleted = 1 THEN 1 ELSE 0 END AS BIT)
                            AS dw_is_deleted
                    FROM [fhir].[Organization] f
                    WHERE f.identifier_value_sor IS NOT NULL
                ) AS src
                ON tgt.organization_id = src.organization_id

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (organization_id, sor_code,
                            dw_is_deleted, dw_timestamp_insert, dw_timestamp_update)
                    VALUES (src.organization_id, src.sor_code,
                            src.dw_is_deleted,
                            CAST(SYSDATETIME() AS datetime2(0)),
                            CAST(SYSDATETIME() AS datetime2(0)))

                WHEN MATCHED AND (
                       ISNULL(tgt.sor_code, '') <> ISNULL(src.sor_code, '')
                    OR tgt.dw_is_deleted <> src.dw_is_deleted
                ) THEN
                    UPDATE SET
                        sor_code            = src.sor_code,
                        dw_is_deleted        = src.dw_is_deleted,
                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0));
            """
            )
            merge_affected = cursor.rowcount

            soft_deleted = flag_deletes + unidentified_deletes + anti_join_deleted

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [ref].[Organisation]")
            total_rows = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM [ref].[Organisation] WHERE dw_is_deleted = 1")
            deleted_total = cursor.fetchone()[0]

            context.log.info(
                f"ref.Organisation: {merge_affected} rows touched by MERGE, "
                f"{soft_deleted} soft-deleted this run ({flag_deletes} from fhir flag, "
                f"{unidentified_deletes} lost their SOR id, "
                f"{anti_join_deleted} anti-join), "
                f"{total_rows} total ({deleted_total} marked deleted)."
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "soft_deleted_this_run": soft_deleted,
            "flag_deletes": flag_deletes,
            "unidentified_deletes": unidentified_deletes,
            "anti_join_deleted": anti_join_deleted,
            "total_rows": total_rows,
            "deleted_rows_total": deleted_total,
            "table": "ref.Organisation",
        }
    )
