# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.Organisation — entity-mapping between fhir.Organization.organization_id and
the INT surrogate DW_EK_Organisation used by dim.Organisation and every fact /
dim that FKs to it.

Replaces map.Organisation as the source of DW_EK_Organisation. Read directly
from fhir.Organization (SOR). MERGE pattern (mirrors ref.Patient_Borger):
  * INSERT new organization_ids — IDENTITY assigns DW_EK_Organisation
  * UPDATE rows whose sor_code changed (re-activates soft-deleted rows)
  * Soft-delete (DW_IsDeleted = 1) rows whose organization_id is no longer
    present in fhir.Organization — preserves FK history for fact tables

Full-scan (no DFM checkpoint): SOR is a small dimension and sof_organization
is itself a full refresh, so the delta machinery used by ref.Patient_Borger
buys nothing here.
"""

import dagster as dg
from core.etl_thresholds import check_soft_delete_threshold, get_soft_delete_threshold
from core.resources import MSSQLResource


@dg.asset(
    name="ref_organisation",
    group_name="ref",
    compute_kind="mssql",
    deps=["sof_organization"],
    tags={"layer": "ref", "module": "dwh_organisation", "domain": "ref"},
    description=(
        "ref.Organisation — assign INT IDENTITY DW_EK_Organisation per "
        "organization_id via MERGE from fhir.Organization. Full-scan MERGE + "
        "cascade soft-delete anti-join."
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

            cursor.execute(
                """
                MERGE INTO [ref].[Organisation] AS tgt
                USING (
                    SELECT
                        f.organization_id,
                        f.identifier_value_sor AS sor_code
                    FROM [fhir].[Organization] f
                    WHERE f.identifier_value_sor IS NOT NULL
                ) AS src
                ON tgt.organization_id = src.organization_id

                WHEN NOT MATCHED BY TARGET THEN
                    INSERT (organization_id, sor_code,
                            DW_IsDeleted, DW_Timestamp_Insert)
                    VALUES (src.organization_id, src.sor_code,
                            0, CAST(SYSDATETIME() AS datetime2(0)))

                WHEN MATCHED AND (
                       ISNULL(tgt.sor_code, '') <> ISNULL(src.sor_code, '')
                    OR tgt.DW_IsDeleted = 1
                ) THEN
                    UPDATE SET
                        sor_code            = src.sor_code,
                        DW_IsDeleted        = 0,
                        DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0));
            """
            )
            merge_affected = cursor.rowcount

            cursor.execute(
                """
                UPDATE tgt
                SET DW_IsDeleted        = 1,
                    DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))
                FROM [ref].[Organisation] tgt
                WHERE tgt.DW_IsDeleted = 0
                  AND NOT EXISTS (
                      SELECT 1 FROM [fhir].[Organization] f
                      WHERE f.organization_id = tgt.organization_id
                  );
            """
            )
            soft_deleted = cursor.rowcount

            check_soft_delete_threshold("ref.Organisation", soft_deleted, context)

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [ref].[Organisation]")
            total_rows = cursor.fetchone()[0]
            cursor.execute("SELECT COUNT(*) FROM [ref].[Organisation] WHERE DW_IsDeleted = 1")
            deleted_total = cursor.fetchone()[0]

            context.log.info(
                f"ref.Organisation: {merge_affected} rows touched by MERGE, "
                f"{soft_deleted} soft-deleted this run, {total_rows} total "
                f"({deleted_total} marked deleted)."
            )
        finally:
            cursor.close()

    return dg.MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "soft_deleted_this_run": soft_deleted,
            "threshold": get_soft_delete_threshold("ref.Organisation"),
            "total_rows": total_rows,
            "deleted_rows_total": deleted_total,
            "table": "ref.Organisation",
        }
    )
