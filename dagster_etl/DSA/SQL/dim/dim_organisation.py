# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Dimension table asset for Organisation (SOR only).

Populates dim.Organisation with Type 1 SCD, sourced from fhir.Organization
(attributes) + ref.Organisation (the INT surrogate DW_EK_Organisation).

This replaces the legacy path (stg_organisation_sor + map.Organisation,
resolved row-by-row in Python). DW_EK_Organisation now comes from
ref.Organisation; the self-referencing parent (DW_EK_Org_Parent) is resolved
by joining fhir.Organization.part_of_id back to ref.Organisation.

Type 1: every attribute (Name, Type, Active, parent) is overwritten on update.
Rows are never deleted — an org that leaves SOR keeps its dim row so historical
fact FKs to DW_EK_Organisation still resolve. The "Unknown" sentinel (-1) is
seeded by the DDL.
"""

import dagster as dg
from core.resources import MSSQLResource


@dg.asset(
    name="dim_organisation",
    deps=["staging_infra_organisation", "ref_organisation"],
    group_name="dwh_organisation",
    compute_kind="mssql",
    description="Populate dim.Organisation (Type 1 SCD, SOR only) from fhir.Organization + ref.Organisation",
)
def dim_organisation(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Populate dim.Organisation (Type 1 SCD) from fhir.Organization + ref.Organisation."""

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            context.log.info(
                "Merging dim.Organisation from fhir.Organization + ref.Organisation..."
            )

            cursor.execute(
                """
                MERGE INTO [dim].[Organisation] AS target
                USING (
                    SELECT
                        r.DW_EK_Organisation,
                        r.sor_code                                  AS Code,
                        COALESCE(rp.DW_EK_Organisation, -1)         AS DW_EK_Org_Parent,
                        rp.sor_code                                 AS ParentCode,
                        f.name                                      AS Name,
                        f.type_display                              AS Type,
                        CASE WHEN LOWER(f.active) = 'true' THEN 1 ELSE 0 END AS Active
                    FROM [ref].[Organisation] r
                    JOIN [fhir].[Organization] f
                        ON f.organization_id = r.organization_id
                    LEFT JOIN [ref].[Organisation] rp
                        ON rp.organization_id = f.part_of_id
                       AND rp.DW_IsDeleted = 0
                    WHERE r.DW_IsDeleted = 0
                ) AS source
                ON target.DW_EK_Organisation = source.DW_EK_Organisation
                WHEN MATCHED THEN
                    UPDATE SET
                        Code = source.Code,
                        DW_EK_Org_Parent = source.DW_EK_Org_Parent,
                        ParentCode = source.ParentCode,
                        Name = source.Name,
                        Type = source.Type,
                        Active = source.Active,
                        DW_Timestamp_Update = GETDATE()
                WHEN NOT MATCHED THEN
                    INSERT (DW_EK_Organisation, Code,
                            DW_EK_Org_Parent, ParentCode,
                            Name, Type, Active,
                            DW_Timestamp_Insert, DW_Timestamp_Update)
                    VALUES (source.DW_EK_Organisation, source.Code,
                            source.DW_EK_Org_Parent, source.ParentCode,
                            source.Name, source.Type, source.Active,
                            GETDATE(), GETDATE())
                OUTPUT $action;
            """
            )

            merge_results = cursor.fetchall()
            inserted = sum(1 for r in merge_results if r[0] == "INSERT")
            updated = sum(1 for r in merge_results if r[0] == "UPDATE")

            conn.commit()

            cursor.execute("SELECT COUNT(*) FROM [dim].[Organisation]")
            total_rows = cursor.fetchone()[0]
        finally:
            cursor.close()

    context.log.info(
        f"dim.Organisation: {inserted} inserted, {updated} updated, {total_rows} total"
    )

    return dg.MaterializeResult(
        metadata={
            "inserted": inserted,
            "updated": updated,
            "total_rows": total_rows,
            "method": "sql_merge_from_ref",
        }
    )
