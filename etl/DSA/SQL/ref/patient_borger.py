# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.PatientToBorger — entity-mapping between fhir.Patient.patient_id and the
INT surrogate dw_ek_Borger used by dim.Borger and fact tables.

Read directly from fhir.Patient (which carries cpr / x_ecpr / d_ecpr as wide
columns after the patient_view projection). MERGE pattern:
  * INSERT new patient_ids — IDENTITY assigns dw_ek_Borger
  * UPDATE rows whose identifier values changed (re-activates soft-deleted
    rows automatically)
  * Soft-delete (dw_is_deleted = 1) rows whose patient_id is no longer present
    in fhir.Patient — preserves FK history for fact tables

Delta: positive-path MERGE uses DFM checkpoint (last_updated > checkpoint).
Cascade soft-delete anti-join remains full-scan (a deleted patient has no
last_updated to filter on).
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.etl_thresholds import check_soft_delete_threshold, get_soft_delete_threshold
from core.resources import MSSQLResource


@dg.asset(
    group_name="ref",
    compute_kind="mssql",
    deps=["sof_patient_dfm"],
    tags={"layer": "ref", "module": "shared_dimensions", "domain": "ref"},
    description=(
        "ref.PatientToBorger — assign INT IDENTITY dw_ek_Borger per patient_id "
        "via MERGE from fhir.Patient. Delta via DFM checkpoint on positive path; "
        "cascade soft-delete via full-scan anti-join."
    ),
)
def ref_patient_borger(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Build / refresh ref.PatientToBorger from fhir.Patient."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "PatientToBorger",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                context.log.info(
                    f"Merging identifiers from fhir.Patient into ref.PatientToBorger "
                    f"(checkpoint: {checkpoint})..."
                )

                cp_param = checkpoint or "1900-01-01"

                cursor.execute(
                    """
                    MERGE INTO [ref].[PatientToBorger] AS tgt
                    USING (
                        SELECT
                            p.patient_id,
                            p.patient_ssn AS cpr,
                            p.x_ecpr,
                            p.d_ecpr
                        FROM [fhir].[Patient] p
                        WHERE (p.patient_ssn IS NOT NULL
                               OR p.x_ecpr   IS NOT NULL
                               OR p.d_ecpr   IS NOT NULL)
                          AND p.last_updated > ?
                    ) AS src
                    ON tgt.patient_id = src.patient_id

                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (patient_id, cpr, x_ecpr, d_ecpr,
                                dw_is_deleted, dw_timestamp_insert, dw_timestamp_update)
                        VALUES (src.patient_id, src.cpr, src.x_ecpr, src.d_ecpr,
                                0,
                                CAST(SYSDATETIME() AS datetime2(0)),
                                CAST(SYSDATETIME() AS datetime2(0)))

                    WHEN MATCHED AND (
                           ISNULL(tgt.cpr,    '') <> ISNULL(src.cpr,    '')
                        OR ISNULL(tgt.x_ecpr, '') <> ISNULL(src.x_ecpr, '')
                        OR ISNULL(tgt.d_ecpr, '') <> ISNULL(src.d_ecpr, '')
                        OR tgt.dw_is_deleted = 1
                    ) THEN
                        UPDATE SET
                            cpr                 = src.cpr,
                            x_ecpr              = src.x_ecpr,
                            d_ecpr              = src.d_ecpr,
                            dw_is_deleted        = 0,
                            dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0));
                """,
                    [cp_param],
                )
                merge_affected = cursor.rowcount

                cursor.execute(
                    """
                    UPDATE tgt
                    SET dw_is_deleted        = 1,
                        dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                    FROM [ref].[PatientToBorger] tgt
                    WHERE tgt.dw_is_deleted = 0
                      AND NOT EXISTS (
                          SELECT 1 FROM [fhir].[Patient] p
                          WHERE p.patient_id = tgt.patient_id
                      );
                """
                )
                soft_deleted = cursor.rowcount

                check_soft_delete_threshold(
                    "ref.PatientToBorger",
                    soft_deleted,
                    context,
                    override=config.soft_delete_threshold,
                )

                cursor.execute(
                    """
                    SELECT CONVERT(NVARCHAR(50), MAX(p.last_updated), 121)
                    FROM [fhir].[Patient] p
                    WHERE p.last_updated > ?
                """,
                    [cp_param],
                )
                row = cursor.fetchone()
                new_checkpoint = row[0] if row and row[0] else checkpoint

                conn.commit()

                if config.should_advance_checkpoint and new_checkpoint is not None:
                    dfm.set_checkpoint(
                        "ref", "PatientToBorger", value=new_checkpoint, mode=config.mode
                    )

                cursor.execute("SELECT COUNT(*) FROM [ref].[PatientToBorger]")
                total_rows = cursor.fetchone()[0]
                cursor.execute(
                    "SELECT COUNT(*) FROM [ref].[PatientToBorger] WHERE dw_is_deleted = 1"
                )
                deleted_total = cursor.fetchone()[0]

                context.log.info(
                    f"ref.PatientToBorger: {merge_affected} rows touched by MERGE, "
                    f"{soft_deleted} soft-deleted this run, {total_rows} total "
                    f"({deleted_total} marked deleted). Checkpoint → {new_checkpoint}."
                )

                dfm.complete_dfm_run(
                    run_ctx.log_id,
                    checkpoint_after=new_checkpoint,
                    rows_affected=merge_affected + soft_deleted,
                )
            finally:
                cursor.close()
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "merge_affected": merge_affected,
            "soft_deleted_this_run": soft_deleted,
            "threshold": get_soft_delete_threshold(
                "ref.PatientToBorger", config.soft_delete_threshold
            ),
            "total_rows": total_rows,
            "deleted_rows_total": deleted_total,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "table": "ref.PatientToBorger",
            "mode": config.mode,
        }
    )
