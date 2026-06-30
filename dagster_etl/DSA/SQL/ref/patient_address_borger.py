# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
ref.Patient_Address_Borger — version-mapping for dim.Borger's T2 history.

Assigns DW_SK_Borger (INT IDENTITY) per (patient x address-period) and
derives non-overlapping half-open [DW_ValidFrom, DW_ValidTo) intervals
from address_period_start via LEAD(). address_period_end does NOT
influence DW_ValidTo — this guarantees fact-table joins on
  WHERE event_date >= DW_ValidFrom AND event_date < DW_ValidTo
always match exactly one row, even when source data has overlapping
period_end values.

Patient-scoped MERGE with delta: only patients whose fhir.Patient_Address
rows have last_updated > checkpoint are reprocessed. For each such patient,
ALL address periods are rebuilt via LEAD to keep DW_ValidTo consistent.
Soft-deleted rows (DW_IsDeleted = 1) are flagged when a (patient_id,
address_period_start) disappears from the rebuilt set.

Rules implemented:
  1. Sort per patient_id by address_period_start ASC; tiebreak by
     last_updated DESC (matches the dedup pattern in fhir.Patient_Address).
  2. DW_ValidFrom = address_period_start, except the first row per
     patient (smallest address_period_start) is forced to '1900-01-01'.
  3. DW_ValidTo = LEAD(address_period_start); last row per patient
     gets '9999-12-31 23:59:59'. Half-open: no -1 second.
  4. DW_IsCurrent = 1 where DW_ValidTo = '9999-12-31 23:59:59'.

Patients with no rows in fhir.Patient_Address still get one default row
(address_period_start = '1900-01-01', DW_ValidFrom/DW_ValidTo = full sentinel
range) so dim.Borger has a current version for every borger.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.etl_thresholds import check_soft_delete_threshold, get_soft_delete_threshold
from core.resources import MSSQLResource


def _global_temp_table(base: str, run_id: str) -> str:
    """Build a session-unique global temp table name."""
    safe_run_id = "".join(ch for ch in run_id if ch.isalnum())[:20]
    return f"##{base}_{safe_run_id}"


@dg.asset(
    group_name="ref",
    compute_kind="mssql",
    deps=["sof_patient_address_dfm", "ref_patient_borger"],
    tags={"layer": "ref", "module": "shared_dimensions", "domain": "ref", "scd_type": "type2"},
    description=(
        "ref.Patient_Address_Borger — INT IDENTITY DW_SK_Borger per "
        "(patient x address-period) with non-overlapping half-open "
        "[DW_ValidFrom, DW_ValidTo) derived via LEAD(address_period_start). "
        "Patient-scoped MERGE with DFM checkpoint."
    ),
)
def ref_patient_address_borger(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Merge ref.Patient_Address_Borger using patient-scoped delta from fhir.Patient_Address."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "Patient_Address_Borger",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cp_param = checkpoint or "1900-01-01"
                changed_patients_table = _global_temp_table("changed_patients_pab", context.run_id)
                staged_rows_table = _global_temp_table("tmp_pab", context.run_id)
                merge_out_table = _global_temp_table("merge_out_pab", context.run_id)

                context.log.info(
                    f"Patient-scoped MERGE for ref.Patient_Address_Borger "
                    f"(checkpoint: {checkpoint})..."
                )

                for table_name in (
                    merge_out_table,
                    staged_rows_table,
                    changed_patients_table,
                ):
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

                try:
                    cursor.execute(
                        f"""
                        SELECT DISTINCT patient_id
                        INTO {changed_patients_table}
                        FROM [fhir].[Patient_Address]
                        WHERE last_updated > ?
                        """,
                        [cp_param],
                    )
                    cursor.execute(f"SELECT COUNT(*) FROM {changed_patients_table}")
                    changed_count = cursor.fetchone()[0]
                    context.log.info(f"Found {changed_count} changed patients since {checkpoint}")

                    if changed_count == 0:
                        conn.commit()
                        cursor.execute("SELECT COUNT(*) FROM [ref].[Patient_Address_Borger]")
                        total = cursor.fetchone()[0]
                        cursor.execute(
                            "SELECT COUNT(*) FROM [ref].[Patient_Address_Borger] "
                            "WHERE DW_IsCurrent = 1 AND DW_IsDeleted = 0"
                        )
                        current = cursor.fetchone()[0]
                        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
                        return dg.MaterializeResult(
                            metadata={
                                "changed_patients": 0,
                                "merge_affected": 0,
                                "soft_deleted_this_run": 0,
                                "threshold": get_soft_delete_threshold(
                                    "ref.Patient_Address_Borger"
                                ),
                                "current_versions": current,
                                "total_versions": total,
                                "checkpoint_before": checkpoint,
                                "checkpoint_after": checkpoint,
                                "table": "ref.Patient_Address_Borger",
                                "mode": config.mode,
                            }
                        )

                    cursor.execute(
                        f"""
                        WITH SourceWithDefaults AS (
                            SELECT
                                rpb.patient_id,
                                rpb.cpr, rpb.x_ecpr, rpb.d_ecpr,
                                COALESCE(
                                    TRY_CAST(pa.address_period_start AS datetime2(0)),
                                    CAST('1900-01-01' AS datetime2(0))
                                ) AS address_period_start,
                                TRY_CAST(pa.address_period_end AS datetime2(0)) AS address_period_end,
                                pa.last_updated
                            FROM [ref].[Patient_Borger] rpb
                            INNER JOIN [fhir].[Patient_Address] pa
                                ON pa.patient_id = rpb.patient_id
                            WHERE rpb.DW_IsDeleted = 0
                              AND rpb.patient_id IN (SELECT patient_id FROM {changed_patients_table})

                            UNION ALL

                            SELECT
                                rpb.patient_id,
                                rpb.cpr, rpb.x_ecpr, rpb.d_ecpr,
                                CAST('1900-01-01' AS datetime2(0)),
                                NULL,
                                NULL
                            FROM [ref].[Patient_Borger] rpb
                            WHERE rpb.DW_IsDeleted = 0
                              AND rpb.patient_id IN (SELECT patient_id FROM {changed_patients_table})
                              AND NOT EXISTS (
                                  SELECT 1 FROM [fhir].[Patient_Address] pa
                                  WHERE pa.patient_id = rpb.patient_id
                              )
                        ),
                        Deduped AS (
                            SELECT *,
                                ROW_NUMBER() OVER (
                                    PARTITION BY patient_id, address_period_start
                                    ORDER BY last_updated DESC, address_period_end DESC
                                ) AS rn
                            FROM SourceWithDefaults
                        ),
                        WithPeriods AS (
                            SELECT
                                patient_id, cpr, x_ecpr, d_ecpr,
                                address_period_start, address_period_end,
                                CASE
                                    WHEN address_period_start =
                                         MIN(address_period_start) OVER (PARTITION BY patient_id)
                                        THEN CAST('1900-01-01' AS datetime2(0))
                                    ELSE address_period_start
                                END AS DW_ValidFrom,
                                COALESCE(
                                    LEAD(address_period_start) OVER (
                                        PARTITION BY patient_id
                                        ORDER BY address_period_start
                                    ),
                                    CAST('9999-12-31 23:59:59' AS datetime2(0))
                                ) AS DW_ValidTo
                            FROM Deduped
                            WHERE rn = 1
                        )
                        SELECT
                            patient_id, cpr, x_ecpr, d_ecpr,
                            address_period_start, address_period_end,
                            DW_ValidFrom, DW_ValidTo,
                            CASE
                                WHEN DW_ValidTo = CAST('9999-12-31 23:59:59' AS datetime2(0))
                                    THEN 1 ELSE 0
                            END AS DW_IsCurrent
                        INTO {staged_rows_table}
                        FROM WithPeriods;
                        """
                    )
                    cursor.execute(f"SELECT COUNT(*) FROM {staged_rows_table}")
                    staged_count = cursor.fetchone()[0]
                    context.log.info(
                        f"Staged {staged_count} rebuilt period rows into {staged_rows_table}"
                    )

                    cursor.execute(
                        f"""
                        CREATE TABLE {merge_out_table} (
                            action_type NVARCHAR(10) NOT NULL,
                            old_is_deleted BIT NULL,
                            new_is_deleted BIT NULL
                        );

                        MERGE INTO [ref].[Patient_Address_Borger] AS tgt
                        USING {staged_rows_table} AS src
                        ON tgt.patient_id = src.patient_id
                           AND tgt.address_period_start = src.address_period_start

                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (
                                patient_id, cpr, x_ecpr, d_ecpr,
                                address_period_start, address_period_end,
                                DW_ValidFrom, DW_ValidTo, DW_IsCurrent, DW_IsDeleted,
                                DW_Timestamp_Insert
                            )
                            VALUES (
                                src.patient_id, src.cpr, src.x_ecpr, src.d_ecpr,
                                src.address_period_start, src.address_period_end,
                                src.DW_ValidFrom, src.DW_ValidTo, src.DW_IsCurrent, 0,
                                CAST(SYSDATETIME() AS datetime2(0))
                            )

                        WHEN MATCHED AND (
                            tgt.DW_ValidTo <> src.DW_ValidTo
                            OR ISNULL(tgt.address_period_end, '1900-01-01')
                                <> ISNULL(src.address_period_end, '1900-01-01')
                            OR ISNULL(tgt.cpr, '') <> ISNULL(src.cpr, '')
                            OR ISNULL(tgt.x_ecpr, '') <> ISNULL(src.x_ecpr, '')
                            OR ISNULL(tgt.d_ecpr, '') <> ISNULL(src.d_ecpr, '')
                            OR tgt.DW_ValidFrom <> src.DW_ValidFrom
                            OR tgt.DW_IsCurrent <> src.DW_IsCurrent
                            OR tgt.DW_IsDeleted = 1
                        ) THEN
                            UPDATE SET
                                cpr                 = src.cpr,
                                x_ecpr              = src.x_ecpr,
                                d_ecpr              = src.d_ecpr,
                                address_period_end  = src.address_period_end,
                                DW_ValidFrom        = src.DW_ValidFrom,
                                DW_ValidTo          = src.DW_ValidTo,
                                DW_IsCurrent        = src.DW_IsCurrent,
                                DW_IsDeleted        = 0,
                                DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

                        WHEN NOT MATCHED BY SOURCE
                            AND tgt.patient_id IN (SELECT patient_id FROM {changed_patients_table})
                            AND tgt.DW_IsDeleted = 0
                        THEN
                            UPDATE SET
                                DW_IsDeleted        = 1,
                                DW_IsCurrent        = 0,
                                DW_ValidTo          = CAST(SYSDATETIME() AS datetime2(0)),
                                DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

                        OUTPUT $action, deleted.DW_IsDeleted, inserted.DW_IsDeleted
                        INTO {merge_out_table} (action_type, old_is_deleted, new_is_deleted);
                        """
                    )
                    merge_affected = cursor.rowcount

                    cursor.execute(
                        f"""
                        SELECT COUNT(*) FROM {merge_out_table}
                        WHERE action_type = 'UPDATE'
                          AND ISNULL(old_is_deleted, 0) = 0
                          AND new_is_deleted = 1
                        """
                    )
                    soft_deleted = cursor.fetchone()[0]

                    check_soft_delete_threshold("ref.Patient_Address_Borger", soft_deleted, context)

                    cursor.execute(
                        """
                        SELECT CONVERT(NVARCHAR(50), MAX(pa.last_updated), 121)
                        FROM [fhir].[Patient_Address] pa
                        WHERE pa.last_updated > ?
                        """,
                        [cp_param],
                    )
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row and row[0] else checkpoint

                    conn.commit()

                    if config.should_advance_checkpoint and new_checkpoint is not None:
                        dfm.set_checkpoint(
                            "ref",
                            "Patient_Address_Borger",
                            value=new_checkpoint,
                            mode=config.mode,
                        )

                    cursor.execute("SELECT COUNT(*) FROM [ref].[Patient_Address_Borger]")
                    total = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM [ref].[Patient_Address_Borger] "
                        "WHERE DW_IsCurrent = 1 AND DW_IsDeleted = 0"
                    )
                    current = cursor.fetchone()[0]

                    context.log.info(
                        f"ref.Patient_Address_Borger: {merge_affected} rows affected by MERGE, "
                        f"{soft_deleted} soft-deleted, {staged_count} staged, "
                        f"{current} current, {total} total. "
                        f"Checkpoint: {checkpoint} → {new_checkpoint}"
                    )

                    dfm.complete_dfm_run(
                        run_ctx.log_id,
                        checkpoint_after=new_checkpoint,
                        rows_affected=merge_affected,
                    )
                finally:
                    for table_name in (
                        merge_out_table,
                        staged_rows_table,
                        changed_patients_table,
                    ):
                        try:
                            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                        except Exception:
                            pass
            finally:
                cursor.close()
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise

    return dg.MaterializeResult(
        metadata={
            "changed_patients": changed_count,
            "staged_rows": staged_count,
            "merge_affected": merge_affected,
            "soft_deleted_this_run": soft_deleted,
            "threshold": get_soft_delete_threshold("ref.Patient_Address_Borger"),
            "current_versions": current,
            "total_versions": total,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "table": "ref.Patient_Address_Borger",
            "mode": config.mode,
            "non_overlap_method": "LEAD(address_period_start), half-open",
        }
    )
