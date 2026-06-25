# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
dim.Borger — Type 2 SCD built from FHIR + ref tables.

Architecture:

  fhir.Patient (Type 1)          --+
  fhir.Patient_Address (T2)      --+--->  ref.Patient_Borger          --->  dim.Borger
                                   |     ref.Patient_Address_Borger  --->
  identifier_systems.py (URIs)   --+

DW_EK_Borger and DW_SK_Borger come from the ref tables — this asset just
joins them to fhir.* projections and produces the wide dim row.

MERGE pattern keyed on DW_SK_Borger (carried from ref.Patient_Address_Borger,
no local IDENTITY). Sentinel rows (-1 = UNKNOWN, -2 = N/A) are excluded from
the NOT MATCHED BY SOURCE branch via DW_SK_Borger > 0.

Delta strategy: patient-scoped via DFM checkpoint on ref.Patient_Borger and
ref.Patient_Address_Borger DW_Timestamp_Update/Insert. Only patients whose
ref rows changed since last run are re-merged; soft-delete is scoped to the
same patient set.
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
    group_name="shared_dimensions",
    compute_kind="mssql",
    deps=[
        "sof_patient_dfm",
        "ref_patient_borger",
        "ref_patient_address_borger",
    ],
    tags={"layer": "dimensions", "module": "shared_dimensions", "scd_type": "type2"},
    description=(
        "dim.Borger — Type 2 SCD joining fhir.Patient (Type 1 attributes) "
        "with ref.Patient_Address_Borger (T2 versioning) and fhir.Patient_Address "
        "(period-specific address detail). Patient-scoped delta MERGE keyed on "
        "DW_SK_Borger with DFM checkpoint on ref table timestamps."
    ),
)
def dim_borger(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """MERGE dim.Borger from fhir.Patient + ref tables with patient-scoped delta."""

    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "dim",
        "Borger",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    checkpoint = run_ctx.checkpoint

    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cp_param = checkpoint or "1900-01-01"
                changed_patients_table = _global_temp_table(
                    "changed_patients_dim_borger", context.run_id
                )
                merge_out_table = _global_temp_table("merge_out_dim_borger", context.run_id)

                context.log.info(
                    f"Patient-scoped MERGE for dim.Borger (checkpoint: {checkpoint})..."
                )

                for table_name in (merge_out_table, changed_patients_table):
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

                try:
                    cursor.execute(
                        f"""
                        SELECT DISTINCT patient_id
                        INTO {changed_patients_table}
                        FROM (
                            SELECT patient_id FROM [ref].[Patient_Borger]
                            WHERE DW_Timestamp_Update > ?
                               OR DW_Timestamp_Insert > ?
                            UNION
                            SELECT patient_id FROM [ref].[Patient_Address_Borger]
                            WHERE DW_Timestamp_Update > ?
                               OR DW_Timestamp_Insert > ?
                        ) delta
                        """,
                        [cp_param, cp_param, cp_param, cp_param],
                    )
                    cursor.execute(f"SELECT COUNT(*) FROM {changed_patients_table}")
                    changed_count = cursor.fetchone()[0]
                    context.log.info(f"Found {changed_count} changed patients since {checkpoint}")

                    if changed_count == 0:
                        conn.commit()
                        cursor.execute("SELECT COUNT(*) FROM [dim].[Borger]")
                        total = cursor.fetchone()[0]
                        cursor.execute(
                            "SELECT COUNT(*) FROM [dim].[Borger] "
                            "WHERE DW_IsCurrent = 1 AND DW_SK_Borger > 0"
                        )
                        current = cursor.fetchone()[0]
                        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
                        return dg.MaterializeResult(
                            metadata={
                                "changed_patients": 0,
                                "merge_affected": 0,
                                "soft_deleted_this_run": 0,
                                "threshold": get_soft_delete_threshold("dim.Borger"),
                                "current_versions": current,
                                "total_versions": total,
                                "checkpoint_before": checkpoint,
                                "checkpoint_after": checkpoint,
                                "table": "dim.Borger",
                                "scd_type": "Type 2",
                                "delta_mode": "patient-scoped",
                                "mode": config.mode,
                            }
                        )

                    cursor.execute(
                        f"""
                        CREATE TABLE {merge_out_table} (
                            action_type NVARCHAR(10) NOT NULL,
                            old_is_deleted BIT NULL,
                            new_is_deleted BIT NULL
                        );

                        WITH CurrentAddress AS (
                            SELECT
                                rpab.patient_id,
                                pa.address_line,
                                pa.address_postal_code,
                                pa.address_city,
                                pa.municipality_code,
                                pa.region_code
                            FROM [ref].[Patient_Address_Borger] rpab
                            LEFT JOIN [fhir].[Patient_Address] pa
                                ON pa.patient_id = rpab.patient_id
                               AND TRY_CAST(pa.address_period_start AS datetime2(0))
                                   = rpab.address_period_start
                            WHERE rpab.DW_IsCurrent = 1
                              AND rpab.DW_IsDeleted = 0
                              AND rpab.patient_id IN (SELECT patient_id FROM {changed_patients_table})
                        ),
                        SourceRows AS (
                            SELECT
                                rpab.DW_SK_Borger,
                                rpb.DW_EK_Borger,
                                p.patient_ssn        AS CPRnummer,
                                p.previous_ssn       AS TidligereCPR,
                                p.x_ecpr             AS X_eCPR_Nummer,
                                p.d_ecpr             AS D_eCPR_Nummer,
                                TRY_CAST(p.ifdb_patient_id AS BIGINT) AS PATIENT_ID,
                                p.name_given         AS Fornavne,
                                p.name_family        AS Efternavn,
                                p.name_text          AS Adresseringsnavn,
                                p.birth_date         AS Foedselsdato,
                                p.gender             AS KoenNavn,
                                p.active             AS Active,
                                p.deceased           AS Deceased,
                                p.mother_ssn         AS MorCPRNummer,
                                p.father_ssn         AS FarCPRNummer,
                                p.marital_status_code    AS CivilStatusKode,
                                p.marital_status_display AS CivilStatus,
                                p.citizenship_code    AS StatsborgerskabKode,
                                p.citizenship_display AS Statsborgerskab,
                                rpab.address_period_start AS DatoTid_Tilflytning,
                                rpab.address_period_end   AS DatoTid_Fraflytning,
                                pa.address_line           AS Gade,
                                pa.address_postal_code    AS Postnr,
                                pa.address_city           AS Postdistrikt,
                                pa.municipality_code      AS KommuneID,
                                CAST(NULL AS NVARCHAR(255)) AS KommuneNavn,
                                pa.region_code            AS RegionID,
                                CAST(NULL AS NVARCHAR(255)) AS RegionNavn,
                                pa.address_country        AS Land,
                                pa.kvhx                   AS kvhx,
                                ca.address_line           AS Gade_Nuv,
                                ca.address_postal_code    AS Postnr_Nuv,
                                ca.address_city           AS Postdistrikt_Nuv,
                                ca.municipality_code      AS KommuneID_Nuv,
                                CAST(NULL AS NVARCHAR(255)) AS KommuneNavn_Nuv,
                                ca.region_code            AS RegionID_Nuv,
                                CAST(NULL AS NVARCHAR(255)) AS RegionNavn_Nuv,
                                p.gp_display              AS LaegeNavn,
                                p.gp_identifier           AS LaegeNummer,
                                p.district_code           AS Person_Distriktkode,
                                p.district_text           AS Person_Distriktkodetekst,
                                p.social_district_code    AS Person_Socialdistriktkode,
                                p.social_district_text    AS Person_Socialdistriktkodetekst,
                                p.address_protection      AS Adressebeskyttelse,
                                p.discretion_flag         AS OnskerDiskretion,
                                p.medcom_test_person      AS Medcom_Testperson,
                                CAST(NULL AS DATETIME2)   AS OpdateretFraCPR,
                                rpab.DW_ValidFrom,
                                rpab.DW_ValidTo,
                                rpab.DW_IsCurrent,
                                ISNULL(p.is_deleted, 0)   AS DW_IsDeleted
                            FROM [ref].[Patient_Address_Borger] rpab
                            INNER JOIN [ref].[Patient_Borger] rpb
                                ON rpb.patient_id = rpab.patient_id
                               AND rpb.DW_IsDeleted = 0
                            INNER JOIN [fhir].[Patient] p
                                ON p.patient_id = rpab.patient_id
                            LEFT JOIN [fhir].[Patient_Address] pa
                                ON pa.patient_id = rpab.patient_id
                               AND TRY_CAST(pa.address_period_start AS datetime2(0))
                                   = rpab.address_period_start
                            LEFT JOIN CurrentAddress ca
                                ON ca.patient_id = rpab.patient_id
                            WHERE rpab.DW_IsDeleted = 0
                              AND rpab.patient_id IN (
                                  SELECT patient_id FROM {changed_patients_table}
                              )
                        )
                        MERGE INTO [dim].[Borger] AS tgt
                        USING SourceRows AS src
                        ON tgt.DW_SK_Borger = src.DW_SK_Borger

                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (
                                DW_SK_Borger, DW_EK_Borger,
                                CPRnummer, TidligereCPR, X_eCPR_Nummer, D_eCPR_Nummer, PATIENT_ID,
                                Fornavne, Efternavn, Adresseringsnavn,
                                Foedselsdato, KoenNavn, Active, Deceased,
                                MorCPRNummer, FarCPRNummer,
                                CivilStatusKode, CivilStatus,
                                StatsborgerskabKode, Statsborgerskab,
                                DatoTid_Tilflytning, DatoTid_Fraflytning,
                                Gade, Postnr, Postdistrikt,
                                KommuneID, KommuneNavn, RegionID, RegionNavn,
                                Land, kvhx,
                                Gade_Nuv, Postnr_Nuv, Postdistrikt_Nuv,
                                KommuneID_Nuv, KommuneNavn_Nuv, RegionID_Nuv, RegionNavn_Nuv,
                                LaegeNavn, LaegeNummer,
                                Person_Distriktkode, Person_Distriktkodetekst,
                                Person_Socialdistriktkode, Person_Socialdistriktkodetekst,
                                Adressebeskyttelse, OnskerDiskretion, Medcom_Testperson,
                                OpdateretFraCPR,
                                DW_ValidFrom, DW_ValidTo, DW_IsCurrent, DW_IsDeleted,
                                DW_Timestamp_Insert
                            )
                            VALUES (
                                src.DW_SK_Borger, src.DW_EK_Borger,
                                src.CPRnummer, src.TidligereCPR,
                                src.X_eCPR_Nummer, src.D_eCPR_Nummer, src.PATIENT_ID,
                                src.Fornavne, src.Efternavn, src.Adresseringsnavn,
                                src.Foedselsdato, src.KoenNavn, src.Active, src.Deceased,
                                src.MorCPRNummer, src.FarCPRNummer,
                                src.CivilStatusKode, src.CivilStatus,
                                src.StatsborgerskabKode, src.Statsborgerskab,
                                src.DatoTid_Tilflytning, src.DatoTid_Fraflytning,
                                src.Gade, src.Postnr, src.Postdistrikt,
                                src.KommuneID, src.KommuneNavn, src.RegionID, src.RegionNavn,
                                src.Land, src.kvhx,
                                src.Gade_Nuv, src.Postnr_Nuv, src.Postdistrikt_Nuv,
                                src.KommuneID_Nuv, src.KommuneNavn_Nuv,
                                src.RegionID_Nuv, src.RegionNavn_Nuv,
                                src.LaegeNavn, src.LaegeNummer,
                                src.Person_Distriktkode, src.Person_Distriktkodetekst,
                                src.Person_Socialdistriktkode, src.Person_Socialdistriktkodetekst,
                                src.Adressebeskyttelse, src.OnskerDiskretion, src.Medcom_Testperson,
                                src.OpdateretFraCPR,
                                src.DW_ValidFrom, src.DW_ValidTo, src.DW_IsCurrent, src.DW_IsDeleted,
                                CAST(SYSDATETIME() AS datetime2(0))
                            )

                        WHEN MATCHED THEN
                            UPDATE SET
                                DW_EK_Borger = src.DW_EK_Borger,
                                CPRnummer = src.CPRnummer,
                                TidligereCPR = src.TidligereCPR,
                                X_eCPR_Nummer = src.X_eCPR_Nummer,
                                D_eCPR_Nummer = src.D_eCPR_Nummer,
                                PATIENT_ID = src.PATIENT_ID,
                                Fornavne = src.Fornavne,
                                Efternavn = src.Efternavn,
                                Adresseringsnavn = src.Adresseringsnavn,
                                Foedselsdato = src.Foedselsdato,
                                KoenNavn = src.KoenNavn,
                                Active = src.Active,
                                Deceased = src.Deceased,
                                MorCPRNummer = src.MorCPRNummer,
                                FarCPRNummer = src.FarCPRNummer,
                                CivilStatusKode = src.CivilStatusKode,
                                CivilStatus = src.CivilStatus,
                                StatsborgerskabKode = src.StatsborgerskabKode,
                                Statsborgerskab = src.Statsborgerskab,
                                DatoTid_Tilflytning = src.DatoTid_Tilflytning,
                                DatoTid_Fraflytning = src.DatoTid_Fraflytning,
                                Gade = src.Gade,
                                Postnr = src.Postnr,
                                Postdistrikt = src.Postdistrikt,
                                KommuneID = src.KommuneID,
                                KommuneNavn = src.KommuneNavn,
                                RegionID = src.RegionID,
                                RegionNavn = src.RegionNavn,
                                Land = src.Land,
                                kvhx = src.kvhx,
                                Gade_Nuv = src.Gade_Nuv,
                                Postnr_Nuv = src.Postnr_Nuv,
                                Postdistrikt_Nuv = src.Postdistrikt_Nuv,
                                KommuneID_Nuv = src.KommuneID_Nuv,
                                KommuneNavn_Nuv = src.KommuneNavn_Nuv,
                                RegionID_Nuv = src.RegionID_Nuv,
                                RegionNavn_Nuv = src.RegionNavn_Nuv,
                                LaegeNavn = src.LaegeNavn,
                                LaegeNummer = src.LaegeNummer,
                                Person_Distriktkode = src.Person_Distriktkode,
                                Person_Distriktkodetekst = src.Person_Distriktkodetekst,
                                Person_Socialdistriktkode = src.Person_Socialdistriktkode,
                                Person_Socialdistriktkodetekst = src.Person_Socialdistriktkodetekst,
                                Adressebeskyttelse = src.Adressebeskyttelse,
                                OnskerDiskretion = src.OnskerDiskretion,
                                Medcom_Testperson = src.Medcom_Testperson,
                                OpdateretFraCPR = src.OpdateretFraCPR,
                                DW_ValidFrom = src.DW_ValidFrom,
                                DW_ValidTo = src.DW_ValidTo,
                                DW_IsCurrent = src.DW_IsCurrent,
                                DW_IsDeleted = src.DW_IsDeleted,
                                DW_Timestamp_Update = CAST(SYSDATETIME() AS datetime2(0))

                        WHEN NOT MATCHED BY SOURCE
                            AND tgt.DW_SK_Borger > 0
                            AND tgt.DW_IsDeleted = 0
                            AND tgt.DW_EK_Borger IN (
                                SELECT rpb.DW_EK_Borger
                                FROM [ref].[Patient_Borger] rpb
                                WHERE rpb.patient_id IN (
                                    SELECT patient_id FROM {changed_patients_table}
                                )
                            )
                        THEN
                            UPDATE SET
                                DW_IsDeleted = 1,
                                DW_IsCurrent = 0,
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

                    check_soft_delete_threshold("dim.Borger", soft_deleted, context)

                    new_checkpoint = checkpoint
                    cursor.execute(
                        """
                        SELECT CONVERT(NVARCHAR(50), MAX(ts), 121)
                        FROM (
                            SELECT MAX(COALESCE(DW_Timestamp_Update, DW_Timestamp_Insert))
                                AS ts
                            FROM [ref].[Patient_Borger]
                            UNION ALL
                            SELECT MAX(COALESCE(DW_Timestamp_Update, DW_Timestamp_Insert))
                            FROM [ref].[Patient_Address_Borger]
                        ) combined
                        """
                    )
                    row = cursor.fetchone()
                    if row and row[0]:
                        new_checkpoint = row[0]

                    conn.commit()

                    if config.should_advance_checkpoint and new_checkpoint is not None:
                        dfm.set_checkpoint("dim", "Borger", value=new_checkpoint, mode=config.mode)

                    cursor.execute("SELECT COUNT(*) FROM [dim].[Borger]")
                    total = cursor.fetchone()[0]
                    cursor.execute(
                        "SELECT COUNT(*) FROM [dim].[Borger] "
                        "WHERE DW_IsCurrent = 1 AND DW_SK_Borger > 0"
                    )
                    current = cursor.fetchone()[0]

                    context.log.info(
                        f"dim.Borger: {merge_affected} rows affected by MERGE, "
                        f"{soft_deleted} soft-deleted, {changed_count} changed patients, "
                        f"{current} current, {total} total. "
                        f"Checkpoint: {checkpoint} → {new_checkpoint}"
                    )

                    dfm.complete_dfm_run(
                        run_ctx.log_id,
                        checkpoint_after=new_checkpoint,
                        rows_affected=merge_affected,
                    )
                finally:
                    for table_name in (merge_out_table, changed_patients_table):
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
            "merge_affected": merge_affected,
            "soft_deleted_this_run": soft_deleted,
            "threshold": get_soft_delete_threshold("dim.Borger"),
            "current_versions": current,
            "total_versions": total,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "table": "dim.Borger",
            "scd_type": "Type 2",
            "delta_mode": "patient-scoped",
            "mode": config.mode,
            "source": (
                "fhir.Patient + ref.Patient_Borger "
                "+ ref.Patient_Address_Borger + fhir.Patient_Address"
            ),
        }
    )
