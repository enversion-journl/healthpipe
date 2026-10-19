# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
dim.Borger — Type 2 SCD built from FHIR + ref tables.

Architecture:

  fhir.Patient (Type 1)          --+
  fhir.Patient_Address (T2)      --+--->  ref.PatientToBorger          --->  dim.Borger
                                   |     ref.Patient_Address_To_Borger  --->
  identifier_systems.py (URIs)   --+     ref.KommuneToPopulationsansvar --->

dw_ek_Borger and dw_sk_Borger come from the ref tables — this asset just
joins them to fhir.* projections and produces the wide dim row.

MERGE pattern keyed on dw_sk_Borger (carried from ref.Patient_Address_To_Borger,
no local IDENTITY). Sentinel rows (-1 = UNKNOWN, -2 = N/A, -99 = MASKERET) are
excluded from the NOT MATCHED BY SOURCE branch via dw_sk_Borger > 0.

Delta strategy: patient-scoped via DFM checkpoint on ref.PatientToBorger and
ref.Patient_Address_To_Borger dw_timestamp_update/Insert. Only patients whose
ref rows changed since last run are re-merged; soft-delete is scoped to the
same patient set.

populationsansvar is not part of the delta: every run sets it on all versions
from fhir.Patient.municipality_code and ref.KommuneToPopulationsansvar, so a
changed mapping reaches the dimension without any patient changing.

A citizen with name and address protection has the standard texts as name and
address on the FHIR Patient. The versions retired when the protection arrives
still hold the earlier name and addresses; they get the same texts in the same
run.
"""

import time

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.etl_thresholds import check_soft_delete_threshold, get_soft_delete_threshold
from core.resources import MSSQLResource
from utils.fhir.protection import ADDRESS_PROTECTED_TEXT, NAME_PROTECTED_TEXT


def _global_temp_table(base: str, run_id: str) -> str:
    """Build a session-unique global temp table name."""
    safe_run_id = "".join(ch for ch in run_id if ch.isalnum())[:20]
    return f"##{base}_{safe_run_id}"


def _timed(context: dg.AssetExecutionContext, timings: dict[str, float], label: str):
    """Context manager: time a statement and log its elapsed seconds."""
    from contextlib import contextmanager

    @contextmanager
    def _cm():
        t0 = time.perf_counter()
        yield
        elapsed = time.perf_counter() - t0
        timings[label] = round(elapsed, 1)
        context.log.info(f"[DIM_BORGER_TIMING] {label} elapsed={elapsed:.1f}s")

    return _cm()


def _refresh_populationsansvar(cursor) -> int:
    """Set populationsansvar on every version from the current municipality; returns rows changed."""
    cursor.execute(
        """
        UPDATE b
        SET b.populationsansvar = r.populationsansvar,
            b.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
        FROM [dim].[Borger] AS b
        LEFT JOIN [fhir].[Patient] AS p
            ON p.patient_id = b.fhir_Patient_id
        LEFT JOIN [ref].[KommuneToPopulationsansvar] AS r
            ON r.kommunekode = p.municipality_code
        WHERE b.dw_sk_Borger > 0
          AND ISNULL(b.populationsansvar, N'') <> ISNULL(r.populationsansvar, N'')
        """
    )
    return cursor.rowcount


def _mask_retired_versions_of_protected(cursor, src_rows_table: str) -> int:
    """Give retired versions of protected citizens the standard texts; returns rows changed."""
    cursor.execute(
        f"""
        UPDATE tgt
        SET fornavne = NULL, efternavn = txt.name_text, adresseringsnavn = txt.name_text,
            gade = txt.address_text, postnr = NULL, postdistrikt = NULL,
            kommune_id = NULL, kommunenavn = NULL, region_id = NULL, regionnavn = NULL,
            land = NULL, kvhx = NULL,
            gade_nuv = txt.address_text, postnr_nuv = NULL, postdistrikt_nuv = NULL,
            kommune_id_nuv = NULL, kommunenavn_nuv = NULL,
            region_id_nuv = NULL, regionnavn_nuv = NULL,
            dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
        FROM [dim].[Borger] AS tgt
        CROSS JOIN (
            SELECT CAST(? AS NVARCHAR(50)) AS name_text, CAST(? AS NVARCHAR(50)) AS address_text
        ) AS txt
        WHERE tgt.dw_sk_Borger > 0
          AND tgt.dw_is_deleted = 1
          AND tgt.dw_ek_Borger IN (
              SELECT dw_ek_Borger FROM {src_rows_table} WHERE adressebeskyttelse = 1
          )
          AND (
              ISNULL(tgt.efternavn, N'') <> txt.name_text
              OR ISNULL(tgt.adresseringsnavn, N'') <> txt.name_text
              OR ISNULL(tgt.gade, N'') <> txt.address_text
              OR ISNULL(tgt.gade_nuv, N'') <> txt.address_text
              OR COALESCE(
                     tgt.fornavne, tgt.postnr, tgt.postdistrikt, tgt.kommune_id, tgt.region_id,
                     tgt.land, tgt.kvhx, tgt.postnr_nuv, tgt.postdistrikt_nuv,
                     tgt.kommune_id_nuv, tgt.region_id_nuv
                 ) IS NOT NULL
          )
        """,
        [NAME_PROTECTED_TEXT, ADDRESS_PROTECTED_TEXT],
    )
    return cursor.rowcount


@dg.asset(
    group_name="shared_dimensions",
    compute_kind="mssql",
    deps=[
        "sof_patient_dfm",
        "ref_patient_borger",
        "ref_patient_address_borger",
        "ref_KommuneToPopulationsansvar",
    ],
    tags={"layer": "dimensions", "module": "shared_dimensions", "scd_type": "type2"},
    description=(
        "dim.Borger — Type 2 SCD joining fhir.Patient (Type 1 attributes) "
        "with ref.Patient_Address_To_Borger (T2 versioning) and fhir.Patient_Address "
        "(period-specific address detail). Patient-scoped delta MERGE keyed on "
        "dw_sk_Borger with DFM checkpoint on ref table timestamps. populationsansvar "
        "is set on all versions from ref.KommuneToPopulationsansvar in every run."
    ),
)
def dim_Borger(
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
                # Optional re-run window: delta semantics unchanged, but the
                # effective checkpoint is moved back N days (benchmark /
                # catch-up lever; combine with advance_checkpoint=False for a
                # side-effect-free re-run of a past window).
                lookback = int(config.lookback_days or 0)
                cp_expr = f"DATEADD(day, -{lookback}, CAST(? AS DATETIME2))" if lookback else "?"
                timings: dict[str, float] = {}
                changed_patients_table = _global_temp_table(
                    "changed_patients_dim_borger", context.run_id
                )
                merge_out_table = _global_temp_table("merge_out_dim_borger", context.run_id)
                changed_ek_table = _global_temp_table("changed_ek_dim_borger", context.run_id)
                src_rows_table = _global_temp_table("src_rows_dim_borger", context.run_id)
                all_temp_tables = (
                    merge_out_table,
                    changed_patients_table,
                    changed_ek_table,
                    src_rows_table,
                )

                context.log.info(
                    f"Patient-scoped MERGE for dim.Borger (checkpoint: {checkpoint})..."
                )

                for table_name in all_temp_tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

                try:
                    with _timed(context, timings, "changed_patients_select"):
                        cursor.execute(
                            f"""
                            SELECT DISTINCT patient_id
                            INTO {changed_patients_table}
                            FROM (
                                SELECT patient_id FROM [ref].[PatientToBorger]
                                WHERE dw_timestamp_update > {cp_expr}
                                   OR dw_timestamp_insert > {cp_expr}
                                UNION
                                SELECT patient_id FROM [ref].[Patient_Address_To_Borger]
                                WHERE dw_timestamp_update > {cp_expr}
                                   OR dw_timestamp_insert > {cp_expr}
                            ) delta
                            """,
                            [cp_param, cp_param, cp_param, cp_param],
                        )
                    cursor.execute(
                        f"CREATE CLUSTERED INDEX [CIX_changed_patients] "
                        f"ON {changed_patients_table} (patient_id)"
                    )
                    cursor.execute(f"SELECT COUNT(*) FROM {changed_patients_table}")
                    changed_count = cursor.fetchone()[0]
                    context.log.info(f"Found {changed_count} changed patients since {checkpoint}")

                    if changed_count == 0:
                        with _timed(context, timings, "populationsansvar_refresh"):
                            populationsansvar_updated = _refresh_populationsansvar(cursor)
                        conn.commit()
                        cursor.execute("SELECT COUNT(*) FROM [dim].[Borger]")
                        total = cursor.fetchone()[0]
                        cursor.execute(
                            "SELECT COUNT(*) FROM [dim].[Borger] "
                            "WHERE dw_is_current = 1 AND dw_sk_Borger > 0"
                        )
                        current = cursor.fetchone()[0]
                        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
                        return dg.MaterializeResult(
                            metadata={
                                "changed_patients": 0,
                                "merge_affected": 0,
                                "soft_deleted_this_run": 0,
                                "populationsansvar_updated": populationsansvar_updated,
                                "threshold": get_soft_delete_threshold(
                                    "dim.Borger", config.soft_delete_threshold
                                ),
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

                    # Entity keys of the changed patients — drives both the
                    # current-clearing UPDATE and the MERGE target scoping.
                    # Same set the previous nested-IN chain produced.
                    with _timed(context, timings, "changed_ek_select"):
                        cursor.execute(
                            f"""
                            SELECT DISTINCT rpb.dw_ek_Borger
                            INTO {changed_ek_table}
                            FROM [ref].[PatientToBorger] rpb
                            INNER JOIN {changed_patients_table} cp
                                ON cp.patient_id = rpb.patient_id
                            """
                        )
                        cursor.execute(
                            f"CREATE CLUSTERED INDEX [CIX_changed_ek] "
                            f"ON {changed_ek_table} (dw_ek_Borger)"
                        )

                    # Unique index UX_Dim_Borger_CurrentEntity is checked per-row
                    # during MERGE; clear outgoing currents first (not sentinels).
                    with _timed(context, timings, "clear_outgoing_currents"):
                        cursor.execute(
                            f"""
                        UPDATE tgt
                        SET tgt.dw_is_current = 0,
                            tgt.dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
                        FROM [dim].[Borger] AS tgt
                        WHERE tgt.dw_is_current = 1
                          AND tgt.dw_sk_Borger > 0
                          AND tgt.dw_ek_Borger IN (
                              SELECT dw_ek_Borger FROM {changed_ek_table}
                          )
                        """
                        )

                    # Materialize the source once (indexed on the merge key):
                    # both the MERGE USING and the target scoping read it.
                    with _timed(context, timings, "source_rows_select"):
                        cursor.execute(
                            f"""
                        WITH CurrentAddress AS (
                            SELECT
                                rpab.patient_id,
                                pa.address_line,
                                pa.address_postal_code,
                                pa.address_city,
                                pa.municipality_code,
                                pa.region_code
                            FROM [ref].[Patient_Address_To_Borger] rpab
                            LEFT JOIN [fhir].[Patient_Address] pa
                                ON pa.patient_id = rpab.patient_id
                               AND pa.address_period_start = rpab.address_period_start
                               AND pa.dw_is_deleted = 0
                            WHERE rpab.dw_is_current = 1
                              AND rpab.dw_is_deleted = 0
                              AND rpab.patient_id IN (SELECT patient_id FROM {changed_patients_table})
                        ),
                        SourceRows AS (
                            SELECT
                                rpab.dw_sk_Borger,
                                rpb.dw_ek_Borger,
                                p.patient_id         AS fhir_Patient_id,
                                p.patient_ssn        AS cpr_nummer,
                                p.previous_ssn       AS tidligere_cpr,
                                p.x_ecpr             AS x_ecpr_nummer,
                                p.d_ecpr             AS d_ecpr_nummer,
                                TRY_CAST(p.ifdb_patient_id AS BIGINT) AS patient_id_ifdb,
                                p.name_given         AS fornavne,
                                p.name_family        AS efternavn,
                                p.name_text          AS adresseringsnavn,
                                p.birth_date         AS foedselsdato,
                                p.gender             AS koen_navn,
                                p.active             AS Active,
                                p.deceased           AS deceased,
                                p.mother_ssn         AS mor_cpr_nummer,
                                p.father_ssn         AS far_cpr_nummer,
                                p.marital_status_code    AS civil_status_kode,
                                p.marital_status_display AS civil_status,
                                p.citizenship_code    AS statsborgerskab_kode,
                                p.citizenship_display AS statsborgerskab,
                                rpab.address_period_start AS dato_tid_tilflytning,
                                rpab.address_period_end   AS dato_tid_fraflytning,
                                pa.address_line           AS gade,
                                pa.address_postal_code    AS postnr,
                                pa.address_city           AS postdistrikt,
                                pa.municipality_code      AS kommune_id,
                                CAST(NULL AS NVARCHAR(255)) AS kommunenavn,
                                pa.region_code            AS region_id,
                                CAST(NULL AS NVARCHAR(255)) AS regionnavn,
                                pa.address_country        AS land,
                                pa.kvhx                   AS kvhx,
                                ca.address_line           AS gade_nuv,
                                ca.address_postal_code    AS postnr_nuv,
                                ca.address_city           AS postdistrikt_nuv,
                                ca.municipality_code      AS kommune_id_nuv,
                                CAST(NULL AS NVARCHAR(255)) AS kommunenavn_nuv,
                                ca.region_code            AS region_id_nuv,
                                CAST(NULL AS NVARCHAR(255)) AS regionnavn_nuv,
                                p.gp_display              AS laege_navn,
                                p.gp_identifier           AS laege_nummer,
                                p.district_code           AS person_distriktkode,
                                p.district_text           AS person_distriktkodetekst,
                                p.social_district_code    AS person_socialdistriktkode,
                                p.social_district_text    AS person_socialdistriktkodetekst,
                                CAST(COALESCE(p.address_protection, 0) AS BIT) AS adressebeskyttelse,
                                p.discretion_flag         AS onsker_diskretion,
                                p.medcom_test_person      AS medcom_testperson,
                                CAST(p.cpr_updated AS DATETIME2) AS opdateret_fra_cpr,
                                rpab.dw_valid_from,
                                rpab.dw_valid_to,
                                CASE
                                    WHEN ISNULL(p.dw_is_deleted, 0) = 1 THEN CAST(0 AS BIT)
                                    ELSE rpab.dw_is_current
                                END AS dw_is_current,
                                ISNULL(p.dw_is_deleted, 0)   AS dw_is_deleted
                            FROM [ref].[Patient_Address_To_Borger] rpab
                            INNER JOIN [ref].[PatientToBorger] rpb
                                ON rpb.patient_id = rpab.patient_id
                               AND rpb.dw_is_deleted = 0
                            INNER JOIN [fhir].[Patient] p
                                ON p.patient_id = rpab.patient_id
                            LEFT JOIN [fhir].[Patient_Address] pa
                                ON pa.patient_id = rpab.patient_id
                               AND pa.address_period_start = rpab.address_period_start
                               AND pa.dw_is_deleted = 0
                            LEFT JOIN CurrentAddress ca
                                ON ca.patient_id = rpab.patient_id
                            WHERE rpab.dw_is_deleted = 0
                              AND rpab.patient_id IN (
                                  SELECT patient_id FROM {changed_patients_table}
                              )
                        )
                        SELECT * INTO {src_rows_table} FROM SourceRows
                        """
                        )
                        cursor.execute(
                            f"CREATE CLUSTERED INDEX [CIX_src_rows] "
                            f"ON {src_rows_table} (dw_sk_Borger)"
                        )

                    # MERGE against a scoped target: the OR keeps the branch row
                    # sets provably identical to the previous full-table MERGE --
                    # the sk side covers every source row (MATCHED, incl. the
                    # ek-remap edge), the ek side covers exactly the rows the
                    # NOT MATCHED BY SOURCE branch may touch. Without scoping,
                    # NMBS forces a full outer scan of all of dim.Borger.
                    with _timed(context, timings, "merge_dim_borger"):
                        cursor.execute(
                            f"""
                        CREATE TABLE {merge_out_table} (
                            action_type NVARCHAR(10) NOT NULL,
                            old_is_deleted BIT NULL,
                            new_is_deleted BIT NULL,
                            -- src.dw_sk_Borger is NULL only in the NOT MATCHED BY
                            -- SOURCE branch, which is the one that retires a row.
                            -- $action is 'UPDATE' for both branches, so without
                            -- this the two are indistinguishable -- see the count
                            -- below.
                            src_sk INT NULL
                        );

                        WITH ScopedTarget AS (
                            SELECT * FROM [dim].[Borger]
                            WHERE dw_sk_Borger IN (
                                      SELECT dw_sk_Borger FROM {src_rows_table}
                                  )
                               OR (
                                      dw_sk_Borger > 0
                                      AND dw_ek_Borger IN (
                                          SELECT dw_ek_Borger FROM {changed_ek_table}
                                      )
                                  )
                        )
                        MERGE INTO ScopedTarget AS tgt
                        USING {src_rows_table} AS src
                        ON tgt.dw_sk_Borger = src.dw_sk_Borger

                        WHEN NOT MATCHED BY TARGET THEN
                            INSERT (
                                dw_sk_Borger, dw_ek_Borger, fhir_Patient_id,
                                cpr_nummer, tidligere_cpr, x_ecpr_nummer, d_ecpr_nummer, patient_id_ifdb,
                                fornavne, efternavn, adresseringsnavn,
                                foedselsdato, koen_navn, Active, deceased,
                                mor_cpr_nummer, far_cpr_nummer,
                                civil_status_kode, civil_status,
                                statsborgerskab_kode, statsborgerskab,
                                dato_tid_tilflytning, dato_tid_fraflytning,
                                gade, postnr, postdistrikt,
                                kommune_id, kommunenavn, region_id, regionnavn,
                                land, kvhx,
                                gade_nuv, postnr_nuv, postdistrikt_nuv,
                                kommune_id_nuv, kommunenavn_nuv, region_id_nuv, regionnavn_nuv,
                                laege_navn, laege_nummer,
                                person_distriktkode, person_distriktkodetekst,
                                person_socialdistriktkode, person_socialdistriktkodetekst,
                                adressebeskyttelse, onsker_diskretion, medcom_testperson,
                                opdateret_fra_cpr,
                                dw_valid_from, dw_valid_to, dw_is_current, dw_is_deleted,
                                dw_timestamp_insert
                            )
                            VALUES (
                                src.dw_sk_Borger, src.dw_ek_Borger, src.fhir_Patient_id,
                                src.cpr_nummer, src.tidligere_cpr,
                                src.x_ecpr_nummer, src.d_ecpr_nummer, src.patient_id_ifdb,
                                src.fornavne, src.efternavn, src.adresseringsnavn,
                                src.foedselsdato, src.koen_navn, src.active, src.deceased,
                                src.mor_cpr_nummer, src.far_cpr_nummer,
                                src.civil_status_kode, src.civil_status,
                                src.statsborgerskab_kode, src.statsborgerskab,
                                src.dato_tid_tilflytning, src.dato_tid_fraflytning,
                                src.gade, src.postnr, src.postdistrikt,
                                src.kommune_id, src.kommunenavn, src.region_id, src.regionnavn,
                                src.land, src.kvhx,
                                src.gade_nuv, src.postnr_nuv, src.postdistrikt_nuv,
                                src.kommune_id_nuv, src.kommunenavn_nuv,
                                src.region_id_nuv, src.regionnavn_nuv,
                                src.laege_navn, src.laege_nummer,
                                src.person_distriktkode, src.person_distriktkodetekst,
                                src.person_socialdistriktkode, src.person_socialdistriktkodetekst,
                                src.adressebeskyttelse, src.onsker_diskretion, src.medcom_testperson,
                                src.opdateret_fra_cpr,
                                src.dw_valid_from, src.dw_valid_to, src.dw_is_current, src.dw_is_deleted,
                                CAST(SYSDATETIME() AS datetime2(0))
                            )

                        WHEN MATCHED THEN
                            UPDATE SET
                                dw_ek_Borger = src.dw_ek_Borger,
                                fhir_Patient_id = src.fhir_Patient_id,
                                cpr_nummer = src.cpr_nummer,
                                tidligere_cpr = src.tidligere_cpr,
                                x_ecpr_nummer = src.x_ecpr_nummer,
                                d_ecpr_nummer = src.d_ecpr_nummer,
                                patient_id_ifdb = src.patient_id_ifdb,
                                fornavne = src.fornavne,
                                efternavn = src.efternavn,
                                adresseringsnavn = src.adresseringsnavn,
                                foedselsdato = src.foedselsdato,
                                koen_navn = src.koen_navn,
                                Active = src.active,
                                deceased = src.deceased,
                                mor_cpr_nummer = src.mor_cpr_nummer,
                                far_cpr_nummer = src.far_cpr_nummer,
                                civil_status_kode = src.civil_status_kode,
                                civil_status = src.civil_status,
                                statsborgerskab_kode = src.statsborgerskab_kode,
                                statsborgerskab = src.statsborgerskab,
                                dato_tid_tilflytning = src.dato_tid_tilflytning,
                                dato_tid_fraflytning = src.dato_tid_fraflytning,
                                gade = src.gade,
                                postnr = src.postnr,
                                postdistrikt = src.postdistrikt,
                                kommune_id = src.kommune_id,
                                kommunenavn = src.kommunenavn,
                                region_id = src.region_id,
                                regionnavn = src.regionnavn,
                                land = src.land,
                                kvhx = src.kvhx,
                                gade_nuv = src.gade_nuv,
                                postnr_nuv = src.postnr_nuv,
                                postdistrikt_nuv = src.postdistrikt_nuv,
                                kommune_id_nuv = src.kommune_id_nuv,
                                kommunenavn_nuv = src.kommunenavn_nuv,
                                region_id_nuv = src.region_id_nuv,
                                regionnavn_nuv = src.regionnavn_nuv,
                                laege_navn = src.laege_navn,
                                laege_nummer = src.laege_nummer,
                                person_distriktkode = src.person_distriktkode,
                                person_distriktkodetekst = src.person_distriktkodetekst,
                                person_socialdistriktkode = src.person_socialdistriktkode,
                                person_socialdistriktkodetekst = src.person_socialdistriktkodetekst,
                                adressebeskyttelse = src.adressebeskyttelse,
                                onsker_diskretion = src.onsker_diskretion,
                                medcom_testperson = src.medcom_testperson,
                                opdateret_fra_cpr = src.opdateret_fra_cpr,
                                dw_valid_from = src.dw_valid_from,
                                dw_valid_to = src.dw_valid_to,
                                dw_is_current = src.dw_is_current,
                                dw_is_deleted = src.dw_is_deleted,
                                dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                        WHEN NOT MATCHED BY SOURCE
                            AND tgt.dw_sk_Borger > 0
                            AND tgt.dw_is_deleted = 0
                            AND tgt.dw_ek_Borger IN (
                                SELECT dw_ek_Borger FROM {changed_ek_table}
                            )
                        THEN
                            UPDATE SET
                                dw_is_deleted = 1,
                                dw_is_current = 0,
                                dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))

                        OUTPUT $action, deleted.dw_is_deleted, inserted.dw_is_deleted,
                               src.dw_sk_Borger
                        INTO {merge_out_table}
                            (action_type, old_is_deleted, new_is_deleted, src_sk);
                        """
                        )
                    merge_affected = cursor.rowcount

                    # Threshold counts NOT MATCHED BY SOURCE only. MATCHED
                    # copying fhir.Patient.dw_is_deleted is attribute propagation.
                    cursor.execute(
                        f"""
                        SELECT COUNT(*) FROM {merge_out_table}
                        WHERE action_type = 'UPDATE'
                          AND src_sk IS NULL
                          AND ISNULL(old_is_deleted, 0) = 0
                          AND new_is_deleted = 1
                        """
                    )
                    soft_deleted = cursor.fetchone()[0]

                    # MATCHED patient-deletes; not part of the threshold count.
                    cursor.execute(
                        f"""
                        SELECT COUNT(*) FROM {merge_out_table}
                        WHERE action_type = 'UPDATE'
                          AND src_sk IS NOT NULL
                          AND ISNULL(old_is_deleted, 0) = 0
                          AND new_is_deleted = 1
                        """
                    )
                    deleted_from_source = cursor.fetchone()[0]
                    if deleted_from_source:
                        context.log.info(
                            f"dim.Borger: {deleted_from_source} row(s) marked deleted because "
                            f"fhir.Patient says the patient is deleted (attribute propagation, "
                            f"not a retired version — not counted against the soft-delete guard)."
                        )

                    check_soft_delete_threshold(
                        "dim.Borger",
                        soft_deleted,
                        context,
                        override=config.soft_delete_threshold,
                    )

                    with _timed(context, timings, "mask_retired_versions_of_protected"):
                        protected_versions_masked = _mask_retired_versions_of_protected(
                            cursor, src_rows_table
                        )
                    with _timed(context, timings, "populationsansvar_refresh"):
                        populationsansvar_updated = _refresh_populationsansvar(cursor)

                    new_checkpoint = checkpoint
                    with _timed(context, timings, "checkpoint_max_select"):
                        cursor.execute(
                            """
                        SELECT CONVERT(NVARCHAR(50), MAX(ts), 121)
                        FROM (
                            SELECT MAX(COALESCE(dw_timestamp_update, dw_timestamp_insert))
                                AS ts
                            FROM [ref].[PatientToBorger]
                            UNION ALL
                            SELECT MAX(COALESCE(dw_timestamp_update, dw_timestamp_insert))
                            FROM [ref].[Patient_Address_To_Borger]
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
                        "WHERE dw_is_current = 1 AND dw_sk_Borger > 0"
                    )
                    current = cursor.fetchone()[0]

                    context.log.info(
                        f"dim.Borger: {merge_affected} rows affected by MERGE, "
                        f"{soft_deleted} soft-deleted, {changed_count} changed patients, "
                        f"{populationsansvar_updated} populationsansvar set, "
                        f"{protected_versions_masked} retired versions of protected "
                        f"citizens masked, {current} current, {total} total. "
                        f"Checkpoint: {checkpoint} → {new_checkpoint}. "
                        f"Timings: {timings}"
                    )

                    dfm.complete_dfm_run(
                        run_ctx.log_id,
                        checkpoint_after=new_checkpoint,
                        rows_affected=merge_affected,
                    )
                finally:
                    for table_name in all_temp_tables:
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
            "deleted_flag_from_fhir_patient": deleted_from_source,
            "populationsansvar_updated": populationsansvar_updated,
            "protected_versions_masked": protected_versions_masked,
            "threshold": get_soft_delete_threshold("dim.Borger", config.soft_delete_threshold),
            "current_versions": current,
            "total_versions": total,
            "checkpoint_before": checkpoint,
            "checkpoint_after": new_checkpoint,
            "table": "dim.Borger",
            "scd_type": "Type 2",
            "delta_mode": "patient-scoped",
            "mode": config.mode,
            "timings_seconds": str(timings),
            "source": (
                "fhir.Patient + ref.PatientToBorger "
                "+ ref.Patient_Address_To_Borger + fhir.Patient_Address"
            ),
        }
    )
