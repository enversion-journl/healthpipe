# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Condition asset for IFDB DIAGNOSE data, using DFM (Data Flow Management).

Single-asset pattern (mirrors ``DocumentReference_IFDB_Epikrise_dfm``): fetch
from ``arc.ifdb_diagnose`` in batches, transform to FHIR Condition, and post to
the FHIR server. Tillægskoder are joined per-batch from
``arc.ifdb_diagnose_tillaegskode`` with the same TILLAEGSKODE_KODE ordering as
the previous Oracle lookup (coding order is observable downstream).

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required
"""

from typing import Any, cast

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
from DSA.FHIR.utils.code_normalization import to_sks_diagnosis_code
from DSA.FHIR.utils.codings import build_unique_ordered_codings
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch_grouped
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import (
    normalize_oracle_id,
    parse_clob_datetime_safe,
)
from DSA.FHIR.utils.reference_resolver import (
    ReferenceFieldConfig,
    ResolveValuesFn,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.condition_categories import build_encounter_diagnosis_category
from utils.fhir.fhir_extensions import EXTENSION_MODTIDSPUNKT
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
    SYSTEM_SKS_D,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

DIAGNOSE_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
]


BATCH_SIZE = 10_000

_CONDITION_CLINICAL_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-clinical"
_CONDITION_VERIFICATION_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-ver-status"


def _fetch_diagnose_tillaegskoder(
    diagnose_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch archived tillægskoder grouped by DIAGNOSE_ID, sorted by TILLAEGSKODE_KODE."""
    result = chunked_in_fetch_grouped(
        diagnose_ids,
        table="arc.ifdb_diagnose_tillaegskode",
        columns=["DIAGNOSE_ID", "TILLAEGSKODE_KODE", "TILLAEGSKODE_KODETEKST"],
        where_column="DIAGNOSE_ID",
        mssql=mssql,
        key_fn=lambda row: normalize_oracle_id(row["DIAGNOSE_ID"]),
        sort_key=lambda row: str(row.get("TILLAEGSKODE_KODE", "")),
    )
    context.log.info(
        f"[TILLAEGSKODE] Fetched {sum(len(v) for v in result.values())} "
        f"tillægskoder for {len(result)} diagnoses"
    )
    return result


def _transform_diagnose_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    tillaegskoder: dict[str, list[dict[str, Any]]] | None = None,
) -> list[Condition]:
    """Transform DIAGNOSE rows to FHIR Condition resources."""
    conditions: list[Condition] = []

    for row_dict in rows:
        objekt_id = row_dict.get("OBJEKT_ID")
        patient_id = row_dict.get("PATIENT_ID")
        diagnose_kode = row_dict.get("DIAGNOSE_KODE")
        diagnose_kodetekst = row_dict.get("DIAGNOSE_KODETEKST")
        slettet = row_dict.get("SLETTET")

        beslut_tidspunkt = row_dict.get("BESLUT_TIDSPUNKT")
        opret_tidspunkt = row_dict.get("OPRET_TIDSPUNKT")
        afslut_tidspunkt = row_dict.get("AFSLUT_TIDSPUNKT")

        deleted_int = int(_is_true_flag(slettet))
        is_deleted = deleted_int != 0
        has_end = afslut_tidspunkt is not None

        if has_end and not is_deleted:
            clinical_status_code = "resolved"
        elif not is_deleted:
            clinical_status_code = "active"
        else:
            clinical_status_code = None

        clinical_status = (
            CodeableConcept(
                coding=[
                    Coding(
                        system=_CONDITION_CLINICAL_STATUS_SYSTEM,
                        code=clinical_status_code,
                    )
                ]
            )
            if clinical_status_code
            else None
        )
        # Business rule:
        # - SLETTET=1 means the source registration was retracted/invalidated.
        #   In FHIR this is represented with verificationStatus=entered-in-error
        #   and no clinicalStatus, so downstream can distinguish deletion from
        #   an ordinary inactive diagnosis.
        # - Otherwise mark diagnosis as confirmed.
        verification_status = CodeableConcept(
            coding=[
                Coding(
                    system=_CONDITION_VERIFICATION_STATUS_SYSTEM,
                    code="entered-in-error" if is_deleted else "confirmed",
                )
            ]
        )

        oid = normalize_oracle_id(objekt_id)
        # Diagnosis codes are canonicalised to SKS-D form via the single
        # codebase-wide rule (to_sks_diagnosis_code). Tillaegskoder are NOT
        # diagnosis codes and keep plain SKS-prefix stripping in the builder.
        code_codings = build_unique_ordered_codings(
            system=SYSTEM_SKS,
            primary_system=SYSTEM_SKS_D,
            primary_code=to_sks_diagnosis_code(diagnose_kode),
            primary_display=diagnose_kodetekst,
            supplementary_rows=(tillaegskoder or {}).get(oid, []) if oid else [],
            supplement_code_key="TILLAEGSKODE_KODE",
            supplement_display_key="TILLAEGSKODE_KODETEKST",
        )

        condition_code = CodeableConcept(
            coding=code_codings if code_codings else None,
            text=str(diagnose_kodetekst) if diagnose_kodetekst else None,
        )

        recorded_date = parse_clob_datetime_safe(beslut_tidspunkt)
        onset_dt = parse_clob_datetime_safe(opret_tidspunkt)
        abatement_dt = parse_clob_datetime_safe(afslut_tidspunkt)

        pid = normalize_oracle_id(patient_id)
        extensions = [build_is_deleted_extension(deleted_int)]
        modtidspunkt_dt = parse_clob_datetime_safe(row_dict.get("MODTIDSPUNKT"))
        if modtidspunkt_dt:
            extensions.append(
                Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt_dt)
            )
        condition = Condition(
            identifier=[Identifier(system=SYSTEM_IFDB_DIAGNOSE, value=oid)] if oid else None,
            extension=extensions,
            category=build_encounter_diagnosis_category(),
            clinicalStatus=clinical_status,
            verificationStatus=verification_status,
            code=condition_code,
            subject=(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_PATIENT_ID,
                        value=pid,
                    )
                )
                if pid
                else None
            ),
            recordedDate=recorded_date,
            onsetDateTime=onset_dt,
            abatementDateTime=abatement_dt,
        )
        conditions.append(condition)

    return conditions


def _post_diagnose_to_fhir(
    context: dg.AssetExecutionContext,
    conditions: list[Condition],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post DIAGNOSE conditions to FHIR database.

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    subject resolution (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    for cond in conditions:
        if cond.subject and getattr(cond.subject, "identifier", None):
            patient_ids_needed.add(str(cond.subject.identifier.value))

    if resolve_missing_dependencies and mssql and patient_ids_needed:
        from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients

        logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
        ensure_patients(
            ifdb_patient_ids=patient_ids_needed,
            mssql=mssql,
            fhir_database=fhir_database,
            context=context,
            need=NEED_REFERENCE,
        )

    conditions = prepare_resources_for_upsert(
        resources=conditions,
        fhir_database=fhir_database,
        resource_type="Condition",
        preferred_id_systems=[SYSTEM_IFDB_DIAGNOSE],
        reference_configs=DIAGNOSE_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=conditions,
        fhir_database=fhir_database,
        resource_type="Condition",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


# ---------------------------------------------------------------------------
# Single DFM asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_condition_diagnose_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_diagnose_ifdb",
        "arc_diagnose_tillaegskode_ifdb",
        "fhir_server_patient_dfm",
    ],
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
    description=(
        "Fetch diagnoses from arc.ifdb_diagnose in batches, transform to FHIR "
        "Condition (tillægskoder joined from arc.ifdb_diagnose_tillaegskode), "
        "and post to the FHIR server. Checkpoint owner for fhir_server.Condition_Diagnose."
    ),
)
def fhir_server_condition_diagnose_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for DIAGNOSE Condition resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Condition_Diagnose",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        # include_deleted=True so retracted diagnoses are tomb-stoned
        # (verificationStatus=entered-in-error) rather than left active.
        query = build_fhir_archive_query(
            "arc.ifdb_diagnose", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_diagnose (mode={config.mode})")

        total_fetched = 0
        total_posted = 0
        batch_num = 0

        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(query)
                columns = [desc[0] for desc in cursor.description]

                while True:
                    raw_rows = cursor.fetchmany(BATCH_SIZE)
                    if not raw_rows:
                        break

                    batch_num += 1
                    batch = [dict(zip(columns, row, strict=False)) for row in raw_rows]
                    total_fetched += len(batch)
                    context.log.info(
                        f"Batch {batch_num}: processing {len(batch)} diagnoses "
                        f"(total so far: {total_fetched})"
                    )

                    diagnose_ids = cast(
                        list[str],
                        [
                            normalize_oracle_id(row["OBJEKT_ID"])
                            for row in batch
                            if row.get("OBJEKT_ID")
                        ],
                    )
                    tillaegskoder = _fetch_diagnose_tillaegskoder(
                        diagnose_ids, mssql_target_staging, context
                    )

                    conditions = _transform_diagnose_to_fhir(batch, context, tillaegskoder)
                    if conditions:
                        _post_diagnose_to_fhir(
                            context=context,
                            conditions=conditions,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            mssql=None,
                            resolve_missing_dependencies=False,
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(conditions)
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Condition_Diagnose", config, total_fetched
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
