# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Condition asset for IFDB PAS_DIAGNOSE data, using DFM (Data Flow Management).

Single-asset pattern (mirrors ``DocumentReference_IFDB_Epikrise_dfm``): fetch
from ``arc.ifdb_pas_diagnose`` in batches, transform to FHIR Condition, and post
to the FHIR server. Distinct from Condition_IFDB_Diagnose_dfm.py which handles
clinician-level DIAGNOSE records.

Inline lookups (batch-scoped, chunked IN against the archive):
    - arc.ifdb_pas_diagnose_encounter_ref  → Condition.encounter
    - arc.ifdb_pas_diagnose_tillaegskode   → Condition.code.coding[] (supplementary codes)
    - arc.ifdb_pas_diagnose_henvisning_ref → source-metadata extension (ServiceRequest scope)

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required
    - Encounter (encounter) - optional
"""

from typing import Any

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
from DSA.FHIR.utils.ifdb.arc_lookup import (
    chunked_in_fetch,
    chunked_in_fetch_grouped,
)
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
from utils.fhir.fhir_extensions import (
    EXTENSION_DIAGNOSETYPE,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_PAS_DIAGNOSE_SOURCE_META,
    EXTENSION_SOURCE_MODIFIED_AT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
    SYSTEM_SKS_D,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

PAS_DIAGNOSE_REFERENCE_CONFIGS = [
    ReferenceFieldConfig(
        field_name="subject",
        target_resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        is_required=True,
        description="Patient reference from IFDB PATIENT_ID",
    ),
    ReferenceFieldConfig(
        field_name="encounter",
        target_resource_type="Encounter",
        identifier_system=SYSTEM_IFDB_OBJECT_ID,
        is_required=False,
        description="Encounter reference from PAS_DIAGNOSE_ENCOUNTER_REF",
    ),
]


BATCH_SIZE = 10_000

# ---------------------------------------------------------------------------
# Inline lookup helpers (batch-scoped, chunked IN against the archive)
# ---------------------------------------------------------------------------


def _fetch_pas_diagnose_encounter_refs(
    diagnose_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """Return ``DIAGNOSE_ID → first ENCOUNTER_ID`` for the given diagnoses."""
    rows = chunked_in_fetch(
        diagnose_ids,
        table="arc.ifdb_pas_diagnose_encounter_ref",
        columns=["DIAGNOSE_ID", "ENCOUNTER_ID"],
        where_column="DIAGNOSE_ID",
        mssql=mssql,
    )
    result: dict[str, str] = {}
    for row in rows:
        if row.get("ENCOUNTER_ID") is None:
            continue
        did = normalize_oracle_id(row["DIAGNOSE_ID"])
        if did is None or did in result:
            continue
        result[did] = normalize_oracle_id(row["ENCOUNTER_ID"])
    context.log.info(f"[ENCOUNTER_REF] Fetched {len(result)} encounter refs for PAS_DIAGNOSE")
    return result


def _fetch_pas_diagnose_tillaegskoder(
    diagnose_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch archived tillægskoder grouped by PAS_DIAGNOSE_ID."""
    result = chunked_in_fetch_grouped(
        diagnose_ids,
        table="arc.ifdb_pas_diagnose_tillaegskode",
        columns=["PAS_DIAGNOSE_ID", "TILLAEGSKODE_KODE", "TILLAEGSKODE_KODETEKST"],
        where_column="PAS_DIAGNOSE_ID",
        mssql=mssql,
        key_fn=lambda row: normalize_oracle_id(row["PAS_DIAGNOSE_ID"]),
        sort_key=lambda row: str(row.get("TILLAEGSKODE_KODE", "")),
    )
    context.log.info(
        f"[PAS_TILLAEGSKODE] Fetched {sum(len(v) for v in result.values())} "
        f"tillægskoder for {len(result)} PAS diagnoses"
    )
    return result


def _fetch_pas_diagnose_henvisning_refs(
    diagnose_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[str]]:
    """Return ``DIAGNOSE_ID → list[HENVISNING_ID]`` (source-metadata only)."""
    grouped = chunked_in_fetch_grouped(
        diagnose_ids,
        table="arc.ifdb_pas_diagnose_henvisning_ref",
        columns=["DIAGNOSE_ID", "HENVISNING_ID"],
        where_column="DIAGNOSE_ID",
        mssql=mssql,
        key_fn=lambda row: (
            normalize_oracle_id(row["DIAGNOSE_ID"])
            if row.get("HENVISNING_ID") is not None
            else None
        ),
    )
    result = {
        did: [normalize_oracle_id(r["HENVISNING_ID"]) for r in rows]
        for did, rows in grouped.items()
    }
    context.log.info(f"[HENVISNING_REF] Fetched refs for {len(result)} PAS diagnoses")
    return result


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def _transform_pas_diagnose_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    encounter_refs: dict[str, str] | None = None,
    tillaegskoder: dict[str, list[dict[str, Any]]] | None = None,
    henvisning_refs: dict[str, list[str]] | None = None,
) -> list[Condition]:
    """Transform PAS_DIAGNOSE rows to FHIR Condition resources."""
    conditions: list[Condition] = []

    for row_dict in rows:
        objekt_id = row_dict.get("OBJEKT_ID")
        patient_id = row_dict.get("PATIENT_ID")
        diagnose_kode = row_dict.get("DIAGNOSE_KODE")
        diagnose_kodetekst = row_dict.get("DIAGNOSE_KODETEKST")
        slettet = row_dict.get("SLETTET")
        diagnosetype = row_dict.get("DIAGNOSETYPE")
        later_disproved = row_dict.get("LATER_DISPROVED")
        opdateret_tid = row_dict.get("OPDATERET_TID")

        deleted_int = int(_is_true_flag(slettet))
        is_deleted = deleted_int != 0
        disproved = _is_true_flag(later_disproved)

        if is_deleted:
            clinical_status_code = None
            verification_code = "entered-in-error"
        elif disproved:
            clinical_status_code = None
            verification_code = "refuted"
        else:
            clinical_status_code = "active"
            verification_code = "confirmed"

        clinical_status = (
            CodeableConcept(
                coding=[
                    Coding(
                        system="http://terminology.hl7.org/CodeSystem/condition-clinical",
                        code=clinical_status_code,
                    )
                ]
            )
            if clinical_status_code
            else None
        )

        verification_status = CodeableConcept(
            coding=[
                Coding(
                    system="http://terminology.hl7.org/CodeSystem/condition-ver-status",
                    code=verification_code,
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

        # Extensions
        extensions: list[Extension] = [build_is_deleted_extension(deleted_int)]

        modified_dt = parse_clob_datetime_safe(opdateret_tid)
        if modified_dt:
            extensions.append(
                Extension.construct(url=EXTENSION_SOURCE_MODIFIED_AT, valueDateTime=modified_dt)
            )

        modtidspunkt_dt = parse_clob_datetime_safe(row_dict.get("MODTIDSPUNKT"))
        if modtidspunkt_dt:
            extensions.append(
                Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt_dt)
            )

        if diagnosetype:
            extensions.append(
                Extension.construct(url=EXTENSION_DIAGNOSETYPE, valueString=str(diagnosetype))
            )

        # Source metadata: modificeret_* fields, henvisning_refs
        meta_parts: list[Extension] = []
        for key, url in [
            ("MODIFICERET_PERSID", "modificeretPersId"),
            ("MODIFICERET_PERSNAVN", "modificeretPersNavn"),
            ("MODIFICERET_BRUGERROLLE", "modificeretBrugerrolle"),
            ("MODIFICERET_ADMENHED_KODE", "modificeretAdmEnhedKode"),
            ("MODIFICERET_ADMENHED_KODETEKST", "modificeretAdmEnhedKodetekst"),
            ("MODIFICERET_ADMENHED_ORGKODE", "modificeretAdmEnhedOrgkode"),
            ("MODIFICERET_ADMENHED_SORKODE", "modificeretAdmEnhedSorkode"),
        ]:
            val = row_dict.get(key)
            if val is not None:
                str_val = str(val).strip()
                if str_val:
                    meta_parts.append(Extension.construct(url=url, valueString=str_val))

        if henvisning_refs and oid:
            h_ids = henvisning_refs.get(oid, [])
            for h_id in h_ids:
                meta_parts.append(Extension.construct(url="henvisningId", valueString=h_id))

        if meta_parts:
            extensions.append(
                Extension.construct(url=EXTENSION_PAS_DIAGNOSE_SOURCE_META, extension=meta_parts)
            )

        # Encounter reference from lookup table
        encounter_ref = None
        if encounter_refs and oid:
            enc_id = encounter_refs.get(oid)
            if enc_id:
                encounter_ref = Reference(
                    identifier=Identifier(system=SYSTEM_IFDB_OBJECT_ID, value=enc_id)
                )

        condition = Condition(
            identifier=[Identifier(system=SYSTEM_IFDB_PAS_DIAGNOSE, value=oid)] if oid else None,
            extension=extensions if extensions else None,
            category=build_encounter_diagnosis_category(),
            clinicalStatus=clinical_status,
            verificationStatus=verification_status,
            code=condition_code,
            subject=(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_PATIENT_ID,
                        value=normalize_oracle_id(patient_id),
                    )
                )
                if patient_id
                else None
            ),
            encounter=encounter_ref,
        )
        conditions.append(condition)

    return conditions


# ---------------------------------------------------------------------------
# Post
# ---------------------------------------------------------------------------


def _post_pas_diagnose_to_fhir(
    context: dg.AssetExecutionContext,
    conditions: list[Condition],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post PAS_DIAGNOSE conditions to FHIR database.

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    subject resolution (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    encounter_ids_needed: set[str] = set()
    for cond in conditions:
        if cond.subject and getattr(cond.subject, "identifier", None):
            patient_ids_needed.add(str(cond.subject.identifier.value))
        if cond.encounter and getattr(cond.encounter, "identifier", None):
            encounter_ids_needed.add(str(cond.encounter.identifier.value))

    if resolve_missing_dependencies and mssql:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_encounters,
            resolve_missing_patients,
        )

        if encounter_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(encounter_ids_needed)} encounters exist")
            resolve_missing_encounters(
                encounter_ids=encounter_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
            )

        if patient_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
            resolve_missing_patients(
                patient_ids=patient_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
            )

    conditions = prepare_resources_for_upsert(
        resources=conditions,
        fhir_database=fhir_database,
        resource_type="Condition",
        preferred_id_systems=[SYSTEM_IFDB_PAS_DIAGNOSE],
        reference_configs=PAS_DIAGNOSE_REFERENCE_CONFIGS,
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
    name="fhir_server_condition_pas_diagnose_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_pas_diagnose_ifdb",
        "arc_pas_diagnose_encounter_ref_ifdb",
        "arc_pas_diagnose_tillaegskode_ifdb",
        "arc_pas_diagnose_henvisning_ref_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
    ],
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
    description=(
        "Fetch PAS-diagnoses from arc.ifdb_pas_diagnose in batches, transform to "
        "FHIR Condition (encounter refs, tillægskoder and henvisning refs joined "
        "from the arc ref tables), and post to the FHIR server. Checkpoint owner "
        "for fhir_server.Condition_PasDiagnose."
    ),
)
def fhir_server_condition_pas_diagnose_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for PAS_DIAGNOSE Condition resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Condition_PasDiagnose",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        # include_deleted=True so retracted PAS-diagnoses are tomb-stoned
        # (verificationStatus=entered-in-error) rather than left active.
        query = build_fhir_archive_query(
            "arc.ifdb_pas_diagnose", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_pas_diagnose (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} PAS-diagnoses "
                        f"(total so far: {total_fetched})"
                    )

                    diagnose_ids = [
                        normalize_oracle_id(row["OBJEKT_ID"])
                        for row in batch
                        if row.get("OBJEKT_ID")
                    ]
                    encounter_refs = _fetch_pas_diagnose_encounter_refs(
                        diagnose_ids, mssql_target_staging, context
                    )
                    tillaegskoder = _fetch_pas_diagnose_tillaegskoder(
                        diagnose_ids, mssql_target_staging, context
                    )
                    henvisning_refs = _fetch_pas_diagnose_henvisning_refs(
                        diagnose_ids, mssql_target_staging, context
                    )

                    conditions = _transform_pas_diagnose_to_fhir(
                        batch,
                        context,
                        encounter_refs,
                        tillaegskoder,
                        henvisning_refs,
                    )
                    if conditions:
                        _post_pas_diagnose_to_fhir(
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

        new_checkpoint: str | None = None
        if config.should_advance_checkpoint and total_fetched > 0:
            with mssql_target_staging.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                    row = cursor.fetchone()
                    new_checkpoint = row[0] if row else None
                finally:
                    cursor.close()

            if new_checkpoint:
                dfm.set_checkpoint(
                    "fhir_server", "Condition_PasDiagnose", value=new_checkpoint, mode=config.mode
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
