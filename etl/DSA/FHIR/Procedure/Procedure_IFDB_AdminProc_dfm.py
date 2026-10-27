# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Procedure asset for IFDB ADMINISTRATIVE_PROCEDURE data (DFM) — lossless mapping.

Single-asset pattern (mirrors ``Condition_IFDB_Diagnose_dfm``): fetch from
``arc.ifdb_administrative_procedure`` in batches, transform to FHIR Procedure,
and post to the FHIR server. Supplementary codes are joined per-batch from
``arc.ifdb_adm_procedure_suppl_codes`` with the same IDX ordering as the
previous Oracle lookup.

ALL 28 source columns are mapped: standard FHIR fields where semantically appropriate,
custom extensions for remaining source metadata.

Standard FHIR fields:
    - Procedure.identifier <- OBJECT_ID
    - Procedure.subject <- PATIENT_ID (required)
    - Procedure.encounter <- ENCOUNTER_ID (optional)
    - Procedure.code <- PROCEDURE_CODE / PROCEDURE_TEXT (SKS)
    - Procedure.status <- completed
    - Procedure.performedPeriod <- PERIOD_START / PERIOD_END
    - Procedure.bodySite <- INDICATION_OF_SIDE_CODE / INDICATION_OF_SIDE_TEXT

Extensions:
    - isDeleted <- DELETED (boolean)
    - episodeOfCare <- EPISODE_OF_CARE_ID
    - sourceCreatedAt <- EPJI_CREATED_DATETIME
    - sourceModifiedAt <- EPJI_MODIFICATION_DATETIME
    - procedureSourceMeta <- STATUS_CODE/TEXT, USED_CONTRAST, STAFF_CATEGORY,
      USER_*, LPR3_IGNORED, IFDB_MODIFICATION_DATETIME
    - procedureLocationMeta <- LOCATION_CODE/NAME/ORGCODE/SORCODE

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required
    - Encounter (encounter) - optional

    Uses NestedReferenceFieldConfig for:
    - EpisodeOfCare (extension.valueReference) - optional
"""

from typing import Any, cast

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
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
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import (
    parse_clob_datetime_safe,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    ResolveValuesFn,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.procedure import Procedure
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_PROCEDURE_LOCATION_META,
    EXTENSION_PROCEDURE_SOURCE_META,
    EXTENSION_SOURCE_CREATED_AT,
    EXTENSION_SOURCE_MODIFIED_AT,
)
from utils.fhir.identifier_systems import (
    PERFORMER_FUNCTION_DISPLAYS,
    PERFORMER_FUNCTION_UDFOERENDE,
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_PERFORMER_FUNCTION,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension
from utils.fhir.resource_flags import FLAG_LPR3_IGNORED, build_flag_meta

PROCEDURE_REFERENCE_CONFIGS = [
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
        description="Encounter reference from IFDB ENCOUNTER_ID resolved via Encounter OBJECT_ID",
    ),
]

PROCEDURE_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        extension_url=EXTENSION_EPISODE_OF_CARE,
        is_required=False,
        description="Episode of care reference from extension",
    ),
    # Udførende enhed (LOCATION_SORCODE) opløses til Organization/<id>, så den
    # flade tabel kan joine til fhir.Organization ligesom ServiceRequest gør.
    # Kliniker-referencen (SYSTEM_IFDB_STAFF) står ikke i mappingen: der findes
    # ingen Practitioner-ressourcer at opløse mod, så den bevares som
    # identifier-reference og projiceres direkte (practitioner_id).
    NestedReferenceFieldConfig(
        field_path="performer.actor",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
        },
        is_required=False,
        description="Udførende enhed (SOR) som Organization-reference",
    ),
]


BATCH_SIZE = 10_000


def _performer_function(code: str) -> CodeableConcept:
    """Rolle for en organisation på proceduren (udførende enhed, stamafdeling)."""
    return CodeableConcept(
        coding=[
            Coding(
                system=SYSTEM_PERFORMER_FUNCTION,
                code=code,
                display=PERFORMER_FUNCTION_DISPLAYS[code],
            )
        ],
        text=PERFORMER_FUNCTION_DISPLAYS[code],
    )


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _adm_suppl_sort_key(row: dict[str, Any]) -> int:
    return int(row.get("IDX") or 0)


# ---------------------------------------------------------------------------
# Lossless extensions builders
# ---------------------------------------------------------------------------


def _build_source_meta_extensions(row_dict: dict[str, Any]) -> list[Extension]:
    """Build procedureSourceMeta extension with all unmapped source fields."""
    parts: list[Extension] = []

    def _add(key: str, val: Any) -> None:
        s = _str_or_none(val)
        if s:
            parts.append(Extension.construct(url=key, valueString=s))

    _add("statusCode", row_dict.get("STATUS_CODE"))
    _add("statusText", row_dict.get("STATUS_TEXT"))
    _add("usedContrastCode", row_dict.get("USED_CONTRAST_CODE"))
    _add("usedContrastText", row_dict.get("USED_CONTRAST_TEXT"))
    _add("staffCategoryCode", row_dict.get("STAFF_CATEGORY_CODE"))
    _add("staffCategoryText", row_dict.get("STAFF_CATEGORY_TEXT"))
    _add("userId", row_dict.get("USER_ID"))
    _add("userName", row_dict.get("USER_NAME"))
    _add("userRole", row_dict.get("USER_ROLE"))
    _add("userRoleName", row_dict.get("USER_ROLE_NAME"))

    ifdb_mod = parse_clob_datetime_safe(row_dict.get("IFDB_MODIFICATION_DATETIME"))
    if ifdb_mod:
        parts.append(Extension.construct(url="ifdbModifiedAt", valueString=ifdb_mod))

    lpr3 = row_dict.get("LPR3_IGNORED")
    if lpr3 is not None:
        parts.append(Extension.construct(url="lpr3Ignored", valueBoolean=_is_true_flag(lpr3)))

    if not parts:
        return []
    return [Extension.construct(url=EXTENSION_PROCEDURE_SOURCE_META, extension=parts)]


def _build_location_meta_extensions(row_dict: dict[str, Any]) -> list[Extension]:
    """Build procedureLocationMeta extension with location columns."""
    parts: list[Extension] = []

    def _add(key: str, val: Any) -> None:
        s = _str_or_none(val)
        if s:
            parts.append(Extension.construct(url=key, valueString=s))

    _add("code", row_dict.get("LOCATION_CODE"))
    _add("name", row_dict.get("LOCATION_NAME"))
    _add("orgCode", row_dict.get("LOCATION_ORGCODE"))
    _add("sorCode", row_dict.get("LOCATION_SORCODE"))

    if not parts:
        return []
    return [Extension.construct(url=EXTENSION_PROCEDURE_LOCATION_META, extension=parts)]


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def _fetch_adm_procedure_suppl_codes(
    parent_object_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch archived supplementary codes grouped by PARENT_OBJECT_ID, sorted by IDX."""
    result = chunked_in_fetch_grouped(
        parent_object_ids,
        table="arc.ifdb_adm_procedure_suppl_codes",
        columns=["PARENT_OBJECT_ID", "SUPPL_CODE", "SUPPL_TEXT", "IDX"],
        where_column="PARENT_OBJECT_ID",
        mssql=mssql,
        key_fn=lambda row: _normalize_oracle_id(row["PARENT_OBJECT_ID"]),
        sort_key=_adm_suppl_sort_key,
    )
    context.log.info(
        f"[SUPPL_CODES] Fetched {sum(len(v) for v in result.values())} "
        f"supplementary codes for {len(result)} procedures"
    )
    return result


def _transform_adm_procedure_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    suppl_codes: dict[str, list[dict[str, Any]]] | None = None,
) -> list[Procedure]:
    """Transform ADMINISTRATIVE_PROCEDURE rows to FHIR Procedure resources.

    Lossless mapping: all 28 source columns are mapped to standard FHIR fields
    or custom extensions. No source data is dropped.
    """
    procedures: list[Procedure] = []

    for row_dict in rows:
        objekt_id = _normalize_oracle_id(row_dict.get("OBJECT_ID"))
        patient_id = _normalize_oracle_id(row_dict.get("PATIENT_ID"))
        encounter_id = _normalize_oracle_id(row_dict.get("ENCOUNTER_ID"))
        episode_id = _normalize_oracle_id(row_dict.get("EPISODE_OF_CARE_ID"))
        procedure_kode = row_dict.get("PROCEDURE_CODE")
        procedure_kodetekst = row_dict.get("PROCEDURE_TEXT")
        slettet = row_dict.get("DELETED")
        period_start_raw = row_dict.get("PERIOD_START")
        period_end_raw = row_dict.get("PERIOD_END")
        side_code = row_dict.get("INDICATION_OF_SIDE_CODE")
        side_text = row_dict.get("INDICATION_OF_SIDE_TEXT")
        epji_created = row_dict.get("EPJI_CREATED_DATETIME")
        epji_modified = row_dict.get("EPJI_MODIFICATION_DATETIME")

        deleted_int = int(_is_true_flag(slettet))
        is_deleted = deleted_int != 0
        status = "entered-in-error" if is_deleted else "completed"

        # --- code ---
        code_codings = build_unique_ordered_codings(
            system=SYSTEM_SKS,
            primary_code=procedure_kode,
            primary_display=procedure_kodetekst,
            supplementary_rows=(
                suppl_codes.get(str(objekt_id), []) if suppl_codes and objekt_id else []
            ),
            supplement_code_key="SUPPL_CODE",
            supplement_display_key="SUPPL_TEXT",
            supplement_sequence_key="IDX",
        )

        procedure_code = CodeableConcept(
            coding=code_codings if code_codings else None,
            text=str(procedure_kodetekst) if procedure_kodetekst else None,
        )

        # --- performedPeriod ---
        period_start = parse_clob_datetime_safe(period_start_raw)
        period_end = parse_clob_datetime_safe(period_end_raw)
        performed_period = None
        if period_start and period_end:
            performed_period = Period.construct(start=period_start, end=period_end)
        elif period_start:
            performed_period = Period.construct(start=period_start)
        elif period_end:
            performed_period = Period.construct(end=period_end)

        # --- bodySite ---
        body_sites = []
        if side_code or side_text:
            bs_codings = []
            if side_code:
                bs_codings.append(
                    Coding(
                        system=SYSTEM_SKS,
                        code=str(side_code),
                        display=str(side_text) if side_text else None,
                    )
                )
            body_sites.append(
                CodeableConcept(
                    coding=bs_codings if bs_codings else None,
                    text=str(side_text) if side_text else None,
                )
            )

        # --- extensions ---
        extensions: list[Extension] = []

        extensions.append(build_is_deleted_extension(deleted_int))

        if episode_id:
            extensions.append(
                Extension(
                    url=EXTENSION_EPISODE_OF_CARE,
                    valueReference=Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            value=_normalize_oracle_id(episode_id),
                        )
                    ),
                )
            )

        created_dt = parse_clob_datetime_safe(epji_created)
        if created_dt:
            extensions.append(
                Extension.construct(url=EXTENSION_SOURCE_CREATED_AT, valueDateTime=created_dt)
            )
        modified_dt = parse_clob_datetime_safe(epji_modified)
        if modified_dt:
            extensions.append(
                Extension.construct(url=EXTENSION_SOURCE_MODIFIED_AT, valueDateTime=modified_dt)
            )

        extensions.extend(_build_source_meta_extensions(row_dict))
        extensions.extend(_build_location_meta_extensions(row_dict))

        # --- performer ---
        performer_list = []
        user_id = row_dict.get("USER_ID")
        user_name = row_dict.get("USER_NAME")
        user_role = row_dict.get("USER_ROLE")
        user_role_name = row_dict.get("USER_ROLE_NAME")
        if user_id or user_name:
            from fhir.resources.R4B.procedure import ProcedurePerformer

            actor_ref = Reference(
                type="Practitioner",
                display=str(user_name) if user_name else None,
                identifier=(
                    Identifier(
                        system=SYSTEM_IFDB_STAFF,
                        value=_normalize_oracle_id(user_id),
                    )
                    if user_id
                    else None
                ),
            )
            function_cc = None
            role_display = str(user_role_name) if user_role_name else None
            if user_role or role_display:
                role_codings = []
                if user_role:
                    role_codings.append(Coding(code=str(user_role), display=role_display))
                function_cc = CodeableConcept(
                    coding=role_codings if role_codings else None,
                    text=role_display,
                )
            performer_list.append(ProcedurePerformer(actor=actor_ref, function=function_cc))

        location_sorcode = _str_or_none(row_dict.get("LOCATION_SORCODE"))
        if location_sorcode:
            from fhir.resources.R4B.procedure import ProcedurePerformer

            performer_list.append(
                ProcedurePerformer(
                    actor=Reference(
                        type="Organization",
                        identifier=Identifier(
                            system=SYSTEM_SOR,
                            value=_normalize_oracle_id(location_sorcode),
                        ),
                    ),
                    function=_performer_function(PERFORMER_FUNCTION_UDFOERENDE),
                )
            )

        # --- usedCode ---
        used_codes = []
        contrast_code = row_dict.get("USED_CONTRAST_CODE")
        contrast_text = row_dict.get("USED_CONTRAST_TEXT")
        if contrast_code or contrast_text:
            cc_codings = []
            if contrast_code:
                cc_codings.append(
                    Coding(
                        system=SYSTEM_SKS,
                        code=str(contrast_code),
                        display=str(contrast_text) if contrast_text else None,
                    )
                )
            used_codes.append(
                CodeableConcept(
                    coding=cc_codings if cc_codings else None,
                    text=str(contrast_text) if contrast_text else None,
                )
            )

        # --- resource ---
        # Driftsflag som meta.tag: kun dermed kan der soeges paa flaget via
        # FHIR-serveren og filtreres paa det som kolonne i de flade tabeller.
        flag_koder = [FLAG_LPR3_IGNORED] if _is_true_flag(row_dict.get("LPR3_IGNORED")) else []
        proc = Procedure(
            meta=build_flag_meta(flag_koder),
            identifier=(
                [
                    Identifier(
                        system=SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
                        value=_normalize_oracle_id(objekt_id),
                    )
                ]
                if objekt_id
                else None
            ),
            status=status,
            code=procedure_code,
            subject=(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_PATIENT_ID,
                        value=_normalize_oracle_id(patient_id),
                    )
                )
                if patient_id
                else None
            ),
            encounter=(
                Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_OBJECT_ID,
                        value=_normalize_oracle_id(encounter_id),
                    )
                )
                if encounter_id
                else None
            ),
            performedPeriod=performed_period,
            bodySite=body_sites if body_sites else None,
            performer=performer_list if performer_list else None,
            usedCode=used_codes if used_codes else None,
            extension=extensions if extensions else None,
        )
        procedures.append(proc)

    return procedures


def _post_adm_procedure_to_fhir(
    context: dg.AssetExecutionContext,
    procedures: list[Procedure],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post ADMINISTRATIVE_PROCEDURE resources to FHIR database.

    ``reference_fallback_fn`` is an optional patient-id → CPR fallback for
    subject resolution (see ``build_patient_cpr_fallback``).
    """
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    encounter_ids_needed: set[str] = set()
    eoc_ids_needed: set[str] = set()
    for proc in procedures:
        if proc.subject and getattr(proc.subject, "identifier", None):
            patient_ids_needed.add(str(proc.subject.identifier.value))
        if proc.encounter and getattr(proc.encounter, "identifier", None):
            encounter_ids_needed.add(str(proc.encounter.identifier.value))
        for ext in proc.extension or []:
            if (
                ext.url == EXTENSION_EPISODE_OF_CARE
                and ext.valueReference
                and getattr(ext.valueReference, "identifier", None)
            ):
                eoc_ids_needed.add(str(ext.valueReference.identifier.value))

    if resolve_missing_dependencies and mssql:
        from DSA.FHIR.Patient.patient_service import NEED_REFERENCE, ensure_patients
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_encounters,
            resolve_missing_episodes_of_care,
        )

        if eoc_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(eoc_ids_needed)} episodes of care exist")
            resolve_missing_episodes_of_care(
                eoc_ids=eoc_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
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
            ensure_patients(
                ifdb_patient_ids=patient_ids_needed,
                mssql=mssql,
                fhir_database=fhir_database,
                context=context,
                need=NEED_REFERENCE,
            )

    procedures = prepare_resources_for_upsert(
        resources=procedures,
        fhir_database=fhir_database,
        resource_type="Procedure",
        preferred_id_systems=[SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE],
        reference_configs=PROCEDURE_REFERENCE_CONFIGS,
        nested_reference_configs=PROCEDURE_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=procedures,
        fhir_database=fhir_database,
        resource_type="Procedure",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


# ---------------------------------------------------------------------------
# Single DFM asset — fetch batches, transform, post
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_procedure_admin_proc_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_administrative_procedure_ifdb",
        "arc_adm_procedure_suppl_codes_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
    description=(
        "Fetch administrative procedures from arc.ifdb_administrative_procedure "
        "in batches, transform to FHIR Procedure (supplementary codes joined from "
        "arc.ifdb_adm_procedure_suppl_codes), and post to the FHIR server. "
        "Checkpoint owner for fhir_server.Procedure_AdminProc."
    ),
)
def fhir_server_procedure_admin_proc_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for ADMINISTRATIVE_PROCEDURE resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Procedure_AdminProc",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        # include_deleted=True so deleted procedures are tomb-stoned (isDeleted
        # extension updated) rather than left active.
        query = build_fhir_archive_query(
            "arc.ifdb_administrative_procedure", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_administrative_procedure (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} procedures "
                        f"(total so far: {total_fetched})"
                    )

                    object_ids = cast(
                        list[str],
                        [
                            _normalize_oracle_id(row["OBJECT_ID"])
                            for row in batch
                            if row.get("OBJECT_ID")
                        ],
                    )
                    suppl_codes = _fetch_adm_procedure_suppl_codes(
                        object_ids, mssql_target_staging, context
                    )

                    procedures = _transform_adm_procedure_to_fhir(batch, context, suppl_codes)
                    if procedures:
                        _post_adm_procedure_to_fhir(
                            context=context,
                            procedures=procedures,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            mssql=None,
                            resolve_missing_dependencies=False,
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(procedures)
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Procedure_AdminProc", config, total_fetched
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
