# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Procedure assets for IFDB ADMINISTRATIVE_PROCEDURE data — lossless mapping.

Transforms extracted administrative procedure data into FHIR Procedure resources.
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

from typing import Any

import dagster as dg
from core.parallel import ChunkedPipelineStage, run_chunked_transform_post
from core.partitions import (
    daily_partitions,
    full_load_partitions,
    monthly_partitions,
)
from core.resources import (
    FHIRDatabaseResource,
    FHIRServerResource,
    IFDBOracleResource,
)
from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.oracle_lookup import chunked_in_fetch_grouped
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
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

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
]


# Chunked transform+post stage. Override with IFDB_ADMIN_PROC_PIPELINE_CHUNK_SIZE.
# AdminProc has more columns than Diagnose so chunks are sized smaller to keep
# peak memory bounded.
ADMIN_PROC_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.procedure.admin_proc.transform_post",
    env_prefix="IFDB_ADMIN_PROC_PIPELINE",
    default_chunk_size=20_000,
    cleanup_resource_types={"Procedure"},
)


def _str_or_none(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


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
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch ADM_PROCEDURE_SUPPL_CODES grouped by PARENT_OBJECT_ID, sorted by IDX."""
    result = chunked_in_fetch_grouped(
        parent_object_ids,
        table="IFDB.ADM_PROCEDURE_SUPPL_CODES",
        columns=["PARENT_OBJECT_ID", "SUPPL_CODE", "SUPPL_TEXT", "IDX"],
        where_column="PARENT_OBJECT_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
        key_fn=lambda row: _normalize_oracle_id(row["PARENT_OBJECT_ID"]),
        sort_key=lambda row: int(row.get("IDX", 0)),
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
        code_codings = []
        if procedure_kode:
            procedure_code_value = normalize_sks_coding_code(procedure_kode)
            if procedure_code_value:
                code_codings.append(
                    Coding(
                        system=SYSTEM_SKS,
                        code=str(procedure_code_value),
                        display=str(procedure_kodetekst) if procedure_kodetekst else None,
                    )
                )
        if suppl_codes and objekt_id:
            for sc_row in suppl_codes.get(str(objekt_id), []):
                sc_code = sc_row.get("SUPPL_CODE")
                sc_text = sc_row.get("SUPPL_TEXT")
                if sc_code:
                    sc_code_value = normalize_sks_coding_code(sc_code)
                    if sc_code_value:
                        code_codings.append(
                            Coding(
                                system=SYSTEM_SKS,
                                code=str(sc_code_value),
                                display=str(sc_text) if sc_text else None,
                            )
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
                        identifier=Identifier(
                            system=SYSTEM_SOR,
                            value=_normalize_oracle_id(location_sorcode),
                        ),
                    ),
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
        proc = Procedure(
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
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post ADMINISTRATIVE_PROCEDURE resources to FHIR database."""
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

    if resolve_missing_dependencies and ifdb_oracle:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_encounters,
            resolve_missing_episodes_of_care,
            resolve_missing_patients,
        )

        if eoc_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(eoc_ids_needed)} episodes of care exist")
            resolve_missing_episodes_of_care(
                eoc_ids=eoc_ids_needed,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

        if encounter_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(encounter_ids_needed)} encounters exist")
            resolve_missing_encounters(
                encounter_ids=encounter_ids_needed,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

        if patient_ids_needed:
            logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
            resolve_missing_patients(
                patient_ids=patient_ids_needed,
                ifdb_oracle=ifdb_oracle,
                fhir_database=fhir_database,
                context=context,
            )

    procedures = prepare_resources_for_upsert(
        resources=procedures,
        fhir_database=fhir_database,
        resource_type="Procedure",
        preferred_id_systems=[SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE],
        reference_configs=PROCEDURE_REFERENCE_CONFIGS,
        nested_reference_configs=PROCEDURE_NESTED_REFERENCE_CONFIGS,
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


def _run_adm_procedure_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Chunked transform+post pipeline shared by all AdminProc assets.

    Behaviour matches the pre-chunk monolithic path:
    - Supplemental codes are fetched once for the full partition via setup_fn.
    - Each chunk is transformed with that shared lookup and posted via
      _post_adm_procedure_to_fhir, which still owns dependency resolution
      (Patient/Encounter/EpisodeOfCare), reference resolution, ID resolution
      and direct DB upsert.
    """

    def setup(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        object_ids = [
            _normalize_oracle_id(row["OBJECT_ID"]) for row in rows if row.get("OBJECT_ID")
        ]
        return _fetch_adm_procedure_suppl_codes(object_ids, ifdb_oracle, context)

    def transform(
        chunk: list[dict[str, Any]],
        suppl_codes: dict[str, list[dict[str, Any]]],
    ) -> list[Procedure]:
        return _transform_adm_procedure_to_fhir(chunk, context, suppl_codes)

    def post(
        procedures: list[Procedure],
        chunk: list[dict[str, Any]],
        _suppl_codes: dict[str, list[dict[str, Any]]],
    ) -> dg.MaterializeResult:
        return _post_adm_procedure_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        ADMIN_PROC_CHUNK_STAGE,
        rows=extracted_data,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


@dg.asset(
    name="fhir_administrative_procedure_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "administrative_procedure_daily"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
)
def fhir_administrative_procedure_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ADMINISTRATIVE_PROCEDURE resources for daily partition."""
    return _run_adm_procedure_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


@dg.asset(
    name="fhir_administrative_procedure_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "administrative_procedure_monthly"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "8"},
)
def fhir_administrative_procedure_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ADMINISTRATIVE_PROCEDURE resources for monthly partition."""
    return _run_adm_procedure_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


@dg.asset(
    name="fhir_administrative_procedure_bulk_ifdb",
    ins={
        "extracted_data": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "administrative_procedure_full_load"])
        ),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "16"},
)
def fhir_administrative_procedure_bulk_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ADMINISTRATIVE_PROCEDURE resources for bulk load."""
    return _run_adm_procedure_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
