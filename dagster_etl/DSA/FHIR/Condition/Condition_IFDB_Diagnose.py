# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Condition assets for IFDB DIAGNOSE data.

Transforms extracted diagnosis data into FHIR Condition resources.
Follows the same pattern as the existing condition.py (HealthIssue -> Condition)
but maps from IFDB.DIAGNOSE which contains ICD-10/SKS diagnosis codes.

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required
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
from DSA.FHIR.utils.code_normalization import to_sks_diagnosis_code
from DSA.FHIR.utils.codings import build_unique_ordered_codings
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
from DSA.FHIR.utils.ifdb.transforms import (
    normalize_oracle_id,
    parse_clob_datetime_safe,
)
from DSA.FHIR.utils.reference_resolver import (
    ReferenceFieldConfig,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
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


# Chunked transform+post stage. Override with IFDB_DIAGNOSE_PIPELINE_CHUNK_SIZE.
# Default sized for monthly partitions where building all Conditions in one
# Pydantic list previously risked OOM.
DIAGNOSE_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.condition.diagnose.transform_post",
    env_prefix="IFDB_DIAGNOSE_PIPELINE",
    default_chunk_size=25_000,
    cleanup_resource_types={"Condition"},
)


def _fetch_diagnose_tillaegskoder(
    diagnose_ids: list[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch DIAGNOSE_TILLAEGSKODE grouped by DIAGNOSE_ID, sorted by TILLAEGSKODE_KODE."""
    result = chunked_in_fetch_grouped(
        diagnose_ids,
        table="IFDB.DIAGNOSE_TILLAEGSKODE",
        columns=["DIAGNOSE_ID", "TILLAEGSKODE_KODE", "TILLAEGSKODE_KODETEKST"],
        where_column="DIAGNOSE_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
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
                        system="http://terminology.hl7.org/CodeSystem/condition-clinical",
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
                    system="http://terminology.hl7.org/CodeSystem/condition-ver-status",
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
        condition = Condition(
            identifier=[Identifier(system=SYSTEM_IFDB_DIAGNOSE, value=oid)] if oid else None,
            extension=[build_is_deleted_extension(deleted_int)],
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
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post DIAGNOSE conditions to FHIR database."""
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    for cond in conditions:
        if cond.subject and getattr(cond.subject, "identifier", None):
            patient_ids_needed.add(str(cond.subject.identifier.value))

    if resolve_missing_dependencies and ifdb_oracle and patient_ids_needed:
        from DSA.FHIR.utils.ifdb.dependency_resolver import resolve_missing_patients

        logger.info(f"[DEPENDENCY] Ensuring {len(patient_ids_needed)} patients exist")
        resolve_missing_patients(
            patient_ids=patient_ids_needed,
            ifdb_oracle=ifdb_oracle,
            fhir_database=fhir_database,
            context=context,
        )

    conditions = prepare_resources_for_upsert(
        resources=conditions,
        fhir_database=fhir_database,
        resource_type="Condition",
        preferred_id_systems=[SYSTEM_IFDB_DIAGNOSE],
        reference_configs=DIAGNOSE_REFERENCE_CONFIGS,
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


def _run_diagnose_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Chunked transform+post pipeline shared by all DIAGNOSE assets.

    Daily, monthly and bulk Condition assets all funnel through this wrapper
    so chunking, cache hygiene and metadata aggregation live in one place.
    Behaviour matches the pre-chunk monolithic path: tillægskoder are fetched
    once for the full partition, then each chunk is transformed and posted in
    isolation. The post helper still owns dependency resolution, reference
    resolution, ID resolution and direct DB upsert.
    """

    def setup(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
        diagnose_ids = [
            normalize_oracle_id(row["OBJEKT_ID"]) for row in rows if row.get("OBJEKT_ID")
        ]
        return _fetch_diagnose_tillaegskoder(diagnose_ids, ifdb_oracle, context)

    def transform(
        chunk: list[dict[str, Any]],
        tillaegskoder: dict[str, list[dict[str, Any]]],
    ) -> list[Condition]:
        return _transform_diagnose_to_fhir(chunk, context, tillaegskoder)

    def post(
        conditions: list[Condition],
        chunk: list[dict[str, Any]],
        _tillaegskoder: dict[str, list[dict[str, Any]]],
    ) -> dg.MaterializeResult:
        return _post_diagnose_to_fhir(
            context=context,
            conditions=conditions,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        DIAGNOSE_CHUNK_STAGE,
        rows=extracted_data,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


@dg.asset(
    name="fhir_diagnose_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "diagnose_daily"])),
    },
    deps=["fhir_server_patient_dfm"],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
)
def fhir_diagnose_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post DIAGNOSE Condition resources for daily partition."""
    return _run_diagnose_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


@dg.asset(
    name="fhir_diagnose_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "diagnose_monthly"])),
    },
    deps=["fhir_server_patient_dfm"],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "8"},
)
def fhir_diagnose_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post DIAGNOSE Condition resources for monthly partition."""
    return _run_diagnose_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )


@dg.asset(
    name="fhir_diagnose_bulk_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "diagnose_full_load"])),
    },
    deps=["fhir_server_patient_dfm"],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "16"},
)
def fhir_diagnose_bulk_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post DIAGNOSE Condition resources for bulk load."""
    return _run_diagnose_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
