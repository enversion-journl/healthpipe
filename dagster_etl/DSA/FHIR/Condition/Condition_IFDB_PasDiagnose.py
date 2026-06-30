# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Condition assets for IFDB PAS_DIAGNOSE data.

Transforms PAS (Patientadministrativt System) encounter-level diagnosis data into
FHIR Condition resources. Distinct from Condition_IFDB_Diagnose.py which handles
clinician-level DIAGNOSE records.

Inline lookups (partition-scoped, chunked IN):
    - PAS_DIAGNOSE_ENCOUNTER_REF  → Condition.encounter
    - PAS_DIAGNOSE_TILLAEGSKODE   → Condition.code.coding[] (supplementary codes)
    - PAS_DIAGNOSE_HENVISNING_REF → source-metadata extension (ServiceRequest scope)

Reference Resolution:
    Uses declarative configuration with ReferenceFieldConfig for:
    - Patient (subject) - required
    - Encounter (encounter) - optional
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
from DSA.FHIR.utils.ifdb.oracle_lookup import (
    chunked_in_fetch,
    chunked_in_fetch_grouped,
)
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
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_PAS_DIAGNOSE_SOURCE_META,
    EXTENSION_SOURCE_MODIFIED_AT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
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


# Chunked transform+post stage. Override with IFDB_PAS_DIAGNOSE_PIPELINE_CHUNK_SIZE.
PAS_DIAGNOSE_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.condition.pas_diagnose.transform_post",
    env_prefix="IFDB_PAS_DIAGNOSE_PIPELINE",
    default_chunk_size=25_000,
    cleanup_resource_types={"Condition"},
)

# ---------------------------------------------------------------------------
# Inline lookup helpers (partition-scoped, chunked IN)
# ---------------------------------------------------------------------------


def _fetch_pas_diagnose_encounter_refs(
    diagnose_ids: list[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, str]:
    """Return ``DIAGNOSE_ID → first ENCOUNTER_ID`` for the given diagnoses."""
    rows = chunked_in_fetch(
        diagnose_ids,
        table="IFDB.PAS_DIAGNOSE_ENCOUNTER_REF",
        columns=["DIAGNOSE_ID", "ENCOUNTER_ID"],
        where_column="DIAGNOSE_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
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
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch PAS_DIAGNOSE_TILLAEGSKODE grouped by PAS_DIAGNOSE_ID."""
    result = chunked_in_fetch_grouped(
        diagnose_ids,
        table="IFDB.PAS_DIAGNOSE_TILLAEGSKODE",
        columns=["PAS_DIAGNOSE_ID", "TILLAEGSKODE_KODE", "TILLAEGSKODE_KODETEKST"],
        where_column="PAS_DIAGNOSE_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
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
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[str]]:
    """Return ``DIAGNOSE_ID → list[HENVISNING_ID]`` (source-metadata only)."""
    grouped = chunked_in_fetch_grouped(
        diagnose_ids,
        table="IFDB.PAS_DIAGNOSE_HENVISNING_REF",
        columns=["DIAGNOSE_ID", "HENVISNING_ID"],
        where_column="DIAGNOSE_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
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

        # Source metadata: diagnosetype, modificeret_* fields, henvisning_refs
        meta_parts: list[Extension] = []
        if diagnosetype:
            meta_parts.append(
                Extension.construct(url="diagnosetype", valueString=str(diagnosetype))
            )
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
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post PAS_DIAGNOSE conditions to FHIR database."""
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    encounter_ids_needed: set[str] = set()
    for cond in conditions:
        if cond.subject and getattr(cond.subject, "identifier", None):
            patient_ids_needed.add(str(cond.subject.identifier.value))
        if cond.encounter and getattr(cond.encounter, "identifier", None):
            encounter_ids_needed.add(str(cond.encounter.identifier.value))

    if resolve_missing_dependencies and ifdb_oracle:
        from DSA.FHIR.utils.ifdb.dependency_resolver import (
            resolve_missing_encounters,
            resolve_missing_patients,
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

    conditions = prepare_resources_for_upsert(
        resources=conditions,
        fhir_database=fhir_database,
        resource_type="Condition",
        preferred_id_systems=[SYSTEM_IFDB_PAS_DIAGNOSE],
        reference_configs=PAS_DIAGNOSE_REFERENCE_CONFIGS,
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
# Shared lookup + transform + post helper
# ---------------------------------------------------------------------------


def _run_pas_diagnose_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Chunked transform+post pipeline shared by all PAS_DIAGNOSE assets.

    Behaviour matches the pre-chunk monolithic path:
    - Encounter refs, tillægskoder and henvisning refs are fetched once for
      the full partition via setup_fn.
    - Each chunk is transformed using those shared lookups and posted via
      _post_pas_diagnose_to_fhir, which still owns dependency resolution,
      reference resolution, ID resolution and direct DB upsert.
    """

    def setup(rows: list[dict[str, Any]]) -> tuple[
        dict[str, str],
        dict[str, list[dict[str, Any]]],
        dict[str, list[str]],
    ]:
        diagnose_ids = [
            normalize_oracle_id(row["OBJEKT_ID"]) for row in rows if row.get("OBJEKT_ID")
        ]
        encounter_refs = _fetch_pas_diagnose_encounter_refs(diagnose_ids, ifdb_oracle, context)
        tillaegskoder = _fetch_pas_diagnose_tillaegskoder(diagnose_ids, ifdb_oracle, context)
        henvisning_refs = _fetch_pas_diagnose_henvisning_refs(diagnose_ids, ifdb_oracle, context)
        return encounter_refs, tillaegskoder, henvisning_refs

    def transform(
        chunk: list[dict[str, Any]],
        lookups: tuple[
            dict[str, str],
            dict[str, list[dict[str, Any]]],
            dict[str, list[str]],
        ],
    ) -> list[Condition]:
        encounter_refs, tillaegskoder, henvisning_refs = lookups
        return _transform_pas_diagnose_to_fhir(
            chunk,
            context,
            encounter_refs,
            tillaegskoder,
            henvisning_refs,
        )

    def post(
        conditions: list[Condition],
        chunk: list[dict[str, Any]],
        _lookups: tuple[
            dict[str, str],
            dict[str, list[dict[str, Any]]],
            dict[str, list[str]],
        ],
    ) -> dg.MaterializeResult:
        return _post_pas_diagnose_to_fhir(
            context=context,
            conditions=conditions,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        PAS_DIAGNOSE_CHUNK_STAGE,
        rows=extracted_data,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_pas_diagnose_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "pas_diagnose_daily"])),
    },
    deps=["fhir_server_patient_dfm", "fhir_server_encounter_dfm"],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
)
def fhir_pas_diagnose_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post PAS_DIAGNOSE Condition resources for daily partition."""
    return _run_pas_diagnose_pipeline(context, extracted_data, ifdb_oracle, fhir_database)


@dg.asset(
    name="fhir_pas_diagnose_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "pas_diagnose_monthly"])),
    },
    deps=["fhir_server_patient_dfm", "fhir_server_encounter_dfm"],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "8"},
)
def fhir_pas_diagnose_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post PAS_DIAGNOSE Condition resources for monthly partition."""
    return _run_pas_diagnose_pipeline(context, extracted_data, ifdb_oracle, fhir_database)


@dg.asset(
    name="fhir_pas_diagnose_bulk_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "pas_diagnose_full_load"])),
    },
    deps=["fhir_server_patient_dfm", "fhir_server_encounter_dfm"],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "16"},
)
def fhir_pas_diagnose_bulk_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post PAS_DIAGNOSE Condition resources for bulk load."""
    return _run_pas_diagnose_pipeline(context, extracted_data, ifdb_oracle, fhir_database)
