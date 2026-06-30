# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Procedure assets for IFDB RIS_YDELSE data — lossless mapping.

Transforms extracted RIS (Radiology Information System) records into FHIR
Procedure resources. ALL source columns are preserved via standard fields and
extensions, and procedure granularity is derived from XML <service> groups
(one-to-many per OBJEKT_ID).

Standard FHIR fields:
    - Procedure.identifier <- OBJEKT_ID:ElementNummer
    - Procedure.subject <- PATIENT_ID (required)
    - Procedure.encounter <- ENCOUNTER_ID (optional)
    - Procedure.code <- XML <service><kode> (primær SKS-kode)
    - Procedure.category <- fixed "radiology" coding
    - Procedure.status <- completed
    - Procedure.performedDateTime <- UDFOERT_TIDSPUNKT

Extensions:
    - isDeleted <- SLETTET (boolean)
    - episodeOfCare <- EPISODE_OF_CARE_ID
    - modtidspunkt <- MODTIDSPUNKT
    - sourceCreatedAt <- (derived from MODTIDSPUNKT)
    - risYdelseSourceMeta <- TILSTEDEVAERELSE_ID, BESKED_ID, FILNAVN, STATE,
      TYPE, RTEE, IGNORED, LPR3_IGNORED, OPDATERET_TID, ElementNummer,
      primaryCode, tillaegskode[]
    - risYdelseLocationMeta <- STAMAFD_*, UDFOERT_ADMENHED_*
    - risYdelseXml <- XML (raw XML preservation)
    - performedPeriod via extension when PERIOD_START/PERIOD_END differ from
      UDFOERT_TIDSPUNKT

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
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import (
    extract_ris_services_from_xml,
)
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import str_or_none as _str_or_none
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import (
    normalize_oracle_id,
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
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_RIS_YDELSE_LOCATION_META,
    EXTENSION_RIS_YDELSE_SOURCE_META,
    EXTENSION_RIS_YDELSE_XML,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

SYSTEM_RIS_CATEGORY = "http://example.org/fhir/CodeSystem/procedure-category"

RIS_YDELSE_REFERENCE_CONFIGS = [
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

RIS_YDELSE_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        extension_url=EXTENSION_EPISODE_OF_CARE,
        is_required=False,
        description="Episode of care reference from extension",
    ),
    NestedReferenceFieldConfig(
        field_path="basedOn",
        identifier_system_to_type={
            SYSTEM_IFDB_RIS_YDELSE: "ServiceRequest",
        },
        is_required=False,
        description=(
            "ServiceRequest (RIS request/order) the Procedure was performed "
            "based on. Resolved to a literal reference; the RIS ServiceRequest "
            "asset is an upstream dep, so it exists by the time Procedure runs. "
            "Optional so the identifier reference is kept if it cannot resolve."
        ),
    ),
]

ALLOWED_RIS_STATES = {"FINAL_REPORT", "MODIFIED_REPORT"}

# Known non-final RIS STATE values from the source. These represent
# booking/workflow stages rather than a completed radiology report and
# must be skipped (no FHIR Procedure is appropriate). They are *expected*
# and should NOT trigger strict-source-validation raises — the strict
# mode is meant to fire on genuinely unknown STATE values so we notice
# new source shapes, not on routinely-seen non-final rows.
KNOWN_NON_FINAL_RIS_STATES = {
    "BOOKED",
    "ORDERED",
    "ORDERED_AWAITING_ACK",
    "EXECUTED",
    # Preliminary reports are interim radiology notes, not final
    # sign-offs — the FHIR Procedure is only meaningful once the report
    # is FINAL_REPORT/MODIFIED_REPORT, so we skip earlier states silently.
    "PRELIMINARY_REPORT",
    # REJECTED rows are submitted-and-withdrawn workflow entries; they
    # never produce a completed radiology procedure and must also skip.
    "REJECTED",
}


# Chunked transform+post stage. Override with IFDB_RIS_YDELSE_PIPELINE_CHUNK_SIZE.
# RIS payloads carry full XML in extensions, so chunks are sized smaller than
# Diagnose/AdminProc to keep peak memory bounded.
RIS_YDELSE_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.procedure.ris_ydelse.transform_post",
    env_prefix="IFDB_RIS_YDELSE_PIPELINE",
    default_chunk_size=15_000,
    cleanup_resource_types={"Procedure"},
)


# ---------------------------------------------------------------------------
# Lossless extensions builders
# ---------------------------------------------------------------------------


def _build_ris_source_meta_extensions(
    row_dict: dict[str, Any], service_group: dict[str, Any] | None = None
) -> list[Extension]:
    """Build risYdelseSourceMeta extension with all unmapped source fields."""
    parts: list[Extension] = []

    def _add(key: str, val: Any) -> None:
        s = _str_or_none(val)
        if s:
            parts.append(Extension.construct(url=key, valueString=s))

    _add("tilstedevaerselseId", row_dict.get("TILSTEDEVAERELSE_ID"))
    _add("beskedId", row_dict.get("BESKED_ID"))
    _add("filnavn", row_dict.get("FILNAVN"))
    _add("state", row_dict.get("STATE"))
    _add("type", row_dict.get("TYPE"))
    _add("rtee", row_dict.get("RTEE"))

    opdateret = parse_clob_datetime_safe(row_dict.get("OPDATERET_TID"))
    if opdateret:
        parts.append(Extension.construct(url="opdateretTid", valueString=opdateret))

    period_start = parse_clob_datetime_safe(row_dict.get("PERIOD_START"))
    if period_start:
        parts.append(Extension.construct(url="periodStart", valueString=period_start))

    period_end = parse_clob_datetime_safe(row_dict.get("PERIOD_END"))
    if period_end:
        parts.append(Extension.construct(url="periodEnd", valueString=period_end))

    ignored = row_dict.get("IGNORED")
    if ignored is not None:
        parts.append(Extension.construct(url="ignored", valueBoolean=_is_true_flag(ignored)))

    lpr3 = row_dict.get("LPR3_IGNORED")
    if lpr3 is not None:
        parts.append(Extension.construct(url="lpr3Ignored", valueBoolean=_is_true_flag(lpr3)))

    if service_group:
        element_nummer = service_group.get("element_nummer")
        if element_nummer is not None:
            parts.append(Extension.construct(url="elementNummer", valueInteger=int(element_nummer)))
        _add("primaryCode", service_group.get("primary_code"))
        for extra_code in service_group.get("tillaegskoder", []):
            _add("tillaegskode", extra_code)

    if not parts:
        return []
    return [Extension.construct(url=EXTENSION_RIS_YDELSE_SOURCE_META, extension=parts)]


def _build_ris_location_meta_extensions(row_dict: dict[str, Any]) -> list[Extension]:
    """Build risYdelseLocationMeta extension with location columns."""
    parts: list[Extension] = []

    def _add(key: str, val: Any) -> None:
        s = _str_or_none(val)
        if s:
            parts.append(Extension.construct(url=key, valueString=s))

    _add("stamafdelingKode", row_dict.get("STAMAFD_KODE"))
    _add("stamafdelingNavn", row_dict.get("STAMAFD_NAVN"))
    _add("stamafdelingOrgkode", row_dict.get("STAMAFD_ORGKODE"))
    _add("stamafdelingSorcode", row_dict.get("STAMAFD_SORCODE"))
    _add("udfoertEnhedKode", row_dict.get("UDFOERT_ADMENHED_KODE"))
    _add("udfoertEnhedNavn", row_dict.get("UDFOERT_ADMENHED_NAVN"))
    _add("udfoertEnhedOrgkode", row_dict.get("UDFOERT_ADMENHED_ORGKODE"))
    _add("udfoertEnhedSorcode", row_dict.get("UDFOERT_ADMENHED_SORCODE"))

    if not parts:
        return []
    return [Extension.construct(url=EXTENSION_RIS_YDELSE_LOCATION_META, extension=parts)]


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------


def _transform_ris_ydelse_to_fhir(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    *,
    strict_source_validation: bool = False,
) -> list[Procedure]:
    """Transform RIS_YDELSE rows to FHIR Procedure resources.

    Lossless mapping: all 25 source columns are mapped to standard FHIR fields
    or custom extensions. Raw XML content is preserved in risYdelseXml extension.

    When ``strict_source_validation`` is enabled, unexpected states and invalid
    XML fail the transform instead of being silently skipped. Daily/monthly
    assets use strict mode; bulk keeps the tolerant behavior for backfills.
    """
    procedures: list[Procedure] = []
    skipped_states: dict[str, int] = {}

    for row_dict in rows:
        # Normalize OBJEKT_ID up front: Oracle NUMBER columns arrive as floats,
        # and the raw value feeds both the Procedure identifier and the
        # ServiceRequest basedOn key. The ServiceRequest pipeline normalizes the
        # same OBJEKT_ID, so both sides must share the normalized value-space or
        # the basedOn -> ServiceRequest link silently fails to resolve.
        objekt_id = normalize_oracle_id(row_dict.get("OBJEKT_ID"))
        patient_id = row_dict.get("PATIENT_ID")
        encounter_id = row_dict.get("ENCOUNTER_ID")
        episode_id = row_dict.get("EPISODE_OF_CARE_ID")
        udfoert_tidspunkt = row_dict.get("UDFOERT_TIDSPUNKT")
        slettet = row_dict.get("SLETTET")
        modtidspunkt = row_dict.get("MODTIDSPUNKT")
        xml_raw = row_dict.get("XML")
        state = _str_or_none(row_dict.get("STATE"))

        # Only include finalized/modified reports. Known non-final states
        # (BOOKED, ORDERED, ...) are skipped silently even in strict mode —
        # they are part of normal source traffic. Strict mode only fires on
        # genuinely unknown STATE values so new source shapes surface loudly.
        if state and state.upper() not in ALLOWED_RIS_STATES:
            upper = state.upper()
            if upper not in KNOWN_NON_FINAL_RIS_STATES and strict_source_validation:
                raise ValueError(f"RIS_YDELSE row {objekt_id} has unsupported STATE={state!r}")
            skipped_states[upper] = skipped_states.get(upper, 0) + 1
            continue

        # One RIS row may contain multiple procedure groups in XML.
        service_groups = extract_ris_services_from_xml(
            xml_raw,
            objekt_id=objekt_id,
            context=context,
            strict_source_validation=strict_source_validation,
        )
        if not service_groups:
            if strict_source_validation:
                raise ValueError(f"RIS_YDELSE row {objekt_id} produced no service groups from XML")
            continue

        deleted_int = int(_is_true_flag(slettet))
        is_deleted = deleted_int != 0
        status = "entered-in-error" if is_deleted else "completed"

        for group in service_groups:
            primary_code = group.get("primary_code")
            if not primary_code:
                continue
            primary_code_value = normalize_sks_coding_code(primary_code)
            if not primary_code_value:
                continue

            procedure_code = CodeableConcept(
                coding=[
                    Coding(
                        system=SYSTEM_SKS,
                        code=str(primary_code_value),
                    )
                ],
                text=str(primary_code),
            )

            category = CodeableConcept(
                coding=[
                    Coding(
                        system=SYSTEM_RIS_CATEGORY,
                        code="radiology",
                        display="Radiology",
                    )
                ]
            )

            p_start = parse_clob_datetime_safe(row_dict.get("PERIOD_START"))
            p_end = parse_clob_datetime_safe(row_dict.get("PERIOD_END"))
            performed_dt = None
            performed_period = None
            if p_start or p_end:
                performed_period = Period.construct(
                    start=p_start or None,
                    end=p_end or None,
                )
            else:
                performed_dt = parse_clob_datetime_safe(udfoert_tidspunkt)

            extensions: list[Extension] = []

            extensions.append(build_is_deleted_extension(deleted_int))

            if episode_id:
                extensions.append(
                    Extension(
                        url=EXTENSION_EPISODE_OF_CARE,
                        valueReference=Reference(
                            identifier=Identifier(
                                system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                                value=normalize_oracle_id(episode_id),
                            )
                        ),
                    )
                )

            modtidspunkt_dt = parse_clob_datetime_safe(modtidspunkt)
            if modtidspunkt_dt:
                extensions.append(
                    Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt_dt)
                )

            if xml_raw:
                xml_str = str(xml_raw).strip()
                if xml_str:
                    extensions.append(
                        Extension.construct(url=EXTENSION_RIS_YDELSE_XML, valueString=xml_str)
                    )

            extensions.extend(_build_ris_source_meta_extensions(row_dict, group))
            extensions.extend(_build_ris_location_meta_extensions(row_dict))

            identifier_value = f"{objekt_id}:{group['element_nummer']}" if objekt_id else None

            performer_list = []
            udfoert_sorcode = _str_or_none(row_dict.get("UDFOERT_ADMENHED_SORCODE"))
            if not udfoert_sorcode:
                udfoert_sorcode = _str_or_none(row_dict.get("STAMAFD_SORCODE"))
            if udfoert_sorcode:
                from fhir.resources.R4B.procedure import ProcedurePerformer

                performer_list.append(
                    ProcedurePerformer(
                        actor=Reference(
                            identifier=Identifier(
                                system=SYSTEM_SOR,
                                value=str(udfoert_sorcode),
                            ),
                        ),
                    )
                )

            proc = Procedure(
                identifier=(
                    [Identifier(system=SYSTEM_IFDB_RIS_YDELSE, value=identifier_value)]
                    if identifier_value
                    else None
                ),
                basedOn=(
                    [
                        Reference(
                            type="ServiceRequest",
                            identifier=Identifier(
                                system=SYSTEM_IFDB_RIS_YDELSE,
                                value=identifier_value,
                            ),
                        )
                    ]
                    if identifier_value
                    else None
                ),
                status=status,
                category=category,
                code=procedure_code,
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
                encounter=(
                    Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_OBJECT_ID,
                            value=normalize_oracle_id(encounter_id),
                        )
                    )
                    if encounter_id
                    else None
                ),
                performedDateTime=performed_dt,
                performedPeriod=performed_period,
                performer=performer_list if performer_list else None,
                extension=extensions if extensions else None,
            )
            procedures.append(proc)

    if skipped_states:
        context.log.info(
            f"Skipped {sum(skipped_states.values())} RIS_YDELSE rows "
            f"with non-final states: {skipped_states}"
        )

    return procedures


def _post_ris_ydelse_to_fhir(
    context: dg.AssetExecutionContext,
    procedures: list[Procedure],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    """Post RIS_YDELSE procedures to FHIR database."""
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
        preferred_id_systems=[SYSTEM_IFDB_RIS_YDELSE],
        reference_configs=RIS_YDELSE_REFERENCE_CONFIGS,
        nested_reference_configs=RIS_YDELSE_NESTED_REFERENCE_CONFIGS,
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


def _run_ris_ydelse_pipeline(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    *,
    strict_source_validation: bool,
) -> dg.MaterializeResult:
    """Chunked transform+post pipeline shared by all RIS_YDELSE assets.

    The strict_source_validation flag is forwarded into each chunk's transform
    so daily/monthly partitions still raise on unknown STATE values or
    unparseable XML, while bulk/full_load tolerates them. Behaviour preserved:

    - Skipped rows (non-final STATE, unparseable XML) are dropped *inside*
      each chunk's _transform_ris_ydelse_to_fhir call. Only the kept
      Procedures reach _post_ris_ydelse_to_fhir.
    - Dependency resolution (Patient/Encounter/EpisodeOfCare) therefore still
      operates on the transformed Procedure list, never on raw rows.
      ``test_ris_ydelse_skipped_dependency_pin`` pins this guarantee.
    - No partition-level setup is required, so setup_fn is None.
    """

    def transform(chunk: list[dict[str, Any]], _setup: None) -> list[Procedure]:
        return _transform_ris_ydelse_to_fhir(
            chunk,
            context,
            strict_source_validation=strict_source_validation,
        )

    def post(
        procedures: list[Procedure],
        chunk: list[dict[str, Any]],
        _setup: None,
    ) -> dg.MaterializeResult:
        return _post_ris_ydelse_to_fhir(
            context=context,
            procedures=procedures,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        RIS_YDELSE_CHUNK_STAGE,
        rows=extracted_data,
        setup_fn=None,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


@dg.asset(
    name="fhir_ris_ydelse_daily_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_daily"])),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
        "fhir_service_request_daily_ifdb",
    ],
    group_name="fhir_ifdb_daily",
    partitions_def=daily_partitions,
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
)
def fhir_ris_ydelse_daily_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post RIS_YDELSE Procedure resources for daily partition."""
    return _run_ris_ydelse_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        strict_source_validation=True,
    )


@dg.asset(
    name="fhir_ris_ydelse_monthly_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_monthly"])),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
        "fhir_service_request_monthly_ifdb",
    ],
    group_name="fhir_ifdb_monthly",
    partitions_def=monthly_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "8"},
)
def fhir_ris_ydelse_monthly_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post RIS_YDELSE Procedure resources for monthly partition."""
    return _run_ris_ydelse_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        strict_source_validation=True,
    )


@dg.asset(
    name="fhir_ris_ydelse_bulk_ifdb",
    ins={
        "extracted_data": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_full_load"])),
    },
    deps=[
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
        "fhir_service_request_bulk_ifdb",
    ],
    group_name="fhir_ifdb_full_load",
    partitions_def=full_load_partitions,
    tags={TAG_CHUNK_SIZE: "1000", TAG_MAX_WORKERS: "16"},
)
def fhir_ris_ydelse_bulk_ifdb(
    context: dg.AssetExecutionContext,
    extracted_data: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post RIS_YDELSE Procedure resources for bulk load."""
    return _run_ris_ydelse_pipeline(
        context=context,
        extracted_data=extracted_data,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        strict_source_validation=False,
    )
