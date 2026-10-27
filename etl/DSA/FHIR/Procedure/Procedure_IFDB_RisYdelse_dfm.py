# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR Procedure asset for IFDB RIS_YDELSE data (DFM) — lossless mapping.

Single-asset pattern (mirrors ``Condition_IFDB_Diagnose_dfm``): fetch from
``arc.ifdb_ris_ydelse`` in batches (XML CLOB stored verbatim in the archive),
transform to FHIR Procedure, and post to the FHIR server. ALL source columns
are preserved via standard fields and extensions, and procedure granularity is
derived from XML <service> groups (one-to-many per OBJEKT_ID).

The asset never validates strictly: unknown STATE values and rows without
service groups are skipped, counted and reported in the run's metadata.

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
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.codings import build_unique_ordered_codings
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import (
    TAG_CHUNK_SIZE,
    TAG_MAX_WORKERS,
)
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import (
    extract_ris_services_from_xml,
)
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import (
    str_or_none as _str_or_none,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import (
    normalize_oracle_id,
    parse_clob_datetime_safe,
)
from DSA.FHIR.utils.ifdb.xml_child_reconcile import FAMILY_RIS_PROCEDURE, reconcile_ris_xml_children
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
from fhir.resources.R4B.procedure import Procedure, ProcedurePerformer
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_RIS_YDELSE_LOCATION_META,
    EXTENSION_RIS_YDELSE_SOURCE_META,
    EXTENSION_RIS_YDELSE_XML,
)
from utils.fhir.identifier_systems import (
    PERFORMER_FUNCTION_DISPLAYS,
    PERFORMER_FUNCTION_STAMAFDELING,
    PERFORMER_FUNCTION_UDFOERENDE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_PERFORMER_FUNCTION,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension
from utils.fhir.resource_flags import FLAG_LPR3_IGNORED, build_flag_meta

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


def _build_organization_performer(sorcode: str, funktion: str) -> ProcedurePerformer:
    """Organisation som performer med rolle (udførende enhed eller stamafdeling)."""
    return ProcedurePerformer(
        actor=Reference(
            type="Organization",
            identifier=Identifier(system=SYSTEM_SOR, value=sorcode),
        ),
        function=CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_PERFORMER_FUNCTION,
                    code=funktion,
                    display=PERFORMER_FUNCTION_DISPLAYS[funktion],
                )
            ],
            text=PERFORMER_FUNCTION_DISPLAYS[funktion],
        ),
    )


RIS_YDELSE_NESTED_REFERENCE_CONFIGS = [
    # Begge organisationer (udførende enhed, stamafdeling) opløses til
    # Organization/<id>, så de kan joines i den flade tabel.
    NestedReferenceFieldConfig(
        field_path="performer.actor",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
        },
        is_required=False,
        description="Udførende enhed og stamafdeling (SOR) som Organization-referencer",
    ),
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
            "based on. Resolved to a literal reference when the legacy RIS "
            "ServiceRequest pipeline has posted it; optional so the identifier "
            "reference is kept if it cannot resolve yet (heals on next touch)."
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

# Counted with the skipped STATE values; the parentheses keep it out of the unknown ones.
_NO_SERVICE_GROUPS = "(no service groups)"


# RIS payloads carry full XML in extensions, so batches are sized smaller than
# Diagnose/AdminProc to keep peak memory bounded.
BATCH_SIZE = 5_000


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
    skipped_total: dict[str, int] | None = None,
) -> list[Procedure]:
    """Transform RIS_YDELSE rows to FHIR Procedure resources.

    Lossless mapping: all 25 source columns are mapped to standard FHIR fields
    or custom extensions. Raw XML content is preserved in risYdelseXml extension.

    When ``strict_source_validation`` is enabled, unexpected states and invalid
    XML fail the transform instead of being skipped.

    ``skipped_total`` akkumulerer på tværs af batches, så asset'et kan lægge
    tallene i sin metadata. Uden den tælles der stadig, bare kun pr. batch.
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

        # Only include finalized/modified reports. NULL/blank STATE is not in
        # the kravspec IN-list and is skipped like known non-final states.
        # Strict mode only fires on genuinely unknown STATE values.
        if not state or state.upper() not in ALLOWED_RIS_STATES:
            upper = state.upper() if state else "(blank)"
            if (
                upper != "(blank)"
                and upper not in KNOWN_NON_FINAL_RIS_STATES
                and strict_source_validation
            ):
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
            skipped_states[_NO_SERVICE_GROUPS] = skipped_states.get(_NO_SERVICE_GROUPS, 0) + 1
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

            code_codings = build_unique_ordered_codings(
                system=SYSTEM_SKS,
                primary_code=primary_code_value,
                supplementary_rows=[{"kode": c} for c in group.get("tillaegskoder", [])],
                supplement_code_key="kode",
                supplement_display_key="display",
            )

            procedure_code = CodeableConcept(
                coding=code_codings,
                text=primary_code_value,
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

            identifier_value = (
                FAMILY_RIS_PROCEDURE.child_value(objekt_id, group["element_nummer"])
                if objekt_id
                else None
            )

            # Udførende enhed og stamafdeling er to forskellige organisationer og
            # står som hver sin performer med funktion, så den flade tabel kan
            # skelne dem. Tidligere faldt vi tilbage til stamafdelingen, når den
            # udførende enhed manglede, og blandede dermed de to sammen.
            performer_list = []
            for felt, funktion in (
                ("UDFOERT_ADMENHED_SORCODE", PERFORMER_FUNCTION_UDFOERENDE),
                ("STAMAFD_SORCODE", PERFORMER_FUNCTION_STAMAFDELING),
            ):
                sorcode = _str_or_none(row_dict.get(felt))
                if sorcode:
                    performer_list.append(_build_organization_performer(sorcode, funktion))

            # Driftsflag som meta.tag: kun dermed kan der soeges paa flaget via
            # FHIR-serveren og filtreres paa det som kolonne i de flade tabeller.
            flag_koder = [FLAG_LPR3_IGNORED] if _is_true_flag(row_dict.get("LPR3_IGNORED")) else []
            proc = Procedure(
                meta=build_flag_meta(flag_koder),
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
            f"with non-final states or no service groups: {skipped_states}"
        )
        # De ukendte er dem, værnet ellers ville være væltet på. De skal frem i
        # lyset ved navn og antal, også når vi ikke stopper kørslen på dem.
        unknown = {
            state: count
            for state, count in skipped_states.items()
            if not state.startswith("(") and state not in KNOWN_NON_FINAL_RIS_STATES
        }
        if unknown:
            context.log.warning(
                f"RIS_YDELSE: unknown STATE values skipped (not in the allowed or "
                f"known non-final lists): {unknown}. Present them to the domain "
                f"experts; extend KNOWN_NON_FINAL_RIS_STATES if they are expected."
            )
        if skipped_total is not None:
            for state, count in skipped_states.items():
                skipped_total[state] = skipped_total.get(state, 0) + count

    return procedures


def _post_ris_ydelse_to_fhir(
    context: dg.AssetExecutionContext,
    procedures: list[Procedure],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    reference_fallback_fn: ResolveValuesFn | None = None,
) -> dg.MaterializeResult:
    """Post RIS_YDELSE procedures to FHIR database.

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
        preferred_id_systems=[SYSTEM_IFDB_RIS_YDELSE],
        reference_configs=RIS_YDELSE_REFERENCE_CONFIGS,
        nested_reference_configs=RIS_YDELSE_NESTED_REFERENCE_CONFIGS,
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
    name="fhir_server_procedure_ris_ydelse_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_ris_ydelse_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
        # Ordering, not data: basedOn → ServiceRequest must be posted first.
        # Without this dep the base job may post the Procedure before its SR,
        # and in a delta pipeline the reference only heals if the RIS source
        # row is touched again — potentially never.
        "fhir_server_service_request_dfm",
    ],
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
    description=(
        "Fetch RIS-ydelser from arc.ifdb_ris_ydelse in batches, transform to "
        "FHIR Procedure (granularity from XML <service> groups), and post to "
        "the FHIR server. Checkpoint owner for fhir_server.Procedure_RisYdelse. "
        "Runs after fhir_server_service_request_dfm so basedOn → ServiceRequest "
        "resolves in the same base-job run."
    ),
)
def fhir_server_procedure_ris_ydelse_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for RIS_YDELSE Procedure resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "Procedure_RisYdelse",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    # Oversprungne STATE-værdier samlet for hele kørslen, så de kan rapporteres.
    skipped_total: dict[str, int] = {}

    try:
        # include_deleted=True so deleted RIS-ydelser are tomb-stoned (isDeleted
        # extension updated) rather than left active.
        query = build_fhir_archive_query(
            "arc.ifdb_ris_ydelse", run_ctx.checkpoint, config, include_deleted=True
        )
        context.log.info(f"Querying arc.ifdb_ris_ydelse (mode={config.mode})")

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
                        f"Batch {batch_num}: processing {len(batch)} RIS-ydelser "
                        f"(total so far: {total_fetched})"
                    )

                    procedures = _transform_ris_ydelse_to_fhir(
                        batch,
                        context,
                        skipped_total=skipped_total,
                    )
                    if procedures:
                        _post_ris_ydelse_to_fhir(
                            context=context,
                            procedures=procedures,
                            fhir_database=fhir_database,
                            extracted_count=len(batch),
                            mssql=None,
                            resolve_missing_dependencies=False,
                            reference_fallback_fn=patient_cpr_fallback,
                        )
                        total_posted += len(procedures)

                    # Uden for procedures-guarden med vilje: en ydelse, der er
                    # forsvundet fra XML'en, giver netop INGEN procedure, og det
                    # er praecis den, der skal pensioneres.
                    reconcile_ris_xml_children(
                        fhir_database=fhir_database,
                        rows=batch,
                        procedures=procedures,
                        logger=context.log,
                    )
            finally:
                cursor.close()

        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted in {batch_num} batches"
        )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "Procedure_RisYdelse", config, total_fetched
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        unknown_states = {
            state: count
            for state, count in skipped_total.items()
            if not state.startswith("(") and state not in KNOWN_NON_FINAL_RIS_STATES
        }
        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "fetched_count": total_fetched,
                "batch_count": batch_num,
                "mode": config.mode,
                "skipped_rows": sum(skipped_total.values()),
                "skipped_states": str(skipped_total or "-"),
                "unknown_states": str(unknown_states or "ingen"),
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
