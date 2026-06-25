# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR ServiceRequest assets for IFDB HENVISNING and RIS_YDELSE data."""

from typing import Any

import dagster as dg
from core.parallel import ChunkedPipelineStage, run_chunked_transform_post
from core.partitions import daily_partitions, full_load_partitions, monthly_partitions
from core.resources import FHIRDatabaseResource, FHIRServerResource, IFDBOracleResource
from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.oracle_lookup import chunked_in_fetch_grouped
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import (
    extract_ris_services_from_xml,
    str_or_none,
)
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
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.servicerequest import ServiceRequest
from utils.fhir.fhir_extensions import (
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_HENVISNING_SOURCE_META,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_HENVISNING_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

SYSTEM_HENVISNING_TYPE = "urn:source:ifdb:henvisning:type"
SYSTEM_RIS_SERVICE_CATEGORY = "http://regionsyddanmark.dk/fhir/CodeSystem/service-request-category"
SYSTEM_RIS_TYPE = "urn:source:ifdb:ris_ydelse:type"

SERVICE_REQUEST_REFERENCE_CONFIGS = [
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
        description="Encounter reference from IFDB ENCOUNTER_ID",
    ),
    ReferenceFieldConfig(
        field_name="requester",
        target_resource_type="Organization",
        identifier_system=SYSTEM_SOR,
        is_required=False,
        description="Requester organization reference by SOR",
    ),
]

SERVICE_REQUEST_NESTED_REFERENCE_CONFIGS = [
    NestedReferenceFieldConfig(
        field_path="extension.valueReference",
        identifier_system_to_type={
            SYSTEM_IFDB_EPISODE_OF_CARE_ID: "EpisodeOfCare",
        },
        extension_url=EXTENSION_EPISODE_OF_CARE,
        is_required=False,
        description="EpisodeOfCare reference from extension",
    ),
    NestedReferenceFieldConfig(
        field_path="performer",
        identifier_system_to_type={
            SYSTEM_SOR: "Organization",
        },
        is_required=False,
        description="Requested performer organization reference by SOR",
    ),
    NestedReferenceFieldConfig(
        field_path="reasonReference",
        identifier_system_to_type={
            SYSTEM_IFDB_PAS_DIAGNOSE: "Condition",
        },
        is_required=False,
        description="Referral diagnosis reference from HENVISNING_HISTORIK.DIAGNOSE_ID",
    ),
]

SERVICE_REQUEST_HENVISNING_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.servicerequest.henvisning.transform_post",
    env_prefix="IFDB_SERVICE_REQUEST_HENVISNING_PIPELINE",
    default_chunk_size=25_000,
    cleanup_resource_types={"ServiceRequest"},
)

SERVICE_REQUEST_RIS_CHUNK_STAGE = ChunkedPipelineStage.from_env(
    stage_id="ifdb.servicerequest.ris_ydelse.transform_post",
    env_prefix="IFDB_SERVICE_REQUEST_RIS_YDELSE_PIPELINE",
    default_chunk_size=15_000,
    cleanup_resource_types={"ServiceRequest"},
)


def _add_string_part(parts: list[Extension], key: str, value: Any) -> None:
    s = str_or_none(value)
    if s:
        parts.append(Extension.construct(url=key, valueString=s))


def _add_bool_part(parts: list[Extension], key: str, value: Any) -> None:
    if value is not None:
        parts.append(Extension.construct(url=key, valueBoolean=_is_true_flag(value)))


def _add_datetime_part(parts: list[Extension], key: str, value: Any) -> None:
    dt = parse_clob_datetime_safe(value)
    if dt:
        parts.append(Extension.construct(url=key, valueDateTime=dt))


def _reference_by_identifier(
    *,
    resource_type: str | None,
    system: str,
    value: Any,
) -> Reference | None:
    normalized = normalize_oracle_id(value) if value is not None else None
    if not normalized:
        return None
    return Reference(
        type=resource_type,
        identifier=Identifier(system=system, value=normalized),
    )


def _reference_by_raw_identifier(
    *,
    resource_type: str | None,
    system: str,
    value: Any,
) -> Reference | None:
    raw = str_or_none(value)
    if not raw:
        return None
    return Reference(type=resource_type, identifier=Identifier(system=system, value=raw))


def _first_reference(*refs: Reference | None) -> Reference | None:
    for ref in refs:
        if ref is not None:
            return ref
    return None


def _map_henvisning_status(row: dict[str, Any]) -> str:
    if _is_true_flag(row.get("SLETTET")):
        return "entered-in-error"

    value = str_or_none(row.get("STATUS_KODE")) or str_or_none(row.get("STATUS_KODETEKST"))
    if not value:
        return "unknown"

    normalized = value.strip().lower()
    exact = {
        "draft": "draft",
        "active": "active",
        "on-hold": "on-hold",
        "revoked": "revoked",
        "completed": "completed",
        "entered-in-error": "entered-in-error",
        "unknown": "unknown",
    }
    if normalized in exact:
        return exact[normalized]

    return "unknown"


def _map_ris_status(row: dict[str, Any]) -> str:
    if _is_true_flag(row.get("SLETTET")):
        return "entered-in-error"

    state = (str_or_none(row.get("STATE")) or "").upper()
    if state in {"ORDERED", "ORDERED_AWAITING_ACK", "BOOKED", "PRELIMINARY_REPORT"}:
        return "active"
    if state in {"EXECUTED", "FINAL_REPORT", "MODIFIED_REPORT"}:
        return "completed"
    if state == "REJECTED":
        return "revoked"
    return "unknown"


def _map_priority(row: dict[str, Any]) -> str | None:
    values = [
        str_or_none(row.get("INDKALDELSESPRIORIT_KODE")),
        str_or_none(row.get("INDKALDELSESPRIORIT_KODETEKST")),
    ]
    normalized_values = {v.lower() for v in values if v}
    if normalized_values & {"asap", "akut", "acute"}:
        return "asap"
    if normalized_values & {"routine", "rutine", "elektiv", "elective"}:
        return "routine"
    return None


def _build_henvisning_source_meta(
    row: dict[str, Any],
    history_row: dict[str, Any] | None,
    refinfo_row: dict[str, Any] | None,
) -> list[Extension]:
    parts: list[Extension] = [Extension.construct(url="sourceTable", valueString="HENVISNING")]
    _add_string_part(parts, "statusKode", row.get("STATUS_KODE"))
    _add_string_part(parts, "statusKodetekst", row.get("STATUS_KODETEKST"))
    _add_string_part(parts, "typeKode", row.get("TYPE_KODE"))
    _add_string_part(parts, "typeKodetekst", row.get("TYPE_KODETEKST"))
    _add_string_part(parts, "indkaldelsesprioritetKode", row.get("INDKALDELSESPRIORIT_KODE"))
    _add_string_part(
        parts,
        "indkaldelsesprioritetKodetekst",
        row.get("INDKALDELSESPRIORIT_KODETEKST"),
    )
    _add_string_part(parts, "henvisningsmaadeKode", row.get("HENVISNINGSMAADE_KODE"))
    _add_string_part(parts, "henvisningsmaadeKodetekst", row.get("HENVISNINGSMAADE_KODETEKST"))
    _add_string_part(parts, "referralPurpose", row.get("REFERRAL_PURPOSE"))
    _add_datetime_part(parts, "modtaget", row.get("MODTAGET"))
    _add_datetime_part(parts, "opdateretTid", row.get("OPDATERET_TID"))

    if history_row:
        hist_parts: list[Extension] = []
        _add_string_part(hist_parts, "handling", history_row.get("HANDLING"))
        _add_string_part(hist_parts, "statusKode", history_row.get("STATUS_KODE"))
        _add_string_part(hist_parts, "statusKodetekst", history_row.get("STATUS_KODETEKST"))
        _add_string_part(hist_parts, "fraKode", history_row.get("FRA_KODE"))
        _add_string_part(hist_parts, "fraKodetekst", history_row.get("FRA_KODETEKST"))
        _add_string_part(hist_parts, "ventegruppe", history_row.get("VENTEGRUPPE"))
        _add_string_part(hist_parts, "ekspertomraade", history_row.get("EKSPERTOMRAADE"))
        _add_string_part(hist_parts, "treatmentRight", history_row.get("TREATMENTRIGHT"))
        _add_bool_part(hist_parts, "faellesvisitation", history_row.get("FAELLESVISITATION"))
        _add_datetime_part(hist_parts, "beslutTidspunkt", history_row.get("BESLUT_TIDSPUNKT"))
        _add_datetime_part(hist_parts, "visitationsdato", history_row.get("VISITATIONSDATO"))
        if hist_parts:
            parts.append(Extension.construct(url="latestHistory", extension=hist_parts))

    if refinfo_row:
        ref_parts: list[Extension] = []
        for column in (
            "REFERRAL_ORG_CODE",
            "REFERRAL_ORG_NAME",
            "REFERRAL_ORG_ORGCODE",
            "PRACTITIONER_NUMBER",
            "PRACTITIONER_NAME",
            "REFERRAL_TYPE_CODE",
            "REFERRAL_TYPE_TEXT",
            "REFERRAL_REASON_CODE",
            "REFERRAL_REASON_TEXT",
            "FREECHOICE_CODE",
            "FREECHOICE_TEXT",
        ):
            _add_string_part(ref_parts, column.lower(), refinfo_row.get(column))
        _add_datetime_part(ref_parts, "referralDate", refinfo_row.get("REFERRAL_DATE"))
        if ref_parts:
            parts.append(Extension.construct(url="referralInformation", extension=ref_parts))

    return [Extension.construct(url=EXTENSION_HENVISNING_SOURCE_META, extension=parts)]


def _build_ris_source_meta(row: dict[str, Any], service_group: dict[str, Any]) -> list[Extension]:
    parts: list[Extension] = [Extension.construct(url="sourceTable", valueString="RIS_YDELSE")]
    _add_string_part(parts, "state", row.get("STATE"))
    _add_string_part(parts, "type", row.get("TYPE"))
    _add_string_part(parts, "tilstedevaerelseId", row.get("TILSTEDEVAERELSE_ID"))
    _add_string_part(parts, "beskedId", row.get("BESKED_ID"))
    _add_string_part(parts, "filnavn", row.get("FILNAVN"))
    _add_string_part(parts, "rtee", row.get("RTEE"))
    _add_bool_part(parts, "ignored", row.get("IGNORED"))
    _add_bool_part(parts, "lpr3Ignored", row.get("LPR3_IGNORED"))
    _add_datetime_part(parts, "opdateretTid", row.get("OPDATERET_TID"))
    _add_datetime_part(parts, "udfoertTidspunkt", row.get("UDFOERT_TIDSPUNKT"))
    _add_string_part(parts, "stamafdelingKode", row.get("STAMAFD_KODE"))
    _add_string_part(parts, "stamafdelingNavn", row.get("STAMAFD_NAVN"))
    _add_string_part(parts, "stamafdelingOrgkode", row.get("STAMAFD_ORGKODE"))
    _add_string_part(parts, "stamafdelingSorcode", row.get("STAMAFD_SORCODE"))
    _add_string_part(parts, "udfoertEnhedKode", row.get("UDFOERT_ADMENHED_KODE"))
    _add_string_part(parts, "udfoertEnhedNavn", row.get("UDFOERT_ADMENHED_NAVN"))
    _add_string_part(parts, "udfoertEnhedOrgkode", row.get("UDFOERT_ADMENHED_ORGKODE"))
    _add_string_part(parts, "udfoertEnhedSorcode", row.get("UDFOERT_ADMENHED_SORCODE"))

    element_nummer = service_group.get("element_nummer")
    if element_nummer is not None:
        parts.append(Extension.construct(url="elementNummer", valueInteger=int(element_nummer)))
    _add_string_part(parts, "primaryCode", service_group.get("primary_code"))
    for extra_code in service_group.get("tillaegskoder", []):
        _add_string_part(parts, "tillaegskode", extra_code)

    return [Extension.construct(url=EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META, extension=parts)]


def _latest_history_by_henvisning(
    history_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in history_rows:
        henvisning_id = normalize_oracle_id(row.get("HENVISNING_ID"))
        if not henvisning_id:
            continue
        current = latest.get(henvisning_id)
        if current is None or _history_sort_key(row) > _history_sort_key(current):
            latest[henvisning_id] = row
    return latest


def _history_sort_key(row: dict[str, Any]) -> tuple[str, int]:
    dt = parse_clob_datetime_safe(row.get("BESLUT_TIDSPUNKT")) or ""
    try:
        ordning = int(row.get("ORDNING") or 0)
    except (TypeError, ValueError):
        ordning = 0
    return (dt, ordning)


def _refinfo_by_episode(refinfo_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for row in refinfo_rows:
        episode_id = normalize_oracle_id(row.get("OBJECT_ID"))
        if not episode_id:
            continue
        current = latest.get(episode_id)
        if current is None or _refinfo_sort_key(row) > _refinfo_sort_key(current):
            latest[episode_id] = row
    return latest


def _refinfo_sort_key(row: dict[str, Any]) -> str:
    return parse_clob_datetime_safe(row.get("REFERRAL_DATE")) or ""


# Columns mirror DSI.extract.IFDB.henvisning_historik / referralinformation
# extract configs. Kept local to avoid a DSA -> DSI import; the
# test_dsi_extraction_configs integration test guards against drift between the
# extract assets and the source columns the transform consumes.
_HISTORY_FETCH_COLUMNS = [
    "HENVISNING_ID",
    "ORDNING",
    "HANDLING",
    "BESLUT_TIDSPUNKT",
    "FRA_KODE",
    "FRA_KODETEKST",
    "STATUS_KODE",
    "STATUS_KODETEKST",
    "TIL_AFDELING_SORKODE",
    "MODTAGETDATO",
    "VISITATIONSDATO",
    "VENTEGRUPPE",
    "EKSPERTOMRAADE",
    "TREATMENTRIGHT",
    "FAELLESVISITATION",
    "DIAGNOSE_ID",
]
_REFINFO_FETCH_COLUMNS = [
    "OBJECT_ID",
    "REFERRAL_ORG_CODE",
    "REFERRAL_ORG_NAME",
    "REFERRAL_ORG_ORGCODE",
    "REFERRAL_ORG_SORCODE",
    "PRACTITIONER_NUMBER",
    "PRACTITIONER_NAME",
    "REFERRAL_TYPE_CODE",
    "REFERRAL_TYPE_TEXT",
    "REFERRAL_REASON_CODE",
    "REFERRAL_REASON_TEXT",
    "FREECHOICE_CODE",
    "FREECHOICE_TEXT",
    "REFERRAL_DATE",
]


def _fetch_latest_history_by_henvisning_ids(
    henvisning_ids: list[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, dict[str, Any]]:
    """Fetch the latest HENVISNING_HISTORIK row per HENVISNING_ID from Oracle."""
    if not henvisning_ids:
        return {}
    grouped = chunked_in_fetch_grouped(
        henvisning_ids,
        table="IFDB.HENVISNING_HISTORIK",
        columns=_HISTORY_FETCH_COLUMNS,
        where_column="HENVISNING_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
        key_fn=lambda row: normalize_oracle_id(row.get("HENVISNING_ID")),
    )
    latest = {hid: max(rows, key=_history_sort_key) for hid, rows in grouped.items()}
    context.log.info(
        "[HISTORY_BACKFILL] Fetched history for %d/%d henvisninger missing from "
        "the co-partition window",
        len(latest),
        len(henvisning_ids),
    )
    return latest


def _fetch_latest_refinfo_by_episode_ids(
    episode_ids: list[str],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> dict[str, dict[str, Any]]:
    """Fetch the latest REFERRALINFORMATION row per episode OBJECT_ID from Oracle."""
    if not episode_ids:
        return {}
    grouped = chunked_in_fetch_grouped(
        episode_ids,
        table="IFDB.REFERRALINFORMATION",
        columns=_REFINFO_FETCH_COLUMNS,
        where_column="OBJECT_ID",
        ifdb_oracle=ifdb_oracle,
        context=context,
        key_fn=lambda row: normalize_oracle_id(row.get("OBJECT_ID")),
    )
    latest = {eid: max(rows, key=_refinfo_sort_key) for eid, rows in grouped.items()}
    context.log.info(
        "[REFINFO_BACKFILL] Fetched referral info for %d/%d episodes missing from "
        "the co-partition window",
        len(latest),
        len(episode_ids),
    )
    return latest


def _backfill_enrichment_from_oracle(
    henvisning_rows: list[dict[str, Any]],
    history_by_id: dict[str, dict[str, Any]],
    refinfo_by_episode: dict[str, dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    context: dg.AssetExecutionContext,
) -> None:
    """Backfill history/refinfo for the partition's henvisninger that are absent
    from the co-partition extracts.

    HENVISNING (driver), HENVISNING_HISTORIK and REFERRALINFORMATION are each
    windowed by an independent timestamp, so an in-window henvisning can have its
    latest history/referral-info change fall outside the window — dropping the
    reasonReference and status-history enrichment. We key-scope a fetch (chunked
    IN) for exactly the henvisninger/episodes that the co-partition lookups did
    not cover, closing that relation gap. Mutates the two lookups in place.
    """
    missing_history_ids = sorted(
        {
            hid
            for row in henvisning_rows
            if (hid := normalize_oracle_id(row.get("OBJEKT_ID"))) and hid not in history_by_id
        }
    )
    for hid, hist_row in _fetch_latest_history_by_henvisning_ids(
        missing_history_ids, ifdb_oracle, context
    ).items():
        history_by_id.setdefault(hid, hist_row)

    missing_episode_ids = sorted(
        {
            eid
            for row in henvisning_rows
            if (eid := normalize_oracle_id(row.get("EPISODE_OF_CARE_ID")))
            and eid not in refinfo_by_episode
        }
    )
    for eid, ref_row in _fetch_latest_refinfo_by_episode_ids(
        missing_episode_ids, ifdb_oracle, context
    ).items():
        refinfo_by_episode.setdefault(eid, ref_row)


def _transform_henvisning_to_service_requests(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
    history_by_id: dict[str, dict[str, Any]],
    refinfo_by_episode: dict[str, dict[str, Any]],
) -> list[ServiceRequest]:
    requests: list[ServiceRequest] = []
    unmapped_statuses: dict[str, int] = {}

    for row in rows:
        objekt_id = row.get("OBJEKT_ID")
        patient_id = row.get("PATIENT_ID")
        source_id = normalize_oracle_id(objekt_id)
        if not source_id or not patient_id:
            continue

        status = _map_henvisning_status(row)
        if status == "unknown":
            source_status = str_or_none(row.get("STATUS_KODE")) or str_or_none(
                row.get("STATUS_KODETEKST")
            )
            if source_status:
                unmapped_statuses[source_status] = unmapped_statuses.get(source_status, 0) + 1

        episode_id = normalize_oracle_id(row.get("EPISODE_OF_CARE_ID"))
        history_row = history_by_id.get(source_id)
        refinfo_row = refinfo_by_episode.get(episode_id) if episode_id else None

        extensions: list[Extension] = [
            build_is_deleted_extension(int(_is_true_flag(row.get("SLETTET"))))
        ]
        if episode_id:
            extensions.append(
                Extension(
                    url=EXTENSION_EPISODE_OF_CARE,
                    valueReference=Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            value=episode_id,
                        )
                    ),
                )
            )
        modtidspunkt = parse_clob_datetime_safe(row.get("MODTIDSPUNKT"))
        if modtidspunkt:
            extensions.append(
                Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt)
            )
        extensions.extend(_build_henvisning_source_meta(row, history_row, refinfo_row))

        category = None
        type_code = str_or_none(row.get("TYPE_KODE"))
        type_text = str_or_none(row.get("TYPE_KODETEKST"))
        if type_code or type_text:
            category = [
                CodeableConcept(
                    coding=(
                        [
                            Coding(
                                system=SYSTEM_HENVISNING_TYPE,
                                code=type_code,
                                display=type_text,
                            )
                        ]
                        if type_code
                        else None
                    ),
                    text=type_text,
                )
            ]

        priority = _map_priority(row)
        occurrence_period = None
        slutdato = parse_clob_datetime_safe(row.get("SLUTDATO"))
        if slutdato:
            occurrence_period = Period(end=slutdato)

        requester = _first_reference(
            _reference_by_raw_identifier(
                resource_type="Organization",
                system=SYSTEM_SOR,
                value=row.get("HENVIST_FRA_AFDELING_SORKODE"),
            ),
            _reference_by_raw_identifier(
                resource_type="Organization",
                system=SYSTEM_SOR,
                value=row.get("AFSENDER_AFDELING_SORKODE"),
            ),
            _reference_by_raw_identifier(
                resource_type="Organization",
                system=SYSTEM_SOR,
                value=row.get("BEHANDLER_AFDELING_SORKODE"),
            ),
            _reference_by_raw_identifier(
                resource_type="Organization",
                system=SYSTEM_SOR,
                value=refinfo_row.get("REFERRAL_ORG_SORCODE") if refinfo_row else None,
            ),
        )
        performer = _reference_by_raw_identifier(
            resource_type="Organization",
            system=SYSTEM_SOR,
            value=row.get("HENVIST_TIL_AFDELING_SORKODE"),
        )

        reason_reference = None
        diagnose_id = history_row.get("DIAGNOSE_ID") if history_row else None
        if diagnose_id:
            ref = _reference_by_identifier(
                resource_type="Condition",
                system=SYSTEM_IFDB_PAS_DIAGNOSE,
                value=diagnose_id,
            )
            if ref:
                reason_reference = [ref]

        authored_on = parse_clob_datetime_safe(
            row.get("HENVISNINGSDATO")
        ) or parse_clob_datetime_safe(row.get("AFSENDELSESDATO"))

        requests.append(
            ServiceRequest(
                identifier=[Identifier(system=SYSTEM_IFDB_HENVISNING_ID, value=source_id)],
                status=status,
                intent="proposal",
                category=category,
                priority=priority,
                subject=Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_PATIENT_ID,
                        value=normalize_oracle_id(patient_id),
                    )
                ),
                occurrencePeriod=occurrence_period,
                authoredOn=authored_on,
                requester=requester,
                performer=[performer] if performer else None,
                reasonReference=reason_reference,
                extension=extensions,
            )
        )

    if unmapped_statuses:
        context.log.info("Unmapped HENVISNING statuses preserved as unknown: %s", unmapped_statuses)
    return requests


def _build_ris_code(service_group: dict[str, Any]) -> CodeableConcept | None:
    primary_code = service_group.get("primary_code")
    normalized = normalize_sks_coding_code(primary_code) if primary_code else None
    if not normalized:
        return None
    return CodeableConcept(
        coding=[Coding(system=SYSTEM_SKS, code=str(normalized))],
        text=str(primary_code),
    )


def _build_ris_order_details(service_group: dict[str, Any]) -> list[CodeableConcept] | None:
    details: list[CodeableConcept] = []
    for extra_code in service_group.get("tillaegskoder", []):
        normalized = normalize_sks_coding_code(extra_code)
        if not normalized:
            continue
        details.append(
            CodeableConcept(
                coding=[Coding(system=SYSTEM_SKS, code=str(normalized))],
                text=str(extra_code),
            )
        )
    return details or None


def _transform_ris_ydelse_to_service_requests(
    rows: list[dict[str, Any]],
    context: dg.AssetExecutionContext,
) -> list[ServiceRequest]:
    requests: list[ServiceRequest] = []
    unmapped_states: dict[str, int] = {}

    for row in rows:
        objekt_id = row.get("OBJEKT_ID")
        patient_id = row.get("PATIENT_ID")
        source_id = normalize_oracle_id(objekt_id)
        if not source_id or not patient_id:
            continue

        service_groups = extract_ris_services_from_xml(
            row.get("XML"),
            objekt_id=objekt_id,
            context=context,
            strict_source_validation=False,
        )
        if not service_groups:
            service_groups = [{"element_nummer": 1, "primary_code": None, "tillaegskoder": []}]

        status = _map_ris_status(row)
        if status == "unknown":
            state = str_or_none(row.get("STATE"))
            if state:
                unmapped_states[state] = unmapped_states.get(state, 0) + 1

        deleted_int = int(_is_true_flag(row.get("SLETTET")))
        episode_id = normalize_oracle_id(row.get("EPISODE_OF_CARE_ID"))

        # Row-invariant values: compute once per row, not per service group.
        # Reference-bearing objects (subject/encounter/performer/episode) are
        # still built fresh per resource below, because reference resolution
        # mutates them in place.
        patient_value = normalize_oracle_id(patient_id)
        modtidspunkt = parse_clob_datetime_safe(row.get("MODTIDSPUNKT"))
        period_start = parse_clob_datetime_safe(row.get("PERIOD_START"))
        period_end = parse_clob_datetime_safe(row.get("PERIOD_END"))
        occurrence_period = (
            Period(start=period_start, end=period_end) if (period_start or period_end) else None
        )
        category_codings = [
            Coding(
                system=SYSTEM_RIS_SERVICE_CATEGORY,
                code="radiology",
                display="Radiology",
            )
        ]
        ris_type = str_or_none(row.get("TYPE"))
        if ris_type:
            category_codings.append(Coding(system=SYSTEM_RIS_TYPE, code=ris_type, display=ris_type))
        category = [CodeableConcept(coding=category_codings)]
        # _first_reference prefers UDFOERT, then STAMAFD — replicated here.
        performer_value = row.get("UDFOERT_ADMENHED_SORCODE") or row.get("STAMAFD_SORCODE")
        encounter_value = row.get("ENCOUNTER_ID")

        for group in service_groups:
            element_nummer = int(group.get("element_nummer") or 1)
            identifier_value = f"{source_id}:{element_nummer}"
            extensions: list[Extension] = [build_is_deleted_extension(deleted_int)]
            if episode_id:
                extensions.append(
                    Extension(
                        url=EXTENSION_EPISODE_OF_CARE,
                        valueReference=Reference(
                            identifier=Identifier(
                                system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                                value=episode_id,
                            )
                        ),
                    )
                )
            if modtidspunkt:
                extensions.append(
                    Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt)
                )
            extensions.extend(_build_ris_source_meta(row, group))

            ris_code = _build_ris_code(group)
            performer = _reference_by_raw_identifier(
                resource_type="Organization",
                system=SYSTEM_SOR,
                value=performer_value,
            )

            requests.append(
                ServiceRequest(
                    identifier=[Identifier(system=SYSTEM_IFDB_RIS_YDELSE, value=identifier_value)],
                    status=status,
                    intent="order",
                    category=category,
                    code=ris_code,
                    orderDetail=_build_ris_order_details(group) if ris_code else None,
                    subject=Reference(
                        identifier=Identifier(
                            system=SYSTEM_IFDB_PATIENT_ID,
                            value=patient_value,
                        )
                    ),
                    encounter=_reference_by_identifier(
                        resource_type="Encounter",
                        system=SYSTEM_IFDB_OBJECT_ID,
                        value=encounter_value,
                    ),
                    occurrencePeriod=occurrence_period,
                    performer=[performer] if performer else None,
                    extension=extensions,
                )
            )

    if unmapped_states:
        context.log.info("Unmapped RIS_YDELSE states preserved as unknown: %s", unmapped_states)
    return requests


def _post_service_requests_to_fhir(
    context: dg.AssetExecutionContext,
    service_requests: list[ServiceRequest],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    ifdb_oracle: IFDBOracleResource | None = None,
    resolve_missing_dependencies: bool = True,
) -> dg.MaterializeResult:
    logger = dg.get_dagster_logger()

    patient_ids_needed: set[str] = set()
    encounter_ids_needed: set[str] = set()
    eoc_ids_needed: set[str] = set()
    for req in service_requests:
        if req.subject and getattr(req.subject, "identifier", None):
            patient_ids_needed.add(str(req.subject.identifier.value))
        if req.encounter and getattr(req.encounter, "identifier", None):
            encounter_ids_needed.add(str(req.encounter.identifier.value))
        for ext in req.extension or []:
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

    service_requests = prepare_resources_for_upsert(
        resources=service_requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        preferred_id_systems=[SYSTEM_IFDB_HENVISNING_ID, SYSTEM_IFDB_RIS_YDELSE],
        reference_configs=SERVICE_REQUEST_REFERENCE_CONFIGS,
        nested_reference_configs=SERVICE_REQUEST_NESTED_REFERENCE_CONFIGS,
        logger=logger,
    )

    return post_resources_direct(
        context=context,
        resources=service_requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )


def _read_result_int(result: dg.MaterializeResult, key: str) -> int:
    value = result.metadata.get(key) if result.metadata else None
    raw = getattr(value, "value", value)
    return int(raw or 0)


def _combine_results(
    henvisning_result: dg.MaterializeResult,
    ris_result: dg.MaterializeResult,
) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "posted_count": dg.MetadataValue.int(
                _read_result_int(henvisning_result, "posted_count")
                + _read_result_int(ris_result, "posted_count")
            ),
            "failed_count": dg.MetadataValue.int(
                _read_result_int(henvisning_result, "failed_count")
                + _read_result_int(ris_result, "failed_count")
            ),
            "extracted_count": dg.MetadataValue.int(
                _read_result_int(henvisning_result, "extracted_count")
                + _read_result_int(ris_result, "extracted_count")
            ),
            "henvisning_chunks": dg.MetadataValue.int(
                _read_result_int(henvisning_result, "chunk_count")
            ),
            "ris_chunks": dg.MetadataValue.int(_read_result_int(ris_result, "chunk_count")),
            "source": dg.MetadataValue.text("ifdb_oracle"),
            "method": dg.MetadataValue.text("direct_postgres"),
        }
    )


def _run_henvisning_pipeline(
    context: dg.AssetExecutionContext,
    henvisning_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    refinfo_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    backfill_enrichment: bool = False,
) -> dg.MaterializeResult:
    def setup(
        rows: list[dict[str, Any]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        history_by_id = _latest_history_by_henvisning(history_rows)
        refinfo_by_episode = _refinfo_by_episode(refinfo_rows)
        # Incremental partitions window the enrichment tables independently of
        # HENVISNING, so key-scope a fetch for the rows the co-partition extracts
        # missed. Full-load already holds the complete enrichment snapshot.
        if backfill_enrichment:
            _backfill_enrichment_from_oracle(
                rows, history_by_id, refinfo_by_episode, ifdb_oracle, context
            )
        return (history_by_id, refinfo_by_episode)

    def transform(
        chunk: list[dict[str, Any]],
        lookups: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
    ) -> list[ServiceRequest]:
        history_by_id, refinfo_lookup = lookups
        return _transform_henvisning_to_service_requests(
            chunk,
            context,
            history_by_id,
            refinfo_lookup,
        )

    def post(
        service_requests: list[ServiceRequest],
        chunk: list[dict[str, Any]],
        _lookups: tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]],
    ) -> dg.MaterializeResult:
        return _post_service_requests_to_fhir(
            context=context,
            service_requests=service_requests,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        SERVICE_REQUEST_HENVISNING_CHUNK_STAGE,
        rows=henvisning_rows,
        setup_fn=setup,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


def _run_ris_pipeline(
    context: dg.AssetExecutionContext,
    ris_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    def transform(chunk: list[dict[str, Any]], _setup: None) -> list[ServiceRequest]:
        return _transform_ris_ydelse_to_service_requests(chunk, context)

    def post(
        service_requests: list[ServiceRequest],
        chunk: list[dict[str, Any]],
        _setup: None,
    ) -> dg.MaterializeResult:
        return _post_service_requests_to_fhir(
            context=context,
            service_requests=service_requests,
            fhir_database=fhir_database,
            extracted_count=len(chunk),
            ifdb_oracle=ifdb_oracle,
        )

    return run_chunked_transform_post(
        SERVICE_REQUEST_RIS_CHUNK_STAGE,
        rows=ris_rows,
        setup_fn=None,
        transform_fn=transform,
        post_fn=post,
        fhir_database=fhir_database,
        context=context,
    )


def _run_service_request_pipeline(
    context: dg.AssetExecutionContext,
    henvisning_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    refinfo_rows: list[dict[str, Any]],
    ris_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_database: FHIRDatabaseResource,
    backfill_enrichment: bool = False,
) -> dg.MaterializeResult:
    henvisning_result = _run_henvisning_pipeline(
        context=context,
        henvisning_rows=henvisning_rows,
        history_rows=history_rows,
        refinfo_rows=refinfo_rows,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        backfill_enrichment=backfill_enrichment,
    )
    ris_result = _run_ris_pipeline(
        context=context,
        ris_rows=ris_rows,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
    return _combine_results(henvisning_result, ris_result)


@dg.asset(
    name="fhir_service_request_daily_ifdb",
    ins={
        "henvisning_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "henvisning_daily"])),
        "history_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "henvisning_historik_daily"])),
        "refinfo_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "referralinformation_daily"])),
        "ris_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_daily"])),
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
def fhir_service_request_daily_ifdb(
    context: dg.AssetExecutionContext,
    henvisning_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    refinfo_rows: list[dict[str, Any]],
    ris_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ServiceRequest resources for daily partition."""
    return _run_service_request_pipeline(
        context=context,
        henvisning_rows=henvisning_rows,
        history_rows=history_rows,
        refinfo_rows=refinfo_rows,
        ris_rows=ris_rows,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        backfill_enrichment=True,
    )


@dg.asset(
    name="fhir_service_request_monthly_ifdb",
    ins={
        "henvisning_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "henvisning_monthly"])),
        "history_rows": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "henvisning_historik_monthly"])
        ),
        "refinfo_rows": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "referralinformation_monthly"])
        ),
        "ris_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_monthly"])),
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
def fhir_service_request_monthly_ifdb(
    context: dg.AssetExecutionContext,
    henvisning_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    refinfo_rows: list[dict[str, Any]],
    ris_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ServiceRequest resources for monthly partition."""
    return _run_service_request_pipeline(
        context=context,
        henvisning_rows=henvisning_rows,
        history_rows=history_rows,
        refinfo_rows=refinfo_rows,
        ris_rows=ris_rows,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
        backfill_enrichment=True,
    )


@dg.asset(
    name="fhir_service_request_bulk_ifdb",
    ins={
        "henvisning_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "henvisning_full_load"])),
        "history_rows": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "henvisning_historik_full_load"])
        ),
        "refinfo_rows": dg.AssetIn(
            key=dg.AssetKey(["ifdb_extract", "referralinformation_full_load"])
        ),
        "ris_rows": dg.AssetIn(key=dg.AssetKey(["ifdb_extract", "ris_ydelse_full_load"])),
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
def fhir_service_request_bulk_ifdb(
    context: dg.AssetExecutionContext,
    henvisning_rows: list[dict[str, Any]],
    history_rows: list[dict[str, Any]],
    refinfo_rows: list[dict[str, Any]],
    ris_rows: list[dict[str, Any]],
    ifdb_oracle: IFDBOracleResource,
    fhir_server: FHIRServerResource,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """Transform and post ServiceRequest resources for full-load partition."""
    return _run_service_request_pipeline(
        context=context,
        henvisning_rows=henvisning_rows,
        history_rows=history_rows,
        refinfo_rows=refinfo_rows,
        ris_rows=ris_rows,
        ifdb_oracle=ifdb_oracle,
        fhir_database=fhir_database,
    )
