# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""FHIR ServiceRequest asset for IFDB HENVISNING and RIS_YDELSE data (DFM).

Reads both source streams from the archive layer in batches, transforms to FHIR
ServiceRequest, and posts to the FHIR server:

    arc.ifdb_henvisning              → ServiceRequest (henvisning stream)
      + arc.ifdb_henvisning_historik   status history / referral diagnosis
      + arc.ifdb_referralinformation   referral origin (keyed by episode id)
    arc.ifdb_ris_ydelse              → ServiceRequest + referral Conditions

No Dagster partitions — checkpoint tracking replaces partition boundaries.

The legacy pipeline windowed the two enrichment tables independently of
HENVISNING and patched the resulting relation gap with a key-scoped Oracle
fetch (``_backfill_enrichment_from_oracle``). Reading the archive removes the
gap at the source: the per-batch lookup below is keyed, not windowed, so it
always sees every history/referral row the archive holds. The "latest row per
key" selection is unchanged.
"""

import re
from typing import Any

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import (
    FHIRDatabaseResource,
    MSSQLResource,
)
from DSA.FHIR.Condition.Condition_IFDB_PasDiagnose_dfm import PAS_DIAGNOSE_REFERENCE_CONFIGS
from DSA.FHIR.utils.code_normalization import (
    normalize_sks_coding_code,
    shak_identifier_value,
    to_sks_diagnosis_code,
)
from DSA.FHIR.utils.dfm_query import build_fhir_archive_query
from DSA.FHIR.utils.fhir_checkpoint import advance_fhir_checkpoint
from DSA.FHIR.utils.ifdb.arc_lookup import chunked_in_fetch_grouped
from DSA.FHIR.utils.ifdb.fhir_posting import (
    post_resources_direct,
    prepare_resources_for_upsert,
)
from DSA.FHIR.utils.ifdb.patient_cpr_fallback import build_patient_cpr_fallback
from DSA.FHIR.utils.ifdb.posting_config import TAG_CHUNK_SIZE, TAG_MAX_WORKERS
from DSA.FHIR.utils.ifdb.ris_ydelse_xml import (
    extract_ris_row_fields,
    extract_ris_services_from_xml,
    str_or_none,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import (
    normalize_oracle_id,
    parse_clob_datetime_safe,
    parse_xml_compact_datetime_safe,
)
from DSA.FHIR.utils.reference_resolver import (
    NestedReferenceFieldConfig,
    ReferenceFieldConfig,
    batch_resolve_references,
)
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.condition import Condition
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from fhir.resources.R4B.servicerequest import ServiceRequest
from utils.fhir.condition_categories import build_referral_indication_category
from utils.fhir.fhir_extensions import (
    EXTENSION_DIAGNOSETYPE,
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_HENVISNING_SOURCE_META,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META,
    EXTENSION_SOURCE_TABLE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_HENVISNING_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
    SYSTEM_IFDB_STAFF,
    SYSTEM_SHAK,
    SYSTEM_SKS,
    SYSTEM_SKS_D,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension

SYSTEM_HENVISNING_TYPE = "urn:source:ifdb:henvisning:type"
SYSTEM_RIS_SERVICE_CATEGORY = "http://example.org/fhir/CodeSystem/service-request-category"

_CONDITION_VERIFICATION_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-ver-status"

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
            SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE: "Condition",
        },
        is_required=False,
        description="Referral diagnosis reference from HENVISNING_HISTORIK.DIAGNOSE_ID",
    ),
]

# Archive fetch batch size. Smaller than the legacy in-memory chunk sizes
# because rows now stream from MSSQL instead of arriving as one partition list.
BATCH_SIZE = 10_000


def _add_string_part(parts: list[Extension], key: str, value: Any) -> None:
    s = str_or_none(value)
    if s:
        parts.append(Extension.construct(url=key, valueString=s))


def _add_identifier_part(parts: list[Extension], key: str, system: str, value: Any) -> None:
    """Add a sub-extension carrying a typed Identifier (system + value).

    Used to carry referral organisation/practitioner identities in source
    metadata as machine-resolvable identifiers (rather than plain strings) so a
    later afterburner can build the real Organization/Practitioner references.
    """
    s = str_or_none(value)
    if s:
        parts.append(
            Extension.construct(url=key, valueIdentifier=Identifier(system=system, value=s))
        )


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


# IFDB's henvisningsstatus (PASHenvisning_ks.ur.Henvisningsstatus.<suffiks>) oversat til
# FHIR ServiceRequest.status. Værdirummet er målt i kilden: ti værdier. Rå kode og tekst
# bevares i henvisningSourceMeta og projiceres som status_kode/status_kodetekst, så
# aftageren kan gruppere anderledes end FHIR-koderne tillader.
_HENVISNING_STATUS_MAP: dict[str, str] = {
    "afsluttet": "unknown",
    "sendt": "active",
    "sendtingenkvittering": "active",
    "videresendt": "active",
    "modtaget": "active",
    "visiteret": "active",
    "omvisiteret": "active",
    "uvisiteret": "active",
    "haandteret": "active",
    "fejlet": "unknown",
}
_FHIR_SERVICEREQUEST_STATUSES = frozenset(
    {
        "draft",
        "active",
        "on-hold",
        "revoked",
        "completed",
        "entered-in-error",
        "unknown",
    }
)


def _henvisning_status_suffix(value: str) -> str:
    """'PASHenvisning_ks.ur.Henvisningsstatus.Afsluttet' -> 'afsluttet'."""
    return value.strip().rsplit(".", 1)[-1].lower()


def _map_henvisning_status(row: dict[str, Any]) -> str:
    if _is_true_flag(row.get("SLETTET")):
        return "entered-in-error"

    value = str_or_none(row.get("STATUS_KODE")) or str_or_none(row.get("STATUS_KODETEKST"))
    if not value:
        return "unknown"

    normalized = value.strip().lower()
    if normalized in _FHIR_SERVICEREQUEST_STATUSES:
        return normalized
    mapped = _HENVISNING_STATUS_MAP.get(_henvisning_status_suffix(value))
    if mapped:
        if _henvisning_status_suffix(value) == "afsluttet":
            return _closed_henvisning_status(row)
        return mapped
    # Kodetekst som fallback ("Sendt (ingen kvittering)" -> "sendtingenkvittering")
    text = str_or_none(row.get("STATUS_KODETEKST"))
    if text:
        key = re.sub(
            r"[^a-z]", "", text.lower().replace("å", "aa").replace("ø", "oe").replace("æ", "ae")
        )
        mapped = _HENVISNING_STATUS_MAP.get(key)
        if mapped:
            if key == "afsluttet":
                return _closed_henvisning_status(row)
            return mapped
    return "unknown"


def _closed_henvisning_status(row: dict[str, Any]) -> str:
    # Afsluttet kan være annulleret. Kun den dokumenterede årsag oversættes;
    # en lukket henvisning beviser ikke, at den bestilte aktivitet er udført.
    reason = str_or_none(row.get("AFSLUTNINGSAARSAG_KODE")) or ""
    if reason.lower() == "pashenvisning_ks.ur.afslutningsaarsag.annulleret":
        return "revoked"
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


def _map_ris_priority(value: Any) -> str | None:
    """Map a RIS_YDELSE.XML <prioritet> value to a FHIR ServiceRequest.priority.

    The ``SPACE`` sentinel is already cleaned to None by the XML parser. Only
    recognised values are mapped; anything else stays None and is preserved
    verbatim in source metadata so the real value space can be discovered.
    """
    s = str_or_none(value)
    if not s:
        return None
    normalized = s.lower()
    if normalized in {"asap", "akut", "acute"}:
        return "asap"
    if normalized in {"stat", "statim"}:
        return "stat"
    if normalized in {"urgent", "haster"}:
        return "urgent"
    if normalized in {"routine", "rutine", "elektiv", "elective", "planlagt"}:
        return "routine"
    dg.get_dagster_logger().warning(
        f"Unmapped RIS prioritet value {s!r}; no FHIR priority set "
        "(raw value preserved in source metadata)"
    )
    return None


def _build_ris_referral_condition(
    row: dict[str, Any],
    xml_fields: dict[str, Any],
    *,
    source_id: str,
    patient_value: str | None,
    encounter_value: Any,
) -> Condition | None:
    """Build a standalone referral-indication Condition from RIS XML diagnose block."""
    raw = str_or_none(xml_fields.get("diagnose_kode"))
    if not raw:
        return None

    deleted_int = int(_is_true_flag(row.get("SLETTET")))
    modtidspunkt = parse_clob_datetime_safe(row.get("MODTIDSPUNKT"))

    code_codings: list[Coding] = []
    sks_d = to_sks_diagnosis_code(raw)
    if sks_d:
        code_codings.append(
            Coding(system=SYSTEM_SKS_D, code=sks_d, userSelected=True),
        )
    else:
        dg.get_dagster_logger().warning(
            "Unrecognised RIS referral diagnosis code %r on row %s; "
            "Condition emitted with text only",
            raw,
            source_id,
        )

    extensions: list[Extension] = [build_is_deleted_extension(deleted_int)]
    if modtidspunkt:
        extensions.append(
            Extension.construct(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt)
        )
    diagnose_type = str_or_none(xml_fields.get("diagnose_type"))
    if diagnose_type:
        extensions.append(
            Extension.construct(url=EXTENSION_DIAGNOSETYPE, valueString=diagnose_type)
        )

    return Condition(
        identifier=[Identifier(system=SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE, value=source_id)],
        category=build_referral_indication_category(),
        verificationStatus=CodeableConcept(
            coding=[
                Coding(
                    system=_CONDITION_VERIFICATION_STATUS_SYSTEM,
                    code="unconfirmed",
                )
            ]
        ),
        code=CodeableConcept(
            coding=code_codings if code_codings else None,
            text=raw,
        ),
        subject=(
            Reference(
                identifier=Identifier(
                    system=SYSTEM_IFDB_PATIENT_ID,
                    value=patient_value,
                )
            )
            if patient_value
            else None
        ),
        encounter=_reference_by_identifier(
            resource_type="Encounter",
            system=SYSTEM_IFDB_OBJECT_ID,
            value=encounter_value,
        ),
        extension=extensions,
    )


def _ris_referral_reason_reference(source_id: str) -> list[Reference]:
    return [
        Reference(
            identifier=Identifier(
                system=SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
                value=source_id,
            )
        )
    ]


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


# Kildens roller bevares særskilt, også når SOR-opslag ikke kan opløses.
_HENVISNING_RAW_FIELDS = {
    "HENVIST_TIL_YDER_NAVN": "henvistTilYderNavn",
    "HENVIST_TIL_YDER_NUMMER": "henvistTilYderNummer",
    "HENVIST_TIL_AFDELING_KODE": "henvistTilAfdelingKode",
    "HENVIST_TIL_AFDELING_KODETEKST": "henvistTilAfdelingKodetekst",
    "HENVIST_TIL_MUNI_KODE": "henvistTilMuniKode",
    "HENVIST_TIL_MUNI_KODETEKST": "henvistTilMuniKodetekst",
    "HENVIST_TIL_MUNI_LOCATION": "henvistTilMuniLocation",
    "HENVIST_FRA_YDER_NAVN": "henvistFraYderNavn",
    "HENVIST_FRA_YDER_NUMMER": "henvistFraYderNummer",
    "HENVIST_FRA_AFDELING_KODE": "henvistFraAfdelingKode",
    "HENVIST_FRA_AFDELING_KODETEKST": "henvistFraAfdelingKodetekst",
    "HENVIST_FRA_INSTITUTION_NAVN": "henvistFraInstitutionNavn",
    "HENVIST_FRA_INSTITUTION_POSTNR": "henvistFraInstitutionPostnr",
    "HENVIST_FRA_INSTITUTION_VEJ": "henvistFraInstitutionVej",
    "HENVIST_FRA_INSTITUTION_TLF_NR": "henvistFraInstitutionTlfNr",
    "AFSLUTNINGSAARSAG_KODE": "afslutningsaarsagKode",
    "AFSLUTNINGSAARSAG_KODETEKST": "afslutningsaarsagKodetekst",
    "AFSENDER_YDER_NAVN": "afsenderYderNavn",
    "AFSENDER_YDER_NUMMER": "afsenderYderNummer",
    "AFSENDER_AFDELING_KODE": "afsenderAfdelingKode",
    "AFSENDER_AFDELING_KODETEKST": "afsenderAfdelingKodetekst",
    "AFSENDER_INSTITUTION_NAVN": "afsenderInstitutionNavn",
    "AFSENDER_INSTITUTION_POSTNR": "afsenderInstitutionPostnr",
    "AFSENDER_INSTITUTION_VEJ": "afsenderInstitutionVej",
    "AFSENDER_INSTITUTION_TLF_NR": "afsenderInstitutionTlfNr",
    "BEHANDLER_YDER_NAVN": "behandlerYderNavn",
    "BEHANDLER_YDER_NUMMER": "behandlerYderNummer",
    "BEHANDLER_AFDELING_KODE": "behandlerAfdelingKode",
    "BEHANDLER_AFDELING_KODETEKST": "behandlerAfdelingKodetekst",
    "SENT_ELECTRONICAL": "sentElectronical",
    "HENVIST_TIL_AFDELING_SORKODE": "henvistTilAfdelingSorkode",
    "HENVIST_FRA_AFDELING_SORKODE": "henvistFraAfdelingSorkode",
    "AFSENDER_AFDELING_SORKODE": "afsenderAfdelingSorkode",
    "BEHANDLER_AFDELING_SORKODE": "behandlerAfdelingSorkode",
}


def _build_henvisning_source_meta(
    row: dict[str, Any],
    history_row: dict[str, Any] | None,
    refinfo_row: dict[str, Any] | None,
) -> list[Extension]:
    parts: list[Extension] = []
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
    for column, url in _HENVISNING_RAW_FIELDS.items():
        _add_string_part(parts, url, row.get(column))
    _add_datetime_part(parts, "henvisningsdato", row.get("HENVISNINGSDATO"))
    _add_datetime_part(parts, "slutdato", row.get("SLUTDATO"))
    _add_datetime_part(parts, "modtaget", row.get("MODTAGET"))
    _add_datetime_part(parts, "afsendelsesdato", row.get("AFSENDELSESDATO"))
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
        _add_string_part(hist_parts, "faellesvisitation", history_row.get("FAELLESVISITATION"))
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
        # Typed identifiers for the referring org (SHAK) and referrer
        # (practitioner number), for later afterburner resolution into real
        # Organization/Practitioner references. SOR is already resolved onto
        # ServiceRequest.requester. SHAK is normalised to bare alphanumerics.
        _add_identifier_part(
            ref_parts,
            "referralOrgShak",
            SYSTEM_SHAK,
            shak_identifier_value(refinfo_row.get("REFERRAL_ORG_ORGCODE")),
        )
        _add_identifier_part(
            ref_parts,
            "practitioner",
            SYSTEM_IFDB_STAFF,
            refinfo_row.get("PRACTITIONER_NUMBER"),
        )
        if ref_parts:
            parts.append(Extension.construct(url="referralInformation", extension=ref_parts))

    return [
        Extension.construct(url=EXTENSION_SOURCE_TABLE, valueString="HENVISNING"),
        Extension.construct(url=EXTENSION_HENVISNING_SOURCE_META, extension=parts),
    ]


def _compose_shak(org: dict[str, Any] | None) -> str | None:
    """Compose a SHAK identifier value from an XML org block's institution+afdeling.

    e.g. {"institution": "4202", "afdeling": "04.1"} -> "4202041". Returns None
    when no institution is present (afdeling alone is not a SHAK).
    """
    if not org:
        return None
    institution = str_or_none(org.get("institution"))
    if not institution:
        return None
    return shak_identifier_value(f"{institution}{str_or_none(org.get('afdeling')) or ''}")


def _build_ris_source_meta(
    row: dict[str, Any],
    service_group: dict[str, Any],
    xml_fields: dict[str, Any],
) -> list[Extension]:
    parts: list[Extension] = []
    _add_string_part(parts, "state", row.get("STATE"))
    _add_string_part(parts, "stamafdelingKode", row.get("STAMAFD_KODE"))
    _add_string_part(parts, "stamafdelingNavn", row.get("STAMAFD_NAVN"))
    _add_string_part(parts, "stamafdelingOrgkode", row.get("STAMAFD_ORGKODE"))
    _add_string_part(parts, "stamafdelingSorcode", row.get("STAMAFD_SORCODE"))
    # Keep both organisation groups as source/audit metadata until the customer
    # has validated which organisation semantics should become durable FHIR refs.
    _add_string_part(parts, "udfoertEnhedKode", row.get("UDFOERT_ADMENHED_KODE"))
    _add_string_part(parts, "udfoertEnhedNavn", row.get("UDFOERT_ADMENHED_NAVN"))
    _add_string_part(parts, "udfoertEnhedOrgkode", row.get("UDFOERT_ADMENHED_ORGKODE"))
    _add_string_part(parts, "udfoertEnhedSorcode", row.get("UDFOERT_ADMENHED_SORCODE"))

    element_nummer = service_group.get("element_nummer")
    if element_nummer is not None:
        parts.append(Extension.construct(url="elementNummer", valueInteger=int(element_nummer)))

    # diagnose/type qualifies the referral diagnosis (standalone Condition). The
    # raw kode/prioritet values are preserved verbatim here: codes that fail
    # SKS-D normalisation and prioritet values outside the mapped set would
    # otherwise be lost entirely.
    _add_string_part(parts, "diagnoseType", xml_fields.get("diagnose_type"))
    _add_string_part(parts, "diagnoseKode", xml_fields.get("diagnose_kode"))
    _add_string_part(parts, "prioritet", xml_fields.get("prioritet"))

    # The XML <kontakt> block carries descriptive contact context but no contact
    # id, so it cannot be resolved to an Encounter reference. Keep it as source
    # metadata so the data is not lost.
    kontakt = xml_fields.get("kontakt") or {}
    if kontakt:
        kontakt_parts: list[Extension] = []
        _add_string_part(kontakt_parts, "starttidspunkt", kontakt.get("starttidspunkt"))
        _add_string_part(kontakt_parts, "type", kontakt.get("type"))
        _add_string_part(kontakt_parts, "henvisningsmaade", kontakt.get("henvisningsmaade"))
        _add_string_part(kontakt_parts, "afslutningsmaade", kontakt.get("afslutningsmaade"))
        _add_string_part(kontakt_parts, "psykiatrisk", kontakt.get("psykiatrisk"))
        _add_string_part(
            kontakt_parts,
            "behandlingsansvarligInstitution",
            kontakt.get("behandlingsansvarlig_institution"),
        )
        _add_string_part(
            kontakt_parts,
            "behandlingsansvarligAfdeling",
            kontakt.get("behandlingsansvarlig_afdeling"),
        )
        if kontakt_parts:
            parts.append(Extension.construct(url="kontakt", extension=kontakt_parts))

    # SHAK identifiers for the ordering (rekvirent -> requester) and performing
    # (producent -> performer) units, composed from XML institution+afdeling.
    # Carried as typed identifiers (no SOR in the XML) for later afterburner
    # resolution into real Organization references.
    _add_identifier_part(
        parts, "rekvirentShak", SYSTEM_SHAK, _compose_shak(xml_fields.get("rekvirent"))
    )
    _add_identifier_part(
        parts, "producentShak", SYSTEM_SHAK, _compose_shak(xml_fields.get("producent"))
    )

    return [
        Extension.construct(url=EXTENSION_SOURCE_TABLE, valueString="RIS_YDELSE"),
        Extension.construct(url=EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META, extension=parts),
    ]


def _latest_history_by_henvisning(
    history_rows: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in history_rows:
        henvisning_id = normalize_oracle_id(row.get("HENVISNING_ID"))
        if not henvisning_id:
            continue
        grouped.setdefault(henvisning_id, []).append(row)
    return {
        hid: current
        for hid, rows in grouped.items()
        if (current := _current_history(rows)) is not None
    }


def _history_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    dt = parse_clob_datetime_safe(row.get("BESLUT_TIDSPUNKT")) or ""
    try:
        ordning = int(normalize_oracle_id(row.get("ORDNING")) or 0)
    except (TypeError, ValueError):
        ordning = 0
    return (ordning, dt)


def _current_history(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """ORDNING bestemmer rækkefølgen; genåbning ophæver foregående afslutning."""
    ordered = sorted(rows, key=_history_sort_key)
    while ordered and str(ordered[-1].get("HANDLING") or "").upper() == "GENAABNING":
        if len(ordered) < 2 or str(ordered[-2].get("HANDLING") or "").upper() != "AFSLUTNING":
            # Ufuldstændig historik må ikke fremstilles som aktuelle visitationsdata.
            return None
        del ordered[-2:]
    return ordered[-1] if ordered else None


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


# Columns are a subset of HENVISNING_HISTORIK_COLUMNS / REFERRALINFORMATION_
# COLUMNS in DSI.oracle.ifdb_extract (the arc-table column sets). Kept local
# to avoid a DSA -> DSI import; test_servicerequest_pipeline_registration pins
# the subset so drift against the archive columns fails loudly.
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


def _fetch_latest_history_for_batch(
    henvisning_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, dict[str, Any]]:
    """Latest HENVISNING_HISTORIK row per HENVISNING_ID from the archive.

    Keyed, not windowed: the lookup always sees every history row the archive
    holds for the batch's henvisninger, so no separate backfill pass is needed.
    ORDNING styrer rækkefølgen. Ved genåbning fjernes genåbning og afslutning
    fra udvælgelsen, så den tidligere visitationsrække bliver aktuel igen.
    """
    if not henvisning_ids:
        return {}
    grouped = chunked_in_fetch_grouped(
        henvisning_ids,
        table="arc.ifdb_henvisning_historik",
        columns=_HISTORY_FETCH_COLUMNS,
        where_column="HENVISNING_ID",
        mssql=mssql,
        key_fn=lambda row: normalize_oracle_id(row.get("HENVISNING_ID")),
    )
    return {
        hid: current
        for hid, rows in grouped.items()
        if (current := _current_history(rows)) is not None
    }


def _fetch_latest_refinfo_for_batch(
    episode_ids: list[str],
    mssql: MSSQLResource,
) -> dict[str, dict[str, Any]]:
    """Latest REFERRALINFORMATION row per episode OBJECT_ID from the archive."""
    if not episode_ids:
        return {}
    grouped = chunked_in_fetch_grouped(
        episode_ids,
        table="arc.ifdb_referralinformation",
        columns=_REFINFO_FETCH_COLUMNS,
        where_column="OBJECT_ID",
        mssql=mssql,
        key_fn=lambda row: normalize_oracle_id(row.get("OBJECT_ID")),
    )
    return {eid: max(rows, key=_refinfo_sort_key) for eid, rows in grouped.items()}


def _build_enrichment_for_batch(
    henvisning_rows: list[dict[str, Any]],
    mssql: MSSQLResource,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Build the (history, refinfo) lookups for one batch of henvisninger."""
    henvisning_ids = sorted(
        {hid for row in henvisning_rows if (hid := normalize_oracle_id(row.get("OBJEKT_ID")))}
    )
    episode_ids = sorted(
        {
            eid
            for row in henvisning_rows
            if (eid := normalize_oracle_id(row.get("EPISODE_OF_CARE_ID")))
        }
    )
    return (
        _fetch_latest_history_for_batch(henvisning_ids, mssql),
        _fetch_latest_refinfo_for_batch(episode_ids, mssql),
    )


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

        # HENVISNINGSDATO er henvisningens oprettelse og hører derfor i det
        # native authoredOn — ellers møder en FHIR-kyndig aftager et tomt
        # standardfelt og må gætte, hvilket kildefelt der svarer til det.
        # Modtagelse, afsendelse og administrativ lukning bevares derimod som
        # kildefelter: de dokumenterer hverken oprettelse eller udførelse, og
        # FHIR har ingen plads til dem, der ikke ville være et misbrug.
        henvisning_authored_on = parse_clob_datetime_safe(row.get("HENVISNINGSDATO"))

        requests.append(
            ServiceRequest(
                identifier=[Identifier(system=SYSTEM_IFDB_HENVISNING_ID, value=source_id)],
                status=status,
                intent="proposal",
                category=category,
                priority=priority,
                authoredOn=henvisning_authored_on,
                subject=Reference(
                    identifier=Identifier(
                        system=SYSTEM_IFDB_PATIENT_ID,
                        value=normalize_oracle_id(patient_id),
                    )
                ),
                requester=requester,
                performer=[performer] if performer else None,
                reasonReference=reason_reference,
                extension=extensions,
            )
        )

    if unmapped_statuses:
        context.log.info(
            "HENVISNING rows with uncertain FHIR status: %s", sum(unmapped_statuses.values())
        )
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
    # Duplicate (system, code) pairs would collide with the
    # ServiceRequest_SupplementaryCode child-table PK; first occurrence wins.
    seen_codes: set[str] = set()
    for extra_code in service_group.get("tillaegskoder", []):
        normalized = normalize_sks_coding_code(extra_code)
        if not normalized or str(normalized) in seen_codes:
            continue
        seen_codes.add(str(normalized))
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
) -> tuple[list[ServiceRequest], list[Condition]]:
    requests: list[ServiceRequest] = []
    referral_conditions: list[Condition] = []
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

        # Row-invariant clinical fields that live ONLY in the XML (no source
        # column carries them): diagnose -> referral Condition + reasonReference,
        # rekvisition/start -> authoredOn, prioritet -> priority, kontakt
        # context. (rekvirent -> requester is deferred to the SHAK/SOR branch.)
        xml_fields = extract_ris_row_fields(row.get("XML"), objekt_id=objekt_id, context=context)

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
        category = [CodeableConcept(coding=category_codings)]
        # _first_reference prefers UDFOERT, then STAMAFD — replicated here.
        performer_value = row.get("UDFOERT_ADMENHED_SORCODE") or row.get("STAMAFD_SORCODE")
        encounter_value = row.get("ENCOUNTER_ID")
        referral_condition = _build_ris_referral_condition(
            row,
            xml_fields,
            source_id=source_id,
            patient_value=patient_value,
            encounter_value=encounter_value,
        )
        reason_reference = _ris_referral_reason_reference(source_id) if referral_condition else None
        if referral_condition:
            referral_conditions.append(referral_condition)
        authored_on = parse_xml_compact_datetime_safe(xml_fields.get("rekvisition_start"))
        priority = _map_ris_priority(xml_fields.get("prioritet"))

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
            extensions.extend(_build_ris_source_meta(row, group, xml_fields))

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
                    priority=priority,
                    category=category,
                    code=ris_code,
                    orderDetail=_build_ris_order_details(group) if ris_code else None,
                    reasonReference=reason_reference,
                    authoredOn=authored_on,
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
    return requests, referral_conditions


def _post_service_requests_to_fhir(
    context: dg.AssetExecutionContext,
    service_requests: list[ServiceRequest],
    fhir_database: FHIRDatabaseResource,
    extracted_count: int,
    mssql: MSSQLResource | None = None,
    resolve_missing_dependencies: bool = True,
    referral_conditions: list[Condition] | None = None,
    reference_fallback_fn: Any | None = None,
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

    # Patienter, der hverken findes i FHIR, kan oprettes fra arkivet eller nås via CPR,
    # kan ikke bære en ServiceRequest (subject er påkrævet). I stedet for at lade den
    # delte reference-resolver vælte hele portionen på den første, tages de berørte
    # ServiceRequests (og deres henvisningsdiagnoser) ud, logges med kildens id og
    # tælles som fejlede: resten af portionen postes, og kørslen fejler først til
    # sidst, så checkpointet ikke rykker, og rækkerne tages igen, når patienten findes.
    dropped_source_ids: list[str] = []
    unresolved_patients = _unresolved_patient_ids(
        patient_ids_needed, fhir_database, reference_fallback_fn
    )
    if unresolved_patients:
        service_requests, referral_conditions, dropped_source_ids = _drop_resources_without_patient(
            service_requests, referral_conditions or [], unresolved_patients
        )
        referral_conditions = referral_conditions or None
        logger.warning(
            "%d ServiceRequest(s) uden gyldig patient taget ud af portionen (%d patient-id'er "
            "findes hverken i FHIR, arkivet eller via CPR). Kilde-id'er: %s",
            len(dropped_source_ids),
            len(unresolved_patients),
            ", ".join(dropped_source_ids[:20]) + (" …" if len(dropped_source_ids) > 20 else ""),
        )

    if referral_conditions:
        referral_conditions = prepare_resources_for_upsert(
            resources=referral_conditions,
            fhir_database=fhir_database,
            resource_type="Condition",
            preferred_id_systems=[SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE],
            reference_configs=PAS_DIAGNOSE_REFERENCE_CONFIGS,
            reference_fallback_fn=reference_fallback_fn,
            logger=logger,
        )
        condition_result = post_resources_direct(
            context=context,
            resources=referral_conditions,
            fhir_database=fhir_database,
            resource_type="Condition",
            source_name="ifdb_oracle",
            extracted_count=extracted_count,
        )
        if _read_result_int(condition_result, "failed_count") > 0:
            return _with_dropped_count(condition_result, dropped_source_ids)

    if not service_requests:
        return _with_dropped_count(
            dg.MaterializeResult(
                metadata={
                    "posted_count": dg.MetadataValue.int(0),
                    "failed_count": dg.MetadataValue.int(0),
                }
            ),
            dropped_source_ids,
        )

    service_requests = prepare_resources_for_upsert(
        resources=service_requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        preferred_id_systems=[SYSTEM_IFDB_HENVISNING_ID, SYSTEM_IFDB_RIS_YDELSE],
        reference_configs=SERVICE_REQUEST_REFERENCE_CONFIGS,
        nested_reference_configs=SERVICE_REQUEST_NESTED_REFERENCE_CONFIGS,
        reference_fallback_fn=reference_fallback_fn,
        logger=logger,
    )

    result = post_resources_direct(
        context=context,
        resources=service_requests,
        fhir_database=fhir_database,
        resource_type="ServiceRequest",
        source_name="ifdb_oracle",
        extracted_count=extracted_count,
    )
    return _with_dropped_count(result, dropped_source_ids)


def _unresolved_patient_ids(
    patient_ids: set[str],
    fhir_database: FHIRDatabaseResource,
    reference_fallback_fn: Any | None,
) -> set[str]:
    """Patient-id'er, der ikke kan løses til en FHIR Patient — hverken direkte
    (efter eventuel oprettelse fra arkivet) eller via CPR-fallback'en."""
    if not patient_ids:
        return set()
    resolved = batch_resolve_references(
        identifiers={pid: "" for pid in patient_ids},
        resource_type="Patient",
        identifier_system=SYSTEM_IFDB_PATIENT_ID,
        fhir_database=fhir_database,
    )
    unresolved = {pid for pid in patient_ids if pid not in resolved}
    if unresolved and reference_fallback_fn is not None:
        supplemental = reference_fallback_fn(
            resource_type="Patient",
            identifier_system=SYSTEM_IFDB_PATIENT_ID,
            values=sorted(unresolved),
        )
        unresolved -= set(supplemental or {})
    return unresolved


def _subject_patient_id(resource: Any) -> str | None:
    subject = getattr(resource, "subject", None)
    identifier = getattr(subject, "identifier", None) if subject is not None else None
    value = getattr(identifier, "value", None) if identifier is not None else None
    return str(value) if value is not None else None


def _source_id(resource: Any) -> str:
    for identifier in getattr(resource, "identifier", None) or []:
        value = getattr(identifier, "value", None)
        if value:
            return str(value)
    return "?"


def _drop_resources_without_patient(
    service_requests: list[ServiceRequest],
    referral_conditions: list[Condition],
    unresolved_patients: set[str],
) -> tuple[list[ServiceRequest], list[Condition], list[str]]:
    """Tag ServiceRequests (og henvisningsdiagnoser) med uløselig patient ud.
    Returnerer (beholdte requests, beholdte conditions, kilde-id'er for de udtagne)."""
    kept_requests = [
        r for r in service_requests if _subject_patient_id(r) not in unresolved_patients
    ]
    dropped_ids = [
        _source_id(r) for r in service_requests if _subject_patient_id(r) in unresolved_patients
    ]
    kept_conditions = [
        c for c in referral_conditions if _subject_patient_id(c) not in unresolved_patients
    ]
    return kept_requests, kept_conditions, dropped_ids


def _with_dropped_count(
    result: dg.MaterializeResult, dropped_source_ids: list[str]
) -> dg.MaterializeResult:
    """Læg de udtagne ServiceRequests til failed_count, så kørslen fejler og checkpointet
    ikke rykker (samme princip som ved fejlede posts)."""
    if not dropped_source_ids:
        return result
    metadata = dict(result.metadata or {})
    failed = _read_result_int(result, "failed_count") + len(dropped_source_ids)
    metadata["failed_count"] = dg.MetadataValue.int(failed)
    metadata["unresolved_patient_skipped"] = dg.MetadataValue.int(len(dropped_source_ids))
    return dg.MaterializeResult(metadata=metadata)


def _read_result_int(result: dg.MaterializeResult, key: str) -> int:
    value = result.metadata.get(key) if result.metadata else None
    raw: Any = getattr(value, "value", value)
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


def _process_henvisning_batch(
    context: dg.AssetExecutionContext,
    batch: list[dict[str, Any]],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    reference_fallback_fn: Any | None,
) -> tuple[int, int]:
    """Transform + post one batch of henvisninger. Returns (posted, failed)."""
    history_by_id, refinfo_by_episode = _build_enrichment_for_batch(batch, mssql)
    service_requests = _transform_henvisning_to_service_requests(
        batch,
        context,
        history_by_id,
        refinfo_by_episode,
    )
    if not service_requests:
        return 0, 0
    # Manglende patienter oprettes fra arc.ifdb_patient, før referencerne løses —
    # samme adfærd som aktivitetsresultat-kæden. Uden dette væltede én henvisning til
    # en patient, der endnu ikke var postet, hele natkæden (subject er påkrævet).
    result = _post_service_requests_to_fhir(
        context=context,
        service_requests=service_requests,
        fhir_database=fhir_database,
        extracted_count=len(batch),
        mssql=mssql,
        resolve_missing_dependencies=True,
        reference_fallback_fn=reference_fallback_fn,
    )
    return _read_result_int(result, "posted_count"), _read_result_int(result, "failed_count")


def _process_ris_batch(
    context: dg.AssetExecutionContext,
    batch: list[dict[str, Any]],
    mssql: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    reference_fallback_fn: Any | None,
) -> tuple[int, int]:
    """Transform + post one batch of RIS-ydelser. Returns (posted, failed).

    NB: on Condition-post failures ``_post_service_requests_to_fhir`` returns
    early WITHOUT posting the batch's ServiceRequests — the failed count must
    reach the asset so the run fails and the checkpoint does not advance
    (otherwise the skipped ServiceRequests would be lost until next touch).
    """
    service_requests, referral_conditions = _transform_ris_ydelse_to_service_requests(
        batch, context
    )
    if not service_requests and not referral_conditions:
        return 0, 0
    result = _post_service_requests_to_fhir(
        context=context,
        service_requests=service_requests,
        fhir_database=fhir_database,
        extracted_count=len(batch),
        mssql=mssql,
        resolve_missing_dependencies=True,
        referral_conditions=referral_conditions or None,
        reference_fallback_fn=reference_fallback_fn,
    )
    return _read_result_int(result, "posted_count"), _read_result_int(result, "failed_count")


def _stream_archive_batches(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    table: str,
    checkpoint: str | None,
    config: RunModeConfig,
    process_batch: Any,
    label: str,
) -> tuple[int, int, int, int]:
    """Stream one archive table in batches through ``process_batch``.

    Returns ``(fetched, posted, failed, batch_count)``.
    """
    # include_deleted=True so retracted rows are tomb-stoned (isDeleted
    # extension updated) rather than left active in the FHIR server.
    query = build_fhir_archive_query(table, checkpoint, config, include_deleted=True)
    context.log.info(f"Querying {table} (mode={config.mode})")

    fetched = 0
    posted = 0
    failed = 0
    batch_num = 0

    with mssql.get_connection() as conn:
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
                fetched += len(batch)
                context.log.info(
                    f"[{label}] Batch {batch_num}: processing {len(batch)} rows "
                    f"(total so far: {fetched})"
                )
                batch_posted, batch_failed = process_batch(batch)
                posted += batch_posted
                failed += batch_failed
        finally:
            cursor.close()

    return fetched, posted, failed, batch_num


# ---------------------------------------------------------------------------
# Single DFM asset — both source streams, one checkpoint
# ---------------------------------------------------------------------------


@dg.asset(
    name="fhir_server_service_request_dfm",
    group_name="fhir_ifdb_dfm",
    compute_kind="fhir",
    deps=[
        "arc_henvisning_ifdb",
        "arc_henvisning_historik_ifdb",
        "arc_referralinformation_ifdb",
        "arc_ris_ydelse_ifdb",
        "fhir_server_patient_dfm",
        "fhir_server_encounter_dfm",
        "fhir_server_eoc_dfm",
    ],
    tags={TAG_CHUNK_SIZE: "400", TAG_MAX_WORKERS: "8"},
    description=(
        "Fetch henvisninger from arc.ifdb_henvisning and RIS-ydelser from "
        "arc.ifdb_ris_ydelse in batches, transform to FHIR ServiceRequest "
        "(status history and referral origin joined per batch from "
        "arc.ifdb_henvisning_historik / arc.ifdb_referralinformation), and post "
        "to the FHIR server. Checkpoint owner for fhir_server.ServiceRequest."
    ),
)
def fhir_server_service_request_dfm(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    fhir_database: FHIRDatabaseResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Batched fetch → transform → post for ServiceRequest resources (DFM)."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "fhir_server",
        "ServiceRequest",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )

    # Patient-id → CPR fallback: resolves subject references to soft-deleted
    # PATIENT_IDs (e.g. a deleted CPR-duplicate) to the active patient posted
    # under the same CPR. Built once and reused for every batch.
    patient_cpr_fallback = build_patient_cpr_fallback(mssql_target_staging, fhir_database)

    try:
        henvisning_fetched, henvisning_posted, henvisning_failed, henvisning_batches = (
            _stream_archive_batches(
                context,
                mssql_target_staging,
                "arc.ifdb_henvisning",
                run_ctx.checkpoint,
                config,
                lambda batch: _process_henvisning_batch(
                    context, batch, mssql_target_staging, fhir_database, patient_cpr_fallback
                ),
                label="henvisning",
            )
        )

        ris_fetched, ris_posted, ris_failed, ris_batches = _stream_archive_batches(
            context,
            mssql_target_staging,
            "arc.ifdb_ris_ydelse",
            run_ctx.checkpoint,
            config,
            lambda batch: _process_ris_batch(
                context, batch, mssql_target_staging, fhir_database, patient_cpr_fallback
            ),
            label="ris_ydelse",
        )

        total_fetched = henvisning_fetched + ris_fetched
        total_posted = henvisning_posted + ris_posted
        total_failed = henvisning_failed + ris_failed
        context.log.info(
            f"Done: {total_fetched} fetched, {total_posted} posted, {total_failed} failed "
            f"({henvisning_batches} henvisning batches, {ris_batches} RIS batches)"
        )

        # Fail BEFORE the checkpoint advances: a Condition-post failure makes
        # _post_service_requests_to_fhir skip the batch's ServiceRequests, so
        # advancing here would silently drop them until the source row is
        # touched again. Failing keeps the window open for the retry.
        if total_failed > 0:
            raise RuntimeError(
                f"{total_failed} resources failed to post "
                f"(henvisning={henvisning_failed}, ris={ris_failed}) — "
                "checkpoint not advanced; rerun after fixing the cause. ServiceRequests "
                "uden gyldig patient står med kilde-id i WARNING-linjerne ovenfor."
            )

        new_checkpoint = advance_fhir_checkpoint(
            dfm, run_ctx, "ServiceRequest", config, total_fetched
        )

        dfm.complete_dfm_run(
            run_ctx.log_id,
            checkpoint_after=new_checkpoint,
            rows_affected=total_posted,
        )

        return dg.MaterializeResult(
            metadata={
                "posted_count": total_posted,
                "failed_count": total_failed,
                "fetched_count": total_fetched,
                "henvisning_posted": henvisning_posted,
                "ris_posted": ris_posted,
                "henvisning_batches": henvisning_batches,
                "ris_batches": ris_batches,
                "mode": config.mode,
            }
        )
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
        raise
