# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Condition builders for AKTIVITETSRESULTAT rows with SKS diagnosis codes."""

import uuid
from typing import Any

from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Aktivitetsresultat.routing_glue import _audit_text
from DSA.FHIR.utils.code_normalization import (
    extract_xml_clinical_diagnosis_code,
    to_sks_diagnosis_code,
)
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.routing import TargetResource
from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from DSA.FHIR.utils.ifdb.transforms import (
    is_true_flag,
    parse_clob_datetime,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import source_deleted_to_int as _slettet_to_int
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from utils.fhir.condition_categories import (
    CODE_ENCOUNTER_DIAGNOSIS,
    SYSTEM_CONDITION_CATEGORY_HL7,
)
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_CONDITION_SOURCE_META,
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_MODTIDSPUNKT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS_D,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict
from utils.fhir.resource_flags import FLAG_LPR3_IGNORED, add_flag_tags_dict

_CONDITION_CLINICAL_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-clinical"
_CONDITION_VERIFICATION_STATUS_SYSTEM = "http://terminology.hl7.org/CodeSystem/condition-ver-status"

_ENCOUNTER_DIAGNOSIS_CATEGORY = [
    {
        "coding": [
            {
                "system": SYSTEM_CONDITION_CATEGORY_HL7,
                "code": CODE_ENCOUNTER_DIAGNOSIS,
            }
        ]
    }
]


def _parse_period_datetime(value: Any) -> str | None:
    if not value:
        return None
    try:
        if isinstance(value, str):
            parsed = parse_clob_datetime(value)
            return str(fhir_datetime(parsed)) if parsed else str(fhir_datetime(value))
        return str(fhir_datetime(value))
    except Exception:
        return None


def _build_aktivitetsresultat_condition_dict(
    row_dict: dict[str, Any],
    resolved: ResolvedRefLookup | None = None,
) -> tuple[str, dict]:
    """Build a Condition dict from an AKTIVITETSRESULTAT row with SKS diagnosis code.

    Only called for rows where ``is_sks_diagnosis_code(AKTIVITETTYPE_KODE)``
    is True. The caller owns whether Condition is emitted as a side projection
    or as an exclusive target.

    Returns ``(resource_id, condition_dict)``.
    """
    objekt_id = row_dict.get("OBJEKT_ID")
    patient_id = row_dict.get("PATIENT_ID")
    encounter_id = row_dict.get("ENCOUNTER_ID")
    episode_id = row_dict.get("EPISODE_OF_CARE_ID")
    objekt_id_norm = _normalize_oracle_id(objekt_id)
    patient_id_norm = _normalize_oracle_id(patient_id)
    encounter_id_norm = _normalize_oracle_id(encounter_id)
    episode_id_norm = _normalize_oracle_id(episode_id)
    activity_type_code = row_dict.get("AKTIVITETTYPE_KODE")
    activity_type_text = row_dict.get("AKTIVITETTYPE_KODETEKST")
    period_start = row_dict.get("PERIOD_START")
    period_end = row_dict.get("PERIOD_END")
    slettet = row_dict.get("SLETTET")

    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    has_end = period_end is not None

    # Kladde betyder at dokumentationen ikke er faerdiggjort. Uden dette led
    # taeller en kladdediagnose som gaeldende, og aftagerne kan ikke filtrere
    # den fra. Samme trappe som resten af familien: slettet slaar kladde.
    is_draft = is_true_flag(row_dict.get("KLADDE"))
    if is_deleted:
        clinical_status_code = None
        verification_status_code = "entered-in-error"
    elif has_end:
        clinical_status_code = "resolved"
        verification_status_code = "provisional" if is_draft else "confirmed"
    else:
        clinical_status_code = "active"
        verification_status_code = "provisional" if is_draft else "confirmed"

    onset_str = _parse_period_datetime(period_start)
    abatement_str = _parse_period_datetime(period_end) if has_end and not is_deleted else None
    recorded_str = _parse_period_datetime(row_dict.get("MODTIDSPUNKT"))

    resource_id = resolved.condition_id(objekt_id_norm) if resolved else str(uuid.uuid4())

    cond: dict[str, Any] = {"resourceType": "Condition", "id": resource_id}

    if objekt_id_norm is not None:
        cond["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
                "value": objekt_id_norm,
            }
        ]

    if clinical_status_code:
        cond["clinicalStatus"] = {
            "coding": [
                {
                    "system": _CONDITION_CLINICAL_STATUS_SYSTEM,
                    "code": clinical_status_code,
                }
            ]
        }

    cond["verificationStatus"] = {
        "coding": [
            {
                "system": _CONDITION_VERIFICATION_STATUS_SYSTEM,
                "code": verification_status_code,
            }
        ]
    }

    code_dict: dict[str, Any] = {}
    if activity_type_code:
        coding_code = to_sks_diagnosis_code(activity_type_code)
        if coding_code:
            coding: dict[str, str | bool] = {
                "system": SYSTEM_SKS_D,
                "code": str(coding_code),
                "userSelected": True,
            }
            if activity_type_text:
                coding["display"] = str(activity_type_text)
            code_dict["coding"] = [coding]
        if activity_type_text:
            code_dict["text"] = str(activity_type_text)
    if code_dict:
        cond["code"] = code_dict

    cond["category"] = _ENCOUNTER_DIAGNOSIS_CATEGORY

    if patient_id_norm is not None:
        if resolved:
            cond["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            cond["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            cond["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            cond["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if onset_str:
        cond["onsetDateTime"] = onset_str
    if abatement_str:
        cond["abatementDateTime"] = abatement_str
    if recorded_str:
        cond["recordedDate"] = recorded_str

    extensions: list[dict] = [build_is_deleted_extension_dict(deleted_int)]
    if recorded_str:
        # Samme datetime-extension som de oevrige diagnosespor -- uden den er
        # den flade modtidspunkt-kolonne NULL for SFI-diagnoser.
        extensions.append({"url": EXTENSION_MODTIDSPUNKT, "valueDateTime": recorded_str})

    if episode_id_norm:
        if resolved:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": resolved.episode_ref(episode_id_norm),
                }
            )
        else:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": {
                        "identifier": {
                            "system": SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            "value": episode_id_norm,
                        }
                    },
                }
            )

    source_parts: list[dict] = []

    def _add(key: str, val: Any) -> None:
        if val is None:
            return
        s = str(val).strip()
        if s:
            source_parts.append({"url": key, "valueString": s})

    _add("udfoertBrugerrolle", row_dict.get("UDFOERT_BRUGERROLLE"))
    _add("udfoertBrugerrolleNavn", row_dict.get("UDFOERT_BRUGERROLLE_NAME"))
    _add("udfoertAdmenhedKode", row_dict.get("UDFOERT_ADMENHED_KODE"))
    _add("udfoertAdmenhedNavn", row_dict.get("UDFOERT_ADMENHED_NAVN"))
    _add("udfoertAdmenhedOrgkode", row_dict.get("UDFOERT_ADMENHED_ORGKODE"))
    _add("udfoertAdmenhedSorcode", row_dict.get("UDFOERT_ADMENHED_SORCODE"))
    _add("modtidspunkt", row_dict.get("MODTIDSPUNKT"))

    if source_parts:
        extensions.append(
            {
                "url": EXTENSION_AKTIVITETSRESULTAT_CONDITION_SOURCE_META,
                "extension": source_parts,
            }
        )

    if extensions:
        cond["extension"] = extensions
    if is_true_flag(row_dict.get("LPR3_IGNORED")):
        # Samme soegbare maerke som Procedure -- flaget sidder paa samme kilderaekke.
        add_flag_tags_dict(cond, [FLAG_LPR3_IGNORED])

    return resource_id, cond


def _build_xml_diagnosis_condition_dict(
    row_dict: dict[str, Any],
    xml_row: AktivitetsresultatRow,
    diagnosis_code: str,
    resolved: ResolvedRefLookup | None = None,
) -> tuple[str, dict]:
    """Build a Condition dict from a structured ``DiagnoseKode`` XML element."""
    objekt_id_norm = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
    patient_id_norm = _normalize_oracle_id(row_dict.get("PATIENT_ID"))
    encounter_id_norm = _normalize_oracle_id(row_dict.get("ENCOUNTER_ID"))
    episode_id_norm = _normalize_oracle_id(row_dict.get("EPISODE_OF_CARE_ID"))
    period_start = row_dict.get("PERIOD_START")
    period_end = row_dict.get("PERIOD_END")
    slettet = row_dict.get("SLETTET")

    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    has_end = period_end is not None

    # Kladde betyder at dokumentationen ikke er faerdiggjort. Uden dette led
    # taeller en kladdediagnose som gaeldende, og aftagerne kan ikke filtrere
    # den fra. Samme trappe som resten af familien: slettet slaar kladde.
    is_draft = is_true_flag(row_dict.get("KLADDE"))
    if is_deleted:
        clinical_status_code = None
        verification_status_code = "entered-in-error"
    elif has_end:
        clinical_status_code = "resolved"
        verification_status_code = "provisional" if is_draft else "confirmed"
    else:
        clinical_status_code = "active"
        verification_status_code = "provisional" if is_draft else "confirmed"

    onset_str = _parse_period_datetime(period_start)
    abatement_str = _parse_period_datetime(period_end) if has_end and not is_deleted else None
    recorded_str = _parse_period_datetime(row_dict.get("MODTIDSPUNKT"))

    element_nummer = xml_row.element_nummer
    if resolved is not None and objekt_id_norm is not None and element_nummer is not None:
        resource_id = resolved.xml_condition_id(objekt_id_norm, element_nummer)
    else:
        resource_id = str(uuid.uuid4())

    cond: dict[str, Any] = {"resourceType": "Condition", "id": resource_id}

    if objekt_id_norm is not None and element_nummer is not None:
        cond["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
                "value": f"{objekt_id_norm}/{element_nummer}",
            }
        ]

    if clinical_status_code:
        cond["clinicalStatus"] = {
            "coding": [
                {
                    "system": _CONDITION_CLINICAL_STATUS_SYSTEM,
                    "code": clinical_status_code,
                }
            ]
        }

    cond["verificationStatus"] = {
        "coding": [
            {
                "system": _CONDITION_VERIFICATION_STATUS_SYSTEM,
                "code": verification_status_code,
            }
        ]
    }

    coding_code = to_sks_diagnosis_code(diagnosis_code)
    display = _audit_text(xml_row.element_ledetekst) or _audit_text(xml_row.element_vaerdi)
    if display == coding_code:
        display = None
    code_dict: dict[str, Any] = {}
    if coding_code:
        coding: dict[str, str | bool] = {
            "system": SYSTEM_SKS_D,
            "code": str(coding_code),
            "userSelected": True,
        }
        if display:
            coding["display"] = display
        code_dict["coding"] = [coding]
        if display:
            code_dict["text"] = display
    if code_dict:
        cond["code"] = code_dict

    cond["category"] = _ENCOUNTER_DIAGNOSIS_CATEGORY

    if patient_id_norm is not None:
        if resolved:
            cond["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            cond["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            cond["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            cond["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if onset_str:
        cond["onsetDateTime"] = onset_str
    if abatement_str:
        cond["abatementDateTime"] = abatement_str
    if recorded_str:
        cond["recordedDate"] = recorded_str

    extensions: list[dict] = [build_is_deleted_extension_dict(deleted_int)]
    if recorded_str:
        # Samme datetime-extension som de oevrige diagnosespor -- uden den er
        # den flade modtidspunkt-kolonne NULL for SFI-diagnoser.
        extensions.append({"url": EXTENSION_MODTIDSPUNKT, "valueDateTime": recorded_str})

    if episode_id_norm:
        if resolved:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": resolved.episode_ref(episode_id_norm),
                }
            )
        else:
            extensions.append(
                {
                    "url": EXTENSION_EPISODE_OF_CARE,
                    "valueReference": {
                        "identifier": {
                            "system": SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                            "value": episode_id_norm,
                        }
                    },
                }
            )

    source_parts: list[dict] = []

    def _add(key: str, val: Any) -> None:
        if val is None:
            return
        s = str(val).strip()
        if s:
            source_parts.append({"url": key, "valueString": s})

    _add("elementNavn", xml_row.element_navn)
    _add("elementNummer", element_nummer)
    _add("elementElementId", xml_row.element_element_id)
    _add("udfoertBrugerrolle", row_dict.get("UDFOERT_BRUGERROLLE"))
    _add("udfoertBrugerrolleNavn", row_dict.get("UDFOERT_BRUGERROLLE_NAME"))
    _add("modtidspunkt", row_dict.get("MODTIDSPUNKT"))

    if source_parts:
        extensions.append(
            {
                "url": EXTENSION_AKTIVITETSRESULTAT_CONDITION_SOURCE_META,
                "extension": source_parts,
            }
        )

    if extensions:
        cond["extension"] = extensions
    if is_true_flag(row_dict.get("LPR3_IGNORED")):
        # Samme soegbare maerke som Procedure -- flaget sidder paa samme kilderaekke.
        add_flag_tags_dict(cond, [FLAG_LPR3_IGNORED])

    return resource_id, cond


def _append_xml_diagnosis_condition_dicts(
    row_dict: dict[str, Any],
    xml_rows: list[AktivitetsresultatRow] | None,
    resolved: ResolvedRefLookup | None,
    sink: list[tuple[str, dict]],
) -> None:
    """Emit Condition dicts for structured ``DiagnoseKode`` XML elements in primary flow."""
    if not xml_rows:
        return
    for xml_row in xml_rows:
        if xml_row.xml_status == -1:
            continue
        if xml_row.raekke_type != "Element":
            continue
        diagnosis_code = extract_xml_clinical_diagnosis_code(
            element_navn=xml_row.element_navn,
            element_kode=xml_row.element_kode,
            element_vaerdi=xml_row.element_vaerdi,
            element_original_kode=xml_row.element_original_kode,
        )
        if not diagnosis_code:
            continue
        resource_id, cond = _build_xml_diagnosis_condition_dict(
            row_dict,
            xml_row,
            diagnosis_code,
            resolved,
        )
        sink.append((resource_id, cond))


def _append_condition_routing_decision(
    sink: list[dict[str, str | None]] | None,
    row_dict: dict[str, Any],
    *,
    reason: str | None = None,
) -> None:
    if sink is None:
        return

    ifdb_aktivitet_id = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
    if not ifdb_aktivitet_id:
        return

    sink.append(
        {
            "ifdb_aktivitet_id": ifdb_aktivitet_id,
            "aktivitetstype_kode": _audit_text(row_dict.get("AKTIVITETTYPE_KODE")),
            "target_resource": "Condition",
            "observation_category": None,
            "routing_source": "diagnosis_code",
            "routing_confidence": "high",
            "routing_reason": (
                reason
                or "AKTIVITETTYPE_KODE matched IFDB diagnosis code rules; "
                "Condition emitted, and no value-bearing XML was routed to "
                "QuestionnaireResponse/Observation."
            ),
        }
    )


def _decision_with_parallel_condition(decision: RoutingDecision) -> RoutingDecision:
    """Preserve the XML route while making the audit reason explicit."""
    return RoutingDecision(
        target_resource=decision.target_resource,
        observation_category=decision.observation_category,
        routing_source=decision.routing_source,
        routing_confidence=decision.routing_confidence,
        routing_reason=(
            f"{decision.routing_reason} Condition emitted in parallel because "
            "AKTIVITETTYPE_KODE matched IFDB diagnosis code rules; XML values "
            f"preserved as {decision.target_resource.value}."
        ),
    )


def _condition_observation_without_numeric_values_decision() -> RoutingDecision:
    return RoutingDecision(
        target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
        observation_category=None,
        routing_source="xml_fallback",
        routing_confidence="low",
        routing_reason=(
            "Matched Observation route for a diagnosis-coded measurement, but "
            "no numeric SFI datapoints were emitted; defaulted XML preservation "
            "to QuestionnaireResponse."
        ),
    )
