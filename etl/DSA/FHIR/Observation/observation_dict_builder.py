# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Dict Observation builders for AKTIVITETSRESULTAT."""

import logging
import uuid
from datetime import datetime
from typing import Any

from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.utils.code_normalization import is_sks_procedure_code, normalize_sks_coding_code
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.transforms import (
    build_performer_dict,
    parse_clob_datetime,
    to_fhir_instant,
)
from DSA.FHIR.utils.ifdb.transforms import is_true_flag as _is_true_flag
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import source_deleted_to_int as _slettet_to_int
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
)
from DSA.FHIR.utils.ifdb.xml_parsing_workers import _XmlComponentData
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_SOURCE_META,
    EXTENSION_EPISODE_OF_CARE,
    EXTENSION_XML_ELEMENT_META,
    EXTENSION_XML_HIERARCHY,
    EXTENSION_XML_KONTEKST,
    EXTENSION_XML_ROOT_META,
    EXTENSION_XML_ROW_TYPE,
)
from utils.fhir.identifier_systems import (
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_SKS,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict

_date_parse_logger = logging.getLogger(__name__)

FHIR_OBSERVATION_CATEGORY_SYSTEM = "http://terminology.hl7.org/CodeSystem/observation-category"


def _sks_observation_coding_dict(code: Any, display: Any | None = None) -> dict | None:
    """Build one SKS coding dict with IFDB source prefixes stripped."""
    normalized = normalize_sks_coding_code(code)
    if not normalized:
        return None
    coding: dict[str, str] = {"system": SYSTEM_SKS, "code": normalized}
    if display:
        coding["display"] = str(display)
    return coding


IFDB_AKTIVITETTYPE_CATEGORY_SYSTEM = "urn:source:ifdb:aktivitettype_category"


def _to_fhir_datetime_string(value: Any) -> str:
    """Convert a date/datetime value to a FHIR-compliant datetime string."""
    dt_or_str = fhir_datetime(value)
    return dt_or_str.isoformat() if isinstance(dt_or_str, datetime) else str(dt_or_str)


def _build_category_dict(aktivitettype_kode: str | None) -> list[dict]:
    """Map AKTIVITETTYPE_KODE to Observation.category using standard FHIR codes.

    Returns a list of category CodeableConcept dicts:
      1) Standard FHIR category (survey/procedure/exam)
      2) Raw LEVEL1 value with a custom system for precise downstream filtering
    """
    if not aktivitettype_kode:
        return []

    kode = str(aktivitettype_kode).strip()
    if not kode:
        return []

    if is_sks_procedure_code(kode):
        fhir_code = "procedure"
    elif kode.startswith("SFI"):
        fhir_code = "survey"
    else:
        fhir_code = "exam"

    level1 = kode.split(".")[0] if "." in kode else kode

    return [
        {
            "coding": [
                {"system": FHIR_OBSERVATION_CATEGORY_SYSTEM, "code": fhir_code},
                {"system": IFDB_AKTIVITETTYPE_CATEGORY_SYSTEM, "code": level1},
            ]
        }
    ]


def _build_lossless_component_dict(row: AktivitetsresultatRow) -> dict:
    """Build one ObservationComponent as a plain dict -- all C# fields preserved."""
    comp_exts: list[dict] = []

    if row.element_element_id:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#elementId",
                "valueString": row.element_element_id,
            }
        )
    if row.element_kode:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#kode",
                "valueString": row.element_kode,
            }
        )
    if row.element_original_kode:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#originalkode",
                "valueString": row.element_original_kode,
            }
        )
    if row.element_administrativ_enhed_id:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#administrativEnhedId",
                "valueString": row.element_administrativ_enhed_id,
            }
        )
    if row.element_pers_id:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#persId",
                "valueString": row.element_pers_id,
            }
        )
    if row.element_moid:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#moid",
                "valueString": row.element_moid,
            }
        )
    if row.element_type:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#type",
                "valueString": row.element_type,
            }
        )
    if row.element_ledetekst:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_ELEMENT_META}#ledetekst",
                "valueString": row.element_ledetekst,
            }
        )

    if row.hierarki:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#hierarki",
                "valueString": row.hierarki,
            }
        )
    if row.klynge_id is not None:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeId",
                "valueInteger": row.klynge_id,
            }
        )
    if row.klynge_id_hierarki:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeIdHierarki",
                "valueString": row.klynge_id_hierarki,
            }
        )
    if row.klynge_niveau:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeNiveau",
                "valueInteger": row.klynge_niveau,
            }
        )
    if row.klynge_navne:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeNavne",
                "valueString": row.klynge_navne,
            }
        )
    if row.klynge_ledetekster:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeLedetekster",
                "valueString": row.klynge_ledetekster,
            }
        )
    if row.klynge_parent_id is not None:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeParentId",
                "valueInteger": row.klynge_parent_id,
            }
        )
    if row.klynge_navn:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeNavn",
                "valueString": row.klynge_navn,
            }
        )
    if row.klynge_ledetekst:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeLedetekst",
                "valueString": row.klynge_ledetekst,
            }
        )
    if row.klynge_valg_ledetekst:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeValgLedetekst",
                "valueString": row.klynge_valg_ledetekst,
            }
        )
    if row.klynge_type:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#klyngeType",
                "valueString": row.klynge_type,
            }
        )
    if row.repetition_id is not None:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#repetitionId",
                "valueInteger": row.repetition_id,
            }
        )
    if row.repetition_id_hierarki:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#repetitionIdHierarki",
                "valueString": row.repetition_id_hierarki,
            }
        )
    if row.repetition_niveau:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#repetitionNiveau",
                "valueInteger": row.repetition_niveau,
            }
        )
    if row.element_nummer is not None:
        comp_exts.append(
            {
                "url": f"{EXTENSION_XML_HIERARCHY}#elementNummer",
                "valueInteger": row.element_nummer,
            }
        )

    if row.raekke_type:
        comp_exts.append({"url": EXTENSION_XML_ROW_TYPE, "valueString": row.raekke_type})

    code_cc: dict | None = None
    if row.element_kode:
        code_cc = {
            "coding": [{"code": row.element_kode, "display": row.element_navn}],
            "text": row.element_navn,
        }
    elif row.element_navn:
        code_cc = {"text": row.element_navn}

    component: dict = {"code": code_cc or {"text": row.klynge_navn}}
    if row.element_vaerdi:
        component["valueString"] = row.element_vaerdi
    if comp_exts:
        component["extension"] = comp_exts
    return component


def _build_kontekst_extensions_dict(
    kontekst: AktivitetskontekstRow | None,
) -> list[dict]:
    """Map Kontekst provenance fields to plain extension dicts."""
    if kontekst is None:
        return []

    parts: list[dict] = []

    def _add_actor(
        prefix: str,
        bruger: str | None,
        pers_id: str | None,
        rolle: str | None,
        adm_enhed: str | None,
        adm_enhed_id: str | None,
        tidspunkt: Any,
        tidsstempel: int,
    ) -> None:
        if not any([bruger, pers_id, rolle, adm_enhed]):
            return
        sub: list[dict] = []
        if bruger:
            sub.append({"url": "bruger", "valueString": bruger})
        if pers_id:
            sub.append({"url": "persId", "valueString": pers_id})
        if rolle:
            sub.append({"url": "rolle", "valueString": rolle})
        if adm_enhed:
            sub.append({"url": "administrativEnhed", "valueString": adm_enhed})
        if adm_enhed_id:
            sub.append({"url": "administrativEnhedId", "valueString": adm_enhed_id})
        if tidspunkt:
            ts_str = tidspunkt.isoformat() if hasattr(tidspunkt, "isoformat") else str(tidspunkt)
            sub.append({"url": "tidspunkt", "valueString": ts_str})
        if tidsstempel:
            sub.append({"url": "tidsstempel", "valueInteger": tidsstempel})
        parts.append({"url": f"{EXTENSION_XML_KONTEKST}#{prefix}", "extension": sub})

    _add_actor(
        "oprettet",
        kontekst.oprettet_bruger,
        kontekst.oprettet_pers_id,
        kontekst.oprettet_rolle,
        kontekst.oprettet_adm_enhed,
        kontekst.oprettet_adm_enhed_id,
        kontekst.oprettet_tidspunkt,
        kontekst.oprettet_tidsstempel,
    )
    _add_actor(
        "aendret",
        kontekst.aendret_bruger,
        kontekst.aendret_pers_id,
        kontekst.aendret_rolle,
        kontekst.aendret_adm_enhed,
        kontekst.aendret_adm_enhed_id,
        kontekst.aendret_tidspunkt,
        kontekst.aendret_tidsstempel,
    )
    _add_actor(
        "besluttet",
        kontekst.besluttet_bruger,
        kontekst.besluttet_pers_id,
        kontekst.besluttet_rolle,
        kontekst.besluttet_adm_enhed,
        kontekst.besluttet_adm_enhed_id,
        kontekst.besluttet_tidspunkt,
        kontekst.besluttet_tidsstempel,
    )
    if kontekst.status:
        parts.append({"url": f"{EXTENSION_XML_KONTEKST}#status", "valueString": kontekst.status})
    return parts


def _build_source_meta_dict(row_dict: dict[str, Any]) -> list[dict]:
    """Build aktivitetsresultatSourceMeta extension dict from row-level columns."""
    parts: list[dict] = []

    def _add(key: str, val: Any) -> None:
        if val is None:
            return
        s = str(val).strip()
        if s:
            parts.append({"url": key, "valueString": s})

    _add("udfoertBrugerrolle", row_dict.get("UDFOERT_BRUGERROLLE"))
    _add("udfoertBrugerrolleNavn", row_dict.get("UDFOERT_BRUGERROLLE_NAME"))
    _add("udfoertAdmenhedKode", row_dict.get("UDFOERT_ADMENHED_KODE"))
    _add("udfoertAdmenhedNavn", row_dict.get("UDFOERT_ADMENHED_NAVN"))
    _add("udfoertAdmenhedOrgkode", row_dict.get("UDFOERT_ADMENHED_ORGKODE"))
    _add("udfoertAdmenhedSorcode", row_dict.get("UDFOERT_ADMENHED_SORCODE"))
    _add("modtidspunkt", row_dict.get("MODTIDSPUNKT"))
    _add("tilstedevaerselseId", row_dict.get("TILSTEDEVAERELSE_ID"))
    _add("udfoerelsesId", row_dict.get("UDFOERELSE_ID"))
    _add("hierarkiskPlacering", row_dict.get("HIERARKISK_PLACERING"))
    _add("domaenenavn", row_dict.get("DOMAENENAVN"))
    _add("modelleringsNavn", row_dict.get("MODELLERINGSNAVN"))
    _add("modelleringsVersion", row_dict.get("MODELLERINGSVERSION"))
    _add("resultType", row_dict.get("RESULT_TYPE"))
    _add("resultMoid", row_dict.get("RESULT_MOID"))

    lpr3 = row_dict.get("LPR3_IGNORED")
    if lpr3 is not None:
        parts.append({"url": "lpr3Ignored", "valueBoolean": _is_true_flag(lpr3)})

    if not parts:
        return []
    return [{"url": EXTENSION_AKTIVITETSRESULTAT_SOURCE_META, "extension": parts}]


def _build_observation_lossless_dict(
    row_dict: dict[str, Any],
    xml_rows: list[AktivitetsresultatRow] | None,
    kontekst: AktivitetskontekstRow | None,
    resolved: ResolvedRefLookup | None = None,
) -> tuple[str, dict]:
    """Build one Observation dict preserving ALL C# fields.

    Returns ``(resource_id, observation_dict)``.  When *resolved* is provided
    references are emitted in Aidbox-native ``{id, resourceType}`` format;
    otherwise identifier-based references are used (useful for regression
    testing against the Pydantic path).
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
    result_type_code = row_dict.get("RESULTATTYPE_KODE")
    result_type_text = row_dict.get("RESULTATTYPE_KODETEKST")
    title = row_dict.get("OVERSKRIFT")
    kladde = row_dict.get("KLADDE")
    period_start = row_dict.get("PERIOD_START")
    period_end = row_dict.get("PERIOD_END")
    slettet = row_dict.get("SLETTET")

    is_draft = _is_true_flag(kladde)
    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    status = "entered-in-error" if is_deleted else ("preliminary" if is_draft else "final")

    start_str: str | None = None
    end_str: str | None = None

    if period_start:
        try:
            if isinstance(period_start, str):
                parsed = parse_clob_datetime(period_start)
                start_str = (
                    _to_fhir_datetime_string(parsed)
                    if parsed
                    else _to_fhir_datetime_string(period_start)
                )
            else:
                start_str = _to_fhir_datetime_string(period_start)
        except Exception as exc:
            _date_parse_logger.warning(
                "PERIOD_START parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )
    if period_end:
        try:
            if isinstance(period_end, str):
                parsed_end = parse_clob_datetime(period_end)
                end_str = (
                    _to_fhir_datetime_string(parsed_end)
                    if parsed_end
                    else _to_fhir_datetime_string(period_end)
                )
            else:
                end_str = _to_fhir_datetime_string(period_end)
        except Exception as exc:
            _date_parse_logger.warning(
                "PERIOD_END parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    resource_id = resolved.observation_id(objekt_id_norm) if resolved else str(uuid.uuid4())

    obs: dict[str, Any] = {"resourceType": "Observation", "id": resource_id}

    if objekt_id_norm is not None:
        obs["identifier"] = [{"system": SYSTEM_IFDB_AKTIVITETSRESULTAT, "value": objekt_id_norm}]

    obs["status"] = status

    cat = _build_category_dict(activity_type_code)
    if cat:
        obs["category"] = cat

    codings: list[dict] = []
    if activity_coding := _sks_observation_coding_dict(activity_type_code, activity_type_text):
        codings.append(activity_coding)
    if result_coding := _sks_observation_coding_dict(result_type_code, result_type_text):
        codings.append(result_coding)

    code_dict: dict[str, Any] = {}
    if codings:
        code_dict["coding"] = codings
    if title:
        code_dict["text"] = str(title)
    if code_dict:
        obs["code"] = code_dict

    if patient_id_norm is not None:
        if resolved:
            obs["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            obs["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            obs["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            obs["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if start_str and end_str:
        obs["effectivePeriod"] = {"start": start_str, "end": end_str}
    elif start_str:
        obs["effectiveDateTime"] = start_str

    udfoert_tidspunkt = row_dict.get("UDFOERT_TIDSPUNKT")
    if udfoert_tidspunkt:
        try:
            if isinstance(udfoert_tidspunkt, str):
                parsed_issued = parse_clob_datetime(udfoert_tidspunkt)
                instant = to_fhir_instant(parsed_issued or udfoert_tidspunkt)
            else:
                instant = to_fhir_instant(udfoert_tidspunkt)
            if instant:
                obs["issued"] = instant
        except Exception as exc:
            _date_parse_logger.warning(
                "UDFOERT_TIDSPUNKT parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    extensions: list[dict] = [build_is_deleted_extension_dict(deleted_int)]
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

    root_meta_parts: list[dict] = []
    if xml_rows:
        first = xml_rows[0]
        if first.overskrift:
            root_meta_parts.append({"url": "overskrift", "valueString": first.overskrift})
        if first.ledetekst:
            root_meta_parts.append({"url": "ledetekst", "valueString": first.ledetekst})
        if first.type:
            root_meta_parts.append({"url": "type", "valueString": first.type})
        if first.kladde:
            root_meta_parts.append({"url": "kladde", "valueString": first.kladde})
    if root_meta_parts:
        extensions.append({"url": EXTENSION_XML_ROOT_META, "extension": root_meta_parts})

    kontekst_exts = _build_kontekst_extensions_dict(kontekst)
    extensions.extend(kontekst_exts)

    source_meta = _build_source_meta_dict(row_dict)
    extensions.extend(source_meta)

    if extensions:
        obs["extension"] = extensions

    performer_refs = build_performer_dict(row_dict)
    if performer_refs:
        obs["performer"] = performer_refs

    components: list[dict] = []
    if xml_rows:
        for xr in xml_rows:
            if xr.xml_status == -1:
                continue
            if xr.raekke_type == "Tom":
                continue
            components.append(_build_lossless_component_dict(xr))
    if components:
        obs["component"] = components

    return resource_id, obs


def _build_observation_from_row_dict(
    row_dict: dict[str, Any],
    component_data: list[_XmlComponentData] | None,
    resolved: ResolvedRefLookup | None = None,
) -> tuple[str, dict]:
    """Build one Observation dict from fast-path (Navn,Vaerdi) tuples."""
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
    result_type_code = row_dict.get("RESULTATTYPE_KODE")
    result_type_text = row_dict.get("RESULTATTYPE_KODETEKST")
    title = row_dict.get("OVERSKRIFT")
    kladde = row_dict.get("KLADDE")
    period_start = row_dict.get("PERIOD_START")
    slettet = row_dict.get("SLETTET")

    is_draft = _is_true_flag(kladde)
    deleted_int = _slettet_to_int(slettet)
    is_deleted = deleted_int != 0
    status = "entered-in-error" if is_deleted else ("preliminary" if is_draft else "final")

    effective_dt: str | None = None
    if period_start:
        try:
            if isinstance(period_start, str):
                parsed = parse_clob_datetime(period_start)
                effective_dt = (
                    _to_fhir_datetime_string(parsed)
                    if parsed
                    else _to_fhir_datetime_string(period_start)
                )
            else:
                effective_dt = _to_fhir_datetime_string(period_start)
        except Exception as exc:
            _date_parse_logger.warning(
                "PERIOD_START parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    resource_id = resolved.observation_id(objekt_id_norm) if resolved else str(uuid.uuid4())

    obs: dict[str, Any] = {"resourceType": "Observation", "id": resource_id}

    if objekt_id_norm is not None:
        obs["identifier"] = [{"system": SYSTEM_IFDB_AKTIVITETSRESULTAT, "value": objekt_id_norm}]

    obs["status"] = status

    cat = _build_category_dict(activity_type_code)
    if cat:
        obs["category"] = cat

    codings: list[dict] = []
    if activity_coding := _sks_observation_coding_dict(activity_type_code, activity_type_text):
        codings.append(activity_coding)
    if result_coding := _sks_observation_coding_dict(result_type_code, result_type_text):
        codings.append(result_coding)

    code_dict: dict[str, Any] = {}
    if codings:
        code_dict["coding"] = codings
    if title:
        code_dict["text"] = str(title)
    if code_dict:
        obs["code"] = code_dict

    if patient_id_norm is not None:
        if resolved:
            obs["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            obs["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            obs["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            obs["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if effective_dt is not None:
        obs["effectiveDateTime"] = effective_dt

    udfoert_tidspunkt = row_dict.get("UDFOERT_TIDSPUNKT")
    if udfoert_tidspunkt:
        try:
            if isinstance(udfoert_tidspunkt, str):
                parsed_issued = parse_clob_datetime(udfoert_tidspunkt)
                instant = to_fhir_instant(parsed_issued or udfoert_tidspunkt)
            else:
                instant = to_fhir_instant(udfoert_tidspunkt)
            if instant:
                obs["issued"] = instant
        except Exception as exc:
            _date_parse_logger.warning(
                "UDFOERT_TIDSPUNKT parse failed for OBJEKT_ID=%s: %s",
                objekt_id,
                exc,
            )

    extensions: list[dict] = [build_is_deleted_extension_dict(deleted_int)]
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
    if extensions:
        obs["extension"] = extensions

    components: list[dict] = []
    if component_data:
        for comp_text, comp_value in component_data:
            comp: dict = {"code": {"text": comp_text}}
            if comp_value:
                comp["valueString"] = comp_value
            components.append(comp)
    if components:
        obs["component"] = components

    return resource_id, obs
