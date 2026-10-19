# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Procedure builders for AKTIVITETSRESULTAT rows with SKS procedure codes."""

import copy
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from DSA.FHIR.Aktivitetsresultat.references import ResolvedRefLookup
from DSA.FHIR.Aktivitetsresultat.routing_glue import _audit_text
from DSA.FHIR.utils.code_normalization import normalize_sks_coding_code
from DSA.FHIR.utils.fhir_utils import fhir_datetime
from DSA.FHIR.utils.ifdb.routing import TargetResource
from DSA.FHIR.utils.ifdb.routing_fallback import RoutingDecision
from DSA.FHIR.utils.ifdb.sks_vaerdi_tillaegskoder import (
    convert_value_tillaegskode,
    is_value_tillaegskode_element,
)
from DSA.FHIR.utils.ifdb.transforms import (
    build_performer_dict,
    is_true_flag,
    parse_clob_datetime,
)
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from DSA.FHIR.utils.ifdb.transforms import source_deleted_to_int as _slettet_to_int
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from DSA.FHIR.utils.ifdb.xml_child_reconcile import FAMILY_DELOP
from DSA.FHIR.utils.sql_on_fhir.period_keys import DW_TIMEZONE
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
    EXTENSION_EPISODE_OF_CARE,
)
from utils.fhir.identifier_systems import (
    PERFORMER_FUNCTION_UDFOERENDE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_STAFF,
    SYSTEM_PERFORMER_FUNCTION,
    SYSTEM_SKS,
    SYSTEM_SOR,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension_dict
from utils.fhir.resource_flags import FLAG_LPR3_IGNORED, add_flag_tags_dict


def _build_aktivitetsresultat_procedure_dict(
    row_dict: dict[str, Any],
    resolved: ResolvedRefLookup | None = None,
    xml_supplements: list[tuple[str, str | None]] | None = None,
) -> tuple[str, dict]:
    """Build a Procedure dict from an AKTIVITETSRESULTAT row with SKS procedure code.

    Only called for rows where ``is_sks_procedure_code`` is True. The caller
    owns whether Procedure is emitted as a side projection or as an exclusive
    target.

    Returns ``(resource_id, procedure_dict)``.
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
    # Kladde betyder at dokumentationen ikke er faerdiggjort. Uden dette led
    # taeller en kladde som en gennemfoert procedure, og aftagerne kan ikke
    # filtrere den fra. Samme trappe som skemasvar og observationer bruger:
    # slettet slaar kladde, kladde slaar faerdig.
    is_draft = is_true_flag(row_dict.get("KLADDE"))
    if is_deleted:
        status = "entered-in-error"
    elif is_draft:
        status = "in-progress"
    else:
        status = "completed"

    start_str: str | None = None
    end_str: str | None = None
    if period_start:
        try:
            if isinstance(period_start, str):
                parsed = parse_clob_datetime(period_start)
                start_str = (
                    str(fhir_datetime(parsed)) if parsed else str(fhir_datetime(period_start))
                )
            else:
                start_str = str(fhir_datetime(period_start))
        except Exception:
            pass
    if period_end:
        try:
            if isinstance(period_end, str):
                parsed_end = parse_clob_datetime(period_end)
                end_str = (
                    str(fhir_datetime(parsed_end)) if parsed_end else str(fhir_datetime(period_end))
                )
            else:
                end_str = str(fhir_datetime(period_end))
        except Exception:
            pass

    resource_id = resolved.procedure_id(objekt_id_norm) if resolved else str(uuid.uuid4())

    proc: dict[str, Any] = {"resourceType": "Procedure", "id": resource_id}

    if objekt_id_norm is not None:
        proc["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
                "value": objekt_id_norm,
            }
        ]

    proc["status"] = status

    code_dict: dict[str, Any] = {}
    codings: list[dict] = []
    seen_codes: set[str] = set()
    if activity_type_code:
        coding_code = normalize_sks_coding_code(activity_type_code)
        if coding_code:
            coding: dict[str, str | bool] = {
                "system": SYSTEM_SKS,
                "code": str(coding_code),
                "userSelected": True,
            }
            if activity_type_text:
                coding["display"] = str(activity_type_text)
            codings.append(coding)
            seen_codes.add(str(coding_code))
        if activity_type_text:
            code_dict["text"] = str(activity_type_text)
    codings.extend(_supplement_codings(xml_supplements, seen_codes))
    if codings:
        code_dict["coding"] = codings
    if code_dict:
        proc["code"] = code_dict

    if patient_id_norm is not None:
        if resolved:
            proc["subject"] = resolved.patient_ref(patient_id_norm)
        else:
            proc["subject"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_PATIENT_ID,
                    "value": patient_id_norm,
                }
            }

    if encounter_id_norm is not None:
        if resolved:
            proc["encounter"] = resolved.encounter_ref(encounter_id_norm)
        else:
            proc["encounter"] = {
                "identifier": {
                    "system": SYSTEM_IFDB_OBJECT_ID,
                    "value": encounter_id_norm,
                }
            }

    if start_str and end_str:
        proc["performedPeriod"] = {"start": start_str, "end": end_str}
    elif start_str:
        proc["performedDateTime"] = start_str

    performer_entries: list[dict] = []
    for ref in build_performer_dict(row_dict):
        entry = {"actor": ref}
        if ref.get("identifier", {}).get("system") == SYSTEM_SOR:
            entry["function"] = {
                "coding": [
                    {
                        "system": SYSTEM_PERFORMER_FUNCTION,
                        "code": PERFORMER_FUNCTION_UDFOERENDE,
                    }
                ]
            }
        performer_entries.append(entry)

    if performer_entries:
        proc["performer"] = performer_entries

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
    _add("udfoertTidspunkt", row_dict.get("UDFOERT_TIDSPUNKT"))

    lpr3 = row_dict.get("LPR3_IGNORED")
    if lpr3 is not None:
        source_parts.append(
            {
                "url": "lpr3Ignored",
                "valueBoolean": is_true_flag(lpr3),
            }
        )
        if is_true_flag(lpr3):
            # Ogsaa som meta.tag: kun dermed kan der soeges paa flaget via
            # FHIR-serveren og filtreres paa det som kolonne i de flade tabeller.
            add_flag_tags_dict(proc, [FLAG_LPR3_IGNORED])

    if source_parts:
        extensions.append(
            {
                "url": EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
                "extension": source_parts,
            }
        )

    if extensions:
        proc["extension"] = extensions

    return resource_id, proc


def _append_procedure_routing_decision(
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
            "target_resource": "Procedure",
            "observation_category": None,
            "routing_source": "procedure_code",
            "routing_confidence": "high",
            "routing_reason": (
                reason
                or "AKTIVITETTYPE_KODE matched IFDB Procedure code rules; "
                "Procedure emitted, and no value-bearing XML was routed to "
                "QuestionnaireResponse/Observation."
            ),
        }
    )


def _decision_with_parallel_procedure(decision: RoutingDecision) -> RoutingDecision:
    """Preserve the XML route while making the audit reason explicit."""
    return RoutingDecision(
        target_resource=decision.target_resource,
        observation_category=decision.observation_category,
        routing_source=decision.routing_source,
        routing_confidence=decision.routing_confidence,
        routing_reason=(
            f"{decision.routing_reason} Procedure emitted in parallel because "
            "AKTIVITETTYPE_KODE matched IFDB Procedure code rules; XML values "
            f"preserved as {decision.target_resource.value}."
        ),
    )


def _procedure_observation_without_numeric_values_decision() -> RoutingDecision:
    return RoutingDecision(
        target_resource=TargetResource.QUESTIONNAIRE_RESPONSE,
        observation_category=None,
        routing_source="xml_fallback",
        routing_confidence="low",
        routing_reason=(
            "Matched Observation route for a procedure-coded measurement, but "
            "no numeric SFI datapoints were emitted; defaulted XML preservation "
            "to QuestionnaireResponse."
        ),
    )


_DELOP_KLYNGE_NAVN = "DelOperationer"
_PERSONALE_KLYNGE_NAVN = "Operationspersonale"
_ANLAGT_AF_KLYNGE_LEDETEKST = "Anlagt af/kompetence"
_SKS_ORIGINAL_PREFIXES = ("SKS.a.", "SKS_Vaerdier.a.til")


def _hash_list_contains(joined: str | None, token: str) -> bool:
    if not joined:
        return False
    return token in joined.split("#")


_KNIVTID_START_ELEMENT = "KnivtidStart"
_KNIVTID_SLUT_ELEMENT = "KnivtidSlut"
_KNIVTID_START_PART = "knivtidStart"
_KNIVTID_SLUT_PART = "knivtidSlut"
_KNIVTID_MINUTTER_PART = "knivtidMinutter"


@dataclass(slots=True)
class _XmlDelop:
    """Én deloperation (én DelOperationer-klynge) med dens egne tillægskoder."""

    code: str
    display: str | None = None
    supplements: list[tuple[str, str | None]] = field(default_factory=list)


@dataclass(slots=True)
class _XmlProcedurePayload:
    """Procedure-relevant indhold udtrukket af en registrerings XML-rækker."""

    main_supplements: list[tuple[str, str | None]] = field(default_factory=list)
    delops: list[_XmlDelop] = field(default_factory=list)
    knivtid_start: str | None = None
    knivtid_slut: str | None = None
    performers: list[dict[str, str | None]] = field(default_factory=list)


def _supplement_codings(
    supplements: list[tuple[str, str | None]] | None,
    seen_codes: set[str],
) -> list[dict]:
    """SKS-codings for tillægskoder, deduplikeret mod allerede byggede koder."""
    codings: list[dict] = []
    for raw_code, display in supplements or []:
        code = normalize_sks_coding_code(raw_code)
        if not code:
            continue
        code_s = str(code)
        if code_s in seen_codes:
            continue
        seen_codes.add(code_s)
        coding: dict[str, Any] = {"system": SYSTEM_SKS, "code": code_s}
        if display:
            coding["display"] = str(display)
        codings.append(coding)
    return codings


def _extract_xml_procedure_payload(
    parsed_rows: list | None,
) -> _XmlProcedurePayload | None:
    """Udtræk tillægskoder, deloperationer, knivtid og personale af XML-rækkerne.

    Klyngen er den eneste pålidelige rollemarkør: primær operation og
    deloperation deler ElementNavn ('Operation') og element-id, og en
    tillægskode hører KUN til den operation, den deler klynge med.
    Elementer med ElementNavn == 'Operation' uden for DelOperationer-klynger
    er registreringens primære kode og springes over (typekoden er kilden).
    """
    if not parsed_rows or not isinstance(parsed_rows[0], AktivitetsresultatRow):
        return None

    payload = _XmlProcedurePayload()
    delop_groups: dict[Any, dict[str, Any]] = {}
    personale_groups: dict[Any, dict[str, str | None]] = {}

    for idx, r in enumerate(parsed_rows):
        navn = r.element_navn
        if not navn:
            continue
        klynge_navne = r.klynge_navne or ""
        in_delop = r.klynge_navn == _DELOP_KLYNGE_NAVN or _hash_list_contains(
            klynge_navne, _DELOP_KLYNGE_NAVN
        )

        if in_delop:
            group_key = r.klynge_id if r.klynge_id is not None else -(idx + 1)
            group = delop_groups.setdefault(
                group_key, {"code": None, "display": None, "supplements": []}
            )

        if is_value_tillaegskode_element(navn):
            value_code = convert_value_tillaegskode(navn, r.element_vaerdi)
            if value_code:
                entry = (value_code, r.element_vaerdi)
                if in_delop:
                    group["supplements"].append(entry)
                else:
                    payload.main_supplements.append(entry)
            continue

        if navn == _KNIVTID_START_ELEMENT:
            value = (r.element_vaerdi or "").strip()
            if value and payload.knivtid_start is None:
                payload.knivtid_start = value
            continue
        if navn == _KNIVTID_SLUT_ELEMENT:
            value = (r.element_vaerdi or "").strip()
            if value and payload.knivtid_slut is None:
                payload.knivtid_slut = value
            continue

        klynge_ledetekster = r.klynge_ledetekster or ""
        if (
            r.klynge_navn == _PERSONALE_KLYNGE_NAVN
            or _hash_list_contains(klynge_navne, _PERSONALE_KLYNGE_NAVN)
            or r.klynge_ledetekst == _ANLAGT_AF_KLYNGE_LEDETEKST
            or _hash_list_contains(klynge_ledetekster, _ANLAGT_AF_KLYNGE_LEDETEKST)
        ):
            pers_key = r.klynge_id if r.klynge_id is not None else -(idx + 1)
            pers = personale_groups.setdefault(
                pers_key, {"rolle": None, "navn": None, "pers_id": None}
            )
            if navn == "PersonaleRolle":
                pers["rolle"] = pers["rolle"] or r.element_vaerdi
            elif navn == "Personale":
                pers["navn"] = pers["navn"] or r.element_vaerdi
                # Parseren emitterer ikke PersonaleRolle som egen række — den
                # folder rollens værdi ind i Personale-rækkens ledetekst
                # (xml_aktivitetsresultat.py: pers_rolle -> el_ldt).
                ledetekst = r.element_ledetekst
                if not pers["rolle"] and ledetekst and ledetekst != "Personale":
                    pers["rolle"] = ledetekst
            if r.element_pers_id and not pers["pers_id"]:
                pers["pers_id"] = r.element_pers_id
            continue

        original = (r.element_original_kode or "").strip()
        if not original or not original.startswith(_SKS_ORIGINAL_PREFIXES):
            continue

        if navn == "Operation":
            if in_delop:
                if group["code"] is None:
                    group["code"] = original
                    group["display"] = r.element_vaerdi
                else:
                    group["supplements"].append((original, r.element_vaerdi))
            # Primær operations-element uden for delop-klynger: typekoden er
            # allerede kilden til hovedkoden — springes over.
            continue

        entry = (original, r.element_vaerdi)
        if in_delop:
            group["supplements"].append(entry)
        else:
            payload.main_supplements.append(entry)

    payload.delops = [
        _XmlDelop(code=g["code"], display=g["display"], supplements=g["supplements"])
        for g in delop_groups.values()
        if g["code"]
    ]
    payload.performers = [
        pers for pers in personale_groups.values() if pers["navn"] or pers["pers_id"]
    ]
    return payload


def _append_source_meta_parts(proc: dict, parts: list[dict]) -> None:
    if not parts:
        return
    extensions = proc.setdefault("extension", [])
    for ext in extensions:
        if ext.get("url") == EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META:
            ext.setdefault("extension", []).extend(parts)
            return
    extensions.append(
        {
            "url": EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META,
            "extension": parts,
        }
    )


def _knivtid_tidspunkt(value: str | None) -> datetime | None:
    """Kildens knivtid som et tidspunkt med dansk zoneangivelse.

    Kilden skriver vægur-tid uden zone. Uden en eksplicit zone læser
    indlæsningen af de flade tabeller værdien som UTC og flytter den 1-2 timer,
    når den normaliseres til dansk tid — knivtid skal kunne bruges til
    planlægning og statistik, så tidspunktet skal være det, der blev
    registreret. Værdier, der ikke kan læses, falder væk: en streng, der ikke
    kan regnes på, er værre end ingen værdi, og et gæt ville være fabrikeret.
    """
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    parsed: datetime | None = None
    try:
        parsed = parse_clob_datetime(text)
    except Exception:
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.fromisoformat(text)
        except (TypeError, ValueError):
            return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=DW_TIMEZONE)
    return parsed


def _apply_operationsinformation(proc: dict, payload: _XmlProcedurePayload) -> None:
    """Læg knivtid og operationspersonale på hovedproceduren."""
    parts: list[dict] = []
    start_dt = _knivtid_tidspunkt(payload.knivtid_start)
    slut_dt = _knivtid_tidspunkt(payload.knivtid_slut)
    if start_dt:
        parts.append({"url": _KNIVTID_START_PART, "valueDateTime": str(fhir_datetime(start_dt))})
    if slut_dt:
        parts.append({"url": _KNIVTID_SLUT_PART, "valueDateTime": str(fhir_datetime(slut_dt))})
    if start_dt and slut_dt:
        # Varigheden beregnes her, hvor begge tidspunkter er parset, så
        # aftagerne kan opgøre knivtid uden at parse strenge i SQL. Et negativt
        # spænd er ikke en varighed — det udelades frem for at gemmes som støj.
        minutter = round((slut_dt - start_dt).total_seconds() / 60)
        if minutter >= 0:
            parts.append({"url": _KNIVTID_MINUTTER_PART, "valueInteger": int(minutter)})
    _append_source_meta_parts(proc, parts)

    performer_entries: list[dict] = []
    for pers in payload.performers:
        navn = pers.get("navn")
        pers_id = pers.get("pers_id")
        actor: dict[str, Any] = {"type": "Practitioner"}
        if navn:
            actor["display"] = str(navn)
        if pers_id:
            actor["identifier"] = {"system": SYSTEM_IFDB_STAFF, "value": str(pers_id)}
        entry: dict[str, Any] = {"actor": actor}
        rolle = pers.get("rolle")
        if rolle:
            entry["function"] = {"text": str(rolle)}
        performer_entries.append(entry)
    if performer_entries:
        proc.setdefault("performer", []).extend(performer_entries)


def _build_delop_procedure_dict(
    row_dict: dict[str, Any],
    resolved: ResolvedRefLookup | None,
    delop: _XmlDelop,
    *,
    seq: int,
    main_rid: str,
    main_proc: dict,
) -> tuple[str, dict]:
    """Byg en selvstændig Procedure for én deloperation med partOf-reference."""
    objekt_id_norm = _normalize_oracle_id(row_dict.get("OBJEKT_ID"))
    ident_value = (
        FAMILY_DELOP.child_value(objekt_id_norm, seq) if objekt_id_norm is not None else None
    )
    resource_id = (
        resolved.procedure_id(ident_value) if (resolved and ident_value) else str(uuid.uuid4())
    )

    proc: dict[str, Any] = {"resourceType": "Procedure", "id": resource_id}
    if ident_value:
        proc["identifier"] = [
            {
                "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
                "value": ident_value,
            }
        ]
    proc["status"] = main_proc.get("status", "completed")

    seen_codes: set[str] = set()
    codings: list[dict] = []
    code_norm = normalize_sks_coding_code(delop.code)
    if code_norm:
        coding: dict[str, Any] = {
            "system": SYSTEM_SKS,
            "code": str(code_norm),
            "userSelected": True,
        }
        if delop.display:
            coding["display"] = str(delop.display)
        codings.append(coding)
        seen_codes.add(str(code_norm))
    codings.extend(_supplement_codings(delop.supplements, seen_codes))
    code_dict: dict[str, Any] = {}
    if codings:
        code_dict["coding"] = codings
    if delop.display:
        code_dict["text"] = str(delop.display)
    if code_dict:
        proc["code"] = code_dict

    for key in ("subject", "encounter", "performedPeriod", "performedDateTime"):
        if key in main_proc:
            proc[key] = copy.deepcopy(main_proc[key])

    # Aidbox-formen {id, resourceType} og ikke en streng-reference: ressourcerne
    # skrives ordret ned i FHIR-serverens database uden normalisering, så en
    # streng ville ikke kunne følges af de flade views, der projicerer .id --
    # præcis som patient- og kontaktreferencerne ovenfor.
    part_of: dict[str, Any] = {"id": main_rid, "resourceType": "Procedure"}
    if objekt_id_norm is not None:
        part_of["identifier"] = {
            "system": SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
            "value": objekt_id_norm,
        }
    proc["partOf"] = [part_of]

    if "extension" in main_proc:
        proc["extension"] = copy.deepcopy(main_proc["extension"])
    if "meta" in main_proc:
        # Deloperationen kommer fra samme kilderaekke som hovedoperationen og
        # baerer derfor de samme driftsflag. Ellers ville et filter paa flaget
        # fange hovedoperationen men ikke dens dele.
        proc["meta"] = copy.deepcopy(main_proc["meta"])
    _append_source_meta_parts(proc, [{"url": "delOperationSekvens", "valueString": str(seq)}])

    return resource_id, proc


def build_aktivitetsresultat_procedure_dicts(
    row_dict: dict[str, Any],
    parsed_rows: list | None = None,
    resolved: ResolvedRefLookup | None = None,
) -> list[tuple[str, dict]]:
    """Byg alle Procedure dicts for en AKTIVITETSRESULTAT-række.

    Hovedproceduren bygges fra typekoden og beriges med klyngebundne
    tillægskoder fra XML'en (almindelige + beregnede talværdi-koder) samt
    knivtid og operationspersonale. Hver DelOperationer-klynge bliver sin
    egen Procedure med partOf-reference til hovedproceduren — nødvendigt
    fordi samme kode kan optræde i begge roller med hver sin sideangivelse.
    """
    payload = _extract_xml_procedure_payload(parsed_rows)
    rid, proc = _build_aktivitetsresultat_procedure_dict(
        row_dict,
        resolved,
        xml_supplements=payload.main_supplements if payload else None,
    )
    results: list[tuple[str, dict]] = [(rid, proc)]
    if payload:
        for seq, delop in enumerate(payload.delops, start=1):
            results.append(
                _build_delop_procedure_dict(
                    row_dict, resolved, delop, seq=seq, main_rid=rid, main_proc=proc
                )
            )
        # Med vilje EFTER delop-løkken: deloperationerne deep-copier hovedets
        # extension-liste i _build_delop_procedure_dict, og knivtid samt
        # operationspersonale hører KUN til hovedproceduren — påføres de før
        # løkken, arver hver delop knivtiden via kopien.
        _apply_operationsinformation(proc, payload)
    return results
