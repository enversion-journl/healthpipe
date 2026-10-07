# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SFI CodeSystem catalog logic — pure functions, no Dagster asset decoration.

This module is safe to import from FHIR pipeline modules (sfi_builder,
orchestrator) without triggering Dagster asset registration or circular
import chains.  The ``@dg.asset``-decorated ``cs_sfi`` lives in ``sfi.py``
and imports from here.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from core.org_config import FHIR_PUBLISHER
from core.resources import MSSQLResource
from DSA.CodeSystem.utils import merge_codesystem_tables
from DSA.FHIR.utils.ifdb.sfi_contract import (
    build_sfi_path,
    generate_sfi_code,
    guess_unit_from_display,
    infer_sfi_datatype,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow
from utils.fhir.identifier_systems import SYSTEM_IFDB_SFI

if TYPE_CHECKING:
    from DSA.FHIR.utils.ifdb.sfi_extraction import SfiDatapoint


@dataclass(slots=True)
class SfiElement:
    """A single SFI concept extracted from an IFDB aktivitetsresultat XML element."""

    code: str
    display: str
    datatype: str
    unit: str
    sfi_path: str
    source_element_id: str
    source_element_name: str
    source_model_name: str
    source_model_version: str
    status: str
    parent_code: str
    source_element_label: str = ""
    source_hierarchy_path: str = ""
    source_group_path: str = ""
    source_repetition_path: str = ""
    klynge_niveau: int = 0
    repetition_niveau: int = 0
    raekke_type: str = ""
    klynge_type: str = ""
    klynge_navne: str = ""
    klynge_ledetekster: str = ""
    klynge_ledetekst: str = ""


def _infer_datatype(element_type: str | None, element_value: str | None) -> str:
    return infer_sfi_datatype(element_type, element_value)


def _build_sfi_path(row: AktivitetsresultatRow) -> str:
    return build_sfi_path(row)


def _guess_unit_from_display(display: str, element_name: str | None) -> str:
    return guess_unit_from_display(display, element_name)


def extract_sfi_elements(
    parsed_rows: list[AktivitetsresultatRow],
    model_name: str | None = None,
    model_version: str | None = None,
) -> list[SfiElement]:
    """Extract SFI elements from parsed aktivitetsresultat rows.

    Each Element or Selektion row from the XML becomes a candidate SFI concept.

    Args:
        parsed_rows: Output from parse_aktivitetsresultat_xml().
        model_name: MODELLERINGSNAVN from the IFDB source row.
        model_version: MODELLERINGSVERSION from the IFDB source row.

    Returns:
        List of SfiElement with stable codes and properties.
    """
    elements: list[SfiElement] = []
    seen_codes: set[str] = set()

    for row in parsed_rows:
        if row.raekke_type not in ("Element", "Selektion"):
            continue

        if row.raekke_type == "Element" and not row.element_navn:
            continue

        if row.raekke_type == "Selektion" and not row.klynge_navn:
            continue

        if row.raekke_type == "Element":
            code, status = generate_sfi_code(row.element_element_id, row.element_navn, model_name)
            display = row.element_ledetekst or row.element_navn or code
            source_element_id = row.element_element_id or ""
            source_element_name = row.element_navn or ""
            datatype = _infer_datatype(row.element_type, row.element_vaerdi)
        else:
            code, status = generate_sfi_code(None, row.klynge_navn, model_name)
            display = row.klynge_ledetekst or row.klynge_navn or code
            source_element_id = ""
            source_element_name = row.klynge_navn or ""
            datatype = "coding"

        if code in seen_codes:
            continue
        seen_codes.add(code)

        unit = ""
        if datatype in ("integer", "decimal", "quantity"):
            unit = _guess_unit_from_display(display, row.element_navn)

        parent_code = ""
        if row.klynge_navn and row.raekke_type == "Element":
            parent_sfi, _ = generate_sfi_code(None, row.klynge_navn, model_name)
            if parent_sfi != code:
                parent_code = parent_sfi

        elements.append(
            SfiElement(
                code=code,
                display=display,
                datatype=datatype,
                unit=unit,
                sfi_path=_build_sfi_path(row),
                source_element_id=source_element_id,
                source_element_name=source_element_name,
                source_element_label=display,
                source_hierarchy_path=row.hierarki
                or row.klynge_ledetekster
                or row.klynge_navne
                or "",
                source_group_path=row.klynge_ledetekster or row.klynge_navne or "",
                source_repetition_path=row.repetition_id_hierarki or "",
                klynge_niveau=row.klynge_niveau or 0,
                repetition_niveau=row.repetition_niveau or 0,
                raekke_type=row.raekke_type or "",
                klynge_type=row.klynge_type or "",
                klynge_navne=row.klynge_navne or "",
                klynge_ledetekster=row.klynge_ledetekster or "",
                klynge_ledetekst=row.klynge_ledetekst or "",
                source_model_name=model_name or "",
                source_model_version=model_version or "",
                status=status,
                parent_code=parent_code,
            )
        )

    return elements


def _version_sort_key(v: str | None) -> tuple[int, int | str]:
    """Sort key for MODELLERINGSVERSION strings.

    Numeric versions sort numerically; non-numeric versions sort
    lexicographically AFTER the numeric ones.
    """
    if not v:
        return (-1, "")
    s = str(v).strip()
    try:
        return (0, int(s))
    except (ValueError, TypeError):
        return (1, s)


def build_sfi_codesystem(
    sfi_observations: dict[str, list[SfiElement]] | list[SfiElement],
) -> tuple[list[dict], list[dict], list[dict]]:
    """Build codesystem triple from SFI element occurrences.

    Accepts either a ``dict[code, list[SfiElement]]`` (preferred) or a flat
    ``list[SfiElement]`` (groups internally).
    """
    if isinstance(sfi_observations, list):
        grouped: dict[str, list[SfiElement]] = {}
        for elem in sfi_observations:
            grouped.setdefault(elem.code, []).append(elem)
        sfi_observations = grouped

    concepts: list[dict] = []
    properties: list[dict] = []
    hierarchy: list[dict] = []

    for code, observations in sfi_observations.items():
        if not observations:
            continue

        sorted_obs = sorted(observations, key=lambda e: _version_sort_key(e.source_model_version))
        latest = sorted_obs[-1]
        earliest = sorted_obs[0]

        versions = sorted(
            {o.source_model_version for o in observations if o.source_model_version},
            key=_version_sort_key,
        )
        seen_versions_csv = ",".join(versions)

        unique_datatypes = {o.datatype for o in observations if o.datatype}
        unique_units = {o.unit for o in observations if o.unit}
        unique_displays = {o.display for o in observations if o.display}
        conflict_fields: list[str] = []
        if len(unique_datatypes) > 1:
            conflict_fields.append(f"datatype:{','.join(sorted(unique_datatypes))}")
        if len(unique_units) > 1:
            conflict_fields.append(f"unit:{','.join(sorted(unique_units))}")
        if len(unique_displays) > 1:
            conflict_fields.append(f"display_count:{len(unique_displays)}")

        concepts.append({"CODE": code, "DISPLAY": latest.display})

        effective_status = "conflict" if conflict_fields else latest.status
        props = [
            ("datatype", latest.datatype, "string"),
            ("unit", latest.unit, "string"),
            ("sfi_path", latest.sfi_path, "string"),
            ("source_element_id", latest.source_element_id, "string"),
            ("source_element_name", latest.source_element_name, "string"),
            ("source_element_label", latest.source_element_label, "string"),
            ("source_hierarchy_path", latest.source_hierarchy_path, "string"),
            ("source_group_path", latest.source_group_path, "string"),
            ("source_repetition_path", latest.source_repetition_path, "string"),
            (
                "klynge_niveau",
                str(latest.klynge_niveau) if latest.klynge_niveau else "",
                "integer",
            ),
            (
                "repetition_niveau",
                str(latest.repetition_niveau) if latest.repetition_niveau else "",
                "integer",
            ),
            ("raekke_type", latest.raekke_type, "string"),
            ("klynge_type", latest.klynge_type, "string"),
            ("klynge_navne", latest.klynge_navne, "string"),
            ("klynge_ledetekster", latest.klynge_ledetekster, "string"),
            ("klynge_ledetekst", latest.klynge_ledetekst, "string"),
            ("source_model_name", latest.source_model_name, "string"),
            ("first_seen_in_version", earliest.source_model_version, "string"),
            ("last_seen_in_version", latest.source_model_version, "string"),
            ("seen_in_versions", seen_versions_csv, "string"),
            ("status", effective_status, "string"),
        ]
        if conflict_fields:
            props.append(("conflict_fields", "; ".join(conflict_fields), "string"))

        for key, value, ptype in props:
            if value:
                properties.append({"CODE": code, "KEY": key, "VALUE": value, "TYPE": ptype})

        if latest.parent_code:
            hierarchy.append({"CODE": code, "PARENT_CODE": latest.parent_code})

    return concepts, properties, hierarchy


def summarize_sfi_conflicts(properties: list[dict], *, sample_limit: int = 5) -> dict[str, Any]:
    """Summarize SFI codesystem conflicts from the built properties list.

    Returns a small, deterministic summary suitable for Dagster metadata/logs:
    total conflicting concepts, a tally of which conflict field types occurred,
    and a capped, sorted sample of conflicting codes. Keeps conflict visibility
    out of manual ``codesystem.sfi_properties`` queries.
    """
    conflict_codes: list[str] = []
    field_tally: dict[str, int] = {}
    for prop in properties:
        if prop.get("KEY") != "conflict_fields" or not prop.get("VALUE"):
            continue
        conflict_codes.append(prop["CODE"])
        for part in str(prop["VALUE"]).split(";"):
            field_type = part.strip().split(":", 1)[0]
            if field_type:
                field_tally[field_type] = field_tally.get(field_type, 0) + 1

    conflict_codes.sort()
    return {
        "conflict_total": len(conflict_codes),
        "conflict_fields_tally": dict(sorted(field_tally.items())),
        "conflict_sample_codes": conflict_codes[:sample_limit],
    }


def add_sfi_catalog_element(sink: dict[str, list[SfiElement]], elem: SfiElement) -> None:
    """Læg et katalogelement i sinken — kun hvis det ikke allerede står der.

    Sinken lever hele partitionen igennem og merges først efter sidste chunk.
    Kataloget bygges mængdebaseret (versioner, datatyper, enheder, displays),
    så forekomster ud over den første tilfører intet — men uden dedupe vokser
    sinken med antallet af datapunkter, og på en historisk partition på
    millioner af rækker koster det barneprocessen livet. Ægte varianter
    (forskellig enhed, display, version) bevares, så konflikter stadig ses.
    """
    varianter = sink.setdefault(elem.code, [])
    if elem not in varianter:
        varianter.append(elem)


def sfi_datapoint_to_element(dp: SfiDatapoint, model_name: str | None = None) -> SfiElement:
    """Convert a FHIR-pipeline SfiDatapoint to a catalog SfiElement."""
    parent_code = ""
    if dp.raekke_type == "Element" and dp.source_group_path:
        first_group = dp.source_group_path.split(" > ")[0].strip()
        if first_group:
            parent_sfi, _ = generate_sfi_code(None, first_group, model_name)
            if parent_sfi != dp.sfi_code:
                parent_code = parent_sfi

    return SfiElement(
        code=dp.sfi_code,
        display=dp.display,
        datatype=dp.datatype,
        unit=dp.unit,
        sfi_path=dp.sfi_path,
        source_element_id=dp.source_element_id,
        source_element_name=dp.source_element_name,
        source_element_label=dp.source_element_label,
        source_hierarchy_path=dp.source_hierarchy_path,
        source_group_path=dp.source_group_path,
        source_repetition_path=dp.source_repetition_path,
        klynge_niveau=dp.klynge_niveau,
        repetition_niveau=dp.repetition_niveau,
        raekke_type=dp.raekke_type,
        klynge_type=dp.klynge_type or "",
        klynge_navne=dp.klynge_navne or "",
        klynge_ledetekster=dp.klynge_ledetekster or "",
        klynge_ledetekst=dp.klynge_ledetekst or "",
        source_model_name=dp.source_model_name,
        source_model_version=dp.source_model_version,
        status=dp.sfi_status,
        parent_code=parent_code,
    )


def merge_sfi_catalog_from_datapoints(
    sfi_catalog_sink: dict[str, list[SfiElement]],
    mssql_target_staging: MSSQLResource,
    logger: Any,
) -> dict[str, int]:
    """Build and MERGE the SFI CodeSystem tables from collected catalog elements.

    Called by the AKTIVITETSRESULTAT orchestrator after all chunks have been
    processed — no separate Oracle round-trip required.
    """
    concepts, properties, hierarchy = build_sfi_codesystem(sfi_catalog_sink)
    logger.info(
        "SFI catalog side-effect: %d concepts, %d properties, %d hierarchy from %d distinct codes",
        len(concepts),
        len(properties),
        len(hierarchy),
        len(sfi_catalog_sink),
    )

    conflicts = summarize_sfi_conflicts(properties)
    if conflicts["conflict_total"] > 0:
        logger.warning(
            "SFI catalog conflicts: %d concept(s) with conflicting datatype/unit/"
            "display across versions. By field: %s. Sample codes: %s",
            conflicts["conflict_total"],
            conflicts["conflict_fields_tally"],
            conflicts["conflict_sample_codes"],
        )

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            result = merge_codesystem_tables(
                cursor, "sfi", concepts, properties, hierarchy, code_max_length=450
            )
            conn.commit()
        finally:
            cursor.close()

    logger.info(
        "codesystem.sfi MERGE: concepts(%di/%du), properties(%di/%du), hierarchy(%di/%du)",
        result["concepts_inserted"],
        result["concepts_updated"],
        result["properties_inserted"],
        result["properties_updated"],
        result["hierarchy_inserted"],
        result["hierarchy_updated"],
    )
    result.update(conflicts)
    return result


_SFI_CODESYSTEM_ID = "ifdb-sfi"
_SFI_CODESYSTEM_URL = SYSTEM_IFDB_SFI


def build_fhir_codesystem_resource(
    sfi_catalog_sink: dict[str, list[SfiElement]],
) -> dict[str, Any]:
    """Build a complete FHIR R4 CodeSystem resource dict from the SFI catalog.

    The resulting dict is suitable for direct upsert into Aidbox via
    ``batch_upsert_resource_dicts``.  Uses a deterministic id so the
    resource is idempotently overwritten on each pipeline run.
    """
    concepts, properties, hierarchy = build_sfi_codesystem(sfi_catalog_sink)

    parent_map: dict[str, str] = {h["CODE"]: h["PARENT_CODE"] for h in hierarchy}
    props_by_code: dict[str, list[dict]] = {}
    for p in properties:
        props_by_code.setdefault(p["CODE"], []).append(p)

    fhir_concepts: list[dict[str, Any]] = []
    for c in concepts:
        code = c["CODE"]
        entry: dict[str, Any] = {
            "code": code,
            "display": c["DISPLAY"],
        }
        code_props = props_by_code.get(code, [])
        if code_props:
            entry["property"] = [
                {"code": p["KEY"], "valueString": p["VALUE"]} for p in code_props if p["VALUE"]
            ]
        fhir_concepts.append(entry)

    property_definitions: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for p in properties:
        key = p["KEY"]
        if key not in seen_keys:
            seen_keys.add(key)
            property_definitions.append(
                {
                    "code": key,
                    "type": "string",
                    "description": key,
                }
            )

    if parent_map:
        if "parent" not in seen_keys:
            property_definitions.append(
                {
                    "code": "parent",
                    "type": "code",
                    "description": "Parent code in the SFI hierarchy",
                }
            )
        for entry in fhir_concepts:
            parent = parent_map.get(entry["code"])
            if parent:
                entry.setdefault("property", []).append({"code": "parent", "valueCode": parent})

    resource: dict[str, Any] = {
        "resourceType": "CodeSystem",
        "id": _SFI_CODESYSTEM_ID,
        "url": _SFI_CODESYSTEM_URL,
        "name": "IFDBSFI",
        "title": "SFI CodeSystem (IFDB Aktivitetsresultat)",
        "status": "active",
        "content": "complete",
        "hierarchyMeaning": "is-a",
        "publisher": FHIR_PUBLISHER,
        "description": (
            "Standardiserede Faelles Indikatorer (SFI) fra IFDB "
            "aktivitetsresultat-XML. Semantisk noeglesystem paa tvaers af "
            "Observation og QuestionnaireResponse."
        ),
        "count": len(fhir_concepts),
        "property": property_definitions,
        "concept": fhir_concepts,
    }
    return resource


def post_sfi_codesystem_to_aidbox(
    sfi_catalog_sink: dict[str, list[SfiElement]],
    fhir_database: Any,
    logger: Any,
) -> None:
    """Build and upsert the SFI CodeSystem FHIR resource into Aidbox.

    This is an additive step run alongside the MSSQL MERGE — it does NOT
    replace the existing codesystem.sfi_* tables in MSSQL.
    """
    if not sfi_catalog_sink:
        logger.info("post_sfi_codesystem_to_aidbox: empty catalog, skipping")
        return

    resource = build_fhir_codesystem_resource(sfi_catalog_sink)
    concept_count = resource.get("count", 0)

    logger.info(
        "Posting SFI CodeSystem to Aidbox: id=%s, %d concepts, %d property definitions",
        _SFI_CODESYSTEM_ID,
        concept_count,
        len(resource.get("property", [])),
    )

    successes, failures, failed_ids = fhir_database.batch_upsert_resource_dicts(
        resource_dicts=[(_SFI_CODESYSTEM_ID, resource)],
        resource_type="CodeSystem",
        invalidate_cache=False,
    )

    if failures:
        logger.warning(
            "SFI CodeSystem Aidbox upsert: %d failures — %s",
            failures,
            failed_ids,
        )
    else:
        logger.info(
            "SFI CodeSystem Aidbox upsert: success (%d concepts posted)",
            concept_count,
        )
