# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared RIS_YDELSE XML helpers.

The Procedure and ServiceRequest pipelines both need the same RIS service
grouping from IFDB.RIS_YDELSE.XML. Keep the parser here so the two resources
stay on the same source grain: ``OBJEKT_ID:element_nummer``.
"""

from typing import Any
from xml.etree import ElementTree as ET

import dagster as dg


def str_or_none(value: Any) -> str | None:
    """Return stripped string or None for empty/NULL values."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _xml_local_name(tag: Any) -> str:
    """Return XML local-name for namespaced/non-namespaced tags."""
    if not isinstance(tag, str):
        return ""
    if "}" in tag:
        return tag.rsplit("}", 1)[-1]
    return tag


def extract_ris_services_from_xml(
    xml_raw: Any,
    *,
    objekt_id: Any,
    context: dg.AssetExecutionContext | None = None,
    strict_source_validation: bool = False,
) -> list[dict[str, Any]]:
    """Extract one-to-many RIS service groups from XML.

    Returns groups with keys:
      - element_nummer: 1-based ordering in XML
      - primary_code: value from ``<kode>``
      - tillaegskoder: values from ``<tillaegskode>``
    """
    xml_str = str_or_none(xml_raw)
    if not xml_str:
        if strict_source_validation:
            raise ValueError(f"RIS_YDELSE row {objekt_id} has missing or empty XML")
        return []

    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError as exc:
        if strict_source_validation:
            raise ValueError(f"RIS_YDELSE row {objekt_id} has invalid XML: {exc}") from exc
        if context is not None:
            context.log.warning(f"Skipping RIS row {objekt_id}: XML parse error in RIS_YDELSE.XML")
        return []

    # ``element_nummer`` is the 1-based absolute position of the <service>
    # element among ALL <service> nodes (including code-less ones that are
    # skipped). Numbering by absolute position — rather than by the count of
    # emitted groups — keeps a service's identifier stable when a sibling
    # service has no <kode> (or later gains one): it no longer shifts the
    # numbering of the services after it. This identifier is shared with the
    # Procedure pipeline (Procedure.basedOn), so the two must stay in lockstep.
    # NOTE: this assumes the source RIS_YDELSE.XML for a given OBJEKT_ID is
    # stable in <service> order/membership across re-runs (it is a stored
    # message); reordering services would still remap identifiers.
    services: list[dict[str, Any]] = []
    service_index = 0
    for node in root.iter():
        if _xml_local_name(node.tag).lower() != "service":
            continue
        service_index += 1

        primary_code: str | None = None
        tillaegskoder: list[str] = []
        for child in list(node):
            name = _xml_local_name(child.tag).lower()
            value = str_or_none(child.text)
            if not value:
                continue
            if name == "kode" and not primary_code:
                primary_code = value
            elif name == "tillaegskode":
                tillaegskoder.append(value)

        if primary_code:
            services.append(
                {
                    "element_nummer": service_index,
                    "primary_code": primary_code,
                    "tillaegskoder": tillaegskoder,
                }
            )

    if services:
        return services

    # Fallback: some XML variants may include a single top-level code. Skip any
    # <kode> nested under <diagnose> — that is the referral diagnosis (e.g. DZ016),
    # not a service code, and must never masquerade as the ordered service.
    parent_map = {child: parent for parent in root.iter() for child in parent}
    top_code: str | None = None
    top_tillaeg: list[str] = []
    for node in root.iter():
        name = _xml_local_name(node.tag).lower()
        value = str_or_none(node.text)
        if not value:
            continue
        parent = parent_map.get(node)
        if parent is not None and _xml_local_name(parent.tag).lower() == "diagnose":
            continue
        if name == "kode" and not top_code:
            top_code = value
        elif name == "tillaegskode":
            top_tillaeg.append(value)

    if top_code:
        return [
            {
                "element_nummer": 1,
                "primary_code": top_code,
                "tillaegskoder": top_tillaeg,
            }
        ]

    return []


# ``SPACE`` is the source sentinel for an empty value in RIS_YDELSE.XML.
_EMPTY_SENTINELS = {"space"}


def _clean_xml_value(value: Any) -> str | None:
    """Return a stripped value, treating the ``SPACE`` sentinel as empty."""
    s = str_or_none(value)
    if s is None or s.lower() in _EMPTY_SENTINELS:
        return None
    return s


def _find_child(parent: Any, name: str) -> Any:
    """Return the first direct child whose local-name matches ``name``."""
    if parent is None:
        return None
    for child in list(parent):
        if _xml_local_name(child.tag).lower() == name:
            return child
    return None


def _child_text(parent: Any, name: str) -> str | None:
    child = _find_child(parent, name)
    return _clean_xml_value(child.text) if child is not None else None


def _org_block(parent: Any, name: str) -> dict[str, str | None]:
    """Extract an institution/afdeling/type org block (rekvirent/producent/...)."""
    block = _find_child(parent, name)
    if block is None:
        return {}
    return {
        "institution": _child_text(block, "institution"),
        "afdeling": _child_text(block, "afdeling"),
        "type": _child_text(block, "type"),
    }


def extract_ris_row_fields(
    xml_raw: Any,
    *,
    objekt_id: Any,
    context: dg.AssetExecutionContext | None = None,
) -> dict[str, Any]:
    """Extract the row-invariant clinical fields from RIS_YDELSE.XML.

    These fields live ONLY in the XML (no dedicated source column carries them),
    so dropping them is real data loss. Returns (keys absent when empty):
      - diagnose_kode / diagnose_type   (referral diagnosis -> standalone Condition)
      - rekvisition_start               (-> authoredOn)
      - prioritet                       (-> priority; ``SPACE`` -> absent)
      - kontakt: {starttidspunkt, type, henvisningsmaade, afslutningsmaade,
                  psykiatrisk, behandlingsansvarlig_institution,
                  behandlingsansvarlig_afdeling}  (descriptive Encounter context;
                  carries no contact id, so it cannot become a reference)
      - rekvirent / producent: {institution, afdeling}  (the ordering resp.
                  performing unit; institution+afdeling compose a SHAK code that
                  is carried as a typed identifier for later resolution)
    """
    xml_str = str_or_none(xml_raw)
    if not xml_str:
        return {}
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        if context is not None:
            context.log.warning(f"Skipping RIS row {objekt_id} clinical fields: XML parse error")
        return {}

    # Find the <diagnose>/<rekvisition>/<kontakt> blocks anywhere in the tree
    # (namespace-robust, position-independent). First <diagnose> block wins —
    # empirically the only shape in IFDB (0 multi-block rows in a 112k sample,
    # 2026-08-05); the lossless guard below logs if that ever changes.
    diagnose = rekvisition = kontakt = None
    diagnose_count = 0
    for node in root.iter():
        local = _xml_local_name(node.tag).lower()
        if local == "diagnose":
            diagnose_count += 1
            if diagnose is None:
                diagnose = node
        elif local == "rekvisition" and rekvisition is None:
            rekvisition = node
        elif local == "kontakt" and kontakt is None:
            kontakt = node

    fields: dict[str, Any] = {}

    if diagnose is not None:
        kode_children = [c for c in diagnose if _xml_local_name(c.tag).lower() == "kode"]
        if (diagnose_count > 1 or len(kode_children) > 1) and context is not None:
            context.log.warning(
                f"RIS row {objekt_id}: XML has {diagnose_count} <diagnose> block(s) "
                f"with {len(kode_children)} <kode> child(ren) in the first — only "
                "the first code is used; remaining diagnosis codes are dropped"
            )
        if (kode := _child_text(diagnose, "kode")) is not None:
            fields["diagnose_kode"] = kode
        if (dtype := _child_text(diagnose, "type")) is not None:
            fields["diagnose_type"] = dtype

    if rekvisition is not None:
        if (start := _child_text(rekvisition, "start")) is not None:
            fields["rekvisition_start"] = start
        if (prioritet := _child_text(rekvisition, "prioritet")) is not None:
            fields["prioritet"] = prioritet
        for role in ("rekvirent", "producent"):
            org = _org_block(rekvisition, role)
            org = {k: v for k, v in org.items() if v is not None}
            if org:
                fields[role] = org

    if kontakt is not None:
        behandlingsansvarlig = _org_block(kontakt, "behandlingsansvarlig")
        kontakt_fields = {
            "starttidspunkt": _child_text(kontakt, "starttidspunkt"),
            "type": _child_text(kontakt, "type"),
            "henvisningsmaade": _child_text(kontakt, "henvisningsmaade"),
            "afslutningsmaade": _child_text(kontakt, "afslutningsmaade"),
            "psykiatrisk": _child_text(kontakt, "psykiatrisk"),
            "behandlingsansvarlig_institution": behandlingsansvarlig.get("institution"),
            "behandlingsansvarlig_afdeling": behandlingsansvarlig.get("afdeling"),
        }
        kontakt_fields = {k: v for k, v in kontakt_fields.items() if v is not None}
        if kontakt_fields:
            fields["kontakt"] = kontakt_fields

    return fields
