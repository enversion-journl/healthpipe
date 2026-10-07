# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Driftsflag på FHIR-ressourcer som meta.tag.

Flag, der siger noget om hvordan en række skal behandles — og ikke noget
klinisk — hører i meta.tag, hvor de er søgbare via FHIR-serverens egne
mekanismer og kan projiceres som en boolsk kolonne i de flade tabeller med
``meta.tag...exists()``. Ligger flaget kun i en udvidelse, kan der hverken søges
eller filtreres på det.

Eget kodesystem og ikke HL7's fortrolighedskoder: ``MSK`` betyder "data skjult
af fortrolighedshensyn", og det er noget helt andet end "denne registrering skal
ikke indberettes". Låner vi fortrolighedskoden, bliver den før eller siden
misforstået — særligt hvis nogen bygger adgangsstyring på fortrolighedskoder.

Mærket sættes KUN når flaget er sandt. Fravær betyder derfor "ikke mærket", og
en kolonne bygget på ``exists()`` er aldrig NULL. Den, der har brug for at skelne
mellem "kilden sagde nej" og "kilden sagde ingenting", kan læse den boolske
værdi i kilde-metadata-udvidelsen.
"""

from __future__ import annotations

from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.meta import Meta

SYSTEM_RESOURCE_FLAG = "http://example.org/fhir/CodeSystem/resource-flag"

# Registreringen skal ikke indberettes til Landspatientregisteret.
FLAG_LPR3_IGNORED = "lpr3-ignored"

_DISPLAYS = {
    FLAG_LPR3_IGNORED: "Ikke til Landspatientregisteret",
}


def build_flag_tag_dict(code: str) -> dict:
    """Byg et driftsflag som meta.tag-coding til dict-baserede pipelines."""
    return {"system": SYSTEM_RESOURCE_FLAG, "code": code, "display": _DISPLAYS[code]}


def add_flag_tags_dict(resource: dict, codes: list[str]) -> None:
    """Læg driftsflag i ressourcens meta.tag uden at fjerne mærker, der allerede står der."""
    if not codes:
        return
    meta = resource.setdefault("meta", {})
    tags = meta.setdefault("tag", [])
    kendte = {(t.get("system"), t.get("code")) for t in tags}
    for code in codes:
        if (SYSTEM_RESOURCE_FLAG, code) not in kendte:
            tags.append(build_flag_tag_dict(code))


def build_flag_meta(codes: list[str]) -> Meta | None:
    """Byg et Meta med driftsflag til Pydantic-baserede pipelines."""
    if not codes:
        return None
    return Meta(tag=[Coding(**build_flag_tag_dict(code)) for code in codes])
