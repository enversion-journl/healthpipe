# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SFI datapoint extraction from parsed IFDB aktivitetsresultat XML rows.

``DSA.CodeSystem.sfi.sfi.extract_sfi_elements`` produces a *catalog* of
distinct SFI concepts (deduplicated by code) for the CodeSystem assets. The
FHIR builders need the same SFI-code generation contract on a per-occurrence
basis where repeated elements and selections each become their own datapoint
with their own value.

This helper:
  - inlines the SFI-code / datatype / unit / path logic so FHIR builders do not
    depend on ``DSA.CodeSystem.sfi.sfi`` at import time (that module is a
    Dagster asset module — importing it triggers ``@dg.asset(...)`` decoration
    which can fail in non-asset contexts and would create a fragile coupling);
  - exposes ``SfiDatapoint`` and ``extract_sfi_datapoints`` for downstream
    Observation/QR builders.

The local helpers are kept structurally identical to the CodeSystem extractor
so the SFI code strings, datatypes and units produced for the same XML are the
same. If the CodeSystem extractor moves its helpers into a non-asset module
later, this file should switch to importing them and delete the duplicates.
"""

from __future__ import annotations

from dataclasses import dataclass

from DSA.FHIR.utils.ifdb.sfi_contract import (
    build_sfi_path,
    generate_sfi_code,
    guess_unit_from_display,
    infer_sfi_datatype,
)
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import AktivitetsresultatRow


@dataclass(slots=True)
class SfiDatapoint:
    """One SFI datapoint occurrence in an IFDB aktivitetsresultat XML.

    Mirrors the ``SfiElement`` catalog shape but adds the raw
    value, the source ``element_nummer`` from the XML parser, and the
    hierarchy/lineage metadata needed by the SQL projection.
    """

    sfi_code: str
    sfi_status: str
    display: str
    datatype: str
    unit: str
    raw_value: str | None
    element_nummer: int | None
    sfi_path: str
    source_element_id: str
    source_element_name: str
    source_element_label: str
    source_hierarchy_path: str
    source_group_path: str
    source_repetition_path: str
    source_model_name: str
    source_model_version: str
    klynge_niveau: int
    repetition_niveau: int
    hierarki: str | None
    raekke_type: str
    klynge_type: str | None = None
    klynge_navne: str | None = None
    klynge_ledetekster: str | None = None
    klynge_ledetekst: str | None = None
    klynge_valg_ledetekst: str | None = None
    klynge_id: int | None = None
    klynge_id_hierarki: str | None = None
    klynge_parent_id: int | None = None
    repetition_id: int | None = None
    repetition_id_hierarki: str | None = None
    opdateret_tid: str | None = None
    xml_status: int | None = None
    xml_status_text: str | None = None
    element_kode: str | None = None
    element_original_kode: str | None = None
    element_administrativ_enhed_id: str | None = None


def _infer_datatype(element_type: str | None, element_value: str | None) -> str:
    """Backward-compatible wrapper around the shared SFI datatype contract."""
    return infer_sfi_datatype(element_type, element_value)


def _guess_unit_from_display(display: str, element_name: str | None) -> str:
    """Backward-compatible wrapper around the shared SFI unit contract."""
    return guess_unit_from_display(display, element_name)


def _build_sfi_path(row: AktivitetsresultatRow) -> str:
    """Backward-compatible wrapper around the shared SFI path contract."""
    return build_sfi_path(row)


def extract_sfi_datapoints(
    parsed_rows: list[AktivitetsresultatRow],
    model_name: str | None = None,
    model_version: str | None = None,
) -> list[SfiDatapoint]:
    """Turn parser output into SFI datapoint occurrences (one per Element/Selektion).

    Unlike ``extract_sfi_elements`` this does NOT deduplicate by code — repeated
    elements (e.g. the same SFI within a Repetition group) each become a
    separate datapoint with their own value and ``element_nummer``.

    Rows with ``raekke_type='Tom'`` or ``xml_status=-1`` are skipped — they are
    parser sentinels for empty/invalid XML, not clinical data.
    """
    datapoints: list[SfiDatapoint] = []

    for row in parsed_rows:
        if row.xml_status == -1:
            continue
        if row.raekke_type not in ("Element", "Selektion"):
            continue

        if row.raekke_type == "Element":
            if not row.element_navn and not row.element_element_id:
                continue
            sfi_code, status = generate_sfi_code(
                row.element_element_id, row.element_navn, model_name
            )
            display = row.element_ledetekst or row.element_navn or sfi_code
            datatype = _infer_datatype(row.element_type, row.element_vaerdi)
            raw_value = row.element_vaerdi if row.element_vaerdi else None
            source_element_id = row.element_element_id or ""
            source_element_name = row.element_navn or ""
        else:
            if not row.klynge_navn:
                continue
            sfi_code, status = generate_sfi_code(None, row.klynge_navn, model_name)
            display = row.klynge_ledetekst or row.klynge_navn or sfi_code
            datatype = "coding"
            raw_value = row.klynge_valg_ledetekst or None
            source_element_id = ""
            source_element_name = row.klynge_navn or ""

        unit = ""
        if datatype in ("integer", "decimal", "quantity"):
            unit = _guess_unit_from_display(display, row.element_navn)

        datapoints.append(
            SfiDatapoint(
                sfi_code=sfi_code,
                sfi_status=status,
                display=display,
                datatype=datatype,
                unit=unit,
                raw_value=raw_value,
                element_nummer=row.element_nummer,
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
                source_model_name=model_name or "",
                source_model_version=model_version or "",
                klynge_niveau=row.klynge_niveau,
                repetition_niveau=row.repetition_niveau,
                hierarki=row.hierarki,
                raekke_type=row.raekke_type or "",
                klynge_type=row.klynge_type,
                klynge_navne=row.klynge_navne,
                klynge_ledetekster=row.klynge_ledetekster,
                klynge_ledetekst=row.klynge_ledetekst,
                klynge_valg_ledetekst=row.klynge_valg_ledetekst,
                klynge_id=row.klynge_id,
                klynge_id_hierarki=row.klynge_id_hierarki,
                klynge_parent_id=row.klynge_parent_id,
                repetition_id=row.repetition_id,
                repetition_id_hierarki=row.repetition_id_hierarki,
                opdateret_tid=(row.opdateret_tid.isoformat() if row.opdateret_tid else None),
                xml_status=row.xml_status if row.xml_status != 0 else None,
                xml_status_text=row.xml_status_text,
                element_kode=row.element_kode,
                element_original_kode=row.element_original_kode,
                element_administrativ_enhed_id=row.element_administrativ_enhed_id,
            )
        )

    return datapoints
