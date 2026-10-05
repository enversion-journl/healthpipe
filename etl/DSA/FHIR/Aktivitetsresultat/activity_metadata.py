# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""AKTIVITETSUDFOERELSE metadata lookups for AKTIVITETSRESULTAT.

DSI/DSA boundary ([internal ticket] review F8 — classification, not a move):
This module mixes two concerns that intentionally stay co-located for now:
  * DSI-flavoured: source-row fetching (``arc_lookup.chunked_in_fetch*``
    against arc.ifdb_aktivitetsudfoerelse) — source extraction.
  * DSA-flavoured: shaping the fetched values into FHIR extensions
    (``EXTENSION_AKTIVITETSUDFOERELSE_*``) — FHIR modelling.
Splitting the fetch helpers into a DSI source layer is a candidate
follow-up, but is deferred to avoid introducing circular imports from the
Dagster asset modules (see commit ddc7c866). Do not move mechanically.

All lookups read ``arc.ifdb_aktivitetsudfoerelse[_speciale]`` via
``arc_lookup``.
"""

from typing import Any

import dagster as dg
from core.resources import MSSQLResource
from DSA.FHIR.utils.code_normalization import split_aktivitettype_kode
from DSA.FHIR.utils.ifdb import arc_lookup
from DSA.FHIR.utils.ifdb.transforms import normalize_oracle_id as _normalize_oracle_id
from utils.fhir.fhir_extensions import (
    EXTENSION_AKTIVITETSUDFOERELSE_META,
    EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE,
    EXTENSION_AKTIVITETTYPE_KODE_HIERARKI,
)

_UDF_COLUMNS = [
    "OBJEKT_ID",
    "OPRET_PERSID",
    "OPRET_PERSNAVN",
    "OPRET_BRUGERROLLE",
    "OPRET_ADMENHED_KODE",
    "OPRET_ADMENHED_NAVN",
    "OPRET_ADMENHED_ORGKODE",
    "OPRET_TIDSPUNKT",
    "AENDRET_PERSID",
    "AENDRET_PERSNAVN",
    "AENDRET_BRUGERROLLE",
    "AENDRET_ADMENHED_KODE",
    "AENDRET_ADMENHED_NAVN",
    "AENDRET_ADMENHED_ORGKODE",
    "AENDRET_TIDSPUNKT",
    "AKTIVITET_ID",
    "FORTR_NIVEAU",
    "FORTR_GRUPPE_ID",
    "FORTR_GRUPPE_NAVN",
]

_SPECIALE_COLUMNS = [
    "AKTIVITETSUDFOERELSE_ID",
    "SPECIALE_HOSPITAL_KODE",
    "SPECIALE_HOSPITAL_NAVN",
    "SPECIALE_HOSPITAL_ORGKODE",
    "SPECIALE_KODE",
    "SPECIALE_KODETEKST",
]


def _fetch_aktivitetsudfoerelse_data_arc(
    udfoerelse_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, dict[str, Any]]:
    """Fetch AKTIVITETSUDFOERELSE rows keyed by OBJEKT_ID from the archive."""
    # include_deleted=True: the archive soft-deletes via SLETTET; filtering
    # dw_is_deleted=0 here would silently
    # drop every UDFOERT_*/OPRET_*/AENDRET_*/FORTR_* extension for
    # aktivitetsresultater whose udførelse is SLETTET-marked.
    rows = arc_lookup.chunked_in_fetch(
        udfoerelse_ids,
        table="arc.ifdb_aktivitetsudfoerelse",
        columns=_UDF_COLUMNS,
        where_column="OBJEKT_ID",
        mssql=mssql,
        include_deleted=True,
    )
    udf_by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        objekt_id_norm = _normalize_oracle_id(row.get("OBJEKT_ID"))
        if objekt_id_norm is not None:
            udf_by_id[objekt_id_norm] = row
    context.log.info(f"[UDF/arc] Fetched {len(udf_by_id)} aktivitetsudførelse rows")
    return udf_by_id


def _build_udf_extensions_dict(udf_row: dict[str, Any]) -> list[dict]:
    """Build AKTIVITETSUDFOERELSE metadata extensions from a UDF row."""
    parts: list[dict] = []

    def _add(url: str, value: Any) -> None:
        if value is not None:
            str_val = str(value).strip()
            if str_val:
                parts.append({"url": url, "valueString": str_val})

    _add("oprettetPersId", udf_row.get("OPRET_PERSID"))
    _add("oprettetPersNavn", udf_row.get("OPRET_PERSNAVN"))
    _add("oprettetBrugerrolle", udf_row.get("OPRET_BRUGERROLLE"))
    _add("oprettetAdmEnhedKode", udf_row.get("OPRET_ADMENHED_KODE"))
    _add("oprettetAdmEnhedNavn", udf_row.get("OPRET_ADMENHED_NAVN"))
    _add("oprettetAdmEnhedOrgKode", udf_row.get("OPRET_ADMENHED_ORGKODE"))
    _add("oprettetTidspunkt", udf_row.get("OPRET_TIDSPUNKT"))
    _add("aendretPersId", udf_row.get("AENDRET_PERSID"))
    _add("aendretPersNavn", udf_row.get("AENDRET_PERSNAVN"))
    _add("aendretBrugerrolle", udf_row.get("AENDRET_BRUGERROLLE"))
    _add("aendretAdmEnhedKode", udf_row.get("AENDRET_ADMENHED_KODE"))
    _add("aendretAdmEnhedNavn", udf_row.get("AENDRET_ADMENHED_NAVN"))
    _add("aendretAdmEnhedOrgKode", udf_row.get("AENDRET_ADMENHED_ORGKODE"))
    _add("aendretTidspunkt", udf_row.get("AENDRET_TIDSPUNKT"))
    _add("aktivitetId", udf_row.get("AKTIVITET_ID"))
    _add("fortrolighedsNiveau", udf_row.get("FORTR_NIVEAU"))
    _add("fortrolighedsGruppeId", udf_row.get("FORTR_GRUPPE_ID"))
    _add("fortrolighedsGruppeNavn", udf_row.get("FORTR_GRUPPE_NAVN"))

    if not parts:
        return []

    return [{"url": EXTENSION_AKTIVITETSUDFOERELSE_META, "extension": parts}]


def _build_level_extensions_dict(activity_type_code: str | None) -> list[dict]:
    """Build AKTIVITETTYPE_KODE hierarchy split extensions."""
    levels = split_aktivitettype_kode(activity_type_code)

    parts: list[dict] = []
    for i, val in enumerate(levels):
        if val is not None:
            parts.append({"url": f"level{i + 1}", "valueString": val})

    if not parts:
        return []

    return [{"url": EXTENSION_AKTIVITETTYPE_KODE_HIERARKI, "extension": parts}]


def _fetch_aktivitetsudfoerelse_speciale_data_arc(
    udfoerelse_ids: list[str],
    mssql: MSSQLResource,
    context: dg.AssetExecutionContext,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch AKTIVITETSUDFOERELSE_SPECIALE grouped by AKTIVITETSUDFOERELSE_ID from arc."""
    result = arc_lookup.chunked_in_fetch_grouped(
        udfoerelse_ids,
        table="arc.ifdb_aktivitetsudfoerelse_speciale",
        columns=_SPECIALE_COLUMNS,
        where_column="AKTIVITETSUDFOERELSE_ID",
        mssql=mssql,
        key_fn=lambda row: _normalize_oracle_id(row["AKTIVITETSUDFOERELSE_ID"]),
        sort_key=lambda row: str(row.get("SPECIALE_KODE", "")),
    )
    context.log.info(
        f"[SPECIALE/arc] Fetched {sum(len(v) for v in result.values())} "
        f"speciale rows for {len(result)} udførelser"
    )
    return result


def _build_speciale_extensions_dict(speciale_rows: list[dict[str, Any]]) -> list[dict]:
    """Build aktivitetsudfoerelseSpeciale extension from speciale rows."""
    if not speciale_rows:
        return []

    parts: list[dict] = []
    for row in speciale_rows:
        entry_parts: list[dict] = []
        for url, key in [
            ("specialeKode", "SPECIALE_KODE"),
            ("specialeKodetekst", "SPECIALE_KODETEKST"),
            ("specialeHospitalKode", "SPECIALE_HOSPITAL_KODE"),
            ("specialeHospitalNavn", "SPECIALE_HOSPITAL_NAVN"),
            ("specialeHospitalOrgkode", "SPECIALE_HOSPITAL_ORGKODE"),
        ]:
            val = row.get(key)
            if val is not None:
                str_val = str(val).strip()
                if str_val:
                    entry_parts.append({"url": url, "valueString": str_val})
        if entry_parts:
            parts.append({"url": "speciale", "extension": entry_parts})

    if not parts:
        return []
    return [{"url": EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE, "extension": parts}]
