# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""XML parse workers for AKTIVITETSRESULTAT."""

from typing import Any

from core.parallel import run_parallel_stage
from DSA.FHIR.utils.ifdb.xml_aktivitetsresultat import (
    AktivitetskontekstRow,
    AktivitetsresultatRow,
    parse_aktivitetsresultat_xml,
    parse_xml_components_fast,
)

from .aktivitetsresultat_config import _XML_FAST_PARSE_STAGE, _XML_LOSSLESS_PARSE_STAGE
from .aktivitetsresultat_identifiers import _build_aktivitets_id

_LosslessParseResult = tuple[
    int,
    list[AktivitetsresultatRow],
    AktivitetskontekstRow | None,
    int,
    str | None,
]

_XmlParseTask = tuple[int, int, str]

_XmlComponentData = tuple[str | None, str | None]

_XmlParseResult = tuple[int, list[_XmlComponentData], int, str | None]


def _parse_xml_lossless_worker(task: _XmlParseTask) -> _LosslessParseResult:
    """Worker for lossless XML parse -- returns full AktivitetsresultatRow list + KontekstRow."""
    row_idx, aktivitets_id, xml_payload = task
    error_message: str | None = None
    try:
        rows, kontekst = parse_aktivitetsresultat_xml(
            xml_string=xml_payload, aktivitets_id=aktivitets_id
        )
        xml_errors = 1 if (rows and rows[0].xml_status == -1) else 0
    except Exception as exc:
        rows = []
        kontekst = None
        xml_errors = 1
        error_message = f"XML parse error for aktivitets_id={aktivitets_id}: {exc}"
    return row_idx, rows, kontekst, xml_errors, error_message


def _parse_xml_lossless_for_rows(
    rows: list[dict[str, Any]],
    logger: Any,
) -> tuple[
    list[list[AktivitetsresultatRow] | None],
    list[AktivitetskontekstRow | None],
    int,
]:
    """Parse XML payloads for all rows using the lossless full parser.

    Returns:
        (parsed_rows_by_index, kontekst_by_index, total_xml_errors)
    """
    n = len(rows)
    parsed_by_index: list[list[AktivitetsresultatRow] | None] = [None] * n
    kontekst_by_index: list[AktivitetskontekstRow | None] = [None] * n
    tasks: list[_XmlParseTask] = []
    for idx, row_dict in enumerate(rows):
        xml_data = row_dict.get("XML")
        if not xml_data:
            continue
        tasks.append((idx, _build_aktivitets_id(row_dict.get("OBJEKT_ID")), str(xml_data)))

    if not tasks:
        return parsed_by_index, kontekst_by_index, 0

    xml_errors = 0
    logged_errors = 0

    def _consume(results_iter: Any) -> None:
        nonlocal xml_errors, logged_errors
        for row_idx, xml_rows, kontekst, err_count, error_message in results_iter:
            parsed_by_index[row_idx] = xml_rows
            kontekst_by_index[row_idx] = kontekst
            xml_errors += err_count
            if error_message and logged_errors < _XML_LOSSLESS_PARSE_STAGE.max_logged_errors:
                logger.warning(error_message)
                logged_errors += 1

    run_parallel_stage(
        _XML_LOSSLESS_PARSE_STAGE,
        tasks=tasks,
        worker_fn=_parse_xml_lossless_worker,
        logger=logger,
        consume_results=_consume,
    )

    if xml_errors:
        logger.warning("Total XML parse errors: %d", xml_errors)

    return parsed_by_index, kontekst_by_index, xml_errors


def _parse_xml_components_worker(task: _XmlParseTask) -> _XmlParseResult:
    """Worker for fast (Navn,Vaerdi)-only parse -- opt-in via IFDB_XML_FAST_MODE=1."""
    row_idx, aktivitets_id, xml_payload = task
    error_message: str | None = None
    try:
        components, xml_errors = parse_xml_components_fast(xml_payload)
    except Exception as exc:
        components = []
        xml_errors = 1
        error_message = f"XML parse error for aktivitets_id={aktivitets_id}: {exc}"
    return row_idx, components, xml_errors, error_message


def _parse_xml_components_for_rows(
    rows: list[dict[str, Any]],
    logger: Any,
) -> tuple[list[list[_XmlComponentData] | None], int]:
    """Parse XML payloads using the fast component-only parser (opt-in mode)."""
    parsed_by_index: list[list[_XmlComponentData] | None] = [None] * len(rows)
    tasks: list[_XmlParseTask] = []
    for idx, row_dict in enumerate(rows):
        xml_data = row_dict.get("XML")
        if not xml_data:
            continue
        tasks.append((idx, _build_aktivitets_id(row_dict.get("OBJEKT_ID")), str(xml_data)))

    if not tasks:
        return parsed_by_index, 0

    xml_errors = 0
    logged_errors = 0

    def _consume(results_iter: Any) -> None:
        nonlocal xml_errors, logged_errors
        for row_idx, components, err_count, error_message in results_iter:
            parsed_by_index[row_idx] = components
            xml_errors += err_count
            if error_message and logged_errors < _XML_FAST_PARSE_STAGE.max_logged_errors:
                logger.warning(error_message)
                logged_errors += 1

    run_parallel_stage(
        _XML_FAST_PARSE_STAGE,
        tasks=tasks,
        worker_fn=_parse_xml_components_worker,
        logger=logger,
        consume_results=_consume,
    )

    if xml_errors:
        logger.warning("Total XML parse errors: %d", xml_errors)

    return parsed_by_index, xml_errors
