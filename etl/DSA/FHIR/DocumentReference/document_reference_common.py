# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared helpers for the IFDB → FHIR DocumentReference transforms (US-FHIR-07).

Used by both ``DocumentReference_IFDB_Epikrise_dfm`` (epikrise) and
``DocumentReference_IFDB_CareDischargeReport_dfm`` (udskrivningsrapport).

DocumentReference-only modelling (no Composition); the document body/text is
not modelled in FHIR (archived separately), so every DocumentReference carries
a single metadata-only ``content`` element (R4B requires content 1..*).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from core.resources import MSSQLResource
from DSA.FHIR.utils.ifdb.transforms import parse_clob_datetime_safe
from fhir.resources.R4B.attachment import Attachment
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.documentreference import DocumentReferenceContent
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.meta import Meta
from utils.fhir.fhir_extensions import EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS
from utils.fhir.identifier_systems import SYSTEM_IFDB_SOURCE_TABLE

# Source-table meta.tag codes (§7 #11 — searchable provenance without polluting
# clinical fields).
SOURCE_TABLE_EPIKRISE = "epikrise"
SOURCE_TABLE_CARE_DISCHARGE_REPORT = "care_discharge_report"

# Relative sub-field URLs inside a "recipient" wrapper (Mønster A — same style as
# the "specialeKode"/... sub-fields under aktivitetsudfoerelseSpeciale).
RECIPIENT_WRAPPER_URL = "recipient"


def map_document_reference_status(deleted_value: int) -> str:
    """Map IFDB soft-delete flag to R4B DocumentReferenceStatus.

    Repo-wide convention: a soft-deleted source row is tomb-stoned as
    ``entered-in-error`` (never hard-excluded). Active rows are ``current``.
    """
    return "entered-in-error" if deleted_value != 0 else "current"


def build_metadata_only_content() -> list[DocumentReferenceContent]:
    """Single metadata-only content element (no ``url``); satisfies R4B content 1..*."""
    return [DocumentReferenceContent(attachment=Attachment(contentType="text/plain"))]


def build_source_table_meta(source_table_code: str) -> Meta:
    """meta.tag carrying source-table provenance (epikrise | care_discharge_report)."""
    return Meta(tag=[Coding(system=SYSTEM_IFDB_SOURCE_TABLE, code=source_table_code)])


def append_subfield(
    subfields: list[Extension], sub_url: str, raw: Any, *, is_date: bool = False
) -> None:
    """Append a scalar sub-field Extension when the source value is present.

    Shared by the signature-role and recipient extension builders (Mønster A/B):
    empty/None values are skipped; ``is_date`` emits ``valueDateTime`` (parsed
    defensively via ``parse_clob_datetime_safe``), otherwise ``valueString``.
    """
    if raw is None:
        return
    if is_date:
        dt = parse_clob_datetime_safe(raw)
        if dt:
            subfields.append(Extension(url=sub_url, valueDateTime=dt))
        return
    value = str(raw).strip()
    if value:
        subfields.append(Extension(url=sub_url, valueString=value))


def build_recipients_extension(
    recipient_subfields: list[list[Extension]],
) -> Extension | None:
    """Build the recipients parent extension (Mønster A).

    ``recipient_subfields`` is one list of scalar sub-field Extensions per
    recipient. Each non-empty recipient becomes a same-url ``"recipient"``
    wrapper under the single parent extension. Returns None when there are no
    recipients (so the caller can omit the extension entirely).
    """
    wrappers = [
        Extension(url=RECIPIENT_WRAPPER_URL, extension=subfields)
        for subfields in recipient_subfields
        if subfields
    ]
    if not wrappers:
        return None
    return Extension(url=EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS, extension=wrappers)


def fetch_child_rows_for_batch(
    mssql: MSSQLResource,
    table: str,
    key_column: str,
    parent_ids: list[str],
    columns: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Fetch active child rows (dw_is_deleted=0) grouped by ``key_column`` value.

    Generic version of Encounter's ``_fetch_location_data_for_batch`` — used to
    pull recipient rows (epikrise_modtager / care_discharge_report_receiver) for
    a batch of parent documents. Chunks IN-lists at 999 to stay within MSSQL
    parameter limits.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if not parent_ids:
        return grouped

    col_sql = ", ".join(f"[{c}]" for c in columns)
    chunk_size = 999
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for i in range(0, len(parent_ids), chunk_size):
                chunk = parent_ids[i : i + chunk_size]
                placeholders = ", ".join(f"'{pid}'" for pid in chunk)
                query = (
                    f"SELECT {col_sql} FROM [{table.split('.')[0]}].[{table.split('.')[1]}] "
                    f"WHERE [dw_is_deleted] = 0 AND [{key_column}] IN ({placeholders})"
                )
                cursor.execute(query)
                result_cols = [desc[0] for desc in cursor.description]
                for row in cursor.fetchall():
                    row_dict = dict(zip(result_cols, row, strict=False))
                    grouped[str(row_dict[key_column])].append(row_dict)
        finally:
            cursor.close()

    return grouped


def advance_fhir_checkpoint(
    dfm: Any,
    mssql: MSSQLResource,
    resource_name: str,
    config: Any,
    total_fetched: int,
) -> str | None:
    """Advance the fhir_server checkpoint to SYSDATETIME() (shared with Encounter pattern)."""
    new_checkpoint: str | None = None
    if config.should_advance_checkpoint and total_fetched > 0:
        with mssql.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT CONVERT(NVARCHAR(50), SYSDATETIME(), 121)")
                row = cursor.fetchone()
                new_checkpoint = row[0] if row else None
            finally:
                cursor.close()
        if new_checkpoint:
            dfm.set_checkpoint("fhir_server", resource_name, value=new_checkpoint, mode=config.mode)
    return new_checkpoint
