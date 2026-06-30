# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Diagnose data from IFDB Oracle.

Extracts diagnosis records from IFDB.DIAGNOSE table.
Columns follow the IFDB naming convention with Danish field names.

NOTE: Oracle metadata verification shows DIAGNOSE does not have OPDATERET_TID.
MODTIDSPUNKT is used as partition column.
"""

from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

DIAGNOSE_CONFIG = TableExtractionConfig(
    table="IFDB.DIAGNOSE",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        # Besluttet (decided)
        "BESLUT_PERSID",
        "BESLUT_PERSNAVN",
        "BESLUT_BRUGERROLLE",
        "BESLUT_ADMENHED_KODE",
        "BESLUT_ADMENHED_NAVN",
        "BESLUT_ADMENHED_ORGKODE",
        "BESLUT_TIDSPUNKT",
        # Afsluttet (concluded)
        "AFSLUT_PERSID",
        "AFSLUT_PERSNAVN",
        "AFSLUT_BRUGERROLLE",
        "AFSLUT_ADMENHED_KODE",
        "AFSLUT_ADMENHED_NAVN",
        "AFSLUT_ADMENHED_ORGKODE",
        "AFSLUT_TIDSPUNKT",
        # Oprettet (created)
        "OPRET_PERSID",
        "OPRET_PERSNAVN",
        "OPRET_BRUGERROLLE",
        "OPRET_ADMENHED_KODE",
        "OPRET_ADMENHED_NAVN",
        "OPRET_ADMENHED_ORGKODE",
        "OPRET_TIDSPUNKT",
        # Diagnosis code
        "DIAGNOSE_KODE",
        "DIAGNOSE_KODETEKST",
    ],
    partition_column="MODTIDSPUNKT",
    description="Patient diagnoses from IFDB with ICD-10/SKS codes.",
)

(
    extract_diagnose_daily,
    extract_diagnose_monthly,
    extract_diagnose_bulk,
) = create_extraction_assets(
    entity_name="diagnose",
    config=DIAGNOSE_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_diagnose_daily",
    "extract_diagnose_monthly",
    "extract_diagnose_bulk",
    "DIAGNOSE_CONFIG",
]
