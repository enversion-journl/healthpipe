# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: PAS_DIAGNOSE data from IFDB Oracle.

Extracts PAS (Patientadministrativt System) diagnosis records from IFDB.PAS_DIAGNOSE.
These are encounter-level diagnoses distinct from the clinician-level DIAGNOSE table.

Partition column: OPDATERET_TID (not MODTIDSPUNKT — verified in Oracle metadata).
"""

from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

PAS_DIAGNOSE_CONFIG = TableExtractionConfig(
    table="IFDB.PAS_DIAGNOSE",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "DIAGNOSETYPE",
        "DIAGNOSE_KODE",
        "DIAGNOSE_KODETEKST",
        "OPDATERET_TID",
        "LATER_DISPROVED",
        "MODIFICERET_PERSID",
        "MODIFICERET_PERSNAVN",
        "MODIFICERET_BRUGERROLLE",
        "MODIFICERET_ADMENHED_KODE",
        "MODIFICERET_ADMENHED_KODETEKST",
        "MODIFICERET_ADMENHED_ORGKODE",
        "MODIFICERET_ADMENHED_SORKODE",
    ],
    partition_column="OPDATERET_TID",
    description="PAS encounter-level diagnoses from IFDB with ICD-10/SKS codes.",
)

(
    extract_pas_diagnose_daily,
    extract_pas_diagnose_monthly,
    extract_pas_diagnose_bulk,
) = create_extraction_assets(
    entity_name="pas_diagnose",
    config=PAS_DIAGNOSE_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_pas_diagnose_daily",
    "extract_pas_diagnose_monthly",
    "extract_pas_diagnose_bulk",
    "PAS_DIAGNOSE_CONFIG",
]
