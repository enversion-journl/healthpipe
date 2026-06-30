# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: AKTIVITETSUDFOERELSE data from IFDB Oracle.

Extracts activity execution metadata from IFDB.AKTIVITETSUDFOERELSE table
with daily, monthly, and bulk load partition support.

Example Region joins this table to AKTIVITETSRESULTAT via
AKTIVITETSRESULTAT.UDFOERELSE_ID = AKTIVITETSUDFOERELSE.OBJEKT_ID
to capture administrative audit trails (creator/modifier), confidentiality,
and the link to PLANLAGT_AKTIVITET.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

AKTIVITETSUDFOERELSE_CONFIG = TableExtractionConfig(
    table="IFDB.AKTIVITETSUDFOERELSE",
    columns=[
        "OBJEKT_ID",
        "OPDATERET_TID",
        "SLETTET",
        "MODTIDSPUNKT",
        "PATIENT_ID",
        "UDFOERT_PERSID",
        "UDFOERT_PERSNAVN",
        "UDFOERT_BRUGERROLLE",
        "UDFOERT_ADMENHED_KODE",
        "UDFOERT_ADMENHED_NAVN",
        "UDFOERT_ADMENHED_ORGKODE",
        "UDFOERT_TIDSPUNKT",
        "KLADDE",
        "AKTIVITET_ID",
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
        "FORTR_NIVEAU",
        "FORTR_GRUPPE_ID",
        "FORTR_GRUPPE_NAVN",
    ],
    partition_column="OPDATERET_TID",
    description="Activity execution metadata from IFDB EPJ system.",
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=2000,
        env_prefix="IFDB_EXTRACT_AKTIVITETSUDFOERELSE",
    ),
)

(
    extract_aktivitetsudfoerelse_daily,
    extract_aktivitetsudfoerelse_monthly,
    extract_aktivitetsudfoerelse_bulk,
) = create_extraction_assets(
    entity_name="aktivitetsudfoerelse",
    config=AKTIVITETSUDFOERELSE_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_aktivitetsudfoerelse_daily",
    "extract_aktivitetsudfoerelse_monthly",
    "extract_aktivitetsudfoerelse_bulk",
    "AKTIVITETSUDFOERELSE_CONFIG",
]
