# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: RIS Ydelse data from IFDB Oracle.

Extracts radiology service (RIS) records from IFDB.RIS_YDELSE table.

NOTE: Column list is inferred from IFDB naming conventions.
Columns and partition column are now aligned with Oracle metadata.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

RIS_YDELSE_CONFIG = TableExtractionConfig(
    table="IFDB.RIS_YDELSE",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "TILSTEDEVAERELSE_ID",
        "XML",
        "STAMAFD_KODE",
        "STAMAFD_NAVN",
        "STAMAFD_ORGKODE",
        "UDFOERT_ADMENHED_KODE",
        "UDFOERT_ADMENHED_NAVN",
        "UDFOERT_ADMENHED_ORGKODE",
        "UDFOERT_TIDSPUNKT",
        "OPDATERET_TID",
        "BESKED_ID",
        "FILNAVN",
        "STATE",
        "TYPE",
        "RTEE",
        "IGNORED",
        "PERIOD_START",
        "PERIOD_END",
        "UDFOERT_ADMENHED_SORCODE",
        "STAMAFD_SORCODE",
        "LPR3_IGNORED",
    ],
    partition_column="OPDATERET_TID",
    description="RIS (Radiology Information System) service records from IFDB.",
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=1000,
        env_prefix="IFDB_EXTRACT_RIS_YDELSE",
    ),
)

(
    extract_ris_ydelse_daily,
    extract_ris_ydelse_monthly,
    extract_ris_ydelse_bulk,
) = create_extraction_assets(
    entity_name="ris_ydelse",
    config=RIS_YDELSE_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_ris_ydelse_daily",
    "extract_ris_ydelse_monthly",
    "extract_ris_ydelse_bulk",
    "RIS_YDELSE_CONFIG",
]
