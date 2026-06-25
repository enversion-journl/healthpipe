# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Aktivitetsresultat data from IFDB Oracle.

Extracts clinical activity results from IFDB.AKTIVITETSRESULTAT table,
including the XML CLOB column containing structured form data.
The XML column is read as a string via LOB handling in fetch_rows_as_dicts().
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

AKTIVITETSRESULTAT_CONFIG = TableExtractionConfig(
    table="IFDB.AKTIVITETSRESULTAT",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "UDFOERELSE_ID",
        "AKTIVITETTYPE_KODE",
        "AKTIVITETTYPE_KODETEKST",
        "RESULTATTYPE_KODE",
        "RESULTATTYPE_KODETEKST",
        "OVERSKRIFT",
        "HIERARKISK_PLACERING",
        "DOMAENENAVN",
        "MODELLERINGSNAVN",
        "MODELLERINGSVERSION",
        "XML",
        "TILSTEDEVAERELSE_ID",
        "UDFOERT_PERSID",
        "UDFOERT_PERSNAVN",
        "UDFOERT_BRUGERROLLE",
        "UDFOERT_ADMENHED_KODE",
        "UDFOERT_ADMENHED_NAVN",
        "UDFOERT_ADMENHED_ORGKODE",
        "UDFOERT_TIDSPUNKT",
        "KLADDE",
        "OPDATERET_TID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "PERIOD_START",
        "PERIOD_END",
        "UDFOERT_ADMENHED_SORCODE",
        "LPR3_IGNORED",
        "UDFOERT_BRUGERROLLE_NAME",
        "RESULT_TYPE",
        "RESULT_MOID",
    ],
    partition_column="OPDATERET_TID",
    description=(
        "Clinical activity results from IFDB, including XML CLOB with structured form data."
    ),
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=50_000,
        env_prefix="IFDB_EXTRACT_AKTIVITETSRESULTAT",
        write_batch_sidecars=True,
    ),
)

(
    extract_aktivitetsresultat_daily,
    extract_aktivitetsresultat_monthly,
    extract_aktivitetsresultat_bulk,
) = create_extraction_assets(
    entity_name="aktivitetsresultat",
    config=AKTIVITETSRESULTAT_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_aktivitetsresultat_daily",
    "extract_aktivitetsresultat_monthly",
    "extract_aktivitetsresultat_bulk",
    "AKTIVITETSRESULTAT_CONFIG",
]
