# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Henvisning data from IFDB Oracle.

Extracts referral request records from IFDB.HENVISNING for ServiceRequest.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

HENVISNING_CONFIG = TableExtractionConfig(
    table="IFDB.HENVISNING",
    columns=[
        "OBJEKT_ID",
        "MODTIDSPUNKT",
        "SLETTET",
        "PATIENT_ID",
        "EPISODE_OF_CARE_ID",
        "HENVISNINGSDATO",
        "AFSENDELSESDATO",
        "SLUTDATO",
        "MODTAGET",
        "STATUS_KODE",
        "STATUS_KODETEKST",
        "TYPE_KODE",
        "TYPE_KODETEKST",
        "INDKALDELSESPRIORIT_KODE",
        "INDKALDELSESPRIORIT_KODETEKST",
        "HENVISNINGSMAADE_KODE",
        "HENVISNINGSMAADE_KODETEKST",
        "REFERRAL_PURPOSE",
        "HENVIST_TIL_AFDELING_SORKODE",
        "HENVIST_FRA_AFDELING_SORKODE",
        "AFSENDER_AFDELING_SORKODE",
        "BEHANDLER_AFDELING_SORKODE",
        "OPDATERET_TID",
    ],
    partition_column="OPDATERET_TID",
    description="Referral request records from IFDB.HENVISNING.",
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=50_000,
        env_prefix="IFDB_EXTRACT_HENVISNING",
    ),
)

(
    extract_henvisning_daily,
    extract_henvisning_monthly,
    extract_henvisning_bulk,
) = create_extraction_assets(
    entity_name="henvisning",
    config=HENVISNING_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_henvisning_daily",
    "extract_henvisning_monthly",
    "extract_henvisning_bulk",
    "HENVISNING_CONFIG",
]
