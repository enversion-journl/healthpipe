# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Henvisning historik data from IFDB Oracle.

Extracts referral history rows used to enrich ServiceRequest resources.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

HENVISNING_HISTORIK_CONFIG = TableExtractionConfig(
    table="IFDB.HENVISNING_HISTORIK",
    columns=[
        "HENVISNING_ID",
        "ORDNING",
        "HANDLING",
        "BESLUT_TIDSPUNKT",
        "FRA_KODE",
        "FRA_KODETEKST",
        "STATUS_KODE",
        "STATUS_KODETEKST",
        "TIL_AFDELING_SORKODE",
        "MODTAGETDATO",
        "VISITATIONSDATO",
        "VENTEGRUPPE",
        "EKSPERTOMRAADE",
        "TREATMENTRIGHT",
        "FAELLESVISITATION",
        "DIAGNOSE_ID",
    ],
    partition_column="BESLUT_TIDSPUNKT",
    description="Referral history rows from IFDB.HENVISNING_HISTORIK.",
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=50_000,
        env_prefix="IFDB_EXTRACT_HENVISNING_HISTORIK",
    ),
)

(
    extract_henvisning_historik_daily,
    extract_henvisning_historik_monthly,
    extract_henvisning_historik_bulk,
) = create_extraction_assets(
    entity_name="henvisning_historik",
    config=HENVISNING_HISTORIK_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_henvisning_historik_daily",
    "extract_henvisning_historik_monthly",
    "extract_henvisning_historik_bulk",
    "HENVISNING_HISTORIK_CONFIG",
]
