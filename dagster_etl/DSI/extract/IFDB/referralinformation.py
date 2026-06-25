# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Referral information data from IFDB Oracle.

Extracts EpisodeOfCare-linked referral metadata used to enrich ServiceRequest.
"""

from core.extract import ExtractBatchingPolicy
from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

REFERRALINFORMATION_CONFIG = TableExtractionConfig(
    table="IFDB.REFERRALINFORMATION",
    columns=[
        "OBJECT_ID",
        "REFERRAL_ORG_CODE",
        "REFERRAL_ORG_NAME",
        "REFERRAL_ORG_ORGCODE",
        "REFERRAL_ORG_SORCODE",
        "PRACTITIONER_NUMBER",
        "PRACTITIONER_NAME",
        "REFERRAL_TYPE_CODE",
        "REFERRAL_TYPE_TEXT",
        "REFERRAL_REASON_CODE",
        "REFERRAL_REASON_TEXT",
        "FREECHOICE_CODE",
        "FREECHOICE_TEXT",
        "REFERRAL_DATE",
    ],
    partition_column="REFERRAL_DATE",
    description="Referral metadata from IFDB.REFERRALINFORMATION.",
    batching=ExtractBatchingPolicy(
        enabled=True,
        fetch_size=50_000,
        env_prefix="IFDB_EXTRACT_REFERRALINFORMATION",
    ),
)

(
    extract_referralinformation_daily,
    extract_referralinformation_monthly,
    extract_referralinformation_bulk,
) = create_extraction_assets(
    entity_name="referralinformation",
    config=REFERRALINFORMATION_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_referralinformation_daily",
    "extract_referralinformation_monthly",
    "extract_referralinformation_bulk",
    "REFERRALINFORMATION_CONFIG",
]
