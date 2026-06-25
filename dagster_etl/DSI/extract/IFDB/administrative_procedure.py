# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DSI Extraction Layer: Administrative procedure data from IFDB Oracle.

Extracts administrative procedures from IFDB.ADMINISTRATIVE_PROCEDURE table.

NOTE: Column list is inferred from IFDB naming conventions and
the related ADM_PROCEDURE_SUPPL_CODES table.
Columns and partition column are now aligned with Oracle metadata.
"""

from DSI.connectors.oracle.asset_factory import create_extraction_assets
from DSI.connectors.oracle.models import TableExtractionConfig

ADMINISTRATIVE_PROCEDURE_CONFIG = TableExtractionConfig(
    table="IFDB.ADMINISTRATIVE_PROCEDURE",
    columns=[
        "OBJECT_ID",
        "PATIENT_ID",
        "ENCOUNTER_ID",
        "EPISODE_OF_CARE_ID",
        "PERIOD_START",
        "PERIOD_END",
        "PROCEDURE_CODE",
        "PROCEDURE_TEXT",
        "STATUS_CODE",
        "STATUS_TEXT",
        "INDICATION_OF_SIDE_CODE",
        "INDICATION_OF_SIDE_TEXT",
        "USED_CONTRAST_CODE",
        "USED_CONTRAST_TEXT",
        "STAFF_CATEGORY_CODE",
        "STAFF_CATEGORY_TEXT",
        "LOCATION_CODE",
        "LOCATION_NAME",
        "LOCATION_ORGCODE",
        "LOCATION_SORCODE",
        "EPJI_CREATED_DATETIME",
        "EPJI_MODIFICATION_DATETIME",
        "IFDB_MODIFICATION_DATETIME",
        "DELETED",
        "USER_ID",
        "USER_NAME",
        "USER_ROLE",
        "LPR3_IGNORED",
        "USER_ROLE_NAME",
    ],
    partition_column="IFDB_MODIFICATION_DATETIME",
    description="Administrative procedures from IFDB with SKS codes.",
)

(
    extract_administrative_procedure_daily,
    extract_administrative_procedure_monthly,
    extract_administrative_procedure_bulk,
) = create_extraction_assets(
    entity_name="administrative_procedure",
    config=ADMINISTRATIVE_PROCEDURE_CONFIG,
    source_deps=[],
)

__all__ = [
    "extract_administrative_procedure_daily",
    "extract_administrative_procedure_monthly",
    "extract_administrative_procedure_bulk",
    "ADMINISTRATIVE_PROCEDURE_CONFIG",
]
