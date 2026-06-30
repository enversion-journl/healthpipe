# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DFM extract configs and factory-generated assets for KMD UdData source."""

import dagster as dg
from core.resources import KmdUddataMSSQLResource
from DSI.connectors.extract_config import DfmExtractConfig
from DSI.connectors.factory import create_dfm_extraction_assets

KMD_EXTRACT_CONFIGS = [
    DfmExtractConfig(
        source_schema="dimension",
        source_table="UNSPSC",
        target_table="unspsc_kmd_uddata",
        delta_expression="GREATEST([DW_Audit_Insert], [DW_Audit_Update], [DW_Audit])",
        checkpoint_type="int",
        checkpoint_from=("arc", "unspsc_kmd_uddata"),
        description="KMD UNSPSC classification (delta via audit-MAX checkpoint).",
    ),
    DfmExtractConfig(
        source_schema="fact",
        source_table="SpendAnalysisLines",
        target_table="spend_analysis_lines_kmd_uddata",
        delta_expression="GREATEST([DW_Audit_Insert], [DW_Audit_Update], [DW_Audit])",
        checkpoint_type="int",
        checkpoint_from=("arc", "spend_analysis_lines_kmd_uddata"),
        description="KMD SpendAnalysisLines (delta via audit-MAX checkpoint).",
    ),
    DfmExtractConfig(
        source_schema="dimension",
        source_table="Invoice",
        target_table="invoice_kmd_uddata",
        delta_expression="GREATEST([DW_Audit_Insert], [DW_Audit_Update], [DW_Audit])",
        checkpoint_type="int",
        checkpoint_from=("arc", "invoice_kmd_uddata"),
        description="KMD dimension.Invoice (delta via audit-MAX checkpoint).",
    ),
]

kmd_uddata_extract_assets = create_dfm_extraction_assets(
    configs=KMD_EXTRACT_CONFIGS,
    source_resource_key="kmd_uddata_mssql",
    source_resource_type=KmdUddataMSSQLResource,
    source_system="KMD_UdData",
    group_name="kmd_uddata_archive",
    domain_tag="kmd_uddata",
    dialect="mssql",
    retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)
