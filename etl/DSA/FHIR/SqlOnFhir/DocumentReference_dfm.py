# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: DocumentReference extraction using DFM checkpoint.

Reads DocumentReference flat view from Aidbox sof schema filtered by DFM checkpoint
(instead of Dagster partitions) and MERGEs into fhir.DocumentReference.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_documentreference_dfm = create_sof_dfm_asset(
    asset_name="sof_documentreference_dfm",
    config_key="documentreference_view",
    checkpoint_resource="DocumentReference",
    deps=[
        "fhir_sof_infrastructure",
        "fhir_server_documentreference_epikrise_dfm",
        "fhir_server_documentreference_care_discharge_report_dfm",
    ],
)
