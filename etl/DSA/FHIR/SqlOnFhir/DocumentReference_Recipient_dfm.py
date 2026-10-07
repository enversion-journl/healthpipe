# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: DocumentReference_Recipient extraction using DFM checkpoint.

Reads the DocumentReference recipient flat view from Aidbox sof schema filtered by
DFM checkpoint and MERGEs into fhir.DocumentReference_Recipient. The view flattens
the recipients extension (forEach over the recipients extension list); recipient_key
is the stable per-recipient identity carried in the FHIR resource.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_documentreference_recipient_dfm = create_sof_dfm_asset(
    asset_name="sof_documentreference_recipient_dfm",
    config_key="documentreference_recipient_view",
    checkpoint_resource="DocumentReference_Recipient",
    deps=[
        "fhir_sof_infrastructure",
        "fhir_server_documentreference_epikrise_dfm",
        "fhir_server_documentreference_care_discharge_report_dfm",
    ],
    description=(
        "Extract DocumentReference recipient flat view from Aidbox sof schema "
        "(DFM checkpoint) and MERGE into fhir.DocumentReference_Recipient"
    ),
)
