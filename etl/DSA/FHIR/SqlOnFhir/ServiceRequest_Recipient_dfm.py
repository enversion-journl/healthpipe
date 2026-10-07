# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: ServiceRequest_Recipient extraction using DFM checkpoint.

Reads the genoptræningsplan-modtager flat view from the Aidbox sof schema filtered
by DFM checkpoint and MERGEs into fhir.ServiceRequest_Recipient. The view flattens
the rehabilitation_plan_receivers extension (forEach over the receiver wrappers);
recipient_key is the stable per-recipient identity carried in the FHIR resource.

Only ServiceRequests from source_table='REHABILITATION_PLAN' carry the receivers
extension, so HENVISNING/RIS ServiceRequests contribute 0 child rows (plain forEach
= inner-join semantics — no all-NULL fallback rows). Mirrors
``sof_documentreference_recipient_dfm``.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_service_request_recipient_dfm = create_sof_dfm_asset(
    asset_name="sof_service_request_recipient_dfm",
    config_key="servicerequest_recipient_view",
    checkpoint_resource="ServiceRequest_Recipient",
    deps=[
        "fhir_sof_infrastructure",
        "fhir_server_rehabilitation_plan_dfm",
    ],
    description=(
        "Extract ServiceRequest recipient flat view from Aidbox sof schema "
        "(DFM checkpoint) and MERGE into fhir.ServiceRequest_Recipient"
    ),
)
