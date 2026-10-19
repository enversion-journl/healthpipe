# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: ServiceRequest extraction using DFM checkpoint.

Reads the ServiceRequest flat view (and its tillaegskode child view) from the
Aidbox sof schema filtered by DFM checkpoint instead of Dagster partitions, and
MERGEs into fhir.ServiceRequest / fhir.ServiceRequest_SupplementaryCode.

Both producers of fhir.ServiceRequest are on the DFM path (the
henvisning/RIS pipeline; the rehabilitation-plan slice was already there), so
this SOF sync can live in the DFM SOF+DSO job. See §4.3 of
docs/archive/2026-08-06_fhir_ifdb_arkiv_migrering_estimat.md for why the SOF
layer must wait for the *last* producer of a flat table.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_SR_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_service_request_dfm",
    "fhir_server_rehabilitation_plan_dfm",
]

sof_service_request_dfm = create_sof_dfm_asset(
    asset_name="sof_service_request_dfm",
    config_key="servicerequest_view",
    checkpoint_resource="ServiceRequest",
    deps=_SR_PRODUCERS,
)

# The child view is derived from the parent's resources, so it follows the
# parent SOF asset rather than re-reading the producers (same pattern as the
# Condition/Procedure child views).
sof_service_request_supplementary_code_dfm = create_sof_dfm_asset(
    asset_name="sof_service_request_supplementary_code_dfm",
    config_key="servicerequest_supplementary_code_view",
    checkpoint_resource="ServiceRequest_SupplementaryCode",
    deps=["fhir_sof_infrastructure", "sof_service_request_dfm"],
    description=(
        "Extract ServiceRequest_SupplementaryCode child view from Aidbox sof schema "
        "(DFM checkpoint) and MERGE into fhir.ServiceRequest_SupplementaryCode"
    ),
)
