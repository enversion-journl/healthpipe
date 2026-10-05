# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: Appointment extraction using DFM checkpoint.

Reads the Appointment flat view from the Aidbox sof schema filtered by DFM
checkpoint and MERGEs into fhir.Appointment. Built by the shared factory so the
extraction lifecycle (checkpoint window, mode handling, run log) stays
identical across all DFM SOF assets.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_appointment_dfm = create_sof_dfm_asset(
    asset_name="sof_appointment_dfm",
    config_key="appointment_view",
    checkpoint_resource="Appointment",
    deps=["fhir_sof_infrastructure", "fhir_server_appointment_dfm"],
)
