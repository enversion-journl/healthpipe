# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: PractitionerRole extraction using DFM checkpoint.

Reads the PractitionerRole flat view from the Aidbox sof schema filtered by DFM
checkpoint and MERGEs into fhir.PractitionerRole.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_practitioner_role_dfm = create_sof_dfm_asset(
    asset_name="sof_practitioner_role_dfm",
    config_key="practitionerrole_view",
    checkpoint_resource="PractitionerRole",
    deps=["fhir_sof_infrastructure", "fhir_server_practitioner_role_dfm"],
    # See Practitioner_dfm.py for why this is not in the shared fhir_sof_dfm.
    group_name="fhir_practitioner_sof",
)
