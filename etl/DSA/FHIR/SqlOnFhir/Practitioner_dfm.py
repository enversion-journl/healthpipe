# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR: Practitioner extraction using DFM checkpoint.

Reads the Practitioner flat view from the Aidbox sof schema filtered by DFM
checkpoint and MERGEs into fhir.Practitioner.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

sof_practitioner_dfm = create_sof_dfm_asset(
    asset_name="sof_practitioner_dfm",
    config_key="practitioner_view",
    checkpoint_resource="Practitioner",
    deps=["fhir_sof_infrastructure", "fhir_server_practitioner_dfm"],
    # Own group, not the shared fhir_sof_dfm: SilkeborgData runs its whole chain
    # in silkeborgdata_daily_job, so the archive, the resources and this
    # extraction are ordered by the asset graph inside one run. In the shared
    # group it would run with the IFDB jobs instead, where nothing guarantees
    # that the SD archive and FHIR layers had finished successfully first.
    group_name="fhir_silkeborgdata_sof",
)
