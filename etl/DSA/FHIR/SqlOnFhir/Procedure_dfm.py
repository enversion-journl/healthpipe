# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL on FHIR: Procedure extraction using DFM checkpoint.

All three producers of fhir.Procedure are on the DFM path: ADMINISTRATIVE_PROCEDURE
and RIS_YDELSE plus Aktivitetsresultat/SFI, so the SOF and
DSO layers move here with them rather than lingering in the partitioned daily job.
"""

from DSA.FHIR.utils.sql_on_fhir.sof_dfm_factory import create_sof_dfm_asset

_PRODUCERS = [
    "fhir_sof_infrastructure",
    "fhir_server_procedure_admin_proc_dfm",
    "fhir_server_procedure_ris_ydelse_dfm",
    "fhir_server_aktivitetsresultat_dfm",
]

sof_procedure_dfm = create_sof_dfm_asset(
    asset_name="sof_procedure_dfm",
    config_key="procedure_view",
    checkpoint_resource="Procedure",
    deps=_PRODUCERS,
)

# The child view is derived from the parent's resources, so it follows the
# parent rather than re-reading the producers.
sof_procedure_tillaegskode_dfm = create_sof_dfm_asset(
    asset_name="sof_procedure_tillaegskode_dfm",
    config_key="procedure_tillaegskode_view",
    checkpoint_resource="Procedure_tillaegskode",
    deps=["fhir_sof_infrastructure", "sof_procedure_dfm"],
)

__all__ = ["sof_procedure_dfm", "sof_procedure_tillaegskode_dfm"]
