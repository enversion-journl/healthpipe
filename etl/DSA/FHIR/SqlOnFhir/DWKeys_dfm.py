# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dw_ek_Borger, dw_sk_Borger and dw_ek_Organisation on the flat fhir.* tables.

One asset per table, after its SoF asset and before its DSO sync. The configuration names
the columns the keys are looked up from; the organisation column is the one pointing at
the organisation responsible for the row.
"""

from DSA.FHIR.utils.sql_on_fhir.dw_keys import DWKeysConfig
from DSA.FHIR.utils.sql_on_fhir.dw_keys_factory import create_dw_keys_asset
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS


def _config(view_name: str, asset_suffix: str, **columns: str) -> DWKeysConfig:
    """Table and primary key come from the SoF config, so both assets lock the same table."""
    sof = SOF_RESOURCE_CONFIGS[view_name]
    (primary_key,) = sof["primary_keys"]
    return DWKeysConfig(
        asset_suffix=asset_suffix, table=sof["mssql_table"], primary_key=primary_key, **columns
    )


DW_KEYS_CONFIG: dict[str, DWKeysConfig] = {
    "episode_of_care": _config(
        "episode_of_care_view",
        "episode_of_care",
        patient_column="patient_id",
        time_column="period_start",
        organisation_column="managing_organization_id",
    ),
    "location": _config(
        "location_view",
        "location",
        organisation_column="managing_organization_id",
    ),
}

fhir_dw_keys_episode_of_care = create_dw_keys_asset(
    DW_KEYS_CONFIG["episode_of_care"], sof_asset="sof_episode_of_care_dfm"
)
fhir_dw_keys_location = create_dw_keys_asset(
    DW_KEYS_CONFIG["location"], sof_asset="sof_location_dfm"
)
