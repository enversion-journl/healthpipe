# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""dw_ek_Borger, dw_sk_Borger and dw_ek_Organisation on the flat fhir.* tables.

One asset per table, after its SoF asset and before its DSO sync. The configuration names
the columns the keys are looked up from; the organisation column is the one pointing at
the organisation responsible for the row. A table with more than one organisation reference
names them by role and gets one ``dw_ek_Organisation_<role>`` per role. A table whose only
organisation link is a reference to another keyed table inherits that table's key.
"""

from DSA.FHIR.utils.sql_on_fhir.dw_keys import DWKeysConfig, OrganisationKey
from DSA.FHIR.utils.sql_on_fhir.dw_keys_factory import create_dw_keys_asset
from DSA.FHIR.utils.sql_on_fhir.sof_utils import SOF_RESOURCE_CONFIGS


def _config(
    view_name: str,
    asset_suffix: str,
    *,
    organisation_column: str | None = None,
    organisation_roles: dict[str, str] | None = None,
    organisation_via: DWKeysConfig | None = None,
    patient_column: str | None = None,
    time_column: str | None = None,
    time_column_is_text: bool = False,
) -> DWKeysConfig:
    """Table and primary key come from the SoF config, so both assets lock the same table.

    ``organisation_roles`` maps each role to its column, in place of ``organisation_column``.
    With ``organisation_via`` the organisation column references that table instead.
    """
    if organisation_column is not None and organisation_roles is None:
        organisations: tuple[OrganisationKey, ...] = (
            OrganisationKey(organisation_column, via=organisation_via),
        )
    elif (
        organisation_roles is not None and organisation_column is None and organisation_via is None
    ):
        organisations = tuple(OrganisationKey(c, role) for role, c in organisation_roles.items())
    else:
        raise ValueError(f"{view_name}: give organisation_column or organisation_roles")
    sof = SOF_RESOURCE_CONFIGS[view_name]
    (primary_key,) = sof["primary_keys"]
    return DWKeysConfig(
        asset_suffix=asset_suffix,
        table=sof["mssql_table"],
        primary_key=primary_key,
        organisations=organisations,
        patient_column=patient_column,
        time_column=time_column,
        time_column_is_text=time_column_is_text,
    )


_LOCATION = _config(
    "location_view",
    "location",
    organisation_column="managing_organization_id",
)

DW_KEYS_CONFIG: dict[str, DWKeysConfig] = {
    "appointment": _config(
        "appointment_view",
        "appointment",
        patient_column="patient_id",
        time_column="start",
        time_column_is_text=True,
        organisation_column="performing_location_id",
        organisation_via=_LOCATION,
    ),
    "episode_of_care": _config(
        "episode_of_care_view",
        "episode_of_care",
        patient_column="patient_id",
        time_column="period_start",
        organisation_column="managing_organization_id",
    ),
    "location": _LOCATION,
    "careplan": _config(
        "careplan_view",
        "careplan",
        patient_column="patient_id",
        time_column="period_start",
        time_column_is_text=True,
        organisation_column="organization_id",
    ),
    "careplan_activity": _config(
        "careplan_activity_view",
        "careplan_activity",
        patient_column="patient_id",
        time_column="activity_timestamp",
        time_column_is_text=True,
        organisation_column="organization_id",
    ),
    "documentreference": _config(
        "documentreference_view",
        "documentreference",
        patient_column="patient_id",
        time_column="doc_date",
        time_column_is_text=True,
        organisation_column="author_org_ref",
    ),
    "encounter": _config(
        "encounter_view",
        "encounter",
        patient_column="patient_id",
        time_column="period_start",
        time_column_is_text=True,
        organisation_column="service_provider_id",
    ),
    "service_request": _config(
        "servicerequest_view",
        "service_request",
        patient_column="patient_id",
        time_column="authored_on",
        time_column_is_text=True,
        organisation_roles={"henvist_til": "performer_id", "henvist_fra": "requester_id"},
    ),
    "ward_unit": _config(
        "ward_unit_view",
        "ward_unit",
        organisation_column="managing_organization_id",
    ),
}

fhir_dw_keys_appointment = create_dw_keys_asset(
    DW_KEYS_CONFIG["appointment"], sof_asset="sof_appointment_dfm"
)
fhir_dw_keys_episode_of_care = create_dw_keys_asset(
    DW_KEYS_CONFIG["episode_of_care"], sof_asset="sof_episode_of_care_dfm"
)
fhir_dw_keys_location = create_dw_keys_asset(
    DW_KEYS_CONFIG["location"], sof_asset="sof_location_dfm"
)
fhir_dw_keys_careplan = create_dw_keys_asset(
    DW_KEYS_CONFIG["careplan"], sof_asset="sof_careplan_dfm"
)
fhir_dw_keys_careplan_activity = create_dw_keys_asset(
    DW_KEYS_CONFIG["careplan_activity"], sof_asset="sof_careplan_activity_dfm"
)
fhir_dw_keys_documentreference = create_dw_keys_asset(
    DW_KEYS_CONFIG["documentreference"], sof_asset="sof_documentreference_dfm"
)
fhir_dw_keys_encounter = create_dw_keys_asset(
    DW_KEYS_CONFIG["encounter"], sof_asset="sof_encounter_dfm"
)
fhir_dw_keys_service_request = create_dw_keys_asset(
    DW_KEYS_CONFIG["service_request"], sof_asset="sof_service_request_dfm"
)
fhir_dw_keys_ward_unit = create_dw_keys_asset(
    DW_KEYS_CONFIG["ward_unit"], sof_asset="sof_ward_unit_dfm"
)
