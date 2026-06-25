# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
SQL on FHIR (SqlOnFhir) -- Reads flattened FHIR data FROM the Aidbox sof schema
and loads it into MSSQL Data Warehouse fhir.* tables.

This is the reverse direction of IFDB which WRITEs TO the FHIR server.
SqlOnFhir reads FROM the FHIR server's SQL on FHIR materialized views.

Architecture:
    1. ViewDefinitions are managed via REST API on Aidbox
    2. Aidbox materializes each ViewDefinition as a SQL view in the "sof" schema
    3. DSA assets query sof views via direct PostgreSQL access (FHIRDatabaseResource)
    4. Results are MERGED into MSSQL fhir.* tables for downstream analytics
"""

from .Appointment import sof_appointment_daily, sof_appointment_monthly
from .CarePlan import (
    sof_careplan_daily,
    sof_careplan_monthly,
)
from .CarePlan_Activity import (
    sof_careplan_activity_daily,
    sof_careplan_activity_monthly,
)
from .Condition import sof_condition_daily, sof_condition_monthly
from .Encounter_dfm import sof_encounter_dfm
from .Encounter_Location_dfm import sof_encounter_location_dfm
from .EpisodeOfCare_dfm import sof_episode_of_care_dfm
from .Location_dfm import sof_location_dfm
from .Observation import sof_observation_daily, sof_observation_monthly
from .Organization import sof_organization
from .Patient_dfm import sof_patient_dfm
from .PatientAddress_dfm import sof_patient_address_dfm
from .procedure import sof_procedure_daily, sof_procedure_monthly
from .QuestionnaireResponse import (
    sof_questionnaire_daily,
    sof_questionnaire_item_daily,
    sof_questionnaire_item_monthly,
    sof_questionnaire_monthly,
    sof_questionnaire_response_daily,
    sof_questionnaire_response_item_daily,
    sof_questionnaire_response_item_monthly,
    sof_questionnaire_response_monthly,
)
from .ServiceRequest import sof_service_request_daily, sof_service_request_monthly

all_assets = [
    sof_organization,
    sof_careplan_daily,
    sof_careplan_monthly,
    sof_careplan_activity_daily,
    sof_careplan_activity_monthly,
    sof_procedure_daily,
    sof_procedure_monthly,
    sof_service_request_daily,
    sof_service_request_monthly,
    sof_observation_daily,
    sof_observation_monthly,
    sof_condition_daily,
    sof_condition_monthly,
    # DFM checkpoint-based SOF assets
    sof_patient_dfm,
    sof_patient_address_dfm,
    sof_episode_of_care_dfm,
    sof_encounter_dfm,
    sof_encounter_location_dfm,
    sof_location_dfm,
    sof_questionnaire_daily,
    sof_questionnaire_monthly,
    sof_questionnaire_item_daily,
    sof_questionnaire_item_monthly,
    sof_questionnaire_response_daily,
    sof_questionnaire_response_monthly,
    sof_questionnaire_response_item_daily,
    sof_questionnaire_response_item_monthly,
    sof_appointment_daily,
    sof_appointment_monthly,
]

__all__ = [
    "sof_organization",
    "sof_careplan_daily",
    "sof_careplan_monthly",
    "sof_careplan_activity_daily",
    "sof_careplan_activity_monthly",
    "sof_procedure_daily",
    "sof_procedure_monthly",
    "sof_service_request_daily",
    "sof_service_request_monthly",
    "sof_observation_daily",
    "sof_observation_monthly",
    "sof_condition_daily",
    "sof_condition_monthly",
    "sof_patient_dfm",
    "sof_patient_address_dfm",
    "sof_episode_of_care_dfm",
    "sof_encounter_dfm",
    "sof_encounter_location_dfm",
    "sof_location_dfm",
    "sof_questionnaire_daily",
    "sof_questionnaire_monthly",
    "sof_questionnaire_item_daily",
    "sof_questionnaire_item_monthly",
    "sof_questionnaire_response_daily",
    "sof_questionnaire_response_monthly",
    "sof_questionnaire_response_item_daily",
    "sof_questionnaire_response_item_monthly",
    "sof_appointment_daily",
    "sof_appointment_monthly",
    "all_assets",
]
