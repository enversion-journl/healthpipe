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

from .Appointment_dfm import sof_appointment_dfm
from .CarePlan_dfm import (
    sof_careplan_activity_dfm,
    sof_careplan_dfm,
)
from .Condition_dfm import (
    sof_condition_dfm,
    sof_condition_henvisning_dfm,
    sof_condition_supplementary_code_dfm,
)
from .DocumentReference_dfm import sof_documentreference_dfm
from .DocumentReference_Recipient_dfm import sof_documentreference_recipient_dfm
from .Encounter_dfm import sof_encounter_dfm
from .Encounter_Location_dfm import sof_encounter_location_dfm
from .EpisodeOfCare_dfm import sof_episode_of_care_dfm
from .Location_dfm import sof_location_dfm
from .Observation_dfm import sof_observation_dfm, sof_observation_speciale_dfm
from .Organization import sof_organization
from .Patient_dfm import sof_patient_dfm
from .PatientAddress_dfm import sof_patient_address_dfm
from .Practitioner_dfm import sof_practitioner_dfm
from .PractitionerRole_dfm import sof_practitioner_role_dfm
from .Procedure_dfm import (
    sof_procedure_dfm,
    sof_procedure_supplementary_code_dfm,
)
from .QuestionnaireResponse_dfm import (
    sof_questionnaire_dfm,
    sof_questionnaire_item_dfm,
    sof_questionnaire_response_dfm,
    sof_questionnaire_response_item_dfm,
)
from .WardUnitBed_dfm import (
    sof_bed_dfm,
    sof_encounter_absence_dfm,
    sof_encounter_bed_space_dfm,
    sof_ward_unit_dfm,
    sof_ward_unit_capacity_dfm,
)
from .ServiceRequest_dfm import (
    sof_service_request_dfm,
    sof_service_request_supplementary_code_dfm,
)
from .ServiceRequest_Recipient_dfm import sof_service_request_recipient_dfm

all_assets = [
    sof_condition_supplementary_code_dfm,
    sof_condition_henvisning_dfm,
    sof_condition_dfm,
    sof_organization,
    sof_careplan_dfm,
    sof_careplan_activity_dfm,
    sof_procedure_dfm,
    sof_procedure_supplementary_code_dfm,
    sof_service_request_dfm,
    sof_service_request_supplementary_code_dfm,
    sof_service_request_recipient_dfm,
    sof_observation_dfm,
    sof_observation_speciale_dfm,
    # DFM checkpoint-based SOF assets
    sof_patient_dfm,
    sof_patient_address_dfm,
    sof_episode_of_care_dfm,
    sof_encounter_dfm,
    sof_encounter_location_dfm,
    sof_documentreference_dfm,
    sof_documentreference_recipient_dfm,
    sof_location_dfm,
    sof_questionnaire_dfm,
    sof_questionnaire_item_dfm,
    sof_questionnaire_response_dfm,
    sof_questionnaire_response_item_dfm,
    sof_appointment_dfm,
    sof_ward_unit_dfm,
    sof_ward_unit_capacity_dfm,
    sof_bed_dfm,
    sof_encounter_bed_space_dfm,
    sof_encounter_absence_dfm,
    sof_practitioner_dfm,
    sof_practitioner_role_dfm,
]

__all__ = [
    "sof_condition_supplementary_code_dfm",
    "sof_condition_henvisning_dfm",
    "sof_condition_dfm",
    "sof_organization",
    "sof_careplan_dfm",
    "sof_careplan_activity_dfm",
    "sof_procedure_dfm",
    "sof_procedure_supplementary_code_dfm",
    "sof_service_request_dfm",
    "sof_service_request_supplementary_code_dfm",
    "sof_service_request_recipient_dfm",
    "sof_observation_dfm",
    "sof_observation_speciale_dfm",
    "sof_patient_dfm",
    "sof_patient_address_dfm",
    "sof_episode_of_care_dfm",
    "sof_encounter_dfm",
    "sof_encounter_location_dfm",
    "sof_documentreference_dfm",
    "sof_documentreference_recipient_dfm",
    "sof_location_dfm",
    "sof_questionnaire_dfm",
    "sof_questionnaire_item_dfm",
    "sof_questionnaire_response_dfm",
    "sof_questionnaire_response_item_dfm",
    "sof_appointment_dfm",
    "sof_ward_unit_dfm",
    "sof_ward_unit_capacity_dfm",
    "sof_bed_dfm",
    "sof_encounter_bed_space_dfm",
    "sof_encounter_absence_dfm",
    "sof_practitioner_dfm",
    "sof_practitioner_role_dfm",
    "all_assets",
]
