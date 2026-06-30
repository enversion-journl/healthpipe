# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR domain assets aggregator.

Collects assets from all FHIR resource directories and SqlOnFhir.
"""

from .Aktivitetsresultat.assets import all_assets as aktivitetsresultat_assets
from .Appointment import all_assets as appointment_assets
from .CarePlan import all_assets as careplan_assets
from .Condition import all_assets as condition_assets
from .Encounter import all_assets as encounter_assets
from .EpisodeOfCare import all_assets as episode_of_care_assets
from .Location import all_assets as location_assets
from .Observation import all_assets as observation_assets
from .Organization import all_assets as organization_assets
from .Patient import all_assets as patient_assets
from .Procedure import all_assets as procedure_assets
from .Questionnaire import all_assets as questionnaire_assets
from .QuestionnaireResponse import all_assets as questionnaire_response_assets
from .ServiceRequest import all_assets as service_request_assets
from .SqlOnFhir import all_assets as sql_on_fhir_assets

all_assets = [
    *patient_assets,
    *encounter_assets,
    *episode_of_care_assets,
    *location_assets,
    *organization_assets,
    *observation_assets,
    *aktivitetsresultat_assets,
    *careplan_assets,
    *condition_assets,
    *service_request_assets,
    *procedure_assets,
    *questionnaire_assets,
    *questionnaire_response_assets,
    *appointment_assets,
    *sql_on_fhir_assets,
]

__all__ = ["all_assets"]
