# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR domain assets aggregator.

Collects assets from all FHIR resource directories and SqlOnFhir.
"""

from DSA.FHIR.Aktivitetsresultat.assets import all_assets as aktivitetsresultat_assets
from DSA.FHIR.Appointment import all_assets as appointment_assets
from DSA.FHIR.CarePlan import all_assets as careplan_assets
from DSA.FHIR.Condition import all_assets as condition_assets
from DSA.FHIR.DocumentReference import all_assets as document_reference_assets
from DSA.FHIR.Encounter import all_assets as encounter_assets
from DSA.FHIR.EpisodeOfCare import all_assets as episode_of_care_assets
from DSA.FHIR.Location import all_assets as location_assets
from DSA.FHIR.Observation import all_assets as observation_assets
from DSA.FHIR.Organization import all_assets as organization_assets
from DSA.FHIR.Patient import all_assets as patient_assets
from DSA.FHIR.Practitioner import all_assets as practitioner_assets
from DSA.FHIR.Procedure import all_assets as procedure_assets
from DSA.FHIR.ServiceRequest import all_assets as service_request_assets
from DSA.FHIR.SqlOnFhir import all_assets as sql_on_fhir_assets

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
    *document_reference_assets,
    *service_request_assets,
    *practitioner_assets,
    *procedure_assets,
    *appointment_assets,
    *sql_on_fhir_assets,
]

__all__ = ["all_assets"]
