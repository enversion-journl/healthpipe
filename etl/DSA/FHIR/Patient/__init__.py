# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from DSA.FHIR.Patient.Patient_CPR_dfm import fhir_server_patient_cpr_dfm
from DSA.FHIR.Patient.Patient_DRG import fhir_patient_psyk_drg, fhir_patient_somatik_drg
from DSA.FHIR.Patient.Patient_HAIBA import fhir_patient_haiba
from DSA.FHIR.Patient.Patient_IFDB_dfm import fhir_server_patient_dfm
from DSA.FHIR.Patient.Patient_Udredningsret import fhir_patient_udredningsret

all_assets = [
    fhir_server_patient_dfm,
    fhir_server_patient_cpr_dfm,
    fhir_patient_haiba,
    fhir_patient_psyk_drg,
    fhir_patient_somatik_drg,
    fhir_patient_udredningsret,
]
