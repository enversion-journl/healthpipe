# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Condition category code systems and builders."""

from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding

SYSTEM_CONDITION_CATEGORY_HL7 = "http://terminology.hl7.org/CodeSystem/condition-category"
SYSTEM_CONDITION_CATEGORY_LOCAL = "http://example.org/fhir/CodeSystem/condition-category"

CODE_ENCOUNTER_DIAGNOSIS = "encounter-diagnosis"
CODE_REFERRAL_INDICATION = "referral-indication"


def build_encounter_diagnosis_category() -> list[CodeableConcept]:
    return [
        CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_CONDITION_CATEGORY_HL7,
                    code=CODE_ENCOUNTER_DIAGNOSIS,
                )
            ]
        )
    ]


def build_referral_indication_category() -> list[CodeableConcept]:
    return [
        CodeableConcept(
            coding=[
                Coding(
                    system=SYSTEM_CONDITION_CATEGORY_LOCAL,
                    code=CODE_REFERRAL_INDICATION,
                    display="Henvisningsindikation",
                )
            ]
        )
    ]
