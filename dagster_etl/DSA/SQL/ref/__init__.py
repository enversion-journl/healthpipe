# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Reference Tables (ref schema)

These tables bridge FHIR-native string IDs (patient_id, encounter_id, ...) to
the INT surrogate keys used by dim.* and fct.* tables. They are derived from
the fhir.* projections, not from any source system directly.

  * ref.Patient_Borger        — entity-mapping (DW_EK_Borger ↔ patient_id)
  * ref.Patient_Address_Borger — version-mapping (DW_SK_Borger per address
                                  period with non-overlapping LEAD intervals)
  * ref.Faktura_To_Postering     — fct.Faktura ↔ fct.Postering bridge with
                                  newest-selection flags for cross-FK apply
  * ref.Organisation           — entity-mapping (DW_EK_Organisation ↔
                                  organization_id), replaces map.Organisation
"""

from .faktura_to_postering import ref_faktura_to_postering
from .organisation import ref_organisation
from .patient_borger import ref_patient_borger
from .patient_address_borger import ref_patient_address_borger

all_assets = [
    ref_patient_borger,
    ref_patient_address_borger,
    ref_faktura_to_postering,
    ref_organisation,
]

__all__ = [
    "ref_patient_borger",
    "ref_patient_address_borger",
    "ref_faktura_to_postering",
    "ref_organisation",
    "all_assets",
]
