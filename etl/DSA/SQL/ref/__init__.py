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

  * ref.PatientToBorger        — entity-mapping (dw_ek_Borger ↔ patient_id)
  * ref.Patient_Address_To_Borger — version-mapping (dw_sk_Borger per address
                                  period with non-overlapping LEAD intervals)
  * ref.FakturaToPostering     — fct.Faktura ↔ fct.Postering bridge with
                                  newest-selection flags for cross-FK apply
  * ref.Organisation           — entity-mapping (dw_ek_Organisation ↔
                                  organization_id), replaces map.Organisation
  * ref.RekvirentUndtagelse    — ILS rekvUndtagne exclusion list for reelt
                                  forbrug (CleanCode PK; orphans kept)
"""

from .faktura_to_postering import ref_faktura_to_postering
from .fakturalinje_matchkeys import ref_fakturalinje_matchkeys
from .kmd_fakturalinje_unspsc_match import ref_kmd_fakturalinje_unspsc_match
from .organisation import ref_organisation
from .patient_borger import ref_patient_borger
from .patient_address_borger import ref_patient_address_borger
from .rekvirent_undtagelse import ref_RekvirentUndtagelse

all_assets = [
    ref_patient_borger,
    ref_patient_address_borger,
    ref_faktura_to_postering,
    ref_kmd_fakturalinje_unspsc_match,
    ref_fakturalinje_matchkeys,
    ref_organisation,
    ref_RekvirentUndtagelse,
]

__all__ = [
    "ref_patient_borger",
    "ref_patient_address_borger",
    "ref_faktura_to_postering",
    "ref_kmd_fakturalinje_unspsc_match",
    "ref_fakturalinje_matchkeys",
    "ref_organisation",
    "ref_RekvirentUndtagelse",
    "all_assets",
]
