# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Constants for FHIR extension URLs used in the Healthpipe ETL pipeline.

These URLs define custom extensions for capturing source system data
that doesn't fit standard FHIR elements.
"""

from __future__ import annotations

# Extension URLs for EpisodeOfCare
EXTENSION_PREVIOUS_EPISODE_OF_CARE = (
    "http://example.org/fhir/StructureDefinition/previousEpisodeOfCare"
)
EXTENSION_PREVIOUS_EPISODE_TYPE = (
    "http://example.org/fhir/StructureDefinition/previousEpisodeOfCareType"
)
EXTENSION_PREVIOUS_EOC_REF_TYPE = "urn:source:ifdb:previous_eoc_ref_type_code"
EXTENSION_DISCHARGE_DISPOSITION = "urn:source:ifdb:discharge_disposition_code"

# Extension URLs for Observation / Procedure
EXTENSION_EPISODE_OF_CARE = "http://example.org/fhir/StructureDefinition/episodeOfCare"
EXTENSION_ENCOUNTER = "http://example.org/fhir/StructureDefinition/encounter"

# Extension URLs for source metadata
EXTENSION_SOURCE_CREATED_AT = "http://example.org/fhir/StructureDefinition/sourceCreatedAt"
EXTENSION_SOURCE_MODIFIED_AT = (
    "http://example.org/fhir/StructureDefinition/sourceModifiedAt"
)
# Extension URLs for AKTIVITETSRESULTAT lossless XML fields
EXTENSION_XML_KONTEKST = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatKontekst"
)
EXTENSION_XML_HIERARCHY = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatHierarki"
)
EXTENSION_XML_ELEMENT_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatElementMeta"
)
EXTENSION_XML_ROOT_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatRootMeta"
)
EXTENSION_XML_ROW_TYPE = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatRowType"
)
EXTENSION_AKTIVITETSRESULTAT_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatSourceMeta"
)

# SFI instance-level metadata (per-registration fields that vary per occurrence)
EXTENSION_SFI_INSTANCE_META = "http://example.org/fhir/StructureDefinition/sfiInstanceMeta"

# AKTIVITETSUDFOERELSE administrative metadata extension
EXTENSION_AKTIVITETSUDFOERELSE_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsudfoerelseMetadata"
)

# AKTIVITETTYPE_KODE hierarchy split (LEVEL1-8)
EXTENSION_AKTIVITETTYPE_KODE_HIERARKI = (
    "http://example.org/fhir/StructureDefinition/aktivitettypeKodeHierarki"
)

EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE = (
    "http://example.org/fhir/StructureDefinition/aktivitetsudfoerelseSpeciale"
)

# AKTIVITETSRESULTAT-derived Procedure extensions
EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatProcedureSourceMeta"
)

# AKTIVITETSRESULTAT-derived Condition extensions
EXTENSION_AKTIVITETSRESULTAT_CONDITION_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatConditionSourceMeta"
)

# =============================================================================
# ADMINISTRATIVE_PROCEDURE lossless extensions
# =============================================================================
EXTENSION_PROCEDURE_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/procedureSourceMeta"
)
EXTENSION_PROCEDURE_LOCATION_META = (
    "http://example.org/fhir/StructureDefinition/procedureLocationMeta"
)

# =============================================================================
# PAS_DIAGNOSE lossless extensions
# =============================================================================
EXTENSION_PAS_DIAGNOSE_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/pasDiagnoseSourceMeta"
)

# =============================================================================
# RIS_YDELSE lossless extensions
# =============================================================================
EXTENSION_RIS_YDELSE_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/risYdelseSourceMeta"
)
EXTENSION_RIS_YDELSE_LOCATION_META = (
    "http://example.org/fhir/StructureDefinition/risYdelseLocationMeta"
)
EXTENSION_RIS_YDELSE_XML = "http://example.org/fhir/StructureDefinition/risYdelseXml"

# =============================================================================
# ServiceRequest source metadata extensions
# =============================================================================
EXTENSION_HENVISNING_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/henvisningSourceMeta"
)
# Distinct from EXTENSION_RIS_YDELSE_SOURCE_META (Procedure): the ServiceRequest
# RIS source-meta carries a different payload (request/order fields), so it must
# have its own URL — one extension URL must map to exactly one definition.
EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META = (
    "http://example.org/fhir/StructureDefinition/serviceRequestRisYdelseSourceMeta"
)

# Extension URL for MODTIDSPUNKT (used for temporal address tracking)
EXTENSION_MODTIDSPUNKT = "http://example.org/fhir/StructureDefinition/modtidspunkt"

# Extension URLs for Patient demographic data
EXTENSION_DISTRICT = "http://example.org/fhir/StructureDefinition/district"
EXTENSION_SOCIAL_DISTRICT = "http://example.org/fhir/StructureDefinition/social-district"
EXTENSION_ADDRESS_PROTECTION = (
    "http://example.org/fhir/StructureDefinition/address-protection"
)

# Danish CPR person status codes
# See: https://cpr.dk/cpr-systemet/personnumre-uden-telegraferingskode/
CPR_STATUS_DECEASED = {"90"}  # Code 90 = Deceased
CPR_STATUS_INACTIVE = {"80"}  # Code 80 = Annulled/cancelled CPR

# =============================================================================
# CarePlan Activity Extension URLs
# =============================================================================
EXTENSION_ACTIVITY_IDENTIFIER = (
    "http://example.org/fhir/StructureDefinition/activity-source-identifier"
)
EXTENSION_ACTIVITY_EPISODE_OF_CARE = (
    "http://example.org/fhir/StructureDefinition/activity-episode-of-care"
)

# =============================================================================
# CarePlan Hospital/Overafdeling Matching Extension URLs
# =============================================================================
# NOTE: These extensions are used to indicate whether post-FHIR hospital/overafdeling
# matching is required for Maksimale Ventetider CarePlans.
#
# For journeys NOT starting with AFB*, AFA*, or AFD*, the start and end markers
# should be from the same hospital (for udredning/behandling) or overafdeling
# (for efterbehandling), UNLESS an omvisitering marker (AFV01X3) is present.
#
# This matching is NOT performed during ETL. It should be performed post-FHIR using
# the Organization references in the CarePlan activities.
EXTENSION_REQUIRES_HOSPITAL_MATCHING = (
    "http://example.org/fhir/StructureDefinition/requires-hospital-matching"
)
EXTENSION_HAS_REREFERRAL = "http://example.org/fhir/StructureDefinition/has-rereferral"
EXTENSION_MATCHING_LEVEL = "http://example.org/fhir/StructureDefinition/matching-level"
