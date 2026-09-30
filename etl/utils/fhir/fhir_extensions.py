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
    "http://regionsyddanmark.dk/fhir/StructureDefinition/previousEpisodeOfCare"
)
EXTENSION_PREVIOUS_EPISODE_TYPE = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/previousEpisodeOfCareType"
)
EXTENSION_PREVIOUS_EOC_REF_TYPE = "urn:source:ifdb:previous_eoc_ref_type_code"
EXTENSION_DISCHARGE_DISPOSITION = "urn:source:ifdb:discharge_disposition_code"
# Forløbstype fra IFDB.HEALTHISSUE, båret som valueCoding-extension. Denne konstant
# er extension-URL'en; valueCoding.system er SYSTEM_IFDB_HEALTHISSUE_CODE i
# identifier_systems.py. Samme strengværdi, men to roller — hold dem adskilt, så
# den ene kan versioneres uden at slæbe den anden med.
EXTENSION_IFDB_HEALTHISSUE_CODE = "urn:source:ifdb:healthissue_code"

# Extension URLs for Observation / Procedure
EXTENSION_EPISODE_OF_CARE = "http://regionsyddanmark.dk/fhir/StructureDefinition/episodeOfCare"
EXTENSION_ENCOUNTER = "http://regionsyddanmark.dk/fhir/StructureDefinition/encounter"

# Extension URLs for source metadata
EXTENSION_SOURCE_CREATED_AT = "http://regionsyddanmark.dk/fhir/StructureDefinition/sourceCreatedAt"
EXTENSION_SOURCE_MODIFIED_AT = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/sourceModifiedAt"
)
# Extension URLs for AKTIVITETSRESULTAT lossless XML fields
EXTENSION_XML_KONTEKST = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatKontekst"
)
EXTENSION_XML_HIERARCHY = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatHierarki"
)
EXTENSION_XML_ELEMENT_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatElementMeta"
)
EXTENSION_XML_ROOT_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRootMeta"
)
EXTENSION_XML_ROW_TYPE = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatRowType"
)
EXTENSION_AKTIVITETSRESULTAT_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatSourceMeta"
)

# SFI instance-level metadata (per-registration fields that vary per occurrence)
EXTENSION_SFI_INSTANCE_META = "http://regionsyddanmark.dk/fhir/StructureDefinition/sfiInstanceMeta"

# AKTIVITETSUDFOERELSE administrative metadata extension
EXTENSION_AKTIVITETSUDFOERELSE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsudfoerelseMetadata"
)

# AKTIVITETTYPE_KODE hierarchy split (LEVEL1-8)
EXTENSION_AKTIVITETTYPE_KODE_HIERARKI = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitettypeKodeHierarki"
)

EXTENSION_AKTIVITETSUDFOERELSE_SPECIALE = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsudfoerelseSpeciale"
)

# AKTIVITETSRESULTAT-derived Procedure extensions
EXTENSION_AKTIVITETSRESULTAT_PROCEDURE_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatProcedureSourceMeta"
)

# AKTIVITETSRESULTAT-derived Condition extensions
EXTENSION_AKTIVITETSRESULTAT_CONDITION_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/aktivitetsresultatConditionSourceMeta"
)

# =============================================================================
# ADMINISTRATIVE_PROCEDURE lossless extensions
# =============================================================================
EXTENSION_PROCEDURE_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/procedureSourceMeta"
)
EXTENSION_PROCEDURE_LOCATION_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/procedureLocationMeta"
)

# =============================================================================
# PAS_DIAGNOSE lossless extensions
# =============================================================================
EXTENSION_PAS_DIAGNOSE_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/pasDiagnoseSourceMeta"
)

# =============================================================================
# RIS_YDELSE lossless extensions
# =============================================================================
EXTENSION_RIS_YDELSE_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/risYdelseSourceMeta"
)
EXTENSION_RIS_YDELSE_LOCATION_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/risYdelseLocationMeta"
)
EXTENSION_RIS_YDELSE_XML = "http://regionsyddanmark.dk/fhir/StructureDefinition/risYdelseXml"

# =============================================================================
# ServiceRequest source metadata extensions
# =============================================================================
# Discriminates which source table a ServiceRequest row came from
# (HENVISNING vs RIS_YDELSE). First-class so the SoF view reads it in one
# anchored hop instead of scanning source-meta sub-extensions.
EXTENSION_SOURCE_TABLE = "http://regionsyddanmark.dk/fhir/StructureDefinition/sourceTable"
EXTENSION_HENVISNING_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/henvisningSourceMeta"
)
# Distinct from EXTENSION_RIS_YDELSE_SOURCE_META (Procedure): the ServiceRequest
# RIS source-meta carries a different payload (request/order fields), so it must
# have its own URL — one extension URL must map to exactly one definition.
EXTENSION_SERVICE_REQUEST_RIS_SOURCE_META = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/serviceRequestRisYdelseSourceMeta"
)

# Extension URL for MODTIDSPUNKT (used for temporal address tracking)
EXTENSION_MODTIDSPUNKT = "http://regionsyddanmark.dk/fhir/StructureDefinition/modtidspunkt"

# Diagnosis type qualifier on Condition (PAS_DIAGNOSE.DIAGNOSETYPE: A=aktions-,
# B=bi-, V=henvisnings-/visitationsdiagnose). FHIR Condition has no native
# diagnosis-role element (the standard puts roles on Encounter.diagnosis), so
# the source code is carried as a first-class extension.
EXTENSION_DIAGNOSETYPE = "http://regionsyddanmark.dk/fhir/StructureDefinition/diagnosetype"

# Extension URLs for Patient demographic data
EXTENSION_DISTRICT = "http://regionsyddanmark.dk/fhir/StructureDefinition/district"
EXTENSION_SOCIAL_DISTRICT = "http://regionsyddanmark.dk/fhir/StructureDefinition/social-district"
EXTENSION_ADDRESS_PROTECTION = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/address-protection"
)

# Danish CPR person status codes
# See: https://cpr.dk/cpr-systemet/personnumre-uden-telegraferingskode/
CPR_STATUS_DECEASED = {"90"}  # Code 90 = Deceased
CPR_STATUS_INACTIVE = {"80"}  # Code 80 = Annulled/cancelled CPR

# =============================================================================
# CarePlan Activity Extension URLs
# =============================================================================
EXTENSION_ACTIVITY_IDENTIFIER = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/activity-source-identifier"
)
EXTENSION_ACTIVITY_EPISODE_OF_CARE = (
    "http://regionsyddanmark.dk/fhir/StructureDefinition/activity-episode-of-care"
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
    "http://regionsyddanmark.dk/fhir/StructureDefinition/requires-hospital-matching"
)
EXTENSION_HAS_REREFERRAL = "http://regionsyddanmark.dk/fhir/StructureDefinition/has-rereferral"
EXTENSION_MATCHING_LEVEL = "http://regionsyddanmark.dk/fhir/StructureDefinition/matching-level"

# =============================================================================
# US-FHIR-07: DocumentReference extensions (epikrise + udskrivningsrapport)
# =============================================================================
# These three use the urn:source:ifdb: identifier scheme (snake_case), matching
# the other source-derived URN extensions and all the DocumentReference
# identifier/code systems in identifier_systems.py — rather than the http
# StructureDefinition form used by most other extensions in this file.
# Recipient list (Mønster A — parent extension whose .extension is a list of
# same-url "recipient" wrappers, each carrying scalar sub-fields).
EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS = "urn:source:ifdb:document_reference_recipients"
# Epikrise priority (PRIORITET) — single valueString extension.
EXTENSION_DOCUMENT_REFERENCE_PRIORITY = "urn:source:ifdb:document_reference_priority"
# Signature roles (Mønster B — top-level distinctly-named extensions with a
# "#underskriver" / "#godkender" / "#registrant" suffix on this base identifier).
EXTENSION_DOCUMENT_REFERENCE_SIGNATURE = "urn:source:ifdb:document_reference_signature_role"

# =============================================================================
# ServiceRequest from IFDB.REHABILITATION_PLAN (+ _RECEIVER)
# =============================================================================
# Recipient list (Mønster A — parent extension whose .extension is a list of
# same-url "receiver" wrappers, each carrying scalar sub-fields). Mirrors
# EXTENSION_DOCUMENT_REFERENCE_RECIPIENTS; urn:source:ifdb:{snake_case} scheme.
EXTENSION_REHABILITATION_PLAN_RECEIVERS = "urn:source:ifdb:rehabilitation_plan_receivers"
# Lossless source-meta (parent-level) carrying sub-fields sourceTable,
# senderAfdelingKode, senderAfdelingKodetekst, planVersion. Pattern precedent is
# EXTENSION_HENVISNING_SOURCE_META — the SOF sourceTable sub-field path
# (extension.extension.where(url='sourceTable')) does NOT filter on the parent
# URL, so [fhir].[ServiceRequest].source_table projects from this extension unchanged.
EXTENSION_REHABILITATION_PLAN_SOURCE_META = "urn:source:ifdb:rehabilitation_plan_source_meta"
