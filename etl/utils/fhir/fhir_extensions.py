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
# Forløbstype fra IFDB.HEALTHISSUE, båret som valueCoding-extension. Denne konstant
# er extension-URL'en; valueCoding.system er SYSTEM_IFDB_HEALTHISSUE_CODE i
# identifier_systems.py. Samme strengværdi, men to roller — hold dem adskilt, så
# den ene kan versioneres uden at slæbe den anden med.
EXTENSION_IFDB_HEALTHISSUE_CODE = "urn:source:ifdb:healthissue_code"

# Extension URLs for Observation / Procedure
EXTENSION_EPISODE_OF_CARE = "http://example.org/fhir/StructureDefinition/episodeOfCare"
EXTENSION_ENCOUNTER = "http://example.org/fhir/StructureDefinition/encounter"

# Extension URLs for Location
EXTENSION_LOCATION_VALIDITY_PERIOD = (
    "http://example.org/fhir/StructureDefinition/locationValidityPeriod"
)

# Patient: written by the CPR register contribution (patient_cpr_contribution.py).
EXTENSION_PATIENT_CITIZENSHIP = "http://hl7.org/fhir/StructureDefinition/patient-citizenship"
EXTENSION_MOTHER_IDENTIFIER = "http://example.org/fhir/StructureDefinition/motherIdentifier"
EXTENSION_FATHER_IDENTIFIER = "http://example.org/fhir/StructureDefinition/fatherIdentifier"
EXTENSION_ADDRESS_KVHX = "http://example.org/fhir/StructureDefinition/addressKvhx"
# Delivery date of the latest CPR delivery that changed the register's fields on the citizen.
EXTENSION_CPR_UPDATED = "http://example.org/fhir/StructureDefinition/cprUpdated"
EXTENSION_ADDRESS_OFFICIAL = "http://hl7.org/fhir/StructureDefinition/address-official"
EXTENSION_DK_MUNICIPALITY_CODES = (
    "http://hl7.dk/fhir/core/StructureDefinition/dk-core-municipalityCodes"
)

# Extension URLs for source metadata
EXTENSION_SOURCE_CREATED_AT = "http://example.org/fhir/StructureDefinition/sourceCreatedAt"
EXTENSION_SOURCE_MODIFIED_AT = "http://example.org/fhir/StructureDefinition/sourceModifiedAt"
# Extension URLs for AKTIVITETSRESULTAT lossless XML fields
EXTENSION_XML_KONTEKST = "http://example.org/fhir/StructureDefinition/aktivitetsresultatKontekst"
EXTENSION_XML_HIERARCHY = "http://example.org/fhir/StructureDefinition/aktivitetsresultatHierarki"
EXTENSION_XML_ELEMENT_META = (
    "http://example.org/fhir/StructureDefinition/aktivitetsresultatElementMeta"
)
EXTENSION_XML_ROOT_META = "http://example.org/fhir/StructureDefinition/aktivitetsresultatRootMeta"
EXTENSION_XML_ROW_TYPE = "http://example.org/fhir/StructureDefinition/aktivitetsresultatRowType"
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
EXTENSION_PROCEDURE_SOURCE_META = "http://example.org/fhir/StructureDefinition/procedureSourceMeta"
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
EXTENSION_RIS_YDELSE_SOURCE_META = "http://example.org/fhir/StructureDefinition/risYdelseSourceMeta"
EXTENSION_RIS_YDELSE_LOCATION_META = (
    "http://example.org/fhir/StructureDefinition/risYdelseLocationMeta"
)
EXTENSION_RIS_YDELSE_XML = "http://example.org/fhir/StructureDefinition/risYdelseXml"

# =============================================================================
# ServiceRequest source metadata extensions
# =============================================================================
# Discriminates which source table a ServiceRequest row came from
# (HENVISNING vs RIS_YDELSE). First-class so the SoF view reads it in one
# anchored hop instead of scanning source-meta sub-extensions.
EXTENSION_SOURCE_TABLE = "http://example.org/fhir/StructureDefinition/sourceTable"
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

# Diagnosis type qualifier on Condition (PAS_DIAGNOSE.DIAGNOSETYPE: A=aktions-,
# B=bi-, V=henvisnings-/visitationsdiagnose). FHIR Condition has no native
# diagnosis-role element (the standard puts roles on Encounter.diagnosis), so
# the source code is carried as a first-class extension.
EXTENSION_DIAGNOSETYPE = "http://example.org/fhir/StructureDefinition/diagnosetype"

# Extension URLs for Patient demographic data
EXTENSION_DISTRICT = "http://example.org/fhir/StructureDefinition/district"
EXTENSION_SOCIAL_DISTRICT = "http://example.org/fhir/StructureDefinition/social-district"
EXTENSION_ADDRESS_PROTECTION = "http://example.org/fhir/StructureDefinition/address-protection"
# Municipality of the current address, set by the Patient builder; kept on a protected Patient.
EXTENSION_RESIDENCE_MUNICIPALITY = (
    "http://example.org/fhir/StructureDefinition/residenceMunicipality"
)

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

# Rækkefølge af en Coding i en liste (fx IDX på tillægskoder). FHIR bevarer ikke
# listeposition gennem SQL-on-FHIR, så kildens ordenstal lægges eksplicit på hver Coding.
EXTENSION_CODING_SEQUENCE = "http://example.org/fhir/StructureDefinition/codingSequence"

# =============================================================================
# Senge / kapacitet (IFDB SENGEENHED, ENCOUNTER_BED_SPACE_HISTORY, ABSENCE)
# =============================================================================
# Lossless ADMENHED triple on the sengeenhed Location (Mønster A — parent
# whose .extension is kode/navn/orgkode valueString sub-fields).
EXTENSION_SENGEENHED_ADMENHED = "urn:source:ifdb:sengeenhed_admenhed"
# Room name on a bed Location (ROOM has no identity of its own).
EXTENSION_BED_ROOM = "urn:source:ifdb:bed_room"
# Kildens enhedskode paa sengeophold og seng, specifikt
# ENCOUNTER_BED_SPACE_HISTORY.LOCATION_CODE -- URL'en navngiver den kolonne, ikke
# "en lokationskode": én extension-URL skal daekke praecis én definition.
# Samme kodeomraade som WardUnit.admin_unit_code (SENGEENHED.ADMENHED_KODE): maalt til
# 100 % direkte match paa alle sengeopholdsraekker, saa belaegning kan joines til
# kapacitet pr. sengeenhed uden omvejen over SOR.
EXTENSION_BED_SPACE_WARD_UNIT_CODE = "urn:source:ifdb:bed_space_ward_unit_code"
# Absence list on Encounter (Mønster A — one parent per source row; sub-fields
# use the fragment-URL convention {base}#{field}: #period, #type, #expectedReturn).
EXTENSION_ENCOUNTER_ABSENCE = "urn:source:ifdb:encounter_absence"

# =============================================================================
# Practitioner/PractitionerRole from SilkeborgData E26PERS
# =============================================================================
# The organisational unit has no reference target: SilkeborgData's Inst and
# Afdelingskode are neither SOR nor SHAK, so PractitionerRole.organization and
# .location cannot be populated. Both are carried as single valueCoding
# extensions rather than pushed into PractitionerRole.code, which is for roles.
# urn:source:silkeborgdata:{snake_case} follows the IFDB extension scheme.
EXTENSION_SILKEBORGDATA_INST = "urn:source:silkeborgdata:inst"
EXTENSION_SILKEBORGDATA_AFDELINGSKODE = "urn:source:silkeborgdata:afdelingskode"

# Statuskode and Jobstatuskode are payroll status codes carried verbatim, with
# no translation: the consumers of the flat tables know them from the payroll
# system. PractitionerRole has no status element in R4B, and a status is not a
# role, so code is not their home either — hence extensions, same shape as Inst
# and Afdelingskode above.
EXTENSION_SILKEBORGDATA_STATUSKODE = "urn:source:silkeborgdata:statuskode"
EXTENSION_SILKEBORGDATA_JOBSTATUSKODE = "urn:source:silkeborgdata:jobstatuskode"
# When the status took effect. Deliberately NOT period.end: that would assert
# that Statuskode 3 means "left", and the codes are carried without a code list
# precisely because nothing here knows what they mean.
EXTENSION_SILKEBORGDATA_STATUSDATO = "urn:source:silkeborgdata:statusdato"

# Two more untranslated payroll codes, both per-employment: measured on the full
# archive, 2.663 of 15.643 persons holding several employments carry differing
# Tjenestekode across them and 8.066 differing Omraadekode, so neither is a
# person attribute. Tjenestekode is 89,6 % filled with 13 values but "0" on
# 86,8 % of the newest period; Omraadekode is 100 % filled with 3 values, "1" on
# 90,1 %, and changes over time on 21,4 % of employments — a state, not stamdata.
EXTENSION_SILKEBORGDATA_TJENESTEKODE = "urn:source:silkeborgdata:tjenestekode"
EXTENSION_SILKEBORGDATA_OMRAADEKODE = "urn:source:silkeborgdata:omraadekode"

# The CPR a person held before a correction. NOT a second identifier under
# SYSTEM_CPR: extract_identifier_value is LIMIT 1 and the unique index sits on
# that expression, so a second identifier of the same system makes the index key
# depend on array position — and the superseded number already owns its own
# (inactive) resource, which would then be the collision partner.
EXTENSION_SILKEBORGDATA_AFLOEST_CPR = "urn:source:silkeborgdata:afloest_cpr"
