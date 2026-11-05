# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Centralized identifier system URIs for FHIR Identifier and Reference.identifier usage.

This module consolidates all identifier systems used across the codebase:
- Shared Danish healthcare identifiers (CPR, SOR, SHAK)
- IFDB-derived identifiers for source system linkage
- Domain-specific identifiers (patient rights, DRG, terminology)

Keep these stable to ensure consistent matching across transformations and loaders.
"""

import re as _re

# =============================================================================
# Person identifiers
# =============================================================================
SYSTEM_CPR = "urn:dk:cpr"
# X-eCPR (national erstatnings-CPR — afventer endelig OID fra Sundhedsdatastyrelsen)
SYSTEM_X_ECPR = "urn:oid:1.2.208.176.1.5.X"  # TODO(SDS): replace with final OID when published
# D-eCPR (regional/decentral erstatnings-CPR)
SYSTEM_D_ECPR = "urn:oid:1.2.208.176.1.5.D"  # TODO(SDS): regional OID placeholder

# =============================================================================
# Danish healthcare organization identifiers
# =============================================================================
SYSTEM_SOR = "https://sundhedsdatastyrelsen.dk/sor"
SYSTEM_SOR_ENTITY_TYPE = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/sor-entity-type"
SYSTEM_SHAK = "urn:dk:shak"  # SHAK codes (Sygehusafdelingskode)
SYSTEM_YDERNUMMER = (
    "urn:oid:1.2.208.176.1.4"  # Ydernummer (NamingSystem/yder) — Danish GP identifier
)

# =============================================================================
# IFDB-derived identifiers (source system linkage)
# =============================================================================
SYSTEM_IFDB_OBJECT_ID = "urn:source:ifdb:object_id"
SYSTEM_IFDB_PATIENT_ID = "urn:source:ifdb:patient_id"
SYSTEM_IFDB_EPISODE_OF_CARE_ID = "urn:source:ifdb:episode_of_care_id"
SYSTEM_IFDB_ENCOUNTER_ID = "urn:source:ifdb:encounter_id"
SYSTEM_IFDB_ORGANIZATION_ID = (
    "urn:source:ifdb:organization_id"  # Corresponds to LOCATION.OBJECT_ID for orgs
)
SYSTEM_IFDB_HEALTHISSUE_ID = "urn:source:ifdb:healthissue_id"
# Code system for forløbstype-koden fra HEALTHISSUE (68 kategorier), båret som
# valueCoding på EpisodeOfCare. Extension-URL'en for samme extension er
# EXTENSION_IFDB_HEALTHISSUE_CODE i fhir_extensions.py — to roller, to konstanter.
# Værdien er (og skal forblive) identisk, så de to kan versioneres uafhængigt.
SYSTEM_IFDB_HEALTHISSUE_CODE = "urn:source:ifdb:healthissue_code"
SYSTEM_IFDB_EVENTMARKER = "urn:source:ifdb:eventmarker"  # EVENTMARKER.OBJECT_ID
SYSTEM_IFDB_AKTIVITETSRESULTAT = (
    "urn:source:ifdb:aktivitetsresultat"  # AKTIVITETSRESULTAT.OBJEKT_ID
)
SYSTEM_IFDB_DIAGNOSE = "urn:source:ifdb:diagnose"  # DIAGNOSE.OBJEKT_ID
SYSTEM_IFDB_PAS_DIAGNOSE = "urn:source:ifdb:pas_diagnose"  # PAS_DIAGNOSE.OBJEKT_ID
SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE = (
    "urn:source:ifdb:administrative_procedure"  # ADMINISTRATIVE_PROCEDURE.OBJEKT_ID
)
SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE = "urn:source:ifdb:aktivitetsresultat_procedure"  # Procedure derived from AKTIVITETSRESULTAT.OBJEKT_ID
SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION = "urn:source:ifdb:aktivitetsresultat_condition"  # Condition derived from AKTIVITETSRESULTAT.OBJEKT_ID
SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION = (
    "urn:source:ifdb:aktivitetsresultat_xml_condition"
    # Condition derived from structured DiagnoseKode XML element: {OBJEKT_ID}/{element_nummer}
)
SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI = "urn:source:ifdb:aktivitetsresultat_sfi"
SYSTEM_IFDB_SFI = "urn:dk:ifdb:sfi"
SYSTEM_IFDB_QUESTIONNAIRE_MODEL = "urn:source:ifdb:questionnaire_model"
SYSTEM_IFDB_HENVISNING_ID = "urn:source:ifdb:henvisning"  # HENVISNING.OBJEKT_ID
SYSTEM_IFDB_RIS_YDELSE = "urn:source:ifdb:ris_ydelse"  # RIS_YDELSE.OBJEKT_ID
SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE = (
    "urn:source:ifdb:ris_ydelse:diagnose"  # RIS_YDELSE.XML <diagnose> — one Condition per row
)
SYSTEM_IFDB_STAFF = "urn:source:ifdb:staff"  # Practitioner staff ID
SYSTEM_IFDB_LOCATION_ID = "urn:source:ifdb:location_id"  # LOCATION.OBJECT_ID
SYSTEM_IFDB_SENGEENHED = "urn:source:ifdb:sengeenhed_id"  # SENGEENHED.OBJEKT_ID
SYSTEM_IFDB_SENGEENHED_KAPACITET = (
    "urn:source:ifdb:sengeenhed_kapacitet"  # {type}:{sengeenhed_id}:{fra_iso}
)
# Observation.category for sengekapacitet. HL7's observation-category har ingen
# kode for driftstal (survey/exam/vital-signs/... er alle kliniske), og bindingen
# er extensible, saa kategorien kommer fra vores eget system paa linje med
# condition-category og procedure-category. Den lader en aftager filtrere
# kapacitet fra uden at kende kapacitetstypens kodesystem.
SYSTEM_RSD_OBSERVATION_CATEGORY = "http://example.org/fhir/CodeSystem/observation-category"
CODE_OBSERVATION_CATEGORY_BED_CAPACITY = "bed-capacity"
# Kapacitetstypen (disponible | normerede). Kilden baerer ikke typen som kode --
# den ligger i, hvilken tabel raekken kommer fra -- saa kodesaettet er vores eget
# og hoerer i samme navnerum som de oevrige egne kodninger (observation-category,
# procedure-category, patient-rights-type). urn:source:ifdb: er forbeholdt
# identifier-systemer, altsaa hvor en noegle kommer fra.
SYSTEM_RSD_WARD_UNIT_CAPACITY_TYPE = "http://example.org/fhir/CodeSystem/ward-unit-capacity-type"
SYSTEM_IFDB_BED = "urn:source:ifdb:bed_id"  # ENCOUNTER_BED_SPACE_HISTORY.BED_ID
# HL7 location-physical-type. Used for bed Locations and Encounter.location entries.
SYSTEM_LOCATION_PHYSICAL_TYPE = "http://terminology.hl7.org/CodeSystem/location-physical-type"

# US-FHIR-07: DocumentReference (epikrise + udskrivningsrapport)
SYSTEM_IFDB_EPIKRISE = "urn:source:ifdb:epikrise"  # EPIKRISE.OBJEKT_ID
SYSTEM_IFDB_CARE_DISCHARGE_REPORT = (
    "urn:source:ifdb:care_discharge_report"  # CARE_DISCHARGE_REPORT.OBJECT_ID
)
SYSTEM_IFDB_NOTIFICATION_IDENTIFIER = (
    "urn:source:ifdb:notification_identifier"  # CARE_DISCHARGE_REPORT.NOTIFICATION_IDENTIFIER
)
# Recipient-key systems — stable per-recipient identity carried into the
# recipients extension so the SOF child table [fhir].[DocumentReference_Recipient]
# has a clean PK (mirrors CarePlan activity-source-identifier / SYSTEM_IFDB_EVENTMARKER).
SYSTEM_IFDB_EPIKRISE_MODTAGER = "urn:source:ifdb:epikrise_modtager"  # EPIKRISE_MODTAGER.OBJEKT_ID
SYSTEM_IFDB_CARE_DISCHARGE_REPORT_RECEIVER = (
    # CARE_DISCHARGE_REPORT_RECEIVER has no own id; key = RECEIVER_AFDELING_KODE|CCRECEIVER
    "urn:source:ifdb:care_discharge_report_receiver"
)

# ServiceRequest from IFDB.REHABILITATION_PLAN (+ _RECEIVER)
SYSTEM_IFDB_REHABILITATION_PLAN = (
    "urn:source:ifdb:rehabilitation_plan"  # REHABILITATION_PLAN.OBJECT_ID
)
# Local plan-type system — TYPE_KODETEKST has no separate code column, so the code IS the text.
SYSTEM_IFDB_REHABILITATION_PLAN_TYPE = "urn:source:ifdb:rehabilitation_plan_type"
# REHABILITATION_PLAN_RECEIVER has no own id; receiverKey = arc modtager_noegle
# (yder, afdeling or SOR code)
SYSTEM_IFDB_REHABILITATION_PLAN_RECEIVER = "urn:source:ifdb:rehabilitation_plan_receiver"

# =============================================================================
# BOM_IFDB-derived identifiers (Bookplan source schema, same Oracle instance as IFDB)
# =============================================================================
SYSTEM_BOM_IFDB_AFTALE_OID = "urn:source:bom_ifdb:aftale_oid"  # AFTALE.OID — Appointment PK
SYSTEM_BOM_IFDB_AFTALE_GUID = "urn:source:bom_ifdb:aftale_guid"  # AFTALE.GUID — stable secondary
SYSTEM_BOM_IFDB_RESSOURCE_OID = (
    "urn:source:bom_ifdb:ressource_oid"  # AFTALERESSOURCEDELTAGER.TRAEK_RESSOURCEOID
)
SYSTEM_BOM_IFDB_PATIENTINFO_OID = (
    "urn:source:bom_ifdb:patientinfo_oid"  # PATIENTINFO.PATIENTINFOOID (CPR-resolver fallback)
)
SYSTEM_BOM_IFDB_REKVISITION_OID = (
    "urn:source:bom_ifdb:rekvisition_oid"  # AFTALE.REKVISITIONOID → REKVISITION.OID
)
SYSTEM_BOM_IFDB_CANCELLATION_REASON = "urn:source:bom_ifdb:cancellation_reason"
SYSTEM_BOM_IFDB_KONTAKTTYPE = "urn:source:bom_ifdb:kontakttype"

# =============================================================================
# Cross-system identifiers
# =============================================================================
SYSTEM_COLUMNA_BOOKING_ACTIVITY_ID = (
    "urn:source:columna:booking_activity_id"  # AFTALE.COLUMNA_AFTALEIDENTIFIKATION
)

# =============================================================================
# Patient Rights CarePlan identifiers
# =============================================================================
# Alle markører is the only patient-rights CarePlan this repository produces.
# The systems for kræftpakker, udredningsret, behandlingsret and maksimale
# ventetider were removed with the transform code behind them: that kind of
# business logic belongs in the datamart repository, which consumes the base
# tables published here.
#
# Common code system for patient rights type (category)
SYSTEM_PATIENT_RIGHTS_TYPE = "http://example.org/fhir/CodeSystem/patient-rights-type"

# Alle markører (All Markers) — one living CarePlan per PERSON, keyed on the
# FHIR patient id (see DSA/FHIR/CarePlan/CarePlan_IFDB_dfm.py).
SYSTEM_CAREPLAN_ALLE_MARKOERER = "urn:dk:sundhed:alle-markoerer:careplan"

# Helbredsforløb (Health Journey) — kept deliberately although nothing writes it
# today. It names a national grouping concept (episodes linked via
# EPISODE_OF_CARE.PREVIOUS_EOC_ID) that a consumer may need to reference, so the
# URI stays stable here rather than being re-invented downstream.
SYSTEM_HELBREDSFORLOEB = "urn:dk:sundhed:helbredsforloeb"

# =============================================================================
# Danish SKS code system (Sundhedsvæsenets Klassifikations System)
# =============================================================================
SYSTEM_SKS = "urn:oid:1.2.208.176.2.4"
# DK Core Condition SKS-D slice (primary diagnosis codings only; supplements stay on SYSTEM_SKS).
SYSTEM_SKS_D = "urn:oid:1.2.208.176.2.4.12"

# =============================================================================
# FHIR Code Systems for Danish healthcare concepts
# =============================================================================
SYSTEM_PAKKETYPEMODEL = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/pakketypemodel"
SYSTEM_PAKKETYPE = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/pakketype"

# =============================================================================
# US-FHIR-07: local IFDB code systems for DocumentReference CodeableConcepts/tags
# (no official SKS/MedCom mapping available — local source code systems)
# =============================================================================
SYSTEM_IFDB_EPIKRISETYPE = "urn:source:ifdb:epikrisetype"  # EPIKRISE.EPIKRISETYPE_KODE
SYSTEM_IFDB_EPIKRISESTATUS = "urn:source:ifdb:epikrisestatus"  # EPIKRISE.EPIKRISESTATUS_KODE
SYSTEM_IFDB_PRIORITET = "urn:source:ifdb:prioritet"  # EPIKRISE.PRIORITET
SYSTEM_IFDB_MESSAGE_STATUS = "urn:source:ifdb:message_status"  # ..._RECEIVER.STATE_KODE
SYSTEM_IFDB_DOCUMENT_TYPE = "urn:source:ifdb:document_type"  # DocumentReference.type (local)
# meta.tag for source-table provenance (epikrise | care_discharge_report)
SYSTEM_IFDB_SOURCE_TABLE = "urn:source:ifdb:source_table"

# =============================================================================
# Procedure.performer.function: hvilken rolle en organisation har på proceduren.
# Udførende enhed og stamafdeling skal kunne skelnes i den flade tabel, når begge
# står som Organization-referencer på samme Procedure (RIS-ydelser).
# =============================================================================
SYSTEM_PERFORMER_FUNCTION = "http://example.org/fhir/CodeSystem/performer-function"
PERFORMER_FUNCTION_UDFOERENDE = "udfoerende"
PERFORMER_FUNCTION_STAMAFDELING = "stamafdeling"
PERFORMER_FUNCTION_DISPLAYS = {
    PERFORMER_FUNCTION_UDFOERENDE: "Udførende enhed",
    PERFORMER_FUNCTION_STAMAFDELING: "Stamafdeling",
}


# =============================================================================
# SilkeborgData E26PERS-derived identifiers and code systems (payroll source)
# =============================================================================
# The employment, not the person: a tjenestenummer is only unique WITHIN an
# institution, and it is reissued to new staff, so the value is composed as
# "{Inst}-{Tjenestenummer}".
SYSTEM_SILKEBORGDATA_TJENESTENUMMER = "urn:source:silkeborgdata:tjenestenummer"
# meta.tag for source-file provenance. IFDB's equivalent is a different source
# and must not be reused.
SYSTEM_SILKEBORGDATA_SOURCE_TABLE = "urn:source:silkeborgdata:source_table"
# One system per source column: filtering PractitionerRole.code on the system is
# the only way to tell the codes apart downstream, so these must never be merged.
SYSTEM_SILKEBORGDATA_STILLINGSKODE = "urn:source:silkeborgdata:stillingskode"
SYSTEM_SILKEBORGDATA_LEDERKODE = "urn:source:silkeborgdata:lederkode"
# DISCO-08-shaped occupational codes. Local system: no official DISCO-08 URI or
# OID has been verified, and the code says what someone is employed as — never
# what they are authorised to do.
SYSTEM_SILKEBORGDATA_DISCOKODE = "urn:source:silkeborgdata:discokode"


# =============================================================================
# EPJI-derived identifiers (EPJ accounts in the PRACTITIONER tables)
# =============================================================================
# The account's own key is EXTERNAL_ID, which is the staff code IFDB carries, so
# EPJI Practitioners use SYSTEM_IFDB_STAFF and not an EPJI system of their own.
# meta.tag for source-table provenance.
SYSTEM_EPJI_SOURCE_TABLE = "urn:source:epji:source_table"
# The region's user identity, as the region's own URI. Shared by a person's
# accounts, so it groups accounts into people and can carry no unique index.
SYSTEM_REGION_REGIONSID = "http://example.org/BSK/RegionsId"
# The other systems in EPJI's PRACTITIONER_IDENTIFIER. None becomes an identifier:
# uniqueid copies EXTERNAL_ID, initials is not unique, and CPR stays in the archive.
EPJI_UNMAPPED_IDENTIFIER_SYSTEMS = frozenset(
    {
        "http://columnafhir.dk/uniqueid",
        "http://columnafhir.dk/initials",
        "http://cpr.dk/personregistret",
    }
)


# =============================================================================
# Safe-character regex for SQL-literal interpolation
# =============================================================================
# Both the FHIR index DDL and the indexed lookup query embed identifier systems
# as SQL literals (expression indexes only match verbatim expressions — a bound
# parameter is opaque to the planner). Callers that interpolate a system into
# SQL must validate it against this regex first.
_SAFE_IDENTIFIER_SYSTEM_RE = _re.compile(r"^[A-Za-z0-9:/_.\-]+$")

# Index specs (UNIQUE/GIN/SoF) have moved to `utils.fhir.index_config` to keep
# this module focused on URI constants. Import from there.

# CPR register PersonID (record 037): stable across personal number changes.
SYSTEM_CPR_PERSON_ID = "urn:dk:cpr:personid"

# CPR register code lists: country code (record 010, e.g. 5100) and municipality code.
SYSTEM_CPR_LANDEKODE = "urn:dk:cpr:landekode"
SYSTEM_DK_MUNICIPALITY_CODES = "http://hl7.dk/fhir/core/CodeSystem/dk-core-MunicipalityCodes"
# HL7 v3 MaritalStatus, which dk-core-patient binds maritalStatus to.
SYSTEM_V3_MARITAL_STATUS = "http://terminology.hl7.org/CodeSystem/v3-MaritalStatus"
# dk-core codes for the two Danish marital statuses without an HL7 code (P, O).
SYSTEM_DK_MARITAL_STATUS = "http://hl7.dk/fhir/core/CodeSystem/dk-marital-status"

# Builder provenance: meta.source is the builder, one meta.tag per contributing source.
SYSTEM_SOURCE_TAG = "http://example.org/fhir/CodeSystem/kilde"
SOURCE_PATIENT_BUILDER = "http://example.org/fhir/source/patient-bygger"
SOURCE_LOCATION_BUILDER = "http://example.org/fhir/source/location-bygger"
# BOM_IFDB.ORGENHED.ORG_TYPE on the SOR-keyed Location's type.
SYSTEM_BOM_IFDB_ORG_TYPE = "urn:source:bom_ifdb:org_type"
