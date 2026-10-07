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
- Domain-specific identifiers (Kræftpakker, etc.)

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
# REHABILITATION_PLAN_RECEIVER has no own id; recipientKey = yder_nummer|afdeling_kode|transmission_time
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
# Common code system for patient rights type (category)
SYSTEM_PATIENT_RIGHTS_TYPE = "http://example.org/fhir/CodeSystem/patient-rights-type"

# Kræftpakker (Cancer Pathways)
SYSTEM_CAREPLAN_KRAEFTPAKKE = "urn:dk:sundhed:kraeftpakke:careplan"

# Udredningsret (Diagnostic Investigation Right)
SYSTEM_CAREPLAN_UDREDNINGSRET = "urn:dk:sundhed:udredningsret:careplan"

# Behandlingsret (Treatment Right) - for future use
SYSTEM_CAREPLAN_BEHANDLINGSRET = "urn:dk:sundhed:behandlingsret:careplan"

# Maksimale ventetider (Maximum Waiting Times) - for future use
SYSTEM_CAREPLAN_MAKSIMALE_VENTETIDER = "urn:dk:sundhed:maksimale-ventetider:careplan"

# Alle markører (All Markers)
SYSTEM_CAREPLAN_ALLE_MARKOERER = "urn:dk:sundhed:alle-markoerer:careplan"

# Helbredsforløb (Health Journey) - derived identifier for grouping
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
# Safe-character regex for SQL-literal interpolation
# =============================================================================
# Both the FHIR index DDL and the indexed lookup query embed identifier systems
# as SQL literals (expression indexes only match verbatim expressions — a bound
# parameter is opaque to the planner). Callers that interpolate a system into
# SQL must validate it against this regex first.
_SAFE_IDENTIFIER_SYSTEM_RE = _re.compile(r"^[A-Za-z0-9:/_.\-]+$")

# Index specs (UNIQUE/GIN/SoF) have moved to `utils.fhir.index_config` to keep
# this module focused on URI constants. Import from there.
