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
SYSTEM_IFDB_STAFF = "urn:source:ifdb:staff"  # Practitioner staff ID
SYSTEM_IFDB_LOCATION_ID = "urn:source:ifdb:location_id"  # LOCATION.OBJECT_ID

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
SYSTEM_PATIENT_RIGHTS_TYPE = "http://regionsyddanmark.dk/fhir/CodeSystem/patient-rights-type"

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

# =============================================================================
# FHIR Code Systems for Danish healthcare concepts
# =============================================================================
SYSTEM_PAKKETYPEMODEL = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/pakketypemodel"
SYSTEM_PAKKETYPE = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/pakketype"

# SOR entity type classification (EntityTypeName from SOR registry)
SYSTEM_SOR_ENTITY_TYPE = "http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/sor-entity-type"


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
