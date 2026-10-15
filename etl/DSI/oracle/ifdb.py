# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB source system table configs.

This module no longer defines extraction assets — every IFDB entity is extracted
by the DFM pipeline (DSI/oracle/ifdb_extract.py). The TableExtractionConfig
definitions remain here because the dependency resolver, the deleted-resource
quality checks, and their tests use them for on-demand Oracle lookups.
"""

from DSI.connectors.oracle.models import TableExtractionConfig

# ---------------------------------------------------------------------------
# Patient
# ---------------------------------------------------------------------------

PATIENT_CONFIG = TableExtractionConfig(
    table="IFDB.PATIENT_OPLYSNINGER_COLUMNA",
    columns=[
        "PATIENT_ID",
        "CPR",
        "FORNAVNE",
        "EFTERNAVN",
        "VEJ",
        "POSTNR",
        "STEDNAVN",
        "KOMMUNEKODE",
        "KOEN_KODE",
        "KOEN_KODETEKST",
        "FOEDSELSDATO",
        "LAEGENAVN",
        "LAEGENUMMER",
        "PERSON_STATUSKODE",
        "PERSON_STATUSKODETEKST",
        "HAR_ADRESSEBESKYTTELSE",
        "OENSKER_DISKRETION",
        "BEMAERKNINGER",
        "PERSON_DISTRIKTKODE",
        "PERSON_DISTRIKTKODETEKST",
        "PERSON_SOCIALDISTRIKTKODE",
        "PERSON_SOCIALDISTRIKTKODETEKST",
        "OPDATERET_TID",
        "SLETTET",
        "MODTIDSPUNKT",
    ],
    partition_column="MODTIDSPUNKT",
    description="Patient demographic data from IFDB EPJ system.",
)

# ---------------------------------------------------------------------------
# Encounter
# ---------------------------------------------------------------------------

ENCOUNTER_CONFIG = TableExtractionConfig(
    table="IFDB.ENCOUNTER",
    columns=[
        "OBJECT_ID",
        "PATIENT_ID",
        "EPISODE_OF_CARE_ID",
        "STATUS_CODE",
        "STATUS_TEXT",
        "CLASS_CODE",
        "CLASS_TEXT",
        "PRIORITY_CODE",
        "PRIORITY_TEXT",
        "CLINICAL_SPECIALITY_CODE",
        "CLINICAL_SPECIALITY_TEXT",
        "PERIOD_START",
        "PERIOD_END",
        "SERVICE_PROVIDER_CODE",
        "SERVICE_PROVIDER_NAME",
        "SERVICE_PROVIDER_ORGCODE",
        "SERVICE_PROVIDER_SORCODE",
        "WARD_CODE",
        "WARD_NAME",
        "WARD_ORGCODE",
        "WARD_SORCODE",
        "BED",
        "ROOM",
        "TEAM",
        "EXPECTED_DISCHARGE",
        "TREATMENT_START",
        "REFERRAL_ID",
        "MUNICIPALITY_CARE",
        "BROUGHT_IN_BY",
        "DELETED",
        "EPJI_CREATED_DATETIME",
        "IFDB_MODIFICATION_DATETIME",
    ],
    partition_column="IFDB_MODIFICATION_DATETIME",
    description="Encounter data from IFDB EPJ system.",
)

# ---------------------------------------------------------------------------
# Episode of Care
# ---------------------------------------------------------------------------

EPISODE_OF_CARE_CONFIG = TableExtractionConfig(
    table="IFDB.EPISODE_OF_CARE",
    columns=[
        "OBJECT_ID",
        "PATIENT_ID",
        "STATUS",
        "PERIOD_START",
        "PERIOD_END",
        "MANAGING_ORGANIZATION_CODE",
        "MANAGING_ORGANIZATION_NAME",
        "MANAGING_ORGANIZATION_SORCODE",
        "CARE_MANAGER_ID",
        "CARE_MANAGER_NAME",
        "PREVIOUS_EOC_ID",
        "PREVIOUS_EOC_REF_TYPE_CODE",
        "HEALTHISSUE_ID",
        "DISCHARGE_DISPOSITION_CODE",
        "DELETED",
        "IFDB_MODIFICATION_DATETIME",
    ],
    partition_column="IFDB_MODIFICATION_DATETIME",
    description="Episode of Care (sygedomsforløb) data from IFDB.",
)

# ---------------------------------------------------------------------------
# Location (custom DISTINCT query — SOR code only, no name)
# ---------------------------------------------------------------------------

LOCATION_CONFIG = TableExtractionConfig(
    table="IFDB.LOCATION",
    columns=[
        "OBJECT_ID",
        "LOCATION_CODE",
        "LOCATION_SORCODE",
    ],
    partition_column=None,
    description="Location master data from IFDB (static, no partition column).",
)
