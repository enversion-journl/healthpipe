# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Archive MERGE assets for IFDB resources.

MERGEs ext.* → arc.* using the shared execute_archive_merge() function
driven by ArchiveMergeConfig. Checkpoint is tracked via DfmManager.

The archive checkpoint is set to the ceiling (from dfm_ifdb_flow_start)
that the extract used as its upper bound, NOT to MAX(delta_column) — so the
next delta extract starts exactly where this one stopped. A delta config's
flow_start_key is its extract's used_ceiling_key, the row where the extract
recorded the ceiling it read; a flow start in a later run cannot move it.

Encounter: delta (upsert on OBJECT_ID)
Location:  full-load (upsert on OBJECT_ID+PERIOD_START, soft-delete detection)
Patient:   delta (upsert on PATIENT_ID)
EpisodeOfCare: delta (upsert on OBJECT_ID)
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from DSI.oracle.ifdb_extract import (
    ABSENCE_COLUMNS,
    ADM_PROCEDURE_SUPPL_CODES_COLUMNS,
    ADMINISTRATIVE_PROCEDURE_COLUMNS,
    ADMINISTRATIVE_PROCEDURE_EXTRACT_CONFIG,
    AKTIVITETSRESULTAT_COLUMNS,
    AKTIVITETSRESULTAT_EXTRACT_CONFIG,
    AKTIVITETSUDFOERELSE_COLUMNS,
    AKTIVITETSUDFOERELSE_EXTRACT_CONFIG,
    AKTIVITETSUDFOERELSE_SPECIALE_COLUMNS,
    AKTIVITETSUDFOERELSE_SPECIALE_EXTRACT_CONFIG,
    BOM_IFDB_AFTALE_COLUMNS,
    BOM_IFDB_AFTALERESSOURCEDELTAGER_COLUMNS,
    BOM_IFDB_ORGENHED_SOR_COLUMNS,
    BOM_IFDB_PATIENTINFO_COLUMNS,
    CARE_DISCHARGE_REPORT_COLUMNS,
    CARE_DISCHARGE_REPORT_EXTRACT_CONFIG,
    CARE_DISCHARGE_REPORT_RECEIVER_COLUMNS,
    DIAGNOSE_COLUMNS,
    DIAGNOSE_EXTRACT_CONFIG,
    DIAGNOSE_TILLAEGSKODE_COLUMNS,
    ENCOUNTER_BED_SPACE_HISTORY_COLUMNS,
    ENCOUNTER_BED_SPACE_HISTORY_EXTRACT_CONFIG,
    ENCOUNTER_COLUMNS,
    ENCOUNTER_EXTRACT_CONFIG,
    EOC_COLUMNS,
    EOC_EXTRACT_CONFIG,
    EPIKRISE_COLUMNS,
    EPIKRISE_EXTRACT_CONFIG,
    EPIKRISE_MODTAGER_COLUMNS,
    EPIKRISE_MODTAGER_EXTRACT_CONFIG,
    EPIKRISE_TEKST_COLUMNS,
    EPIKRISE_TEKST_EXTRACT_CONFIG,
    EVENTMARKER_COLUMNS,
    EVENTMARKER_EXTRACT_CONFIG,
    HEALTHISSUE_COLUMNS,
    HEALTHISSUE_EXTRACT_CONFIG,
    HENVISNING_COLUMNS,
    HENVISNING_EXTRACT_CONFIG,
    HENVISNING_HISTORIK_COLUMNS,
    HENVISNING_HISTORIK_EXTRACT_CONFIG,
    IFDB_EPJ_CEILING,
    LOCATION_COLUMNS,
    LOCATION_EXTRACT_CONFIG,
    PAS_DIAGNOSE_COLUMNS,
    PAS_DIAGNOSE_ENCOUNTER_REF_EXTRACT_CONFIG,
    PAS_DIAGNOSE_EXTRACT_CONFIG,
    PAS_DIAGNOSE_HENVISNING_REF_EXTRACT_CONFIG,
    PAS_DIAGNOSE_TILLAEGSKODE_COLUMNS,
    PAS_DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG,
    PATIENT_COLUMNS,
    PATIENT_EXTRACT_CONFIG,
    REFERRALINFORMATION_COLUMNS,
    REFERRALINFORMATION_EXTRACT_CONFIG,
    REHABILITATION_PLAN_COLUMNS,
    REHABILITATION_PLAN_EXTRACT_CONFIG,
    REHABILITATION_PLAN_RECEIVER_COLUMNS,
    RIS_YDELSE_COLUMNS,
    RIS_YDELSE_EXTRACT_CONFIG,
    SENGEENHED_COLUMNS,
    SENGEENHED_DISPONIBLE_COLUMNS,
    SENGEENHED_EXTRACT_CONFIG,
    SENGEENHED_NORMEREDE_COLUMNS,
)

# ---------------------------------------------------------------------------
# Encounter archive config
# ---------------------------------------------------------------------------

_ENCOUNTER_MERGE_KEY = ("OBJECT_ID",)
_ENCOUNTER_NON_KEY = tuple(c for c in ENCOUNTER_COLUMNS if c != "OBJECT_ID")

ENCOUNTER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_encounter",
    target_schema="arc",
    target_table="ifdb_encounter",
    merge_keys=_ENCOUNTER_MERGE_KEY,
    non_key_columns=_ENCOUNTER_NON_KEY,
    flow_start_key=ENCOUNTER_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# ---------------------------------------------------------------------------
# Location archive config
# ---------------------------------------------------------------------------

_LOCATION_MERGE_KEYS = ("OBJECT_ID", "PERIOD_START")
_LOCATION_NON_KEY = tuple(c for c in LOCATION_COLUMNS if c not in _LOCATION_MERGE_KEYS)

LOCATION_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_location",
    target_schema="arc",
    target_table="ifdb_location",
    merge_keys=_LOCATION_MERGE_KEYS,
    non_key_columns=_LOCATION_NON_KEY,
    flow_start_key=LOCATION_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # ext holds only the locations whose encounter moved inside the delta
    # window, so NOT MATCHED BY SOURCE would soft-delete nearly the whole
    # archive on every run. A location row deleted upstream (IFDB.LOCATION has
    # no delete flag to read) is instead retired by the operator-triggered
    # dqa_arc_ifdb_vanished_cleanup; source_archive_reconciliation counts the backlog
    # weekly as vanished_not_marked_deleted.
    detect_deletes=False,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=False,
)

# ---------------------------------------------------------------------------
# HEALTHISSUE archive config (delta; soft-delete via DELETED)
# ---------------------------------------------------------------------------

_HEALTHISSUE_MERGE_KEY = ("OBJECT_ID",)
_HEALTHISSUE_NON_KEY = tuple(c for c in HEALTHISSUE_COLUMNS if c != "OBJECT_ID")

HEALTHISSUE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_healthissue",
    target_schema="arc",
    target_table="ifdb_healthissue",
    merge_keys=_HEALTHISSUE_MERGE_KEY,
    non_key_columns=_HEALTHISSUE_NON_KEY,
    flow_start_key=HEALTHISSUE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
    # Lookup-only archive (HEALTHISSUE has no own FHIR resource): the EpisodeOfCare
    # enrichment + quality-check read it via build_fhir_archive_query(...,
    # include_deleted=True), so the cold-load dw_is_deleted state is not consumed
    # downstream. OBJECT_ID is the source PK (0 duplicates in PROD, 2026-07-10), so
    # the direct bulk insert cannot violate the arc PK — safe to fast-load an empty
    # archive.
    enable_bulk_insert_on_first_load=True,
)

# ---------------------------------------------------------------------------
# Rehabilitationplan (ServiceRequest) archive configs
# Parent: delta, upsert on OBJECT_ID, soft-delete via DELETED (1:1 CDR parent).
# Receiver: full-load, upsert on (OBJECT_ID, modtager_noegle),
#   soft-delete via NOT MATCHED BY SOURCE (CDRR pattern).
# ---------------------------------------------------------------------------

_REHABILITATION_PLAN_MERGE_KEY = ("OBJECT_ID",)
_REHABILITATION_PLAN_NON_KEY = tuple(c for c in REHABILITATION_PLAN_COLUMNS if c != "OBJECT_ID")

REHABILITATION_PLAN_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_rehabilitation_plan",
    target_schema="arc",
    target_table="ifdb_rehabilitation_plan",
    merge_keys=_REHABILITATION_PLAN_MERGE_KEY,
    non_key_columns=_REHABILITATION_PLAN_NON_KEY,
    flow_start_key=REHABILITATION_PLAN_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# modtager_noegle is NOT NULL in ext, so the MERGE ON clause never compares NULLs.
_REHABILITATION_PLAN_RECEIVER_MERGE_KEYS = ("OBJECT_ID", "modtager_noegle")
_REHABILITATION_PLAN_RECEIVER_NON_KEY = tuple(
    c
    for c in REHABILITATION_PLAN_RECEIVER_COLUMNS
    if c not in _REHABILITATION_PLAN_RECEIVER_MERGE_KEYS
)

REHABILITATION_PLAN_RECEIVER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_rehabilitation_plan_receiver",
    target_schema="arc",
    target_table="ifdb_rehabilitation_plan_receiver",
    merge_keys=_REHABILITATION_PLAN_RECEIVER_MERGE_KEYS,
    non_key_columns=_REHABILITATION_PLAN_RECEIVER_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
)


# ---------------------------------------------------------------------------
# Patient archive config
# ---------------------------------------------------------------------------

_PATIENT_MERGE_KEY = ("PATIENT_ID",)
_PATIENT_NON_KEY = tuple(c for c in PATIENT_COLUMNS if c != "PATIENT_ID")

PATIENT_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_patient",
    target_schema="arc",
    target_table="ifdb_patient",
    merge_keys=_PATIENT_MERGE_KEY,
    non_key_columns=_PATIENT_NON_KEY,
    flow_start_key=PATIENT_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
)

# ---------------------------------------------------------------------------
# EpisodeOfCare archive config
# ---------------------------------------------------------------------------

_EOC_MERGE_KEY = ("OBJECT_ID",)
_EOC_NON_KEY = tuple(c for c in EOC_COLUMNS if c != "OBJECT_ID")

EOC_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_episode_of_care",
    target_schema="arc",
    target_table="ifdb_episode_of_care",
    merge_keys=_EOC_MERGE_KEY,
    non_key_columns=_EOC_NON_KEY,
    flow_start_key=EOC_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# ---------------------------------------------------------------------------
# Epikrise archive config (delta; soft-delete via SLETTET)
# ---------------------------------------------------------------------------

_EPIKRISE_MERGE_KEY = ("OBJEKT_ID",)
_EPIKRISE_NON_KEY = tuple(c for c in EPIKRISE_COLUMNS if c != "OBJEKT_ID")

EPIKRISE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_epikrise",
    target_schema="arc",
    target_table="ifdb_epikrise",
    merge_keys=_EPIKRISE_MERGE_KEY,
    non_key_columns=_EPIKRISE_NON_KEY,
    flow_start_key=EPIKRISE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
    # No bulk-insert fast path: the fast load inserts ext rows raw and does not
    # compute dw_is_deleted from source_delete_column, so a cold load would leave
    # every soft-deleted epikrise at dw_is_deleted=0 forever (delta never
    # re-extracts unchanged rows). FHIR tombstoning depends on the soft-delete
    # state, so this parent stays on the always-correct MERGE.
)

# ---------------------------------------------------------------------------
# Epikrise modtager archive config (inner-join-delta child; no
# source-delete column — deletes not detectable through the parent join)
# ---------------------------------------------------------------------------

_EPIKRISE_MODTAGER_MERGE_KEY = ("OBJEKT_ID",)
_EPIKRISE_MODTAGER_NON_KEY = tuple(c for c in EPIKRISE_MODTAGER_COLUMNS if c != "OBJEKT_ID")

EPIKRISE_MODTAGER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_epikrise_modtager",
    target_schema="arc",
    target_table="ifdb_epikrise_modtager",
    merge_keys=_EPIKRISE_MODTAGER_MERGE_KEY,
    non_key_columns=_EPIKRISE_MODTAGER_NON_KEY,
    flow_start_key=EPIKRISE_MODTAGER_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    # Safe for fast load: no source_delete_column (nothing to soft-delete), and
    # OBJEKT_ID is the child PK in Oracle (unique in ext), so the direct insert
    # cannot violate the arc PK.
    enable_bulk_insert_on_first_load=True,
)

# ---------------------------------------------------------------------------
# Epikrise tekst archive config (inner-join-delta child; CLOB; archive only)
# ---------------------------------------------------------------------------

_EPIKRISE_TEKST_MERGE_KEY = ("OBJEKT_ID",)
_EPIKRISE_TEKST_NON_KEY = tuple(c for c in EPIKRISE_TEKST_COLUMNS if c != "OBJEKT_ID")

EPIKRISE_TEKST_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_epikrise_tekst",
    target_schema="arc",
    target_table="ifdb_epikrise_tekst",
    merge_keys=_EPIKRISE_TEKST_MERGE_KEY,
    non_key_columns=_EPIKRISE_TEKST_NON_KEY,
    flow_start_key=EPIKRISE_TEKST_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    # Safe for fast load: no source_delete_column, and OBJEKT_ID is the child PK
    # in Oracle (unique in ext), so the direct insert cannot violate the arc PK.
    enable_bulk_insert_on_first_load=True,
)

# ---------------------------------------------------------------------------
# Care discharge report archive config (delta; soft-delete via DELETED)
# ---------------------------------------------------------------------------

_CARE_DISCHARGE_REPORT_MERGE_KEY = ("OBJECT_ID",)
_CARE_DISCHARGE_REPORT_NON_KEY = tuple(c for c in CARE_DISCHARGE_REPORT_COLUMNS if c != "OBJECT_ID")

CARE_DISCHARGE_REPORT_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_care_discharge_report",
    target_schema="arc",
    target_table="ifdb_care_discharge_report",
    merge_keys=_CARE_DISCHARGE_REPORT_MERGE_KEY,
    non_key_columns=_CARE_DISCHARGE_REPORT_NON_KEY,
    flow_start_key=CARE_DISCHARGE_REPORT_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# ---------------------------------------------------------------------------
# Care discharge report receiver archive config (full-load; composite
# key; soft-delete via NOT MATCHED BY SOURCE — Location pattern)
# ---------------------------------------------------------------------------

_CARE_DISCHARGE_REPORT_RECEIVER_MERGE_KEYS = (
    "OBJECT_ID",
    "RECEIVER_AFDELING_KODE",
    "CCRECEIVER",
)
_CARE_DISCHARGE_REPORT_RECEIVER_NON_KEY = tuple(
    c
    for c in CARE_DISCHARGE_REPORT_RECEIVER_COLUMNS
    if c not in _CARE_DISCHARGE_REPORT_RECEIVER_MERGE_KEYS
)

CARE_DISCHARGE_REPORT_RECEIVER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_care_discharge_report_receiver",
    target_schema="arc",
    target_table="ifdb_care_discharge_report_receiver",
    merge_keys=_CARE_DISCHARGE_REPORT_RECEIVER_MERGE_KEYS,
    non_key_columns=_CARE_DISCHARGE_REPORT_RECEIVER_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
)


# ---------------------------------------------------------------------------
# Diagnose archive config (delta; soft-delete via SLETTET)
# ---------------------------------------------------------------------------

_DIAGNOSE_MERGE_KEY = ("OBJEKT_ID",)
_DIAGNOSE_NON_KEY = tuple(c for c in DIAGNOSE_COLUMNS if c != "OBJEKT_ID")

DIAGNOSE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_diagnose",
    target_schema="arc",
    target_table="ifdb_diagnose",
    merge_keys=_DIAGNOSE_MERGE_KEY,
    non_key_columns=_DIAGNOSE_NON_KEY,
    flow_start_key=DIAGNOSE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
    # No bulk-insert fast path: source_delete_column would leave soft-deleted
    # diagnoses at dw_is_deleted=0 forever on a cold load (see arc_epikrise_ifdb).
)

# ---------------------------------------------------------------------------
# Diagnose tillaegskode archive config (full-load; composite key;
# soft-delete via NOT MATCHED BY SOURCE — Location pattern)
# ---------------------------------------------------------------------------

_DIAGNOSE_TILLAEGSKODE_MERGE_KEYS = ("DIAGNOSE_ID", "TILLAEGSKODE_KODE")
_DIAGNOSE_TILLAEGSKODE_NON_KEY = tuple(
    c for c in DIAGNOSE_TILLAEGSKODE_COLUMNS if c not in _DIAGNOSE_TILLAEGSKODE_MERGE_KEYS
)

DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_diagnose_tillaegskode",
    target_schema="arc",
    target_table="ifdb_diagnose_tillaegskode",
    merge_keys=_DIAGNOSE_TILLAEGSKODE_MERGE_KEYS,
    non_key_columns=_DIAGNOSE_TILLAEGSKODE_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)

# ---------------------------------------------------------------------------
# PAS-diagnose archive config (delta; soft-delete via SLETTET)
# ---------------------------------------------------------------------------

_PAS_DIAGNOSE_MERGE_KEY = ("OBJEKT_ID",)
_PAS_DIAGNOSE_NON_KEY = tuple(c for c in PAS_DIAGNOSE_COLUMNS if c != "OBJEKT_ID")

PAS_DIAGNOSE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_pas_diagnose",
    target_schema="arc",
    target_table="ifdb_pas_diagnose",
    merge_keys=_PAS_DIAGNOSE_MERGE_KEY,
    non_key_columns=_PAS_DIAGNOSE_NON_KEY,
    flow_start_key=PAS_DIAGNOSE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
)

# ---------------------------------------------------------------------------
# PAS-diagnose ref/child archive configs (full-load link tables; every
# column is part of the merge key except tillaegskode's KODETEKST)
# ---------------------------------------------------------------------------

_PAS_DIAGNOSE_ENCOUNTER_REF_MERGE_KEYS = ("DIAGNOSE_ID", "ENCOUNTER_ID")

PAS_DIAGNOSE_ENCOUNTER_REF_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_pas_diagnose_encounter_ref",
    target_schema="arc",
    target_table="ifdb_pas_diagnose_encounter_ref",
    merge_keys=_PAS_DIAGNOSE_ENCOUNTER_REF_MERGE_KEYS,
    non_key_columns=(),
    flow_start_key=PAS_DIAGNOSE_ENCOUNTER_REF_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Delta-extract: NOT MATCHED BY SOURCE ville soft-delete alt uden for
    # vinduet. Ingen source_delete_column findes heller, så child-raekker
    # retireres aldrig — ryd op med full_load via period_orchestrator.
    detect_deletes=False,
    # ext holds a join-delta slice, not a full snapshot.
    upstream_is_full_load=False,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)

_PAS_DIAGNOSE_TILLAEGSKODE_MERGE_KEYS = ("PAS_DIAGNOSE_ID", "TILLAEGSKODE_KODE")
_PAS_DIAGNOSE_TILLAEGSKODE_NON_KEY = tuple(
    c for c in PAS_DIAGNOSE_TILLAEGSKODE_COLUMNS if c not in _PAS_DIAGNOSE_TILLAEGSKODE_MERGE_KEYS
)

PAS_DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_pas_diagnose_tillaegskode",
    target_schema="arc",
    target_table="ifdb_pas_diagnose_tillaegskode",
    merge_keys=_PAS_DIAGNOSE_TILLAEGSKODE_MERGE_KEYS,
    non_key_columns=_PAS_DIAGNOSE_TILLAEGSKODE_NON_KEY,
    flow_start_key=PAS_DIAGNOSE_TILLAEGSKODE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Delta-extract: NOT MATCHED BY SOURCE ville soft-delete alt uden for
    # vinduet. Ingen source_delete_column findes heller, så child-raekker
    # retireres aldrig — ryd op med full_load via period_orchestrator.
    detect_deletes=False,
    # ext holds a join-delta slice, not a full snapshot.
    upstream_is_full_load=False,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)

_PAS_DIAGNOSE_HENVISNING_REF_MERGE_KEYS = ("DIAGNOSE_ID", "HENVISNING_ID")

PAS_DIAGNOSE_HENVISNING_REF_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_pas_diagnose_henvisning_ref",
    target_schema="arc",
    target_table="ifdb_pas_diagnose_henvisning_ref",
    merge_keys=_PAS_DIAGNOSE_HENVISNING_REF_MERGE_KEYS,
    non_key_columns=(),
    flow_start_key=PAS_DIAGNOSE_HENVISNING_REF_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Fjernede koblinger forsvinder fra kildeviewet. En fuld genindlæsning
    # skal derfor markere dem slettet. Den fælles MERGE-motor slår automatisk
    # dette fra for delta/period/ids og ved et udtræk med nul rækker.
    detect_deletes=True,
    # ext holds a join-delta slice, not a full snapshot.
    upstream_is_full_load=False,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)


# ---------------------------------------------------------------------------
# Administrative procedure archive config (delta; soft-delete via DELETED)
# ---------------------------------------------------------------------------

_ADMINISTRATIVE_PROCEDURE_MERGE_KEY = ("OBJECT_ID",)
_ADMINISTRATIVE_PROCEDURE_NON_KEY = tuple(
    c for c in ADMINISTRATIVE_PROCEDURE_COLUMNS if c != "OBJECT_ID"
)

ADMINISTRATIVE_PROCEDURE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_administrative_procedure",
    target_schema="arc",
    target_table="ifdb_administrative_procedure",
    merge_keys=_ADMINISTRATIVE_PROCEDURE_MERGE_KEY,
    non_key_columns=_ADMINISTRATIVE_PROCEDURE_NON_KEY,
    flow_start_key=ADMINISTRATIVE_PROCEDURE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DELETED",
)

# ---------------------------------------------------------------------------
# Administrative procedure suppl codes archive config (full-load child;
# composite key on parent + IDX position)
# ---------------------------------------------------------------------------

_ADM_PROCEDURE_SUPPL_CODES_MERGE_KEYS = ("PARENT_OBJECT_ID", "IDX")
_ADM_PROCEDURE_SUPPL_CODES_NON_KEY = tuple(
    c for c in ADM_PROCEDURE_SUPPL_CODES_COLUMNS if c not in _ADM_PROCEDURE_SUPPL_CODES_MERGE_KEYS
)

ADM_PROCEDURE_SUPPL_CODES_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_adm_procedure_suppl_codes",
    target_schema="arc",
    target_table="ifdb_adm_procedure_suppl_codes",
    merge_keys=_ADM_PROCEDURE_SUPPL_CODES_MERGE_KEYS,
    non_key_columns=_ADM_PROCEDURE_SUPPL_CODES_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)

# ---------------------------------------------------------------------------
# RIS-ydelse archive config (delta; soft-delete via SLETTET; XML CLOB verbatim)
# ---------------------------------------------------------------------------

_RIS_YDELSE_MERGE_KEY = ("OBJEKT_ID",)
_RIS_YDELSE_NON_KEY = tuple(c for c in RIS_YDELSE_COLUMNS if c != "OBJEKT_ID")

RIS_YDELSE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_ris_ydelse",
    target_schema="arc",
    target_table="ifdb_ris_ydelse",
    merge_keys=_RIS_YDELSE_MERGE_KEY,
    non_key_columns=_RIS_YDELSE_NON_KEY,
    flow_start_key=RIS_YDELSE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
    # No bulk-insert fast path: source_delete_column would leave soft-deleted
    # RIS-ydelser at dw_is_deleted=0 forever on a cold load (see arc_epikrise_ifdb).
)

# ---------------------------------------------------------------------------
# Aktivitetsresultat archive config (delta; soft-delete via SLETTET; XML CLOB)
# ---------------------------------------------------------------------------

_AKTIVITETSRESULTAT_MERGE_KEY = ("OBJEKT_ID",)
_AKTIVITETSRESULTAT_NON_KEY = tuple(c for c in AKTIVITETSRESULTAT_COLUMNS if c != "OBJEKT_ID")

AKTIVITETSRESULTAT_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_aktivitetsresultat",
    target_schema="arc",
    target_table="ifdb_aktivitetsresultat",
    merge_keys=_AKTIVITETSRESULTAT_MERGE_KEY,
    non_key_columns=_AKTIVITETSRESULTAT_NON_KEY,
    flow_start_key=AKTIVITETSRESULTAT_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
)

# ---------------------------------------------------------------------------
# Aktivitetsudførelse archive config (delta; soft-delete via SLETTET)
# ---------------------------------------------------------------------------

_AKTIVITETSUDFOERELSE_MERGE_KEY = ("OBJEKT_ID",)
_AKTIVITETSUDFOERELSE_NON_KEY = tuple(c for c in AKTIVITETSUDFOERELSE_COLUMNS if c != "OBJEKT_ID")

AKTIVITETSUDFOERELSE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_aktivitetsudfoerelse",
    target_schema="arc",
    target_table="ifdb_aktivitetsudfoerelse",
    merge_keys=_AKTIVITETSUDFOERELSE_MERGE_KEY,
    non_key_columns=_AKTIVITETSUDFOERELSE_NON_KEY,
    flow_start_key=AKTIVITETSUDFOERELSE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
)

# ---------------------------------------------------------------------------
# Aktivitetsudførelse-speciale archive config (full-load child; soft-delete)
# ---------------------------------------------------------------------------

_AKTIVITETSUDFOERELSE_SPECIALE_MERGE_KEYS = ("AKTIVITETSUDFOERELSE_ID", "SPECIALE_KODE")
_AKTIVITETSUDFOERELSE_SPECIALE_NON_KEY = tuple(
    c
    for c in AKTIVITETSUDFOERELSE_SPECIALE_COLUMNS
    if c not in _AKTIVITETSUDFOERELSE_SPECIALE_MERGE_KEYS
)

AKTIVITETSUDFOERELSE_SPECIALE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_aktivitetsudfoerelse_speciale",
    target_schema="arc",
    target_table="ifdb_aktivitetsudfoerelse_speciale",
    merge_keys=_AKTIVITETSUDFOERELSE_SPECIALE_MERGE_KEYS,
    non_key_columns=_AKTIVITETSUDFOERELSE_SPECIALE_NON_KEY,
    flow_start_key=AKTIVITETSUDFOERELSE_SPECIALE_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Delta-extract: NOT MATCHED BY SOURCE ville soft-delete alt uden for
    # vinduet. Ingen source_delete_column findes heller, så child-raekker
    # retireres aldrig — ryd op med full_load via period_orchestrator.
    detect_deletes=False,
    # ext holds a join-delta slice, not a full snapshot.
    upstream_is_full_load=False,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever. IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # modbeviste paastanden i praksis (PK-violation paa foerste MERGE), saa
    # kilden deduplikeres nu foer baade MERGE og fast path.
    dedup_source_on_keys=True,
)


# ---------------------------------------------------------------------------
# ServiceRequest archive configs (henvisning side)
#
# Henvisning: delta, soft-delete via SLETTET (the CDR-parent pattern).
# The two enrichment tables carry no delete flag and are read per batch by key.
# Neither drives delete detection: an extract that is not a complete snapshot
# cannot drive NOT MATCHED BY SOURCE, and both are delta-scoped — historik
# through the parent's OPDATERET_TID, referralinformation through its own
# REFERRAL_DATE. That matches the epikrise child tables.
# ---------------------------------------------------------------------------

_HENVISNING_MERGE_KEY = ("OBJEKT_ID",)
_HENVISNING_NON_KEY = tuple(c for c in HENVISNING_COLUMNS if c != "OBJEKT_ID")
HENVISNING_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_henvisning",
    target_schema="arc",
    target_table="ifdb_henvisning",
    merge_keys=_HENVISNING_MERGE_KEY,
    non_key_columns=_HENVISNING_NON_KEY,
    flow_start_key=HENVISNING_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="SLETTET",
    # No bulk-insert fast path: source_delete_column would leave soft-deleted
    # henvisninger at dw_is_deleted=0 forever on a cold load.
)

_HENVISNING_HISTORIK_MERGE_KEYS = ("HENVISNING_ID", "ORDNING")
_HENVISNING_HISTORIK_NON_KEY = tuple(
    c for c in HENVISNING_HISTORIK_COLUMNS if c not in _HENVISNING_HISTORIK_MERGE_KEYS
)
HENVISNING_HISTORIK_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_henvisning_historik",
    target_schema="arc",
    target_table="ifdb_henvisning_historik",
    merge_keys=_HENVISNING_HISTORIK_MERGE_KEYS,
    non_key_columns=_HENVISNING_HISTORIK_NON_KEY,
    flow_start_key=HENVISNING_HISTORIK_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Delta extract → NOT MATCHED BY SOURCE would soft-delete every row outside
    # the window. No source delete flag exists either, so history rows are
    # never retired (same as arc_epikrise_modtager_ifdb).
    detect_deletes=False,
    # Safe here: no source_delete_column, and the merge key is the source's
    # own primary key, so ext rows are unique on it.
    enable_bulk_insert_on_first_load=True,
)

_REFERRALINFORMATION_MERGE_KEY = ("OBJECT_ID",)
_REFERRALINFORMATION_NON_KEY = tuple(c for c in REFERRALINFORMATION_COLUMNS if c != "OBJECT_ID")
REFERRALINFORMATION_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_referralinformation",
    target_schema="arc",
    target_table="ifdb_referralinformation",
    merge_keys=_REFERRALINFORMATION_MERGE_KEY,
    non_key_columns=_REFERRALINFORMATION_NON_KEY,
    flow_start_key=REFERRALINFORMATION_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    enable_bulk_insert_on_first_load=True,
)


# ---------------------------------------------------------------------------
# Eventmarker archive config (delta; soft-delete via DELETED)
# ---------------------------------------------------------------------------

_EVENTMARKER_MERGE_KEY = ("OBJECT_ID",)
_EVENTMARKER_NON_KEY = tuple(c for c in EVENTMARKER_COLUMNS if c != "OBJECT_ID")

EVENTMARKER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_eventmarker",
    target_schema="arc",
    target_table="ifdb_eventmarker",
    merge_keys=_EVENTMARKER_MERGE_KEY,
    non_key_columns=_EVENTMARKER_NON_KEY,
    flow_start_key=EVENTMARKER_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # ext holds only the markers inside the delta window, so NOT MATCHED BY
    # SOURCE would retire the whole archive on every run. IFDB soft-deletes
    # instead (DELETED flips to 1 and IFDB_MODIFICATION_DATETIME advances), so
    # the delta carries the retirement and the CarePlan transform reads DELETED.
    detect_deletes=False,
    source_delete_column="DELETED",
)


# ---------------------------------------------------------------------------
# Archive Assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_encounter_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_encounter"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "encounter",
    },
    description=(
        "MERGE upsert ext.ifdb_encounter → arc.ifdb_encounter; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_encounter_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Encounter."""
    result = execute_archive_merge(context, mssql_target_staging, ENCOUNTER_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_encounter "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_encounter",
            target_table="arc.ifdb_encounter",
        )
    )


@dg.asset(
    name="arc_location_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_location"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "location",
    },
    description=(
        "MERGE upsert ext.ifdb_location → arc.ifdb_location (join-delta via "
        "ENCOUNTER; no delete detection — ext is not a full snapshot)."
    ),
)
def arc_location_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Location (delta window, no delete detection)."""
    result = execute_archive_merge(context, mssql_target_staging, LOCATION_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_location "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_location",
            target_table="arc.ifdb_location",
        )
    )


@dg.asset(
    name="arc_patient_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_patient"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "patient",
    },
    description=(
        "MERGE upsert ext.ifdb_patient → arc.ifdb_patient; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_patient_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Patient."""
    result = execute_archive_merge(context, mssql_target_staging, PATIENT_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_patient "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_patient",
            target_table="arc.ifdb_patient",
        )
    )


@dg.asset(
    name="arc_episode_of_care_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_episode_of_care"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "episode_of_care",
    },
    description=(
        "MERGE upsert ext.ifdb_episode_of_care → arc.ifdb_episode_of_care; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_episode_of_care_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB EpisodeOfCare."""
    result = execute_archive_merge(context, mssql_target_staging, EOC_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_episode_of_care "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_episode_of_care",
            target_table="arc.ifdb_episode_of_care",
        )
    )


# ---------------------------------------------------------------------------
# Epikrise + Care discharge report archive assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_epikrise_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_epikrise"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "epikrise",
    },
    description=(
        "MERGE upsert ext.ifdb_epikrise → arc.ifdb_epikrise; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_epikrise_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Epikrise."""
    result = execute_archive_merge(context, mssql_target_staging, EPIKRISE_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_epikrise "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_epikrise",
            target_table="arc.ifdb_epikrise",
        )
    )


@dg.asset(
    name="arc_epikrise_modtager_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_epikrise_modtager"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "epikrise_modtager",
    },
    description=(
        "MERGE upsert ext.ifdb_epikrise_modtager → arc.ifdb_epikrise_modtager "
        "(inner-join-delta child of Epikrise)."
    ),
)
def arc_epikrise_modtager_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Epikrise modtagere."""
    result = execute_archive_merge(
        context, mssql_target_staging, EPIKRISE_MODTAGER_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_epikrise_modtager "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_epikrise_modtager",
            target_table="arc.ifdb_epikrise_modtager",
        )
    )


@dg.asset(
    name="arc_epikrise_tekst_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_epikrise_tekst"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "epikrise_tekst",
    },
    description=(
        "MERGE upsert ext.ifdb_epikrise_tekst → arc.ifdb_epikrise_tekst "
        "(CLOB; archive only; inner-join-delta child of Epikrise)."
    ),
)
def arc_epikrise_tekst_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Epikrise tekst (archive only)."""
    result = execute_archive_merge(
        context, mssql_target_staging, EPIKRISE_TEKST_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_epikrise_tekst "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_epikrise_tekst",
            target_table="arc.ifdb_epikrise_tekst",
        )
    )


@dg.asset(
    name="arc_care_discharge_report_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_care_discharge_report"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "care_discharge_report",
    },
    description=(
        "MERGE upsert ext.ifdb_care_discharge_report → "
        "arc.ifdb_care_discharge_report; advance checkpoint to the flow-start timestamp."
    ),
)
def arc_care_discharge_report_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Udskrivningsrapport."""
    result = execute_archive_merge(
        context, mssql_target_staging, CARE_DISCHARGE_REPORT_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_care_discharge_report "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_care_discharge_report",
            target_table="arc.ifdb_care_discharge_report",
        )
    )


@dg.asset(
    name="arc_care_discharge_report_receiver_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_care_discharge_report_receiver"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "care_discharge_report_receiver",
    },
    description=(
        "MERGE upsert ext.ifdb_care_discharge_report_receiver → "
        "arc.ifdb_care_discharge_report_receiver (full-load, soft-delete on "
        "NOT MATCHED BY SOURCE)."
    ),
)
def arc_care_discharge_report_receiver_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Udskrivningsrapport-modtagere with soft-delete detection."""
    result = execute_archive_merge(
        context, mssql_target_staging, CARE_DISCHARGE_REPORT_RECEIVER_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_care_discharge_report_receiver "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_care_discharge_report_receiver",
            target_table="arc.ifdb_care_discharge_report_receiver",
        )
    )


# ---------------------------------------------------------------------------
# Condition (Diagnose + PAS-diagnose) archive assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_diagnose_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_diagnose"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "diagnose",
    },
    description=(
        "MERGE upsert ext.ifdb_diagnose → arc.ifdb_diagnose; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_diagnose_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Diagnose."""
    result = execute_archive_merge(context, mssql_target_staging, DIAGNOSE_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_diagnose "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_diagnose",
            target_table="arc.ifdb_diagnose",
        )
    )


@dg.asset(
    name="arc_diagnose_tillaegskode_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_diagnose_tillaegskode"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "diagnose_tillaegskode",
    },
    description=(
        "MERGE upsert ext.ifdb_diagnose_tillaegskode → arc.ifdb_diagnose_tillaegskode "
        "(full-load, soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_diagnose_tillaegskode_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Diagnose-tillægskoder with soft-delete detection."""
    result = execute_archive_merge(
        context, mssql_target_staging, DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_diagnose_tillaegskode "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_diagnose_tillaegskode",
            target_table="arc.ifdb_diagnose_tillaegskode",
        )
    )


@dg.asset(
    name="arc_pas_diagnose_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_pas_diagnose"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "pas_diagnose",
    },
    description=(
        "MERGE upsert ext.ifdb_pas_diagnose → arc.ifdb_pas_diagnose; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_pas_diagnose_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB PAS-diagnose."""
    result = execute_archive_merge(
        context, mssql_target_staging, PAS_DIAGNOSE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_pas_diagnose "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_pas_diagnose",
            target_table="arc.ifdb_pas_diagnose",
        )
    )


@dg.asset(
    name="arc_pas_diagnose_encounter_ref_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_pas_diagnose_encounter_ref"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "pas_diagnose_encounter_ref",
    },
    description=(
        "MERGE upsert ext.ifdb_pas_diagnose_encounter_ref → "
        "arc.ifdb_pas_diagnose_encounter_ref (full-load link table, "
        "soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_pas_diagnose_encounter_ref_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB PAS-diagnose encounter-referencer."""
    result = execute_archive_merge(
        context, mssql_target_staging, PAS_DIAGNOSE_ENCOUNTER_REF_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_pas_diagnose_encounter_ref "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_pas_diagnose_encounter_ref",
            target_table="arc.ifdb_pas_diagnose_encounter_ref",
        )
    )


@dg.asset(
    name="arc_pas_diagnose_tillaegskode_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_pas_diagnose_tillaegskode"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "pas_diagnose_tillaegskode",
    },
    description=(
        "MERGE upsert ext.ifdb_pas_diagnose_tillaegskode → "
        "arc.ifdb_pas_diagnose_tillaegskode (join-delta slice via PAS_DIAGNOSE; "
        "no delete detection — removals are cleaned up out-of-band)."
    ),
)
def arc_pas_diagnose_tillaegskode_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB PAS-diagnose-tillægskoder (delta slice, no delete detection)."""
    result = execute_archive_merge(
        context, mssql_target_staging, PAS_DIAGNOSE_TILLAEGSKODE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_pas_diagnose_tillaegskode "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_pas_diagnose_tillaegskode",
            target_table="arc.ifdb_pas_diagnose_tillaegskode",
        )
    )


@dg.asset(
    name="arc_pas_diagnose_henvisning_ref_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_pas_diagnose_henvisning_ref"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "pas_diagnose_henvisning_ref",
    },
    description=(
        "MERGE upsert ext.ifdb_pas_diagnose_henvisning_ref → "
        "arc.ifdb_pas_diagnose_henvisning_ref (full-load link table, "
        "soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_pas_diagnose_henvisning_ref_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB PAS-diagnose henvisnings-referencer."""
    result = execute_archive_merge(
        context, mssql_target_staging, PAS_DIAGNOSE_HENVISNING_REF_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_pas_diagnose_henvisning_ref "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_pas_diagnose_henvisning_ref",
            target_table="arc.ifdb_pas_diagnose_henvisning_ref",
        )
    )


# ---------------------------------------------------------------------------
# Procedure (AdminProc + RIS-ydelse) archive assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_administrative_procedure_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_administrative_procedure"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "administrative_procedure",
    },
    description=(
        "MERGE upsert ext.ifdb_administrative_procedure → "
        "arc.ifdb_administrative_procedure; advance checkpoint to the flow-start timestamp."
    ),
)
def arc_administrative_procedure_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Administrative procedure."""
    result = execute_archive_merge(
        context, mssql_target_staging, ADMINISTRATIVE_PROCEDURE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_administrative_procedure "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_administrative_procedure",
            target_table="arc.ifdb_administrative_procedure",
        )
    )


@dg.asset(
    name="arc_adm_procedure_suppl_codes_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_adm_procedure_suppl_codes"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "adm_procedure_suppl_codes",
    },
    description=(
        "MERGE upsert ext.ifdb_adm_procedure_suppl_codes → "
        "arc.ifdb_adm_procedure_suppl_codes (full-load, soft-delete on "
        "NOT MATCHED BY SOURCE)."
    ),
)
def arc_adm_procedure_suppl_codes_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Administrative procedure-tillægskoder."""
    result = execute_archive_merge(
        context, mssql_target_staging, ADM_PROCEDURE_SUPPL_CODES_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_adm_procedure_suppl_codes "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_adm_procedure_suppl_codes",
            target_table="arc.ifdb_adm_procedure_suppl_codes",
        )
    )


@dg.asset(
    name="arc_ris_ydelse_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_ris_ydelse"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "ris_ydelse",
    },
    description=(
        "MERGE upsert ext.ifdb_ris_ydelse → arc.ifdb_ris_ydelse "
        "(XML CLOB verbatim); advance checkpoint to the flow-start timestamp."
    ),
)
def arc_ris_ydelse_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB RIS-ydelse."""
    result = execute_archive_merge(context, mssql_target_staging, RIS_YDELSE_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_ris_ydelse "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_ris_ydelse",
            target_table="arc.ifdb_ris_ydelse",
        )
    )


@dg.asset(
    name="arc_aktivitetsresultat_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_aktivitetsresultat"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "aktivitetsresultat",
    },
    description=(
        "MERGE upsert ext.ifdb_aktivitetsresultat → arc.ifdb_aktivitetsresultat "
        "(XML CLOB verbatim); advance checkpoint to the flow-start timestamp."
    ),
)
def arc_aktivitetsresultat_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Aktivitetsresultat."""
    result = execute_archive_merge(
        context, mssql_target_staging, AKTIVITETSRESULTAT_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_aktivitetsresultat "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_aktivitetsresultat",
            target_table="arc.ifdb_aktivitetsresultat",
        )
    )


@dg.asset(
    name="arc_aktivitetsudfoerelse_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_aktivitetsudfoerelse"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "aktivitetsudfoerelse",
    },
    description=(
        "MERGE upsert ext.ifdb_aktivitetsudfoerelse → arc.ifdb_aktivitetsudfoerelse; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_aktivitetsudfoerelse_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Aktivitetsudførelse."""
    result = execute_archive_merge(
        context, mssql_target_staging, AKTIVITETSUDFOERELSE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_aktivitetsudfoerelse "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_aktivitetsudfoerelse",
            target_table="arc.ifdb_aktivitetsudfoerelse",
        )
    )


@dg.asset(
    name="arc_aktivitetsudfoerelse_speciale_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_aktivitetsudfoerelse_speciale"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "aktivitetsudfoerelse_speciale",
    },
    description=(
        "MERGE upsert ext.ifdb_aktivitetsudfoerelse_speciale → "
        "arc.ifdb_aktivitetsudfoerelse_speciale (join-delta slice via "
        "AKTIVITETSUDFOERELSE; no delete detection)."
    ),
)
def arc_aktivitetsudfoerelse_speciale_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Aktivitetsudførelse-speciale (delta slice, no delete detection)."""
    result = execute_archive_merge(
        context, mssql_target_staging, AKTIVITETSUDFOERELSE_SPECIALE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_aktivitetsudfoerelse_speciale "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_aktivitetsudfoerelse_speciale",
            target_table="arc.ifdb_aktivitetsudfoerelse_speciale",
        )
    )


@dg.asset(
    name="arc_henvisning_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_henvisning"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "henvisning",
    },
    description=(
        "MERGE upsert ext.ifdb_henvisning → arc.ifdb_henvisning; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_henvisning_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Henvisning."""
    result = execute_archive_merge(context, mssql_target_staging, HENVISNING_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_henvisning "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_henvisning",
            target_table="arc.ifdb_henvisning",
        )
    )


@dg.asset(
    name="arc_henvisning_historik_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_henvisning_historik"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "henvisning_historik",
    },
    description=(
        "MERGE upsert ext.ifdb_henvisning_historik → arc.ifdb_henvisning_historik "
        "(status-history enrichment, keyed HENVISNING_ID + ORDNING); "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_henvisning_historik_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Henvisnings-historik."""
    result = execute_archive_merge(
        context, mssql_target_staging, HENVISNING_HISTORIK_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_henvisning_historik "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_henvisning_historik",
            target_table="arc.ifdb_henvisning_historik",
        )
    )


@dg.asset(
    name="arc_referralinformation_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_referralinformation"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "referralinformation",
    },
    description=(
        "MERGE upsert ext.ifdb_referralinformation → arc.ifdb_referralinformation "
        "(referral-origin enrichment, keyed on the episode-of-care id); "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_referralinformation_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Referral-information."""
    result = execute_archive_merge(
        context, mssql_target_staging, REFERRALINFORMATION_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_referralinformation "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_referralinformation",
            target_table="arc.ifdb_referralinformation",
        )
    )


@dg.asset(
    name="arc_healthissue_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_healthissue"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "healthissue",
    },
    description=(
        "MERGE upsert ext.ifdb_healthissue → arc.ifdb_healthissue "
        "(lookup archive for EpisodeOfCare healthissue_code enrichment); "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_healthissue_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB HEALTHISSUE (lookup archive)."""
    result = execute_archive_merge(
        context, mssql_target_staging, HEALTHISSUE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_healthissue "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_healthissue",
            target_table="arc.ifdb_healthissue",
        )
    )


@dg.asset(
    name="arc_rehabilitation_plan_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_rehabilitation_plan"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "rehabilitation_plan",
    },
    description=(
        "MERGE upsert ext.ifdb_rehabilitation_plan → arc.ifdb_rehabilitation_plan "
        "(delta; soft-delete via DELETED); advance checkpoint to the flow-start timestamp."
    ),
)
def arc_rehabilitation_plan_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Genoptræningsplan (G-GOP)."""
    result = execute_archive_merge(
        context, mssql_target_staging, REHABILITATION_PLAN_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_rehabilitation_plan "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_rehabilitation_plan",
            target_table="arc.ifdb_rehabilitation_plan",
        )
    )


@dg.asset(
    name="arc_rehabilitation_plan_receiver_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_rehabilitation_plan_receiver"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "rehabilitation_plan_receiver",
    },
    description=(
        "MERGE upsert ext.ifdb_rehabilitation_plan_receiver → "
        "arc.ifdb_rehabilitation_plan_receiver (full-load, keyed on OBJECT_ID + "
        "modtager_noegle, soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_rehabilitation_plan_receiver_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Genoptræningsplan-modtagere with soft-delete detection."""
    result = execute_archive_merge(
        context, mssql_target_staging, REHABILITATION_PLAN_RECEIVER_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_rehabilitation_plan_receiver "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_rehabilitation_plan_receiver",
            target_table="arc.ifdb_rehabilitation_plan_receiver",
        )
    )


# ---------------------------------------------------------------------------
# BOM_IFDB: Location_SOR (ORGENHED⋈SORENHED) archive config
# Full-load, detect_deletes, upstream_is_full_load
# ---------------------------------------------------------------------------

_BOM_IFDB_ORGENHED_SOR_MERGE_KEY = ("OID",)
_BOM_IFDB_ORGENHED_SOR_NON_KEY = tuple(c for c in BOM_IFDB_ORGENHED_SOR_COLUMNS if c != "OID")

BOM_IFDB_ORGENHED_SOR_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="bom_ifdb_orgenhed_sor",
    target_schema="arc",
    target_table="bom_ifdb_orgenhed_sor",
    merge_keys=_BOM_IFDB_ORGENHED_SOR_MERGE_KEY,
    non_key_columns=_BOM_IFDB_ORGENHED_SOR_NON_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    # Sikker fast path: ingen source_delete_column, og OID er unik i den
    # flattenede ORGENHED-x-SORENHED-projektion (1:1 pr. organisationsenhed).
    enable_bulk_insert_on_first_load=True,
)

# ---------------------------------------------------------------------------
# BOM_IFDB: Appointment (AFTALE) archive config
# Delta, soft-delete via DATOSLETTET (source_delete_column)
# ---------------------------------------------------------------------------

_BOM_IFDB_AFTALE_MERGE_KEY = ("OID",)
_BOM_IFDB_AFTALE_NON_KEY = tuple(c for c in BOM_IFDB_AFTALE_COLUMNS if c != "OID")

BOM_IFDB_AFTALE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="bom_ifdb_aftale",
    target_schema="arc",
    target_table="bom_ifdb_aftale",
    merge_keys=_BOM_IFDB_AFTALE_MERGE_KEY,
    non_key_columns=_BOM_IFDB_AFTALE_NON_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
    source_delete_column="DATOSLETTET",
)

# ---------------------------------------------------------------------------
# BOM_IFDB: AFTALERESSOURCEDELTAGER archive config
# Join-delta slice (windowed through AFTALE.LASTMODIFIED), so the MERGE can
# never see removals: participant links routinely disappear in the source
# (rebooking), and the transform reads the archive per AFTALEOID, so a stale
# row is re-attached on every future re-transform. Retirement therefore runs
# out-of-band: the table is a DQA arc-subject, so the weekly
# source_archive_reconciliation counts the backlog (vanished_not_marked_deleted) and
# the operator-triggered dqa_arc_ifdb_vanished_cleanup soft-deletes it.
# ---------------------------------------------------------------------------

_BOM_IFDB_AFTALERESSOURCEDELTAGER_MERGE_KEY = ("OID",)
_BOM_IFDB_AFTALERESSOURCEDELTAGER_NON_KEY = tuple(
    c for c in BOM_IFDB_AFTALERESSOURCEDELTAGER_COLUMNS if c != "OID"
)

BOM_IFDB_AFTALERESSOURCEDELTAGER_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="bom_ifdb_aftaleressourcedeltager",
    target_schema="arc",
    target_table="bom_ifdb_aftaleressourcedeltager",
    merge_keys=_BOM_IFDB_AFTALERESSOURCEDELTAGER_MERGE_KEY,
    non_key_columns=_BOM_IFDB_AFTALERESSOURCEDELTAGER_NON_KEY,
    strategy="upsert",
    update_only_on_change=True,
    # A delta slice must never drive NOT MATCHED BY SOURCE, and the source has
    # no delete flag. Removed participants are retired by
    # dqa_arc_ifdb_vanished_cleanup (see the block comment above).
    detect_deletes=False,
    enable_bulk_insert_on_first_load=True,
)

# ---------------------------------------------------------------------------
# BOM_IFDB: PATIENTINFO archive config
# Delta on LASTMODIFIED (decision 2026-08-11, data minimisation): no nightly
# full copy of the person register. Accepted trade-off: hard-deleted source
# rows linger in arc with dw_is_deleted=0 (the CPR fallback is key-based).
# ---------------------------------------------------------------------------

_BOM_IFDB_PATIENTINFO_MERGE_KEY = ("PATIENTINFOOID",)
_BOM_IFDB_PATIENTINFO_NON_KEY = tuple(
    c for c in BOM_IFDB_PATIENTINFO_COLUMNS if c != "PATIENTINFOOID"
)

BOM_IFDB_PATIENTINFO_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="bom_ifdb_patientinfo",
    target_schema="arc",
    target_table="bom_ifdb_patientinfo",
    merge_keys=_BOM_IFDB_PATIENTINFO_MERGE_KEY,
    non_key_columns=_BOM_IFDB_PATIENTINFO_NON_KEY,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=False,
)


# ---------------------------------------------------------------------------
# BOM_IFDB archive assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_bom_ifdb_orgenhed_sor",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_bom_ifdb_orgenhed_sor"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "bom_ifdb",
        "entity": "orgenhed_sor",
    },
    description=(
        "MERGE upsert ext.bom_ifdb_orgenhed_sor → arc.bom_ifdb_orgenhed_sor "
        "(full-load, soft-delete on NOT MATCHED BY SOURCE)."
    ),
)
def arc_bom_ifdb_orgenhed_sor(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for BOM_IFDB ORGENHED+SORENHED with soft-delete detection."""
    result = execute_archive_merge(
        context, mssql_target_staging, BOM_IFDB_ORGENHED_SOR_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.bom_ifdb_orgenhed_sor "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.bom_ifdb_orgenhed_sor",
            target_table="arc.bom_ifdb_orgenhed_sor",
        )
    )


@dg.asset(
    name="arc_bom_ifdb_aftale",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_bom_ifdb_aftale"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "bom_ifdb",
        "entity": "aftale",
    },
    description=(
        "MERGE upsert ext.bom_ifdb_aftale → arc.bom_ifdb_aftale "
        "(delta; soft-delete via DATOSLETTET)."
    ),
)
def arc_bom_ifdb_aftale(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for BOM_IFDB AFTALE."""
    result = execute_archive_merge(
        context, mssql_target_staging, BOM_IFDB_AFTALE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.bom_ifdb_aftale "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.bom_ifdb_aftale",
            target_table="arc.bom_ifdb_aftale",
        )
    )


@dg.asset(
    name="arc_bom_ifdb_aftaleressourcedeltager",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_bom_ifdb_aftaleressourcedeltager"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "bom_ifdb",
        "entity": "aftaleressourcedeltager",
    },
    description=(
        "MERGE upsert ext.bom_ifdb_aftaleressourcedeltager → "
        "arc.bom_ifdb_aftaleressourcedeltager "
        "(inner-join-delta child of AFTALE)."
    ),
)
def arc_bom_ifdb_aftaleressourcedeltager(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for BOM_IFDB AFTALERESSOURCEDELTAGER."""
    result = execute_archive_merge(
        context,
        mssql_target_staging,
        BOM_IFDB_AFTALERESSOURCEDELTAGER_ARCHIVE_CONFIG,
        config,
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.bom_ifdb_aftaleressourcedeltager "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.bom_ifdb_aftaleressourcedeltager",
            target_table="arc.bom_ifdb_aftaleressourcedeltager",
        )
    )


@dg.asset(
    name="arc_bom_ifdb_patientinfo",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_bom_ifdb_patientinfo"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "bom_ifdb",
        "entity": "patientinfo",
    },
    description=(
        "MERGE upsert ext.bom_ifdb_patientinfo → arc.bom_ifdb_patientinfo "
        "(delta on LASTMODIFIED; no delete detection — hard-deleted source "
        "rows linger with dw_is_deleted=0)."
    ),
)
def arc_bom_ifdb_patientinfo(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for BOM_IFDB PATIENTINFO (delta, no delete detection)."""
    result = execute_archive_merge(
        context, mssql_target_staging, BOM_IFDB_PATIENTINFO_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.bom_ifdb_patientinfo "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.bom_ifdb_patientinfo",
            target_table="arc.bom_ifdb_patientinfo",
        )
    )


@dg.asset(
    name="arc_eventmarker_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_eventmarker"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "eventmarker",
    },
    description=(
        "MERGE upsert ext.ifdb_eventmarker → arc.ifdb_eventmarker; "
        "advance checkpoint to the flow-start timestamp."
    ),
)
def arc_eventmarker_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext → arc for IFDB Eventmarker."""
    result = execute_archive_merge(
        context, mssql_target_staging, EVENTMARKER_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_eventmarker "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}); "
        f"checkpoint → {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_eventmarker",
            target_table="arc.ifdb_eventmarker",
        )
    )


# ---------------------------------------------------------------------------
# Asset lists
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Senge og kapacitet — arkiv-configs
#
# Merge-nøglerne er MÅLT mod den levende kilde, ikke antaget: alle fire
# PK-løse tabeller viste sig entydige på nøglerne nedenfor (0 dubletter), så
# dedup_source_on_keys er unødvendig. Rækketal og distinkte nøgler står ved
# hver tabel i ddl/sources/ifdb/ifdb_sengeenhed.sql.
# ---------------------------------------------------------------------------

_SENGEENHED_MERGE_KEYS = ("OBJEKT_ID",)
_SENGEENHED_NON_KEY = tuple(c for c in SENGEENHED_COLUMNS if c not in _SENGEENHED_MERGE_KEYS)

SENGEENHED_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_sengeenhed",
    target_schema="arc",
    target_table="ifdb_sengeenhed",
    merge_keys=_SENGEENHED_MERGE_KEYS,
    non_key_columns=_SENGEENHED_NON_KEY,
    flow_start_key=SENGEENHED_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # Kilden markerer selv sletning via SLETTET, saa NOT MATCHED BY SOURCE er
    # hverken noedvendig eller forsvarlig paa et delta-load.
    detect_deletes=False,
    source_delete_column="SLETTET",
)

_SENGEENHED_DISPONIBLE_MERGE_KEYS = ("SENGEENHED_ID", "FRA")
_SENGEENHED_DISPONIBLE_NON_KEY = tuple(
    c for c in SENGEENHED_DISPONIBLE_COLUMNS if c not in _SENGEENHED_DISPONIBLE_MERGE_KEYS
)

SENGEENHED_DISPONIBLE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_sengeenhed_disponible",
    target_schema="arc",
    target_table="ifdb_sengeenhed_disponible",
    merge_keys=_SENGEENHED_DISPONIBLE_MERGE_KEYS,
    non_key_columns=_SENGEENHED_DISPONIBLE_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    # Full snapshot: kilden har intet slette-signal, saa NOT MATCHED BY SOURCE
    # er den eneste vej til at pensionere en fjernet periode.
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever, og en maaling paa ét tidspunkt er
    # ingen garanti. Samme laerestreg som IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # (PK-violation paa foerste MERGE), saa kilden deduplikeres foer MERGE og
    # fast path. rows_dedup_dropped i asset-metadata melder, hvis grain'en er forkert.
    dedup_source_on_keys=True,
)

_SENGEENHED_NORMEREDE_MERGE_KEYS = ("SENGEENHED_ID", "FRA")
_SENGEENHED_NORMEREDE_NON_KEY = tuple(
    c for c in SENGEENHED_NORMEREDE_COLUMNS if c not in _SENGEENHED_NORMEREDE_MERGE_KEYS
)

SENGEENHED_NORMEREDE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_sengeenhed_normerede",
    target_schema="arc",
    target_table="ifdb_sengeenhed_normerede",
    merge_keys=_SENGEENHED_NORMEREDE_MERGE_KEYS,
    non_key_columns=_SENGEENHED_NORMEREDE_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever, og en maaling paa ét tidspunkt er
    # ingen garanti. Samme laerestreg som IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # (PK-violation paa foerste MERGE), saa kilden deduplikeres foer MERGE og
    # fast path. rows_dedup_dropped i asset-metadata melder, hvis grain'en er forkert.
    dedup_source_on_keys=True,
)

_ENCOUNTER_BED_SPACE_HISTORY_MERGE_KEYS = ("OBJECT_ID", "PERIOD_START_REGISTRATION")
_ENCOUNTER_BED_SPACE_HISTORY_NON_KEY = tuple(
    c
    for c in ENCOUNTER_BED_SPACE_HISTORY_COLUMNS
    if c not in _ENCOUNTER_BED_SPACE_HISTORY_MERGE_KEYS
)

ENCOUNTER_BED_SPACE_HISTORY_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_encounter_bed_space_history",
    target_schema="arc",
    target_table="ifdb_encounter_bed_space_history",
    merge_keys=_ENCOUNTER_BED_SPACE_HISTORY_MERGE_KEYS,
    non_key_columns=_ENCOUNTER_BED_SPACE_HISTORY_NON_KEY,
    flow_start_key=ENCOUNTER_BED_SPACE_HISTORY_EXTRACT_CONFIG.used_ceiling_key,
    strategy="upsert",
    update_only_on_change=True,
    # ext rummer kun de sengeraekker, hvis kontakt flyttede sig inde i
    # delta-vinduet, saa NOT MATCHED BY SOURCE ville soft-slette naesten hele
    # arkivet ved hver koersel. Samme afvejning som arc.ifdb_location: en raekke
    # slettet i kilden pensioneres i stedet af dqa_arc_ifdb_vanished_cleanup.
    detect_deletes=False,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=False,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever, og en maaling paa ét tidspunkt er
    # ingen garanti. Samme laerestreg som IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # (PK-violation paa foerste MERGE), saa kilden deduplikeres foer MERGE og
    # fast path. rows_dedup_dropped i asset-metadata melder, hvis grain'en er forkert.
    dedup_source_on_keys=True,
)

_ABSENCE_MERGE_KEYS = ("OBJECT_ID", "PERIOD_START")
_ABSENCE_NON_KEY = tuple(c for c in ABSENCE_COLUMNS if c not in _ABSENCE_MERGE_KEYS)

ABSENCE_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table="ifdb_absence",
    target_schema="arc",
    target_table="ifdb_absence",
    merge_keys=_ABSENCE_MERGE_KEYS,
    non_key_columns=_ABSENCE_NON_KEY,
    flow_start_key=IFDB_EPJ_CEILING,
    strategy="upsert",
    update_only_on_change=True,
    detect_deletes=True,
    soft_delete_column="dw_is_deleted",
    upstream_is_full_load=True,
    enable_bulk_insert_on_first_load=True,
    # Kilden har INGEN primaernoegle i Oracle — merge_keys er en grain vi
    # paastaar, ikke en som kilden haandhaever, og en maaling paa ét tidspunkt er
    # ingen garanti. Samme laerestreg som IFDB.AKTIVITETSUDFOERELSE_SPECIALE
    # (PK-violation paa foerste MERGE), saa kilden deduplikeres foer MERGE og
    # fast path. rows_dedup_dropped i asset-metadata melder, hvis grain'en er forkert.
    dedup_source_on_keys=True,
)


# ---------------------------------------------------------------------------
# Senge og kapacitet — arkiv-assets
# ---------------------------------------------------------------------------


@dg.asset(
    name="arc_sengeenhed_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_sengeenhed"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "sengeenhed",
    },
    description=(
        "MERGE upsert ext.ifdb_sengeenhed -> arc.ifdb_sengeenhed (delta on MODTIDSPUNKT; soft-delete propagated from SLETTET)."
    ),
)
def arc_sengeenhed_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext -> arc for IFDB Sengeenhed."""
    result = execute_archive_merge(context, mssql_target_staging, SENGEENHED_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_sengeenhed "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint -> {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_sengeenhed",
            target_table="arc.ifdb_sengeenhed",
        )
    )


@dg.asset(
    name="arc_sengeenhed_disponible_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_sengeenhed_disponible"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "sengeenhed_disponible",
    },
    description=(
        "MERGE upsert ext.ifdb_sengeenhed_disponible -> arc.ifdb_sengeenhed_disponible (full snapshot + delete detection)."
    ),
)
def arc_sengeenhed_disponible_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext -> arc for disponible senge (snapshot; deletes detected)."""
    result = execute_archive_merge(
        context, mssql_target_staging, SENGEENHED_DISPONIBLE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_sengeenhed_disponible "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint -> {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_sengeenhed_disponible",
            target_table="arc.ifdb_sengeenhed_disponible",
        )
    )


@dg.asset(
    name="arc_sengeenhed_normerede_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_sengeenhed_normerede"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "sengeenhed_normerede",
    },
    description=(
        "MERGE upsert ext.ifdb_sengeenhed_normerede -> arc.ifdb_sengeenhed_normerede (full snapshot + delete detection)."
    ),
)
def arc_sengeenhed_normerede_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext -> arc for normerede senge (snapshot; deletes detected)."""
    result = execute_archive_merge(
        context, mssql_target_staging, SENGEENHED_NORMEREDE_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_sengeenhed_normerede "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint -> {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_sengeenhed_normerede",
            target_table="arc.ifdb_sengeenhed_normerede",
        )
    )


@dg.asset(
    name="arc_encounter_bed_space_history_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_encounter_bed_space_history"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "encounter_bed_space_history",
    },
    description=(
        "MERGE upsert ext.ifdb_encounter_bed_space_history -> arc.ifdb_encounter_bed_space_history (join-delta via ENCOUNTER; no delete detection - ext is not a full snapshot)."
    ),
)
def arc_encounter_bed_space_history_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext -> arc for sengepladshistorik (delta window, no delete detection)."""
    result = execute_archive_merge(
        context, mssql_target_staging, ENCOUNTER_BED_SPACE_HISTORY_ARCHIVE_CONFIG, config
    )

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_encounter_bed_space_history "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint -> {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_encounter_bed_space_history",
            target_table="arc.ifdb_encounter_bed_space_history",
        )
    )


@dg.asset(
    name="arc_absence_ifdb",
    group_name="ifdb_archive",
    compute_kind="mssql",
    deps=[dg.AssetKey(["ext_ifdb_absence"])],
    tags={
        "domain": "ifdb",
        "layer": "archive",
        "source": "ifdb",
        "entity": "absence",
    },
    description=(
        "MERGE upsert ext.ifdb_absence -> arc.ifdb_absence (full snapshot + delete detection)."
    ),
)
def arc_absence_ifdb(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """UPSERT ext -> arc for fravaer (snapshot; deletes detected)."""
    result = execute_archive_merge(context, mssql_target_staging, ABSENCE_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd {result.row_count} rows into arc.ifdb_absence "
        f"(inserted: {result.rows_inserted}, updated: {result.rows_updated}, "
        f"soft_deleted: {result.rows_soft_deleted}); "
        f"mode = {config.mode}, checkpoint -> {result.new_checkpoint}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table="ext.ifdb_absence",
            target_table="arc.ifdb_absence",
        )
    )


ifdb_archive_assets = [
    arc_encounter_ifdb,
    arc_location_ifdb,
    arc_patient_ifdb,
    arc_episode_of_care_ifdb,
    arc_epikrise_ifdb,
    arc_epikrise_modtager_ifdb,
    arc_epikrise_tekst_ifdb,
    arc_care_discharge_report_ifdb,
    arc_care_discharge_report_receiver_ifdb,
    arc_healthissue_ifdb,
    arc_rehabilitation_plan_ifdb,
    arc_rehabilitation_plan_receiver_ifdb,
    arc_bom_ifdb_orgenhed_sor,
    arc_bom_ifdb_aftale,
    arc_bom_ifdb_aftaleressourcedeltager,
    arc_bom_ifdb_patientinfo,
    arc_diagnose_ifdb,
    arc_diagnose_tillaegskode_ifdb,
    arc_pas_diagnose_ifdb,
    arc_pas_diagnose_encounter_ref_ifdb,
    arc_pas_diagnose_tillaegskode_ifdb,
    arc_pas_diagnose_henvisning_ref_ifdb,
    arc_administrative_procedure_ifdb,
    arc_adm_procedure_suppl_codes_ifdb,
    arc_ris_ydelse_ifdb,
    # ServiceRequest (henvisning side)
    arc_henvisning_ifdb,
    arc_henvisning_historik_ifdb,
    arc_referralinformation_ifdb,
    arc_aktivitetsresultat_ifdb,
    arc_aktivitetsudfoerelse_ifdb,
    arc_aktivitetsudfoerelse_speciale_ifdb,
    # Senge og kapacitet
    arc_sengeenhed_ifdb,
    arc_sengeenhed_disponible_ifdb,
    arc_sengeenhed_normerede_ifdb,
    arc_encounter_bed_space_history_ifdb,
    arc_absence_ifdb,
    arc_eventmarker_ifdb,
]
