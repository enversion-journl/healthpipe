# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Central configuration for FHIR Postgres indexes.

This module is the single source of truth for which (resource_type, identifier_system)
pairs get UNIQUE/GIN/id-btree indexes, which raw resource tables get the
(ts) INCLUDE (cts) btree used by the SQL on FHIR extract path, which tables
get a (txid) btree to speed up the MAX(txid) lookup in batch_upsert_resources,
and which indexes Aidbox's FAR terminology schema needs for content-complete
CodeSystem operations.

All index families are created by the single `fhir_ifdb_database_indexes` asset.

Consumers:
- `infrastructure.assets.fhir_ifdb_infrastructure` reads these to create indexes.
- `core.fhir.identifier_lookup` reads `IDENTIFIER_UNIQUE_INDEX_SET` to decide
  whether to use the indexed `extract_identifier_value + @>` query path.

URI constants themselves live in `utils.fhir.identifier_systems`.
"""

from utils.fhir.identifier_systems import (
    _SAFE_IDENTIFIER_SYSTEM_RE,
    SYSTEM_BOM_IFDB_AFTALE_OID,
    SYSTEM_CAREPLAN_ALLE_MARKOERER,
    SYSTEM_CPR,
    SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI,
    SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION,
    SYSTEM_IFDB_CARE_DISCHARGE_REPORT,
    SYSTEM_IFDB_DIAGNOSE,
    SYSTEM_IFDB_EPIKRISE,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_HENVISNING_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PAS_DIAGNOSE,
    SYSTEM_IFDB_PATIENT_ID,
    SYSTEM_IFDB_QUESTIONNAIRE_MODEL,
    SYSTEM_IFDB_REHABILITATION_PLAN,
    SYSTEM_IFDB_RIS_YDELSE,
    SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE,
    SYSTEM_SOR,
)

# =============================================================================
# UNIQUE + GIN identifier indexes
# =============================================================================
# (resource_type, identifier_system) pairs that get:
#   * a partial GIN index on resource->'identifier' for the system, and
#   * a UNIQUE partial expression index on extract_identifier_value(resource, system)
#
# DDL (UNIQUE variant):
#
#   CREATE UNIQUE INDEX idx_<table>_<system>_uniq
#       ON <table> (extract_identifier_value(resource, '<system>'))
#       WHERE resource->'identifier' @> '[{"system":"<system>"}]'::jsonb
#         AND extract_identifier_value(resource, '<system>') IS NOT NULL
#
# The id-btree index set (Phase 2 in fhir_ifdb_database_indexes) is derived
# from the resource types appearing in this list.
#
# Adding a pair here without re-materializing `fhir_ifdb_database_indexes`
# will cause the resolver to issue queries that depend on an index that
# does not exist yet.
IDENTIFIER_UNIQUE_INDEX_SPECS: list[tuple[str, str]] = [
    ("Patient", SYSTEM_CPR),
    ("Patient", SYSTEM_IFDB_PATIENT_ID),
    ("EpisodeOfCare", SYSTEM_IFDB_EPISODE_OF_CARE_ID),
    ("Encounter", SYSTEM_IFDB_OBJECT_ID),
    ("Location", SYSTEM_SOR),
    ("CarePlan", SYSTEM_CAREPLAN_ALLE_MARKOERER),
    ("Condition", SYSTEM_IFDB_DIAGNOSE),
    ("Condition", SYSTEM_IFDB_PAS_DIAGNOSE),
    ("Condition", SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION),
    ("Condition", SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION),
    ("Condition", SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE),
    ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT),
    ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI),
    ("Procedure", SYSTEM_IFDB_ADMINISTRATIVE_PROCEDURE),
    ("Procedure", SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE),
    ("Procedure", SYSTEM_IFDB_RIS_YDELSE),
    ("ServiceRequest", SYSTEM_IFDB_HENVISNING_ID),
    ("ServiceRequest", SYSTEM_IFDB_RIS_YDELSE),
    ("ServiceRequest", SYSTEM_IFDB_REHABILITATION_PLAN),
    ("Questionnaire", SYSTEM_IFDB_QUESTIONNAIRE_MODEL),
    ("QuestionnaireResponse", SYSTEM_IFDB_AKTIVITETSRESULTAT),
    ("Organization", SYSTEM_SOR),
    ("Appointment", SYSTEM_BOM_IFDB_AFTALE_OID),
    ("DocumentReference", SYSTEM_IFDB_EPIKRISE),
    ("DocumentReference", SYSTEM_IFDB_CARE_DISCHARGE_REPORT),
]

# Validate at import time that the systems are safe for SQL-literal
# interpolation. Both the index DDL and the indexed lookup query embed
# the system as a SQL literal because expression indexes only match
# verbatim expressions — a bound parameter is opaque to the planner.
for _rt, _sys in IDENTIFIER_UNIQUE_INDEX_SPECS:
    if not _SAFE_IDENTIFIER_SYSTEM_RE.match(_sys):
        raise ValueError(
            f"Identifier system {_sys!r} in IDENTIFIER_UNIQUE_INDEX_SPECS "
            "contains characters unsafe for SQL-literal interpolation"
        )

# Membership test set used by the identifier-lookup resolver.
IDENTIFIER_UNIQUE_INDEX_SET: frozenset[tuple[str, str]] = frozenset(IDENTIFIER_UNIQUE_INDEX_SPECS)


# =============================================================================
# txid btree indexes (for batch_upsert_resources MAX(txid) lookup)
# =============================================================================
# Every batch_upsert_resources / sharded upsert call starts with
# ``SELECT COALESCE(MAX(txid), 0) + 1 FROM <table>`` to reserve a txid range.
# Without a btree on txid this is a full sequential scan of multi-GB tables
# (e.g. 46 s on careplan at 10 GB). A btree turns it into an O(1) "Index Only
# Scan Backward" reading a single leaf page.
#
# DDL:
#   CREATE INDEX IF NOT EXISTS idx_<table>_txid ON <table> (txid)
TXID_BTREE_RESOURCES: list[str] = [
    "Patient",
    "Encounter",
    "Observation",
    "EpisodeOfCare",
    "Location",
    "Organization",
    "CarePlan",
    "Procedure",
    "Condition",
    "ServiceRequest",
    "Appointment",
    "Questionnaire",
    "QuestionnaireResponse",
    "DocumentReference",
]


# =============================================================================
# SQL on FHIR (ts) INCLUDE (cts) btree indexes
# =============================================================================
# Raw resource tables read by the SQL on FHIR extract path. The query in
# extract_and_merge_sof_view() filters by r.ts and projects r.ts/r.cts as
# last_updated/created_at, so each table needs a (ts) INCLUDE (cts) btree
# to avoid a full sequential scan of the (often multi-GB) resource table.
SOF_TS_RESOURCES: list[str] = [
    "Patient",
    "Encounter",
    "Observation",
    "EpisodeOfCare",
    "Location",
    "Organization",
    "CarePlan",
    "Procedure",
    "Condition",
    "ServiceRequest",
    "Appointment",
    "DocumentReference",
]


# =============================================================================
# FAR terminology indexes (Aidbox hybrid terminology engine)
# =============================================================================
# Aidbox's hybrid terminology engine stores the concepts of content-complete
# CodeSystems normalized in far.codesystemconcept and reconstructs the resource
# (on update-PUT, GET and DELETE) via the recursive far.build_concept_tree()
# plpgsql function. That function probes `EXISTS (SELECT 1 FROM
# far.codesystemconcept WHERE parent = c.id)` per concept node; Aidbox 2508
# ships the table with indexes on (id) and (codesystem) only, so every probe is
# a full table scan and resource updates degrade as
# O(concepts_in_resource × total_rows_in_table). Measured on a 9,149-concept
# CodeSystem: update-PUT 147 s without these indexes, 6.5 s with them.
#
# The far schema is Aidbox-internal: it only exists when
# BOX_FHIR_TERMINOLOGY_ENGINE=hybrid, and an Aidbox upgrade may recreate it,
# so the asset (re)applies these idempotently on every materialization.
#
# DDL:
#   CREATE INDEX IF NOT EXISTS <index_name> ON far.codesystemconcept (<columns>)
FAR_CODESYSTEMCONCEPT_INDEXES: list[tuple[str, str]] = [
    # Serves the per-node EXISTS(child) probe in far.build_concept_tree()
    ("codesystemconcept_parent_idx", "parent"),
    # Serves the per-level child listing (WHERE codesystem = $1 AND parent = $2)
    ("codesystemconcept_cs_parent_idx", "codesystem, parent"),
]
