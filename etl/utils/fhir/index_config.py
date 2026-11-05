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

import re

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
    SYSTEM_IFDB_BED,
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
    SYSTEM_IFDB_SENGEENHED,
    SYSTEM_IFDB_SENGEENHED_KAPACITET,
    SYSTEM_IFDB_STAFF,
    SYSTEM_SILKEBORGDATA_TJENESTENUMMER,
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
    ("Location", SYSTEM_IFDB_SENGEENHED),
    ("Location", SYSTEM_IFDB_BED),
    ("CarePlan", SYSTEM_CAREPLAN_ALLE_MARKOERER),
    ("Condition", SYSTEM_IFDB_DIAGNOSE),
    ("Condition", SYSTEM_IFDB_PAS_DIAGNOSE),
    ("Condition", SYSTEM_IFDB_AKTIVITETSRESULTAT_CONDITION),
    ("Condition", SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION),
    ("Condition", SYSTEM_IFDB_RIS_YDELSE_DIAGNOSE),
    ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT),
    ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI),
    ("Observation", SYSTEM_IFDB_SENGEENHED_KAPACITET),
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
    # Staff are usually also patients, so the same CPR carries both a Patient
    # and a Practitioner. The index is per (resource_type, system), so the two
    # do not collide — test_practitioner_transforms pins that.
    ("Practitioner", SYSTEM_CPR),
    # EPJI accounts. The index compares case-sensitively, so the two case
    # variants of one staff code that EPJI holds as separate accounts stay apart.
    ("Practitioner", SYSTEM_IFDB_STAFF),
    ("PractitionerRole", SYSTEM_SILKEBORGDATA_TJENESTENUMMER),
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

_UNIQUE_PAIRS = set(IDENTIFIER_UNIQUE_INDEX_SPECS)

# -----------------------------------------------------------------------------
# Eksplicitte indeksnavne, hvor det genererede navn ikke er inden for graensen
# -----------------------------------------------------------------------------
# Postgres afkorter identifikatorer til 63 tegn UDEN at fejle. Det genererede
# navn "idx_practitionerrole_urn_source_silkeborgdata_tjenestenummer_uniq" er
# 65 tegn og blev gemt som "..._un": kendetegnet "_uniq" blev klippet af.
# To ting brister af det:
#   1. Oprydningen af INVALID indekser slaar op paa pg_class.relname med det
#      FULDE navn og ramte derfor aldrig raekken. Et INVALID indeks fra en
#      strandet CONCURRENTLY-bygning blev aldrig ryddet op.
#   2. CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS no-oppede derefter mod
#      det afkortede navn, og assetet rapporterede "Created UNIQUE index" paa
#      hver eneste foelgende koersel, mens indekset haandhaevede ingenting.
#
# Rettelsen er et kortere navn, ikke en afkortning: navnet skal vaere det
# samme, foer og efter Postgres har set det. "urn_source_" baerer ingen
# information, som ikke allerede staar i resten af navnet.
#
# Kun PractitionerRole er daekket her med vilje. De tre oevrige specs over
# graensen (Condition/aktivitetsresultat_xml_condition 67, QuestionnaireResponse
# 65, DocumentReference 64) er IFDB-ressourcer og hoerer til deres egen sag: et
# nyt navn paa et udrullet indeks koster en genopbygning, og de har levende
# data. PractitionerRole har ikke.
IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES: dict[tuple[str, str], str] = {
    ("PractitionerRole", SYSTEM_SILKEBORGDATA_TJENESTENUMMER): (
        "idx_practitionerrole_silkeborgdata_tjenestenummer_uniq"
    ),
}

# Et override, der selv er for langt, eller som peger paa et par uden for
# specs, ville vaere praecis den lydloese fejl, det er skrevet for at fjerne.
_SAFE_INDEX_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*$")
for _pair, _navn in IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES.items():
    if _pair not in _UNIQUE_PAIRS:
        raise ValueError(
            f"{_pair} is in IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES but not in "
            "IDENTIFIER_UNIQUE_INDEX_SPECS — the name would never be used"
        )
    if len(_navn) > 63:
        raise ValueError(
            f"Index name {_navn!r} is {len(_navn)} characters; PostgreSQL "
            "truncates identifiers at 63, which is what the override exists "
            "to avoid"
        )
    if not _SAFE_INDEX_NAME_RE.match(_navn):
        raise ValueError(f"Index name {_navn!r} is not a safe unquoted SQL identifier")

# =============================================================================
# Praefiks-indekser til oprydning af XML-boern
# =============================================================================
# Boern af en XML-forael der baerer identifier-vaerdien "{foraelder}{infix}{n}".
# Oprydningen skal finde alle levende boern for en given foraelder, altsaa et
# praefiks-opslag. Det UNIKKE indeks ovenfor bruger standard-opklassen og kan
# derfor kun slaa op paa hele vaerdien; et praefiks-interval kraever
# text_pattern_ops, som sammenligner byte for byte og dermed goer intervallet
# [praefiks, praefiks-med-sidste-tegn-plus-en) eksakt uanset databasens collation.
#
# Uden dette indeks maa opslaget scanne hele familiens bestand for hver portion
# af foraeldre. Maalt paa produktionslignende data: ti minutter for 29 boern.
#
# DDL:
#
#   CREATE INDEX idx_<table>_<system>_prefix
#       ON <table> (extract_identifier_value(resource, '<system>') text_pattern_ops)
#       WHERE resource->'identifier' @> '[{"system":"<system>"}]'::jsonb
#         AND extract_identifier_value(resource, '<system>') IS NOT NULL
IDENTIFIER_PREFIX_INDEX_SPECS: list[tuple[str, str]] = [
    ("Procedure", SYSTEM_IFDB_AKTIVITETSRESULTAT_PROCEDURE),
    ("Procedure", SYSTEM_IFDB_RIS_YDELSE),
    ("Observation", SYSTEM_IFDB_AKTIVITETSRESULTAT_SFI),
    ("Condition", SYSTEM_IFDB_AKTIVITETSRESULTAT_XML_CONDITION),
]

# Hvert praefiks-par skal ogsaa have det unikke indeks: den partielle
# WHERE-klausul er den samme, og opslaget forudsaetter begge.
for _rt, _sys in IDENTIFIER_PREFIX_INDEX_SPECS:
    if not _SAFE_IDENTIFIER_SYSTEM_RE.match(_sys):
        raise ValueError(
            f"Identifier system {_sys!r} in IDENTIFIER_PREFIX_INDEX_SPECS "
            "contains characters unsafe for SQL-literal interpolation"
        )
    if (_rt, _sys) not in _UNIQUE_PAIRS:
        raise ValueError(
            f"({_rt}, {_sys}) is in IDENTIFIER_PREFIX_INDEX_SPECS but not in "
            "IDENTIFIER_UNIQUE_INDEX_SPECS — the prefix index reuses the same "
            "partial predicate and would never be created"
        )

IDENTIFIER_PREFIX_INDEX_SET: frozenset[tuple[str, str]] = frozenset(IDENTIFIER_PREFIX_INDEX_SPECS)


# Membership test set used by the identifier-lookup resolver.
IDENTIFIER_UNIQUE_INDEX_SET: frozenset[tuple[str, str]] = frozenset(IDENTIFIER_UNIQUE_INDEX_SPECS)


# =============================================================================
# Alias GIN indexes -- every identifier value on the resource, not only the first
# =============================================================================
# The UNIQUE expression index sees only the first identifier per system, so alias
# lookups (core.fhir.alias_lookup) probe the values through this index and
# recheck the system on the candidate rows. Indexing values only keeps the shared
# system key out of the index; jsonb_path_ops and fastupdate = off keep a probe to
# a few pages. Named ``_identifier_alias`` so Phase 0 does not drop it.
ALIAS_GIN_EXPRESSION = "jsonb_path_query_array(resource->'identifier', '$[*].value')"

# DDL:
#   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_<table>_identifier_alias
#       ON <table> USING gin
#       ((jsonb_path_query_array(resource->'identifier', '$[*].value')) jsonb_path_ops)
#       WITH (fastupdate = off)
IDENTIFIER_ALIAS_GIN_RESOURCES: list[str] = ["Patient"]

ALIAS_GIN_OPCLASS = "jsonb_path_ops"
ALIAS_GIN_OPTIONS = "fastupdate = off"


def alias_gin_index_name(table_name: str) -> str:
    """Index name for the alias GIN index on a raw resource table."""
    return f"idx_{table_name}_identifier_alias"


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
    "Practitioner",
    "PractitionerRole",
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
    "Practitioner",
    "PractitionerRole",
    "Questionnaire",
    "QuestionnaireResponse",
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
