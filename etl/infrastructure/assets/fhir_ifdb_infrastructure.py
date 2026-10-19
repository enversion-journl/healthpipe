# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
FHIR IFDB Database Indexing Assets.

This module provides assets for creating optimized database indexes to improve
ID resolution performance during ETL operations.

The indexes are created once and reused across all partition types (daily, monthly, full load).
"""

import json
from typing import Any

import dagster as dg
from core.resources import FHIRDatabaseResource
from DSA.FHIR.utils.sql_on_fhir.sof_utils import check_sof_required_indexes
from utils.fhir.identifier_index_names import sanitize_system_for_identifier
from utils.fhir.index_config import (
    ALIAS_GIN_EXPRESSION,
    ALIAS_GIN_OPCLASS,
    ALIAS_GIN_OPTIONS,
    FAR_CODESYSTEMCONCEPT_INDEXES,
    IDENTIFIER_ALIAS_GIN_RESOURCES,
    IDENTIFIER_PREFIX_INDEX_SPECS,
    IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES,
    IDENTIFIER_UNIQUE_INDEX_SPECS,
    SOF_TS_RESOURCES,
    TXID_BTREE_RESOURCES,
    alias_gin_index_name,
)

# Safe-character validation for identifier system constants lives in
# utils.fhir.index_config and runs at import time for every entry
# in IDENTIFIER_UNIQUE_INDEX_SPECS — no need to duplicate it here.


_EXTRACT_IDENTIFIER_VALUE_DDL = """
CREATE OR REPLACE FUNCTION extract_identifier_value(
    resource jsonb, identifier_system text
) RETURNS text
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
    SELECT elem->>'value'
    FROM jsonb_array_elements(resource->'identifier') AS elem
    WHERE elem->>'system' = identifier_system
    LIMIT 1
$$;
"""


def _ensure_identifier_extract_function(cur: Any) -> None:
    # IMMUTABLE so PostgreSQL accepts it in index expressions; the underlying
    # jsonb_array_elements is STABLE, but the function is deterministic for
    # a given (resource, system) input which is all that matters for index
    # correctness.
    cur.execute(_EXTRACT_IDENTIFIER_VALUE_DDL)


def _drop_invalid_index_if_exists(cur: Any, index_name: str, logger: Any) -> None:
    """Drop an INVALID index left behind by a failed CREATE INDEX CONCURRENTLY."""
    cur.execute(
        "SELECT 1 FROM pg_index i "
        "JOIN pg_class c ON c.oid = i.indexrelid "
        "WHERE c.relname = %s AND NOT i.indisvalid",
        (index_name,),
    )
    if cur.fetchone():
        logger.warning(f"[INDEXING] Dropping INVALID index {index_name} before re-creation")
        cur.execute(f"DROP INDEX IF EXISTS {index_name}")


ALIAS_REFERENCE_TABLE = "pg_temp.alias_index_reference"
ALIAS_REFERENCE_INDEX = "alias_index_reference_idx"

_INDEX_FACTS_SQL = (
    "SELECT pg_get_expr(i.indexprs, i.indrelid), "
    "ARRAY(SELECT o.opcname FROM unnest(i.indclass::oid[]) AS k(oid) "
    "JOIN pg_opclass o ON o.oid = k.oid), "
    "COALESCE(c.reloptions, ARRAY[]::text[]) "
    "FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
    "WHERE i.indexrelid = to_regclass(%s)"
)


def _alias_index_is_current(cur: Any, index_name: str) -> bool:
    """Does the alias index have the configured expression, opclass and options?

    PostgreSQL stores the expression in its own normalised form, so the facts are
    compared with an index built from the same DDL on an empty temporary table.
    """
    cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {ALIAS_REFERENCE_TABLE} (resource jsonb)")
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS {ALIAS_REFERENCE_INDEX} ON {ALIAS_REFERENCE_TABLE} "
        f"USING gin (({ALIAS_GIN_EXPRESSION}) {ALIAS_GIN_OPCLASS}) WITH ({ALIAS_GIN_OPTIONS})"
    )
    cur.execute(_INDEX_FACTS_SQL, (f"pg_temp.{ALIAS_REFERENCE_INDEX}",))
    reference = cur.fetchone()
    cur.execute(_INDEX_FACTS_SQL, (index_name,))
    existing = cur.fetchone()
    return existing is None or tuple(existing) == tuple(reference)


def _unique_index_name(resource_type: str, identifier_system: str) -> str:
    """Navnet paa det unikke identifier-indeks — aldrig laengere end 63 tegn.

    Postgres afkorter identifikatorer til 63 tegn uden at fejle, og et afkortet
    navn goer to ting lydloest forkerte: ``_drop_invalid_index_if_exists``
    slaar op paa det fulde navn og rammer aldrig raekken, og
    ``CREATE ... IF NOT EXISTS`` no-opper derefter mod det gemte navn, saa et
    mislykket byg rapporteres som succes paa hver eneste foelgende koersel.

    Derfor afkortes her ikke: par, hvis genererede navn ikke er inden for
    graensen, faar et eksplicit, kortere navn i
    ``IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES``. Laengden haandhaeves ved
    import af ``utils.fhir.index_config``.
    """
    override = IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES.get((resource_type, identifier_system))
    if override:
        return override
    table_name = resource_type.lower()
    system_safe = sanitize_system_for_identifier(identifier_system)
    return f"idx_{table_name}_{system_safe}_uniq"


def _legacy_truncated_unique_index_name(resource_type: str, identifier_system: str) -> str | None:
    """Det navn, et override har afloest, som Postgres faktisk gemte det.

    Er indekset allerede bygget under det afkortede navn (dev har det fra
    2026-09-22), skal den gamle raekke vaek, naar den nye er paa plads —
    ellers staar to unikke indekser paa samme udtryk og koster dobbelt
    skrive-I/O paa hver upsert for evigt. ``None``, naar der ikke er noget
    gammelt navn, eller naar det gamle navn er det nye.
    """
    if (resource_type, identifier_system) not in IDENTIFIER_UNIQUE_INDEX_NAME_OVERRIDES:
        return None
    table_name = resource_type.lower()
    system_safe = sanitize_system_for_identifier(identifier_system)
    gammelt = f"idx_{table_name}_{system_safe}_uniq"[:63]
    if gammelt == _unique_index_name(resource_type, identifier_system):
        return None
    return gammelt


def _create_unique_identifier_index(
    cur: Any, resource_type: str, identifier_system: str, logger: Any
) -> str:
    table_name = resource_type.lower()
    index_name = _unique_index_name(resource_type, identifier_system)
    system_filter = json.dumps([{"system": identifier_system}])

    _drop_invalid_index_if_exists(cur, index_name, logger)

    cur.execute(
        f"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
        f"ON {table_name} ("
        f"    extract_identifier_value(resource, '{identifier_system}')"
        f") "
        f"WHERE resource->'identifier' @> '{system_filter}'::jsonb"
        f"  AND extract_identifier_value(resource, '{identifier_system}') "
        f"IS NOT NULL"
    )
    return index_name


def _prefix_index_name(table_name: str, system_safe: str) -> str:
    """Navn til præfiks-indekset, der overlever 63-tegns-grænsen.

    Postgres afkorter identifikatorer til 63 tegn. For lange systemnavne
    (fx aktivitetsresultat_xml_condition) blev "..._prefix" afkortet til
    PRÆCIS samme navn som det unikke indeks — og CREATE INDEX IF NOT EXISTS
    no-oppede lydløst, så indekset aldrig blev oprettet. Det kostede en
    fuld indeksscanning pr. opslag i oprydningen (målt: 33 ms mod 0,05 ms).

    Kendetegnet ("pfx") lægges derfor FØRST, når navnet ellers ville blive
    afkortet, så det aldrig kan klippes væk. Eksisterende korte navne
    bevares uændret, så allerede oprettede indekser genfindes.
    """
    name = f"idx_{table_name}_{system_safe}_prefix"
    if len(name) <= 63:
        return name
    return f"idx_{table_name}_pfx_{system_safe}"[:63]


def _create_prefix_identifier_index(
    cur: Any, resource_type: str, identifier_system: str, logger: Any
) -> str:
    """Praefiks-indeks til oprydning af XML-boern.

    Boernenes identifier-vaerdi er "{foraelder}{infix}{n}", saa oprydningen
    skal finde alt med et givet praefiks. Det unikke indeks bruger standard-
    opklassen og kan kun slaa op paa hele vaerdien; ``text_pattern_ops``
    sammenligner byte for byte, saa intervallet fra praefikset til praefikset
    med sidste tegn plus en er eksakt uanset databasens collation.

    Samme partielle praedikat som det unikke indeks, saa planlaeggeren kan
    matche det fra samme forespoergsel.
    """
    table_name = resource_type.lower()
    system_safe = sanitize_system_for_identifier(identifier_system)
    index_name = _prefix_index_name(table_name, system_safe)
    system_filter = json.dumps([{"system": identifier_system}])

    _drop_invalid_index_if_exists(cur, index_name, logger)

    cur.execute(
        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
        f"ON {table_name} ("
        f"    extract_identifier_value(resource, '{identifier_system}') text_pattern_ops"
        f") "
        f"WHERE resource->'identifier' @> '{system_filter}'::jsonb"
        f"  AND extract_identifier_value(resource, '{identifier_system}') "
        f"IS NOT NULL"
    )
    return index_name


@dg.asset(
    name="fhir_ifdb_database_indexes",
    group_name="fhir_infrastructure",
    description=(
        "Create optimized indexes for FHIR IFDB resources: UNIQUE expression indexes "
        "on identifier for idempotent upserts and fast targeted lookups, btree indexes "
        "on txid to speed up MAX(txid) lookups during batch upserts, (ts) INCLUDE "
        "(cts) btree indexes to speed up partition-window extraction by the SQL on FHIR "
        "pipeline, and btree indexes on far.codesystemconcept so the hybrid terminology "
        "engine's recursive concept-tree rebuild (update-PUT/GET/DELETE of "
        "content-complete CodeSystems) is index-driven instead of quadratic. Legacy GIN "
        "identifier indexes are dropped (the UNIQUE expression indexes supersede them "
        "for all query patterns)."
    ),
    io_manager_key="io_manager",
)
def fhir_ifdb_database_indexes(
    context: dg.AssetExecutionContext,
    fhir_database: FHIRDatabaseResource,
) -> dg.MaterializeResult:
    """
    Create optimized indexes for FHIR IFDB resources to improve query performance.

    Phases:
      0. Drop legacy GIN identifier indexes (superseded by UNIQUE expression indexes)
      1. UNIQUE indexes on identifier for idempotent upserts
      2. txid btree indexes for MAX(txid) in batch upserts
      3. (ts) INCLUDE (cts) btree indexes for SQL on FHIR extract
      4. far.codesystemconcept btree indexes for the hybrid terminology engine
         (skipped with a warning when the far schema is absent, i.e. the FHIR
         server does not run BOX_FHIR_TERMINOLOGY_ENGINE=hybrid)
      5. GIN alias indexes over resource->'identifier' for the resource types
         whose lookups must see every identifier, not only the first per system

    Uses a single connection in autocommit mode so that each DDL statement is
    its own transaction. This is required for CREATE INDEX CONCURRENTLY (avoids
    ACCESS EXCLUSIVE locks on large tables) and means a failure on one index
    does not roll back previously created indexes. A single ANALYZE pass per
    touched table runs at the end.
    """
    logger = dg.get_dagster_logger()
    logger.info("[INDEXING] Starting creation of optimized FHIR IFDB database indexes")

    indexes_created: list[str] = []
    indexes_dropped: list[str] = []
    errors: list[str] = []
    # UNIQUE-index issues are tracked separately because they reflect data
    # quality (duplicate identifiers in Aidbox) rather than infrastructure
    # failure. Surfacing them as warnings keeps the daily/monthly ETL
    # pipeline unblocked when historic data has a single bad row, while
    # still making the problem highly visible in Dagster metadata.
    unique_index_warnings: list[str] = []
    unique_indexes_skipped: list[str] = []
    tables_to_analyze: set[str] = set()

    unique_index_specs = IDENTIFIER_UNIQUE_INDEX_SPECS

    with fhir_database.get_connection() as conn:
        # autocommit required for CREATE INDEX CONCURRENTLY (cannot run inside
        # a transaction block). Each statement commits immediately.
        conn.autocommit = True

        with conn.cursor() as cur:
            # --- Phase 0: Drop legacy GIN identifier indexes ---
            # The _opt GIN indexes were superseded by the _uniq expression
            # indexes which the query planner always prefers (confirmed by
            # pg_stat_user_indexes showing 0 scans on every _opt index).
            # Keeping them wastes write I/O on every upsert and buffer cache
            # space on multi-GB tables.
            for resource_type, identifier_system in unique_index_specs:
                table_name = resource_type.lower()
                system_safe = sanitize_system_for_identifier(identifier_system)
                gin_index_name = f"idx_{table_name}_{system_safe}_opt"
                try:
                    cur.execute(
                        "SELECT 1 FROM pg_class WHERE relname = %s",
                        (gin_index_name,),
                    )
                    if cur.fetchone():
                        logger.info(f"[INDEXING] Dropping legacy GIN index {gin_index_name}")
                        cur.execute(f"DROP INDEX IF EXISTS {gin_index_name}")
                        indexes_dropped.append(gin_index_name)
                        logger.info(f"[INDEXING] Dropped legacy GIN index {gin_index_name}")
                except Exception as exc:
                    logger.warning(f"[INDEXING] Failed to drop GIN index {gin_index_name}: {exc}")

            # --- Phase 1: UNIQUE indexes on identifier for idempotent upserts ---
            try:
                _ensure_identifier_extract_function(cur)
            except Exception as exc:
                error_msg = f"Failed to create extract_identifier_value function: {exc}"
                logger.error(f"[INDEXING] {error_msg}")
                errors.append(error_msg)
            else:
                for resource_type, identifier_system in unique_index_specs:
                    table_name = resource_type.lower()
                    index_name = _unique_index_name(resource_type, identifier_system)

                    try:
                        logger.info(
                            f"[INDEXING] Creating UNIQUE index {index_name} " f"for {resource_type}"
                        )
                        _create_unique_identifier_index(
                            cur, resource_type, identifier_system, logger
                        )

                        indexes_created.append(f"{resource_type}:{identifier_system}:unique")
                        tables_to_analyze.add(table_name)
                        logger.info(f"[INDEXING] Created UNIQUE index for {resource_type}")

                        # Foerst naar det nye navn staar, ryddes det afkortede
                        # op: raekkefoelgen er omvendt af praefiks-indeksernes,
                        # fordi dette indeks haandhaever unikhed og ikke maa
                        # vaere fravaerende i mellemtiden.
                        gammelt_navn = _legacy_truncated_unique_index_name(
                            resource_type, identifier_system
                        )
                        if gammelt_navn:
                            # Egen try: en fejlet oprydning maa ikke melde det
                            # nyoprettede indeks som sprunget over.
                            try:
                                cur.execute(
                                    "SELECT 1 FROM pg_class WHERE relname = %s", (gammelt_navn,)
                                )
                                if cur.fetchone():
                                    logger.info(
                                        f"[INDEXING] Dropping superseded truncated unique "
                                        f"index {gammelt_navn}"
                                    )
                                    cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {gammelt_navn}")
                                    indexes_dropped.append(gammelt_navn)
                            except Exception as exc:
                                logger.warning(
                                    f"[INDEXING] Failed to drop superseded truncated unique "
                                    f"index {gammelt_navn}: {exc}"
                                )
                    except Exception as exc:
                        # Treat UNIQUE-index DDL failures as warnings, not
                        # fatal. The most common cause is historic duplicate
                        # identifier values in Aidbox; failing the asset would
                        # block upstream daily/monthly ETL jobs until the dirty
                        # row is cleaned up manually. INVALID indexes left by
                        # failed CONCURRENTLY builds are auto-dropped on the
                        # next run by _drop_invalid_index_if_exists.
                        warning_msg = (
                            f"UNIQUE index {index_name} for {resource_type} "
                            f"failed (non-fatal): {exc}"
                        )
                        logger.warning(f"[INDEXING] {warning_msg}")
                        unique_index_warnings.append(warning_msg)
                        unique_indexes_skipped.append(f"{resource_type}:{identifier_system}")

            # --- Phase 1b: praefiks-indekser til oprydning af XML-boern ---
            for resource_type, identifier_system in IDENTIFIER_PREFIX_INDEX_SPECS:
                table_name = resource_type.lower()
                system_safe = sanitize_system_for_identifier(identifier_system)
                index_name = _prefix_index_name(table_name, system_safe)

                # Ryd det gamle navn op, hvor kollisionssikringen har givet et
                # nyt: ellers staar to identiske praefiks-indekser og koster
                # dobbelt skrive-I/O paa hver upsert for evigt.
                gammelt_navn = f"idx_{table_name}_{system_safe}_prefix"[:63]
                if gammelt_navn != index_name:
                    try:
                        cur.execute("SELECT 1 FROM pg_class WHERE relname = %s", (gammelt_navn,))
                        if cur.fetchone():
                            logger.info(
                                f"[INDEXING] Dropping superseded prefix index {gammelt_navn}"
                            )
                            cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {gammelt_navn}")
                            indexes_dropped.append(gammelt_navn)
                    except Exception as exc:
                        logger.warning(
                            f"[INDEXING] Failed to drop superseded prefix index "
                            f"{gammelt_navn}: {exc}"
                        )
                try:
                    logger.info(
                        f"[INDEXING] Creating prefix index {index_name} for {resource_type}"
                    )
                    _create_prefix_identifier_index(cur, resource_type, identifier_system, logger)
                    indexes_created.append(f"{resource_type}:{identifier_system}:prefix")
                    tables_to_analyze.add(table_name)
                    logger.info(f"[INDEXING] Created prefix index for {resource_type}")
                except Exception as exc:
                    # Ikke-fatal af samme grund som de unikke indekser: uden
                    # indekset er oprydningen langsom, ikke forkert.
                    warning_msg = (
                        f"Prefix index {index_name} for {resource_type} failed "
                        f"(non-fatal, reconcile falls back to a scan): {exc}"
                    )
                    logger.warning(f"[INDEXING] {warning_msg}")
                    unique_index_warnings.append(warning_msg)

            # --- Phase 2: txid btree indexes for MAX(txid) in batch upserts ---
            for resource_type in TXID_BTREE_RESOURCES:
                table_name = resource_type.lower()
                index_name = f"idx_{table_name}_txid"
                try:
                    logger.info(
                        f"[INDEXING] Creating btree index {index_name} " f"on {resource_type}(txid)"
                    )
                    cur.execute(
                        f"CREATE INDEX IF NOT EXISTS {index_name} " f"ON {table_name} (txid)"
                    )
                    indexes_created.append(f"{resource_type}:txid")
                    tables_to_analyze.add(table_name)
                    logger.info(f"[INDEXING] Created txid index for {resource_type}")
                except Exception as exc:
                    error_msg = f"Failed to create txid index for {resource_type}: {exc}"
                    logger.error(f"[INDEXING] {error_msg}")
                    errors.append(error_msg)

            # --- Phase 3: (ts) INCLUDE (cts) btree for SQL on FHIR extract ---
            logger.info(
                f"[INDEXING] Ensuring (ts) INCLUDE (cts) indexes on "
                f"{len(SOF_TS_RESOURCES)} raw resource tables"
            )
            for resource_type in SOF_TS_RESOURCES:
                table_name = resource_type.lower()
                index_name = f"idx_{table_name}_ts_cts"
                try:
                    logger.info(
                        f"[INDEXING] Creating btree index {index_name} "
                        f"on {resource_type}(ts) INCLUDE (cts)"
                    )
                    _drop_invalid_index_if_exists(cur, index_name, logger)
                    cur.execute(
                        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
                        f"ON {table_name} (ts) INCLUDE (cts)"
                    )
                    indexes_created.append(f"{resource_type}:ts_cts")
                    tables_to_analyze.add(table_name)
                except Exception as exc:
                    error_msg = f"Failed to create ts/cts index for {resource_type}: {exc}"
                    logger.error(f"[INDEXING] {error_msg}")
                    errors.append(error_msg)

            # --- Phase 4: far.codesystemconcept btree indexes (terminology) ---
            # Aidbox's far.build_concept_tree() rebuilds content-complete
            # CodeSystems recursively with a per-node probe on parent; without
            # these indexes every update-PUT/GET/DELETE of a CodeSystem scans
            # the whole table once per concept. The far schema only exists in
            # hybrid terminology-engine mode, and an Aidbox upgrade may
            # recreate it, so the indexes are re-ensured on every run.
            far_indexes_skipped = False
            cur.execute("SELECT to_regclass('far.codesystemconcept')")
            if cur.fetchone()[0] is None:
                far_indexes_skipped = True
                logger.warning(
                    "[INDEXING] far.codesystemconcept not found (FHIR server "
                    "not running BOX_FHIR_TERMINOLOGY_ENGINE=hybrid?) — "
                    "skipping FAR terminology indexes"
                )
            else:
                for index_name, columns in FAR_CODESYSTEMCONCEPT_INDEXES:
                    try:
                        logger.info(
                            f"[INDEXING] Creating btree index {index_name} "
                            f"on far.codesystemconcept ({columns})"
                        )
                        cur.execute(
                            f"CREATE INDEX IF NOT EXISTS {index_name} "
                            f"ON far.codesystemconcept ({columns})"
                        )
                        indexes_created.append(f"far.codesystemconcept:{index_name}")
                        tables_to_analyze.add("far.codesystemconcept")
                    except Exception as exc:
                        error_msg = f"Failed to create FAR terminology index {index_name}: {exc}"
                        logger.error(f"[INDEXING] {error_msg}")
                        errors.append(error_msg)

            # --- Phase 5: GIN alias indexes (every identifier value) ---
            for resource_type in IDENTIFIER_ALIAS_GIN_RESOURCES:
                table_name = resource_type.lower()
                index_name = alias_gin_index_name(table_name)
                try:
                    _drop_invalid_index_if_exists(cur, index_name, logger)
                    # An index with another expression, opclass or options is rebuilt.
                    if not _alias_index_is_current(cur, index_name):
                        logger.info(
                            f"[INDEXING] Rebuilding {index_name}: definition is out of date"
                        )
                        cur.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
                    logger.info(
                        f"[INDEXING] Creating GIN alias index {index_name} "
                        f"on {resource_type} identifier values"
                    )
                    cur.execute(
                        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} "
                        f"ON {table_name} USING gin "
                        f"(({ALIAS_GIN_EXPRESSION}) {ALIAS_GIN_OPCLASS}) "
                        f"WITH ({ALIAS_GIN_OPTIONS})"
                    )
                    indexes_created.append(f"{resource_type}:identifier_alias")
                    tables_to_analyze.add(table_name)
                    logger.info(f"[INDEXING] Created GIN alias index for {resource_type}")
                except Exception as exc:
                    error_msg = f"Failed to create GIN alias index for {resource_type}: {exc}"
                    logger.error(f"[INDEXING] {error_msg}")
                    errors.append(error_msg)

            # --- Single ANALYZE pass per table ---
            for table_name in sorted(tables_to_analyze):
                try:
                    logger.info(f"[INDEXING] ANALYZE {table_name}")
                    cur.execute(f"ANALYZE {table_name}")
                except Exception as exc:
                    logger.warning(f"[INDEXING] ANALYZE failed for {table_name}: {exc}")

    success_count = len(indexes_created)
    dropped_count = len(indexes_dropped)
    error_count = len(errors)
    warning_count = len(unique_index_warnings)

    logger.info(
        f"[INDEXING] Completed: {success_count} indexes created, "
        f"{dropped_count} legacy GIN indexes dropped, "
        f"{warning_count} unique-index warnings, {error_count} fatal errors"
    )

    missing_sof_index_warnings = check_sof_required_indexes(fhir_database, context)

    metadata: dict[str, dg.MetadataValue] = {
        "indexes_created": dg.MetadataValue.int(success_count),
        "gin_indexes_dropped": dg.MetadataValue.int(dropped_count),
        "errors": dg.MetadataValue.int(error_count),
        "unique_index_warnings": dg.MetadataValue.int(warning_count),
        "index_specs": dg.MetadataValue.json(indexes_created),
        "missing_sof_indexes_after_run": dg.MetadataValue.int(len(missing_sof_index_warnings)),
        "far_indexes_skipped": dg.MetadataValue.bool(far_indexes_skipped),
    }
    if indexes_dropped:
        metadata["gin_indexes_dropped_list"] = dg.MetadataValue.json(indexes_dropped)
    if missing_sof_index_warnings:
        metadata["missing_sof_index_details"] = dg.MetadataValue.json(missing_sof_index_warnings)
    if unique_indexes_skipped:
        metadata["unique_indexes_skipped"] = dg.MetadataValue.json(unique_indexes_skipped)
        metadata["unique_index_warning_details"] = dg.MetadataValue.json(unique_index_warnings)

    if errors:
        logger.warning(f"[INDEXING] Errors encountered: {'; '.join(errors)}")
        metadata["error_details"] = dg.MetadataValue.json(errors)
        raise dg.Failure(
            description=(f"{error_count} index creation(s) failed: {'; '.join(errors)}"),
            metadata=metadata,
        )

    return dg.MaterializeResult(metadata=metadata)
