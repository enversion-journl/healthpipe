# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Identifier lookup orchestration for FHIR database resources."""

from __future__ import annotations

import json
import os
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg
from core.cache import SharedIdentifierCache
from core.parallel import env_bool, env_int
from fhir.resources.R4B.resource import Resource
from utils.fhir.identifier_systems import _SAFE_IDENTIFIER_SYSTEM_RE
from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SET

IdentifierLookup = SharedIdentifierCache | dict[tuple[str, str], str]
IdentifierLookupSpec = tuple[str, list[str] | None]


def _logger():
    from core import resources as resources_mod

    return resources_mod.dg.get_dagster_logger()


# Per-key counter for ``_resolve_identifier_values_direct`` calls in the
# current worker process. Reset explicitly by chunked pipeline and IFDB
# partition setup lifecycle hooks. When the count exceeds
# ``FHIR_TARGETED_LOOKUP_WARN_THRESHOLD`` (default 50) within one scope,
# a single WARN is emitted to flag a possible infinite-retry loop.
# Tracked here at module level (not on the FHIRDatabaseResource) so the
# counter survives between resource method calls without touching the
# resource's hot-path attribute set. Under multiprocess execution each
# worker gets its own copy of this module-level state.
_targeted_lookup_call_counts: dict[tuple[str, str], int] = defaultdict(int)
_targeted_lookup_warn_emitted: set[tuple[str, str]] = set()


def reset_targeted_lookup_counters(self=None) -> None:
    """Reset targeted-lookup regression counters for a new partition/setup scope."""
    del self
    _targeted_lookup_call_counts.clear()
    _targeted_lookup_warn_emitted.clear()


def _cache_key(
    resource_type: str,
    identifier_systems: list[str] | None = None,
) -> tuple[str, frozenset[str]]:
    systems_key = frozenset(identifier_systems) if identifier_systems else frozenset()
    return (resource_type, systems_key)


def get_loaded_identifier_cache(
    self,
    resource_type: str,
    identifier_systems: list[str] | None = None,
) -> IdentifierLookup | None:
    """Return an already-loaded cache without triggering preload."""
    return self._identifier_caches.get(self._cache_key(resource_type, identifier_systems))


def get_identifier_cache(
    self,
    resource_type: str,
    identifier_systems: list[str] | None = None,
    force_reload: bool = False,
    use_cache: bool = True,
) -> IdentifierLookup:
    """Load and return identifier cache for (resource_type, systems)."""
    self.preload_identifier_cache(
        resource_type=resource_type,
        identifier_systems=identifier_systems,
        force_reload=force_reload,
        use_cache=use_cache,
    )
    cache = self.get_loaded_identifier_cache(resource_type, identifier_systems)
    if cache is None:
        raise RuntimeError(
            "Identifier cache missing after preload for "
            f"{resource_type} ({identifier_systems or []})"
        )
    return cache


def get_identifier_lookup(
    self,
    resource_type: str,
    identifier_systems: list[str] | None = None,
    force_reload: bool = False,
) -> IdentifierLookup:
    """Return the canonical identifier lookup for a resource/system pair.

    This is the production-facing facade for identifier cache consumers.
    Backend choice, preload, TTL, locking, and per-process reuse stay behind
    this method so callers only express the lookup they need.
    """
    return self.get_identifier_cache(
        resource_type=resource_type,
        identifier_systems=identifier_systems,
        force_reload=force_reload,
        use_cache=True,
    )


def get_identifier_lookups(
    self,
    specs: Sequence[IdentifierLookupSpec],
    force_reload: bool = False,
    parallel: bool = True,
) -> dict[str, IdentifierLookup]:
    """Load several identifier lookups and return them keyed by resource type."""
    spec_list = list(specs)
    if not spec_list:
        return {}

    resource_types = [resource_type for resource_type, _ in spec_list]
    duplicates = {
        resource_type for resource_type in resource_types if resource_types.count(resource_type) > 1
    }
    if duplicates:
        duplicate_list = ", ".join(sorted(duplicates))
        raise ValueError(
            "get_identifier_lookups returns lookups keyed by resource_type; "
            f"duplicate resource types are ambiguous: {duplicate_list}"
        )

    def _load(spec: IdentifierLookupSpec) -> tuple[str, IdentifierLookup]:
        resource_type, systems = spec
        return (
            resource_type,
            self.get_identifier_lookup(
                resource_type=resource_type,
                identifier_systems=systems,
                force_reload=force_reload,
            ),
        )

    if parallel and len(spec_list) > 1:
        lookups: dict[str, IdentifierLookup] = {}
        # I/O-bound (Postgres streaming) — threads release the GIL on socket reads.
        with ThreadPoolExecutor(
            max_workers=len(spec_list),
            thread_name_prefix="identifier-lookup-warmup",
        ) as pool:
            futures = [pool.submit(_load, spec) for spec in spec_list]
            for future in as_completed(futures):
                resource_type, lookup = future.result()
                lookups[resource_type] = lookup
        return lookups

    return dict(_load(spec) for spec in spec_list)


def resolve_identifier_values(
    self,
    resource_type: str,
    identifier_system: str,
    values: Sequence[str],
    force_reload: bool = False,
) -> dict[str, str]:
    """Resolve identifier values to FHIR ids.

    For resource types that have a UNIQUE partial expression index the
    targeted SQL path is always fastest (sub-second Index Scan).

    For resource types *without* a unique index (e.g. Observation) we
    auto-preload a SharedIdentifierCache the first time we see the
    (resource_type, system) pair and serve subsequent calls from it.
    This avoids paying a sequential-scan + lateral-unnest cost per
    batch on multi-million-row tables. If the preload yields no usable
    cache (small/test fixtures, infra failure) we fall back to the
    targeted SQL path.
    """
    if not values:
        return {}

    use_indexed_path = (
        resource_type,
        identifier_system,
    ) in IDENTIFIER_UNIQUE_INDEX_SET

    # Indexed path: always hits live Postgres via _resolve_identifier_values_direct
    # (no in-process cache), so force_reload is intentionally a no-op here.
    if not use_indexed_path:
        cache_key = self._cache_key(resource_type, [identifier_system])
        if force_reload:
            _discard_loaded_identifier_cache(self, cache_key)
        else:
            lookup = self.get_loaded_identifier_cache(
                resource_type=resource_type,
                identifier_systems=[identifier_system],
            )
            if lookup is not None and _loaded_lookup_is_current(self, resource_type, lookup):
                return _resolve_from_loaded_lookup(
                    lookup=lookup,
                    identifier_system=identifier_system,
                    values=values,
                )
            if lookup is not None:
                _discard_loaded_identifier_cache(self, cache_key)

        # Auto-preload the shared cache for non-indexed types so we don't
        # re-scan the table per batch. preload_identifier_cache is a no-op
        # when a current cache already exists in this worker.
        self.preload_identifier_cache(
            resource_type=resource_type,
            identifier_systems=[identifier_system],
            force_reload=force_reload,
            use_cache=True,
        )
        lookup = self.get_loaded_identifier_cache(
            resource_type=resource_type,
            identifier_systems=[identifier_system],
        )
        if lookup is not None:
            return _resolve_from_loaded_lookup(
                lookup=lookup,
                identifier_system=identifier_system,
                values=values,
            )

    return _resolve_identifier_values_direct(
        self,
        resource_type=resource_type,
        identifier_system=identifier_system,
        values=values,
    )


def _loaded_lookup_is_current(
    self,
    resource_type: str,
    lookup: IdentifierLookup,
) -> bool:
    if not isinstance(lookup, SharedIdentifierCache):
        return True

    cache_ttl = env_int("IDENTIFIER_CACHE_TTL_SECONDS", 3600)
    if cache_ttl <= 0:
        cache_ttl = 3600
    return self.cache_coordinator.is_cache_file_current(
        resource_type,
        lookup.db_path,
        ttl_seconds=cache_ttl,
    )


def _discard_loaded_identifier_cache(
    self,
    cache_key: tuple[str, frozenset[str]],
) -> None:
    cached = self._identifier_caches.pop(cache_key, None)
    if isinstance(cached, SharedIdentifierCache):
        cached.close()
    if self._active_cache_key == cache_key:
        self._identifier_cache = None
        self._active_cache_key = None


def _resolve_from_loaded_lookup(
    *,
    lookup: IdentifierLookup,
    identifier_system: str,
    values: Sequence[str],
) -> dict[str, str]:
    resolved: dict[str, str] = {}
    for value in values:
        value_str = str(value)
        fhir_id = lookup.get((identifier_system, value_str))
        if fhir_id:
            resolved[value_str] = fhir_id
    return resolved


def _resolve_identifier_values_direct(
    self,
    *,
    resource_type: str,
    identifier_system: str,
    values: Sequence[str],
) -> dict[str, str]:
    value_list = _dedupe_identifier_values(values)
    if not value_list:
        return {}

    table_name = _fhir_table_name(resource_type)
    batch_size = env_int("FHIR_IDENTIFIER_TARGETED_LOOKUP_BATCH_SIZE", 10_000)
    if batch_size <= 0:
        batch_size = 10_000
    total_batches = (len(value_list) + batch_size - 1) // batch_size
    logger = _logger()

    # Regression alarm. With unique indexes, targeted lookup per chunk is
    # expected and fast. Threshold raised to flag truly excessive call
    # counts that might indicate an infinite-retry loop.
    counter_key = (resource_type, identifier_system)
    _targeted_lookup_call_counts[counter_key] += 1
    threshold = env_int("FHIR_TARGETED_LOOKUP_WARN_THRESHOLD", 50, minimum=1)
    current_count = _targeted_lookup_call_counts[counter_key]
    if current_count > threshold and counter_key not in _targeted_lookup_warn_emitted:
        _targeted_lookup_warn_emitted.add(counter_key)
        logger.warning(
            "[IdentifierLookup] Targeted %s/%s lookup called %d times in this "
            "partition (threshold=%d). Check for infinite-retry loops.",
            resource_type,
            identifier_system,
            current_count,
            threshold,
        )

    start_time = time.perf_counter()
    resolved: dict[str, str] = {}

    # Choose query shape based on whether a UNIQUE partial expression index
    # exists for this (resource_type, identifier_system). When it does, use
    # extract_identifier_value(...) + @> so the planner can match the index
    # expression verbatim and run an Index Scan instead of Seq Scan +
    # lateral unnest. Identifier system must be embedded as a SQL literal
    # because expression indexes do not match bound parameters; the
    # _SAFE_IDENTIFIER_SYSTEM_RE check guards against SQL injection.
    use_indexed_path = (
        resource_type,
        identifier_system,
    ) in IDENTIFIER_UNIQUE_INDEX_SET

    if use_indexed_path:
        if not _SAFE_IDENTIFIER_SYSTEM_RE.match(identifier_system):
            raise ValueError(
                f"identifier_system {identifier_system!r} is in the indexed "
                "set but failed the safe-character check for SQL-literal "
                "interpolation"
            )
        system_filter_json = json.dumps([{"system": identifier_system}])
        # Inline the @> operand as a SQL literal so it matches the partial
        # UNIQUE index predicate verbatim under both custom and generic
        # plans. With a bound %s::jsonb parameter, generic plans cannot
        # prove the parameter implies the index predicate and fall back to
        # heap-rechecking @> per candidate row (~50x slower).
        query = psycopg.sql.SQL(
            "SELECT t.id,"
            " extract_identifier_value(t.resource, {system}) AS value"
            " FROM {table} t"
            " WHERE t.resource->'identifier' @> {filter}::jsonb"
            " AND extract_identifier_value(t.resource, {system}) = ANY(%s)"
        ).format(
            system=psycopg.sql.Literal(identifier_system),
            table=psycopg.sql.Identifier(table_name),
            filter=psycopg.sql.Literal(system_filter_json),
        )

        def query_params_for(batch):
            return (batch,)

        path_label = "indexed"
    else:
        query = f"""
            SELECT
                t.id,
                identifier_elem->>'value' AS value
            FROM {table_name} t,
                 jsonb_array_elements(t.resource->'identifier') AS identifier_elem
            WHERE t.resource->'identifier' IS NOT NULL
              AND identifier_elem->>'system' = %s
              AND identifier_elem->>'value' = ANY(%s)
        """

        def query_params_for(batch):
            return identifier_system, batch

        path_label = "legacy"

    logger.info(
        f"[IdentifierLookup] Targeted {resource_type} lookup ({path_label} path) "
        f"for {len(value_list)} values in {total_batches} batches "
        f"(batch_size={batch_size})"
    )

    with self.get_connection() as conn:
        with conn.cursor() as cur:
            # Force per-execution re-planning so the planner can use the
            # bound ANY(%s) array for selectivity instead of switching to a
            # generic plan after 5 executions. SET LOCAL scopes to the
            # transaction and reverts on commit.
            cur.execute("SET LOCAL plan_cache_mode = force_custom_plan")
            for batch in _chunks(value_list, batch_size):
                cur.execute(query, query_params_for(batch))
                for row in cur.fetchall():
                    fhir_id, value = row[0], row[1]
                    if fhir_id and value:
                        value_str = str(value)
                        if value_str not in resolved:
                            resolved[value_str] = str(fhir_id)

    elapsed = time.perf_counter() - start_time
    logger.info(
        f"[IdentifierLookup] Targeted {resource_type} lookup resolved "
        f"{len(resolved)}/{len(value_list)} values in {elapsed:.2f}s"
    )
    return resolved


def _dedupe_identifier_values(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for value in values:
        if value is None:
            continue
        value_str = str(value)
        if value_str in seen:
            continue
        seen.add(value_str)
        deduped.append(value_str)
    return deduped


def _chunks(values: Sequence[str], size: int) -> Iterator[list[str]]:
    for start in range(0, len(values), size):
        yield list(values[start : start + size])


def _fhir_table_name(resource_type: str) -> str:
    table_name = resource_type.lower()
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Invalid FHIR resource type for table lookup: {resource_type}")
    return table_name


def resolve_resource_ids(
    self,
    resources: Sequence[Resource],
    resource_type: str,
    preferred_systems: list[str],
    force_reload: bool = False,
) -> dict[int, str | None]:
    """Resolve resource indexes to existing FHIR ids via targeted SQL.

    Collects the unique identifier values present in the batch and resolves
    them through ``resolve_identifier_values`` (targeted Index Scan) instead
    of preloading the entire resource table into a SQLite cache.
    """
    if not resources:
        return {}

    logger = _logger()
    index_to_id: dict[int, str | None] = {}

    for preferred_system in preferred_systems:
        values_by_index: dict[int, str] = {}
        for idx, resource in enumerate(resources):
            if idx in index_to_id:
                continue
            for ident in getattr(resource, "identifier", None) or []:
                system = getattr(ident, "system", None)
                value = getattr(ident, "value", None)
                if system == preferred_system and value:
                    values_by_index[idx] = str(value)
                    break

        if not values_by_index:
            continue

        unique_values = list(dict.fromkeys(values_by_index.values()))
        resolved_ids = self.resolve_identifier_values(
            resource_type=resource_type,
            identifier_system=preferred_system,
            values=unique_values,
            force_reload=force_reload,
        )

        for idx, identifier_value in values_by_index.items():
            fhir_id = resolved_ids.get(identifier_value)
            if fhir_id:
                index_to_id[idx] = fhir_id

    logger.info(
        f"[IdentifierLookup] Resolved {len(index_to_id)}/{len(resources)} "
        f"{resource_type} IDs (targeted)"
    )
    return index_to_id


def preload_identifier_cache(
    self,
    resource_type: str,
    identifier_systems: list[str] | None = None,
    force_reload: bool = False,
    use_cache: bool = True,
) -> None:
    """Preload identifier mappings into the per-resource registry.

    Two backends are supported, selected at call time:

    - ``use_cache=True`` (default): build (or reuse) a shared on-disk
      SQLite cache via :class:`core.cache.CacheCoordinator`.
      Cross-process coordination, atomic publishing, mmap'd reads.
      Required for prod-scale FHIR tables (millions of rows) where a
      per-process Python dict would blow worker memory.

    - ``use_cache=False``: build a per-process Python dict
      ``{(system, value): fhir_id}`` from a single Postgres query.
      Simple, self-contained, no disk I/O. Only safe when the target
      table is tiny (tests, fixtures, or future targeted-query
      optimisations on small batches). Default-OFF would OOM on prod.

    Both backends populate ``self._identifier_caches`` with a lookup
    object that supports ``get((system, value))`` + ``len(...)``, so the
    downstream resolver path is identical.

    Cross-process coordination, atomic publishing, and read-only access
    are handled by :class:`core.cache.CacheCoordinator`. This method just
    provides the FHIR-specific Postgres query that produces the source
    rows, plus the per-process in-memory short-circuit for repeat calls
    within the same worker.

    The cache is valid for 1 hour by default (configurable via
    ``IDENTIFIER_CACHE_TTL_SECONDS``).

    Args:
        resource_type: Type of resource (e.g., "Patient")
        identifier_systems: Optional list of systems to filter (loads all if None)
        force_reload: If True, reload even if already cached (default: False)
    """
    cache_key = self._cache_key(resource_type, identifier_systems)
    _, systems_key = cache_key
    logger = _logger()

    # In-memory short-circuit — reuse an already-loaded entry of the
    # matching backing type. Type mismatch (e.g. caller now wants dict
    # but we have a SharedIdentifierCache) triggers a rebuild so the
    # caller gets the semantics it asked for.
    if not force_reload and cache_key in self._identifier_caches:
        cached = self._identifier_caches[cache_key]
        cached_is_shared = isinstance(cached, SharedIdentifierCache)

        if use_cache and cached_is_shared:
            cache_ttl = env_int(
                "IDENTIFIER_CACHE_TTL_SECONDS",
                3600,
                minimum=1,
            )
            if self.cache_coordinator.is_cache_file_current(
                resource_type,
                cached.db_path,
                ttl_seconds=cache_ttl,
            ):
                logger.info(
                    f"[SharedCache] Using in-memory {resource_type} cache "
                    f"({len(cached)} entries) — skipping disk and DB"
                )
                self._identifier_cache = cached
                self._active_cache_key = cache_key
                return
            # Stale → fall through to discard + rebuild
        elif not use_cache and not cached_is_shared:
            # Dict path: lifetime is this worker process. Cleared explicitly
            # by clear_identifier_caches_for_resources after upserts.
            logger.info(
                f"[Memory Cache] Using existing {resource_type} dict "
                f"({len(cached)} entries) — skipping DB query"
            )
            self._identifier_cache = cached
            self._active_cache_key = cache_key
            return

        stale = self._identifier_caches.pop(cache_key)
        if isinstance(stale, SharedIdentifierCache):
            stale.close()
        if self._active_cache_key == cache_key:
            self._identifier_cache = None
            self._active_cache_key = None
        if use_cache != cached_is_shared:
            logger.info(
                f"[Cache] {resource_type} backing change requested "
                f"(had {'SQLite' if cached_is_shared else 'dict'}, "
                f"want {'SQLite' if use_cache else 'dict'}) — rebuilding"
            )
        else:
            logger.info(
                f"[SharedCache] In-memory {resource_type} cache is stale "
                "after namespace invalidation — rebuilding"
            )

    if use_cache:
        cache_ttl = env_int(
            "IDENTIFIER_CACHE_TTL_SECONDS",
            3600,
            minimum=1,
        )

        def _builder() -> Iterator[tuple[str, str, str]]:
            """Stream FHIR identifiers from Postgres on demand.

            Generator: the psycopg server-side cursor stays open for the
            entire duration of the caller's iteration and is closed via
            the ``finally`` when the generator is exhausted or discarded.
            """
            yield from self._stream_fhir_identifiers(
                resource_type=resource_type,
                identifier_systems=identifier_systems,
            )

        cache: IdentifierLookup = self.cache_coordinator.load_or_build(
            namespace=resource_type,
            discriminator=systems_key,
            builder_fn=_builder,
            ttl_seconds=cache_ttl,
            force_reload=force_reload,
        )
    else:
        cache = self._build_identifier_dict(
            resource_type=resource_type,
            identifier_systems=identifier_systems,
        )

    self._identifier_caches[cache_key] = cache
    self._identifier_cache = cache
    self._active_cache_key = cache_key


def _build_identifier_dict(
    self,
    resource_type: str,
    identifier_systems: list[str] | None,
) -> dict[tuple[str, str], str]:
    """Build a per-process Python dict of identifier mappings.

    Single Postgres query; no disk I/O, no cross-process coordination.
    Lifetime is the worker process — cleared explicitly by
    :meth:`clear_identifier_caches_for_resources` after a successful
    bulk upsert. This is the historic pre-shared-cache path, kept as
    the default backing so callers that don't need parallel-worker
    memory sharing can avoid the SQLite/mmap machinery entirely.
    """
    table_name = resource_type.lower()
    logger = _logger()

    systems_info = f" for systems {identifier_systems}" if identifier_systems else " (all systems)"
    logger.info(f"[Memory Cache] Building identifier dict for {resource_type}{systems_info}")

    start_time = time.perf_counter()
    new_cache: dict[tuple[str, str], str] = {}

    with self.get_connection() as conn:
        with conn.cursor() as cur:
            if identifier_systems:
                query = f"""
                SELECT
                    t.id,
                    identifier_elem->>'system' as system,
                    identifier_elem->>'value' as value
                FROM {table_name} t,
                     jsonb_array_elements(t.resource->'identifier') AS identifier_elem
                WHERE t.resource->'identifier' IS NOT NULL
                  AND identifier_elem->>'system' = ANY(%s)
                  AND identifier_elem->>'value' IS NOT NULL
                """
                cur.execute(query, (identifier_systems,))
                for row in cur.fetchall():
                    fhir_id, system, value = row[0], row[1], row[2]
                    if system and value:
                        new_cache[(str(system), str(value))] = str(fhir_id)
            else:
                query = f"""
                SELECT
                    id,
                    jsonb_array_elements(resource->'identifier') as identifier
                FROM {table_name}
                WHERE resource->'identifier' IS NOT NULL
                """
                cur.execute(query)
                for row in cur.fetchall():
                    fhir_id = row[0]
                    identifier = row[1]
                    if isinstance(identifier, str):
                        identifier = json.loads(identifier)
                    system = identifier.get("system")
                    value = identifier.get("value")
                    if system and value:
                        new_cache[(str(system), str(value))] = str(fhir_id)

    elapsed = time.perf_counter() - start_time
    logger.info(
        f"[Memory Cache] Loaded {len(new_cache)} {resource_type} identifier "
        f"mappings in {elapsed:.2f}s"
    )
    return new_cache


def _stream_fhir_identifiers(
    self,
    *,
    resource_type: str,
    identifier_systems: list[str] | None,
) -> Iterator[tuple[str, str, str]]:
    """FHIR-specific row source: yield (system, value, fhir_id) tuples.

    Uses a server-side psycopg cursor so rows are streamed in batches of
    10 000 from Postgres rather than materialised client-side. The
    generator form ensures the cursor is closed exactly when the caller
    stops iterating (successfully, via exception, or via GC).
    """
    table_name = resource_type.lower()
    cursor_name = f"identifier_cache_stream_{uuid.uuid4().hex}"

    with self.get_connection() as conn:
        server_cursor = conn.cursor(name=cursor_name)
        server_cursor.itersize = 10_000
        try:
            if identifier_systems:
                query = f"""
                SELECT
                    t.id,
                    identifier_elem->>'system' as system,
                    identifier_elem->>'value' as value
                FROM {table_name} t,
                     jsonb_array_elements(t.resource->'identifier') AS identifier_elem
                WHERE t.resource->'identifier' IS NOT NULL
                  AND identifier_elem->>'system' = ANY(%s)
                  AND identifier_elem->>'value' IS NOT NULL
                """
                server_cursor.execute(query, (identifier_systems,))
                for row in server_cursor:
                    fhir_id, system, value = row[0], row[1], row[2]
                    if system and value:
                        yield (str(system), str(value), str(fhir_id))
            else:
                query = f"""
                SELECT
                    id,
                    jsonb_array_elements(resource->'identifier') as identifier
                FROM {table_name}
                WHERE resource->'identifier' IS NOT NULL
                """
                server_cursor.execute(query)
                for row in server_cursor:
                    fhir_id = row[0]
                    identifier = row[1]
                    if isinstance(identifier, str):
                        identifier = json.loads(identifier)
                    system = identifier.get("system")
                    value = identifier.get("value")
                    if system and value:
                        yield (str(system), str(value), str(fhir_id))
        finally:
            server_cursor.close()


def suspend_identifier_caches(self) -> None:
    """Close SQLite connections on all loaded identifier caches without
    removing the on-disk files.

    This releases mmap'd memory (~256 MB per cache) so memory-intensive
    operations like ``ProcessPoolExecutor`` spawns can run without
    hitting the pod memory limit.  The caches reopen lazily on the next
    ``.get()`` call — no rebuild cost, just one ``sqlite3.connect()``.
    """
    closed = 0
    for cached in self._identifier_caches.values():
        if isinstance(cached, SharedIdentifierCache):
            cached.close()
            closed += 1
    if isinstance(self._identifier_cache, SharedIdentifierCache):
        self._identifier_cache.close()
    if closed:
        _logger().info(
            "[SharedCache] Suspended %d identifier cache connection(s) " "to free mmap memory",
            closed,
        )


def log_identifier_lookup_status(
    self,
    label: str,
    lookups: Mapping[str, IdentifierLookup],
) -> None:
    """Log cache-file status for loaded lookups when diagnostics are enabled."""
    if not env_bool("FHIR_IDENTIFIER_CACHE_DIAG", False):
        return

    logger = _logger()
    for name, lookup in lookups.items():
        path = getattr(lookup, "db_path", None) or getattr(
            lookup,
            "_db_path",
            None,
        )
        if not path:
            logger.info("[CACHE_DIAG] %s %s: in-memory lookup", label, name)
            continue
        exists = os.path.exists(path)
        logger.info(
            "[CACHE_DIAG] %s %s: path=%s exists=%s size=%d",
            label,
            name,
            path,
            exists,
            os.path.getsize(path) if exists else 0,
        )


def safe_clear_identifier_caches_for_resources(
    self,
    resource_types: set[str],
    *,
    reason: str | None = None,
) -> bool:
    """Clear selected caches and turn cleanup failures into diagnostic logs."""
    try:
        self.clear_identifier_caches_for_resources(resource_types)
        return True
    except Exception as exc:  # noqa: BLE001
        reason_text = f" ({reason})" if reason else ""
        _logger().warning(
            "[SharedCache] safe invalidation failed%s for %s: %s",
            reason_text,
            ", ".join(sorted(resource_types)),
            exc,
            exc_info=True,
        )
        return False


def clear_identifier_caches_for_resources(self, resource_types: set[str]) -> None:
    """Invalidate selected identifier-cache namespaces.

    Drops matching entries from this resource's in-memory registry and closes
    any live SQLite connections held by this process. On-disk cache files are
    intentionally not deleted here: other workers may still hold references and
    lazily reopen after ``suspend_identifier_caches()``.
    """
    if not resource_types:
        return

    # Reset the targeted-lookup regression counters for the resource types
    # we are clearing. The chunked pipeline runner calls this in its
    # ``finally`` block at partition end, so this is the natural reset point
    # for the partition-scoped counter.
    counter_keys_to_drop = [key for key in _targeted_lookup_call_counts if key[0] in resource_types]
    for key in counter_keys_to_drop:
        _targeted_lookup_call_counts.pop(key, None)
        _targeted_lookup_warn_emitted.discard(key)

    detached = 0
    detached_instances: set[int] = set()
    keys_to_remove = [key for key in self._identifier_caches.keys() if key[0] in resource_types]

    for key in keys_to_remove:
        cached = self._identifier_caches.pop(key, None)
        if not isinstance(cached, SharedIdentifierCache):
            continue
        cache_id = id(cached)
        if cache_id in detached_instances:
            continue
        detached_instances.add(cache_id)
        # Close this process' handle to reduce mmap pressure; namespace
        # invalidation below is the cross-process freshness signal.
        try:
            cached.close()
            detached += 1
        except Exception:  # noqa: BLE001
            pass

    invalidated_namespaces = 0
    for resource_type in resource_types:
        try:
            self.cache_coordinator.invalidate_namespace(resource_type)
            invalidated_namespaces += 1
        except OSError as exc:
            _logger().warning(
                f"[SharedCache] Could not invalidate {resource_type} namespace: {exc}"
            )

    if self._active_cache_key is not None and self._active_cache_key[0] in resource_types:
        self._identifier_cache = None
        self._active_cache_key = None
    elif (
        isinstance(self._identifier_cache, SharedIdentifierCache)
        and id(self._identifier_cache) in detached_instances
    ):
        self._identifier_cache = None
        self._active_cache_key = None

    _logger().info(
        "[SharedCache] Cleared identifier caches for %s "
        "(detached %d loaded cache(s), invalidated %d namespace marker(s))",
        ", ".join(sorted(resource_types)),
        detached,
        invalidated_namespaces,
    )


def clear_identifier_caches(self) -> None:
    """Invalidate all identifier caches held by this resource.

    This is the runtime-safe global cleanup primitive:
    - closes any loaded SQLite handles in this process,
    - drops loaded entries from the in-memory registry,
    - marks every touched namespace stale via invalidation markers.

    It intentionally does NOT delete cache files on disk. Readers in other
    workers may still reopen those files after ``suspend_identifier_caches()``.
    """
    _targeted_lookup_call_counts.clear()
    _targeted_lookup_warn_emitted.clear()

    detached = 0
    detached_instances: set[int] = set()
    resource_types = {key[0] for key in self._identifier_caches}
    if self._active_cache_key is not None:
        resource_types.add(self._active_cache_key[0])

    for cached in list(self._identifier_caches.values()):
        if isinstance(cached, SharedIdentifierCache):
            cache_id = id(cached)
            if cache_id in detached_instances:
                continue
            detached_instances.add(cache_id)
            try:
                cached.close()
                detached += 1
            except OSError as exc:
                _logger().warning(f"[SharedCache] Could not close {cached.db_path}: {exc}")
    self._identifier_caches.clear()

    if isinstance(self._identifier_cache, SharedIdentifierCache):
        cache_id = id(self._identifier_cache)
        if cache_id not in detached_instances:
            try:
                self._identifier_cache.close()
                detached += 1
            except OSError as exc:
                _logger().warning(
                    f"[SharedCache] Could not close {self._identifier_cache.db_path}: {exc}"
                )
    self._identifier_cache = None
    self._active_cache_key = None

    invalidated_namespaces = 0
    for resource_type in resource_types:
        try:
            self.cache_coordinator.invalidate_namespace(resource_type)
            invalidated_namespaces += 1
        except OSError as exc:
            _logger().warning(
                f"[SharedCache] Could not invalidate {resource_type} namespace: {exc}"
            )

    _logger().info(
        "[SharedCache] Cleared all identifier caches "
        "(detached %d loaded cache(s), invalidated %d namespace marker(s))",
        detached,
        invalidated_namespaces,
    )


def finalize_identifier_caches(
    self,
    resource_types: set[str],
    *,
    reason: str | None = None,
) -> None:
    """Invalidate and purge cache files for resource types whose partition is done.

    Combines physical file cleanup with runtime invalidation in a single
    call. Purge runs first (while entries are still in the registry), then
    clear drops in-memory state. Safe to use in a ``finally`` block at
    partition end, when no suspended readers will reopen these files.
    """
    purge_identifier_cache_files(self, resource_types=resource_types)
    clear_identifier_caches_for_resources(self, resource_types)
    reason_text = f" ({reason})" if reason else ""
    _logger().info(
        "[SharedCache] Finalized identifier caches for %s%s",
        ", ".join(sorted(resource_types)),
        reason_text,
    )


def purge_identifier_cache_files(
    self,
    resource_types: set[str] | None = None,
) -> int:
    """Destructively delete cache files for selected namespaces.

    Use only in controlled maintenance/test workflows where no active readers
    are expected. Runtime pipelines should use marker-based invalidation via
    ``clear_identifier_caches()`` or
    ``clear_identifier_caches_for_resources()``.
    """
    if resource_types is None:
        target_resource_types = {key[0] for key in self._identifier_caches}
        if self._active_cache_key is not None:
            target_resource_types.add(self._active_cache_key[0])
    else:
        target_resource_types = set(resource_types)

    if not target_resource_types:
        _logger().info("[SharedCache] Purge skipped (no matching resource types)")
        return 0

    counter_keys_to_drop = [
        key for key in _targeted_lookup_call_counts if key[0] in target_resource_types
    ]
    for key in counter_keys_to_drop:
        _targeted_lookup_call_counts.pop(key, None)
        _targeted_lookup_warn_emitted.discard(key)

    removed = 0
    removed_instances: set[int] = set()
    keys_to_remove = [
        key for key in self._identifier_caches.keys() if key[0] in target_resource_types
    ]
    for key in keys_to_remove:
        cached = self._identifier_caches.pop(key, None)
        if not isinstance(cached, SharedIdentifierCache):
            continue
        cache_id = id(cached)
        if cache_id in removed_instances:
            continue
        removed_instances.add(cache_id)
        try:
            self.cache_coordinator.delete(cached)
            removed += 1
        except OSError as exc:
            _logger().warning(f"[SharedCache] Could not delete {cached.db_path}: {exc}")

    if self._active_cache_key is not None and self._active_cache_key[0] in target_resource_types:
        if isinstance(self._identifier_cache, SharedIdentifierCache):
            cache_id = id(self._identifier_cache)
            if cache_id not in removed_instances:
                try:
                    self.cache_coordinator.delete(self._identifier_cache)
                    removed += 1
                except OSError as exc:
                    _logger().warning(
                        "[SharedCache] Could not delete active cache "
                        f"{self._identifier_cache.db_path}: {exc}"
                    )
        self._identifier_cache = None
        self._active_cache_key = None
    elif (
        isinstance(self._identifier_cache, SharedIdentifierCache)
        and id(self._identifier_cache) in removed_instances
    ):
        self._identifier_cache = None
        self._active_cache_key = None

    invalidated_namespaces = 0
    for resource_type in target_resource_types:
        try:
            self.cache_coordinator.invalidate_namespace(resource_type)
            invalidated_namespaces += 1
        except OSError as exc:
            _logger().warning(
                f"[SharedCache] Could not invalidate {resource_type} namespace: {exc}"
            )

    _logger().info(
        "[SharedCache] Purged identifier cache files for %s "
        "(removed %d file(s), invalidated %d namespace marker(s))",
        ", ".join(sorted(target_resource_types)),
        removed,
        invalidated_namespaces,
    )
    return removed
