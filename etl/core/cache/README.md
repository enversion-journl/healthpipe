<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# Shared identifier cache

A cross-process lookup cache for mapping `(system, value)` tuples to a target
id (typically a FHIR UUID). Designed for jobs that resolve millions of
references per partition and would otherwise each load the full identifier
mapping into a per-process dict.

Architecture overview: `etl/docs/architecture/core_cache_parallel_execution.md`.

The cache is backed by a SQLite file on local disk, shared across worker
processes on the same host via filesystem mmap. An fcntl-based advisory
lock ensures exactly one worker builds the cache per TTL window; the rest
block on the lock and reuse the result.

---

## When to use this

Good fit:
- You resolve many (hundreds of thousands+) identifiers per run, repeatedly.
- The source-of-truth (Postgres / Oracle / etc.) is expensive to query or
  has strict per-query latency you want to amortise.
- Multiple Dagster workers on the same host would otherwise each load
  the full mapping.
- The mapping is relatively stable across runs so the TTL (default 1h)
  can be reused across partitions.

Bad fit:
- Small partitions with a handful of lookups — one batch SELECT to the
  source is cheaper than maintaining a cache file.
- Rapidly-changing source data where you'd have to invalidate after
  every batch anyway.
- Cross-host coordination is needed (this is a single-host primitive —
  the cache directory must be on a local filesystem shared by all
  workers; it does not coordinate across machines).

---

## Quickstart

```python
from core.cache import CacheCoordinator

coordinator = CacheCoordinator()  # defaults to $DAGSTER_HOME/cache/identifier_caches

def _stream_rows():
    # Anything that yields (system, value, target_id) tuples.
    # Typically a generator over a DB cursor so the source stays open
    # for the duration of the SQLite write.
    with some_db_resource.get_connection() as conn:
        cursor = conn.cursor(name="my_stream")
        try:
            cursor.execute("SELECT system, value, fhir_id FROM ...")
            for row in cursor:
                yield (row[0], row[1], row[2])
        finally:
            cursor.close()

cache = coordinator.load_or_build(
    namespace="myjob.patient_master",        # human-readable key
    discriminator=frozenset(["cpr"]),        # set of dimensions that partition the cache
    builder_fn=_stream_rows,
    ttl_seconds=3600,                        # optional, defaults to 1h
)

fhir_id = cache.get(("cpr", "010180-1234"))  # O(1) local lookup
```

---

## Semantics

**Fast path (no lock):** if the cache file exists and is younger than
`ttl_seconds`, just open it read-only and return. No lock overhead — this
is the hot path for warm workers.

**Slow path (leader election):** if the file is missing or stale, or
`force_reload=True`, acquire an exclusive fcntl lock on a sidecar
`.build.lock` file. Under the lock, re-check freshness (double-checked
locking) — another worker may have built it while we waited. Only if
still stale do we actually call `builder_fn`.

**Parallel-safe:** the build produces a file at a unique temporary path
and publishes via atomic `os.replace()`. Concurrent readers of the old
file continue to read the old inode until they close; new readers see
the new file.

**Crash-safe:** fcntl advisory locks are tied to the open file
description and released automatically when the holding process exits.
A leader that dies mid-build cannot deadlock the fleet.

**Read-only readers:** the cache is immutable after build. Reader
connections are opened `mode=ro`, which eliminates the journal-mode
transitions that previously caused `database is locked` errors under
concurrency.

---

## Cache invalidation

When the source data changes mid-run (e.g. after inserting new rows
that downstream lookups must see), call:

```python
coordinator.invalidate_namespace("Patient")
# Next call to load_or_build will rebuild under the lock.
```

Invalidation touches a namespace marker file. Freshness checks compare
marker mtime against cache-file mtime, so stale files rebuild on next
`load_or_build` without unlinking live files that other workers may still
need to reopen after `suspend_identifier_caches()`.

---

## Namespacing rules

`namespace` should be a dot-separated human-readable key that identifies
the logical cache. Use the source system and the entity, e.g.:

- `fhir.Patient`
- `fhir.EpisodeOfCare`
- `erp.lagerbeholdning_master`
- `mssql.dim.patient_crosswalk`

`discriminator` is an arbitrary set of strings that further partition
the cache. Two caches with the same namespace but different
discriminators are independent files. Typical discriminators:

- The set of identifier systems the cache filters on.
- The source database name when multiple sources populate the same
  namespace.
- An environment marker (`"dev"`, `"prod"`) if multiple environments
  share a cache directory (generally not recommended — use a
  per-environment `cache_dir` instead).

Collisions: two callers using the same `(namespace, discriminator)` will
share the cache file. This is the intended cross-process behaviour. Pick
discriminator sets carefully.

---

## What this does NOT solve

- **Cross-host distribution.** The cache is a filesystem file. Workers
  on different hosts do not share it. Use a shared network filesystem
  (with care — NFS has caveats around fcntl locking) or a separate
  cache layer (Redis, etc.) if you need cross-host.
- **Read-time invalidation.** Readers see a snapshot taken at build
  time. If the source changes, readers still see the old mapping until
  the namespace is explicitly invalidated and rebuilt (or the TTL
  expires).
- **Large individual lookups.** Optimised for many tiny lookups. If
  your callers need bulk lookups, prefer a single source-side SQL query
  over calling `cache.get()` in a hot loop.

---

## Adoption checklist

For FHIR jobs, prefer the facade on `FHIRDatabaseResource` instead of using
`CacheCoordinator` directly:

```python
lookups = fhir_database.get_identifier_lookups(
    [
        ("Patient", ["urn:source:ifdb:patient_id"]),
        ("Encounter", ["urn:source:ifdb:object_id"]),
    ],
    parallel=True,
)

fhir_database.suspend_identifier_caches()  # before memory-heavy process pools
fhir_database.safe_clear_identifier_caches_for_resources(
    {"Observation", "Procedure"},
    reason="post-partition cleanup",
)
```

`safe_clear_identifier_caches_for_resources()` is the production cleanup
primitive for long-running jobs: it logs cleanup failures and returns `False`
instead of letting non-critical cache invalidation hide the real job result.

To add this to a non-FHIR resource:

1. Import: `from core.cache import CacheCoordinator`.
2. Construct once, reuse per resource: `self._cache_coordinator = CacheCoordinator()`.
3. Write a generator function that yields `(system, value, target_id)` tuples
   from your source. Keep source cursor alive via a `with` inside the generator.
4. Call `coordinator.load_or_build(namespace=..., discriminator=..., builder_fn=...)`
   and use the returned `SharedIdentifierCache` via `.get()`.
5. If your job mutates the source mid-run and downstream lookups must see
   the new rows, call `coordinator.invalidate_namespace(namespace)` after
   the mutation. Avoid deleting cache files directly while workers may still
   hold handles to them.

The first adopter (FHIR IFDB pipeline) is exposed to call sites through
`etl/core/resources.py::FHIRDatabaseResource.get_identifier_lookup`
and related facade methods.
