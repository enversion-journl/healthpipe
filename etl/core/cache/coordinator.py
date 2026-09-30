# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CacheCoordinator: cross-process leader-election for shared identifier caches.

Provides the ``load_or_build`` primitive that ensures exactly ONE worker
process pays the cost of streaming source data into the SQLite cache
while N-1 other workers block on a filesystem lock and reuse the result.

Source-agnostic: the caller supplies a ``builder_fn`` that yields rows of
``(system, value, id)`` tuples. The coordinator handles file paths, TTL
checks, lock acquisition, double-checked locking, and atomic publication.

For usage examples and adoption notes see README.md in this package.
"""

from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Callable, Iterable

import dagster as dg
from core.cache.sqlite_cache import SharedIdentifierCache


class CacheCoordinator:
    """Cross-process coordinator for shared identifier caches.

    Thread-safe and process-safe. A single coordinator instance can be
    shared across many caches (different namespaces / discriminators) —
    each cache gets its own independent lock.
    """

    def __init__(self, cache_dir: str | None = None):
        """
        Args:
            cache_dir: Directory for SQLite cache files and lock sidecars.
                Defaults to ``$DAGSTER_HOME/cache/identifier_caches``. All
                workers on the same host must point to the same directory
                to benefit from cross-process coordination.
        """
        self._cache_dir = cache_dir

    @property
    def cache_dir(self) -> str:
        return self._cache_dir or SharedIdentifierCache.default_cache_dir()

    def _db_path(self, namespace: str, discriminator: frozenset[str]) -> str:
        return SharedIdentifierCache.cache_path(namespace, discriminator, cache_dir=self._cache_dir)

    def _invalidation_marker_path(self, namespace: str) -> str:
        return os.path.join(
            self.cache_dir,
            f"{SharedIdentifierCache.safe_namespace(namespace)}.invalidated",
        )

    def _marker_mtime(self, namespace: str) -> float | None:
        marker_path = self._invalidation_marker_path(namespace)
        try:
            with open(marker_path) as f:
                content = f.read().strip()
            return float(content) if content else None
        except (FileNotFoundError, ValueError):
            return None

    def _touch_invalidation_marker(self, namespace: str) -> None:
        os.makedirs(self.cache_dir, exist_ok=True)
        marker_path = self._invalidation_marker_path(namespace)
        with open(marker_path, "w") as f:
            f.write(f"{time.time():.9f}")

    def is_current(
        self,
        namespace: str,
        discriminator: frozenset[str],
        ttl_seconds: int = 3600,
    ) -> bool:
        """Return True when the cache file is fresh and not namespace-invalidated."""
        return self.is_cache_file_current(
            namespace,
            self._db_path(namespace, discriminator),
            ttl_seconds=ttl_seconds,
        )

    def is_cache_file_current(
        self,
        namespace: str,
        db_path: str,
        ttl_seconds: int = 3600,
    ) -> bool:
        """Return True when a concrete cache file is fresh and not invalidated."""
        if not SharedIdentifierCache.is_fresh(db_path, ttl_seconds):
            return False

        # File could disappear between is_fresh and getmtime — treat the race as "stale".
        try:
            marker_mtime = self._marker_mtime(namespace)
            return marker_mtime is None or os.path.getmtime(db_path) > marker_mtime
        except FileNotFoundError:
            return False

    def load_or_build(
        self,
        namespace: str,
        discriminator: frozenset[str],
        builder_fn: Callable[[], Iterable[tuple[str, str, str]]],
        ttl_seconds: int = 3600,
        force_reload: bool = False,
    ) -> SharedIdentifierCache:
        """Return a ready-to-read cache for the given (namespace, discriminator).

        Fast path: if the on-disk cache file is fresh (younger than
        ``ttl_seconds``) and ``force_reload`` is False, open it read-only
        and return — no lock overhead.

        Slow path: acquire an exclusive fcntl lock on a sidecar .build.lock
        file. Under the lock, re-check freshness (another worker may have
        built it while we were waiting). If still stale or forced, call
        ``builder_fn()`` and stream the yielded rows into SQLite, then
        atomically publish the result and release the lock.

        ``builder_fn`` is typically a generator function so the underlying
        source (Postgres cursor, Oracle cursor, etc.) stays alive for the
        duration of the stream and is cleaned up via its own finally.

        Args:
            namespace: Human-readable key, e.g. ``"fhir.Patient"``.
            discriminator: Arbitrary set that further partitions the
                namespace, e.g. the set of identifier systems.
            builder_fn: Zero-arg callable that yields ``(system, value, id)``
                tuples. Called only when a build is required.
            ttl_seconds: Max age of the cache file before it is considered
                stale and rebuilt. Defaults to 1 hour.
            force_reload: Skip the freshness check and always rebuild.

        Returns:
            A ``SharedIdentifierCache`` whose file on disk is fresh and ready
            to read. The caller owns the returned instance and should call
            ``.close()`` when done.
        """
        db_path = self._db_path(namespace, discriminator)
        logger = dg.get_dagster_logger()

        # Fast path: fresh on disk, nobody needs to build, no lock.
        if not force_reload and self.is_current(namespace, discriminator, ttl_seconds):
            logger.info(f"[SharedCache] Reusing existing cache for {namespace}: {db_path}")
            return SharedIdentifierCache(db_path)

        # Slow path: coordinate with concurrent workers.
        return self._load_or_build_under_lock(
            namespace=namespace,
            discriminator=discriminator,
            db_path=db_path,
            builder_fn=builder_fn,
            ttl_seconds=ttl_seconds,
            force_reload=force_reload,
        )

    def _load_or_build_under_lock(
        self,
        *,
        namespace: str,
        discriminator: frozenset[str],
        db_path: str,
        builder_fn: Callable[[], Iterable[tuple[str, str, str]]],
        ttl_seconds: int,
        force_reload: bool,
    ) -> SharedIdentifierCache:
        logger = dg.get_dagster_logger()
        lock_path = db_path + ".build.lock"

        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        # The lock file is a zero-byte coordination file — its content is
        # irrelevant; only the advisory lock on its open file description
        # matters. fcntl locks are released automatically on process exit
        # so a crashed leader cannot deadlock the fleet.
        wait_start = time.perf_counter()
        lock_fd = open(lock_path, "a")  # noqa: SIM115 — closed in finally
        try:
            logger.info(f"[SharedCache] Acquiring build lock for {namespace} ({lock_path})")
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
            wait_elapsed = time.perf_counter() - wait_start
            if wait_elapsed > 1.0:
                logger.info(
                    f"[SharedCache] Waited {wait_elapsed:.1f}s for build lock "
                    f"on {namespace} (another worker was building)"
                )

            # Double-checked locking: leader may have already produced.
            if not force_reload and self.is_current(namespace, discriminator, ttl_seconds):
                logger.info(
                    f"[SharedCache] {namespace} cache was produced by another "
                    "worker while we waited — reusing it"
                )
                return SharedIdentifierCache(db_path)

            # Cap retries so a caller invalidating in a tight loop fails
            # loud instead of pinning a worker on a runaway build.
            max_retries = 3
            attempt = 0
            while True:
                logger.info(
                    f"[SharedCache] Building {namespace} cache (leader) — streaming "
                    "from source..."
                )
                build_started_at = time.time()
                start = time.perf_counter()
                rows = builder_fn()
                cache = SharedIdentifierCache.build_from_iter(rows, db_path)
                elapsed = time.perf_counter() - start

                marker_mtime = self._marker_mtime(namespace)
                if marker_mtime is not None and marker_mtime >= build_started_at:
                    attempt += 1
                    cache.close()
                    # The invalidated build has already been atomically published by
                    # build_from_iter(). Do not unlink it: suspended readers may still
                    # need to reopen this path. Touch the marker again so the file is
                    # stale for new preload checks until the retry overwrites it.
                    self._touch_invalidation_marker(namespace)
                    if attempt >= max_retries:
                        raise RuntimeError(
                            f"[SharedCache] {namespace}: cache invalidated "
                            f"{max_retries} times during build — refusing to retry. "
                            "A caller is likely invalidating in a tight loop."
                        )
                    logger.info(
                        f"[SharedCache] {namespace} was invalidated while this "
                        f"cache was building — discarding and rebuilding "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                    continue

                logger.info(
                    f"[SharedCache] Built {namespace} cache with {len(cache)} entries "
                    f"in {elapsed:.2f}s → {db_path}"
                )
                return cache
        finally:
            try:
                fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
            lock_fd.close()

    def delete(self, cache: SharedIdentifierCache) -> None:
        """Close the cache and remove its on-disk file + sidecars.

        Idempotent: safe to call on a cache whose file was already
        removed by another process.
        """
        path = cache.db_path
        cache.close()
        SharedIdentifierCache.delete(path)

    def invalidate_namespace(self, namespace: str) -> int:
        """Mark every cache file in a namespace as stale via the marker file.

        Touches the namespace's invalidation marker so that any subsequent
        ``preload_identifier_cache`` call sees ``marker_mtime > db_mtime`` and
        rebuilds. This is the cross-process invalidation contract.

        Does NOT delete cache files on disk. Other processes may hold open
        file descriptors to a file in this namespace via a different
        ``(resource_type, identifier_systems)`` discriminator (e.g. another
        asset preloaded ``Procedure`` for a different identifier system);
        deleting all matching files would rip the floor out from under those
        readers and cause ``sqlite3.OperationalError: unable to open database
        file`` mid-run. Stale files age out via the TTL check on next
        preload — the marker is enough for correctness.

        Returns 0 (no files removed) for callers that previously relied on
        the count for logging only.
        """
        self._touch_invalidation_marker(namespace)
        return 0
