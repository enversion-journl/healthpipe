# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQLite-backed identifier cache storage.

Generic storage primitive. Knows nothing about FHIR, Postgres, or any
particular source system — it just maps (system, value) tuples to a
string id via a SQLite file on disk.

The caller is responsible for producing the (system, value, id) rows.
CacheCoordinator (see coordinator.py) provides the cross-process
coordination that ensures these are built once per TTL window.
"""

from __future__ import annotations

import glob
import hashlib
import os
import re
import sqlite3
import threading
import time
import uuid
from collections.abc import Iterable


class SharedIdentifierCache:
    """Dict-like read-only cache backed by SQLite on disk.

    Provides the same ``.get(key)`` interface as a
    ``dict[tuple[str, str], str]``, but stores data in a SQLite file so
    multiple worker processes can share a single on-disk cache instead of
    each loading the full dataset into RAM.

    After ``build_from_iter`` completes, the file at ``db_path`` is stable
    and can be opened read-only by any number of concurrent readers. Linux
    mmap + page cache ensures physical memory is shared across processes
    with a file descriptor on the same file.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = None
        self._len: int | None = None
        # Serialises first-use connection init so two threads sharing a
        # cache instance can't both create a connection and orphan one.
        self._init_lock = threading.Lock()

    @property
    def db_path(self) -> str:
        return self._db_path

    def _ensure_connection(self) -> sqlite3.Connection:
        if self._conn is None:
            with self._init_lock:
                if self._conn is None:
                    # Open read-only. The cache is immutable after build_from_iter
                    # completes, so readers don't need write journals. Read-only
                    # connections also skip the PRAGMA journal_mode transition that
                    # otherwise triggers "database is locked" when multiple worker
                    # processes open the same cache concurrently.
                    try:
                        self._conn = sqlite3.connect(
                            f"file:{self._db_path}?mode=ro",
                            uri=True,
                            check_same_thread=False,
                        )
                        self._conn.execute("PRAGMA mmap_size=268435456")  # 256 MB mmap window
                    except sqlite3.OperationalError as exc:
                        # Diagnostic: capture the on-disk state at the moment of
                        # failure so we can tell whether the file was deleted
                        # between preload and read, never built, or just
                        # permission-locked.
                        import traceback as _tb

                        try:
                            db_dir = os.path.dirname(self._db_path)
                            file_exists = os.path.exists(self._db_path)
                            dir_listing = (
                                sorted(os.listdir(db_dir))
                                if os.path.isdir(db_dir)
                                else "<dir-missing>"
                            )
                        except Exception as _diag_exc:  # noqa: BLE001
                            file_exists = f"<diag-error:{_diag_exc}>"
                            dir_listing = f"<diag-error:{_diag_exc}>"
                        raise sqlite3.OperationalError(
                            f"{exc} | db_path={self._db_path} | "
                            f"exists={file_exists} | pid={os.getpid()} | "
                            f"dir_listing={dir_listing} | "
                            f"caller_stack={_tb.format_stack(limit=8)}"
                        ) from exc
        return self._conn

    def get(self, key: tuple[str, str], default: str | None = None) -> str | None:
        conn = self._ensure_connection()
        row = conn.execute(
            "SELECT fhir_id FROM identifier_cache WHERE system = ? AND value = ?",
            (key[0], key[1]),
        ).fetchone()
        return row[0] if row else default

    def __contains__(self, key: tuple[str, str]) -> bool:
        return self.get(key) is not None

    def __len__(self) -> int:
        if self._len is None:
            conn = self._ensure_connection()
            row = conn.execute("SELECT COUNT(*) FROM identifier_cache").fetchone()
            self._len = row[0] if row else 0
        return self._len

    def keys(self) -> list[tuple[str, str]]:
        """Return every (system, value) key.

        Materialises the full key set in memory. Caches with millions of
        entries should prefer ``len(cache)`` for size or a direct SQL
        query against the underlying file for filtered access.
        """
        conn = self._ensure_connection()
        rows = conn.execute("SELECT system, value FROM identifier_cache").fetchall()
        return [(r[0], r[1]) for r in rows]

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None
            self._len = None

    @staticmethod
    def default_cache_dir() -> str:
        return os.path.join(os.environ.get("DAGSTER_HOME", "."), "cache", "identifier_caches")

    @staticmethod
    def safe_namespace(namespace: str) -> str:
        """Convert a logical namespace to a filesystem-safe filename component.

        Lowercases, then replaces every char outside ``[a-z0-9._-]`` with ``_``.
        Allowlist (rather than denylist) so accidental whitespace, backslashes,
        or shell metachars in a caller-provided name cannot produce surprising
        paths or escape the cache directory.
        """
        return re.sub(r"[^a-z0-9._-]", "_", namespace.lower())

    @staticmethod
    def build_from_dict(data: dict[tuple[str, str], str], db_path: str) -> SharedIdentifierCache:
        """Write a dict cache to SQLite and return a cache reader.

        Thin wrapper over ``build_from_iter`` for callers that already have
        the data materialised as a dict.
        """
        return SharedIdentifierCache.build_from_iter(
            ((k[0], k[1], v) for k, v in data.items()),
            db_path,
        )

    @staticmethod
    def build_from_iter(
        rows: Iterable[tuple[str, str, str]],
        db_path: str,
        batch_size: int = 10_000,
    ) -> SharedIdentifierCache:
        """Stream an iterable of (system, value, id) tuples into SQLite.

        Memory is bounded to ``batch_size`` rows at a time — does NOT build
        an intermediate Python dict. Required when source datasets exceed a
        few million rows.

        Uses a unique temporary filename per build so concurrent workers
        targeting the same ``db_path`` cannot clobber each other's in-progress
        files. ``os.replace()`` is atomic — last writer wins, content is
        equivalent if all workers are rebuilding from the same source state.
        """
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        tmp_path = f"{db_path}.tmp.{uuid.uuid4().hex}"

        conn = sqlite3.connect(tmp_path)
        total = 0
        published = False
        try:
            # MEMORY journal keeps the rollback journal in RAM — no on-disk
            # sidecar files that os.replace() would orphan.
            conn.execute("PRAGMA journal_mode=MEMORY")
            conn.execute("PRAGMA synchronous=OFF")
            conn.execute(
                "CREATE TABLE identifier_cache "
                "(system TEXT NOT NULL, value TEXT NOT NULL, fhir_id TEXT NOT NULL)"
            )

            insert_sql = "INSERT INTO identifier_cache (system, value, fhir_id) VALUES (?, ?, ?)"
            batch: list[tuple[str, str, str]] = []
            for row in rows:
                batch.append(row)
                if len(batch) >= batch_size:
                    conn.executemany(insert_sql, batch)
                    conn.commit()
                    total += len(batch)
                    batch.clear()
            if batch:
                conn.executemany(insert_sql, batch)
                conn.commit()
                total += len(batch)
                batch.clear()

            conn.execute("CREATE INDEX idx_system_value ON identifier_cache (system, value)")
            conn.commit()
            conn.close()
            os.replace(tmp_path, db_path)
            published = True
        finally:
            if not published:
                try:
                    conn.close()
                except Exception:
                    pass
                for suffix in ("", "-wal", "-shm", "-journal"):
                    try:
                        os.remove(tmp_path + suffix)
                    except OSError:
                        pass

        # Remove any sidecar files that a previous generation at this path
        # may have left behind (e.g. from a reader process that was SIGKILL'd).
        for suffix in ("-wal", "-shm", "-journal"):
            p = db_path + suffix
            if os.path.exists(p):
                try:
                    os.remove(p)
                except OSError:
                    pass
        cache = SharedIdentifierCache(db_path)
        cache._len = total
        return cache

    @staticmethod
    def cache_path(
        namespace: str,
        discriminator: frozenset[str],
        cache_dir: str | None = None,
    ) -> str:
        """Deterministic cache file path for a given (namespace, discriminator).

        ``namespace`` is a human-readable key (e.g. "fhir.Patient").
        ``discriminator`` is an arbitrary set of strings that further partition
        the namespace (e.g. the set of identifier systems involved). Different
        discriminators produce different files.
        """
        if cache_dir is None:
            cache_dir = SharedIdentifierCache.default_cache_dir()
        disc_hash = hashlib.md5("|".join(sorted(discriminator)).encode()).hexdigest()[:10]
        safe_ns = SharedIdentifierCache.safe_namespace(namespace)
        return os.path.join(cache_dir, f"{safe_ns}_{disc_hash}.sqlite")

    @staticmethod
    def is_fresh(db_path: str, max_age_seconds: int = 3600) -> bool:
        """Check if a cache file exists and is younger than max_age_seconds."""
        if not os.path.exists(db_path):
            return False
        age = time.time() - os.path.getmtime(db_path)
        return age < max_age_seconds

    @staticmethod
    def delete(db_path: str) -> bool:
        """Remove a cache file and all its SQLite sidecar files.

        Returns True if the main .sqlite file was removed. Idempotent:
        returns False without error if the file didn't exist.
        """
        removed = False
        try:
            os.remove(db_path)
            removed = True
        except FileNotFoundError:
            pass
        for suffix in ("-wal", "-shm", "-journal"):
            try:
                os.remove(db_path + suffix)
            except FileNotFoundError:
                pass
        return removed

    @staticmethod
    def delete_namespace(namespace: str, cache_dir: str | None = None) -> int:
        """Remove all cache DB files for a namespace, across discriminators.

        Lock files are intentionally left in place. Removing an advisory-lock
        file while another process holds it can split future contenders across
        two inode locks.

        Orphan ``.tmp.*`` files from SIGKILLed builds are NOT swept here:
        an in-progress build of a different discriminator under the same
        namespace might own one. Tmp orphans from crashed builds accumulate
        but are harmless (next build allocates a new uuid suffix).
        """
        if cache_dir is None:
            cache_dir = SharedIdentifierCache.default_cache_dir()
        pattern = os.path.join(
            cache_dir, f"{SharedIdentifierCache.safe_namespace(namespace)}_*.sqlite"
        )
        removed = 0
        for db_path in glob.glob(pattern):
            if SharedIdentifierCache.delete(db_path):
                removed += 1
        return removed
