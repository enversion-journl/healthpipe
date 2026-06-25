# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Unit tests for FHIRDatabaseResource.clear_identifier_caches.

Regression tests for identifier-cache lifecycle semantics:

- Runtime cleanup (`clear_identifier_caches` and
  `clear_identifier_caches_for_resources`) must be non-destructive:
  close local handles + invalidate namespace markers, but keep files on disk.
- Explicit destructive cleanup is available via
  `purge_identifier_cache_files` for maintenance/test workflows.
"""

import fcntl
import multiprocessing
import os
import time

import pytest
from core.cache import CacheCoordinator
from core.resources import FHIRDatabaseResource, SqliteIdentifierCache


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def test_clear_identifier_caches_invalidates_marker_and_keeps_sqlite_file(tmp_path):
    db_path = str(tmp_path / "patient_test.sqlite")
    cache = SqliteIdentifierCache.build_from_dict({("sys", "v"): "fhir-id-1"}, db_path)
    assert os.path.exists(db_path)

    resource = _build_resource()
    resource._identifier_caches[("Patient", frozenset(["sys"]))] = cache
    resource._identifier_cache = cache
    resource._active_cache_key = ("Patient", frozenset(["sys"]))
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))

    resource.clear_identifier_caches()

    marker_path = tmp_path / "patient.invalidated"
    assert os.path.exists(db_path), "Runtime clear must not unlink cache files."
    assert marker_path.exists()
    assert os.path.getmtime(marker_path) >= os.path.getmtime(db_path)
    assert resource._identifier_cache is None
    assert resource._active_cache_key is None
    assert resource._identifier_caches == {}


def test_clear_identifier_caches_handles_missing_file(tmp_path):
    """Should not raise if file was already removed by another process."""
    db_path = str(tmp_path / "patient_already_gone.sqlite")
    cache = SqliteIdentifierCache.build_from_dict({("sys", "v"): "fid"}, db_path)

    # Delete the file behind the cache's back
    cache.close()
    os.remove(db_path)
    assert not os.path.exists(db_path)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["sys"]))] = cache

    resource.clear_identifier_caches()  # must not raise
    assert (tmp_path / "patient.invalidated").exists()


def test_clear_identifier_caches_invalidates_multiple_namespaces(tmp_path):
    """Multiple caches (different systems) must all be invalidated."""
    path_a = str(tmp_path / "patient_a.sqlite")
    path_b = str(tmp_path / "patient_b.sqlite")
    cache_a = SqliteIdentifierCache.build_from_dict({("s", "1"): "a"}, path_a)
    cache_b = SqliteIdentifierCache.build_from_dict({("s", "2"): "b"}, path_b)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["s_a"]))] = cache_a
    resource._identifier_caches[("Encounter", frozenset(["s_b"]))] = cache_b

    resource.clear_identifier_caches()

    assert os.path.exists(path_a)
    assert os.path.exists(path_b)
    patient_marker = tmp_path / "patient.invalidated"
    encounter_marker = tmp_path / "encounter.invalidated"
    assert patient_marker.exists()
    assert encounter_marker.exists()
    assert os.path.getmtime(patient_marker) >= os.path.getmtime(path_a)
    assert os.path.getmtime(encounter_marker) >= os.path.getmtime(path_b)


def test_purge_identifier_cache_files_removes_selected_namespace(tmp_path):
    patient_path = str(tmp_path / "patient_purge.sqlite")
    encounter_path = str(tmp_path / "encounter_purge.sqlite")
    patient_cache = SqliteIdentifierCache.build_from_dict({("s", "p"): "pat"}, patient_path)
    encounter_cache = SqliteIdentifierCache.build_from_dict({("s", "e"): "enc"}, encounter_path)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["sys:p"]))] = patient_cache
    resource._identifier_caches[("Encounter", frozenset(["sys:e"]))] = encounter_cache
    resource._identifier_cache = patient_cache
    resource._active_cache_key = ("Patient", frozenset(["sys:p"]))

    removed = resource.purge_identifier_cache_files({"Patient"})

    assert removed == 1
    assert not os.path.exists(patient_path)
    assert os.path.exists(encounter_path)
    assert ("Patient", frozenset(["sys:p"])) not in resource._identifier_caches
    assert ("Encounter", frozenset(["sys:e"])) in resource._identifier_caches
    assert resource._identifier_cache is None
    assert resource._active_cache_key is None
    assert (tmp_path / "patient.invalidated").exists()
    assert not (tmp_path / "encounter.invalidated").exists()


def test_purge_identifier_cache_files_without_scope_removes_all_loaded_files(tmp_path):
    patient_path = str(tmp_path / "patient_purge_all.sqlite")
    encounter_path = str(tmp_path / "encounter_purge_all.sqlite")
    patient_cache = SqliteIdentifierCache.build_from_dict({("s", "p"): "pat"}, patient_path)
    encounter_cache = SqliteIdentifierCache.build_from_dict({("s", "e"): "enc"}, encounter_path)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["sys:p"]))] = patient_cache
    resource._identifier_caches[("Encounter", frozenset(["sys:e"]))] = encounter_cache

    removed = resource.purge_identifier_cache_files()

    assert removed == 2
    assert not os.path.exists(patient_path)
    assert not os.path.exists(encounter_path)
    assert resource._identifier_caches == {}
    assert (tmp_path / "patient.invalidated").exists()
    assert (tmp_path / "encounter.invalidated").exists()


def test_clear_identifier_caches_for_resources_invalidates_selected_namespace_only(tmp_path):
    patient_path = str(tmp_path / "patient.sqlite")
    encounter_path = str(tmp_path / "encounter.sqlite")
    patient_cache = SqliteIdentifierCache.build_from_dict(
        {("urn:source:ifdb:patient_id", "P1"): "pat-1"}, patient_path
    )
    encounter_cache = SqliteIdentifierCache.build_from_dict(
        {("urn:source:ifdb:object_id", "E1"): "enc-1"}, encounter_path
    )

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["urn:source:ifdb:patient_id"]))] = (
        patient_cache
    )
    resource._identifier_caches[("Encounter", frozenset(["urn:source:ifdb:object_id"]))] = (
        encounter_cache
    )
    resource._identifier_cache = patient_cache
    resource._active_cache_key = ("Patient", frozenset(["urn:source:ifdb:patient_id"]))

    resource.clear_identifier_caches_for_resources({"Patient"})

    # Patient namespace is invalidated (marker), not physically deleted.
    assert os.path.exists(patient_path)
    assert os.path.exists(encounter_path)
    marker_path = tmp_path / "patient.invalidated"
    assert marker_path.exists()
    assert os.path.getmtime(marker_path) >= os.path.getmtime(patient_path)
    assert not (tmp_path / "encounter.invalidated").exists()
    assert ("Patient", frozenset(["urn:source:ifdb:patient_id"])) not in resource._identifier_caches
    assert ("Encounter", frozenset(["urn:source:ifdb:object_id"])) in resource._identifier_caches
    assert resource._identifier_cache is None
    assert resource._active_cache_key is None


def test_clear_identifier_caches_for_resources_keeps_file_for_suspended_reopen(tmp_path):
    """Regression: invalidating one worker must not delete another worker's DB file.

    The chunked pipelines call ``suspend_identifier_caches()`` between chunks,
    then lazily reopen on next lookup. If another worker invalidates the same
    namespace, reopen must still succeed (marker-based staleness, no unlink).
    """
    system = "urn:source:ifdb:episode_of_care_id"
    db_path = SqliteIdentifierCache.cache_path(
        "EpisodeOfCare",
        frozenset([system]),
        cache_dir=str(tmp_path),
    )
    SqliteIdentifierCache.build_from_iter(
        iter([(system, "EOC-1", "episode-uuid-1")]),
        db_path,
    )

    reader_cache = SqliteIdentifierCache(db_path)
    invalidator_cache = SqliteIdentifierCache(db_path)
    assert reader_cache.get((system, "EOC-1")) == "episode-uuid-1"

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("EpisodeOfCare", frozenset([system]))] = invalidator_cache

    resource.clear_identifier_caches_for_resources({"EpisodeOfCare"})
    reader_cache.close()  # simulate suspend_identifier_caches() between chunks

    assert os.path.exists(db_path), "Invalidation must not delete live cache files."
    assert reader_cache.get((system, "EOC-1")) == "episode-uuid-1"
    marker_path = tmp_path / "episodeofcare.invalidated"
    assert marker_path.exists()
    assert os.path.getmtime(marker_path) >= os.path.getmtime(db_path)


def test_clear_identifier_caches_for_resources_invalidates_unloaded_namespace(tmp_path):
    """Invalidation must mark unloaded namespace files stale even when this
    resource instance never opened them. Stale files age out via the TTL
    check on the next preload — invalidate_namespace deliberately does NOT
    delete files, because other workers may hold open file descriptors
    against the same namespace via a different (resource_type,
    identifier_systems) discriminator (see CacheCoordinator.invalidate_namespace).
    """
    systems_key = frozenset(["urn:source:ifdb:observation_id"])
    db_path = SqliteIdentifierCache.cache_path(
        "Observation",
        systems_key,
        cache_dir=str(tmp_path),
    )
    SqliteIdentifierCache.build_from_iter(
        iter([("urn:source:ifdb:observation_id", "123", "obs-123")]),
        db_path,
    )
    assert os.path.exists(db_path)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))

    resource.clear_identifier_caches_for_resources({"Observation"})

    # Marker is written so the next preload sees the cache as stale and rebuilds.
    marker_path = tmp_path / "observation.invalidated"
    assert marker_path.exists(), (
        "invalidate_namespace must touch the marker so subsequent "
        "is_cache_file_current() checks treat the file as stale."
    )
    # Marker mtime must be >= file mtime so the freshness check fails on next read.
    assert os.path.getmtime(marker_path) >= os.path.getmtime(db_path)


def test_suspend_identifier_caches_closes_connections_without_deleting_files(tmp_path):
    patient_path = str(tmp_path / "patient_suspend.sqlite")
    encounter_path = str(tmp_path / "encounter_suspend.sqlite")
    patient_cache = SqliteIdentifierCache.build_from_dict(
        {("urn:source:ifdb:patient_id", "P1"): "pat-1"}, patient_path
    )
    encounter_cache = SqliteIdentifierCache.build_from_dict(
        {("urn:source:ifdb:object_id", "E1"): "enc-1"}, encounter_path
    )

    # Open read connections so suspend has something to close.
    assert patient_cache.get(("urn:source:ifdb:patient_id", "P1")) == "pat-1"
    assert encounter_cache.get(("urn:source:ifdb:object_id", "E1")) == "enc-1"
    assert patient_cache._conn is not None
    assert encounter_cache._conn is not None

    resource = _build_resource()
    resource._identifier_caches[("Patient", frozenset(["urn:source:ifdb:patient_id"]))] = (
        patient_cache
    )
    resource._identifier_caches[("Encounter", frozenset(["urn:source:ifdb:object_id"]))] = (
        encounter_cache
    )
    resource._identifier_cache = patient_cache
    resource._active_cache_key = ("Patient", frozenset(["urn:source:ifdb:patient_id"]))

    resource.suspend_identifier_caches()

    assert os.path.exists(patient_path)
    assert os.path.exists(encounter_path)
    assert patient_cache._conn is None
    assert encounter_cache._conn is None

    # Lazy reopen still works after suspend.
    assert patient_cache.get(("urn:source:ifdb:patient_id", "P1")) == "pat-1"
    assert patient_cache._conn is not None


def test_get_identifier_cache_returns_loaded_cache_without_db_roundtrip(tmp_path):
    resource = _build_resource()
    systems = ["urn:source:ifdb:patient_id"]
    cache_key = ("Patient", frozenset(systems))
    db_path = str(tmp_path / "patient_accessor.sqlite")
    cache = SqliteIdentifierCache.build_from_dict({(systems[0], "P1"): "pat-1"}, db_path)
    resource._identifier_caches[cache_key] = cache
    # use_cache=True keeps the SQLite-backing semantics this test is asserting
    # against; without it the default opt-out would discard the SQLite cache
    # and rebuild as a dict (different test entirely).
    loaded = resource.get_identifier_cache("Patient", systems, use_cache=True)
    assert loaded is cache


def test_clear_identifier_caches_deduplicates_path(tmp_path):
    """When _identifier_cache points to the same cache as an entry in
    _identifier_caches, close only once and keep file on disk."""
    db_path = str(tmp_path / "patient_shared.sqlite")
    cache = SqliteIdentifierCache.build_from_dict({("s", "v"): "f"}, db_path)

    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    resource._identifier_caches[("Patient", frozenset(["s"]))] = cache
    resource._identifier_cache = cache  # same instance

    resource.clear_identifier_caches()  # must not raise on double-remove

    assert os.path.exists(db_path)
    assert (tmp_path / "patient.invalidated").exists()


def test_build_from_iter_streams_without_intermediate_dict(tmp_path):
    """build_from_iter consumes a generator without materialising a dict."""
    db_path = str(tmp_path / "stream.sqlite")
    consumed: list[int] = []

    def gen():
        for i in range(1000):
            consumed.append(i)
            yield (f"sys-{i}", f"v-{i}", f"fhir-{i}")

    cache = SqliteIdentifierCache.build_from_iter(gen(), db_path, batch_size=250)

    assert len(cache) == 1000
    assert len(consumed) == 1000
    assert cache.get(("sys-0", "v-0")) == "fhir-0"
    assert cache.get(("sys-999", "v-999")) == "fhir-999"
    assert cache.get(("missing", "v")) is None


def test_build_from_iter_ignores_unrelated_leftover_tmp_file(tmp_path):
    """Unique per-build tmp names mean a shared legacy .tmp file from a
    previous (pre-unique-name) crashed build does NOT affect a new build.
    Build still succeeds and produces a valid cache."""
    db_path = str(tmp_path / "recover.sqlite")
    old_tmp = db_path + ".tmp"
    with open(old_tmp, "w") as f:
        f.write("garbage from previous run")

    cache = SqliteIdentifierCache.build_from_iter(iter([("s", "v", "f")]), db_path)

    assert os.path.exists(db_path)
    assert cache.get(("s", "v")) == "f"


def test_build_from_iter_cleans_unique_tmp_file_on_failure(tmp_path):
    """A failed streaming build must not leave a partially-written temp DB."""
    db_path = str(tmp_path / "failed.sqlite")

    def gen():
        yield ("s", "v", "f")
        raise RuntimeError("boom")

    try:
        SqliteIdentifierCache.build_from_iter(gen(), db_path, batch_size=1)
        raise AssertionError("build_from_iter should have propagated generator failure")
    except RuntimeError as exc:
        assert str(exc) == "boom"

    assert not os.path.exists(db_path)
    assert list(tmp_path.glob("failed.sqlite.tmp.*")) == []
    assert list(tmp_path.glob("failed.sqlite.tmp.*-wal")) == []
    assert list(tmp_path.glob("failed.sqlite.tmp.*-shm")) == []
    assert list(tmp_path.glob("failed.sqlite.tmp.*-journal")) == []


def test_parallel_builds_do_not_clobber_each_other(tmp_path):
    """Two+ concurrent builds targeting the same db_path must all complete
    successfully. With a shared tmp_path, worker B's cleanup could delete
    worker A's in-progress file and A would crash on os.replace() with
    FileNotFoundError — which is what motivated switching to per-build
    unique tmp names."""
    import threading

    db_path = str(tmp_path / "parallel.sqlite")
    errors: list[BaseException] = []
    barrier = threading.Barrier(4)

    def _build(idx: int):
        try:
            barrier.wait()
            SqliteIdentifierCache.build_from_iter(
                iter([(f"s{idx}-{i}", f"v{i}", f"f{idx}-{i}") for i in range(500)]),
                db_path,
                batch_size=50,
            )
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=_build, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors, f"Concurrent build failed: {errors}"
    cache = SqliteIdentifierCache(db_path)
    assert len(cache) == 500  # one of the four builds wins the os.replace


def test_build_from_iter_cleans_db_path_sidecars(tmp_path):
    """Orphan SQLite sidecar files at the final db_path (left by a reader
    connection that got SIGKILL'd before closing) must be removed after the
    new build's os.replace(), so the next reader doesn't see stale journal
    state alongside a fresh main DB file."""
    db_path = str(tmp_path / "orphan_wal.sqlite")
    wal = db_path + "-wal"
    shm = db_path + "-shm"
    with open(wal, "wb") as f:
        f.write(b"\x00" * 32)
    with open(shm, "wb") as f:
        f.write(b"\x00" * 32)

    cache = SqliteIdentifierCache.build_from_iter(iter([("s", "v", "f")]), db_path)

    assert not os.path.exists(wal)
    assert not os.path.exists(shm)
    assert cache.get(("s", "v")) == "f"


def test_build_from_iter_table_readable_after_replace(tmp_path):
    """Regression: close of MEMORY-journal build must leave table readable
    at db_path after os.replace — no data-stranded-in-WAL bug."""
    db_path = str(tmp_path / "table_readable.sqlite")
    rows = [(f"s{i}", f"v{i}", f"f{i}") for i in range(5000)]

    cache = SqliteIdentifierCache.build_from_iter(iter(rows), db_path, batch_size=100)
    cache.close()

    # Re-open from scratch (simulates cross-process read)
    fresh = SqliteIdentifierCache(db_path)
    assert len(fresh) == 5000
    assert fresh.get(("s0", "v0")) == "f0"
    assert fresh.get(("s4999", "v4999")) == "f4999"


def test_build_from_iter_handles_empty_iterable(tmp_path):
    db_path = str(tmp_path / "empty.sqlite")
    cache = SqliteIdentifierCache.build_from_iter(iter([]), db_path)
    assert len(cache) == 0
    assert cache.get(("x", "y")) is None


def test_cache_coordinator_discards_build_invalidated_mid_stream(tmp_path):
    """If invalidation races with a cache build, the leader must rebuild from
    source instead of publishing a now-stale snapshot as current.
    """
    coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    calls = 0

    def builder():
        nonlocal calls
        calls += 1
        if calls == 1:
            coordinator.invalidate_namespace("Patient")
            yield ("sys", "v", "stale")
        else:
            yield ("sys", "v", "fresh")

    cache = coordinator.load_or_build(
        namespace="Patient",
        discriminator=frozenset(["sys"]),
        builder_fn=builder,
        ttl_seconds=600,
    )

    assert calls == 2
    assert cache.get(("sys", "v")) == "fresh"


def test_cache_coordinator_caps_invalidation_retries(tmp_path):
    """A caller invalidating in a tight loop must fail loud, not pin the
    worker on a runaway rebuild. The coordinator caps retries (currently 3)
    and raises so the misbehaving caller gets surfaced."""
    coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    calls = 0

    def relentless_invalidator():
        nonlocal calls
        calls += 1
        # Re-invalidate on every build attempt so the leader never wins.
        coordinator.invalidate_namespace("Patient")
        yield ("sys", "v", "row")

    with pytest.raises(RuntimeError, match="invalidated 3 times"):
        coordinator.load_or_build(
            namespace="Patient",
            discriminator=frozenset(["sys"]),
            builder_fn=relentless_invalidator,
            ttl_seconds=600,
        )
    assert calls == 3, f"expected exactly 3 attempts, got {calls}"
    # The final published file remains on disk for suspended readers, but the
    # marker makes it stale so future preload calls refuse to reuse it.
    db_path = SqliteIdentifierCache.cache_path(
        "Patient", frozenset(["sys"]), cache_dir=str(tmp_path)
    )
    assert os.path.exists(db_path)
    assert not coordinator.is_current("Patient", frozenset(["sys"]), ttl_seconds=600)

    reader = SqliteIdentifierCache(db_path)
    assert reader.get(("sys", "v")) == "row"


def test_concurrent_readers_do_not_lock_each_other(tmp_path):
    """Multiple SqliteIdentifierCache readers on the same file must not
    deadlock or raise 'database is locked'. Read-only connection mode
    eliminates the journal-mode transition that used to conflict when
    Dagster subprocesses opened the same cache concurrently."""
    db_path = str(tmp_path / "concurrent.sqlite")
    rows = [(f"s{i}", f"v{i}", f"f{i}") for i in range(100)]
    SqliteIdentifierCache.build_from_iter(iter(rows), db_path)

    readers = [SqliteIdentifierCache(db_path) for _ in range(5)]
    try:
        for i, reader in enumerate(readers):
            assert reader.get((f"s{i}", f"v{i}")) == f"f{i}"
        # All five should be able to iterate interleaved without locking.
        for key_i in range(10):
            for reader in readers:
                assert reader.get((f"s{key_i}", f"v{key_i}")) == f"f{key_i}"
    finally:
        for reader in readers:
            reader.close()


def test_reader_cannot_write(tmp_path):
    """Readers are read-only; attempted writes must fail explicitly rather
    than succeeding silently."""
    import sqlite3 as _sqlite3

    db_path = str(tmp_path / "readonly.sqlite")
    SqliteIdentifierCache.build_from_iter(iter([("s", "v", "f")]), db_path)

    reader = SqliteIdentifierCache(db_path)
    try:
        conn = reader._ensure_connection()
        try:
            conn.execute("INSERT INTO identifier_cache VALUES ('x', 'y', 'z')")
            conn.commit()
            raise AssertionError("Write on read-only connection should have raised")
        except _sqlite3.OperationalError:
            pass  # expected
    finally:
        reader.close()


def test_build_from_dict_still_works(tmp_path):
    """Backwards compatibility: existing callers of build_from_dict work."""
    db_path = str(tmp_path / "compat.sqlite")
    cache = SqliteIdentifierCache.build_from_dict({("s1", "v1"): "f1", ("s2", "v2"): "f2"}, db_path)
    assert len(cache) == 2
    assert cache.get(("s1", "v1")) == "f1"
    assert cache.get(("s2", "v2")) == "f2"


# ===========================================================================
# Build-lock coordination (fcntl.flock)
# ===========================================================================


def _worker_acquire_lock_and_build(
    lock_path: str, db_path: str, worker_id: int, start_barrier_path: str, build_count_path: str
):
    """Helper for cross-process lock test. Simulates the double-checked-locking
    pattern from preload_identifier_cache: acquire lock, re-check freshness,
    build only if still stale."""
    # Wait on a filesystem barrier so all workers race the lock simultaneously.
    while not os.path.exists(start_barrier_path):
        time.sleep(0.01)

    with open(lock_path, "a") as lock_fd:
        fcntl.flock(lock_fd.fileno(), fcntl.LOCK_EX)
        try:
            # Double-check under lock: has another worker already produced?
            if SqliteIdentifierCache.is_fresh(db_path, max_age_seconds=600):
                return  # follower — skip build

            # Leader — build and count our invocation so the test can verify
            # exactly one worker took the leader path.
            SqliteIdentifierCache.build_from_iter(
                iter([(f"s-{worker_id}-{i}", f"v{i}", f"fid-{worker_id}-{i}") for i in range(200)]),
                db_path,
            )
            with open(build_count_path, "a") as counter:
                fcntl.flock(counter.fileno(), fcntl.LOCK_EX)
                counter.write(f"{worker_id}\n")
                counter.flush()
        finally:
            fcntl.flock(lock_fd.fileno(), fcntl.LOCK_UN)


def test_fcntl_lock_serializes_parallel_builds(tmp_path):
    """End-to-end: multiple OS processes race to build the same cache.
    The fcntl lock + double-check must result in exactly ONE build and
    N-1 reuses, regardless of how close together the processes start."""
    db_path = str(tmp_path / "coordinated.sqlite")
    lock_path = db_path + ".build.lock"
    start_barrier = str(tmp_path / "go")
    build_count_file = str(tmp_path / "build_count.txt")
    open(build_count_file, "w").close()

    num_workers = 4
    procs = [
        multiprocessing.Process(
            target=_worker_acquire_lock_and_build,
            args=(lock_path, db_path, i, start_barrier, build_count_file),
        )
        for i in range(num_workers)
    ]
    for p in procs:
        p.start()

    # Release all workers at once.
    open(start_barrier, "w").close()

    for p in procs:
        p.join(timeout=30)
        assert p.exitcode == 0, f"Worker exited with {p.exitcode}"

    # Exactly one worker should have entered the "leader — build" branch.
    with open(build_count_file) as f:
        build_entries = [ln for ln in f.read().splitlines() if ln.strip()]
    assert len(build_entries) == 1, (
        f"Expected exactly 1 leader build, got {len(build_entries)}: "
        f"{build_entries}. Double-checked-locking is not collapsing "
        "concurrent builds into a single one."
    )

    # The resulting cache must be readable.
    cache = SqliteIdentifierCache(db_path)
    assert len(cache) == 200


def test_preload_fast_path_does_not_acquire_lock(tmp_path, monkeypatch):
    """When the cache is already fresh on disk, preload_identifier_cache
    must skip the lock-acquisition path entirely — no lock overhead for
    the common case."""
    # Pre-build a fresh cache file inside tmp_path, then point the
    # resource's coordinator at the same dir so preload_identifier_cache
    # finds it via its own cache_path() lookup.
    resource_type = "TestResourceFast"
    systems_key = frozenset(["sys-fast"])
    db_path = SqliteIdentifierCache.cache_path(resource_type, systems_key, cache_dir=str(tmp_path))
    SqliteIdentifierCache.build_from_iter(iter([("sys-fast", "v1", "fid-1")]), db_path)

    lock_acquired = {"was": False}
    real_flock = fcntl.flock

    def _spying_flock(*args, **kwargs):
        lock_acquired["was"] = True
        return real_flock(*args, **kwargs)

    monkeypatch.setattr(fcntl, "flock", _spying_flock)

    resource = FHIRDatabaseResource(
        host="localhost", port=5432, database="x", user="x", password="x"
    )
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    # This test asserts SQLite-fast-path behavior — must opt in to the
    # shared cache backing or preload would route to the dict path.
    resource.preload_identifier_cache(
        resource_type=resource_type,
        identifier_systems=["sys-fast"],
        use_cache=True,
    )

    assert lock_acquired["was"] is False, (
        "Fast path took the build lock — it should only be acquired when a "
        "build is actually required (stale or missing cache)."
    )
