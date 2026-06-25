# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Race/interleaving regression tests for identifier cache lifecycle."""

from __future__ import annotations

import os
import sqlite3
import threading
from pathlib import Path

import pytest
from core.cache import CacheCoordinator
from core.resources import FHIRDatabaseResource, SqliteIdentifierCache

_SYSTEM = "urn:test:cache-race:patient"
_VALUE = "P1"


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def _seed_loaded_cache(
    tmp_path: Path,
    resource: FHIRDatabaseResource,
) -> tuple[str, SqliteIdentifierCache]:
    db_path = SqliteIdentifierCache.cache_path(
        "Patient",
        frozenset([_SYSTEM]),
        cache_dir=str(tmp_path),
    )
    SqliteIdentifierCache.build_from_iter(
        iter([(_SYSTEM, _VALUE, "patient-uuid-1")]),
        db_path,
    )
    lookup = SqliteIdentifierCache(db_path)
    cache_key = ("Patient", frozenset([_SYSTEM]))
    resource._identifier_caches[cache_key] = lookup
    resource._identifier_cache = lookup
    resource._active_cache_key = cache_key
    return db_path, lookup


@pytest.mark.parametrize("clear_mode", ["selected", "all"])
def test_suspend_clear_reopen_interleaving_is_stable(
    tmp_path: Path,
    clear_mode: str,
) -> None:
    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    db_path, _lookup = _seed_loaded_cache(tmp_path, resource)
    reader = SqliteIdentifierCache(db_path)
    assert reader.get((_SYSTEM, _VALUE)) == "patient-uuid-1"

    barrier = threading.Barrier(2)
    errors: list[BaseException] = []

    def _reader_worker() -> None:
        try:
            # Simulate chunk boundary: close and reopen lazily later.
            reader.close()
            barrier.wait()
            assert reader.get((_SYSTEM, _VALUE)) == "patient-uuid-1"
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    def _clear_worker() -> None:
        try:
            barrier.wait()
            if clear_mode == "selected":
                resource.clear_identifier_caches_for_resources({"Patient"})
            else:
                resource.clear_identifier_caches()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    t_reader = threading.Thread(target=_reader_worker, name="reader")
    t_clear = threading.Thread(target=_clear_worker, name="clear")
    t_reader.start()
    t_clear.start()
    t_reader.join(timeout=10)
    t_clear.join(timeout=10)

    assert not errors, f"interleaving failed: {errors}"
    assert os.path.exists(db_path), "Runtime clear must not delete cache file."
    marker = tmp_path / "patient.invalidated"
    assert marker.exists()
    assert os.path.getmtime(marker) >= os.path.getmtime(db_path)


def test_explicit_purge_deletes_file_and_reopen_fails(tmp_path: Path) -> None:
    resource = _build_resource()
    resource._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    db_path, _lookup = _seed_loaded_cache(tmp_path, resource)
    reader = SqliteIdentifierCache(db_path)
    assert reader.get((_SYSTEM, _VALUE)) == "patient-uuid-1"

    # Purge is deliberately destructive: next lazy reopen must fail.
    removed = resource.purge_identifier_cache_files({"Patient"})
    assert removed == 1
    assert not os.path.exists(db_path)

    reader.close()
    with pytest.raises(sqlite3.OperationalError):
        reader.get((_SYSTEM, _VALUE))
