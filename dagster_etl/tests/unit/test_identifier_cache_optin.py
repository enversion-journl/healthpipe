# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the use_cache opt-in flag on FHIRDatabaseResource.

The identifier registry supports two backings:
- Per-process Python dict (``use_cache=False``, default) — historic
  pre-shared-cache path.
- Shared SQLite + mmap (``use_cache=True``) — for parallel-worker memory
  sharing during heavy cross-reference resolution.

These tests verify both the build path and the in-memory short-circuit
behavior, including the type-mismatch rebuild when a caller flips the
flag between calls.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from core import resources as core_resources
from core.cache import CacheCoordinator
from core.cache.sqlite_cache import SharedIdentifierCache
from core.resources import FHIRDatabaseResource


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


@pytest.fixture
def resource(tmp_path) -> FHIRDatabaseResource:
    res = _build_resource()
    # Isolate cache files in a temp dir so SQLite tests don't pollute the
    # default $DAGSTER_HOME cache.
    res._cache_coordinator = CacheCoordinator(cache_dir=str(tmp_path))
    return res


def _patch_connection(fetched_rows: list[tuple]):
    """Patch FHIRDatabaseResource.get_connection at the class level so
    cur.fetchall() returns ``fetched_rows``. Class-level patch is required
    because the resource is a frozen pydantic model — instance attribute
    assignment is rejected.
    """
    cur = MagicMock()
    cur.fetchall.return_value = fetched_rows
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cur
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)

    get_connection_mock = MagicMock(return_value=conn)
    return (
        patch.object(FHIRDatabaseResource, "get_connection", get_connection_mock),
        get_connection_mock,
    )


class TestDictPathBuild:
    """preload_identifier_cache(use_cache=False) builds a Python dict."""

    def test_builds_plain_dict_when_use_cache_false(self, resource):
        rows = [
            ("fhir-uuid-1", "urn:source:ifdb:patient_id", "P001"),
            ("fhir-uuid-2", "urn:source:ifdb:patient_id", "P002"),
        ]
        ctx, _ = _patch_connection(rows)
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["urn:source:ifdb:patient_id"],
                use_cache=False,
            )

        cache_key = ("Patient", frozenset(["urn:source:ifdb:patient_id"]))
        cached = resource._identifier_caches[cache_key]

        assert isinstance(cached, dict), (
            "use_cache=False must store a plain Python dict, not a " "SharedIdentifierCache."
        )
        assert not isinstance(cached, SharedIdentifierCache)
        assert cached[("urn:source:ifdb:patient_id", "P001")] == "fhir-uuid-1"
        assert cached[("urn:source:ifdb:patient_id", "P002")] == "fhir-uuid-2"

    def test_dict_satisfies_identifier_lookup_protocol(self, resource):
        rows = [("fhir-uuid-1", "sys-a", "v-1")]
        ctx, _ = _patch_connection(rows)
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys-a"],
                use_cache=False,
            )

        cached = resource._identifier_caches[("Patient", frozenset(["sys-a"]))]
        # IdentifierLookup protocol contract: .get((system, value)) and len()
        assert cached.get(("sys-a", "v-1")) == "fhir-uuid-1"
        assert cached.get(("sys-a", "missing")) is None
        assert cached.get(("sys-a", "missing"), "default") == "default"
        assert len(cached) == 1

    def test_skips_rows_with_null_system_or_value(self, resource):
        rows = [
            ("fhir-1", "sys", "v-1"),
            ("fhir-2", None, "v-2"),
            ("fhir-3", "sys", None),
            ("fhir-4", "sys", "v-4"),
        ]
        ctx, _ = _patch_connection(rows)
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )

        cached = resource._identifier_caches[("Patient", frozenset(["sys"]))]
        assert len(cached) == 2
        assert cached[("sys", "v-1")] == "fhir-1"
        assert cached[("sys", "v-4")] == "fhir-4"


class TestDictPathReuse:
    """Repeat calls within the same worker reuse the cached dict."""

    def test_second_call_reuses_dict_without_db_query(self, resource):
        rows = [("fhir-1", "sys", "v-1")]
        ctx, get_conn = _patch_connection(rows)
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )
            assert get_conn.call_count == 1

            # Second call — must not re-query
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )
            assert get_conn.call_count == 1, (
                "Repeat preload with same (resource_type, systems) and "
                "use_cache=False must reuse the in-memory dict, not re-query."
            )

    def test_force_reload_rebuilds_dict(self, resource):
        rows = [("fhir-1", "sys", "v-1")]
        ctx, get_conn = _patch_connection(rows)
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )
            assert get_conn.call_count == 1

            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
                force_reload=True,
            )
            assert get_conn.call_count == 2


class TestBackingChange:
    """Flipping use_cache between calls rebuilds with the requested backing."""

    def test_dict_to_sqlite_rebuilds(self, resource):
        # First call builds dict
        ctx, _ = _patch_connection([("fhir-1", "sys", "v-1")])
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )
        cache_key = ("Patient", frozenset(["sys"]))
        assert isinstance(resource._identifier_caches[cache_key], dict)

        # Patch the SQLite path so it returns a synthetic SharedIdentifierCache
        fake_sqlite = MagicMock(spec=SharedIdentifierCache)
        fake_sqlite.__len__ = MagicMock(return_value=1)
        with patch.object(resource.cache_coordinator, "load_or_build", return_value=fake_sqlite):
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=True,
            )

        assert resource._identifier_caches[cache_key] is fake_sqlite, (
            "Switching use_cache False→True must replace the dict with the " "SQLite-backed cache."
        )

    def test_sqlite_to_dict_rebuilds(self, resource):
        cache_key = ("Patient", frozenset(["sys"]))
        # Simulate a previously loaded SharedIdentifierCache
        fake_sqlite = MagicMock(spec=SharedIdentifierCache)
        fake_sqlite.__len__ = MagicMock(return_value=1)
        fake_sqlite.close = MagicMock()
        resource._identifier_caches[cache_key] = fake_sqlite
        resource._identifier_cache = fake_sqlite
        resource._active_cache_key = cache_key

        # Now caller asks for dict
        ctx, _ = _patch_connection([("fhir-1", "sys", "v-1")])
        with ctx:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=False,
            )

        cached = resource._identifier_caches[cache_key]
        assert isinstance(cached, dict)
        assert not isinstance(cached, SharedIdentifierCache)
        fake_sqlite.close.assert_called_once_with()


class TestClearHandlesBoth:
    """clear_identifier_caches_for_resources is type-safe across backings."""

    def test_clear_removes_dict_entry_without_filesystem_calls(self, resource, tmp_path):
        cache_key = ("Patient", frozenset(["sys"]))
        resource._identifier_caches[cache_key] = {("sys", "v"): "fhir-1"}
        resource._identifier_cache = resource._identifier_caches[cache_key]
        resource._active_cache_key = cache_key

        resource.clear_identifier_caches_for_resources({"Patient"})

        assert cache_key not in resource._identifier_caches
        assert resource._identifier_cache is None
        assert resource._active_cache_key is None
        # Namespace marker still gets touched — it's the cross-process signal.
        assert os.path.exists(tmp_path / "patient.invalidated")


class TestIdentifierLookupFacade:
    """Canonical lookup facade hides backend details from production callers."""

    def test_malformed_identifier_cache_ttl_falls_back_to_default(self, resource, monkeypatch):
        monkeypatch.setenv("IDENTIFIER_CACHE_TTL_SECONDS", "not-an-int")
        fake_sqlite = MagicMock(spec=SharedIdentifierCache)

        with patch.object(
            resource.cache_coordinator,
            "load_or_build",
            return_value=fake_sqlite,
        ) as load_or_build:
            resource.preload_identifier_cache(
                resource_type="Patient",
                identifier_systems=["sys"],
                use_cache=True,
            )

        assert load_or_build.call_args.kwargs["ttl_seconds"] == 3600

    def test_resolve_identifier_values_uses_canonical_lookup(self, resource, tmp_path):
        # NOTE: combos in IDENTIFIER_UNIQUE_INDEX_SET (e.g. Patient +
        # urn:source:ifdb:patient_id) intentionally BYPASS the in-process cache
        # and always hit live Postgres via the indexed direct path. The
        # canonical-cache facade is only exercised for NON-indexed combos, so
        # this test uses one.
        system = "urn:test:nonindexed-alias"
        cache = SharedIdentifierCache.build_from_dict(
            {
                (system, "P1"): "patient-uuid-1",
                (system, "P2"): "patient-uuid-2",
            },
            str(tmp_path / "patient_values.sqlite"),
        )
        resource._identifier_caches[("Patient", frozenset([system]))] = cache

        resolved = resource.resolve_identifier_values(
            resource_type="Patient",
            identifier_system=system,
            values=["P1", "P2", "missing"],
        )

        assert resolved == {
            "P1": "patient-uuid-1",
            "P2": "patient-uuid-2",
        }

    def test_resolve_identifier_values_direct_uses_established_identifier_scan_query(
        self,
        resource,
    ):
        system = "urn:source:ifdb:object_id"
        cur = MagicMock()
        cur.fetchall.return_value = [("encounter-uuid-1", "E1")]
        cur_cm = MagicMock()
        cur_cm.__enter__.return_value = cur
        cur_cm.__exit__.return_value = False
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        conn.cursor.return_value = cur_cm

        with patch.object(FHIRDatabaseResource, "get_connection", return_value=conn):
            resolved = resource.resolve_identifier_values(
                resource_type="Encounter",
                identifier_system=system,
                values=["E1", "E2"],
            )

        assert resolved == {"E1": "encounter-uuid-1"}
        query, params = cur.execute.call_args.args
        query_str = str(query)
        assert "extract_identifier_value" in query_str
        assert "identifier" in query_str
        assert "ANY" in query_str

    def test_resolve_resource_ids_uses_requested_key_not_active_cache(self, resource, tmp_path):
        patient_system = "urn:source:ifdb:patient_id"
        encounter_system = "urn:source:ifdb:object_id"
        patient_cache = SharedIdentifierCache.build_from_dict(
            {(patient_system, "P1"): "patient-uuid"},
            str(tmp_path / "patient_active.sqlite"),
        )
        patient_key = ("Patient", frozenset([patient_system]))
        resource._identifier_caches[patient_key] = patient_cache
        resource._identifier_cache = patient_cache
        resource._active_cache_key = patient_key

        cur = MagicMock()
        cur.fetchall.return_value = [("encounter-uuid", "E1")]
        cur_cm = MagicMock()
        cur_cm.__enter__.return_value = cur
        cur_cm.__exit__.return_value = False
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        conn.cursor.return_value = cur_cm

        resources = [
            SimpleNamespace(
                identifier=[
                    SimpleNamespace(system=encounter_system, value="E1"),
                ],
            )
        ]

        with patch.object(FHIRDatabaseResource, "get_connection", return_value=conn):
            resolved = resource.resolve_resource_ids(
                resources=resources,
                resource_type="Encounter",
                preferred_systems=[encounter_system],
            )

        assert resolved == {0: "encounter-uuid"}

    def test_get_identifier_lookups_returns_requested_lookups(self, resource):
        calls: list[tuple[str, list[str] | None, bool]] = []

        def fake_get_identifier_lookup(
            self,
            resource_type: str,
            identifier_systems: list[str] | None = None,
            force_reload: bool = False,
        ):
            calls.append((resource_type, identifier_systems, force_reload))
            return {(identifier_systems or [""])[0], "v"}  # satisfies dict-like return

        with patch.object(
            FHIRDatabaseResource,
            "get_identifier_lookup",
            fake_get_identifier_lookup,
        ):
            lookups = resource.get_identifier_lookups(
                [
                    ("Patient", ["patient-system"]),
                    ("Encounter", ["encounter-system"]),
                ],
                force_reload=True,
                parallel=False,
            )

        assert set(lookups) == {"Patient", "Encounter"}
        assert calls == [
            ("Patient", ["patient-system"], True),
            ("Encounter", ["encounter-system"], True),
        ]

    def test_get_identifier_lookups_rejects_duplicate_resource_types(self, resource):
        with pytest.raises(ValueError, match="duplicate resource types"):
            resource.get_identifier_lookups(
                [
                    ("Patient", ["system-a"]),
                    ("Patient", ["system-b"]),
                ]
            )

    def test_safe_clear_identifier_caches_converts_failures_to_false(self, resource):
        with patch.object(
            FHIRDatabaseResource,
            "clear_identifier_caches_for_resources",
            side_effect=RuntimeError("boom"),
        ):
            assert (
                resource.safe_clear_identifier_caches_for_resources(
                    {"Patient"},
                    reason="unit test",
                )
                is False
            )

    def test_log_identifier_lookup_status_is_safe_for_dict_and_sqlite(
        self, resource, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("FHIR_IDENTIFIER_CACHE_DIAG", "1")
        cache = SharedIdentifierCache.build_from_dict(
            {("sys", "v"): "fhir-1"},
            str(tmp_path / "diag.sqlite"),
        )

        resource.log_identifier_lookup_status(
            "unit-test",
            {
                "Patient": {("sys", "v"): "fhir-1"},
                "Observation": cache,
            },
        )

    def test_missing_mssql_python_fails_only_when_connection_is_used(self):
        if core_resources.MSSQL_PYTHON_AVAILABLE:
            pytest.skip("mssql_python is installed in this environment")

        mssql_resource = core_resources.MSSQLResource(
            host="localhost",
            port=1433,
            database="db",
            user="user",
            password="pw",
        )
        with pytest.raises(RuntimeError, match="mssql_python is required"):
            with mssql_resource.get_connection():
                pass
