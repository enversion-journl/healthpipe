# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the identifier lookup indexed-path, SQL-safety regex, and
plan_cache_mode workaround in core.fhir.identifier_lookup and
utils.fhir.index_config.
"""

from __future__ import annotations

import importlib
from unittest.mock import MagicMock, patch

import pytest
from core.resources import FHIRDatabaseResource
from utils.fhir.identifier_systems import _SAFE_IDENTIFIER_SYSTEM_RE

pytestmark = pytest.mark.unit


def _build_resource() -> FHIRDatabaseResource:
    return FHIRDatabaseResource(
        host="localhost",
        port=5432,
        database="aidbox",
        user="postgres",
        password="secret",
    )


def _patch_connection(fetched_rows: list[tuple]):
    """Patch FHIRDatabaseResource.get_connection at the class level so
    cur.fetchall() returns ``fetched_rows``.
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
    return patch.object(FHIRDatabaseResource, "get_connection", get_connection_mock), cur


# ────────────────────────────────────────────────────────────────────────────
# I7 — indexed-path branch in _resolve_identifier_values_direct
# ────────────────────────────────────────────────────────────────────────────


class TestIndexedPathBranch:
    """Verify _resolve_identifier_values_direct uses the indexed query shape
    when (resource_type, identifier_system) is in IDENTIFIER_UNIQUE_INDEX_SET."""

    @patch("core.fhir.identifier_lookup._logger")
    def test_indexed_path_uses_extract_identifier_value(self, mock_logger_fn):
        mock_logger_fn.return_value = MagicMock()
        resource = _build_resource()

        conn_patch, mock_cursor = _patch_connection([("uuid-1", "VAL1")])
        with conn_patch:
            from core.fhir.identifier_lookup import (
                _resolve_identifier_values_direct,
                reset_targeted_lookup_counters,
            )
            from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SPECS

            reset_targeted_lookup_counters()

            spec = next(s for s in IDENTIFIER_UNIQUE_INDEX_SPECS if s[0] != "Patient")
            rt, system = spec

            result = _resolve_identifier_values_direct(
                resource,
                resource_type=rt,
                identifier_system=system,
                values=["VAL1"],
            )

            assert result == {"VAL1": "uuid-1"}

            executed_calls = mock_cursor.execute.call_args_list
            set_local_call = executed_calls[0]
            assert "plan_cache_mode" in str(set_local_call)

            query_call = executed_calls[1]
            query_obj = query_call[0][0]
            query_str = (
                query_obj.as_string(None) if hasattr(query_obj, "as_string") else str(query_obj)
            )
            assert "extract_identifier_value" in query_str
            assert system in query_str

    @patch("core.fhir.identifier_lookup._logger")
    def test_legacy_path_uses_jsonb_array_elements(self, mock_logger_fn):
        """Non-indexed systems fall back to lateral unnest."""
        mock_logger_fn.return_value = MagicMock()
        resource = _build_resource()

        conn_patch, mock_cursor = _patch_connection([("uuid-2", "VAL2")])
        with conn_patch:
            from core.fhir.identifier_lookup import (
                _resolve_identifier_values_direct,
                reset_targeted_lookup_counters,
            )

            reset_targeted_lookup_counters()

            result = _resolve_identifier_values_direct(
                resource,
                resource_type="UnknownResource",
                identifier_system="urn:test:not-indexed",
                values=["VAL2"],
            )

            assert result == {"VAL2": "uuid-2"}

            executed_calls = mock_cursor.execute.call_args_list
            query_call = executed_calls[1]
            query_str = str(query_call[0][0])
            assert "jsonb_array_elements" in query_str
            assert "extract_identifier_value" not in query_str


# ────────────────────────────────────────────────────────────────────────────
# Auto-preload for non-indexed types (regression guard)
# ────────────────────────────────────────────────────────────────────────────


class TestAutoPreloadForNonIndexedTypes:
    """resolve_identifier_values must auto-preload a SharedIdentifierCache the
    first time it sees a non-indexed (resource_type, system) pair, so we don't
    re-scan the underlying table on every batch.
    """

    @patch("core.fhir.identifier_lookup._logger")
    def test_auto_preloads_and_serves_from_cache(self, mock_logger_fn):
        mock_logger_fn.return_value = MagicMock()
        resource = _build_resource()

        # A fake "loaded cache" the preload will publish into _identifier_caches.
        fake_lookup = {("urn:test:not-indexed", "V1"): "uuid-cache-1"}

        def fake_preload(
            self_, resource_type, identifier_systems=None, force_reload=False, use_cache=True
        ):
            cache_key = self_._cache_key(resource_type, identifier_systems)
            self_._identifier_caches[cache_key] = fake_lookup

        preload_mock = MagicMock(side_effect=fake_preload)
        with (
            patch(
                "core.fhir.identifier_lookup.preload_identifier_cache",
                preload_mock,
            ),
            patch(
                "core.fhir.identifier_lookup._loaded_lookup_is_current",
                return_value=True,
            ),
            patch("core.fhir.identifier_lookup._resolve_identifier_values_direct") as direct_mock,
        ):
            result = resource.resolve_identifier_values(
                resource_type="Observation",
                identifier_system="urn:test:not-indexed",
                values=["V1"],
            )

        assert result == {"V1": "uuid-cache-1"}
        preload_mock.assert_called_once()
        # Direct SQL path must NOT run when the auto-preload yields a usable cache.
        direct_mock.assert_not_called()


# ────────────────────────────────────────────────────────────────────────────
# I7 — _SAFE_IDENTIFIER_SYSTEM_RE ValueError path
# ────────────────────────────────────────────────────────────────────────────


class TestSafeIdentifierSystemRE:
    """The regex rejects systems with characters unsafe for SQL-literal interpolation."""

    @pytest.mark.parametrize(
        "system,expected",
        [
            ("urn:dk:cpr", True),
            ("http://sundhedsdatastyrelsen.dk/fhir/CodeSystem/sor-entity-type", True),
            ("urn:ifdb/object_id", True),
            ("urn:system'; DROP TABLE --", False),
            ('urn:system"; DROP TABLE --', False),
            ("urn:sys tem", False),
            ("urn:sys\ntem", False),
            ("", False),
        ],
    )
    def test_regex_matches_safe_systems(self, system, expected):
        assert bool(_SAFE_IDENTIFIER_SYSTEM_RE.match(system)) is expected

    @patch("core.fhir.identifier_lookup._logger")
    def test_indexed_path_raises_on_unsafe_system(self, mock_logger_fn):
        """If a system is in the indexed set but fails the regex, ValueError is raised."""
        mock_logger_fn.return_value = MagicMock()
        resource = _build_resource()

        unsafe_system = "urn:bad'; DROP TABLE"
        fake_index_set = frozenset([("Encounter", unsafe_system)])

        conn_patch, _ = _patch_connection([])
        with (
            conn_patch,
            patch(
                "core.fhir.identifier_lookup.IDENTIFIER_UNIQUE_INDEX_SET",
                fake_index_set,
            ),
        ):
            from core.fhir.identifier_lookup import (
                _resolve_identifier_values_direct,
                reset_targeted_lookup_counters,
            )

            reset_targeted_lookup_counters()

            with pytest.raises(ValueError, match="safe-character check"):
                _resolve_identifier_values_direct(
                    resource,
                    resource_type="Encounter",
                    identifier_system=unsafe_system,
                    values=["V1"],
                )


# ────────────────────────────────────────────────────────────────────────────
# I7 — SET LOCAL plan_cache_mode workaround
# ────────────────────────────────────────────────────────────────────────────


class TestPlanCacheMode:
    @patch("core.fhir.identifier_lookup._logger")
    def test_set_local_plan_cache_mode_precedes_query(self, mock_logger_fn):
        """SET LOCAL plan_cache_mode = force_custom_plan must run before the query."""
        mock_logger_fn.return_value = MagicMock()
        resource = _build_resource()

        conn_patch, mock_cursor = _patch_connection([("uuid-1", "V1")])
        with conn_patch:
            from core.fhir.identifier_lookup import (
                _resolve_identifier_values_direct,
                reset_targeted_lookup_counters,
            )

            reset_targeted_lookup_counters()

            _resolve_identifier_values_direct(
                resource,
                resource_type="UnknownResource",
                identifier_system="urn:test:system",
                values=["V1"],
            )

            executed = mock_cursor.execute.call_args_list
            assert len(executed) >= 2
            first_stmt = str(executed[0])
            assert "force_custom_plan" in first_stmt


# ────────────────────────────────────────────────────────────────────────────
# I8 — import-time _SAFE_IDENTIFIER_SYSTEM_RE guard in index_config.py
# ────────────────────────────────────────────────────────────────────────────


class TestIndexConfigImportTimeValidation:
    """The import-time guard in index_config.py validates every system in
    IDENTIFIER_UNIQUE_INDEX_SPECS against _SAFE_IDENTIFIER_SYSTEM_RE."""

    def test_unsafe_system_raises_at_import_time(self, monkeypatch):
        """Force the import-time guard in index_config.py to fail by
        narrowing _SAFE_IDENTIFIER_SYSTEM_RE so the real configured systems
        no longer match, then reload the module."""
        import re

        from utils.fhir import identifier_systems, index_config

        monkeypatch.setattr(
            identifier_systems,
            "_SAFE_IDENTIFIER_SYSTEM_RE",
            re.compile(r"^__never_matches__$"),
        )
        try:
            with pytest.raises(ValueError, match="unsafe for SQL-literal"):
                importlib.reload(index_config)
        finally:
            monkeypatch.undo()
            importlib.reload(index_config)

    def test_all_configured_specs_pass_validation(self):
        """Every system in the real IDENTIFIER_UNIQUE_INDEX_SPECS must pass."""
        from utils.fhir.index_config import IDENTIFIER_UNIQUE_INDEX_SPECS

        for _rt, _sys in IDENTIFIER_UNIQUE_INDEX_SPECS:
            assert _SAFE_IDENTIFIER_SYSTEM_RE.match(
                _sys
            ), f"System {_sys!r} for {_rt} fails _SAFE_IDENTIFIER_SYSTEM_RE"
