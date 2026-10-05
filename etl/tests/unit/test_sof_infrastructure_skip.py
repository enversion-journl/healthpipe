# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests: fhir_sof_infrastructure skips unchanged ViewDefinitions.

Measured 2026-08-26: $materialize of questionnaire_response_view spent
1.511s — 25 minutes — queued behind the concurrent SOF extract's open
cursor on the same view. Re-creating an identical plain VIEW changes
nothing, so unchanged definitions (with the sof view present) are skipped.
"""

from types import SimpleNamespace

import pytest
from DSA.FHIR.utils.sql_on_fhir.sof_utils import server_view_definition_matches

pytestmark = pytest.mark.unit


class FakeResponse:
    def __init__(self, ok=True, payload=None):
        self.ok = ok
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeServer:
    def __init__(self, response):
        self.base_url = "http://fhir/"
        self.session = SimpleNamespace(get=lambda url, headers=None, timeout=None: response)


VIEW_DEF = {
    "resourceType": "ViewDefinition",
    "name": "patient_view",
    "resource": "Patient",
    "status": "active",
    "select": [{"column": [{"name": "patient_id", "path": "id"}]}],
}


class TestServerViewDefinitionMatches:
    def test_identical_definition_matches(self):
        server_copy = {**VIEW_DEF, "id": "patient_view", "meta": {"versionId": "9"}}
        assert server_view_definition_matches(
            FakeServer(FakeResponse(payload=server_copy)), VIEW_DEF
        )

    def test_changed_select_does_not_match(self):
        server_copy = {**VIEW_DEF, "select": [{"column": [{"name": "x", "path": "id"}]}]}
        assert not server_view_definition_matches(
            FakeServer(FakeResponse(payload=server_copy)), VIEW_DEF
        )

    def test_missing_resource_does_not_match(self):
        assert not server_view_definition_matches(FakeServer(FakeResponse(ok=False)), VIEW_DEF)

    def test_network_error_does_not_match(self):
        class Boom:
            base_url = "http://fhir/"
            session = SimpleNamespace(
                get=lambda url, headers=None, timeout=None: (_ for _ in ()).throw(OSError("net"))
            )

        assert not server_view_definition_matches(Boom(), VIEW_DEF)


class TestInfrastructureAssetSkipFlow:
    def _run(self, monkeypatch, *, existing_views, matches):
        import importlib

        infra = importlib.import_module("infrastructure.assets.fhir_sof_infrastructure")

        ensured = []
        monkeypatch.setattr(infra, "load_view_definitions", lambda: [VIEW_DEF])
        monkeypatch.setattr(infra, "existing_sof_view_names", lambda db: existing_views)
        monkeypatch.setattr(
            infra, "server_view_definition_matches", lambda srv, vd, ctx=None: matches
        )
        monkeypatch.setattr(
            infra, "ensure_view_definition", lambda srv, vd, ctx=None: ensured.append(vd["name"])
        )

        asset_fn = infra.fhir_sof_infrastructure
        compute_fn = asset_fn.op.compute_fn
        fn = getattr(compute_fn, "decorated_fn", compute_fn)
        ctx = SimpleNamespace(log=SimpleNamespace(info=lambda m: None))
        result = fn(ctx, SimpleNamespace(), SimpleNamespace())
        return ensured, result

    def test_unchanged_and_existing_is_skipped(self, monkeypatch):
        ensured, result = self._run(monkeypatch, existing_views={"patient_view"}, matches=True)
        assert ensured == []
        meta = {k: getattr(v, "value", v) for k, v in result.metadata.items()}
        assert meta["skipped_unchanged_count"] == 1
        assert meta["materialized_count"] == 0

    def test_changed_definition_is_materialized(self, monkeypatch):
        ensured, _ = self._run(monkeypatch, existing_views={"patient_view"}, matches=False)
        assert ensured == ["patient_view"]

    def test_missing_sof_view_is_materialized_even_if_definition_matches(self, monkeypatch):
        ensured, _ = self._run(monkeypatch, existing_views=set(), matches=True)
        assert ensured == ["patient_view"]
