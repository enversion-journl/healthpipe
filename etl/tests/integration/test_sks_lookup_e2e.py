# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Integration test: hybrid-mode terminology resolution for SKS.

Verifies the core outcome of the FAR-loading work: with
BOX_FHIR_TERMINOLOGY_ENGINE=hybrid, a content-complete CodeSystem PUT is
indexed by Aidbox so that native $lookup / $validate-code resolve directly
from the resource.

Deliberately lightweight: builds a tiny in-memory CodeSystem (no MSSQL
staging, no medinfo.dk network), so it runs against any reachable Aidbox.
Gated only on Aidbox reachability — it does NOT require USE_REAL_DB_FOR_TESTS.

It FAILS (not skips) if the server is not in hybrid mode, because $lookup then
returns an empty bundle (or the PUT 500s) instead of a Parameters resource —
exactly the regression to catch.

Run with:
    TEST_FHIR_SERVER_URL=http://fhir-server:8888 \
        pytest tests/integration/test_sks_lookup_e2e.py -v
"""

import json
import os

import pytest
import requests
from core.resources import FHIRServerResource
from requests.auth import HTTPBasicAuth

pytestmark = pytest.mark.integration

_ITEST_ID = "sks-itest"
_ITEST_SYSTEM = "urn:oid:1.2.208.176.2.4.999"  # test-only OID, never clobbers real sks
_ITEST_CODESYSTEM = {
    "resourceType": "CodeSystem",
    "id": _ITEST_ID,
    "url": _ITEST_SYSTEM,
    "version": "itest",
    "name": "SKS_ITEST",
    "status": "active",
    "content": "complete",
    "hierarchyMeaning": "is-a",
    "publisher": "Example Region",
    "property": [
        {"code": "classification", "type": "string", "description": "SKS classification category"}
    ],
    "concept": [
        {
            "code": "BA",
            "display": "Nervesystemet",
            "concept": [
                {
                    "code": "BAB",
                    "display": "BAB gruppe",
                    "concept": [
                        {
                            "code": "BABD",
                            "display": "Kateteranvendelse ved regionalanaestesi",
                            "property": [{"code": "classification", "valueString": "treatments"}],
                        }
                    ],
                }
            ],
        }
    ],
}


@pytest.fixture(scope="module")
def fhir_server_resource():
    return FHIRServerResource(
        base_url=os.getenv("TEST_FHIR_SERVER_URL")
        or os.getenv("FHIR_SERVER_URL", "http://fhir-server:8888"),
        client_username=os.getenv("AIDBOX_CLIENT_ID", "root"),
        client_password=os.getenv("AIDBOX_CLIENT_SECRET", "secret"),
    )


@pytest.fixture(scope="module")
def aidbox_available(fhir_server_resource):
    try:
        resp = requests.get(
            f"{fhir_server_resource.base_url.rstrip('/')}/health",
            timeout=5,
            auth=HTTPBasicAuth(
                fhir_server_resource.client_username,
                fhir_server_resource.client_password,
            ),
        )
        return resp.status_code == 200
    except (requests.ConnectionError, requests.Timeout):
        return False


@pytest.fixture(scope="module")
def itest_codesystem_loaded(fhir_server_resource, aidbox_available):
    """PUT the tiny test CodeSystem once for the module; delete it on teardown."""
    if not aidbox_available:
        pytest.skip("Aidbox not reachable")

    base = fhir_server_resource.base_url.rstrip("/")
    put_url = f"{base}/fhir/CodeSystem/{_ITEST_ID}"
    resp = fhir_server_resource.session.put(
        put_url,
        data=json.dumps(_ITEST_CODESYSTEM, ensure_ascii=False).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        timeout=120,
    )
    assert resp.status_code in (200, 201), (
        f"PUT {put_url} returned {resp.status_code}: {resp.text[:500]}\n"
        "If 500: the server is likely NOT in BOX_FHIR_TERMINOLOGY_ENGINE=hybrid."
    )
    yield base
    fhir_server_resource.session.delete(put_url, timeout=60)


def _params(resp):
    assert resp.status_code == 200, f"{resp.status_code}: {resp.text[:500]}"
    data = resp.json()
    assert data.get("resourceType") == "Parameters", (
        f"Expected Parameters (got {data!r}). Server is not resolving SKS "
        "locally — check BOX_FHIR_TERMINOLOGY_ENGINE=hybrid."
    )
    return {p["name"]: p for p in data["parameter"]}


class TestHybridTerminologyResolution:
    """$lookup / $validate-code must resolve from the content-complete PUT."""

    def test_lookup_resolves_display(self, fhir_server_resource, itest_codesystem_loaded):
        base = itest_codesystem_loaded
        resp = fhir_server_resource.session.get(
            f"{base}/fhir/CodeSystem/$lookup?system={_ITEST_SYSTEM}&code=BA", timeout=60
        )
        assert _params(resp)["display"]["valueString"] == "Nervesystemet"

    def test_lookup_resolves_nested_leaf(self, fhir_server_resource, itest_codesystem_loaded):
        base = itest_codesystem_loaded
        resp = fhir_server_resource.session.get(
            f"{base}/fhir/CodeSystem/$lookup?system={_ITEST_SYSTEM}&code=BABD", timeout=60
        )
        assert _params(resp)["display"]["valueString"] == "Kateteranvendelse ved regionalanaestesi"

    def test_validate_code_known_true(self, fhir_server_resource, itest_codesystem_loaded):
        base = itest_codesystem_loaded
        resp = fhir_server_resource.session.get(
            f"{base}/fhir/CodeSystem/$validate-code?url={_ITEST_SYSTEM}&code=BAB", timeout=60
        )
        assert _params(resp)["result"]["valueBoolean"] is True

    def test_validate_code_unknown_false(self, fhir_server_resource, itest_codesystem_loaded):
        base = itest_codesystem_loaded
        resp = fhir_server_resource.session.get(
            f"{base}/fhir/CodeSystem/$validate-code?url={_ITEST_SYSTEM}&code=NOPE999", timeout=60
        )
        assert _params(resp)["result"]["valueBoolean"] is False
