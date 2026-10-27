# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Unit tests for the OS2mo GraphQL transport.

Covers the three things that are easy to get wrong and expensive to discover in
production: that the version pin cannot be bypassed, that the token is fetched
once and refreshed exactly once on a 401, and that a contract error (400) is
not retried while a transient one is.
"""

import json

import pytest
import requests
import responses
from core.os2mo.client import (
    DEFAULT_GRAPHQL_VERSION,
    OS2moClient,
    OS2moConfigurationError,
    build_graphql_url,
)
from dagster import Failure

pytestmark = pytest.mark.unit

BASE_URL = "https://mo.example.dk"
REALM_URL = "https://keycloak.example.dk/auth/realms/mo"
DISCOVERY_URL = f"{REALM_URL}/.well-known/openid-configuration"
TOKEN_URL = f"{REALM_URL}/protocol/openid-connect/token"
GRAPHQL_URL = f"{BASE_URL}/graphql/{DEFAULT_GRAPHQL_VERSION}"

DOCUMENT = "query Ping { org_units { objects { current { uuid } } } }"


def _client(**overrides) -> OS2moClient:
    kwargs = {
        "base_url": BASE_URL,
        "graphql_version": DEFAULT_GRAPHQL_VERSION,
        "realm_url": REALM_URL,
        "client_id": "rsd-client",
        "client_secret": "s3cret",
        "max_retries": 2,
        "retry_backoff_sec": 0.0,
    }
    kwargs.update(overrides)
    return OS2moClient(**kwargs)


def _discovery_response() -> None:
    responses.add(
        responses.GET,
        DISCOVERY_URL,
        json={"token_endpoint": TOKEN_URL, "issuer": REALM_URL},
        status=200,
    )


def _token_response(expires_in: int = 300) -> None:
    """Keycloak discovery + the token it points at.

    Both are registered together because the client cannot reach the token
    endpoint without first learning where it is.
    """
    _discovery_response()
    responses.add(
        responses.POST,
        TOKEN_URL,
        json={"access_token": "tok-1", "expires_in": expires_in},
        status=200,
    )


class TestVersionPin:
    def test_builds_the_pinned_url(self):
        assert build_graphql_url(BASE_URL, "v30") == f"{BASE_URL}/graphql/v30"

    def test_trailing_slash_does_not_double_up(self):
        assert build_graphql_url(f"{BASE_URL}/", "v30") == f"{BASE_URL}/graphql/v30"

    @pytest.mark.parametrize("version", ["", None, "latest", "graphql", "30", "v", "v30/"])
    def test_anything_but_an_explicit_version_is_refused(self, version):
        with pytest.raises(OS2moConfigurationError, match="version pin"):
            build_graphql_url(BASE_URL, version)

    @pytest.mark.parametrize("base_url", ["", None, "NOT SET"])
    def test_unconfigured_base_url_names_the_variable(self, base_url):
        with pytest.raises(OS2moConfigurationError, match="OS2MO_BASE_URL"):
            build_graphql_url(base_url, "v30")


class TestCredentials:
    @responses.activate
    def test_missing_credentials_name_the_variables(self):
        _discovery_response()
        client = _client(client_secret=None, client_id="NOT SET")
        with pytest.raises(OS2moConfigurationError) as excinfo:
            client.execute(DOCUMENT)
        message = str(excinfo.value)
        assert "OS2MO_CLIENT_ID" in message
        assert "OS2MO_CLIENT_SECRET" in message


class TestTokenEndpointDiscovery:
    """The token path is read from Keycloak, not assembled from a convention.

    That convention is exactly what moved: Keycloak 17 dropped the `/auth`
    prefix, so `<host>/auth/realms/mo/...` and `<host>/realms/mo/...` are both
    right depending on the version. A deployment names the realm; the server
    says where its token endpoint is.
    """

    @responses.activate
    def test_the_endpoint_comes_from_the_well_known_document(self):
        _discovery_response()
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "t", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": 1}

        assert responses.calls[0].request.url == DISCOVERY_URL

    @responses.activate
    def test_a_realm_url_with_a_trailing_slash_still_works(self):
        _discovery_response()
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "t", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)

        _client(realm_url=f"{REALM_URL}/").execute(DOCUMENT)

        assert responses.calls[0].request.url == DISCOVERY_URL

    @responses.activate
    def test_discovery_happens_once_and_is_cached(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)

        client = _client()
        client.execute(DOCUMENT)
        client.execute(DOCUMENT)

        discovery_calls = [c for c in responses.calls if c.request.url == DISCOVERY_URL]
        assert len(discovery_calls) == 1

    @responses.activate
    def test_an_unreachable_realm_names_the_variable(self):
        responses.add(responses.GET, DISCOVERY_URL, body=requests.ConnectionError("no route"))

        with pytest.raises(OS2moConfigurationError, match="OS2MO_KEYCLOAK_REALM_URL"):
            _client().execute(DOCUMENT)

    @responses.activate
    def test_a_document_without_a_token_endpoint_says_so(self):
        # The usual mistake: pointing at the Keycloak server root rather than a
        # realm — it answers, just not with what we need.
        responses.add(responses.GET, DISCOVERY_URL, json={"issuer": REALM_URL}, status=200)

        with pytest.raises(OS2moConfigurationError, match="no.*token_endpoint"):
            _client().execute(DOCUMENT)

    def test_no_realm_names_the_variable(self):
        with pytest.raises(OS2moConfigurationError, match="OS2MO_KEYCLOAK_REALM_URL"):
            _client(realm_url=None).execute(DOCUMENT)


class TestExecute:
    @responses.activate
    def test_returns_the_data_object(self):
        _token_response()
        responses.add(
            responses.POST, GRAPHQL_URL, json={"data": {"org_units": {"objects": []}}}, status=200
        )

        assert _client().execute(DOCUMENT) == {"org_units": {"objects": []}}

    @responses.activate
    def test_token_is_fetched_once_and_reused(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)

        client = _client()
        client.execute(DOCUMENT)
        client.execute(DOCUMENT)

        token_calls = [c for c in responses.calls if c.request.url == TOKEN_URL]
        assert len(token_calls) == 1

    @responses.activate
    def test_bearer_token_is_sent(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)

        _client().execute(DOCUMENT)

        graphql_call = responses.calls[-1]
        assert graphql_call.request.headers["Authorization"] == "Bearer tok-1"

    @responses.activate
    def test_401_triggers_exactly_one_refresh(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"errors": ["expired"]}, status=401)
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "tok-2", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": True}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": True}
        assert len([c for c in responses.calls if c.request.url == TOKEN_URL]) == 2

    @responses.activate
    def test_the_refresh_does_not_spend_an_attempt(self):
        # With no retries at all, a 401 must still be refreshed AND re-sent —
        # otherwise the fresh token is fetched and never used.
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={}, status=401)
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "tok-2", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": True}}, status=200)

        assert _client(max_retries=0).execute(DOCUMENT) == {"ok": True}

    @responses.activate
    def test_a_401_on_the_last_attempt_is_still_resent(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={}, status=503)
        responses.add(responses.POST, GRAPHQL_URL, json={}, status=503)
        responses.add(responses.POST, GRAPHQL_URL, json={}, status=401)
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "tok-2", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": True}}, status=200)

        assert _client(max_retries=2).execute(DOCUMENT) == {"ok": True}

    @responses.activate
    def test_repeated_401_fails_instead_of_looping(self):
        _token_response()
        for _ in range(6):
            responses.add(responses.POST, GRAPHQL_URL, json={}, status=401)
            responses.add(responses.POST, TOKEN_URL, json={"access_token": "t", "expires_in": 300})

        with pytest.raises(Failure):
            _client().execute(DOCUMENT)

    @responses.activate
    def test_graphql_errors_are_terminal(self):
        _token_response()
        responses.add(
            responses.POST,
            GRAPHQL_URL,
            json={"errors": [{"message": "unknown field 'parent'"}]},
            status=200,
        )

        with pytest.raises(Failure, match="GraphQL errors"):
            _client().execute(DOCUMENT)

        graphql_calls = [c for c in responses.calls if c.request.url == GRAPHQL_URL]
        assert len(graphql_calls) == 1, "a GraphQL error must not be retried"

    @responses.activate
    def test_400_is_not_retried(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body="bad input", status=400)

        with pytest.raises(Failure, match="400"):
            _client().execute(DOCUMENT)

        assert len([c for c in responses.calls if c.request.url == GRAPHQL_URL]) == 1

    @responses.activate
    def test_503_is_retried_then_succeeds(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body="down", status=503)
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": 1}

    @responses.activate
    def test_retries_are_bounded(self):
        _token_response()
        for _ in range(5):
            responses.add(responses.POST, GRAPHQL_URL, body="down", status=503)

        with pytest.raises(Failure):
            _client().execute(DOCUMENT)

        # max_retries=2 -> three attempts in total, not an unbounded loop.
        assert len([c for c in responses.calls if c.request.url == GRAPHQL_URL]) == 3

    @responses.activate
    def test_transport_errors_are_retried(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body=requests.ConnectionError("reset"))
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": 1}

    @responses.activate
    def test_a_request_that_is_not_retry_safe_fails_on_a_transport_error(self):
        """A lost response says nothing about whether a write landed."""
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body=requests.ConnectionError("reset"))
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        with pytest.raises(Failure, match="not safe to retry"):
            _client().execute(DOCUMENT, retry_safe=False)

        assert len([c for c in responses.calls if c.request.url == GRAPHQL_URL]) == 1

    @responses.activate
    def test_a_request_that_is_not_retry_safe_fails_on_a_503(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body="down", status=503)
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        with pytest.raises(Failure):
            _client().execute(DOCUMENT, retry_safe=False)

        assert len([c for c in responses.calls if c.request.url == GRAPHQL_URL]) == 1


class TestKeycloakRetries:
    """A brief Keycloak outage must not cost the night's delivery."""

    @responses.activate
    def test_a_503_from_discovery_is_retried(self):
        responses.add(responses.GET, DISCOVERY_URL, body="down", status=503)
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": 1}

    @responses.activate
    def test_a_transport_error_on_the_token_is_retried(self):
        _discovery_response()
        responses.add(responses.POST, TOKEN_URL, body=requests.ConnectionError("reset"))
        responses.add(responses.POST, TOKEN_URL, json={"access_token": "tok", "expires_in": 300})
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().execute(DOCUMENT) == {"ok": 1}

    @responses.activate
    def test_a_lasting_keycloak_outage_still_fails(self):
        _discovery_response()
        for _ in range(3):
            responses.add(responses.POST, TOKEN_URL, body="down", status=503)

        with pytest.raises(Failure, match="HTTP 503"):
            _client().execute(DOCUMENT)

    @responses.activate
    def test_a_401_from_keycloak_is_not_retried(self):
        _discovery_response()
        responses.add(responses.POST, TOKEN_URL, json={"error": "invalid_client"}, status=401)

        with pytest.raises(Failure):
            _client().execute(DOCUMENT)
        assert len([c for c in responses.calls if c.request.url == TOKEN_URL]) == 1


class TestUploadFile:
    """The delivery itself: one multipart request per file."""

    @staticmethod
    def _parts(request) -> dict[str, bytes]:
        """Split a multipart body into {part name: content}."""
        content_type = request.headers["Content-Type"]
        assert content_type.startswith("multipart/form-data")
        boundary = content_type.split("boundary=")[1].encode()
        parts = {}
        for chunk in request.body.split(b"--" + boundary):
            if b"name=" not in chunk:
                continue
            head, _, body = chunk.partition(b"\r\n\r\n")
            name = head.split(b'name="')[1].split(b'"')[0].decode()
            # Each part's content is followed by exactly one CRLF before the
            # next boundary.
            parts[name] = body[:-2]
        return parts

    @responses.activate
    def test_the_request_follows_the_multipart_spec(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"upload_file": "ok"}}, status=200)

        _client().upload_file("rsyd_sor_20261006_013000.csv", b"\xef\xbb\xbfCode\r\n1\r\n")

        parts = self._parts(responses.calls[-1].request)
        operations = json.loads(parts["operations"])
        assert "upload_file" in operations["query"]
        assert operations["variables"] == {"file_store": "INSERTS", "file": None, "force": True}
        assert json.loads(parts["map"]) == {"0": ["variables.file"]}
        assert parts["0"] == b"\xef\xbb\xbfCode\r\n1\r\n"

    @responses.activate
    def test_the_file_name_is_sent(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {}}, status=200)

        _client().upload_file("rsyd_okosted_20261006_013000.csv", b"x")

        assert b'filename="rsyd_okosted_20261006_013000.csv"' in responses.calls[-1].request.body

    @responses.activate
    def test_a_forced_upload_is_retried(self):
        # Same bytes under the same name: repeating it leaves the same file.
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body="down", status=503)
        responses.add(responses.POST, GRAPHQL_URL, json={"data": {"ok": 1}}, status=200)

        assert _client().upload_file("rsyd_sor_20261006_013000.csv", b"x") == {"ok": 1}

    @responses.activate
    def test_an_unforced_upload_is_not_retried(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, body="down", status=503)

        with pytest.raises(Failure):
            _client().upload_file("rsyd_sor_20261006_013000.csv", b"x", force=False)

        assert len([c for c in responses.calls if c.request.url == GRAPHQL_URL]) == 1

    @responses.activate
    def test_graphql_errors_fail_the_upload(self):
        _token_response()
        responses.add(responses.POST, GRAPHQL_URL, json={"errors": [{"message": "no"}]}, status=200)

        with pytest.raises(Failure, match="GraphQL errors"):
            _client().upload_file("rsyd_sor_20261006_013000.csv", b"x")


class TestExecuteErrors:
    @responses.activate
    def test_token_failure_does_not_leak_the_secret(self):
        _discovery_response()
        responses.add(responses.POST, TOKEN_URL, json={"error": "invalid_client"}, status=401)

        with pytest.raises(Failure) as excinfo:
            _client().execute(DOCUMENT)

        assert "s3cret" not in str(excinfo.value)


class TestPaginate:
    @responses.activate
    def test_walks_every_page_and_stops_on_a_null_cursor(self):
        _token_response()
        responses.add(
            responses.POST,
            GRAPHQL_URL,
            json={
                "data": {
                    "org_units": {
                        "objects": [{"current": {"uuid": "a"}}],
                        "page_info": {"next_cursor": "c1"},
                    }
                }
            },
            status=200,
        )
        responses.add(
            responses.POST,
            GRAPHQL_URL,
            json={
                "data": {
                    "org_units": {
                        "objects": [{"current": {"uuid": "b"}}],
                        "page_info": {"next_cursor": None},
                    }
                }
            },
            status=200,
        )

        def extract(data):
            units = [o["current"] for o in data["org_units"]["objects"]]
            return units, data["org_units"]["page_info"]["next_cursor"]

        got = list(_client().paginate(DOCUMENT, {"limit": 1}, extract))
        assert [unit["uuid"] for unit in got] == ["a", "b"]


class TestResourceConfiguration:
    """How os2mo_defs() reads the environment.

    The deployment chain (Nomad variable -> watcher -> code-server job) forwards
    every OS2MO_* key unconditionally, as an empty string when it is unseeded.
    os.getenv only falls back on an ABSENT key, so a default behind a key the
    chain always sets is a default that never applies.
    """

    def test_empty_version_falls_back_to_the_pin(self, monkeypatch):
        from core.resources import os2mo_defs

        monkeypatch.setenv("OS2MO_BASE_URL", BASE_URL)
        monkeypatch.setenv("OS2MO_GRAPHQL_VERSION", "")

        resource = os2mo_defs().resources["os2mo"]

        assert resource.graphql_version == DEFAULT_GRAPHQL_VERSION
        assert resource.graphql_url == GRAPHQL_URL

    def test_an_absent_version_also_falls_back(self, monkeypatch):
        from core.resources import os2mo_defs

        monkeypatch.setenv("OS2MO_BASE_URL", BASE_URL)
        monkeypatch.delenv("OS2MO_GRAPHQL_VERSION", raising=False)

        assert os2mo_defs().resources["os2mo"].graphql_version == DEFAULT_GRAPHQL_VERSION

    def test_an_explicit_wrong_version_is_still_rejected(self, monkeypatch):
        # Falling back on empty must not soften the pin itself.
        from core.resources import os2mo_defs

        monkeypatch.setenv("OS2MO_BASE_URL", BASE_URL)
        monkeypatch.setenv("OS2MO_GRAPHQL_VERSION", "latest")

        with pytest.raises(OS2moConfigurationError, match="version pin"):
            _ = os2mo_defs().resources["os2mo"].graphql_url

    def test_it_loads_with_no_os2mo_variables_at_all(self, monkeypatch):
        """The resource builds, and can dry-run, with no OS2MO_* variables set."""
        from core.resources import os2mo_defs

        for key in (
            "OS2MO_ENABLED",
            "OS2MO_BASE_URL",
            "OS2MO_GRAPHQL_VERSION",
            "OS2MO_CLIENT_ID",
            "OS2MO_CLIENT_SECRET",
        ):
            monkeypatch.delenv(key, raising=False)

        resource = os2mo_defs().resources["os2mo"]

        assert resource.client_secret is None
        assert resource.enabled is False
        assert resource.graphql_version == DEFAULT_GRAPHQL_VERSION
        # Unconfigured, and it says which variable is missing.
        with pytest.raises(OS2moConfigurationError, match="OS2MO_BASE_URL"):
            _ = resource.graphql_url

    def test_the_secret_is_still_a_dagster_reference_when_present(self, monkeypatch):
        from core.resources import os2mo_defs

        monkeypatch.setenv("OS2MO_CLIENT_SECRET", "s3cret")

        assert os2mo_defs().resources["os2mo"].client_secret is not None

    def test_an_explicit_version_still_wins(self, monkeypatch):
        from core.resources import os2mo_defs

        monkeypatch.setenv("OS2MO_BASE_URL", BASE_URL)
        monkeypatch.setenv("OS2MO_GRAPHQL_VERSION", "v31")

        assert os2mo_defs().resources["os2mo"].graphql_url == f"{BASE_URL}/graphql/v31"


class TestKillSwitch:
    """OS2MO_ENABLED=false stops every call to OS2mo, reads included."""

    def _resource(self, enabled: bool):
        from core.resources import OS2moResource

        return OS2moResource(
            base_url=BASE_URL,
            realm_url=REALM_URL,
            client_id="rsd-client",
            client_secret="s3cret",
            enabled=enabled,
        )

    def test_the_read_back_is_refused_when_disabled(self):
        with pytest.raises(Failure, match="OS2MO_ENABLED=false"):
            self._resource(enabled=False).paginate(DOCUMENT, {}, lambda data: ([], None))

    def test_the_upload_is_refused_when_disabled(self):
        with pytest.raises(Failure, match="OS2MO_ENABLED=false"):
            self._resource(enabled=False).upload_file("f.csv", b"x")

    @responses.activate
    def test_an_enabled_resource_reads(self):
        _token_response()
        responses.add(
            responses.POST,
            GRAPHQL_URL,
            json={"data": {"org_units": {"objects": []}}},
            status=200,
        )
        resource = self._resource(enabled=True)
        assert list(resource.paginate(DOCUMENT, {}, lambda data: ([], None))) == []
