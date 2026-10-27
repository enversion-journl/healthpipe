# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Transport for the OS2mo (Magenta) GraphQL API.

Two things make this different from every other outbound call in the repo, and
both live here rather than in the assets:

1. **Keycloak client credentials.** Every other external system we write to
   uses HTTP Basic (Aidbox) or a PAT (Azure DevOps). OS2mo requires an OAuth2
   client-credentials token with an admin role, so the client owns a token
   cache and a single forced refresh on 401.

   The token endpoint is **discovered**, never configured: a deployment names
   the Keycloak realm, and the client reads ``token_endpoint`` from the realm's
   ``.well-known/openid-configuration``. One fewer path for an operator to get
   right — and the one that actually moved between Keycloak versions, when the
   ``/auth`` prefix was dropped in 17.

2. **A pinned GraphQL version.** The unversioned ``/graphql`` endpoint follows
   whatever version OS2mo currently ships, which would silently change the
   contract under us. :func:`build_graphql_url` therefore refuses anything that
   is not an explicit ``vNN``, so the pin cannot be bypassed from code.

The retry classification mirrors ``FHIRServerResource.post_resource_batches``:
429/5xx and transport errors are retried with exponential backoff, while 400 is
a contract error that is never retried. GraphQL's own ``errors`` array is also
terminal — the server understood us and said no.
"""

import json
import re
import time
from collections.abc import Callable, Iterator
from typing import Any

import dagster as dg
import requests

__all__ = [
    "DEFAULT_FILE_STORE",
    "OS2moClient",
    "OS2moConfigurationError",
    "build_graphql_url",
]

# The pinned OS2mo GraphQL version — the one the contract with Magenta names.
# Overridable per deployment through OS2MO_GRAPHQL_VERSION, but never unversioned.
DEFAULT_GRAPHQL_VERSION = "v30"

_VERSION_RE = re.compile(r"^v\d+$")

# Refresh this many seconds before the token actually expires, so a long
# mutation batch cannot start with a token that dies mid-flight.
TOKEN_EXPIRY_MARGIN_SEC = 60

# Where an uploaded file lands in OS2mo's file storage. INSERTS is the store
# OS2mo's own importers read from; EXPORTS is where OS2mo writes its exports.
DEFAULT_FILE_STORE = "INSERTS"

UPLOAD_FILE = """
mutation Os2moUploadFile($file_store: FileStore!, $file: Upload!, $force: Boolean!) {
  upload_file(file_store: $file_store, file: $file, force: $force)
}
"""

# Statuses worth another attempt: rate limiting and server-side faults.
RETRYABLE_STATUS = (429, 500, 502, 503, 504)

# "NOT SET" is the sentinel the MSSQL/FHIR resources already use for an
# unconfigured deployment; keeping it identical means one recognisable failure.
UNSET = "NOT SET"


class OS2moConfigurationError(RuntimeError):
    """Raised when the OS2mo resource is missing or has invalid configuration.

    A missing Nomad variable should fail with this rather than with a puzzling
    401 from Keycloak, so the error names the variable that has to be set.
    """


def build_graphql_url(base_url: str | None, graphql_version: str | None) -> str:
    """Build the pinned GraphQL endpoint URL.

    Raises:
        OS2moConfigurationError: if the base URL is unset, or the version is
            anything other than an explicit ``vNN`` pin.
    """
    if not base_url or base_url == UNSET:
        raise OS2moConfigurationError(
            "OS2MO_BASE_URL is not configured. Set it as a Nomad variable "
            "(production) or in your .env (devcontainer); it is the OS2mo root "
            "URL, e.g. https://<host> — without the /graphql path."
        )

    version = (graphql_version or "").strip()
    if not _VERSION_RE.match(version):
        raise OS2moConfigurationError(
            f"OS2MO_GRAPHQL_VERSION must be an explicit version pin like "
            f"'{DEFAULT_GRAPHQL_VERSION}', got {version!r}. The unversioned "
            "/graphql endpoint follows whatever OS2mo currently ships and must "
            "never be called from code."
        )

    return f"{base_url.rstrip('/')}/graphql/{version}"


class OS2moClient:
    """A thin, retrying GraphQL client for OS2mo.

    Holds one ``requests.Session`` (connection pooling) and one cached bearer
    token.
    """

    def __init__(
        self,
        base_url: str | None,
        graphql_version: str | None,
        client_id: str | None,
        client_secret: str | None,
        realm_url: str | None = None,
        timeout_sec: int = 120,
        max_retries: int = 3,
        retry_backoff_sec: float = 0.5,
        session: requests.Session | None = None,
    ) -> None:
        self._base_url = base_url
        self._graphql_version = graphql_version
        self._realm_url = realm_url
        self._discovered_token_url: str | None = None
        self._client_id = client_id
        self._client_secret = client_secret
        self._timeout_sec = timeout_sec
        self._max_retries = max_retries
        self._retry_backoff_sec = retry_backoff_sec
        self._session = session or requests.Session()
        self._token: str | None = None
        self._token_expires_at: float = 0.0

    # ------------------------------------------------------------------
    # Configuration
    # ------------------------------------------------------------------

    @property
    def graphql_url(self) -> str:
        """The pinned endpoint. Raises OS2moConfigurationError if unusable."""
        return build_graphql_url(self._base_url, self._graphql_version)

    def _resolve_token_url(self) -> str:
        """The token endpoint, read from Keycloak's own discovery document.

        A deployment names the realm and nothing else. The token path is a
        Keycloak version detail — 17 moved the ``/auth`` prefix — and the server
        publishes it at ``<realm>/.well-known/openid-configuration``, so there
        is no reason for anyone here to know it.

        Discovered once per client and cached: a realm's endpoints do not move
        while a run is in flight.
        """
        if self._discovered_token_url:
            return self._discovered_token_url

        if not self._realm_url or self._realm_url == UNSET:
            raise OS2moConfigurationError(
                "OS2MO_KEYCLOAK_REALM_URL is not configured. It is the Keycloak "
                "realm root, e.g. https://<host>/auth/realms/mo — the token "
                "endpoint is read from its .well-known document."
            )

        discovery_url = f"{self._realm_url.rstrip('/')}/.well-known/openid-configuration"
        try:
            response = self._keycloak_request(
                "discovery", lambda: self._session.get(discovery_url, timeout=self._timeout_sec)
            )
            response.raise_for_status()
            token_endpoint = response.json().get("token_endpoint")
        except requests.RequestException as exc:
            raise OS2moConfigurationError(
                f"Could not read the Keycloak discovery document at {discovery_url}: "
                f"{exc}. Check OS2MO_KEYCLOAK_REALM_URL."
            ) from exc

        if not token_endpoint:
            raise OS2moConfigurationError(
                f"The Keycloak discovery document at {discovery_url} carries no "
                "token_endpoint. Check that the URL is a realm root and not the "
                "Keycloak server root."
            )

        self._discovered_token_url = str(token_endpoint)
        dg.get_dagster_logger().info(
            f"OS2mo: discovered token endpoint {self._discovered_token_url}"
        )
        return self._discovered_token_url

    def _require_credentials(self) -> tuple[str, str, str]:
        """Return (token_url, client_id, client_secret) or fail by name."""
        token_url = self._resolve_token_url()
        missing = [
            name
            for name, value in (
                ("OS2MO_CLIENT_ID", self._client_id),
                ("OS2MO_CLIENT_SECRET", self._client_secret),
            )
            if not value or value == UNSET
        ]
        if missing:
            raise OS2moConfigurationError(
                "OS2mo credentials are not configured: "
                + ", ".join(missing)
                + ". They are Nomad variables in production (seeded from "
                "Bitwarden) and .env entries in the devcontainer."
            )
        # mypy: the guard above proves these are non-empty strings.
        return token_url, str(self._client_id), str(self._client_secret)

    # ------------------------------------------------------------------
    # Authentication
    # ------------------------------------------------------------------

    def _keycloak_request(
        self, label: str, send: Callable[[], requests.Response]
    ) -> requests.Response:
        """Send a Keycloak request with the GraphQL calls' retry policy.

        Both Keycloak calls only read or mint a token, so they are always safe
        to repeat. A transport error on the last attempt is re-raised for the
        caller to classify; a retryable status on it is returned as is.
        """
        backoff = self._retry_backoff_sec
        for attempt in range(self._max_retries + 1):
            last_attempt = attempt >= self._max_retries
            try:
                response = send()
            except requests.RequestException as exc:
                if last_attempt:
                    raise
                dg.get_dagster_logger().warning(
                    f"OS2mo Keycloak {label}: transport error ({exc}); retrying in {backoff:.1f}s"
                )
            else:
                if response.status_code not in RETRYABLE_STATUS or last_attempt:
                    return response
                dg.get_dagster_logger().warning(
                    f"OS2mo Keycloak {label}: HTTP {response.status_code}; "
                    f"retrying in {backoff:.1f}s"
                )
            time.sleep(backoff)
            backoff *= 2
        raise AssertionError("unreachable")  # the loop always returns or raises

    def _fetch_token(self) -> str:
        """Fetch a fresh bearer token via the client-credentials grant."""
        token_url, client_id, client_secret = self._require_credentials()

        response = self._keycloak_request(
            "token",
            lambda: self._session.post(
                token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=self._timeout_sec,
            ),
        )
        if response.status_code >= 400:
            # Deliberately without the body: a Keycloak error response can echo
            # back parts of the request.
            raise dg.Failure(
                description=(
                    f"OS2mo token request failed with HTTP {response.status_code} "
                    f"for client_id {client_id!r}. Check the Keycloak client, its "
                    "secret and that it carries the admin role."
                )
            )

        payload = response.json()
        token = payload.get("access_token")
        if not token:
            raise dg.Failure(description="OS2mo token response contained no access_token.")

        expires_in = float(payload.get("expires_in", 300))
        self._token = str(token)
        self._token_expires_at = time.monotonic() + max(0.0, expires_in - TOKEN_EXPIRY_MARGIN_SEC)
        return self._token

    def _bearer_token(self, force_refresh: bool = False) -> str:
        """Return a valid token, refreshing it when stale or forced."""
        if force_refresh or self._token is None or time.monotonic() >= self._token_expires_at:
            return self._fetch_token()
        return self._token

    # ------------------------------------------------------------------
    # GraphQL
    # ------------------------------------------------------------------

    def _send(
        self,
        post: Callable[[dict[str, str]], requests.Response],
        label: str,
        retry_safe: bool,
    ) -> dict[str, Any]:
        """Send one GraphQL request with auth, retries and error classification.

        ``post`` performs the HTTP call given the auth headers, so a JSON
        request and a multipart upload share one retry policy.

        Raises:
            dagster.Failure: on a GraphQL ``errors`` array, on HTTP 400 (a
                contract error), on a transport error or a retryable status for
                a request that is not ``retry_safe``, or when the retries are
                exhausted.
        """
        logger = dg.get_dagster_logger()

        backoff = self._retry_backoff_sec
        refreshed_once = False
        last_status: int | None = None

        # A while loop, not range(): the one forced token refresh re-sends the
        # request WITHOUT spending an attempt. Otherwise a 401 on the last
        # attempt — or any 401 with max_retries=0 — fetches a fresh token and
        # then gives up without ever using it.
        attempt = 0
        while attempt <= self._max_retries:
            token = self._bearer_token()
            try:
                response = post({"Authorization": f"Bearer {token}", "Accept": "application/json"})
            except requests.RequestException as exc:
                if not retry_safe:
                    raise dg.Failure(
                        description=(
                            f"OS2mo {label} hit a transport error and is not safe to "
                            f"retry, so it is reported as failed: {exc}"
                        )
                    ) from exc
                if attempt >= self._max_retries:
                    raise dg.Failure(
                        description=f"OS2mo {label} failed after {attempt + 1} attempts: {exc}"
                    ) from exc
                logger.warning(
                    f"OS2mo {label}: transport error ({exc}); retrying in {backoff:.1f}s"
                )
                time.sleep(backoff)
                backoff *= 2
                attempt += 1
                continue

            last_status = response.status_code

            if response.status_code == 401 and not refreshed_once:
                # The token may have been revoked or rotated mid-run. One
                # forced refresh, then treat a repeat 401 as a real failure.
                refreshed_once = True
                logger.info(f"OS2mo {label}: got 401, refreshing token once")
                self._bearer_token(force_refresh=True)
                continue

            if (
                retry_safe
                and response.status_code in RETRYABLE_STATUS
                and attempt < self._max_retries
            ):
                logger.warning(
                    f"OS2mo {label}: HTTP {response.status_code}; retrying in {backoff:.1f}s"
                )
                time.sleep(backoff)
                backoff *= 2
                attempt += 1
                continue

            if response.status_code >= 400:
                # 400 means OS2mo understood the request and rejected it —
                # retrying cannot help, and the body names the offending field.
                raise dg.Failure(
                    description=(
                        f"OS2mo {label} failed: HTTP {response.status_code} "
                        f"{response.reason}: {response.text[:2000]}"
                    )
                )

            body = response.json()
            errors = body.get("errors")
            if errors:
                raise dg.Failure(description=f"OS2mo {label} returned GraphQL errors: {errors}")
            return body.get("data") or {}

        raise dg.Failure(
            description=(
                f"OS2mo {label} failed after {self._max_retries + 1} attempts "
                f"(last HTTP status {last_status})."
            )
        )

    def execute(
        self,
        document: str,
        variables: dict[str, Any] | None = None,
        operation: str | None = None,
        retry_safe: bool = True,
    ) -> dict[str, Any]:
        """Run one GraphQL document and return its ``data`` object.

        Args:
            document: The GraphQL query or mutation.
            variables: Variable values for the document.
            operation: A short label used in log and error messages only.
            retry_safe: Whether repeating this document is harmless. A lost
                response — a read timeout, a 504 from a proxy — says nothing
                about whether a write landed, so a write that is not idempotent
                must not be retried.
        """
        url = self.graphql_url
        payload: dict[str, Any] = {"query": document, "variables": variables or {}}

        def post(headers: dict[str, str]) -> requests.Response:
            return self._session.post(
                url,
                json=payload,
                headers={**headers, "Content-Type": "application/json"},
                timeout=self._timeout_sec,
            )

        return self._send(post, operation or "graphql", retry_safe)

    def query(
        self,
        document: str,
        variables: dict[str, Any] | None = None,
        operation: str | None = None,
    ) -> dict[str, Any]:
        """Read-side alias for :meth:`execute` — reads better at the call site."""
        return self.execute(document, variables, operation=operation or "query")

    def upload_file(
        self,
        filename: str,
        content: bytes,
        file_store: str = DEFAULT_FILE_STORE,
        force: bool = True,
    ) -> dict[str, Any]:
        """Upload one file through OS2mo's ``upload_file`` mutation.

        Sent as a GraphQL multipart request (the graphql-multipart-request-spec
        that OS2mo's Strawberry server implements): the ``operations`` part
        carries the mutation with ``file: null``, the ``map`` part points that
        variable at the file part.

        ``force`` overwrites a file of the same name. Each delivered set has
        its own name, so a forced upload only ever overwrites an earlier
        attempt at the SAME set — which is what makes the request safe to
        retry: a timed-out upload that did land is simply replaced by the same
        bytes, rather than failing the retry as "file exists".
        """
        url = self.graphql_url
        operations = json.dumps(
            {
                "query": UPLOAD_FILE,
                "variables": {"file_store": file_store, "file": None, "force": force},
            }
        )
        file_map = json.dumps({"0": ["variables.file"]})

        def post(headers: dict[str, str]) -> requests.Response:
            # No Content-Type header: requests sets multipart/form-data with
            # its boundary itself.
            return self._session.post(
                url,
                data={"operations": operations, "map": file_map},
                files={"0": (filename, content, "text/csv")},
                headers=headers,
                timeout=self._timeout_sec,
            )

        return self._send(post, f"upload_file {filename}", retry_safe=force)

    def paginate(
        self,
        document: str,
        variables: dict[str, Any],
        extract: Callable[[dict[str, Any]], tuple[list[dict[str, Any]], str | None]],
        operation: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield every object across OS2mo's cursor pagination.

        ``extract`` maps one response's ``data`` to ``(objects, next_cursor)``;
        keeping it a callback lets the same loop serve org units, classes and
        anything added later without this module knowing their shapes.
        """
        cursor: str | None = None
        while True:
            page_variables = {**variables, "cursor": cursor}
            data = self.query(document, page_variables, operation=operation)
            objects, cursor = extract(data)
            yield from objects
            if not cursor:
                return
