# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Core resource definitions for database connections.

This module consolidates all resource definitions (database connections)
that were previously scattered in the Connections/ folder.
"""

import json
import os
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dagster as dg
import oracledb
import pymssql
import requests
from core.cache import CacheCoordinator, SharedIdentifierCache
from core.fhir import direct_upsert as _direct_upsert
from core.fhir import identifier_lookup as _identifier_lookup
from core.fhir.aidbox_json import (
    transform_references_for_aidbox as _transform_references_for_aidbox,
)
from core.parallel import env_int
from dotenv import load_dotenv

# Try to import psycopg for PostgreSQL support
try:
    import psycopg

    PSYCOPG_AVAILABLE = True
except ImportError:
    PSYCOPG_AVAILABLE = False

BATCH_UPSERT_FALLBACK_ERROR: type[BaseException] = (
    psycopg.Error if PSYCOPG_AVAILABLE else RuntimeError
)

# Load environment variables with override=True to ensure latest values are used
# This is important when running dagster dev as environment may be cached
# Try to load from project root .env first, then from tests/config/test.env
project_root = Path(__file__).parent.parent.parent
env_files = [
    project_root / ".env",
    project_root / "tests" / "config" / "test.env",
]

for env_file in env_files:
    if env_file.exists():
        load_dotenv(env_file, override=True)
        break
else:
    # Fallback to default behavior if no .env file found
    load_dotenv(override=True)

# Use FHIR R4B (4.3) model classes
from fhir.resources.R4B.resource import Resource
from requests.auth import HTTPBasicAuth
from utils.fhir.identifier_index_names import sanitize_system_for_identifier

try:
    import mssql_python

    MSSQL_PYTHON_AVAILABLE = True
except ImportError:
    mssql_python = None
    MSSQL_PYTHON_AVAILABLE = False


def _connect_mssql_python(conn_str: str, timeout: int = 0, login_timeout: int = 60):
    """Connect via mssql-python.

    ``timeout`` is the *query* timeout applied to every cursor on the resulting
    connection (the driver stores it on ``Connection.timeout`` despite the
    misleading top-level docstring). ``login_timeout`` is passed through
    ``attrs_before={SQL_ATTR_LOGIN_TIMEOUT: ...}`` so a dead network fails fast
    during connect instead of waiting for the full query timeout.
    """
    if mssql_python is None:
        raise RuntimeError(
            "mssql_python is required for MSSQLResource mssql-python connections. "
            "Install the driver or use a code path that does not require MSSQL."
        )
    return mssql_python.connect(
        conn_str,
        timeout=timeout,
        attrs_before={mssql_python.SQL_ATTR_LOGIN_TIMEOUT: login_timeout},
    )


def build_mssql_conn_str(host, port, database, user, password) -> str:
    """Canonical mssql-python connection string for all MSSQL resources.

    Resolves a Dagster-secret ``password`` to its plain value so the string can
    be pickled to a spawn worker (same-host IPC only).
    """
    if hasattr(password, "get_value"):
        password = password.get_value()
    return (
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={password};"
        f"Encrypt=yes;TrustServerCertificate=yes;"
    )


# ============================================================================
# MSSQL Resources
# ============================================================================


class MSSQLResource(dg.ConfigurableResource):
    """
    A Dagster resource for connecting to Microsoft SQL Server.

    Uses mssql-python for connections.
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    def _build_conn_str(self) -> str:
        return build_mssql_conn_str(self.host, self.port, self.database, self.user, self.password)

    @contextmanager
    def get_connection(self, timeout: int = 0):
        """Yield a mssql-python connection; close on exit.

        The ``mssql-python`` driver exposes a ``cursor.bulkcopy()`` API that
        maps to SQL Server's high-performance TDS bulk-insert protocol, so this
        connection is also bulk-copy capable.

        ``timeout=0`` means no query timeout (the default). Staging queries
        operate on multi-million-row ext tables and routinely exceed fixed
        ceilings; the Dagster op-level timeout is the real safety net.
        """
        conn = _connect_mssql_python(self._build_conn_str(), timeout=timeout)
        try:
            yield conn
        finally:
            conn.close()

    def get_raw_connection(self, timeout: int = 0):
        """Raw (non-contextmanager) connection via the same path as
        get_connection(). Caller owns the lifecycle and must close() it — for
        reconnect-on-retry loops that must survive an exception without
        auto-closing (which would drop a session-scoped temp table).
        """
        return _connect_mssql_python(self._build_conn_str(), timeout=timeout)


class MySupplyMSSQLResource(dg.ConfigurableResource):
    """A Dagster resource for connecting to the mySupply Microsoft SQL Server database.

    Uses mssql-python for extraction.
    """

    host: str = dg.EnvVar("MYSUPPLY_PROD_MSSQL_SERVER").get_value()
    port: int = int(dg.EnvVar("MYSUPPLY_PROD_MSSQL_PORT").get_value())
    database: str = dg.EnvVar("MYSUPPLY_PROD_MSSQL_DATABASE").get_value()
    user: str = dg.EnvVar("MYSUPPLY_PROD_MSSQL_USERNAME").get_value()
    password: str = dg.EnvVar("MYSUPPLY_PROD_MSSQL_PASSWORD")

    @contextmanager
    def get_connection(self):
        """Context manager yielding a database connection."""
        dg.get_dagster_logger().info("Using mssql-python for MySupplyMSSQLResource")
        conn_str = build_mssql_conn_str(
            self.host, self.port, self.database, self.user, self.password
        )
        conn = _connect_mssql_python(conn_str, timeout=6000)
        try:
            yield conn
        finally:
            conn.close()


class SydaxMSSQLResource(dg.ConfigurableResource):
    """A Dagster resource for connecting to the SYDAX Microsoft SQL Server database.

    Uses mssql-python for extraction.
    """

    host: str = dg.EnvVar("SYDAX_PROD_MSSQL_SERVER").get_value()
    port: int = int(dg.EnvVar("SYDAX_PROD_MSSQL_PORT").get_value())
    database: str = dg.EnvVar("SYDAX_PROD_MSSQL_DATABASE").get_value()
    user: str = dg.EnvVar("SYDAX_PROD_MSSQL_USERNAME").get_value()
    password: str = dg.EnvVar("SYDAX_PROD_MSSQL_PASSWORD")

    @contextmanager
    def get_connection(self):
        """A context manager that yields a database connection and closes it when exiting."""
        dg.get_dagster_logger().info("Using mssql-python for SydaxMSSQLResource")
        conn_str = build_mssql_conn_str(
            self.host, self.port, self.database, self.user, self.password
        )
        conn = _connect_mssql_python(conn_str, timeout=6000)
        try:
            yield conn
        finally:
            conn.close()


class ILSMSSQLResource(dg.ConfigurableResource):
    """A Dagster resource for connecting to the ILS Microsoft SQL Server database."""

    host: str = dg.EnvVar("ILS_PROD_MSSQL_SERVER").get_value()
    port: int = int(dg.EnvVar("ILS_PROD_MSSQL_PORT").get_value())
    database: str = dg.EnvVar("ILS_PROD_MSSQL_DATABASE").get_value()
    user: str = dg.EnvVar("ILS_PROD_MSSQL_USERNAME").get_value()
    password: str = dg.EnvVar("ILS_PROD_MSSQL_PASSWORD")

    @contextmanager
    def get_connection(self):
        """A context manager that yields a pymssql database connection and closes it when exiting."""
        password = self.password
        if hasattr(password, "get_value"):
            password = password.get_value()
        conn = pymssql.connect(
            server=self.host,
            port=self.port,
            user=self.user,
            password=password,
            database=self.database,
        )
        try:
            yield conn
        finally:
            conn.close()


class KmdUddataMSSQLResource(dg.ConfigurableResource):
    """A Dagster resource for connecting to the KMD Spend UdData Azure Synapse database.

    Uses mssql-python for extraction. The Synapse serverless pool has a cold-start
    latency; the existing ERP retry policy (max_retries=2, delay=30) at the asset
    level handles transient connection failures.
    """

    host: str = os.environ.get("SPEND_KMD_MSSQL_SERVER", "")
    port: int = int(os.environ.get("SPEND_KMD_MSSQL_PORT", "1433"))
    database: str = os.environ.get("SPEND_KMD_MSSQL_DATABASE", "")
    user: str = os.environ.get("SPEND_KMD_MSSQL_USER", "")
    password: str = dg.EnvVar("SPEND_KMD_MSSQL_PASSWORD")

    @contextmanager
    def get_connection(self):
        """Context manager yielding a database connection."""
        dg.get_dagster_logger().info("Using mssql-python for KmdUddataMSSQLResource")
        conn_str = build_mssql_conn_str(
            self.host, self.port, self.database, self.user, self.password
        )
        conn = _connect_mssql_python(conn_str, timeout=6000)
        try:
            yield conn
        finally:
            conn.close()


# ============================================================================
# Oracle Resources
# ============================================================================


def _ifdb_oracle_output_type_handler(cursor, metadata):
    """Connection-wide fetch bindings for IFDB Oracle.

    Two concerns in one handler (a cursor-level handler would *replace* it,
    so everything lives here):

    - Strings decode from WE8MSWIN1252 with ``encoding_errors="replace"`` so a
      stray undecodable byte degrades to U+FFFD instead of failing the fetch.
    - CLOB/NCLOB (and BLOB) fetch inline instead of as LOB locators; a locator
      costs one extra network round trip per row for the ``LOB.read()``, which
      dominates fetch time on XML-bearing tables. Inline fetch caps a value at
      1 GB — far above anything in IFDB; ``_convert_oracle_value`` still reads
      any LOB that slips through.
    """
    if metadata.type_code in (
        oracledb.DB_TYPE_VARCHAR,
        oracledb.DB_TYPE_CHAR,
        oracledb.DB_TYPE_LONG,
        oracledb.DB_TYPE_NVARCHAR,
        oracledb.DB_TYPE_NCHAR,
    ):
        return cursor.var(str, arraysize=cursor.arraysize, encoding_errors="replace")
    if metadata.type_code in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(
            oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize, encoding_errors="replace"
        )
    if metadata.type_code is oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    return None


def _epji_oracle_output_type_handler(cursor, metadata):
    """Connection-wide fetch bindings for EPJI Oracle.

    Same two concerns as ``_ifdb_oracle_output_type_handler`` (a cursor-level
    handler would *replace* it, so everything lives here):

    - Strings decode from WE8ISO8859P1 (EPJI's NLS_CHARACTERSET, confirmed
      against prod-epjsyd — distinct from IFDB's WE8MSWIN1252, though the two
      agree on the Danish letters æ/ø/å) with ``encoding_errors="replace"`` so
      a stray undecodable byte degrades to U+FFFD instead of failing the fetch.
    - CLOB/NCLOB (and BLOB) fetch inline instead of as LOB locators, avoiding
      a per-row round trip for ``LOB.read()``.
    """
    if metadata.type_code in (
        oracledb.DB_TYPE_VARCHAR,
        oracledb.DB_TYPE_CHAR,
        oracledb.DB_TYPE_LONG,
        oracledb.DB_TYPE_NVARCHAR,
        oracledb.DB_TYPE_NCHAR,
    ):
        return cursor.var(str, arraysize=cursor.arraysize, encoding_errors="replace")
    if metadata.type_code in (oracledb.DB_TYPE_CLOB, oracledb.DB_TYPE_NCLOB):
        return cursor.var(
            oracledb.DB_TYPE_LONG, arraysize=cursor.arraysize, encoding_errors="replace"
        )
    if metadata.type_code is oracledb.DB_TYPE_BLOB:
        return cursor.var(oracledb.DB_TYPE_LONG_RAW, arraysize=cursor.arraysize)
    return None


class IFDBOracleResource(dg.ConfigurableResource):
    """
    A Dagster resource for connecting to the IFDB Oracle database.

    It is configured using environment variables, which are loaded from a .env file.

    Encoding: Uses UTF-8 encoding to properly handle Danish characters (æ, ø, å).
    """

    host: str = dg.EnvVar("IFDB_PROD_ORACLE_SERVER").get_value()
    port: int = int(dg.EnvVar("IFDB_PROD_ORACLE_PORT").get_value())
    database: str = dg.EnvVar("IFDB_PROD_ORACLE_DATABASE").get_value()
    user: str = dg.EnvVar("IFDB_PROD_ORACLE_USERNAME").get_value()
    password: str = dg.EnvVar("IFDB_PROD_ORACLE_PASSWORD")
    schema_name: str = dg.EnvVar("IFDB_PROD_ORACLE_SCHEMA").get_value()

    if "ext" in user.lower() and '"' not in user:
        user = f'"{user}"'

    @contextmanager
    def get_connection(self):
        """
        A context manager that yields an oracledb database connection and closes it when exiting.
        Sets the current schema for the session.

        Encoding: Uses output type handler to properly decode Danish characters (æ, ø, å).
        The IFDB database uses WE8MSWIN1252 (Windows-1252) character set.
        """
        dsn = oracledb.makedsn(self.host, self.port, service_name=self.database)
        conn = oracledb.connect(user=self.user, password=self.password, dsn=dsn)
        conn.outputtypehandler = _ifdb_oracle_output_type_handler

        try:
            # Set the current schema for the session
            cursor = conn.cursor()
            try:
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.schema_name}")
            finally:
                cursor.close()
            yield conn
        finally:
            conn.close()


class EPJIOracleResource(dg.ConfigurableResource):
    """EPJI DWH (location + location_identifier), Oracle. Service accounts: svc-healthpipepov-dwh-*.

    location_identifier has no update timestamp; callers must full-snapshot the
    mapping each run so in-place SOR identifier changes are picked up.
    """

    host: str = dg.EnvVar("EPJI_ORACLE_SERVER")
    port: int = dg.EnvVar.int("EPJI_ORACLE_PORT")
    database: str = dg.EnvVar("EPJI_ORACLE_DATABASE")
    user: str = dg.EnvVar("EPJI_ORACLE_USERNAME")
    password: str = dg.EnvVar("EPJI_ORACLE_PASSWORD")
    schema_name: str = dg.EnvVar("EPJI_ORACLE_SCHEMA")

    @contextmanager
    def get_connection(self):
        """Yield an oracledb connection to the EPJI DWH and close it on exit.

        Encoding: output type handler decodes WE8ISO8859P1 free text (e.g.
        LOCATION.NAME) so Danish characters (æ, ø, å) survive the fetch.
        """
        user = self.user if '"' in self.user else f'"{self.user}"'
        dsn = oracledb.makedsn(self.host, self.port, service_name=self.database)
        conn = oracledb.connect(user=user, password=self.password, dsn=dsn)
        conn.outputtypehandler = _epji_oracle_output_type_handler

        try:
            cursor = conn.cursor()
            try:
                cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA = {self.schema_name}")
            finally:
                cursor.close()
            yield conn
        finally:
            conn.close()


# ============================================================================
# FHIR Server Resource
# ============================================================================


def save_failing_bundle_to_disk(
    chunk_idx: int,
    bundle: dict[str, Any],
    response_text: str,
    response_status: int,
) -> str | None:
    """Save a failing FHIR bundle + server response to disk for local repro.

    Opt-in only: does nothing unless ``FHIR_DEBUG_SAVE_FAILING_BUNDLE=1``, so it
    never runs in production (where the bundle would be unredacted patient PII on
    the compute host). When enabled, the files are intentionally NOT masked — their
    sole purpose is to reproduce the exact FHIR validation error locally. The
    target directory is ``FHIR_FAILURES_DIR`` (default ``/tmp/fhir_failures``).

    Returns the bundle path, or ``None`` when disabled.
    """
    if os.environ.get("FHIR_DEBUG_SAVE_FAILING_BUNDLE") != "1":
        return None

    from datetime import datetime

    logger = dg.get_dagster_logger()
    failures_dir = os.environ.get("FHIR_FAILURES_DIR", "/tmp/fhir_failures")
    os.makedirs(failures_dir, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_name = f"{timestamp}_chunk{chunk_idx + 1}"

    bundle_path = os.path.join(failures_dir, f"{base_name}_bundle.json")
    try:
        with open(bundle_path, "w") as f:
            json.dump(bundle, f, indent=2)
        logger.info(f"Saved failing bundle to: {bundle_path}")
    except Exception as e:
        logger.warning(f"Could not save bundle: {e}")

    response_path = os.path.join(failures_dir, f"{base_name}_response.json")
    try:
        try:
            response_json = json.loads(response_text)
            response_data = {"status_code": response_status, "response": response_json}
        except json.JSONDecodeError:
            response_data = {"status_code": response_status, "response_text": response_text}

        with open(response_path, "w") as f:
            json.dump(response_data, f, indent=2)
        logger.info(f"Saved FHIR error response to: {response_path}")
    except Exception as e:
        logger.warning(f"Could not save response: {e}")

    logger.info(
        f"\n{'=' * 60}\n"
        f"FHIR VALIDATION ERROR - Files saved for reproduction:\n"
        f"  Bundle: {bundle_path}\n"
        f"  Response: {response_path}\n"
        f"\nTo reproduce locally, run:\n"
        f"  python -m pytest etl/tests/integration/fhir_ifdb/test_fhir_validation.py "
        f"--bundle-file={bundle_path}\n"
        f"{'=' * 60}"
    )
    return bundle_path


class FHIRServerResource(dg.ConfigurableResource):
    """
    A Dagster resource for interacting with a FHIR server.
    Uses connection pooling via requests.Session for better performance.
    """

    base_url: str
    client_username: str | None = None
    client_password: str | None = None
    use_fhir_api: str | None = None

    def __init__(self, **data):
        """Initialize resource and set up session for connection pooling."""
        super().__init__(**data)
        self._session: requests.Session | None = None

    @property
    def session(self) -> requests.Session:
        """
        Get or create a requests.Session for connection pooling.
        Session reuses TCP connections across requests for better performance.
        """
        if self._session is None:
            self._session = requests.Session()
            # Set up authentication on session if credentials provided
            if self.client_username and self.client_password:
                self._session.auth = HTTPBasicAuth(self.client_username, self.client_password)
        return self._session

    @property
    def _use_fhir_api_bool(self) -> bool:
        """Convert USE_FHIR_API string to boolean."""
        if self.use_fhir_api is None:
            return False
        return str(self.use_fhir_api).lower() in ("true", "1", "yes", "on")

    @property
    def effective_base_url(self) -> str:
        """Returns the base URL with /fhir appended if USE_FHIR_API is true.

        Raises:
            ValueError: If base_url is not configured (e.g., "NOT SET")
        """
        if self.base_url == "NOT SET" or not self.base_url:
            raise ValueError(
                "FHIR_SERVER_URL is not configured. "
                "Set FHIR_SERVER_URL in your .env; the local devcontainer reaches "
                "the Aidbox 'fhir-server' compose service at http://fhir-server:8888."
            )
        if self._use_fhir_api_bool:
            return f"{self.base_url.rstrip('/')}/fhir"
        return self.base_url

    def _save_failing_bundle(
        self,
        chunk_idx: int,
        bundle: dict[str, Any],
        response_text: str,
        response_status: int,
    ) -> None:
        """
        Save a failing FHIR bundle to disk for local debugging and test reproduction.

        Delegates to :func:`save_failing_bundle_to_disk`, which is opt-in via
        ``FHIR_DEBUG_SAVE_FAILING_BUNDLE=1`` and a no-op otherwise (so unredacted
        patient data is never written to disk in production).

        Args:
            chunk_idx: The chunk index that failed
            bundle: The FHIR bundle dict that was sent
            response_text: The raw response text from the server
            response_status: The HTTP status code
        """
        save_failing_bundle_to_disk(chunk_idx, bundle, response_text, response_status)

    def post_resource(self, resource: Resource) -> requests.Response:
        """
        Posts a FHIR resource to the server, creating or updating it.
        Uses a PUT request with the resource's ID to ensure idempotency.
        """
        resource_type = resource.get_resource_type()
        has_id = bool(resource.id)
        url = (
            f"{self.effective_base_url}/{resource_type}/{resource.id}"
            if has_id
            else f"{self.effective_base_url}/{resource_type}"
        )

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        dg.get_dagster_logger().info(f"Posting {resource_type} with id {resource.id} to {url}")

        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        try:
            # Serialize with exclude_none to ensure Reference objects only contain
            # the field that was set (reference OR identifier, not both)
            resource_json = resource.json(exclude_none=True)
            if has_id:
                response = requests.put(url, data=resource_json, headers=headers, auth=auth)
            else:
                response = requests.post(url, data=resource_json, headers=headers, auth=auth)

            # Raise an exception for bad status codes (4xx or 5xx)
            response.raise_for_status()

            # For newly created resources (POST), capture the FHIR-generated ID from response
            if not has_id and response.status_code == 201:
                try:
                    response_data = response.json()
                    if isinstance(response_data, dict) and response_data.get("id"):
                        # Set the FHIR-generated ID on the resource object
                        resource.id = response_data["id"]
                        dg.get_dagster_logger().info(
                            f"Resource created with FHIR ID: {resource.id}"
                        )
                except Exception as e:
                    dg.get_dagster_logger().warning(
                        f"Could not parse created resource ID from response: {e}"
                    )

        except requests.HTTPError:
            # Surface OperationOutcome or server message for easier debugging
            try:
                body = response.text
            except Exception:
                body = "<no-body>"
            dg.get_dagster_logger().error(
                f"FHIR request failed: {response.status_code} {response.reason}; body: {body}"
            )
            raise

        return response

    def resolve_id_by_identifier(
        self,
        resource_type: str,
        identifier_system: str,
        identifier_value: str,
    ) -> str | None:
        """
        Search the FHIR server for a resource of type resource_type by identifier system|value.
        Returns the resource id if found, else None.
        """
        url = f"{self.effective_base_url}/{resource_type}"
        params = {
            "identifier": f"{identifier_system}|{identifier_value}",
            "_count": "1",
            "_elements": "id",
        }
        headers = {"Accept": "application/json"}

        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        resp = requests.get(url, params=params, headers=headers, auth=auth)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            dg.get_dagster_logger().error(
                f"FHIR search failed {resp.status_code} {resp.reason}: {resp.text}"
            )
            raise

        data = resp.json()
        if isinstance(data, dict) and data.get("resourceType") == "Bundle":
            entries = data.get("entry") or []
            if entries:
                first = entries[0].get("resource", {})
                return first.get("id")
        return None

    def search_resources(
        self,
        resource_type: str,
        search_params: dict[str, str] | None = None,
        count_only: bool = False,
    ) -> list[dict] | int:
        """
        Search for FHIR resources of the specified type.

        Args:
            resource_type: The FHIR resource type (e.g., "Patient", "Observation")
            search_params: Dictionary of search parameters
            count_only: If True, return only the count of matching resources

        Returns:
            If count_only is False: List of resource dictionaries
            If count_only is True: Integer count of matching resources
        """
        url = f"{self.effective_base_url}/{resource_type}"
        params = search_params or {}

        if count_only:
            params["_count"] = "0"  # Don't return any resources, just count
            params["_summary"] = "count"

        headers = {"Accept": "application/json"}

        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        resp = requests.get(url, params=params, headers=headers, auth=auth)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            dg.get_dagster_logger().error(
                f"FHIR search failed {resp.status_code} {resp.reason}: {resp.text}"
            )
            raise

        data = resp.json()
        if isinstance(data, dict) and data.get("resourceType") == "Bundle":
            if count_only:
                return data.get("total", 0)
            else:
                entries = data.get("entry") or []
                return [entry.get("resource", {}) for entry in entries]
        return [] if not count_only else 0

    def attach_id_from_identifiers(
        self,
        resource: Resource,
        preferred_systems: list[str] | None = None,
    ) -> Resource:
        """
        Given a FHIR resource instance with one or more identifiers, try to resolve an
        existing server id using the first matching identifier system in preferred_systems.

        If an id is found, sets resource.id and returns the resource; otherwise returns as-is.
        """
        try:
            identifiers = getattr(resource, "identifier", None)
        except Exception:
            identifiers = None

        if not identifiers:
            return resource

        # Build a list of (system, value) pairs from the resource identifiers
        system_to_value: dict[str, Any] = {}
        for ident in identifiers:
            try:
                system = getattr(ident, "system", None) or ident.get("system")  # type: ignore[attr-defined]
                value = getattr(ident, "value", None) or ident.get("value")  # type: ignore[attr-defined]
            except Exception:
                system = None
                value = None
            if system and value:
                system_to_value[str(system)] = str(value)

        if not system_to_value:
            return resource

        # Determine resource type from instance
        resource_type = resource.get_resource_type()

        systems_order: list[str] = preferred_systems or list(system_to_value.keys())
        for system in systems_order:
            if system in system_to_value:
                found_id = self.resolve_id_by_identifier(
                    resource_type=resource_type,
                    identifier_system=system,
                    identifier_value=system_to_value[system],
                )
                if found_id:
                    # Set id on the given resource and return
                    try:
                        resource.id = found_id
                    except Exception:
                        # If the model is frozen, reconstruct from dict
                        payload = resource.dict()
                        payload["id"] = found_id
                        resource = resource.__class__(**payload)
                    break

        return resource

    def delete_resource(self, resource_type: str, resource_id: str) -> requests.Response:
        """
        Deletes a FHIR resource from the server by resource type and ID.
        """
        url = f"{self.effective_base_url}/{resource_type}/{resource_id}"

        headers = {
            "Accept": "application/json",
        }

        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        dg.get_dagster_logger().info(f"Deleting {resource_type} with id {resource_id} from {url}")

        try:
            response = requests.delete(url, headers=headers, auth=auth)
            # For delete operations, 200, 202, or 204 are typically success codes
            # Some servers return 404 if resource doesn't exist, which we can treat as success
            if response.status_code not in (200, 202, 204, 404):
                response.raise_for_status()
        except requests.HTTPError:
            # Surface OperationOutcome or server message for easier debugging
            try:
                body = response.text
            except Exception:
                body = "<no-body>"
            dg.get_dagster_logger().error(
                f"FHIR delete failed: {response.status_code} {response.reason}; body: {body}"
            )
            raise

        return response

    def get_all_resources(self, resource_type: str, batch_size: int = 100) -> list[dict[str, Any]]:
        """
        Retrieves all resources of a given type from the FHIR server.
        Uses pagination to get all resources.
        """
        resources = []
        url = f"{self.effective_base_url}/{resource_type}"
        params = {
            "_count": str(batch_size),
            "_elements": "id,meta",
        }

        headers = {"Accept": "application/json"}

        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        while url:
            resp = requests.get(
                url,
                params=params if url == f"{self.effective_base_url}/{resource_type}" else None,
                headers=headers,
                auth=auth,
            )
            try:
                resp.raise_for_status()
            except requests.HTTPError:
                dg.get_dagster_logger().error(
                    f"FHIR search failed {resp.status_code} {resp.reason}: {resp.text}"
                )
                break

            data = resp.json()
            if isinstance(data, dict) and data.get("resourceType") == "Bundle":
                entries = data.get("entry") or []
                resources.extend([entry.get("resource", {}) for entry in entries])

                # Check for pagination - try multiple formats
                next_url = None

                # First, try HTTP Link header (standard FHIR)
                link_header = resp.headers.get("link", "")
                if 'rel="next"' in link_header:
                    import re

                    next_match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
                    if next_match:
                        next_url = next_match.group(1)

                # If not found in header, try JSON link array (Aidbox format)
                if not next_url and isinstance(data, dict):
                    links = data.get("link", [])
                    if isinstance(links, list):
                        for link in links:
                            if isinstance(link, dict) and link.get("relation") == "next":
                                next_url = link.get("url")
                                break

                url = next_url
                params = {}  # Clear params for subsequent requests
            else:
                break

        return resources

    def post_resource_batches(
        self,
        resources: Sequence[Resource | dict[str, Any]],
        chunk_size: int = 400,
        bundle_type: str = "transaction",
        max_isolation_level: str | None = None,
        max_retries: int = 3,
        retry_backoff_sec: float = 0.5,
        max_workers: int = 8,
    ) -> list[requests.Response]:
        """
        Posts FHIR resources to the server in batches using FHIR Bundles.

        This method efficiently updates the FHIR server in bulk by sending resources
        in chunks as FHIR Transaction Bundles (atomic operations). Multiple chunks
        can be processed in parallel for better performance.

        Args:
            resources: Sequence of FHIR resources (Resource objects or dicts)
            chunk_size: Number of resources per bundle (default 200)
            bundle_type: FHIR bundle type, defaults to "transaction" for atomicity
            max_isolation_level: Optional isolation level header to reduce conflicts
            max_retries: Maximum retry attempts per chunk on transient failures
            retry_backoff_sec: Backoff time between retries in seconds
            max_workers: Number of parallel workers for batch processing (default 4)

        Returns:
            List of HTTP responses, one per chunk submitted

        Raises:
            requests.HTTPError: If a chunk fails all retry attempts
        """
        if not resources:
            return []

        # Chunk the resources
        chunks = [resources[i : i + chunk_size] for i in range(0, len(resources), chunk_size)]

        dg.get_dagster_logger().info(
            f"Posting {len(resources)} resources in {len(chunks)} chunks "
            f"(chunk_size={chunk_size}, bundle_type={bundle_type}, max_workers={max_workers})"
        )

        url = self.effective_base_url

        # Set up authentication
        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        # Set up base headers
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Add isolation level header if specified
        if max_isolation_level:
            headers["x-max-isolation-level"] = max_isolation_level

        # Define function to process a single chunk
        def process_chunk(
            chunk_idx: int, chunk: Sequence[Resource | dict[str, Any]]
        ) -> requests.Response:
            """Process a single chunk and return the response."""
            overall_start = time.perf_counter()

            dg.get_dagster_logger().info(
                f"Processing chunk {chunk_idx + 1}/{len(chunks)} with {len(chunk)} resources"
            )

            # Build FHIR Bundle
            start_time = time.perf_counter()
            bundle = {
                "resourceType": "Bundle",
                "type": bundle_type,
                "entry": [],
            }

            for resource in chunk:
                # Handle both Resource objects and dicts
                if isinstance(resource, Resource):
                    # Use json() and then parse back to dict to ensure proper serialization
                    resource_dict = json.loads(resource.json(exclude_none=True))
                    resource_type = resource.get_resource_type()
                    resource_id = getattr(resource, "id", None)
                elif isinstance(resource, dict):
                    resource_dict = resource
                    resource_type = resource.get("resourceType")
                    resource_id = resource.get("id")
                else:
                    raise ValueError(f"Unsupported resource type: {type(resource)}")

                if not resource_type:
                    raise ValueError("Resource must have a resourceType")

                # Transform references from {id, type} to {id, resourceType} for Aidbox
                # The FHIR library serializes Reference.type as 'type', but Aidbox requires
                # 'resourceType' for internal references to enable UI navigation
                _transform_references_for_aidbox(resource_dict)

                # Build request entry
                # Note: FHIR transaction bundle URLs should NOT have leading slashes
                # They are relative to the bundle's base URL
                if resource_id:
                    # Update existing resource
                    request = {
                        "method": "PUT",
                        "url": f"{resource_type}/{resource_id}",
                    }
                else:
                    # Create new resource
                    request = {
                        "method": "POST",
                        "url": f"{resource_type}",
                    }

                bundle["entry"].append(
                    {
                        "resource": resource_dict,
                        "request": request,
                    }
                )

            build_time = (time.perf_counter() - start_time) * 1000

            # Log first entry for debugging. Opt-in only (FHIR_DEBUG_LOG_BUNDLE=1)
            # and PII-masked: bundle entries carry patient CPR/name/address.
            if (
                chunk_idx == 0
                and bundle["entry"]
                and os.environ.get("FHIR_DEBUG_LOG_BUNDLE") == "1"
            ):
                from DSA.FHIR.utils.gdpr_masking import mask_fhir_resource_for_logging

                masked_entry = mask_fhir_resource_for_logging(bundle["entry"][0])
                dg.get_dagster_logger().debug(
                    f"First bundle entry (masked, for debugging):\n{masked_entry}"
                )

            # Serialize bundle
            start_time = time.perf_counter()
            json_data = json.dumps(bundle)
            serialize_time = (time.perf_counter() - start_time) * 1000
            payload_size_kb = len(json_data) / 1024

            # Submit chunk with retries
            response = None
            last_error = None
            chunk_retry_backoff = retry_backoff_sec  # Local copy for this chunk

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    dg.get_dagster_logger().info(
                        f"Submitting chunk {chunk_idx + 1}, attempt {attempt + 1}"
                    )

                    # Use session for connection pooling
                    start_time = time.perf_counter()
                    response = self.session.post(
                        url,
                        data=json_data,
                        headers=headers,
                        auth=auth,
                    )
                    http_time = (time.perf_counter() - start_time) * 1000

                    # Check for retryable errors (conflicts, rate limits, server errors)
                    if response.status_code in (409, 412, 429) or response.status_code >= 500:
                        last_error = requests.HTTPError(
                            f"HTTP {response.status_code} {response.reason}",
                            response=response,
                        )

                        if attempt < max_retries:
                            dg.get_dagster_logger().warning(
                                f"Chunk {chunk_idx + 1} attempt {attempt + 1} failed "
                                f"with {response.status_code}, "
                                f"retrying in {chunk_retry_backoff}s..."
                            )
                            time.sleep(chunk_retry_backoff)
                            chunk_retry_backoff *= 2  # Exponential backoff
                            continue
                        else:
                            dg.get_dagster_logger().error(
                                f"Chunk {chunk_idx + 1} failed after {max_retries + 1} attempts: "
                                f"{response.status_code} {response.reason}; "
                                f"body: {response.text}"
                            )
                            raise last_error
                    elif response.status_code == 400:
                        # 400 Bad Request - validation error, don't retry
                        # Log the full response body to see what the FHIR server rejected
                        dg.get_dagster_logger().error(
                            f"Chunk {chunk_idx + 1} FHIR validation error (400 Bad Request). "
                            f"Response body:\n{response.text}"
                        )

                        # Save failing bundle to file for local reproduction
                        self._save_failing_bundle(
                            chunk_idx=chunk_idx,
                            bundle=bundle,
                            response_text=response.text,
                            response_status=response.status_code,
                        )

                        raise requests.HTTPError(
                            "400 Bad Request: FHIR validation failed",
                            response=response,
                        )
                    else:
                        # Success or other non-retryable error
                        response.raise_for_status()

                        # Parse response to measure size
                        start_time = time.perf_counter()
                        response_size_kb = len(response.content) / 1024
                        parse_time = (time.perf_counter() - start_time) * 1000

                        # Calculate total time for this chunk
                        total_time = (time.perf_counter() - overall_start) * 1000

                        # Log detailed timing breakdown
                        dg.get_dagster_logger().info(
                            f"Chunk {chunk_idx + 1} timing breakdown: build={build_time:.1f}ms, "
                            f"serialize={serialize_time:.1f}ms ({payload_size_kb:.1f}KB), "
                            f"http={http_time:.1f}ms (status={response.status_code}), "
                            f"response={parse_time:.1f}ms ({response_size_kb:.1f}KB), "
                            f"TOTAL={total_time:.1f}ms, "
                            f"posted={len(chunk)} resources"
                        )

                        break

                except requests.HTTPError as e:
                    # HTTP errors (like 400 validation errors) - don't retry
                    # The error was already logged above with the response body
                    dg.get_dagster_logger().error(
                        f"Chunk {chunk_idx + 1} failed with HTTP error: {e}"
                    )
                    raise
                except requests.RequestException as e:
                    # Network errors (connection, timeout, etc.) - retry with backoff
                    last_error = e
                    if attempt < max_retries:
                        dg.get_dagster_logger().warning(
                            f"Chunk {chunk_idx + 1} attempt {attempt + 1} failed "
                            f"with network error, retrying in "
                            f"{chunk_retry_backoff}s: {e}"
                        )
                        time.sleep(chunk_retry_backoff)
                        chunk_retry_backoff *= 2  # Exponential backoff
                        continue
                    else:
                        dg.get_dagster_logger().error(
                            f"Chunk {chunk_idx + 1} failed after {max_retries + 1} attempts: {e}"
                        )
                        raise

            # Should have a successful response here
            if response:
                dg.get_dagster_logger().info(
                    f"Chunk {chunk_idx + 1} completed successfully "
                    f"with status {response.status_code}"
                )
                return response
            else:
                raise RuntimeError(f"No response for chunk {chunk_idx + 1}")

        # Process chunks in parallel using ThreadPoolExecutor
        from concurrent.futures import ThreadPoolExecutor, as_completed

        responses = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks for parallel processing
            future_to_chunk = {
                executor.submit(process_chunk, idx, chunk): idx for idx, chunk in enumerate(chunks)
            }

            # Collect results as they complete
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    response = future.result()
                    responses.append(response)
                    dg.get_dagster_logger().info(
                        f"Completed posting chunk {chunk_idx + 1}/{len(chunks)}"
                    )
                except Exception as e:
                    dg.get_dagster_logger().error(f"Failed to post chunk {chunk_idx + 1}: {e}")
                    raise

        dg.get_dagster_logger().info(
            f"All {len(chunks)} chunks completed, {len(resources)} resources processed"
        )
        return responses

    def post_resource_batches_with_details(
        self,
        resources: Sequence[Resource | dict[str, Any]],
        chunk_size: int = 400,
        bundle_type: str = "batch",
        max_isolation_level: str | None = None,
        max_retries: int = 3,
        retry_backoff_sec: float = 0.5,
        max_workers: int = 8,
    ) -> dict[str, Any]:
        """
        Posts FHIR resources with detailed per-resource response parsing.

        Unlike post_resource_batches which returns raw responses, this method
        parses the response bundle to provide granular success/failure information
        for each resource. This is especially useful for batch bundles where
        individual resources can fail independently.

        Args:
            resources: Sequence of FHIR resources (Resource objects or dicts)
            chunk_size: Number of resources per bundle (default 200)
            bundle_type: FHIR bundle type - "batch" (non-atomic) or "transaction" (atomic)
            max_isolation_level: Optional isolation level header to reduce conflicts
            max_retries: Maximum retry attempts per chunk on transient failures
            retry_backoff_sec: Backoff time between retries in seconds
            max_workers: Number of parallel workers for batch processing (default 4)

        Returns:
            Dictionary with detailed results:
            {
                "total": int,
                "successes": int,
                "failures": int,
                "success_ids": list[str],
                "failed_indices": list[int],
                "errors": list[dict],  # Per-resource error details
            }
        """
        if not resources:
            return {
                "total": 0,
                "successes": 0,
                "failures": 0,
                "success_ids": [],
                "failed_indices": [],
                "errors": [],
            }

        # Post using the standard method
        responses = self.post_resource_batches(
            resources=resources,
            chunk_size=chunk_size,
            bundle_type=bundle_type,
            max_isolation_level=max_isolation_level,
            max_retries=max_retries,
            retry_backoff_sec=retry_backoff_sec,
            max_workers=max_workers,
        )

        # Parse responses for detailed results
        total = len(resources)
        successes = 0
        failures = 0
        success_ids: list[str] = []
        failed_indices: list[int] = []
        errors: list[dict[str, Any]] = []

        for chunk_idx, response in enumerate(responses):
            chunk_start = chunk_idx * chunk_size
            chunk_end = min(chunk_start + chunk_size, total)
            chunk_resources = chunk_end - chunk_start

            if response.status_code != 200:
                # Entire chunk failed
                if bundle_type == "transaction":
                    # Transaction bundle - all resources in chunk failed
                    for i in range(chunk_start, chunk_end):
                        failed_indices.append(i)
                        errors.append(
                            {
                                "index": i,
                                "status": str(response.status_code),
                                "error": f"Chunk {chunk_idx + 1} failed: {response.text[:200]}",
                            }
                        )
                    failures += chunk_resources
                else:
                    # Batch bundle - need to parse individual responses
                    # But if we can't parse, assume all failed
                    for i in range(chunk_start, chunk_end):
                        failed_indices.append(i)
                        errors.append(
                            {
                                "index": i,
                                "status": str(response.status_code),
                                "error": f"Chunk {chunk_idx + 1} failed",
                            }
                        )
                    failures += chunk_resources
                continue

            # Parse successful response bundle
            try:
                response_bundle = response.json()
                entries = response_bundle.get("entry", [])

                for entry_idx, entry in enumerate(entries):
                    resource_index = chunk_start + entry_idx
                    if resource_index >= total:
                        break

                    response_part = entry.get("response", {})
                    status = response_part.get("status", "")

                    # Check status - 2xx codes are success
                    if status.startswith("2"):
                        successes += 1
                        # Try to extract the FHIR ID from location header
                        location = response_part.get("location", "")
                        if location:
                            # Location format: ResourceType/id/_history/version
                            parts = location.split("/")
                            if len(parts) >= 2:
                                success_ids.append(parts[1])
                        elif "id" in entry.get("resource", {}):
                            success_ids.append(entry["resource"]["id"])
                    else:
                        failures += 1
                        failed_indices.append(resource_index)
                        # Extract error details from OperationOutcome if present
                        outcome = entry.get("resource", {})
                        error_detail = status
                        if outcome.get("resourceType") == "OperationOutcome":
                            issues = outcome.get("issue", [])
                            if issues:
                                error_detail = issues[0].get("diagnostics", status)
                        errors.append(
                            {
                                "index": resource_index,
                                "status": status,
                                "error": error_detail,
                            }
                        )

            except Exception as e:
                # Can't parse response - assume chunk succeeded for transaction
                dg.get_dagster_logger().warning(
                    f"Could not parse response for chunk {chunk_idx + 1}: {e}"
                )
                if bundle_type == "transaction":
                    successes += chunk_resources
                else:
                    # For batch, we can't know individual status
                    successes += chunk_resources

        result = {
            "total": total,
            "successes": successes,
            "failures": failures,
            "success_ids": success_ids,
            "failed_indices": failed_indices,
            "errors": errors,
        }

        dg.get_dagster_logger().info(
            f"Batch posting details: {successes}/{total} succeeded, "
            f"{failures} failed, {len(errors)} errors recorded"
        )

        return result

    def batch_fetch_patients_by_cpr(
        self,
        cpr_list: list[str],
        chunk_size: int = 1000,
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch multiple Patient resources by CPR identifiers using FHIR batch bundles.

        Args:
            cpr_list: List of CPR identifiers to fetch
            chunk_size: Number of search requests per bundle (default 100)

        Returns:
            Dictionary mapping CPR -> Patient resource dict
        """
        if not cpr_list:
            return {}

        from utils.fhir.identifier_systems import SYSTEM_CPR

        # Chunk the CPR list
        chunks = [cpr_list[i : i + chunk_size] for i in range(0, len(cpr_list), chunk_size)]

        dg.get_dagster_logger().info(
            f"Fetching {len(cpr_list)} patients by CPR in {len(chunks)} chunks "
            f"(chunk_size={chunk_size})"
        )

        cpr_to_patient: dict[str, dict[str, Any]] = {}
        url = self.effective_base_url

        # Set up authentication
        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Process each chunk
        for chunk_idx, chunk in enumerate(chunks):
            dg.get_dagster_logger().info(
                f"Fetching chunk {chunk_idx + 1}/{len(chunks)} with {len(chunk)} CPRs"
            )

            # Build FHIR Batch Bundle (for read operations)
            bundle = {
                "resourceType": "Bundle",
                "type": "batch",
                "entry": [],
            }

            for cpr in chunk:
                # Create GET request for each CPR
                bundle["entry"].append(
                    {
                        "request": {
                            "method": "GET",
                            "url": f"Patient?identifier={SYSTEM_CPR}|{cpr}",
                        }
                    }
                )

            try:
                response = requests.post(
                    url,
                    data=json.dumps(bundle),
                    headers=headers,
                    auth=auth,
                )
                response.raise_for_status()

                # Parse response bundle
                response_bundle = response.json()
                if response_bundle.get("resourceType") == "Bundle":
                    entries = response_bundle.get("entry", [])

                    for entry in entries:
                        # Each entry contains a response with a search bundle
                        response_part = entry.get("response", {})

                        # Skip if response indicates error
                        if response_part.get("status", "").startswith("4") or response_part.get(
                            "status", ""
                        ).startswith("5"):
                            continue

                        # Get the resource which should be a search Bundle
                        resource = entry.get("resource", {})

                        if resource.get("resourceType") == "Bundle":
                            # This is a search result bundle
                            search_entries = resource.get("entry", [])
                            if search_entries:
                                # Get the first patient from search results
                                patient = search_entries[0].get("resource", {})

                                # Extract CPR from patient identifiers
                                identifiers = patient.get("identifier", [])
                                for identifier in identifiers:
                                    if identifier.get("system") == SYSTEM_CPR:
                                        patient_cpr = identifier.get("value")
                                        if patient_cpr:
                                            cpr_to_patient[patient_cpr] = patient
                                        break

            except requests.HTTPError as e:
                dg.get_dagster_logger().warning(
                    f"Failed to fetch chunk {chunk_idx + 1}: {e}. Continuing with remaining chunks."
                )
            except Exception as e:
                dg.get_dagster_logger().warning(
                    f"Unexpected error fetching chunk {chunk_idx + 1}: {e}. "
                    "Continuing with remaining chunks."
                )

        dg.get_dagster_logger().info(
            f"Successfully fetched {len(cpr_to_patient)} patients out of {len(cpr_list)} CPRs"
        )
        return cpr_to_patient

    def attach_ids_from_identifiers_batch_sql(
        self,
        resources: list[Resource],
        preferred_systems: list[str] | None = None,
    ) -> list[Resource]:
        """
        Batch resolve IDs using Aidbox /$sql endpoint.

        Uses direct SQL query via Aidbox REST API - much faster than FHIR search
        and no URI length limits. Executes SQL against Aidbox's PostgreSQL database
        through the /$sql REST endpoint.

        Args:
            resources: List of FHIR resources with identifiers
            preferred_systems: Ordered list of identifier systems to prioritize

        Returns:
            Same list of resources with .id set where matches found
        """
        if not resources:
            return resources

        # Determine resource type (assume all same type)
        resource_type = resources[0].get_resource_type()

        # Build map of identifier -> resource for later ID assignment
        identifier_to_resources: dict[str, list[Resource]] = {}
        all_identifiers: list[tuple[str, str]] = []

        for resource in resources:
            try:
                identifiers = getattr(resource, "identifier", None)
            except Exception:
                identifiers = None

            if not identifiers:
                continue

            # Extract system/value pairs
            system_to_value: dict[str, Any] = {}
            for ident in identifiers:
                try:
                    system = getattr(ident, "system", None) or ident.get("system")  # type: ignore[attr-defined]
                    value = getattr(ident, "value", None) or ident.get("value")  # type: ignore[attr-defined]
                except Exception:
                    system = None
                    value = None
                if system and value:
                    system_to_value[str(system)] = str(value)

            if not system_to_value:
                continue

            # Use preferred systems order or all available
            systems_order: list[str] = preferred_systems or list(system_to_value.keys())

            # Map resource by its primary identifier (first in preferred order)
            for system in systems_order:
                if system in system_to_value:
                    identifier_key = f"{system}|{system_to_value[system]}"
                    if identifier_key not in identifier_to_resources:
                        identifier_to_resources[identifier_key] = []
                        all_identifiers.append((system, system_to_value[system]))
                    identifier_to_resources[identifier_key].append(resource)
                    break  # Only use first matching system

        if not all_identifiers:
            return resources

        # Split identifiers into smaller sub-batches for SQL queries
        # Use 20 identifiers per query (sweet spot for query performance)
        sql_sub_batch_size = 20
        identifier_batches = [
            all_identifiers[i : i + sql_sub_batch_size]
            for i in range(0, len(all_identifiers), sql_sub_batch_size)
        ]

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        dg.get_dagster_logger().info(
            f"Batch resolving IDs for {len(identifier_to_resources)} {resource_type} resources "
            f"using /$sql endpoint ({len(identifier_batches)} SQL queries, {sql_sub_batch_size} ids each)"
        )

        # Escape single quotes by doubling them for SQL safety
        def escape_sql_string(s: str) -> str:
            return s.replace("'", "''")

        resolved_count = 0

        try:
            # Execute SQL queries in sub-batches
            for batch_idx, identifier_batch in enumerate(identifier_batches):
                # Build VALUES list for this sub-batch
                values_list = ", ".join(
                    f"('{escape_sql_string(system)}', '{escape_sql_string(value)}')"
                    for system, value in identifier_batch
                )

                # Build SQL query
                sql_query = f"SELECT o.id, identifier_elem->>'system' AS system, identifier_elem->>'value' AS value FROM {resource_type.lower()} o, jsonb_array_elements(o.resource->'identifier') AS identifier_elem INNER JOIN (VALUES {values_list}) AS v(sys, val) ON identifier_elem->>'system' = v.sys AND identifier_elem->>'value' = v.val"

                # Wrap SQL in JSON array as Aidbox expects: ["SQL_QUERY"]
                request_body = [sql_query]

                # Execute SQL query with _format=json parameter
                response = requests.post(
                    f"{self.effective_base_url}/$sql?_format=json",
                    json=request_body,
                    headers=headers,
                    auth=auth,
                    timeout=30,
                )
                response.raise_for_status()
                results = response.json()

                if not isinstance(results, list):
                    dg.get_dagster_logger().warning(
                        f"SQL sub-batch {batch_idx + 1}: Unexpected response type: {type(results)}"
                    )
                    continue

                # Map results back to resources
                for row in results:
                    found_id = row.get("id")
                    found_system = row.get("system")
                    found_value = row.get("value")

                    if not found_id or not found_system or not found_value:
                        continue

                    identifier_key = f"{found_system}|{found_value}"
                    if identifier_key in identifier_to_resources:
                        # Assign ID to all matching resources
                        for res in identifier_to_resources[identifier_key]:
                            try:
                                res.id = found_id
                                resolved_count += 1
                            except Exception:
                                # If model is frozen, reconstruct
                                payload = res.dict()
                                payload["id"] = found_id
                                res = res.__class__(**payload)
                                resolved_count += 1

            dg.get_dagster_logger().info(
                f"SQL batch resolution: {resolved_count} resources matched with existing IDs"
            )

        except Exception as e:
            # Log error details for debugging
            error_detail = ""
            try:
                if hasattr(e, "response") and hasattr(e.response, "text"):  # type: ignore
                    error_detail = f"; response: {e.response.text[:500]}"  # type: ignore
            except Exception:
                pass

            dg.get_dagster_logger().warning(
                f"SQL batch ID resolution failed: {e}{error_detail}; returning resources as-is"
            )

        return resources

    def delete_resource_batches(
        self,
        resource_ids: Sequence[str],
        resource_type: str,
        chunk_size: int = 400,
        bundle_type: str = "transaction",
        max_retries: int = 3,
        retry_backoff_sec: float = 0.5,
    ) -> list[requests.Response]:
        """
        Deletes FHIR resources from the server in batches using FHIR Bundles.

        This method efficiently deletes resources from the FHIR server in bulk by sending
        resource IDs in chunks as FHIR Transaction Bundles (atomic operations). Each chunk is
        processed atomically - if any delete in a chunk fails, the entire chunk
        is rolled back.

        Args:
            resource_ids: Sequence of FHIR resource IDs to delete
            resource_type: The FHIR resource type (e.g., "Patient", "Observation")
            chunk_size: Number of resources per bundle (default 200)
            bundle_type: FHIR bundle type, defaults to "transaction" for atomicity
            max_retries: Maximum retry attempts per chunk on transient failures
            retry_backoff_sec: Backoff time between retries in seconds

        Returns:
            List of HTTP responses, one per chunk submitted

        Raises:
            requests.HTTPError: If a chunk fails all retry attempts
        """
        if not resource_ids:
            return []

        # Chunk the resource IDs
        chunks = [resource_ids[i : i + chunk_size] for i in range(0, len(resource_ids), chunk_size)]

        dg.get_dagster_logger().info(
            f"Deleting {len(resource_ids)} {resource_type} resources in {len(chunks)} chunks "
            f"(chunk_size={chunk_size}, bundle_type={bundle_type})"
        )

        responses = []
        url = self.effective_base_url

        # Set up authentication
        auth = None
        if self.client_username and self.client_password:
            auth = HTTPBasicAuth(self.client_username, self.client_password)

        # Set up base headers
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        # Process each chunk
        for chunk_idx, chunk in enumerate(chunks):
            dg.get_dagster_logger().info(
                f"Processing delete chunk {chunk_idx + 1}/{len(chunks)} with {len(chunk)} resources"
            )

            # Build FHIR Bundle for deletion
            bundle = {
                "resourceType": "Bundle",
                "type": bundle_type,
                "entry": [],
            }

            for resource_id in chunk:
                # Build delete request entry
                bundle["entry"].append(
                    {
                        "request": {
                            "method": "DELETE",
                            "url": f"/{resource_type}/{resource_id}",
                        }
                    }
                )

            # Submit chunk with retries
            response = None
            last_error = None

            for attempt in range(max_retries + 1):  # +1 for initial attempt
                try:
                    dg.get_dagster_logger().info(
                        f"Submitting delete chunk {chunk_idx + 1}, attempt {attempt + 1}"
                    )

                    response = requests.post(
                        url,
                        data=json.dumps(bundle),
                        headers=headers,
                        auth=auth,
                    )

                    # Check for retryable errors
                    if response.status_code in (409, 412, 429) or response.status_code >= 500:
                        last_error = requests.HTTPError(
                            f"HTTP {response.status_code} {response.reason}",
                            response=response,
                        )

                        if attempt < max_retries:
                            dg.get_dagster_logger().warning(
                                f"Delete chunk {chunk_idx + 1} attempt {attempt + 1} failed "
                                f"with {response.status_code}, "
                                f"retrying in {retry_backoff_sec}s..."
                            )
                            time.sleep(retry_backoff_sec)
                            retry_backoff_sec *= 2  # Exponential backoff
                            continue
                        else:
                            dg.get_dagster_logger().error(
                                f"Delete chunk {chunk_idx + 1} failed after {max_retries + 1} attempts: "
                                f"{response.status_code} {response.reason}; "
                                f"body: {response.text}"
                            )
                            raise last_error
                    else:
                        # Success or non-retryable error
                        response.raise_for_status()
                        break

                except requests.RequestException as e:
                    last_error = e
                    if attempt < max_retries:
                        dg.get_dagster_logger().warning(
                            f"Delete chunk {chunk_idx + 1} attempt {attempt + 1} failed "
                            f"with network error, retrying in "
                            f"{retry_backoff_sec}s: {e}"
                        )
                        time.sleep(retry_backoff_sec)
                        retry_backoff_sec *= 2  # Exponential backoff
                        continue
                    else:
                        dg.get_dagster_logger().error(
                            f"Delete chunk {chunk_idx + 1} failed after "
                            f"{max_retries + 1} attempts: {e}"
                        )
                        raise

            # Should have a successful response here
            if response:
                dg.get_dagster_logger().info(
                    f"Delete chunk {chunk_idx + 1} completed successfully "
                    f"with status {response.status_code}"
                )
                responses.append(response)
            else:
                raise RuntimeError(f"No response for delete chunk {chunk_idx + 1}")

        dg.get_dagster_logger().info(
            f"All {len(chunks)} delete chunks completed, {len(resource_ids)} {resource_type} resources deleted"
        )
        return responses


# ============================================================================
# Linked Server Configuration
# ============================================================================


def get_linked_server_config() -> dict[str, str]:
    """
    Read linked-server configuration from environment variables.

    Canonical variables (MSSQL_LINKED_SERVER_*) take precedence.
    Falls back to MSSQL_SOURCE_STAGING_* for backward compatibility.
    """
    host = os.getenv("MSSQL_LINKED_SERVER_HOST") or os.getenv(
        "MSSQL_SOURCE_STAGING_HOST", "localhost"
    )
    port = os.getenv("MSSQL_LINKED_SERVER_PORT") or os.getenv("MSSQL_SOURCE_STAGING_PORT", "1433")
    database = os.getenv("MSSQL_LINKED_SERVER_DB") or os.getenv("MSSQL_SOURCE_STAGING_DB", "DW")
    user = os.getenv("MSSQL_LINKED_SERVER_USER") or os.getenv("MSSQL_SOURCE_STAGING_USER", "sa")
    password = os.getenv("MSSQL_LINKED_SERVER_PASSWORD") or os.getenv(
        "MSSQL_SOURCE_STAGING_PASSWORD", ""
    )
    name = os.getenv("MSSQL_LINKED_SERVER_NAME", "STAGING_LINK")
    datasrc = f"{host},{port}" if port else host

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "name": name,
        "datasrc": datasrc,
    }


# ============================================================================
# Resource Factory Functions
# ============================================================================


def mssql_defs() -> dg.Definitions:
    """
    Defines MSSQL resources for staging (DSI/DSA) and DSO (reporting) databases.

    Resources:
    - mssql_source_staging: Read from staging/DSI/DSA database
    - mssql_target_staging: Write to staging/DSI/DSA database (stg, map, dim, fct tables)
    - mssql_source_dso: Read from DSO/Report database
    - mssql_target_dso: Write to DSO/Report database (views, final tables)

    Architecture:
    - Source systems → mssql_target_staging (DSI/DSA DB) → manual copy → DSO DB
    - This reduces I/O on the reporting database during ETL operations.

    Driver Selection:
    - Uses mssql-python.
    """

    # ========================================
    # Staging Database Resources (DSI/DSA Layer)
    # ========================================

    mssql_source_staging = MSSQLResource(
        host=os.getenv("MSSQL_SOURCE_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_SOURCE_STAGING_PORT", "1433")),
        database=os.getenv("MSSQL_SOURCE_STAGING_DB", "DW"),
        user=os.getenv("MSSQL_SOURCE_STAGING_USER", "sa"),
        password=dg.EnvVar("MSSQL_SOURCE_STAGING_PASSWORD"),
    )

    mssql_target_staging = MSSQLResource(
        host=os.getenv("MSSQL_TARGET_STAGING_HOST", "localhost"),
        port=int(os.getenv("MSSQL_TARGET_STAGING_PORT", "1433")),
        database=os.getenv("MSSQL_TARGET_STAGING_DB", "DW"),
        user=os.getenv("MSSQL_TARGET_STAGING_USER", "sa"),
        password=dg.EnvVar("MSSQL_TARGET_STAGING_PASSWORD"),
    )

    # ========================================
    # DSO Database Resources (Reporting Layer)
    # ========================================

    dso_host = os.getenv("MSSQL_SOURCE_DSO_HOST", "localhost")

    mssql_source_dso = MSSQLResource(
        host=dso_host,
        port=int(os.getenv("MSSQL_SOURCE_DSO_PORT", "1433")),
        database=os.getenv("MSSQL_SOURCE_DSO_DB", "DW"),
        user=os.getenv("MSSQL_SOURCE_DSO_USER", "sa"),
        password=dg.EnvVar("MSSQL_SOURCE_DSO_PASSWORD"),
    )

    mssql_target_dso = MSSQLResource(
        host=os.getenv("MSSQL_TARGET_DSO_HOST", "localhost"),
        port=int(os.getenv("MSSQL_TARGET_DSO_PORT", "1433")),
        database=os.getenv("MSSQL_TARGET_DSO_DB", "DW"),
        user=os.getenv("MSSQL_TARGET_DSO_USER", "sa"),
        password=dg.EnvVar("MSSQL_TARGET_DSO_PASSWORD"),
    )

    return dg.Definitions(
        resources={
            "mssql_source_staging": mssql_source_staging,
            "mssql_target_staging": mssql_target_staging,
            "mssql_source_dso": mssql_source_dso,
            "mssql_target_dso": mssql_target_dso,
        }
    )


# ============================================================================
# FHIR Database Resource (Direct PostgreSQL Access)
# ============================================================================


# Shared identifier cache — generic utility lives in core.cache.
# The name `SqliteIdentifierCache` is preserved as an alias so existing
# callers (tests, utilities) continue to work without modification.

SqliteIdentifierCache = SharedIdentifierCache


# Concrete lookup backends used by cache consumers. Keeping this alias explicit
# avoids the extra Protocol class indirection while still documenting the
# supported runtime types.
IdentifierLookup = SharedIdentifierCache | dict[tuple[str, str], str]
IdentifierLookupSpec = tuple[str, list[str] | None]


# Direct-DB upsert parallelism — see :meth:`FHIRDatabaseResource.batch_upsert_resource_dicts`.
# Defaults are conservative: 4 shards is well within the typical Aidbox PG
# connection budget, and 5 000 rows/shard keeps the per-shard fixed cost
# (connection setup + txid bookkeeping) amortised against real work.
_DB_UPSERT_MAX_WORKERS = env_int("FHIR_DB_UPSERT_MAX_WORKERS", 4, minimum=1)
_DB_UPSERT_MIN_ROWS_PER_WORKER = env_int("FHIR_DB_UPSERT_MIN_ROWS_PER_WORKER", 5_000, minimum=1)


class FHIRDatabaseResource(dg.ConfigurableResource):
    """
    Direct PostgreSQL database access for FHIR server for performance-critical operations.

    Bypasses the FHIR HTTP API to query the underlying PostgreSQL database directly.
    This is significantly faster for batch ID resolution operations.
    """

    host: str = os.getenv("PG_FHIR_HOST", "fhir-server-db")
    port: int = env_int("PG_FHIR_PORT", 5432, minimum=1)
    database: str = os.getenv("PG_FHIR_DATABASE", "aidbox")
    user: str = os.getenv("PG_FHIR_USER", "postgres_user")
    password: str = os.getenv("PG_FHIR_PASSWORD", "postgres_password")

    def __init__(self, **data):
        """Initialize resource."""
        super().__init__(**data)
        self._conn = None
        # Per-process in-memory cache registry keyed by (resource_type, systems).
        # Cross-process build coordination + on-disk storage live in
        # core.cache.CacheCoordinator; this dict is the hot-path short-circuit
        # that avoids even the fast-path fresh-file check on repeat calls.
        self._identifier_caches: dict[
            tuple[str, frozenset[str]],
            IdentifierLookup,
        ] = {}
        self._identifier_cache: IdentifierLookup | None = None
        self._active_cache_key: tuple[str, frozenset[str]] | None = None
        # Lazily-constructed coordinator shared across all preload calls
        # on this resource instance. Workers in separate OS processes each
        # have their own instance, but they all point at the same
        # filesystem cache directory and coordinate via fcntl locks.
        self._cache_coordinator: CacheCoordinator | None = None
        # Resource types whose FHIR state was mutated during this asset run
        # (e.g. by dependency resolution persisting a CPR-bridge identifier).
        # batch_resolve_references auto-applies force_reload for stale types
        # so posting functions never need manual cache coordination.
        self._stale_resource_types: set[str] = set()

    @property
    def cache_coordinator(self) -> CacheCoordinator:
        if self._cache_coordinator is None:
            self._cache_coordinator = CacheCoordinator()
        return self._cache_coordinator

    @contextmanager
    def get_connection(self):
        """
        Get a PostgreSQL database connection.

        Yields:
            psycopg.Connection: Database connection

        Raises:
            ValueError: If host is not configured (e.g., "NOT SET")
            ImportError: If psycopg is not available
        """
        if not PSYCOPG_AVAILABLE:
            raise ImportError(
                "psycopg is required for FHIRDatabaseResource. "
                "Install with: pip install 'psycopg[binary]>=3.1,<4'"
            )

        if self.host == "NOT SET" or not self.host:
            raise ValueError(
                "PG_FHIR_HOST is not configured. "
                "Set PG_FHIR_HOST (and PG_FHIR_PORT/USER/PASSWORD/DATABASE) in your .env; "
                "the local devcontainer uses the 'fhir-server-db' compose service."
            )

        dg.get_dagster_logger().info(
            f"Connecting to PostgreSQL database {self.database} on {self.host}:{self.port}"
        )

        conn = psycopg.connect(
            host=self.host,
            port=self.port,
            dbname=self.database,
            user=self.user,
            password=self.password,
            connect_timeout=30,
        )
        try:
            yield conn
        finally:
            conn.close()

    def batch_resolve_ids_direct_query(
        self,
        resources: Sequence[Resource],
        resource_type: str,
        preferred_systems: list[str],
        batch_size: int = 1000,
    ) -> dict[int, str | None]:
        """
        Approach 1: Direct PostgreSQL query with batching.

        Bypasses FHIR HTTP API and queries database directly using raw SQL.
        Processes identifiers in batches using UNION ALL queries.

        Args:
            resources: List of FHIR resources with identifiers
            resource_type: Type of resource (e.g., "Patient")
            preferred_systems: List of identifier systems to try
            batch_size: Number of identifiers per batch query

        Returns:
            Dictionary mapping resource index -> resolved FHIR server ID
        """
        if not resources:
            return {}

        # Extract identifiers from resources
        resource_identifiers: list[tuple[int, str, str]] = []

        for idx, resource in enumerate(resources):
            identifiers = getattr(resource, "identifier", None)
            if not identifiers:
                continue

            system_to_value: dict[str, str] = {}
            for ident in identifiers:
                system = getattr(ident, "system", None)
                value = getattr(ident, "value", None)
                if system and value:
                    system_to_value[str(system)] = str(value)

            for preferred_system in preferred_systems:
                if preferred_system in system_to_value:
                    resource_identifiers.append(
                        (
                            idx,
                            preferred_system,
                            system_to_value[preferred_system],
                        )
                    )
                    break

        if not resource_identifiers:
            return {}

        dg.get_dagster_logger().info(
            f"[Direct Query] Resolving {len(resource_identifiers)} IDs in batches of {batch_size}"
        )

        index_to_id: dict[int, str | None] = {}
        table_name = resource_type.lower()

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Process in batches
                for batch_start in range(0, len(resource_identifiers), batch_size):
                    batch = resource_identifiers[batch_start : batch_start + batch_size]

                    # Build query with UNION ALL for each identifier
                    # Note: Each SELECT needs to be wrapped in parentheses for UNION ALL
                    queries = []
                    params = []

                    for idx, system, value in batch:
                        queries.append(
                            f"(SELECT %s as idx, id FROM {table_name} WHERE resource @> %s LIMIT 1)"
                        )
                        params.extend(
                            [
                                idx,
                                json.dumps({"identifier": [{"system": system, "value": value}]}),
                            ]
                        )

                    if queries:
                        union_query = " UNION ALL ".join(queries)
                        cur.execute(union_query, params)

                        for row in cur.fetchall():
                            index_to_id[row[0]] = row[1]

        dg.get_dagster_logger().info(
            f"[Direct Query] Resolved {len(index_to_id)}/{len(resource_identifiers)} IDs"
        )
        return index_to_id

    @staticmethod
    def _cache_key(
        resource_type: str,
        identifier_systems: list[str] | None = None,
    ) -> tuple[str, frozenset[str]]:
        return _identifier_lookup._cache_key(resource_type, identifier_systems)

    def get_loaded_identifier_cache(
        self,
        resource_type: str,
        identifier_systems: list[str] | None = None,
    ) -> IdentifierLookup | None:
        return _identifier_lookup.get_loaded_identifier_cache(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
        )

    def get_identifier_cache(
        self,
        resource_type: str,
        identifier_systems: list[str] | None = None,
        force_reload: bool = False,
        use_cache: bool = True,
    ) -> IdentifierLookup:
        return _identifier_lookup.get_identifier_cache(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
            force_reload=force_reload,
            use_cache=use_cache,
        )

    def get_identifier_lookup(
        self,
        resource_type: str,
        identifier_systems: list[str] | None = None,
        force_reload: bool = False,
    ) -> IdentifierLookup:
        return _identifier_lookup.get_identifier_lookup(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
            force_reload=force_reload,
        )

    def get_identifier_lookups(
        self,
        specs: Sequence[IdentifierLookupSpec],
        force_reload: bool = False,
        parallel: bool = True,
    ) -> dict[str, IdentifierLookup]:
        return _identifier_lookup.get_identifier_lookups(
            self,
            specs=specs,
            force_reload=force_reload,
            parallel=parallel,
        )

    def mark_stale(self, resource_type: str) -> None:
        """Signal that FHIR state for *resource_type* was mutated during this run.

        Downstream :func:`batch_resolve_references` calls automatically
        use ``force_reload=True`` for stale types, so posting functions
        never need manual cache coordination.
        """
        self._stale_resource_types.add(resource_type)

    def is_stale(self, resource_type: str) -> bool:
        """True if *resource_type* was mutated during this asset run."""
        return resource_type in self._stale_resource_types

    def resolve_identifier_values(
        self,
        resource_type: str,
        identifier_system: str,
        values: Sequence[str],
        force_reload: bool = False,
    ) -> dict[str, str]:
        return _identifier_lookup.resolve_identifier_values(
            self,
            resource_type=resource_type,
            identifier_system=identifier_system,
            values=values,
            force_reload=force_reload or self.is_stale(resource_type),
        )

    def resolve_resource_ids(
        self,
        resources: Sequence[Resource],
        resource_type: str,
        preferred_systems: list[str],
        force_reload: bool = False,
    ) -> dict[int, str | None]:
        return _identifier_lookup.resolve_resource_ids(
            self,
            resources=resources,
            resource_type=resource_type,
            preferred_systems=preferred_systems,
            force_reload=force_reload or self.is_stale(resource_type),
        )

    def invalidate_identifier_lookups(self, resource_types: set[str]) -> None:
        """Invalidate canonical identifier lookups for selected resource types."""
        self.clear_identifier_caches_for_resources(resource_types)

    def preload_identifier_cache(
        self,
        resource_type: str,
        identifier_systems: list[str] | None = None,
        force_reload: bool = False,
        use_cache: bool = True,
    ) -> None:
        return _identifier_lookup.preload_identifier_cache(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
            force_reload=force_reload,
            use_cache=use_cache,
        )

    def _build_identifier_dict(
        self,
        resource_type: str,
        identifier_systems: list[str] | None,
    ) -> dict[tuple[str, str], str]:
        return _identifier_lookup._build_identifier_dict(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
        )

    def _stream_fhir_identifiers(
        self,
        *,
        resource_type: str,
        identifier_systems: list[str] | None,
    ) -> Iterator[tuple[str, str, str]]:
        return _identifier_lookup._stream_fhir_identifiers(
            self,
            resource_type=resource_type,
            identifier_systems=identifier_systems,
        )

    def suspend_identifier_caches(self) -> None:
        return _identifier_lookup.suspend_identifier_caches(self)

    def log_identifier_lookup_status(
        self,
        label: str,
        lookups: Mapping[str, IdentifierLookup],
    ) -> None:
        return _identifier_lookup.log_identifier_lookup_status(
            self,
            label=label,
            lookups=lookups,
        )

    def reset_identifier_lookup_counters(self) -> None:
        return _identifier_lookup.reset_targeted_lookup_counters(self)

    def safe_clear_identifier_caches_for_resources(
        self,
        resource_types: set[str],
        *,
        reason: str | None = None,
    ) -> bool:
        return _identifier_lookup.safe_clear_identifier_caches_for_resources(
            self,
            resource_types=resource_types,
            reason=reason,
        )

    def clear_identifier_caches_for_resources(self, resource_types: set[str]) -> None:
        return _identifier_lookup.clear_identifier_caches_for_resources(
            self,
            resource_types=resource_types,
        )

    def clear_identifier_caches(self) -> None:
        return _identifier_lookup.clear_identifier_caches(self)

    def finalize_identifier_caches(
        self,
        resource_types: set[str],
        *,
        reason: str | None = None,
    ) -> None:
        return _identifier_lookup.finalize_identifier_caches(
            self,
            resource_types=resource_types,
            reason=reason,
        )

    def purge_identifier_cache_files(
        self,
        resource_types: set[str] | None = None,
    ) -> int:
        return _identifier_lookup.purge_identifier_cache_files(
            self,
            resource_types=resource_types,
        )

    def batch_resolve_ids_memory_cache(
        self,
        resources: Sequence[Resource],
        resource_type: str,
        preferred_systems: list[str],
    ) -> dict[int, str | None]:
        """
        Compatibility wrapper for resolving IDs through the canonical lookup.

        Args:
            resources: List of FHIR resources with identifiers
            resource_type: Type of resource (e.g., "Patient")
            preferred_systems: List of identifier systems to try

        Returns:
            Dictionary mapping resource index -> resolved FHIR server ID
        """
        return self.resolve_resource_ids(
            resources=resources,
            resource_type=resource_type,
            preferred_systems=preferred_systems,
        )

    def create_optimized_index(
        self,
        resource_type: str,
        identifier_system: str,
        drop_existing: bool = False,
        concurrently: bool = True,
        analyze_after: bool = False,
    ) -> None:
        """
        Approach 2: Create optimized GIN index with expression for specific identifier system.

        Creates a specialized GIN index that filters to a specific identifier system,
        which is more targeted than a generic GIN index on all identifiers.

        Args:
            resource_type: Type of resource (e.g., "Patient")
            identifier_system: Identifier system to index (e.g., "urn:dk:cpr")
            drop_existing: Whether to drop existing index first
            concurrently: Use PostgreSQL ``CREATE INDEX CONCURRENTLY``. This is
                the safe default for live Aidbox tables, but it requires
                autocommit because PostgreSQL rejects it inside a transaction.
            analyze_after: Run ``ANALYZE`` after creating the index. Disabled by
                default because the affected Aidbox tables can be very large.
        """
        table_name = resource_type.lower()
        if not table_name.isalnum():
            raise ValueError(f"Unsafe FHIR resource table name: {table_name!r}")
        system_safe = sanitize_system_for_identifier(identifier_system)
        index_name = f"idx_{table_name}_{system_safe}_opt"
        concurrent_sql = "CONCURRENTLY " if concurrently else ""

        dg.get_dagster_logger().info(
            f"[Optimized Index] Creating index {index_name} for system {identifier_system}"
        )

        with self.get_connection() as conn:
            previous_autocommit = getattr(conn, "autocommit", None)
            if concurrently:
                conn.autocommit = True

            try:
                with conn.cursor() as cur:
                    if drop_existing:
                        cur.execute(f"DROP INDEX {concurrent_sql}IF EXISTS {index_name}")

                    # Create partial GIN index optimized for specific identifier system
                    # This is more targeted than a generic GIN index on all identifiers
                    system_filter = json.dumps([{"system": identifier_system}])

                    query = f"""
                    CREATE INDEX {concurrent_sql}IF NOT EXISTS {index_name}
                    ON {table_name}
                    USING GIN ((resource -> 'identifier') jsonb_path_ops)
                    WHERE resource -> 'identifier' @> '{system_filter}'::jsonb
                    """

                    cur.execute(query)

                    if analyze_after:
                        cur.execute(f"ANALYZE {table_name}")

                if not concurrently:
                    conn.commit()
            finally:
                if concurrently and previous_autocommit is not None:
                    conn.autocommit = previous_autocommit

        dg.get_dagster_logger().info(f"[Optimized Index] Created index {index_name}")

    def create_id_index(
        self,
        resource_type: str,
        index_type: str = "btree",
        drop_existing: bool = False,
    ) -> None:
        """
        Create an index on the FHIR resource 'id' field for optimized ID-based queries.

        The 'id' field is the FHIR logical ID used for direct resource lookups.
        While this field typically has a primary key index, additional indexes can
        optimize specific query patterns.

        Args:
            resource_type: Type of FHIR resource (e.g., "Patient", "EpisodeOfCare")
            index_type: Type of index to create ("btree", "hash", "gin", "gist")
            drop_existing: Whether to drop existing index first
        """
        table_name = resource_type.lower()
        index_name = f"idx_{table_name}_id_{index_type}"

        dg.get_dagster_logger().info(
            f"[ID Index] Creating {index_type} index {index_name} on {resource_type}.id"
        )

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                if drop_existing:
                    cur.execute(f"DROP INDEX IF EXISTS {index_name}")

                # Create index on the id column
                query = f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table_name}
                USING {index_type} (id)
                """

                cur.execute(query)
                conn.commit()

                # Analyze table to update statistics
                cur.execute(f"ANALYZE {table_name}")
                conn.commit()

        dg.get_dagster_logger().info(f"[ID Index] Created {index_type} index {index_name}")

    def create_txid_index(
        self,
        resource_type: str,
        drop_existing: bool = False,
        concurrently: bool = True,
        analyze_after: bool = False,
    ) -> None:
        """
        Create a btree index on Aidbox's raw resource-table ``txid`` column.

        Direct PostgreSQL upserts reserve a txid range by running
        ``SELECT COALESCE(MAX(txid), 0) + 1 FROM <resource_table>`` before
        writing. On large Aidbox tables, that query becomes a sequential scan
        unless ``txid`` is indexed.

        Args:
            resource_type: Type of FHIR resource (e.g. "Observation").
            drop_existing: Whether to drop the existing txid index first.
            concurrently: Use PostgreSQL ``CREATE INDEX CONCURRENTLY``. This is
                the safe default for live Aidbox tables, but it requires
                autocommit because PostgreSQL rejects it inside a transaction.
            analyze_after: Run ``ANALYZE`` after creating the index. Disabled by
                default because the affected Aidbox tables can be very large.
        """
        table_name = resource_type.lower()
        if not table_name.isalnum():
            raise ValueError(f"Unsafe FHIR resource table name: {table_name!r}")

        index_name = f"idx_{table_name}_txid"
        concurrent_sql = "CONCURRENTLY " if concurrently else ""

        dg.get_dagster_logger().info(
            f"[TXID Index] Creating txid index {index_name} on {table_name}"
        )

        with self.get_connection() as conn:
            previous_autocommit = getattr(conn, "autocommit", None)
            if concurrently:
                conn.autocommit = True

            try:
                with conn.cursor() as cur:
                    if drop_existing:
                        cur.execute(f"DROP INDEX {concurrent_sql}IF EXISTS {index_name}")

                    cur.execute(
                        f"CREATE INDEX {concurrent_sql}IF NOT EXISTS {index_name} "
                        f"ON {table_name} (txid)"
                    )

                    if analyze_after:
                        cur.execute(f"ANALYZE {table_name}")

                if not concurrently:
                    conn.commit()
            finally:
                if concurrently and previous_autocommit is not None:
                    conn.autocommit = previous_autocommit

        dg.get_dagster_logger().info(f"[TXID Index] Created txid index {index_name}")

    def test_index_performance(
        self,
        resource_type: str,
        identifier_system: str,
        identifier_value: str,
    ) -> tuple[float, str | None]:
        """
        Test query performance with EXPLAIN ANALYZE.

        Returns execution time and found ID for a sample query.
        Useful for comparing index strategies.
        """
        table_name = resource_type.lower()

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = f"""
                EXPLAIN ANALYZE
                SELECT id FROM {table_name}
                WHERE resource @> %s
                LIMIT 1
                """

                identifier_json = json.dumps(
                    {"identifier": [{"system": identifier_system, "value": identifier_value}]}
                )

                start_time = time.perf_counter()
                cur.execute(query, [identifier_json])
                explain_result = cur.fetchall()
                elapsed = time.perf_counter() - start_time

                # Get actual result
                cur.execute(
                    f"SELECT id FROM {table_name} WHERE resource @> %s LIMIT 1",
                    [identifier_json],
                )
                result = cur.fetchone()
                fhir_id = result[0] if result else None

                # Parse execution time from EXPLAIN ANALYZE
                for line in explain_result:
                    if "Execution Time:" in str(line):
                        dg.get_dagster_logger().info(f"Query plan: {line}")

        return elapsed, fhir_id

    def batch_upsert_resources(
        self,
        resources: Sequence[Resource],
        resource_type: str,
        batch_size: int = 2000,
        invalidate_cache: bool = True,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert.batch_upsert_resources(
            self,
            resources=resources,
            resource_type=resource_type,
            batch_size=batch_size,
            invalidate_cache=invalidate_cache,
        )

    def batch_upsert_resource_dicts(
        self,
        resource_dicts: list[tuple[str, dict]],
        resource_type: str,
        batch_size: int = 2000,
        invalidate_cache: bool = True,
        max_workers: int | None = None,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert.batch_upsert_resource_dicts(
            self,
            resource_dicts=resource_dicts,
            resource_type=resource_type,
            batch_size=batch_size,
            invalidate_cache=invalidate_cache,
            max_workers=max_workers,
        )

    @staticmethod
    def _prepare_resource_dicts_for_upsert(
        resource_dicts: list[tuple[str, dict]],
        resource_type: str,
    ) -> tuple[list[tuple[str, dict]], int, list[str]]:
        return _direct_upsert._prepare_resource_dicts_for_upsert(
            resource_dicts=resource_dicts,
            resource_type=resource_type,
        )

    @staticmethod
    def _resolve_dict_upsert_workers(
        *,
        n_rows: int,
        batch_size: int,
        requested: int | None,
    ) -> int:
        return _direct_upsert._resolve_dict_upsert_workers(
            n_rows=n_rows,
            batch_size=batch_size,
            requested=requested,
        )

    def _upsert_dicts_single_connection(
        self,
        *,
        prepared: list[tuple[str, dict]],
        resource_type: str,
        batch_size: int,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert._upsert_dicts_single_connection(
            self,
            prepared=prepared,
            resource_type=resource_type,
            batch_size=batch_size,
        )

    def _upsert_dicts_sharded(
        self,
        *,
        prepared: list[tuple[str, dict]],
        resource_type: str,
        batch_size: int,
        workers: int,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert._upsert_dicts_sharded(
            self,
            prepared=prepared,
            resource_type=resource_type,
            batch_size=batch_size,
            workers=workers,
        )

    def _reserve_txid_base(self, table_name: str) -> int:
        return _direct_upsert._reserve_txid_base(self, table_name=table_name)

    def _upsert_dicts_shard_worker(
        self,
        *,
        shard_rows: list[tuple[str, dict]],
        table_name: str,
        batch_size: int,
        txid_base: int,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert._upsert_dicts_shard_worker(
            self,
            shard_rows=shard_rows,
            table_name=table_name,
            batch_size=batch_size,
            txid_base=txid_base,
        )

    @staticmethod
    def _run_dict_upsert_batches(
        *,
        cur: Any,
        conn: Any,
        rows: list[tuple[str, dict]],
        table_name: str,
        batch_size: int,
        txid_base: int,
    ) -> _direct_upsert.UpsertResult:
        return _direct_upsert._run_dict_upsert_batches(
            cur=cur,
            conn=conn,
            rows=rows,
            table_name=table_name,
            batch_size=batch_size,
            txid_base=txid_base,
        )


def fhir_defs() -> dg.Definitions:
    """
    Defines the FHIR Server and Database resources, making them available to assets.
    """
    use_fhir_api = os.getenv("USE_FHIR_API")
    return dg.Definitions(
        resources={
            "fhir_server": FHIRServerResource(
                base_url=os.getenv("FHIR_SERVER_URL", "NOT SET"),
                client_username=os.getenv("FHIR_CLIENT_USERNAME", "NOT SET"),
                client_password=dg.EnvVar("FHIR_CLIENT_PASSWORD"),
                use_fhir_api=use_fhir_api,
            ),
            "fhir_database": FHIRDatabaseResource(
                host=os.getenv("PG_FHIR_HOST", "NOT SET"),
                port=env_int("PG_FHIR_PORT", 5432, minimum=1),
                database=os.getenv("PG_FHIR_DATABASE", "NOT SET"),
                user=os.getenv("PG_FHIR_USER", "NOT SET"),
                password=dg.EnvVar("PG_FHIR_PASSWORD"),
            ),
        }
    )
