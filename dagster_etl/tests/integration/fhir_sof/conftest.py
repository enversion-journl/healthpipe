# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Fixtures for SQL on FHIR integration tests.

These tests require:
- A running Aidbox FHIR server (fhir-server)
- A running PostgreSQL database (fhir-server-db)
- Optionally a running MSSQL database (mssql)
"""

import os

import pytest
import requests
from core.resources import FHIRDatabaseResource, FHIRServerResource
from requests.auth import HTTPBasicAuth


@pytest.fixture(scope="session")
def fhir_server_url() -> str:
    """Base URL for the Aidbox FHIR server."""
    return os.getenv("FHIR_SERVER_URL", "http://fhir-server:8888")


@pytest.fixture(scope="session")
def fhir_server(fhir_server_url) -> FHIRServerResource:
    """FHIRServerResource configured for the dev Aidbox instance."""
    return FHIRServerResource(
        base_url=fhir_server_url,
        client_username=os.getenv("AIDBOX_CLIENT_ID", "root"),
        client_password=os.getenv("AIDBOX_CLIENT_SECRET", "secret"),
    )


@pytest.fixture(scope="session")
def fhir_database() -> FHIRDatabaseResource:
    """FHIRDatabaseResource configured for direct PostgreSQL access."""
    return FHIRDatabaseResource(
        host=os.getenv("FHIR_DB_HOST", "fhir-server-db"),
        port=int(os.getenv("FHIR_DB_PORT", "5432")),
        database=os.getenv("FHIR_DB_NAME", "aidbox"),
        user=os.getenv("FHIR_DB_USER", "postgres_user"),
        password=os.getenv("FHIR_DB_PASSWORD", "postgres_password"),
    )


@pytest.fixture(scope="session")
def aidbox_available(fhir_server_url) -> bool:
    """Check if Aidbox is reachable."""
    try:
        resp = requests.get(
            f"{fhir_server_url}/health",
            timeout=5,
            auth=HTTPBasicAuth(
                os.getenv("AIDBOX_CLIENT_ID", "root"),
                os.getenv("AIDBOX_CLIENT_SECRET", "secret"),
            ),
        )
        return resp.status_code == 200
    except requests.ConnectionError:
        return False
