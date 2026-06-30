# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Test file for investigating SQL on FHIR with Aidbox.

This script demonstrates how to use SQL on FHIR (https://build.fhir.org/ig/FHIR/sql-on-fhir-v2)
with Aidbox to extract FHIR resources into flat, queryable tables.

Architecture:
    1. ViewDefinition resources are CRUD-managed via REST at /ViewDefinition/{id}
    2. Aidbox auto-materializes each ViewDefinition as a SQL view in the "sof" schema
    3. Flat views are queried via POST /$sql with raw SQL, or via direct PostgreSQL access
    4. Cross-resource joins are done in the analytics layer using SQL JOINs on flat views

    See: https://www.nature.com/articles/s41746-025-01708-w

Configuration:
    Set the following environment variables before running:
    - FHIR_SERVER_URL: The base URL of the FHIR server (e.g., "http://fhir-server:8888")
    - FHIR_CLIENT_USERNAME: Basic auth username (default: "root")
    - FHIR_CLIENT_PASSWORD: Basic auth password (default: "secret")

Usage:
    # Run as a script
    python dagster_etl/tests/test_patient_view.py

    # Run with pytest
    pytest dagster_etl/tests/test_patient_view.py -v -s

    # Run only integration tests
    pytest dagster_etl/tests/test_patient_view.py -v -s -m integration

View Structure (patient_view):
    Extracts the following columns from Patient resources:
    - active: Patient active status (path: active)
    - patient_id: Patient ID (path: id)
    - gender: Patient gender (path: gender)
    - dob: Date of birth (path: birthDate)
    - patient_ssn: SSN identifier (path: identifier.where(...).value)
    - given: Given names joined by space (path: given.join(' '), forEach: name)
    - family: Family name (path: family, forEach: name)
"""

import json
import os
import sys
from typing import Any

import pytest
import requests
from requests.auth import HTTPBasicAuth

pytestmark = pytest.mark.skipif(
    os.getenv("RUN_SQL_ON_FHIR_INVESTIGATION_TESTS", "false").lower() != "true",
    reason="SQL-on-FHIR investigation tests are opt-in (set RUN_SQL_ON_FHIR_INVESTIGATION_TESTS=true)",
)

# =============================================================================
# Configuration
# =============================================================================

# The ViewDefinition resource ID in Aidbox (NOT the sof schema-qualified name).
# Aidbox stores it at /ViewDefinition/patient_view and materializes it
# as a SQL view at sof.patient_view automatically.
VIEW_DEFINITION_NAME = "patient_view"

# Expected columns in the patient_view flat table.
# Aligned with the dk-core projection in
# dagster_etl/DSA/FHIR/SqlOnFhir/view_definitions/patient_view.json:
# wide column set including all identifier slices and demographics. Address
# columns intentionally live in patient_address_view.
PATIENT_VIEW_COLUMNS = {
    "patient_id",
    "patient_ssn",
    "previous_ssn",
    "x_ecpr",
    "d_ecpr",
    "ifdb_patient_id",
    "active",
    "deceased",
    "name_use",
    "name_given",
    "name_family",
    "name_text",
    "gender",
    "birth_date",
}

# ViewDefinition for Encounter resources (used in create/join tests).
# Uses "subject.id" to extract the referenced Patient ID -- this is the Aidbox-compatible
# FHIRPath for generating a foreign key. The SQL on FHIR spec also defines
# getReferenceKey(Patient) for this purpose, but Aidbox 2508 uses subject.id.
ENCOUNTER_VIEW_DEF = {
    "resourceType": "ViewDefinition",
    "name": "encounter_view",
    "resource": "Encounter",
    "status": "active",
    "select": [
        {
            "column": [
                {"name": "encounter_id", "path": "id"},
                {"name": "patient_id", "path": "subject.id"},
                {"name": "status", "path": "status"},
                {"name": "class_code", "path": "class.code"},
                {"name": "period_start", "path": "period.start"},
                {"name": "period_end", "path": "period.end"},
            ]
        }
    ],
}

# ViewDefinition for Condition resources (used in join tests)
CONDITION_VIEW_DEF = {
    "resourceType": "ViewDefinition",
    "name": "condition_view",
    "resource": "Condition",
    "status": "active",
    "select": [
        {
            "column": [
                {"name": "condition_id", "path": "id"},
                {"name": "patient_id", "path": "subject.id"},
                {"name": "clinical_status", "path": "clinicalStatus.coding.code.first()"},
                {"name": "verification_status", "path": "verificationStatus.coding.code.first()"},
                {"name": "recorded_date", "path": "recordedDate"},
            ]
        },
        {
            "forEach": "code.coding",
            "column": [
                {"name": "code", "path": "code"},
                {"name": "code_system", "path": "system"},
                {"name": "code_display", "path": "display"},
            ],
        },
    ],
}


# =============================================================================
# AIDBOX Client Class
# =============================================================================


class AidboxClient:
    """
    HTTP client for interacting with an Aidbox FHIR server.

    Provides methods for:
    - ViewDefinition CRUD (REST API)
    - Querying flat views via the /$sql endpoint
    - Querying FHIR resources via the /fhir/ endpoint
    """

    def __init__(
        self,
        base_url: str | None = None,
        username: str | None = None,
        password: str | None = None,
    ):
        """
        Initialize the Aidbox client.

        Args:
            base_url: The base URL of the FHIR server.
                      Defaults to FHIR_SERVER_URL env var.
            username: Basic auth username.
                      Defaults to FHIR_CLIENT_USERNAME env var.
            password: Basic auth password.
                      Defaults to FHIR_CLIENT_PASSWORD env var.
        """
        self.base_url = (base_url or os.getenv("FHIR_SERVER_URL", "")).rstrip("/")
        self.username = username or os.getenv("FHIR_CLIENT_USERNAME", "root")
        self.password = password or os.getenv("FHIR_CLIENT_PASSWORD", "secret")

        self.session = requests.Session()
        if self.username and self.password:
            self.session.auth = HTTPBasicAuth(self.username, self.password)

        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

    # -------------------------------------------------------------------------
    # ViewDefinition CRUD
    # -------------------------------------------------------------------------

    def get_view_definition(self, view_name: str) -> dict[str, Any]:
        """
        Retrieve a ViewDefinition resource from Aidbox.

        Args:
            view_name: The ID of the ViewDefinition resource.

        Returns:
            The ViewDefinition resource as a dictionary.

        Raises:
            requests.HTTPError: If the request fails.
        """
        url = f"{self.base_url}/ViewDefinition/{view_name}"
        print(f"\n[INFO] Fetching ViewDefinition from: {url}")
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def list_view_definitions(self) -> list[dict[str, Any]]:
        """
        List all ViewDefinition resources.

        Returns:
            A list of ViewDefinition resources.
        """
        url = f"{self.base_url}/ViewDefinition"
        print(f"\n[INFO] Listing ViewDefinitions from: {url}")
        response = self.session.get(url)
        response.raise_for_status()
        bundle = response.json()
        return [entry["resource"] for entry in bundle.get("entry", [])]

    def create_view_definition(self, view_def: dict[str, Any]) -> dict[str, Any]:
        """
        Create or update a ViewDefinition resource via PUT.

        Aidbox automatically materializes the corresponding SQL view in the
        "sof" schema when a ViewDefinition is created or updated.

        Args:
            view_def: The ViewDefinition resource. Must include "name".

        Returns:
            The created/updated ViewDefinition resource.

        Raises:
            requests.HTTPError: If the request fails.
        """
        name = view_def["name"]
        url = f"{self.base_url}/ViewDefinition/{name}"
        print(f"\n[INFO] Creating/updating ViewDefinition at: {url}")
        response = self.session.put(url, json=view_def)
        response.raise_for_status()
        return response.json()

    def delete_view_definition(self, view_name: str) -> None:
        """
        Delete a ViewDefinition resource. Also drops the materialized SQL view.

        Args:
            view_name: The ID of the ViewDefinition to delete.
        """
        url = f"{self.base_url}/ViewDefinition/{view_name}"
        print(f"\n[INFO] Deleting ViewDefinition at: {url}")
        response = self.session.delete(url)
        response.raise_for_status()

    def materialize_view(
        self,
        view_name: str,
        materialization_type: str = "view",
        schema: str = "sof",
    ) -> dict[str, Any]:
        """
        Trigger materialization of a ViewDefinition as a SQL view/table.

        In Aidbox 2508+, creating a ViewDefinition via REST does NOT auto-materialize
        the SQL view. You must call the $materialize operation explicitly via the
        /fhir/ endpoint.

        Args:
            view_name: The ID of the ViewDefinition to materialize.
            materialization_type: One of "view", "table", or "materialized-view".
            schema: The PostgreSQL schema to create the view in (default: "sof").

        Returns:
            The Parameters response with viewName, viewType, and viewSchema.

        Raises:
            requests.HTTPError: If materialization fails.
        """
        url = f"{self.base_url}/fhir/ViewDefinition/{view_name}/$materialize"
        body = {
            "resourceType": "Parameters",
            "parameter": [
                {"name": "type", "valueCode": materialization_type},
            ],
        }
        print(
            f"\n[INFO] Materializing ViewDefinition '{view_name}' as {materialization_type} in {schema}"
        )
        response = self.session.post(url, json=body)
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # SQL on FHIR: Query flat views via /$sql
    # -------------------------------------------------------------------------

    def query_sql(self, sql: str) -> list[dict[str, Any]]:
        """
        Execute a SQL query against the Aidbox PostgreSQL database via /$sql.

        This is the primary way to query flat views materialized by ViewDefinitions.
        The views live in the "sof" schema (e.g., "SELECT * FROM sof.patient_view").

        Args:
            sql: The SQL query string to execute.

        Returns:
            A list of row dictionaries.

        Raises:
            requests.HTTPError: If the request fails.
        """
        url = f"{self.base_url}/$sql"
        print(f"\n[INFO] Executing SQL via /$sql: {sql[:120]}...")
        response = self.session.post(url, json=[sql], params={"_format": "json"})
        response.raise_for_status()
        return response.json()

    # -------------------------------------------------------------------------
    # FHIR REST API helpers
    # -------------------------------------------------------------------------

    def get_patients(self, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Query Patient resources via the FHIR REST API.

        Args:
            params: Optional FHIR search parameters.

        Returns:
            A FHIR Bundle containing Patient resources.
        """
        url = f"{self.base_url}/fhir/Patient"
        print(f"\n[INFO] Querying Patients from: {url}")
        response = self.session.get(url, params=params or {})
        response.raise_for_status()
        return response.json()

    def check_health(self) -> dict[str, Any]:
        """
        Check Aidbox server health.

        Returns:
            The health check response including version info.
        """
        url = f"{self.base_url}/health"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()


# =============================================================================
# Helper Functions
# =============================================================================


def _display_tabular_results(rows: list[dict[str, Any]], max_rows: int = 20) -> None:
    """
    Display query results in a readable tabular format.

    Args:
        rows: List of row dictionaries from a SQL query.
        max_rows: Maximum number of rows to display.
    """
    if not rows:
        print("  (no rows returned)")
        return

    columns = list(rows[0].keys())
    header = " | ".join(f"{col:20}" for col in columns)
    print(header)
    print("-" * len(header))

    for row in rows[:max_rows]:
        row_str = " | ".join(f"{str(row.get(col, '') or ''):20}" for col in columns)
        print(row_str)

    if len(rows) > max_rows:
        print(f"\n... and {len(rows) - max_rows} more rows")


def _ensure_view_exists(client: AidboxClient, view_def: dict[str, Any]) -> None:
    """
    Ensure a ViewDefinition exists and is materialized in the sof schema.

    Creates the ViewDefinition if it doesn't exist, then checks whether the
    corresponding SQL view is materialized. If not, triggers materialization
    via the /fhir/ViewDefinition/{id}/$materialize operation.

    Args:
        client: The AidboxClient instance.
        view_def: The ViewDefinition resource dict (must include "name").
    """
    view_name = view_def["name"]

    # Create the ViewDefinition resource if it doesn't exist
    try:
        client.get_view_definition(view_name)
        print(f"[INFO] ViewDefinition '{view_name}' already exists")
    except requests.exceptions.HTTPError:
        print(f"[INFO] Creating ViewDefinition '{view_name}'...")
        client.create_view_definition(view_def)

    # Check if the SQL view is materialized
    tables = client.query_sql(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = 'sof' AND table_name = '{view_name}'"
    )
    if not tables:
        print(f"[INFO] Materializing '{view_name}' in sof schema...")
        client.materialize_view(view_name)
    else:
        print(f"[INFO] SQL view sof.{view_name} already materialized")


# =============================================================================
# Test Functions
# =============================================================================


def test_aidbox_connection():
    """
    Test basic connectivity to the Aidbox server.

    Verifies that the server is reachable and returns health/version info.
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print("TEST: Aidbox Connection")
    print(f"{'=' * 60}")
    print(f"Base URL: {client.base_url}")

    try:
        health = client.check_health()
        about = health.get("about", {})

        print("[SUCCESS] Connected to Aidbox server!")
        print(f"  - Status: {health.get('status', 'N/A')}")
        print(f"  - Version: {about.get('version', 'N/A')}")
        print(f"  - FHIR Version: {about.get('zen-fhir-version', 'N/A')}")
        return True

    except requests.exceptions.ConnectionError as e:
        print(f"[ERROR] Could not connect to Aidbox at {client.base_url}")
        print(f"Details: {e}")
        return False

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] HTTP error: {e}")
        return False


def test_get_view_definition():
    """
    Test retrieving the patient_view ViewDefinition resource.

    Verifies the ViewDefinition exists and its structure matches the
    SQL on FHIR spec (resource, name, select with column and forEach).
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print(f"TEST: Get ViewDefinition '{VIEW_DEFINITION_NAME}'")
    print(f"{'=' * 60}")

    try:
        view_def = client.get_view_definition(VIEW_DEFINITION_NAME)

        print("[SUCCESS] Retrieved ViewDefinition!")
        print(f"  - ID: {view_def.get('id', 'N/A')}")
        print(f"  - Resource Type: {view_def.get('resourceType', 'N/A')}")
        print(f"  - Target Resource: {view_def.get('resource', 'N/A')}")
        print(f"  - Name: {view_def.get('name', 'N/A')}")
        print(f"  - Status: {view_def.get('status', 'N/A')}")

        # Show column definitions
        for i, select_block in enumerate(view_def.get("select", [])):
            columns = select_block.get("column", [])
            if columns:
                print(f"\n  Select block {i} columns:")
                for col in columns:
                    print(f"    - {col.get('name', '?')}: {col.get('path', '?')}")

            # Show nested selects with forEach (SQL on FHIR unnesting)
            nested_selects = select_block.get("select", [])
            for nested in nested_selects:
                for_each = nested.get("forEach", "")
                nested_cols = nested.get("column", [])
                print(f"\n  Nested select (forEach: {for_each}):")
                for col in nested_cols:
                    print(f"    - {col.get('name', '?')}: {col.get('path', '?')}")

        print("\nFull ViewDefinition:")
        print(json.dumps(view_def, indent=2))
        return view_def

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"[ERROR] ViewDefinition '{VIEW_DEFINITION_NAME}' not found!")
            print("Hint: The resource ID should NOT include the 'sof.' schema prefix.")
        else:
            print(f"[ERROR] HTTP {e.response.status_code}: {e.response.text[:500]}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def test_query_view_via_sql():
    """
    Test querying the patient_view flat table via the Aidbox /$sql endpoint.

    This is the primary way to consume SQL on FHIR views in Aidbox:
    ViewDefinitions are auto-materialized as SQL views in the "sof" schema,
    and can be queried with standard SQL via POST /$sql.
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print(f"TEST: Query View via /$sql (sof.{VIEW_DEFINITION_NAME})")
    print(f"{'=' * 60}")

    try:
        # rows = client.query_sql(f"SELECT * FROM sof.{VIEW_DEFINITION_NAME} LIMIT 10")
        rows = client.query_sql(f"SELECT * FROM sof.{VIEW_DEFINITION_NAME}")

        print(f"[SUCCESS] Got {len(rows)} rows from sof.{VIEW_DEFINITION_NAME}")
        print()
        _display_tabular_results(rows)

        # Verify columns match expected schema
        if rows:
            actual_columns = set(rows[0].keys())
            print(f"\n  Actual columns: {sorted(actual_columns)}")
            print(f"  Expected columns: {sorted(PATIENT_VIEW_COLUMNS)}")
            missing = PATIENT_VIEW_COLUMNS - actual_columns
            extra = actual_columns - PATIENT_VIEW_COLUMNS
            if missing:
                print(f"  [WARN] Missing columns: {missing}")
            if extra:
                print(f"  [INFO] Extra columns: {extra}")

        return rows

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] SQL query failed: {e}")
        print(f"Response: {e.response.text[:500]}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def test_create_view_definition():
    """
    Test creating a new ViewDefinition via the REST API.

    Creates an encounter_view ViewDefinition and then explicitly materializes it
    using the /fhir/ViewDefinition/{id}/$materialize operation. In Aidbox 2508+,
    creating a ViewDefinition via REST does NOT auto-materialize; the $materialize
    call is required.
    """
    client = AidboxClient()
    view_name = ENCOUNTER_VIEW_DEF["name"]
    print(f"\n{'=' * 60}")
    print(f"TEST: Create ViewDefinition '{view_name}'")
    print(f"{'=' * 60}")

    try:
        # Step 1: Create the ViewDefinition resource
        result = client.create_view_definition(ENCOUNTER_VIEW_DEF)
        print(f"[SUCCESS] Created ViewDefinition '{view_name}'!")
        print(f"  - ID: {result.get('id', 'N/A')}")
        print(f"  - Resource: {result.get('resource', 'N/A')}")

        # Step 2: Materialize the SQL view in the sof schema
        mat_result = client.materialize_view(view_name)
        print("[SUCCESS] Materialized view!")
        for param in mat_result.get("parameter", []):
            print(f"  - {param.get('name')}: {param.get('valueString', 'N/A')}")

        # Step 3: Verify the view exists and has data
        tables = client.query_sql(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = 'sof' AND table_name = '{view_name}'"
        )
        view_exists = len(tables) > 0
        print(f"  - Materialized in sof schema: {view_exists}")

        if view_exists:
            rows = client.query_sql(f"SELECT * FROM sof.{view_name} LIMIT 5")
            print(f"  - Sample rows: {len(rows)}")
            if rows:
                print()
                _display_tabular_results(rows, max_rows=5)

        return result

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] Failed to create ViewDefinition: {e}")
        print(f"Response: {e.response.text[:500]}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def test_cross_resource_join():
    """
    Test cross-resource SQL JOIN using SQL on FHIR flat views.

    Demonstrates the key SQL on FHIR analytics pattern from the Nature paper
    (https://www.nature.com/articles/s41746-025-01708-w):
    - Create flat views for Patient and Encounter resources
    - Join them using standard SQL on the patient_id key
    - This uses getReferenceKey(Patient) in the Encounter ViewDefinition
      to generate a foreign key that matches the Patient view's id column.
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print("TEST: Cross-Resource JOIN (Patient + Encounter)")
    print(f"{'=' * 60}")

    try:
        # Ensure the encounter_view exists and is materialized
        _ensure_view_exists(client, ENCOUNTER_VIEW_DEF)

        # Execute a cross-resource JOIN query via /$sql
        join_sql = """
            SELECT
                p.patient_id,
                p.name_given,
                p.name_family,
                p.gender,
                p.birth_date,
                e.encounter_id,
                e.status AS encounter_status,
                e.class_code,
                e.period_start,
                e.period_end
            FROM sof.patient_view p
            JOIN sof.encounter_view e ON e.patient_id = p.patient_id
            ORDER BY p.name_family, p.name_given, e.period_start
            LIMIT 20
        """

        rows = client.query_sql(join_sql)
        print(f"[SUCCESS] JOIN returned {len(rows)} rows")
        print()
        _display_tabular_results(rows)

        # Also show a count summary
        count_sql = """
            SELECT
                COUNT(*) AS total_joined_rows,
                COUNT(DISTINCT p.patient_id) AS distinct_patients,
                COUNT(DISTINCT e.encounter_id) AS distinct_encounters
            FROM sof.patient_view p
            JOIN sof.encounter_view e ON e.patient_id = p.patient_id
        """
        counts = client.query_sql(count_sql)
        if counts:
            print("\n  Join summary:")
            print(f"    - Total joined rows: {counts[0].get('total_joined_rows')}")
            print(f"    - Distinct patients: {counts[0].get('distinct_patients')}")
            print(f"    - Distinct encounters: {counts[0].get('distinct_encounters')}")

        return rows

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] JOIN query failed: {e}")
        print(f"Response: {e.response.text[:500]}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def test_view_row_count_vs_resources():
    """
    Test comparing flat view row count to FHIR resource count.

    Demonstrates the forEach unnesting behavior in SQL on FHIR:
    the patient_view uses "forEach: name" which generates one row
    per name entry per Patient. Patients with multiple names produce
    multiple rows, so the view row count may exceed the resource count.
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print("TEST: View Row Count vs FHIR Resource Count")
    print(f"{'=' * 60}")

    try:
        # Get row count from the flat view
        view_count_result = client.query_sql(
            f"SELECT count(*) AS row_count FROM sof.{VIEW_DEFINITION_NAME}"
        )
        view_rows = view_count_result[0]["row_count"] if view_count_result else 0

        # Get distinct patient count from the flat view
        distinct_result = client.query_sql(
            f"SELECT count(DISTINCT patient_id) AS patient_count "
            f"FROM sof.{VIEW_DEFINITION_NAME}"
        )
        view_patients = distinct_result[0]["patient_count"] if distinct_result else 0

        # Get total Patient resource count from the FHIR table
        resource_count_result = client.query_sql("SELECT count(*) AS total FROM patient")
        resource_count = resource_count_result[0]["total"] if resource_count_result else 0

        print(f"  Patient resources in FHIR DB:       {resource_count}")
        print(f"  Distinct patients in flat view:      {view_patients}")
        print(f"  Total rows in flat view:             {view_rows}")
        print(f"  Expansion factor (forEach: name):    {view_rows / max(view_patients, 1):.2f}x")

        if view_rows > view_patients:
            print(
                "\n  [INFO] Row count > patient count because 'forEach: name' "
                "unnests multiple name entries per patient into separate rows."
            )
        elif view_rows == view_patients:
            print("\n  [INFO] Each patient has exactly one name entry.")

        return {
            "resource_count": resource_count,
            "view_patients": view_patients,
            "view_rows": view_rows,
        }

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] Count query failed: {e}")
        print(f"Response: {e.response.text[:500]}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


def test_query_patients_direct():
    """
    Test querying Patient resources directly via the FHIR REST API.

    Serves as a baseline comparison: shows the raw FHIR JSON structure
    vs. the flattened SQL on FHIR view output.
    """
    client = AidboxClient()
    print(f"\n{'=' * 60}")
    print("TEST: Query Patients via FHIR REST API (baseline)")
    print(f"{'=' * 60}")

    try:
        result = client.get_patients(params={"_count": 3})
        total = result.get("total", "unknown")
        entries = result.get("entry", [])

        print(f"[SUCCESS] FHIR Patient search returned {len(entries)} entries (total: {total})")

        for i, entry in enumerate(entries[:3]):
            patient = entry.get("resource", {})
            print(f"\n  Patient {i + 1}:")
            print(f"    - ID: {patient.get('id', 'N/A')}")
            print(f"    - Gender: {patient.get('gender', 'N/A')}")
            print(f"    - Birth Date: {patient.get('birthDate', 'N/A')}")
            print(f"    - Active: {patient.get('active', 'N/A')}")

            names = patient.get("name", [])
            for name in names:
                given = " ".join(name.get("given", []))
                family = name.get("family", "")
                print(f"    - Name: {given} {family}".strip())

            identifiers = patient.get("identifier", [])
            for ident in identifiers:
                print(f"    - Identifier: {ident.get('system', '?')} = {ident.get('value', '?')}")

        return result

    except requests.exceptions.HTTPError as e:
        print(f"[ERROR] FHIR query failed: {e}")
        return None

    except Exception as e:
        print(f"[ERROR] Unexpected error: {e}")
        return None


# =============================================================================
# Main Execution
# =============================================================================


def run_all_tests():
    """Run all tests in sequence and print a summary."""
    print(f"\n{'#' * 60}")
    print("# SQL on FHIR Investigation Test Suite")
    print(f"{'#' * 60}")

    results = []

    # Test 1: Connection
    results.append(("Connection Test", test_aidbox_connection()))

    # Test 2: Get ViewDefinition
    view_def = test_get_view_definition()
    results.append(("Get ViewDefinition", view_def is not None))

    # Test 3: Query view via /$sql
    view_rows = test_query_view_via_sql()
    results.append(("Query View via /$sql", view_rows is not None and len(view_rows) > 0))

    # Test 4: Row count vs resource count
    counts = test_view_row_count_vs_resources()
    results.append(("View Row Count vs Resources", counts is not None))

    # Test 5: Create ViewDefinition
    encounter_def = test_create_view_definition()
    results.append(("Create ViewDefinition", encounter_def is not None))

    # Test 6: Cross-resource JOIN
    join_result = test_cross_resource_join()
    results.append(("Cross-Resource JOIN", join_result is not None and len(join_result) > 0))

    # Test 7: Direct FHIR REST query (baseline)
    patients = test_query_patients_direct()
    results.append(("Query Patients Direct", patients is not None))

    # Summary
    print(f"\n{'=' * 60}")
    print("TEST SUMMARY")
    print(f"{'=' * 60}")
    for name, passed in results:
        status = "PASSED" if passed else "FAILED"
        print(f"  [{status}] {name}")

    all_passed = all(passed for _, passed in results)
    print(f"\nOverall: {'All tests passed!' if all_passed else 'Some tests failed.'}")

    return all_passed


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)


# =============================================================================
# Pytest Test Cases
# =============================================================================


@pytest.mark.integration
class TestSQLOnFHIR:
    """Pytest test class for SQL on FHIR investigation."""

    def test_aidbox_connection(self):
        """Test that we can connect to the Aidbox server."""
        assert test_aidbox_connection(), "Failed to connect to Aidbox server"

    def test_get_view_definition(self):
        """Test that we can retrieve the patient_view ViewDefinition."""
        view_def = test_get_view_definition()
        assert view_def is not None, "Failed to retrieve ViewDefinition"
        assert view_def.get("id") == VIEW_DEFINITION_NAME, "ViewDefinition ID mismatch"
        assert view_def.get("resourceType") == "ViewDefinition"
        assert view_def.get("resource") == "Patient"
        assert view_def.get("name") == VIEW_DEFINITION_NAME

    def test_query_view_via_sql(self):
        """Test querying the flat view via the /$sql endpoint."""
        rows = test_query_view_via_sql()
        assert rows is not None, "Failed to query view"
        assert len(rows) > 0, "View returned no rows"
        actual_columns = set(rows[0].keys())
        assert PATIENT_VIEW_COLUMNS.issubset(
            actual_columns
        ), f"Missing columns: {PATIENT_VIEW_COLUMNS - actual_columns}"

    def test_view_row_count_vs_resources(self):
        """Test that view row count relates correctly to resource count."""
        counts = test_view_row_count_vs_resources()
        assert counts is not None, "Failed to get counts"
        assert counts["resource_count"] > 0, "No Patient resources found"
        assert counts["view_rows"] > 0, "View has no rows"
        assert (
            counts["view_patients"] <= counts["resource_count"]
        ), "Distinct patients in view should not exceed total resources"

    def test_create_view_definition(self):
        """Test creating a new ViewDefinition and verifying materialization."""
        result = test_create_view_definition()
        assert result is not None, "Failed to create ViewDefinition"
        assert result.get("resourceType") == "ViewDefinition"
        assert result.get("resource") == "Encounter"

    def test_cross_resource_join(self):
        """Test cross-resource SQL JOIN on flat views."""
        rows = test_cross_resource_join()
        assert rows is not None, "Failed to execute cross-resource join"
        assert len(rows) > 0, "Join returned no rows"
        # Verify joined columns from both views are present
        expected_join_cols = {"patient_id", "given", "family", "encounter_id", "encounter_status"}
        actual_cols = set(rows[0].keys())
        assert expected_join_cols.issubset(
            actual_cols
        ), f"Missing columns in join: {expected_join_cols - actual_cols}"

    def test_query_patients_direct(self):
        """Test querying Patients directly via FHIR REST API."""
        result = test_query_patients_direct()
        assert result is not None, "Failed to query Patients"
        assert result.get("resourceType") == "Bundle"
