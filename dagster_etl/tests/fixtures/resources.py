"""
Resource fixtures for testing - supports both mock and real resources.

This module provides:
- Factory functions for creating mock resources (DRY pattern)
- Context manager for environment variable overrides
- Mock and real fixtures for all database types
- Cleanup utilities for test isolation
"""

import os
from contextlib import contextmanager
from unittest.mock import MagicMock, Mock

import pytest
from core.resources import (
    FHIRServerResource,
    IFDBOracleResource,
    ILSMSSQLResource,
    MSSQLResource,
    MySupplyMSSQLResource,
    SydaxMSSQLResource,
)

# =============================================================================
# Environment Helpers
# =============================================================================


def should_use_real_resources() -> bool:
    """Check if tests should use real resources based on environment."""
    return os.getenv("USE_REAL_DB_FOR_TESTS", "false").lower() == "true"


def is_safe_environment() -> bool:
    """Check if current environment is safe for real database connections."""
    env = os.getenv("TEST_ENV", "").lower()
    dagster_env = os.getenv("DAGSTER_ENVIRONMENT", "").lower()

    # Refuse to use real resources in production
    if "prod" in env or "prod" in dagster_env:
        return False

    # Allow in dev/test/local environments
    return env in ("dev", "test", "development", "local", "") or dagster_env in (
        "dev",
        "test",
        "development",
        "local",
    )


@contextmanager
def env_var_override(test_to_prod_mapping: dict[str, str]):
    """
    Context manager to temporarily override PROD_* variables with TEST_* values.

    Args:
        test_to_prod_mapping: Dict mapping TEST_* variable names to PROD_* variable names.
                              If a TEST_* variable is set, it will temporarily override
                              the corresponding PROD_* variable.

    Example:
        >>> with env_var_override({"TEST_HOST": "PROD_HOST"}):
        ...     # PROD_HOST now has the value of TEST_HOST (if TEST_HOST is set)
        ...     resource = SomeResource()
    """
    original_env = {}

    # Override PROD variables with TEST values if TEST values exist
    for test_var, prod_var in test_to_prod_mapping.items():
        test_value = os.getenv(test_var)
        if test_value:
            original_env[prod_var] = os.getenv(prod_var)
            os.environ[prod_var] = test_value

    try:
        yield
    finally:
        # Restore original environment
        for prod_var, original_value in original_env.items():
            if original_value is None:
                os.environ.pop(prod_var, None)
            else:
                os.environ[prod_var] = original_value


def _skip_if_not_real_or_unsafe():
    """Helper to skip tests when real resources aren't enabled or environment is unsafe."""
    if not should_use_real_resources():
        pytest.skip("USE_REAL_DB_FOR_TESTS not enabled")
    if not is_safe_environment():
        pytest.skip("Not safe to use real database in current environment")


# =============================================================================
# Table Definitions for Cleanup
# =============================================================================

# Ordered list so that child tables (facts) are cleared before parents (maps/dims)
ERP_TARGET_TABLES = (
    ("fct", "Fakturalinje"),
    ("fct", "Postering"),
    ("dim", "Kreditor"),
    ("dim", "Kontoplan"),
    ("dim", "Vare"),
    ("map", "Vare"),
    ("map", "Kreditor"),
    ("map", "Kontoplan"),
)

# ERP staging tables
ERP_STAGING_TABLES = (
    ("stg", "statistic_invoice_line_mysupply"),
    ("stg", "statistic_invoice_header_mysupply"),
    ("stg", "metadata_mysupply"),
    ("stg", "kreditor_sydax"),
    ("stg", "finanskonto_sydax"),
    ("stg", "finanspostering_sydax"),
    ("stg", "lev_ils"),
    ("stg", "vare_ils"),
)

# ERP map tables
ERP_MAP_TABLES = (
    ("map", "Vare"),
    ("map", "Kreditor"),
    ("map", "Kontoplan"),
)

# ERP dimension tables
ERP_DIM_TABLES = (
    ("dim", "Vare"),
    ("dim", "Kreditor"),
    ("dim", "Kontoplan"),
)

# ERP fact tables
ERP_FACT_TABLES = (
    ("fct", "Fakturalinje"),
    ("fct", "Postering"),
)


# =============================================================================
# Mock Resource Factory Functions
# =============================================================================


def _create_mock_mssql_resource(
    resource_class: type,
    host: str,
    database: str,
) -> tuple[Mock, MagicMock]:
    """
    Factory function for creating mock MSSQL resources.

    Args:
        resource_class: The resource class to mock (e.g., MSSQLResource, MySupplyMSSQLResource)
        host: Mock host name
        database: Mock database name

    Returns:
        Tuple of (mock_resource, mock_cursor)
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = lambda _: mock_cursor
    mock_conn.cursor.return_value.__exit__ = lambda *args: None

    resource = Mock(spec=resource_class)
    resource.get_connection = lambda: mock_conn
    resource.host = host
    resource.database = database

    return resource, mock_cursor


def _create_mock_oracle_resource() -> tuple[Mock, MagicMock]:
    """
    Factory function for creating mock Oracle resources.

    Returns:
        Tuple of (mock_resource, mock_cursor)
    """
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    resource = Mock(spec=IFDBOracleResource)

    @contextmanager
    def mock_connection():
        yield mock_conn

    resource.get_connection = mock_connection
    return resource, mock_cursor


# =============================================================================
# Mock Fixtures
# =============================================================================


@pytest.fixture
def mock_mssql_resource():
    """Mock MSSQL resource for unit tests."""
    return _create_mock_mssql_resource(MSSQLResource, "mock_host", "mock_db")


@pytest.fixture
def mock_fhir_server_resource():
    """Mock FHIR Server resource for unit tests."""
    resource = Mock(spec=FHIRServerResource)
    resource.base_url = "http://mock-fhir-server"
    resource.post_resource = Mock(return_value=Mock(status_code=201))
    resource.resolve_id_by_identifier = Mock(return_value=None)
    resource.attach_id_from_identifiers = lambda resource, **kw: resource
    return resource


@pytest.fixture
def mock_oracle_resource():
    """Mock Oracle resource for unit tests."""
    return _create_mock_oracle_resource()


@pytest.fixture
def mock_mysupply_mssql_resource():
    """Mock MySupply MSSQL resource for unit tests."""
    return _create_mock_mssql_resource(
        MySupplyMSSQLResource, "mock_mysupply_host", "mock_mysupply_db"
    )


@pytest.fixture
def mock_sydax_mssql_resource():
    """Mock Sydax MSSQL resource for unit tests."""
    return _create_mock_mssql_resource(SydaxMSSQLResource, "mock_sydax_host", "mock_sydax_db")


@pytest.fixture
def mock_ils_mssql_resource():
    """Mock ILS MSSQL resource for unit tests."""
    return _create_mock_mssql_resource(ILSMSSQLResource, "mock_ils_host", "mock_ils_db")


# =============================================================================
# Real Resource Fixtures
# =============================================================================


@pytest.fixture
def real_mssql_source_dso_resource():
    """
    Real MSSQL source DSO resource for integration tests.
    Used for reading from the EPJ/IFDB source database.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    """
    _skip_if_not_real_or_unsafe()

    host = os.getenv("TEST_MSSQL_SOURCE_DSO_HOST") or os.getenv("MSSQL_SOURCE_DSO_HOST", "mssql")
    port = int(
        os.getenv("TEST_MSSQL_SOURCE_DSO_PORT") or os.getenv("MSSQL_SOURCE_DSO_PORT", "1433")
    )
    database = os.getenv("TEST_MSSQL_SOURCE_DSO_DB") or os.getenv("MSSQL_SOURCE_DSO_DB", "IFDB_EPJ")
    user = os.getenv("TEST_MSSQL_SOURCE_DSO_USER") or os.getenv("MSSQL_SOURCE_DSO_USER", "sa")
    password = os.getenv("TEST_MSSQL_SOURCE_DSO_PASSWORD") or os.getenv(
        "MSSQL_SOURCE_DSO_PASSWORD", ""
    )

    return MSSQLResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


# Alias for backward compatibility
@pytest.fixture
def real_mssql_resource(real_mssql_source_dso_resource):
    """Alias for real_mssql_source_dso_resource (backward compatibility)."""
    return real_mssql_source_dso_resource


@pytest.fixture
def real_fhir_server_resource():
    """
    Real FHIR Server resource for integration tests.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    """
    _skip_if_not_real_or_unsafe()

    base_url = os.getenv("TEST_FHIR_SERVER_URL") or os.getenv(
        "FHIR_SERVER_URL", "http://fhir-server:8888"
    )
    username = os.getenv("TEST_FHIR_CLIENT_USERNAME") or os.getenv("FHIR_CLIENT_USERNAME")
    password = os.getenv("TEST_FHIR_CLIENT_PASSWORD") or os.getenv("FHIR_CLIENT_PASSWORD")
    use_fhir_api = os.getenv("USE_FHIR_API")

    return FHIRServerResource(
        base_url=base_url,
        client_username=username,
        client_password=password,
        use_fhir_api=use_fhir_api,
    )


@pytest.fixture
def real_fhir_database_resource():
    """
    Real FHIR Database (PostgreSQL) resource for integration tests.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.

    This provides direct database access for performance-critical operations
    like batch ID resolution.
    """
    from core.resources import FHIRDatabaseResource

    _skip_if_not_real_or_unsafe()

    host = os.getenv("FHIR_DATABASE_HOST") or os.getenv("PG_FHIR_HOST", "fhir-server-db")
    port = int(os.getenv("FHIR_DATABASE_PORT") or os.getenv("PG_FHIR_PORT", "5432"))
    database = os.getenv("FHIR_DATABASE_NAME") or os.getenv("PG_FHIR_DATABASE", "aidbox")
    user = os.getenv("FHIR_DATABASE_USER") or os.getenv("PG_FHIR_USER", "postgres_user")
    password = os.getenv("FHIR_DATABASE_PASSWORD") or os.getenv(
        "PG_FHIR_PASSWORD", "postgres_password"
    )

    return FHIRDatabaseResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


@pytest.fixture(scope="module")
def real_mssql_target_staging_resource():
    """
    Real MSSQL target staging (DSI/DSA layer) resource for integration tests.
    Used for writing to the staging database (stg, map, dim, fct tables).
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.

    Uses the default MSSQLResource connection behavior to match production.
    """
    _skip_if_not_real_or_unsafe()

    host = os.getenv("TEST_MSSQL_TARGET_STAGING_HOST") or os.getenv(
        "MSSQL_TARGET_STAGING_HOST", "localhost"
    )
    port = int(
        os.getenv("TEST_MSSQL_TARGET_STAGING_PORT")
        or os.getenv("MSSQL_TARGET_STAGING_PORT", "1433")
    )
    database = os.getenv("TEST_MSSQL_TARGET_STAGING_DB") or os.getenv(
        "MSSQL_TARGET_STAGING_DB", "DW"
    )
    user = os.getenv("TEST_MSSQL_TARGET_STAGING_USER") or os.getenv(
        "MSSQL_TARGET_STAGING_USER", "sa"
    )
    password = os.getenv("TEST_MSSQL_TARGET_STAGING_PASSWORD") or os.getenv(
        "MSSQL_TARGET_STAGING_PASSWORD", ""
    )

    return MSSQLResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


# Alias for backward compatibility
@pytest.fixture(scope="module")
def real_mssql_target_resource(real_mssql_target_staging_resource):
    """Alias for real_mssql_target_staging_resource (backward compatibility)."""
    return real_mssql_target_staging_resource


@pytest.fixture
def real_mssql_source_staging_resource():
    """
    Real MSSQL source staging resource for integration tests.
    Used for reading from the staging database.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    """
    _skip_if_not_real_or_unsafe()

    host = os.getenv("TEST_MSSQL_SOURCE_STAGING_HOST") or os.getenv(
        "MSSQL_SOURCE_STAGING_HOST", "localhost"
    )
    port = int(
        os.getenv("TEST_MSSQL_SOURCE_STAGING_PORT")
        or os.getenv("MSSQL_SOURCE_STAGING_PORT", "1433")
    )
    database = os.getenv("TEST_MSSQL_SOURCE_STAGING_DB") or os.getenv(
        "MSSQL_SOURCE_STAGING_DB", "DW"
    )
    user = os.getenv("TEST_MSSQL_SOURCE_STAGING_USER") or os.getenv(
        "MSSQL_SOURCE_STAGING_USER", "sa"
    )
    password = os.getenv("TEST_MSSQL_SOURCE_STAGING_PASSWORD") or os.getenv(
        "MSSQL_SOURCE_STAGING_PASSWORD", ""
    )

    return MSSQLResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


@pytest.fixture
def real_mssql_target_dso_resource():
    """
    Real MSSQL target DSO (reporting layer) resource for integration tests.
    Used for writing to the DSO/Report database.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    """
    _skip_if_not_real_or_unsafe()

    host = os.getenv("TEST_MSSQL_TARGET_DSO_HOST") or os.getenv(
        "MSSQL_TARGET_DSO_HOST", "localhost"
    )
    port = int(
        os.getenv("TEST_MSSQL_TARGET_DSO_PORT") or os.getenv("MSSQL_TARGET_DSO_PORT", "1433")
    )
    database = os.getenv("TEST_MSSQL_TARGET_DSO_DB") or os.getenv("MSSQL_TARGET_DSO_DB", "DW")
    user = os.getenv("TEST_MSSQL_TARGET_DSO_USER") or os.getenv("MSSQL_TARGET_DSO_USER", "sa")
    password = os.getenv("TEST_MSSQL_TARGET_DSO_PASSWORD") or os.getenv(
        "MSSQL_TARGET_DSO_PASSWORD", ""
    )

    return MSSQLResource(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )


# Environment variable mappings for each resource type
_MYSUPPLY_ENV_VARS = {
    "TEST_MYSUPPLY_MSSQL_SERVER": "MYSUPPLY_PROD_MSSQL_SERVER",
    "TEST_MYSUPPLY_MSSQL_PORT": "MYSUPPLY_PROD_MSSQL_PORT",
    "TEST_MYSUPPLY_MSSQL_DATABASE": "MYSUPPLY_PROD_MSSQL_DATABASE",
    "TEST_MYSUPPLY_MSSQL_USERNAME": "MYSUPPLY_PROD_MSSQL_USERNAME",
    "TEST_MYSUPPLY_MSSQL_PASSWORD": "MYSUPPLY_PROD_MSSQL_PASSWORD",
}

_SYDAX_ENV_VARS = {
    "TEST_SYDAX_MSSQL_SERVER": "SYDAX_PROD_MSSQL_SERVER",
    "TEST_SYDAX_MSSQL_PORT": "SYDAX_PROD_MSSQL_PORT",
    "TEST_SYDAX_MSSQL_DATABASE": "SYDAX_PROD_MSSQL_DATABASE",
    "TEST_SYDAX_MSSQL_USERNAME": "SYDAX_PROD_MSSQL_USERNAME",
    "TEST_SYDAX_MSSQL_PASSWORD": "SYDAX_PROD_MSSQL_PASSWORD",
}

_ILS_ENV_VARS = {
    "TEST_ILS_MSSQL_SERVER": "ILS_PROD_MSSQL_SERVER",
    "TEST_ILS_MSSQL_PORT": "ILS_PROD_MSSQL_PORT",
    "TEST_ILS_MSSQL_DATABASE": "ILS_PROD_MSSQL_DATABASE",
    "TEST_ILS_MSSQL_USERNAME": "ILS_PROD_MSSQL_USERNAME",
    "TEST_ILS_MSSQL_PASSWORD": "ILS_PROD_MSSQL_PASSWORD",
}

_IFDB_ORACLE_ENV_VARS = {
    "TEST_IFDB_ORACLE_SERVER": "IFDB_PROD_ORACLE_SERVER",
    "TEST_IFDB_ORACLE_PORT": "IFDB_PROD_ORACLE_PORT",
    "TEST_IFDB_ORACLE_DATABASE": "IFDB_PROD_ORACLE_DATABASE",
    "TEST_IFDB_ORACLE_USERNAME": "IFDB_PROD_ORACLE_USERNAME",
    "TEST_IFDB_ORACLE_PASSWORD": "IFDB_PROD_ORACLE_PASSWORD",
    "TEST_IFDB_ORACLE_SCHEMA": "IFDB_PROD_ORACLE_SCHEMA",
}


@pytest.fixture
def real_mysupply_mssql_resource():
    """
    Real MySupply MSSQL resource for integration tests.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    TEST_* variables take precedence over PROD_* variables.
    """
    _skip_if_not_real_or_unsafe()

    with env_var_override(_MYSUPPLY_ENV_VARS):
        return MySupplyMSSQLResource()


@pytest.fixture
def real_sydax_mssql_resource():
    """
    Real Sydax MSSQL resource for integration tests.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    TEST_* variables take precedence over PROD_* variables.
    """
    _skip_if_not_real_or_unsafe()

    with env_var_override(_SYDAX_ENV_VARS):
        return SydaxMSSQLResource()


@pytest.fixture
def real_ils_mssql_resource():
    """
    Real ILS MSSQL resource for integration tests.
    Only creates resource if USE_REAL_DB_FOR_TESTS=true and environment is safe.
    TEST_* variables take precedence over PROD_* variables.
    """
    _skip_if_not_real_or_unsafe()

    with env_var_override(_ILS_ENV_VARS):
        return ILSMSSQLResource()


# =============================================================================
# Flexible Fixtures (Auto-select Mock or Real)
# =============================================================================


@pytest.fixture
def mssql_resource(request, mock_mssql_resource):
    """
    Flexible MSSQL resource - auto-selects mock or real based on configuration.
    Use this fixture when you want tests to work with either.
    """
    if should_use_real_resources() and is_safe_environment():
        try:
            return request.getfixturevalue("real_mssql_resource")
        except (pytest.skip.Exception, Exception):
            pass

    mock_resource, _ = mock_mssql_resource
    return mock_resource


@pytest.fixture
def fhir_server_resource(request, mock_fhir_server_resource):
    """
    Flexible FHIR Server resource - auto-selects mock or real based on configuration.
    Use this fixture when you want tests to work with either.
    """
    if should_use_real_resources() and is_safe_environment():
        try:
            return request.getfixturevalue("real_fhir_server_resource")
        except (pytest.skip.Exception, Exception):
            pass

    return mock_fhir_server_resource


@pytest.fixture
def real_ifdb_oracle_resource(request, mock_oracle_resource):
    """
    Flexible IFDB Oracle resource - auto-selects mock or real based on configuration.

    For tests marked with @pytest.mark.real_resources, the test will be skipped if
    real resources aren't available. For other tests, it falls back to mock.
    TEST_* variables take precedence over PROD_* variables.
    """
    test_markers = [mark.name for mark in request.node.iter_markers()]
    requires_real = "real_resources" in test_markers

    if should_use_real_resources() and is_safe_environment():
        with env_var_override(_IFDB_ORACLE_ENV_VARS):
            try:
                return IFDBOracleResource()
            except Exception:
                if requires_real:
                    pytest.skip("Real IFDB Oracle resource unavailable - connection failed")
    elif requires_real:
        pytest.skip("USE_REAL_DB_FOR_TESTS not enabled or environment not safe")

    mock_resource, _ = mock_oracle_resource
    return mock_resource


# =============================================================================
# Cleanup Utilities
# =============================================================================


def _execute_cleanup(cursor, schema: str, table: str) -> None:
    """Clean table but preserve UNKNOWN rows (DW_EK_* = -1)."""
    if schema == "dim":
        # Determine the primary key column name - all dim tables use DW_EK_<TableName>
        pk_column = f"DW_EK_{table}"
        cursor.execute(
            f"""
            IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
            BEGIN
                DELETE FROM [{schema}].[{table}] WHERE [{pk_column}] > 0;
            END
            """
        )
        return

    if schema in ["stg", "map"]:
        # Use TRUNCATE for both staging and map tables (minimally logged)
        cursor.execute(
            f"""
            IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
            BEGIN
                BEGIN TRY
                    TRUNCATE TABLE [{schema}].[{table}];
                END TRY
                BEGIN CATCH
                    DELETE FROM [{schema}].[{table}];
                END CATCH
            END
            """
        )
        return

    # Keep DELETE for dim/fct tables (may need to preserve UNKNOWN rows)
    cursor.execute(
        f"""
        IF OBJECT_ID('[{schema}].[{table}]', 'U') IS NOT NULL
        BEGIN
            DELETE FROM [{schema}].[{table}];
        END
        """
    )


def _cleanup_tables(resource: MSSQLResource, tables: tuple[tuple[str, str], ...]) -> None:
    """Generic cleanup function for a list of tables."""
    with resource.get_connection() as conn:
        cursor = conn.cursor()
        try:
            for schema, table in tables:
                _execute_cleanup(cursor, schema, table)
            conn.commit()
        finally:
            cursor.close()


def _cleanup_erp_tables(resource: MSSQLResource) -> None:
    """Clean all erp target tables (facts, dims, maps)."""
    _cleanup_tables(resource, ERP_TARGET_TABLES)


def _cleanup_erp_staging_tables(resource: MSSQLResource) -> None:
    """Clean only staging tables for erp."""
    _cleanup_tables(resource, ERP_STAGING_TABLES)


def _cleanup_erp_map_tables(resource: MSSQLResource) -> None:
    """Clean only map tables for erp (preserves staging data)."""
    _cleanup_tables(resource, ERP_MAP_TABLES)


def _cleanup_erp_dim_tables(resource: MSSQLResource) -> None:
    """Clean only dim tables for erp (preserves map and staging data)."""
    _cleanup_tables(resource, ERP_DIM_TABLES)


def _cleanup_erp_fact_tables(resource: MSSQLResource) -> None:
    """Clean only fact tables for erp (preserves dim, map, and staging data)."""
    _cleanup_tables(resource, ERP_FACT_TABLES)


# =============================================================================
# Cleanup Fixtures
# =============================================================================


@pytest.fixture
def truncate_erp_target_tables(request):
    """
    Ensures map/dim/fact tables for ERP are empty when tests hit a real DW.

    Usage:
        def test_real_erp_flow(truncate_erp_target_tables, real_mssql_target_staging_resource, ...):
            ...

    The fixture runs a DELETE before and after the test whenever:
        - USE_REAL_DB_FOR_TESTS=true
        - ENABLE_TEST_WRITES=true
    Otherwise it is a no-op.
    """
    use_real = should_use_real_resources()
    writes_enabled = os.getenv("ENABLE_TEST_WRITES", "false").lower() == "true"

    if not (use_real and writes_enabled):
        yield lambda: None
        return

    try:
        target_resource = request.getfixturevalue("real_mssql_target_staging_resource")
    except pytest.skip.Exception:
        yield lambda: None
        return

    _cleanup_erp_tables(target_resource)
    yield lambda: _cleanup_erp_tables(target_resource)
    _cleanup_erp_tables(target_resource)
