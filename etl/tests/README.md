<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# ETL Pipeline Testing Guide

This directory contains the testing framework for Dagster ETL pipelines.
For folder-specific conventions, see `.cursor/rules/05-testing.mdc`.

## Table of Contents

1. [Quick Start](#quick-start)
2. [Directory Structure](#directory-structure)
3. [Configuration](#configuration)
4. [Test Methodologies](#test-methodologies)
5. [Writing Tests](#writing-tests)
6. [Advanced Setup](#advanced-setup)
7. [Troubleshooting](#troubleshooting)

---

## Quick Start

### Setup (Already Done!)

✅ Test configuration created from `.env copy` → `tests/config/test.env`
✅ All test infrastructure files created
✅ Dependencies added to `pyproject.toml`

### Quick Commands

#### 1. Run Unit Tests (Fast, Mocked Resources)

```bash
cd etl
pytest tests/unit/ -v
```

**Expected Output**: all unit tests pass (several thousand, mocked resources)

#### 2. Run Integration Tests (Mocked Resources)

```bash
pytest tests/integration/ -m unit -v
```

**What this tests**: Pipeline structure, asset dependencies, job configurations

#### 3. Test with Real Databases (Read-Only)

```bash
# Enable real resources
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev

# Run read-only tests
pytest -m real_resources -m read_only -v
```

**Safety**: Tests will refuse to run if `TEST_ENV` contains "prod"

#### 4. Test Specific Patient

```bash
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev
export TEST_PATIENT_ID=12345

pytest -m patient_test -v
```

### Test Markers

| Marker | Purpose | Example |
|--------|---------|---------|
| `@pytest.mark.unit` | Fast unit tests | `pytest -m unit` |
| `@pytest.mark.integration` | Integration tests | `pytest -m integration` |
| `@pytest.mark.real_resources` | Requires real DBs | `pytest -m real_resources` |
| `@pytest.mark.read_only` | Safe for real DBs | `pytest -m read_only` |
| `@pytest.mark.patient_test` | Patient-specific | `pytest -m patient_test` |

### Common Workflows

**Development Workflow:**

```bash
# 1. Write/update code
# 2. Run fast unit tests
pytest tests/unit/ -v

# 3. Test integration structure
pytest tests/integration/ -m unit -v

# 3b. Run datawarehouse domain suites (once created)
pytest tests/integration/datawarehouse/ -m integration -v

# 4. Test with real data (when ready)
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev
pytest -m real_resources -v
```

**Pre-Deployment Validation:**

```bash
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev
export TEST_PATIENT_ID=<known-good-patient-id>

# Run all patient tests
pytest -m patient_test -v

# Run patient flow tests
pytest tests/integration/fhir_ifdb/test_patient_flow.py -v
```

---

## Directory Structure

```
tests/
├── README.md                     # This guide
├── conftest.py                    # Pytest configuration and shared fixtures
├── config/                        # Test configuration
│   ├── test.env                   # Local override (copy from test.env.example)
│   └── test.env.example           # Sample configuration
├── fixtures/                      # Shared fixtures for all suites
│   ├── dagster_instance.py        # Dagster instance setup helpers
│   ├── resources.py               # Mock and real resource factories
│   └── test_data.py               # Test data management utilities
├── integration/
│   ├── fhir_ifdb/                 # IFDB → FHIR end-to-end flows
│   │   ├── conftest.py
│   │   ├── test_extraction.py
│   │   ├── test_patient_flow.py
│   │   ├── test_transformation.py
│   │   └── ...                    # Additional IFDB/FHIR suites
│   └── datawarehouse/             # (New) Per-domain DW pipelines live here
├── unit/
│   ├── test_ifdb_transforms.py    # IFDB-specific transform coverage
│   ├── test_transforms.py         # Shared transform utilities
│   └── test_yaml_loader.py        # Configuration loader coverage
└── utils/                         # Test utilities
    ├── materialize_helpers.py     # Asset materialization helpers
    ├── query_helpers.py           # Query modification helpers
    └── verification_helpers.py    # Output verification utilities
```

---

## Configuration

### Test Configuration File

Configuration file: `tests/config/test.env`

The test configuration is loaded in `tests/conftest.py` before any tests run:
- First loads `tests/config/test.env`
- Falls back to root `.env` if test.env doesn't exist
- All environment variables are available to test fixtures

**Key Settings:**
- `USE_REAL_DB_FOR_TESTS=false` - Start with false (use mocks)
- `TEST_ENV=local` - Set to dev/test for real resources
- `TEST_PATIENT_ID=12345` - Patient ID for testing
- `ENABLE_TEST_WRITES=false` - Allow writes to real databases

**All database credentials are pre-configured from `.env copy`**

### Environment Variables

Key environment variables for testing:

| Variable | Default | Purpose |
|----------|---------|---------|
| `USE_REAL_DB_FOR_TESTS` | `false` | Enable real database connections |
| `TEST_ENV` | `local` | Environment identifier (dev/test/local) |
| `TEST_PATIENT_ID` | `12345` | Default patient ID for testing |
| `ENABLE_TEST_WRITES` | `false` | Allow writes to real databases |

**Configuration Priority:**
1. Environment variables (highest priority)
2. `tests/config/test.env`
3. Root `.env` (fallback)
4. Defaults in code

### Database Configurations

All database configurations are loaded from `test.env`:

**MySupply Database:**
- Variables: `MYSUPPLY_PROD_MSSQL_*`
- Fixtures: `mock_mysupply_mssql_resource`, `real_mysupply_mssql_resource`

**Sydax Database:**
- Variables: `SYDAX_PROD_MSSQL_*`
- Fixtures: `mock_sydax_mssql_resource`, `real_sydax_mssql_resource`

**ILS Database:**
- Variables: `ILS_PROD_MSSQL_*`
- Fixtures: `mock_ils_mssql_resource`, `real_ils_mssql_resource`

**IFDB Oracle Database:**
- Variables: `IFDB_PROD_ORACLE_*`
- Fixtures: `mock_oracle_resource`, `real_ifdb_oracle_resource`

**Configuration Precedence:**
Test-specific variables can override production variables:
1. **TEST_* variables** - Take highest precedence if set
2. **PROD_* variables** - From `test.env` file
3. **Default values** - Hard-coded fallbacks

Example:
```bash
# In test.env
MYSUPPLY_PROD_MSSQL_SERVER=mssql-production

# Override for specific test
TEST_MYSUPPLY_MSSQL_SERVER=mssql-test
```

---

## Test Methodologies

The framework supports multiple testing methodologies, each suited for different purposes:

### Method 1: Unit Tests with Mocks

**Purpose**: Fast, isolated tests for transform logic and utilities

**Characteristics:**
- Uses mocked resources (no real database connections)
- Fast execution (milliseconds)
- Completely isolated from external systems
- Perfect for CI/CD pipelines

**When to Use:**
- Testing transform functions
- Validating data conversion logic
- Unit testing utilities
- Testing business logic in isolation

**Run with:**
```bash
pytest -m unit -v
```

**Speed**: ⚡ Fast (milliseconds)

### Method 2: Integration Tests with Mocked Resources

**Purpose**: Test pipeline structure and asset dependencies without real data

**Characteristics:**
- Uses mocked resources
- Tests asset dependencies and structure
- Verifies pipeline orchestration
- Still fast (no network calls)

**When to Use:**
- Testing asset dependency graphs
- Verifying job configurations
- Testing partition handling
- Pipeline structure validation

**Run with:**
```bash
pytest -m integration -m unit -v
```

**Speed**: ⚡ Fast

### Method 3: Integration Tests with Real Resources (Read-Only)

**Purpose**: Test against real databases with safety constraints

**Characteristics:**
- Connects to real test/dev databases
- Read-only operations by default
- Automatically filters by PATIENT_ID
- Validates real data structures
- Slower (network/database calls)

**When to Use:**
- Validating query correctness
- Testing against real data schemas
- Verifying database connectivity
- Integration testing before deployment

**Setup Required:**
```bash
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev  # or 'test', but NEVER 'production'
```

**Run with:**
```bash
# Enable real resources
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev

# Run integration tests with real resources
pytest -m real_resources -m read_only -v

# Test specific patient
export TEST_PATIENT_ID=12345
pytest -m patient_test -v
```

**Speed**: 🐢 Slower (network/database calls)

### Method 4: Full Pipeline Tests (Real DBs)

**Purpose**: End-to-end testing of complete pipeline with real data

**Characteristics:**
- Full pipeline materialization
- Reads from real source databases
- Writes to real target systems (if enabled)
- Validates complete data flow
- Requires careful configuration

**When to Use:**
- Pre-deployment validation
- Testing complete workflows
- Validating data transformation end-to-end
- Integration testing with target systems

**Setup Required:**
```bash
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev
export ENABLE_TEST_WRITES=false  # Keep read-only for safety
```

**Run with:**
```bash
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev
export TEST_PATIENT_ID=12345

pytest tests/integration/fhir_ifdb/test_patient_flow.py -v
```

**Speed**: 🐢 Slower

**Important Safety Notes:**
- Never use in production environment
- Use dedicated test databases when possible
- Verify ENABLE_TEST_WRITES setting
- Test with specific PATIENT_ID filters

---

## Writing Tests

### Example 1: Unit Test with Mocks

```python
@pytest.mark.unit
@pytest.mark.parametrize("patient_id", ["12345"])
def test_patient_transform(mock_mssql_resource, patient_id):
    """Test patient transformation with mocked data."""
    mock_resource, mock_cursor = mock_mssql_resource

    # Setup mock data
    # ... setup code ...

    # Test transformation
    # ... test code ...
```

**Example - Transform Function:**
```python
@pytest.mark.unit
def test_fhir_date_conversion():
    """Test FHIR date conversion utility."""
    result = fhir_date(date(1990, 1, 1))
    assert result == "1990-01-01"
```

### Example 2: Integration Test with Real Resources

```python
@pytest.mark.integration
@pytest.mark.real_resources
@pytest.mark.read_only
def test_patient_pipeline_real(real_mssql_resource, real_fhir_server_resource, patient_id):
    """Test against real data sources."""
    # Safety check
    assert os.getenv("TEST_ENV") != "production"
    # Test with real resources
    # Queries are automatically filtered by PATIENT_ID
```

### Example 3: Flexible Test (Works with Both)

```python
@pytest.mark.integration
def test_patient_pipeline_flexible(mssql_resource, fhir_server_resource):
    """Test that works with mock or real resources."""
    # Automatically uses real if USE_REAL_DB_FOR_TESTS=true
    # Falls back to mock otherwise
```

### Example 4: Integration Test with Mocked Resources

```python
@pytest.mark.integration
@pytest.mark.unit
def test_fhir_patient_pipeline_mock(
    dagster_instance,
    mock_mssql_resource,
    mock_fhir_server_resource,
    patient_id,
    test_partition,
):
    """Test patient pipeline structure with mocked resources."""
    # Test verifies pipeline structure, not actual data transformation
```

---

## Datawarehouse Domain Testing

The datawarehouse pipelines live under `DSA/SQL/` and `DSI/`.
Each domain (`erp`, `kraeftpakker`, etc.) exposes Dagster assets for staging,
mapping, and fact tables that we now want to cover with automated tests.

### Where Tests Belong

- Create one folder per domain under `tests/integration/datawarehouse/`.
- Mirror the asset grouping in the codebase, e.g.
  `tests/integration/datawarehouse/erp/test_staging_vare.py`.
- Share common fixtures via `tests/conftest.py` and `tests/fixtures/`.

### Recommended Test Flow

1. **Select assets** – Identify the staging/update asset keys you want to exercise.
2. **Materialize with dependencies** – Use `tests.utils.materialize_helpers.materialize_assets_with_dependencies`
   to execute the transform and downstream load assets together.
3. **Provide resources** – Pass in mocked resources by default; opt into real
   MSSQL connections via the environment flags described earlier.
4. **Validate metadata** – Leverage `tests.utils.materialize_helpers.get_materialization_result`
   to inspect Dagster metadata such as row counts.
5. **Query targets directly** – Use `tests.utils.verification_helpers.verify_database_output`
   (or custom SQL assertions) against the destination schema to confirm result rows.

### Example Skeleton

```python
from tests.utils.materialize_helpers import (
    get_materialization_result,
    materialize_assets_with_dependencies,
)

@pytest.mark.integration
@pytest.mark.parametrize("partition", ["2024-10-31"])
def test_transformed_erp_vare_partition(
    dagster_instance,
    mock_mssql_source_resource,
    mock_mssql_target_resource,
    partition,
):
    assets = [
        "stg_vare_ils",
        "map_vare",
        "dim_vare",
    ]

    materialize_assets_with_dependencies(
        instance=dagster_instance,
        asset_keys=assets,
        partition=partition,
        resources={
            "mysupply_mssql": mock_mssql_source_resource,
            "mssql_target": mock_mssql_target_resource,
            # Add additional domain-specific resources (e.g. sydax_mssql) as needed
    },
    )

    # Inspect Dagster metadata returned for the target asset
    metadata = get_materialization_result(dagster_instance, assets[-1])
    posted = metadata.get("row_count").value
    assert posted > 0
```

### Naming & Markers

- Keep using `@pytest.mark.integration` for end-to-end DW coverage.
- Add per-domain markers (e.g. `@pytest.mark.erp`) once you register them in `pytest.ini`.
- Prefer parametrized partitions to verify both `daily` and `full_load` scenarios.

### Real Database Runs

- Set `USE_REAL_DB_FOR_TESTS=true` and choose a safe `TEST_ENV` to switch fixtures
  from mocks to real resources.
- Ensure the SQL the test executes is read-only unless you explicitly set
  `ENABLE_TEST_WRITES=true`.
- Seed realistic data via source system snapshots or domain-specific fixtures
  in `tests/fixtures/test_data.py`.
- For erp staging suites, the flexible fixtures let the same tests exercise
  real sources: just replace `mock_*` fixtures with their flexible counterparts
  (`mysupply_mssql_resource`, `sydax_mssql_resource`, `ils_mssql_resource`,
  `mssql_resource`) so Dagster swaps to live connections once the env flags and
  `test.env` credentials are in place.


## PATIENT_ID Filtering

When testing with real databases, queries are automatically filtered by PATIENT_ID to:
- **Limit scope** to specific test scenarios
- **Improve performance** by processing only test data
- **Maintain safety** by not processing all production data

Filtering is applied through:
1. Query modification helpers (`query_helpers.add_patient_id_filter`)
2. Asset materialization with patient_id parameter
3. Test data loading functions

---

## Advanced Setup

### Test Setup Modes

The framework supports three setup modes:

#### 1. Mock Mode (Default)

**Configuration:**
```bash
USE_REAL_DB_FOR_TESTS=false
TEST_ENV=local
```

**Behavior:**
- All database fixtures return mock objects
- No actual database connections are made
- Fast execution, suitable for unit tests
- All mock fixtures are available

**Available Fixtures:**
- `mock_mssql_resource` - Mock MSSQL connection
- `mock_fhir_server_resource` - Mock FHIR server
- `mock_oracle_resource` - Mock Oracle connection
- `mock_mysupply_mssql_resource` - Mock MySupply MSSQL
- `mock_sydax_mssql_resource` - Mock Sydax MSSQL
- `mock_ils_mssql_resource` - Mock ILS MSSQL

**Example:**
```python
def test_with_mocks(mock_mssql_resource):
    resource, cursor = mock_mssql_resource
    # Use mock resource for testing
```

#### 2. Real Resources Mode

**Configuration:**
```bash
USE_REAL_DB_FOR_TESTS=true
TEST_ENV=dev  # or test, local - NOT production
```

**Behavior:**
- Creates actual database connections using credentials from `test.env`
- Real fixtures are available when environment is safe
- Safety checks prevent use in production environments
- Integration tests can query real databases

**Safety Checks:**
- `TEST_ENV` cannot contain "prod" or "production"
- `DAGSTER_ENVIRONMENT` cannot contain "prod" or "production"
- Safe environments: `local`, `dev`, `test`, `development`

**Available Fixtures:**
- `real_mssql_resource` - Real MSSQL connection
- `real_mssql_target_resource` - Real warehouse connection (mssql_target)
- `real_fhir_server_resource` - Real FHIR server
- `real_ifdb_oracle_resource` - Real IFDB Oracle connection
- `real_mysupply_mssql_resource` - Real MySupply MSSQL
- `real_sydax_mssql_resource` - Real Sydax MSSQL
- `real_ils_mssql_resource` - Real ILS MSSQL
- `truncate_erp_target_tables` - Deletes ERP map/dim/fct tables before/after a test when writes are enabled

**Example:**
```python
@pytest.mark.real_resources
def test_with_real_db(real_mssql_resource):
    with real_mssql_resource.get_connection() as conn:
        # Query real database
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
```

#### 3. Flexible Mode

**Configuration:**
- Works with either mock or real mode
- Auto-selects based on `USE_REAL_DB_FOR_TESTS` flag

**Behavior:**
- If `USE_REAL_DB_FOR_TESTS=true` and environment is safe → uses real resources
- Otherwise → uses mock resources
- Tests work in both modes without modification

**Available Fixtures:**
- `mssql_resource` - Flexible MSSQL (mock or real)
- `fhir_server_resource` - Flexible FHIR server (mock or real)

**Example:**
```python
def test_flexible(mssql_resource):
    # Works with both mock and real resources
    with mssql_resource.get_connection() as conn:
        # Use connection
```

### Warehouse Cleanup Fixture (ERP)

When you allow writes to the real warehouse (`ENABLE_TEST_WRITES=true`), use the
`truncate_erp_target_tables` fixture to keep ERP map/dim/fct tables clean:

```python
@pytest.mark.real_resources
def test_erp_pipeline(
    truncate_erp_target_tables,
    real_mssql_target_resource,
    ...
):
    # Tables are emptied before and after this test
    ...
```

- The fixture deletes rows from:
  - `fct.Fakturalinje`, `fct.Postering`
  - `dim.Kreditor`, `dim.Kontoplan`, `dim.Leverandoervare`
  - `map.Kreditor`, `map.Kontoplan`, `map.Leverandoervare`
- Update the list in `tests/fixtures/resources.py` whenever new ERP map/dim/fct tables are introduced.

### Safety Features

1. **Production Protection**
   - Tests cannot run against production when `TEST_ENV` or `DAGSTER_ENVIRONMENT` contains "prod"
   - Safety check fails tests if production environment detected

2. **Explicit Enabling**
   - Real resources require `USE_REAL_DB_FOR_TESTS=true`
   - Default is `false` (mock mode)

3. **Read-Only Default**
   - Real database tests are read-only by default
   - Must explicitly enable writes with `ENABLE_TEST_WRITES=true`

4. **Environment Isolation**
   - Test environment variables are separate from production
   - TEST_* variables can override PROD_* without affecting production

5. **Query Filtering**
   - Always filters by PATIENT_ID when using real databases
   - Limits scope to test data only

### Running Tests with Different Modes

**Run with Mocks (Default):**
```bash
# Uses mock resources by default
pytest tests/unit/test_fixture_setups.py
```

**Run with Real Resources:**
```bash
# Enable real resources
export USE_REAL_DB_FOR_TESTS=true
export TEST_ENV=dev

pytest tests/unit/test_fixture_setups.py -m real_resources
```

### Best Practices

1. **Use Mock Fixtures for Unit Tests**
   - Fast execution
   - No external dependencies
   - Predictable results

2. **Use Real Fixtures for Integration Tests**
   - Verify actual database connectivity
   - Test with real data
   - Mark with `@pytest.mark.real_resources`

3. **Use Flexible Fixtures for Adaptable Tests**
   - Same test works in both modes
   - Good for CI/CD pipelines
   - Can switch between mock/real easily

4. **Never Use Real Resources in Production**
   - Always verify `TEST_ENV` is not "production"
   - Safety checks should prevent accidental production access

---

## Troubleshooting

### Tests Not Found

```bash
# Ensure you're in etl directory
cd etl

# Check Python path (package is at etl/)
pytest tests/ -v --collect-only
```

### Import Errors

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Ensure you're in the etl directory
cd etl

# Run with explicit Python path
PYTHONPATH=src pytest tests/ -v
```

### "USE_REAL_DB_FOR_TESTS not enabled"

**Solution**: Set environment variable:
```bash
export USE_REAL_DB_FOR_TESTS=true
```

Or configure in `tests/config/test.env`

### "Not safe to use real database in current environment"

**Cause**: TEST_ENV or DAGSTER_ENVIRONMENT contains "prod" or "production"

**Solution**:
```bash
export TEST_ENV=dev  # or 'test', 'local'
export DAGSTER_ENVIRONMENT=development
```

### Real Database Connection Failed

1. Check database is running
2. Verify credentials in `tests/config/test.env`
3. Test connection:
   ```bash
   export USE_REAL_DB_FOR_TESTS=true
   export TEST_ENV=dev
   pytest tests/integration/fhir_ifdb/test_patient_flow.py -v
   ```

### "Patient not found in database"

**Cause**: Test patient ID doesn't exist in database

**Solution:**
1. Verify patient exists:
   ```bash
   export USE_REAL_DB_FOR_TESTS=true
   export TEST_ENV=dev
   pytest tests/integration/fhir_ifdb/test_patient_flow.py -v
   ```

2. Update TEST_PATIENT_ID to an existing patient:
   ```bash
   export TEST_PATIENT_ID=<valid-patient-id>
   ```

### Database Connection Errors

**Cause**: Incorrect credentials or database unavailable

**Solution:**
1. Verify database is running
2. Check credentials in `tests/config/test.env`
3. Test connection manually:
   ```python
   from core.resources import MSSQLResource
   resource = MSSQLResource(
       host="mssql",
       port=1433,
       database="IFDB_EPJ",
       user="sa",
       password="YourPassword"
   )
   with resource.get_connection() as conn:
       print("Connection successful!")
   ```

### Fixtures Not Found

- Ensure `conftest.py` imports fixtures from `tests.fixtures.resources`
- Check that fixtures are defined in `tests/fixtures/resources.py`

### Environment Variables Not Loading

- Verify `test.env` exists in `tests/config/`
- Check that `conftest.py` loads the file (should happen before imports)
- Use `pytest tests/unit/test_setup_verification.py` to verify loading

### Enable Verbose Logging

```bash
pytest tests/integration/ -v -s --log-cli-level=DEBUG
```

### Run Single Test

```bash
pytest tests/integration/fhir_ifdb/test_patient_flow.py::test_bulk_import_configuration_validation -v
```

### Capture Logs in Tests

```python
def test_with_logging(caplog):
    with caplog.at_level(logging.DEBUG):
        # Your test code
        logger.debug("Debug message")
```

---

## Next Steps

1. ✅ Run your first test: `pytest tests/unit/ -v`
2. Write unit tests for your transform functions
3. Add integration tests based on examples in `tests/integration/`
4. Test with mocked resources during development (fast feedback)
5. Configure real resources for integration testing
6. Customize `tests/config/test.env` for your environment

---

## Summary

The testing framework provides comprehensive support for:

- ✅ **Unit Tests** - Fast, isolated tests with mocks
- ✅ **Integration Tests** - Pipeline structure and dependencies
- ✅ **Real Resource Tests** - Integration with actual databases (read-only)
- ✅ **Full Pipeline Tests** - End-to-end validation
- ✅ **Safety Features** - Production protection and query filtering
- ✅ **Flexible Configuration** - Switch between mock and real resources easily
- ✅ **Multiple Methodologies** - Choose the right approach for each test scenario

Start testing with: `cd etl && pytest tests/unit/ -v`
