<!--
  SPDX-FileCopyrightText: 2026 Enversion Journl ApS
  SPDX-License-Identifier: AGPL-3.0-or-later

  This file is part of Healthpipe. See the LICENSE file in the project
  root for the full AGPL-3.0 license text.
-->

# ERP Integration Tests - Real Database Mode

## Overview

The integration tests for ERP staging assets have been converted to use real database connections instead of mocks. This provides end-to-end validation of the ETL pipeline with actual database queries and transformations.

## Test Files Converted

1. `test_staging_metadata_mysupply.py` - MySupply metadata staging
2. `test_staging_mysupply_header.py` - MySupply invoice header staging
3. `test_staging_mysupply.py` - MySupply invoice line staging (with relationships)
4. `test_staging_sydax.py` - Sydax staging (Kreditor, Finanskonto, Finanspostering)
5. `test_staging_ils.py` - ILS staging (lev, vare)

## Prerequisites

### 1. Environment Configuration

Tests require `test.env` to be configured with:
```bash
USE_REAL_DB_FOR_TESTS=true
ENABLE_TEST_WRITES=true
TEST_ENV=local
```

Database connection settings are already configured in `/workspaces/healthpipe/dagster_etl/tests/config/test.env`.

### 2. Test Data Setup

**IMPORTANT**: Before running these tests, you must seed the test data into your databases.

#### Option A: Run SQL Seed Script Manually

Execute the seed script against your databases:
```bash
# Location: dagster_etl/tests/fixtures/seed_erp_test_data.sql
# Run this script using SQL Server Management Studio or sqlcmd
```

#### Option B: Integrate into Docker Seed Container

Add the seed script to your existing Docker container that loads data. See `dagster_etl/tests/fixtures/README_SEED_DATA.md` for detailed instructions.

### 3. Required Test Data

The tests expect these specific records to exist:

**MySupply Database (mysupply):**
- metadata: CVR "12345678"
- statistic_invoice_header: ID 10, CVR "12345678"
- statistic_invoice_line: ID 1, header_id 10, item "ABC123"

**Sydax Database (sydax):**
- Kreditor.Kreditor: CVR "12345678"
- Finans.Finanskonto: "ACC-123"
- Finans.Finanspostering: "POST-001"

**ILS Database (ils):**
- dbo.lev: CVR "12345678"
- dbo.vare: item "ITEM123", CVR "12345678"

## Running the Tests

### Run All Real Resource Tests

```bash
cd /workspaces/healthpipe/dagster_etl
pytest tests/integration/datawarehouse/erp/ -m "real_resources and erp" -v
```

### Run Individual Test Files

```bash
# MySupply metadata test
pytest tests/integration/datawarehouse/erp/test_staging_metadata_mysupply.py -v

# MySupply header test
pytest tests/integration/datawarehouse/erp/test_staging_mysupply_header.py -v

# MySupply line test (complex, with relationships)
pytest tests/integration/datawarehouse/erp/test_staging_mysupply.py -v

# Sydax tests (parametrized - runs 3 test cases)
pytest tests/integration/datawarehouse/erp/test_staging_sydax.py -v

# ILS tests (parametrized - runs 2 test cases)
pytest tests/integration/datawarehouse/erp/test_staging_ils.py -v
```

## Test Architecture

### Key Changes from Mock-Based Tests

**Before (Mock-based):**
- Created fake database cursors with `MagicMock`
- Simulated SQL responses with hardcoded data
- Validated by inspecting mock method calls

**After (Real database):**
- Use actual database connections
- Execute real SQL queries
- Query staging tables to validate results

### Helper Utilities

Tests use `staging_helpers.py` for common operations:

```python
from tests.utils.staging_helpers import query_and_find_test_record

# Query staging table and find specific test record
test_row = query_and_find_test_record(
    real_mssql_target_resource,
    "metadata_mysupply",
    0,  # Column index to search
    "12345678"  # Value to find
)
```

### Test Isolation

- **Source databases**: Tests read from shared test data (no cleanup)
- **Target databases**: Staging tables cleaned before each test run via `truncate_erp_target_tables` fixture
- Tests can coexist with other data in source databases

## Troubleshooting

### Error: "Test record not found in stg.X"

**Cause**: Seed data not present in source databases

**Solution**:
1. Verify test data exists by running verification queries from `seed_erp_test_data.sql`
2. If missing, execute the seed script
3. Re-run tests

### Error: "Invalid column name"

**Cause**: Database schema doesn't match expectations

**Solution**: Check that your database schemas match those defined in the staging assets. The seed script assumes standard column names.

### Tests Pass Individually But Fail Together

**Cause**: Target table cleanup not working between tests

**Solution**: Ensure `ENABLE_TEST_WRITES=true` is set in `test.env`

## Benefits of Real Database Testing

1. **Integration Validation**: Tests verify actual database queries, not mocked behavior
2. **Schema Validation**: Catches schema mismatches between source and target
3. **Clean Code Generation**: Validates that clean codes are generated correctly from real data
4. **Relationship Testing**: Verifies foreign key relationships work end-to-end
5. **Performance Insights**: Identifies slow queries in real database environment

## Limitations

1. **Slower Execution**: Real database I/O is slower than in-memory mocks (~1-2s per test vs ~0.1s)
2. **Data Dependency**: Requires test data to be seeded first
3. **Environment Setup**: More complex setup than pure mocks

## Next Steps

1. Run `seed_erp_test_data.sql` to populate test data
2. Execute tests to validate conversion
3. Integrate seed script into your Docker container setup
4. Consider adding more test cases for edge cases
