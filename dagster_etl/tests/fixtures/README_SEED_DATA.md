# ERP Test Data Seeding

## Purpose

This directory contains SQL scripts to seed deterministic test data for ERP integration tests. The test data ensures proper relationships exist between source tables across MySupply, Sydax, and ILS databases.

## Test Data Overview

The seed script creates a consistent set of test records with proper foreign key relationships:

| Database | Table | Key Field | Value |
|----------|-------|-----------|-------|
| MySupply | metadata | LeverandorCVR | 12345678 |
| MySupply | statistic_invoice_header | statistic_invoice_header_id | 10 |
| MySupply | statistic_invoice_header | LeverandorCVR | 12345678 |
| MySupply | statistic_invoice_line | statistic_invoice_line_id | 1 |
| MySupply | statistic_invoice_line | statistic_invoice_header_id | 10 (FK) |
| MySupply | statistic_invoice_line | LeverandorCVR | 12345678 |
| MySupply | statistic_invoice_line | Item_FakturaUdstederVareNr | ABC123 |
| Sydax | Kreditor | Cvrnr | 12345678 |
| Sydax | Finanskonto | FinanskontoId | ACC-123 |
| Sydax | Finanspostering | FinansposteringId | POST-001 |
| ILS | lev | cvr_nr | 12345678 |
| ILS | vare | Vare_CleanCode_Source | ITEM123 |
| ILS | vare | cvr_nr | 12345678 (FK) |

## Key Relationships

1. **MySupply Invoice Chain**:
   - `statistic_invoice_line.statistic_invoice_header_id` → `statistic_invoice_header.statistic_invoice_header_id`
   - Both reference the same `LeverandorCVR` (12345678)

2. **Kreditor/Supplier Consistency**:
   - Same CVR (12345678) exists across MySupply.metadata, Sydax.Kreditor, and ILS.lev
   - Enables cross-system supplier matching in map_kreditor

3. **Vare/Item Linkage**:
   - ILS.vare links item "ITEM123" to supplier CVR "12345678"
   - Matches MySupply invoice line item code for end-to-end testing

## Integration with Docker Container

### Option 1: Manual Execution

Run the seed script manually against your databases:

```bash
# From SQL Server Management Studio or Azure Data Studio
# Open: seed_erp_test_data.sql
# Execute sections sequentially for each database
```

### Option 2: Docker Container Integration

Add the seed script to your existing Docker seed container:

1. **Copy the SQL file** to your seed container's SQL directory
2. **Modify your seed entrypoint** to execute this script after main data load:
   ```bash
   # In your Dockerfile or entrypoint script
   /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD \
     -i /seed/seed_erp_test_data.sql
   ```

3. **Or split by database** if your container processes databases separately:
   - Extract MySupply section → `seed_mysupply_test.sql`
   - Extract Sydax section → `seed_sydax_test.sql`
   - Extract ILS section → `seed_ils_test.sql`

### Option 3: Docker Compose Integration

If using docker-compose, add a volume mount:

```yaml
services:
  mssql:
    volumes:
      - ./dagster_etl/tests/fixtures:/docker-seed-data
    # Then reference the file in your initialization script
```

## Verification

After running the seed script, verify data is present:

### MySupply Database

```sql
USE [mysupply];
SELECT * FROM [dbo].[metadata] WHERE [LeverandorCVR] = '12345678';
SELECT * FROM [dbo].[statistic_invoice_header] WHERE [statistic_invoice_header_id] = 10;
SELECT * FROM [dbo].[statistic_invoice_line] WHERE [statistic_invoice_line_id] = 1;
```

**Expected**: 1 row in each table

### Sydax Database

```sql
USE [sydax];
SELECT * FROM [Kreditor].[Kreditor] WHERE [Cvrnr] = '12345678';
SELECT * FROM [Finans].[Finanskonto] WHERE [FinanskontoId] = 'ACC-123';
SELECT * FROM [Finans].[Finanspostering] WHERE [FinansposteringId] = 'POST-001';
```

**Expected**: 1 row in each table

### ILS Database

```sql
USE [ils];
SELECT * FROM [dbo].[lev] WHERE [cvr_nr] = '12345678';
SELECT * FROM [dbo].[vare] WHERE [Vare_CleanCode_Source] = 'ITEM123' AND [cvr_nr] = '12345678';
```

**Expected**: 1 row in lev, 1 row in vare

## Idempotency

The seed script uses `MERGE` statements, making it safe to run multiple times:
- First run: Inserts test data
- Subsequent runs: Updates existing records with same values
- No duplicates created
- No errors if data already exists

## Test Integration

Integration tests expect this data to exist:

- `test_staging_metadata_mysupply.py` - queries MySupply.metadata
- `test_staging_mysupply_header.py` - queries MySupply.statistic_invoice_header
- `test_staging_mysupply.py` - queries MySupply invoice line with relationships
- `test_staging_sydax.py` - queries Sydax Kreditor, Finanskonto, Finanspostering
- `test_staging_ils.py` - queries ILS lev and vare

## Troubleshooting

### Tests Failing with "No Data Found"

**Cause**: Seed data not present in databases

**Solution**:
1. Run verification queries above
2. If data missing, execute `seed_erp_test_data.sql`
3. Re-run tests

### "Cannot insert duplicate key" Error

**Cause**: Attempting to run script with INSERT instead of MERGE

**Solution**: The provided script uses MERGE - ensure you're using the correct file

### Foreign Key Violations

**Cause**: Tables being seeded in wrong order

**Solution**: Execute the script sections in order:
1. MySupply metadata, header, then line
2. Sydax in any order (no FK dependencies)
3. ILS lev before vare

## Schema Assumptions

The seed script assumes standard table schemas. If your schema differs, you may need to adjust:

1. **Column names** - script uses exact names from staging asset queries
2. **Data types** - matches expected types (NVARCHAR for CVR, INT for IDs, etc.)
3. **Schema names** - uses `dbo`, `Kreditor`, `Finans` as per configuration

Check your database schemas match those in:
- `dagster_etl/DSI/mssql/` (extract asset configs) and `dagster_etl/DSI/connectors/mssql/` (shared MSSQL extraction machinery)
- `dagster_etl/DSA/SQL/erp/staging/` (staging asset definitions)
