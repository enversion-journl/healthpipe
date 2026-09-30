# Performance Test Data Infrastructure

This document describes how to set up and use the performance test data infrastructure for testing the FHIR ETL pipeline with production-scale data volumes.

## Overview

The performance test infrastructure allows you to:
- Generate 2.5 million synthetic patient records
- Generate 5 million synthetic encounter records with proper relationships
- Generate episode of care and location records with foreign key integrity
- Distribute data evenly across 53 monthly partitions (2021-09 to 2026-01)
- Run the full FHIR pipelines locally for profiling and optimization
- Benchmark different configurations and strategies

## Data Distribution

### Patient Data

| Metric | Value |
|--------|-------|
| Total records | 2,500,000 |
| Partition range | 2021-09 to 2026-01 |
| Number of partitions | 53 months |
| Records per partition | ~47,170 |

### Encounter Pipeline Data

| Entity | Records | Notes |
|--------|---------|-------|
| Patients | 2,500,000 | Base data (must be seeded first) |
| Episodes of Care | ~3,850,000 | ~1.5 per patient |
| Encounters | 5,000,000 | ~2 per patient, linked to episodes |
| Locations | 5,000,000 | One per encounter |
| Aktivitetsresultat | 500,000 | With realistic XML CLOBs (~1.3KB each) |

This distribution matches the production data pattern, where records are distributed across monthly partitions based on their modification timestamps.

## Quick Start

### 1. Seed Patient Data (Required First)

From within the devcontainer, run:

```bash
cd etl
python -m scripts.seed_ifdb_performance_data
```

This will:
- Connect to the local Oracle IFDB container
- Create the `IFDB.PATIENT_OPLYSNINGER_COLUMNA` table if needed
- Generate and insert 2.5M patient records in batches
- Data is persisted in the Docker volume (survives container restarts)

**Options:**

```bash
# Custom row count (e.g., 100k for quick tests)
python -m scripts.seed_ifdb_performance_data --rows 100000

# Force re-seed (truncates existing data)
python -m scripts.seed_ifdb_performance_data --force

# Custom Oracle host (if not using docker-compose)
python -m scripts.seed_ifdb_performance_data --host localhost --port 1522
```

### 2. Seed Encounter Pipeline Data (Optional)

After seeding patient data, you can seed the encounter pipeline:

```bash
cd etl
python -m scripts.seed_ifdb_encounter_data
```

This will:
- Create `IFDB.EPISODE_OF_CARE`, `IFDB.ENCOUNTER`, and `IFDB.LOCATION` tables
- Generate ~3.85M episodes linked to existing patients
- Generate 5M encounters linked to episodes
- Generate location records for each encounter
- All records have proper foreign key relationships

**Options:**

```bash
# Custom encounter count (e.g., 500k for quick tests)
python -m scripts.seed_ifdb_encounter_data --encounters 500000

# Force re-seed (truncates existing encounter-related tables)
python -m scripts.seed_ifdb_encounter_data --force
```

### 3. Seed Aktivitetsresultat Data (Optional)

After seeding patient and encounter data, seed activity results with XML:

```bash
cd etl
python -m scripts.seed_ifdb_aktivitetsresultat_data
```

This will:
- Create `IFDB.AKTIVITETSRESULTAT` table with 34 columns including XML CLOB
- Generate 500K activity result records with realistic clinical XML
- XML contains 10 different clinical form structures (vitals, pain, nutrition, etc.)
- Links to existing patients, encounters, and episodes

**Options:**

```bash
# Custom row count (e.g., 100k for quick tests)
python -m scripts.seed_ifdb_aktivitetsresultat_data --rows 100000

# Force re-seed
python -m scripts.seed_ifdb_aktivitetsresultat_data --force
```

### 4. Run Performance Tests

```bash
# Run all performance tests
USE_REAL_DB_FOR_TESTS=true pytest tests/performance/ -v -s

# Run specific test class
USE_REAL_DB_FOR_TESTS=true pytest tests/performance/test_patient_pipeline_perf.py::TestExtractionPerformance -v -s

# Run with timing details
USE_REAL_DB_FOR_TESTS=true pytest tests/performance/ -v -s --durations=0

# Limit rows processed (for faster iteration)
PERF_TEST_MAX_ROWS=1000 USE_REAL_DB_FOR_TESTS=true pytest tests/performance/ -v -s
```

## Alternative: Docker Compose Profile

You can also seed data using docker compose:

```bash
# From repository root
# Seed patient data
docker compose -f .devcontainer/docker-compose.yml --profile performance up seed-performance-data

# Seed encounter pipeline data (after patient data)
docker compose -f .devcontainer/docker-compose.yml --profile performance up seed-encounter-data
```

This runs the seed scripts in containers with the correct Oracle connection settings.

## Generated Data Schema

### Patient Table (IFDB.PATIENT_OPLYSNINGER_COLUMNA)

The patient seed script generates data matching this schema:

| Column | Description | Sample Value |
|--------|-------------|--------------|
| PATIENT_ID | Unique patient identifier | 1, 2, 3, ... |
| CPR | Danish CPR number | "0101901234" |
| FORNAVNE | First name(s) | "Anders" |
| EFTERNAVN | Surname | "Jensen" |
| VEJ | Street address | "Hovedgaden 42" |
| POSTNR | Postal code | "5000" |
| STEDNAVN | City name | "Odense" |
| KOMMUNEKODE | Municipality code | "461" |
| KOEN_KODE | Gender code | "1" (male), "2" (female) |
| KOEN_KODETEKST | Gender text | "Mand", "Kvinde" |
| FOEDSELSDATO | Birth date | "01JAN1990" |
| LAEGENAVN | Doctor name | "Dr. Hansen" |
| LAEGENUMMER | Doctor ID | "123456" |
| PERSON_STATUSKODE | Status code | "01" |
| PERSON_STATUSKODETEKST | Status text | "aktiv" |
| HAR_ADRESSEBESKYTTELSE | Address protection | 0, 1 |
| OENSKER_DISKRETION | Discretion flag | 0, 1 |
| BEMAERKNINGER | Remarks | NULL or text |
| PERSON_DISTRIKTKODE | District code | "D001" |
| PERSON_DISTRIKTKODETEKST | District text | "Distrikt Nord" |
| PERSON_SOCIALDISTRIKTKODE | Social district code | "S001" |
| PERSON_SOCIALDISTRIKTKODETEKST | Social district text | "Social Distrikt A" |
| OPDATERET_TID | Update timestamp | 2023-06-15 10:30:00 |
| MODTIDSPUNKT | Partition timestamp | 2023-06-15 10:30:00 |

### Episode of Care Table (IFDB.EPISODE_OF_CARE)

| Column | Description | Sample Value |
|--------|-------------|--------------|
| OBJECT_ID | Unique episode identifier | 1, 2, 3, ... |
| PATIENT_ID | Foreign key to Patient | 12345 |
| STATUS | Episode status | "active", "finished" |
| PERIOD_START | Episode start time | 2023-06-01 08:00:00 |
| PERIOD_END | Episode end time | 2023-06-15 10:00:00 |
| PREVIOUS_EOC_ID | Link to previous episode | 999 |
| MANAGING_ORGANIZATION_CODE | Hospital code | "OUH001" |
| MANAGING_ORGANIZATION_SORCODE | SOR code | "1234567890123" |
| IFDB_MODIFICATION_DATETIME | Partition timestamp | 2023-06-15 10:30:00 |

### Encounter Table (IFDB.ENCOUNTER)

| Column | Description | Sample Value |
|--------|-------------|--------------|
| OBJECT_ID | Unique encounter identifier | 1, 2, 3, ... |
| PATIENT_ID | Foreign key to Patient | 12345 |
| EPISODE_OF_CARE_ID | Foreign key to Episode | 67890 |
| STATUS_CODE | Encounter status | "in-progress", "finished" |
| CLASS_CODE | Encounter class | "IMP" (inpatient), "AMB" (ambulatory) |
| PERIOD_START | Encounter start time | 2023-06-10 08:00:00 |
| PERIOD_END | Encounter end time | 2023-06-12 14:00:00 |
| SERVICE_PROVIDER_SORCODE | Hospital SOR code | "1234567890123" |
| WARD_CODE | Ward identifier | "a1" |
| EPJI_MODIFICATION_DATETIME | Partition timestamp | 2023-06-15 10:30:00 |

### Location Table (IFDB.LOCATION)

| Column | Description | Sample Value |
|--------|-------------|--------------|
| OBJECT_ID | Links to Encounter | 12345 |
| LOCATION_CODE | Unique location code | "OUH001-a1" |
| LOCATION_NAME | Location name | "Afdeling a1 - Odense" |
| LOCATION_SORCODE | Organization SOR code | "1234567890123" |
| LOCATION_ORGCODE | Organization code | "461001" |

## Performance Test Classes

### TestDataVerification

Verifies that test data is properly seeded:
- `test_total_row_count` - Checks total rows in table
- `test_partition_has_data` - Checks each partition has data

### TestExtractionPerformance

Benchmarks Oracle extraction:
- `test_single_partition_extraction` - Extract one month (~47k rows)
- `test_extraction_consistency` - Verify consistent extraction across partitions

### TestTransformationPerformance

Benchmarks FHIR transformation:
- `test_patient_transformation_throughput` - Measure transformation rate

### TestFullPipelinePerformance

End-to-end pipeline benchmark:
- `test_full_pipeline_single_partition` - Full ETL cycle with timing breakdown

### TestIdResolutionStrategies

Compare ID resolution strategies:
- `test_compare_id_resolution_strategies` - memory_cache vs direct_postgres

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| USE_REAL_DB_FOR_TESTS | false | Enable real database connections |
| PERF_TEST_MAX_ROWS | 5000 | Max rows to process in tests |
| IFDB_ORACLE_HOST | oracle | Oracle host |
| IFDB_ORACLE_PORT | 1521 | Oracle port |
| IFDB_ORACLE_SERVICE | ORACLE_DB | Oracle service name |
| IFDB_ORACLE_USER | myuser | Oracle username |
| IFDB_ORACLE_PASSWORD | tempPassword1! | Oracle password |

## Timing Expectations

Based on production observations:

| Phase | Expected Rate | Notes |
|-------|---------------|-------|
| Extraction | 5,000-10,000 rows/s | Depends on Oracle performance |
| Transformation | 10,000-50,000 rows/s | CPU-bound |
| ID Resolution (cache) | 50,000+ IDs/s | After cache preload |
| ID Resolution (query) | 1,000-5,000 IDs/s | Direct PostgreSQL queries |
| Posting | 100-500 resources/s | Network-bound to FHIR server |

## Troubleshooting

### "Cannot connect to Oracle"

1. Check Oracle container is running and healthy:
   ```bash
   docker ps | grep oracle
   ```

2. Verify credentials match docker-compose.yml

3. Try connecting manually:
   ```bash
   sqlplus myuser/tempPassword1!@oracle:1521/ORACLE_DB
   ```

### "Insufficient test data"

Run the seed script to populate data:
```bash
cd etl
python -m scripts.seed_ifdb_performance_data
```

### Tests are too slow

Limit the rows processed:
```bash
PERF_TEST_MAX_ROWS=1000 USE_REAL_DB_FOR_TESTS=true pytest tests/performance/ -v -s
```

### Data corruption / need to reseed

Force a fresh seed:
```bash
python -m scripts.seed_ifdb_performance_data --force
```

## Optimization Workflow

1. **Baseline**: Run full pipeline test to establish baseline timing
2. **Identify bottleneck**: Look at timing breakdown to find slowest phase
3. **Make changes**: Modify code (batch sizes, parallelism, caching, etc.)
4. **Measure**: Re-run tests to compare
5. **Iterate**: Repeat until performance meets requirements

Example optimization targets:
- Increase extraction batch size
- Tune chunk_size for FHIR posting
- Adjust max_workers for parallelism
- Switch ID resolution strategy
- Optimize database indexes
