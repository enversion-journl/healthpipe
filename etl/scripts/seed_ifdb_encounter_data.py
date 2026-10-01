# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Performance test data seeder for IFDB Oracle - Encounter Pipeline.

This script generates synthetic Encounter, Episode of Care, and Location records
with proper foreign key relationships to existing Patient data. Data is distributed
evenly across 53 monthly partitions (2021-09 to 2026-01) to match production patterns.

Prerequisites:
    - Patient data must be seeded first (run seed_ifdb_performance_data.py)
    - Oracle IFDB container must be running and healthy

Usage:
    # From etl directory
    python -m scripts.seed_ifdb_encounter_data

    # With custom encounter count (default: 5M encounters)
    python -m scripts.seed_ifdb_encounter_data --encounters 1000000

    # Force re-seed (truncate existing data)
    python -m scripts.seed_ifdb_encounter_data --force
"""

import argparse
import logging
import os
import random
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

try:
    import oracledb
except ImportError:
    print("ERROR: oracledb package not installed. Run: pip install oracledb")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)8s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

# Default counts - ratio: ~2 encounters per patient, ~1.5 episodes per patient
DEFAULT_ENCOUNTERS = 5_000_000
DEFAULT_EPISODES_PER_PATIENT = 1.5  # Average episodes per patient
DEFAULT_ENCOUNTERS_PER_EPISODE = 1.3  # Average encounters per episode

BATCH_SIZE = 10_000  # Rows per batch insert
COMMIT_INTERVAL = 50_000  # Commit every N rows

# Partition date range (53 months) - same as patient data
START_PARTITION = datetime(2021, 9, 1)
END_PARTITION = datetime(2026, 1, 31)

# Oracle connection defaults (from docker-compose)
ORACLE_CONFIG = {
    "host": os.getenv("IFDB_PROD_ORACLE_SERVER", "oracle"),
    "port": int(os.getenv("IFDB_PROD_ORACLE_PORT", "1521")),
    "service_name": os.getenv("IFDB_PROD_ORACLE_DATABASE", "ORACLE_DB"),
    "user": os.getenv("IFDB_PROD_ORACLE_USERNAME", "myuser"),
    "password": os.getenv("IFDB_PROD_ORACLE_PASSWORD", "tempPassword1!"),
}

# =============================================================================
# Danish Test Data - Clinical
# =============================================================================

ENCOUNTER_STATUSES = [
    ("planned", "Planlagt"),
    ("arrived", "Ankommet"),
    ("triaged", "Triageret"),
    ("in-progress", "I gang"),
    ("onleave", "Orlov"),
    ("finished", "Afsluttet"),
    ("cancelled", "Annulleret"),
]

ENCOUNTER_CLASSES = [
    ("IMP", "Indlagt"),
    ("AMB", "Ambulant"),
    ("EMER", "Akut"),
    ("HH", "Hjemmebesøg"),
    ("VR", "Virtuelt"),
]

ENCOUNTER_PRIORITIES = [
    ("1", "Akut"),
    ("2", "Hastende"),
    ("3", "Normal"),
    ("4", "Elektiv"),
]

CLINICAL_SPECIALITIES = [
    ("01", "Intern medicin"),
    ("02", "Kirurgi"),
    ("03", "Ortopædi"),
    ("04", "Kardiologi"),
    ("05", "Neurologi"),
    ("06", "Pædiatri"),
    ("07", "Gynækologi"),
    ("08", "Psykiatri"),
    ("09", "Onkologi"),
    ("10", "Geriatri"),
    ("11", "Lungemedicin"),
    ("12", "Gastroenterologi"),
]

EOC_STATUSES = [
    "planned",
    "waitlist",
    "active",
    "onhold",
    "finished",
    "cancelled",
]

PREVIOUS_EOC_REF_TYPES = [
    ("relapse", "Tilbagefald"),
    ("replacement", "Erstatning"),
    ("refer", "Henvisning"),
]

DISCHARGE_DISPOSITIONS = [
    ("home", "Udskrevet til hjemmet"),
    ("hosp-trans", "Overført til andet hospital"),
    ("long", "Langtidspleje"),
    ("aadvice", "Udskrevet mod lægelig råd"),
    ("exp", "Død"),
    ("rehab", "Rehabilitering"),
]

# Hospitals/Organizations with SOR codes (realistic Danish hospitals)
HOSPITALS = [
    ("OUH001", "Odense Universitetshospital", "461001", "1234567890123"),
    ("AAL001", "Aalborg Universitetshospital", "851001", "1234567890124"),
    ("SLB001", "Sygehus Lillebælt - Vejle", "630001", "1234567890125"),
    ("SLB002", "Sygehus Lillebælt - Kolding", "621001", "1234567890126"),
    ("SHS001", "Sydvestjysk Sygehus - Esbjerg", "561001", "1234567890127"),
    ("SVS002", "Sydvestjysk Sygehus - Grindsted", "573001", "1234567890128"),
    ("SAS001", "Sygehus Sønderjylland - Aabenraa", "580001", "1234567890129"),
    ("SAS002", "Sygehus Sønderjylland - Sønderborg", "540001", "1234567890130"),
]

# Wards/Locations
WARDS = [
    ("a1", "Afdeling a1 - Medicinsk", "MED"),
    ("a2", "Afdeling a2 - Kirurgisk", "KIR"),
    ("B1", "Afdeling B1 - Intensiv", "INT"),
    ("B2", "Afdeling B2 - Akutmodtagelse", "AKU"),
    ("C1", "Afdeling C1 - Ortopædi", "ORT"),
    ("C2", "Afdeling C2 - Kardiologi", "KAR"),
    ("d1", "Afdeling d1 - Neurologi", "NEU"),
    ("D2", "Afdeling D2 - Onkologi", "ONK"),
    ("E1", "Afdeling E1 - Pædiatri", "PÆD"),
    ("E2", "Afdeling E2 - Gynækologi", "GYN"),
]

BROUGHT_IN_BY = [
    "Selv",
    "Ambulance",
    "Politi",
    "Pårørende",
    "Hjemmepleje",
    "Egen læge",
]


# =============================================================================
# Data Generation Functions
# =============================================================================


def generate_partition_dates(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Generate list of (month_start, month_end) tuples for partition distribution."""
    partitions = []
    current = start

    while current <= end:
        if current.month == 12:
            next_month = datetime(current.year + 1, 1, 1)
        else:
            next_month = datetime(current.year, current.month + 1, 1)
        month_end = next_month - timedelta(days=1)
        partitions.append((current, month_end))
        current = next_month

    return partitions


def random_datetime_in_range(start: datetime, end: datetime) -> datetime:
    """Generate a random datetime within the given range."""
    delta = end - start
    random_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=random_seconds)


def generate_episode_of_care_batch(
    start_id: int,
    batch_size: int,
    patient_ids: list[int],
    partitions: list[tuple[datetime, datetime]],
    rows_per_partition: int,
    existing_eoc_ids: list[int],
) -> list[tuple]:
    """
    Generate a batch of Episode of Care records.

    Returns list of tuples matching IFDB.EPISODE_OF_CARE schema.
    """
    batch = []

    for i in range(batch_size):
        eoc_id = start_id + i

        # Determine which partition this episode belongs to
        partition_idx = eoc_id // rows_per_partition
        if partition_idx >= len(partitions):
            partition_idx = len(partitions) - 1

        partition_start, partition_end = partitions[partition_idx]

        # Random datetime within the partition month
        modification_datetime = random_datetime_in_range(partition_start, partition_end)

        # Assign to a random patient
        patient_id = random.choice(patient_ids)

        # Status
        status = random.choice(EOC_STATUSES)

        # Period - start before modification, end may be None for active episodes
        period_start = modification_datetime - timedelta(days=random.randint(0, 30))
        period_end = None
        if status in ["finished", "cancelled"]:
            period_end = modification_datetime - timedelta(hours=random.randint(1, 48))

        # Previous EOC reference (10% have one)
        previous_eoc_id = None
        if existing_eoc_ids and random.random() < 0.1:
            previous_eoc_id = random.choice(existing_eoc_ids)
            ref_type = random.choice(PREVIOUS_EOC_REF_TYPES)
            ref_type[0]
            ref_type[1]

        # Managing organization
        hospital = random.choice(HOSPITALS)
        org_code, org_name, org_orgcode, org_sorcode = hospital

        # Discharge info (only for finished episodes)
        discharge_code = None
        discharge_text = None
        discharge_to_code = None
        discharge_to_text = None
        if status == "finished":
            discharge = random.choice(DISCHARGE_DISPOSITIONS)
            discharge_code = discharge[0]
            discharge_text = discharge[1]
            discharge_to_code = discharge[0]
            discharge_to_text = discharge[1]

        # Created datetime (slightly before modification)
        created_datetime = modification_datetime - timedelta(
            days=random.randint(0, 30),
            hours=random.randint(0, 23),
        )

        # Group (care team identifier)
        group_name = f"Team-{random.randint(1, 20)}" if random.random() < 0.5 else None

        record = (
            eoc_id,  # OBJECT_ID
            patient_id,  # PATIENT_ID
            None,  # HEALTHISSUE_ID (not always present)
            status,  # STATUS
            period_start,  # PERIOD_START
            period_end,  # PERIOD_END
            previous_eoc_id,  # PREVIOUS_EOC_ID
            org_code,  # MANAGING_ORGANIZATION_CODE
            org_name,  # MANAGING_ORGANIZATION_NAME
            org_orgcode,  # MANAGING_ORGANIZATION_ORGCODE
            org_sorcode,  # MANAGING_ORGANIZATION_SORCODE
            discharge_code,  # DISCHARGE_DISPOSITION_CODE
            discharge_text,  # DISCHARGE_DISPOSITION_TEXT
            discharge_to_code,  # DISCHARGE_TO_CODE
            discharge_to_text,  # DISCHARGE_TO_TEXT
            f"CM{random.randint(1000, 9999)}" if random.random() < 0.3 else None,  # CARE_MANAGER_ID
            (
                f"Dr. {random.choice(['Hansen', 'Nielsen', 'Jensen'])}"
                if random.random() < 0.3
                else None
            ),  # CARE_MANAGER_NAME
            group_name,  # "GROUP"
            created_datetime,  # EPJI_CREATED_DATETIME
            modification_datetime,  # IFDB_MODIFICATION_DATETIME
        )
        batch.append(record)

    return batch


def generate_encounter_batch(
    start_id: int,
    batch_size: int,
    episode_patient_map: dict[int, int],  # eoc_id -> patient_id
    partitions: list[tuple[datetime, datetime]],
    rows_per_partition: int,
    location_codes: list[str],
) -> list[tuple]:
    """
    Generate a batch of Encounter records.

    Returns list of tuples matching IFDB.ENCOUNTER schema.
    """
    batch = []
    eoc_ids = list(episode_patient_map.keys())

    for i in range(batch_size):
        encounter_id = start_id + i

        # Determine which partition this encounter belongs to
        partition_idx = encounter_id // rows_per_partition
        if partition_idx >= len(partitions):
            partition_idx = len(partitions) - 1

        partition_start, partition_end = partitions[partition_idx]

        # Random datetime within the partition month
        modification_datetime = random_datetime_in_range(partition_start, partition_end)

        # Assign to an episode of care (and thus a patient)
        eoc_id = random.choice(eoc_ids)
        patient_id = episode_patient_map[eoc_id]

        # Status
        status_code, status_text = random.choice(ENCOUNTER_STATUSES)

        # Class
        class_code, class_text = random.choice(ENCOUNTER_CLASSES)

        # Priority
        priority_code, priority_text = random.choice(ENCOUNTER_PRIORITIES)

        # Clinical speciality
        spec_code, spec_text = random.choice(CLINICAL_SPECIALITIES)

        # Period
        period_start = modification_datetime - timedelta(
            days=random.randint(0, 7),
            hours=random.randint(0, 23),
        )
        period_end = None
        if status_code == "finished":
            period_end = modification_datetime - timedelta(hours=random.randint(1, 24))

        # Service provider (hospital)
        hospital = random.choice(HOSPITALS)
        sp_code, sp_name, sp_orgcode, sp_sorcode = hospital

        # Ward
        ward = random.choice(WARDS)
        ward_code, ward_name, ward_type = ward

        # Bed and room
        bed = f"Seng-{random.randint(1, 30)}" if class_code == "IMP" else None
        room = f"Stue-{random.randint(100, 500)}" if class_code == "IMP" else None

        # Team
        team = f"Team-{random.randint(1, 20)}" if random.random() < 0.5 else None

        # Expected discharge (for inpatients)
        expected_discharge = None
        if class_code == "IMP" and status_code == "in-progress":
            expected_discharge = modification_datetime + timedelta(days=random.randint(1, 14))

        # Treatment start
        treatment_start = (
            period_start + timedelta(hours=random.randint(0, 4)) if random.random() < 0.7 else None
        )

        # Referral ID (30% have one)
        referral_id = f"REF{random.randint(100000, 999999)}" if random.random() < 0.3 else None

        # Municipality care flag
        municipality_care = 1 if random.random() < 0.1 else 0

        # Brought in by
        brought_in = random.choice(BROUGHT_IN_BY) if class_code == "EMER" else None

        # Created datetime
        created_datetime = period_start - timedelta(
            hours=random.randint(0, 4),
            minutes=random.randint(0, 59),
        )

        record = (
            encounter_id,  # OBJECT_ID
            patient_id,  # PATIENT_ID
            eoc_id,  # EPISODE_OF_CARE_ID
            status_code,  # STATUS_CODE
            status_text,  # STATUS_TEXT
            class_code,  # CLASS_CODE
            class_text,  # CLASS_TEXT
            priority_code,  # PRIORITY_CODE
            priority_text,  # PRIORITY_TEXT
            spec_code,  # CLINICAL_SPECIALITY_CODE
            spec_text,  # CLINICAL_SPECIALITY_TEXT
            period_start,  # PERIOD_START
            period_end,  # PERIOD_END
            sp_code,  # SERVICE_PROVIDER_CODE
            sp_name,  # SERVICE_PROVIDER_NAME
            sp_orgcode,  # SERVICE_PROVIDER_ORGCODE
            sp_sorcode,  # SERVICE_PROVIDER_SORCODE
            ward_code,  # WARD_CODE
            ward_name,  # WARD_NAME
            sp_orgcode,  # WARD_ORGCODE (same as service provider)
            sp_sorcode,  # WARD_SORCODE
            bed,  # BED
            room,  # ROOM
            team,  # TEAM
            expected_discharge,  # EXPECTED_DISCHARGE
            treatment_start,  # TREATMENT_START
            referral_id,  # REFERRAL_ID
            municipality_care,  # MUNICIPALITY_CARE
            brought_in,  # BROUGHT_IN_BY
            created_datetime,  # EPJI_CREATED_DATETIME
            modification_datetime,  # EPJI_MODIFICATION_DATETIME
        )
        batch.append(record)

    return batch


def generate_location_batch(
    start_id: int,
    encounter_ids: list[int],
    hospitals: list[tuple],
    wards: list[tuple],
) -> list[tuple]:
    """
    Generate Location records linking encounters to wards.

    Returns list of tuples matching IFDB.LOCATION schema.
    """
    batch = []

    for i, encounter_id in enumerate(encounter_ids):
        hospital = random.choice(hospitals)
        ward = random.choice(wards)

        start_id + i
        location_code = f"{hospital[0]}-{ward[0]}"
        location_name = f"{ward[1]} - {hospital[1]}"
        location_sorcode = hospital[3]
        location_orgcode = hospital[2]

        record = (
            encounter_id,  # OBJECT_ID (links to encounter)
            location_code,  # LOCATION_CODE
            location_name,  # LOCATION_NAME
            location_sorcode,  # LOCATION_SORCODE
            location_orgcode,  # LOCATION_ORGCODE
        )
        batch.append(record)

    return batch


# =============================================================================
# Database Operations
# =============================================================================


def get_oracle_connection():
    """Create Oracle database connection."""
    dsn = oracledb.makedsn(
        ORACLE_CONFIG["host"],
        ORACLE_CONFIG["port"],
        service_name=ORACLE_CONFIG["service_name"],
    )

    logger.info(f"Connecting to Oracle at {ORACLE_CONFIG['host']}:{ORACLE_CONFIG['port']}")

    connection = oracledb.connect(
        user=ORACLE_CONFIG["user"],
        password=ORACLE_CONFIG["password"],
        dsn=dsn,
    )

    return connection


def get_patient_ids(connection) -> list[int]:
    """Get all patient IDs from the database."""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT PATIENT_ID FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA")
        return [row[0] for row in cursor.fetchall()]
    finally:
        cursor.close()


def ensure_tables_exist(connection) -> bool:
    """
    Ensure all required tables exist (EPISODE_OF_CARE, ENCOUNTER, LOCATION).
    Creates them if they don't exist.
    """
    cursor = connection.cursor()

    try:
        # Check and create EPISODE_OF_CARE
        cursor.execute(
            """
            SELECT COUNT(*) FROM all_tables
            WHERE owner = 'IFDB' AND table_name = 'EPISODE_OF_CARE'
        """
        )
        if cursor.fetchone()[0] == 0:
            logger.info("Creating IFDB.EPISODE_OF_CARE table...")
            cursor.execute(
                """
                CREATE TABLE IFDB.EPISODE_OF_CARE (
                    OBJECT_ID NUMBER(19) PRIMARY KEY,
                    PATIENT_ID NUMBER(19),
                    HEALTHISSUE_ID NUMBER(19),
                    STATUS VARCHAR2(50),
                    PERIOD_START TIMESTAMP,
                    PERIOD_END TIMESTAMP,
                    PREVIOUS_EOC_ID NUMBER(19),
                    PREVIOUS_EOC_REF_TYPE_CODE VARCHAR2(50),
                    PREVIOUS_EOC_REF_TYPE_TEXT VARCHAR2(200),
                    MANAGING_ORGANIZATION_CODE VARCHAR2(50),
                    MANAGING_ORGANIZATION_NAME VARCHAR2(500),
                    MANAGING_ORGANIZATION_ORGCODE VARCHAR2(50),
                    MANAGING_ORGANIZATION_SORCODE VARCHAR2(50),
                    DISCHARGE_DISPOSITION_CODE VARCHAR2(50),
                    DISCHARGE_DISPOSITION_TEXT VARCHAR2(200),
                    DISCHARGE_TO_CODE VARCHAR2(50),
                    DISCHARGE_TO_TEXT VARCHAR2(200),
                    CARE_MANAGER_ID VARCHAR2(50),
                    CARE_MANAGER_NAME VARCHAR2(200),
                    "GROUP" VARCHAR2(100),
                    EPJI_CREATED_DATETIME TIMESTAMP,
                    IFDB_MODIFICATION_DATETIME TIMESTAMP
                )
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_EOC_MOD_DATETIME
                ON IFDB.EPISODE_OF_CARE(IFDB_MODIFICATION_DATETIME)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_EOC_PATIENT_ID
                ON IFDB.EPISODE_OF_CARE(PATIENT_ID)
            """
            )
            connection.commit()
            logger.info("EPISODE_OF_CARE table created")
        else:
            logger.info("EPISODE_OF_CARE table already exists")

        # Check and create ENCOUNTER
        cursor.execute(
            """
            SELECT COUNT(*) FROM all_tables
            WHERE owner = 'IFDB' AND table_name = 'ENCOUNTER'
        """
        )
        if cursor.fetchone()[0] == 0:
            logger.info("Creating IFDB.ENCOUNTER table...")
            cursor.execute(
                """
                CREATE TABLE IFDB.ENCOUNTER (
                    OBJECT_ID NUMBER(19) PRIMARY KEY,
                    PATIENT_ID NUMBER(19),
                    EPISODE_OF_CARE_ID NUMBER(19),
                    STATUS_CODE VARCHAR2(50),
                    STATUS_TEXT VARCHAR2(200),
                    CLASS_CODE VARCHAR2(50),
                    CLASS_TEXT VARCHAR2(200),
                    PRIORITY_CODE VARCHAR2(50),
                    PRIORITY_TEXT VARCHAR2(200),
                    CLINICAL_SPECIALITY_CODE VARCHAR2(50),
                    CLINICAL_SPECIALITY_TEXT VARCHAR2(200),
                    PERIOD_START TIMESTAMP,
                    PERIOD_END TIMESTAMP,
                    SERVICE_PROVIDER_CODE VARCHAR2(50),
                    SERVICE_PROVIDER_NAME VARCHAR2(500),
                    SERVICE_PROVIDER_ORGCODE VARCHAR2(50),
                    SERVICE_PROVIDER_SORCODE VARCHAR2(50),
                    WARD_CODE VARCHAR2(50),
                    WARD_NAME VARCHAR2(500),
                    WARD_ORGCODE VARCHAR2(50),
                    WARD_SORCODE VARCHAR2(50),
                    BED VARCHAR2(100),
                    ROOM VARCHAR2(100),
                    TEAM VARCHAR2(100),
                    EXPECTED_DISCHARGE TIMESTAMP,
                    TREATMENT_START TIMESTAMP,
                    REFERRAL_ID VARCHAR2(100),
                    MUNICIPALITY_CARE NUMBER(1),
                    BROUGHT_IN_BY VARCHAR2(200),
                    EPJI_CREATED_DATETIME TIMESTAMP,
                    EPJI_MODIFICATION_DATETIME TIMESTAMP
                )
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_ENC_MOD_DATETIME
                ON IFDB.ENCOUNTER(EPJI_MODIFICATION_DATETIME)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_ENC_PATIENT_ID
                ON IFDB.ENCOUNTER(PATIENT_ID)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_ENC_EOC_ID
                ON IFDB.ENCOUNTER(EPISODE_OF_CARE_ID)
            """
            )
            connection.commit()
            logger.info("ENCOUNTER table created")
        else:
            logger.info("ENCOUNTER table already exists")

        # Check and create LOCATION
        cursor.execute(
            """
            SELECT COUNT(*) FROM all_tables
            WHERE owner = 'IFDB' AND table_name = 'LOCATION'
        """
        )
        if cursor.fetchone()[0] == 0:
            logger.info("Creating IFDB.LOCATION table...")
            cursor.execute(
                """
                CREATE TABLE IFDB.LOCATION (
                    OBJECT_ID NUMBER(19),
                    LOCATION_CODE VARCHAR2(100),
                    LOCATION_NAME VARCHAR2(500),
                    LOCATION_SORCODE VARCHAR2(50),
                    LOCATION_ORGCODE VARCHAR2(50),
                    PERIOD_START TIMESTAMP,
                    PERIOD_END TIMESTAMP
                )
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_LOC_OBJECT_ID
                ON IFDB.LOCATION(OBJECT_ID)
            """
            )
            cursor.execute(
                """
                CREATE INDEX IFDB.IDX_LOC_CODE
                ON IFDB.LOCATION(LOCATION_CODE)
            """
            )
            connection.commit()
            logger.info("LOCATION table created")
        else:
            logger.info("LOCATION table already exists")

        return True

    except oracledb.DatabaseError as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        cursor.close()


def get_row_counts(connection) -> dict[str, int]:
    """Get current row counts for all tables."""
    cursor = connection.cursor()
    counts = {}

    for table in ["EPISODE_OF_CARE", "ENCOUNTER", "LOCATION"]:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM IFDB.{table}")
            counts[table] = cursor.fetchone()[0]
        except oracledb.DatabaseError:
            counts[table] = 0

    cursor.close()
    return counts


def truncate_tables(connection):
    """Truncate encounter-related tables (uses DELETE if TRUNCATE not permitted)."""
    cursor = connection.cursor()
    try:
        for table in ["LOCATION", "ENCOUNTER", "EPISODE_OF_CARE"]:
            logger.info(f"Clearing IFDB.{table}...")
            try:
                # Try TRUNCATE first (faster)
                cursor.execute(f"TRUNCATE TABLE IFDB.{table}")
            except oracledb.DatabaseError:
                # Fall back to DELETE if TRUNCATE not permitted
                try:
                    cursor.execute(f"DELETE FROM IFDB.{table}")
                    logger.info(f"Deleted all rows from {table}")
                except oracledb.DatabaseError as e:
                    logger.warning(f"Could not clear {table}: {e}")
        connection.commit()
    finally:
        cursor.close()


def insert_eoc_batch(connection, cursor, batch: list[tuple]):
    """Insert a batch of Episode of Care records."""
    insert_sql = """
        INSERT INTO IFDB.EPISODE_OF_CARE (
            OBJECT_ID, PATIENT_ID, HEALTHISSUE_ID, STATUS,
            PERIOD_START, PERIOD_END, PREVIOUS_EOC_ID,
            MANAGING_ORGANIZATION_CODE, MANAGING_ORGANIZATION_NAME,
            MANAGING_ORGANIZATION_ORGCODE, MANAGING_ORGANIZATION_SORCODE,
            DISCHARGE_DISPOSITION_CODE, DISCHARGE_DISPOSITION_TEXT,
            DISCHARGE_TO_CODE, DISCHARGE_TO_TEXT,
            CARE_MANAGER_ID, CARE_MANAGER_NAME, "GROUP",
            EPJI_CREATED_DATETIME, IFDB_MODIFICATION_DATETIME
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
            :11, :12, :13, :14, :15, :16, :17, :18, :19, :20
        )
    """
    cursor.executemany(insert_sql, batch)


def insert_encounter_batch(connection, cursor, batch: list[tuple]):
    """Insert a batch of Encounter records."""
    insert_sql = """
        INSERT INTO IFDB.ENCOUNTER (
            OBJECT_ID, PATIENT_ID, EPISODE_OF_CARE_ID,
            STATUS_CODE, STATUS_TEXT, CLASS_CODE, CLASS_TEXT,
            PRIORITY_CODE, PRIORITY_TEXT,
            CLINICAL_SPECIALITY_CODE, CLINICAL_SPECIALITY_TEXT,
            PERIOD_START, PERIOD_END,
            SERVICE_PROVIDER_CODE, SERVICE_PROVIDER_NAME,
            SERVICE_PROVIDER_ORGCODE, SERVICE_PROVIDER_SORCODE,
            WARD_CODE, WARD_NAME, WARD_ORGCODE, WARD_SORCODE,
            BED, ROOM, TEAM, EXPECTED_DISCHARGE, TREATMENT_START,
            REFERRAL_ID, MUNICIPALITY_CARE, BROUGHT_IN_BY,
            EPJI_CREATED_DATETIME, EPJI_MODIFICATION_DATETIME
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10,
            :11, :12, :13, :14, :15, :16, :17, :18, :19, :20,
            :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31
        )
    """
    cursor.executemany(insert_sql, batch)


def insert_location_batch(connection, cursor, batch: list[tuple]):
    """Insert a batch of Location records."""
    insert_sql = """
        INSERT INTO IFDB.LOCATION (
            OBJECT_ID, LOCATION_CODE, LOCATION_NAME,
            LOCATION_SORCODE, LOCATION_ORGCODE
        ) VALUES (:1, :2, :3, :4, :5)
    """
    cursor.executemany(insert_sql, batch)


# =============================================================================
# Main Seeding Logic
# =============================================================================


def seed_encounter_data(total_encounters: int, force: bool = False):
    """
    Main function to seed encounter-related performance test data.

    Args:
        total_encounters: Total number of encounter records to generate
        force: If True, truncate existing data and re-seed
    """
    logger.info("=" * 70)
    logger.info("IFDB Encounter Pipeline Data Seeder")
    logger.info("=" * 70)

    # Calculate record counts
    total_episodes = int(total_encounters / DEFAULT_ENCOUNTERS_PER_EPISODE)

    logger.info(f"Target encounters: {total_encounters:,}")
    logger.info(f"Target episodes: {total_episodes:,}")

    # Generate partition list
    partitions = generate_partition_dates(START_PARTITION, END_PARTITION)
    eoc_per_partition = total_episodes // len(partitions)
    enc_per_partition = total_encounters // len(partitions)

    logger.info(f"Partitions: {len(partitions)} months")
    logger.info(f"Episodes per partition: ~{eoc_per_partition:,}")
    logger.info(f"Encounters per partition: ~{enc_per_partition:,}")
    logger.info("=" * 70)

    # Connect to Oracle
    try:
        connection = get_oracle_connection()
        logger.info("Connected to Oracle successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        sys.exit(1)

    try:
        # Get patient IDs
        patient_ids = get_patient_ids(connection)
        if not patient_ids:
            logger.error("No patients found! Run seed_ifdb_performance_data.py first.")
            sys.exit(1)
        logger.info(f"Found {len(patient_ids):,} patients")

        # Ensure tables exist
        if not ensure_tables_exist(connection):
            logger.error("Failed to ensure tables exist")
            sys.exit(1)

        # Check current counts
        counts = get_row_counts(connection)
        logger.info(
            f"Current row counts: EOC={counts['EPISODE_OF_CARE']:,}, "
            f"Encounter={counts['ENCOUNTER']:,}, Location={counts['LOCATION']:,}"
        )

        if counts["ENCOUNTER"] >= total_encounters and not force:
            logger.info("Tables already have sufficient data (>= target)")
            logger.info("Use --force to re-seed data")
            return

        if any(c > 0 for c in counts.values()) and force:
            truncate_tables(connection)
            counts = {k: 0 for k in counts}

        # =====================================================================
        # Phase 1: Seed Episode of Care
        # =====================================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("Phase 1: Seeding Episode of Care records")
        logger.info("=" * 70)

        cursor = connection.cursor()
        start_time = time.time()
        eoc_inserted = 0
        last_commit = 0
        existing_eoc_ids: list[int] = []
        episode_patient_map: dict[int, int] = {}  # eoc_id -> patient_id

        try:
            while eoc_inserted < total_episodes:
                batch_start = eoc_inserted
                remaining = total_episodes - eoc_inserted
                current_batch_size = min(BATCH_SIZE, remaining)

                batch = generate_episode_of_care_batch(
                    start_id=batch_start + 1,
                    batch_size=current_batch_size,
                    patient_ids=patient_ids,
                    partitions=partitions,
                    rows_per_partition=eoc_per_partition,
                    existing_eoc_ids=existing_eoc_ids,
                )

                insert_eoc_batch(connection, cursor, batch)

                # Track for later reference
                for record in batch:
                    eoc_id = record[0]
                    patient_id = record[1]
                    existing_eoc_ids.append(eoc_id)
                    episode_patient_map[eoc_id] = patient_id

                eoc_inserted += current_batch_size

                if eoc_inserted - last_commit >= COMMIT_INTERVAL:
                    connection.commit()
                    last_commit = eoc_inserted

                    elapsed = time.time() - start_time
                    rate = eoc_inserted / elapsed if elapsed > 0 else 0
                    eta = (total_episodes - eoc_inserted) / rate if rate > 0 else 0

                    logger.info(
                        f"EOC Progress: {eoc_inserted:,}/{total_episodes:,} "
                        f"({100 * eoc_inserted / total_episodes:.1f}%) | "
                        f"Rate: {rate:.0f}/sec | ETA: {eta / 60:.1f} min"
                    )

            connection.commit()
            logger.info(f"Phase 1 complete: {eoc_inserted:,} Episode of Care records inserted")

        except Exception as e:
            logger.error(f"Error inserting EOC: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()

        # =====================================================================
        # Phase 2: Seed Encounters
        # =====================================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("Phase 2: Seeding Encounter records")
        logger.info("=" * 70)

        cursor = connection.cursor()
        start_time = time.time()
        enc_inserted = 0
        last_commit = 0
        location_codes = [f"{h[0]}-{w[0]}" for h in HOSPITALS for w in WARDS]

        try:
            while enc_inserted < total_encounters:
                batch_start = enc_inserted
                remaining = total_encounters - enc_inserted
                current_batch_size = min(BATCH_SIZE, remaining)

                batch = generate_encounter_batch(
                    start_id=batch_start + 1,
                    batch_size=current_batch_size,
                    episode_patient_map=episode_patient_map,
                    partitions=partitions,
                    rows_per_partition=enc_per_partition,
                    location_codes=location_codes,
                )

                insert_encounter_batch(connection, cursor, batch)
                enc_inserted += current_batch_size

                if enc_inserted - last_commit >= COMMIT_INTERVAL:
                    connection.commit()
                    last_commit = enc_inserted

                    elapsed = time.time() - start_time
                    rate = enc_inserted / elapsed if elapsed > 0 else 0
                    eta = (total_encounters - enc_inserted) / rate if rate > 0 else 0

                    logger.info(
                        f"Encounter Progress: {enc_inserted:,}/{total_encounters:,} "
                        f"({100 * enc_inserted / total_encounters:.1f}%) | "
                        f"Rate: {rate:.0f}/sec | ETA: {eta / 60:.1f} min"
                    )

            connection.commit()
            logger.info(f"Phase 2 complete: {enc_inserted:,} Encounter records inserted")

        except Exception as e:
            logger.error(f"Error inserting Encounters: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()

        # =====================================================================
        # Phase 3: Seed Locations (one per encounter)
        # =====================================================================
        logger.info("")
        logger.info("=" * 70)
        logger.info("Phase 3: Seeding Location records")
        logger.info("=" * 70)

        cursor = connection.cursor()
        start_time = time.time()
        loc_inserted = 0
        last_commit = 0

        # Get all encounter IDs
        cursor.execute("SELECT OBJECT_ID FROM IFDB.ENCOUNTER ORDER BY OBJECT_ID")
        all_encounter_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()

        cursor = connection.cursor()
        try:
            # Process in batches
            for i in range(0, len(all_encounter_ids), BATCH_SIZE):
                batch_encounter_ids = all_encounter_ids[i : i + BATCH_SIZE]
                batch = generate_location_batch(
                    start_id=loc_inserted + 1,
                    encounter_ids=batch_encounter_ids,
                    hospitals=HOSPITALS,
                    wards=WARDS,
                )

                insert_location_batch(connection, cursor, batch)
                loc_inserted += len(batch)

                if loc_inserted - last_commit >= COMMIT_INTERVAL:
                    connection.commit()
                    last_commit = loc_inserted

                    elapsed = time.time() - start_time
                    rate = loc_inserted / elapsed if elapsed > 0 else 0
                    eta = (len(all_encounter_ids) - loc_inserted) / rate if rate > 0 else 0

                    logger.info(
                        f"Location Progress: {loc_inserted:,}/{len(all_encounter_ids):,} "
                        f"({100 * loc_inserted / len(all_encounter_ids):.1f}%) | "
                        f"Rate: {rate:.0f}/sec | ETA: {eta / 60:.1f} min"
                    )

            connection.commit()
            logger.info(f"Phase 3 complete: {loc_inserted:,} Location records inserted")

        except Exception as e:
            logger.error(f"Error inserting Locations: {e}")
            connection.rollback()
            raise
        finally:
            cursor.close()

        # =====================================================================
        # Final Report
        # =====================================================================
        final_counts = get_row_counts(connection)

        logger.info("")
        logger.info("=" * 70)
        logger.info("SEEDING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Episode of Care records: {final_counts['EPISODE_OF_CARE']:,}")
        logger.info(f"Encounter records: {final_counts['ENCOUNTER']:,}")
        logger.info(f"Location records: {final_counts['LOCATION']:,}")
        logger.info("=" * 70)

        # Show partition distribution
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    TO_CHAR(EPJI_MODIFICATION_DATETIME, 'YYYY-MM') as partition_month,
                    COUNT(*) as row_count
                FROM IFDB.ENCOUNTER
                GROUP BY TO_CHAR(EPJI_MODIFICATION_DATETIME, 'YYYY-MM')
                ORDER BY partition_month
            """
            )

            logger.info("\nEncounter Partition Distribution (sample):")
            rows = cursor.fetchall()
            for i, (month, count) in enumerate(rows):
                if i < 3 or i >= len(rows) - 3:
                    logger.info(f"  {month}: {count:,} encounters")
                elif i == 3:
                    logger.info("  ...")

        finally:
            cursor.close()

    finally:
        connection.close()
        logger.info("Connection closed")


# =============================================================================
# CLI Entry Point
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Seed IFDB Oracle with Encounter pipeline performance test data"
    )
    parser.add_argument(
        "--encounters",
        type=int,
        default=DEFAULT_ENCOUNTERS,
        help=f"Total number of encounters to generate (default: {DEFAULT_ENCOUNTERS:,})",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-seed by truncating existing data",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Oracle host (overrides IFDB_ORACLE_HOST env var)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help="Oracle port (overrides IFDB_ORACLE_PORT env var)",
    )

    args = parser.parse_args()

    if args.host:
        ORACLE_CONFIG["host"] = args.host
    if args.port:
        ORACLE_CONFIG["port"] = args.port

    seed_encounter_data(args.encounters, args.force)


if __name__ == "__main__":
    main()
