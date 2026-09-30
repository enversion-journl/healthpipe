# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Performance test data seeder for IFDB Oracle database.

This script generates 2.5 million synthetic patient records and seeds them
into the local Oracle IFDB container. Data is distributed evenly across
53 monthly partitions (2021-09 to 2026-01) to match production data patterns.

Usage:
    # From etl directory
    python -m scripts.seed_ifdb_performance_data

    # With custom row count
    python -m scripts.seed_ifdb_performance_data --rows 100000

    # Force re-seed (truncate existing data)
    python -m scripts.seed_ifdb_performance_data --force
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

DEFAULT_TOTAL_ROWS = 2_500_000
BATCH_SIZE = 10_000  # Rows per batch insert
COMMIT_INTERVAL = 50_000  # Commit every N rows

# Partition date range (53 months)
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
# Danish Test Data
# =============================================================================

DANISH_FIRST_NAMES_MALE = [
    "Anders",
    "Bo",
    "Christian",
    "Daniel",
    "Erik",
    "Frederik",
    "Gustav",
    "Henrik",
    "Ivan",
    "Jakob",
    "Klaus",
    "Lars",
    "Martin",
    "Niels",
    "Ole",
    "Peter",
    "Rasmus",
    "Søren",
    "Thomas",
    "Ulrik",
    "Viktor",
    "William",
    "Jens",
    "Mads",
    "Mikkel",
    "Jonas",
    "Emil",
    "Simon",
    "Sebastian",
    "Oscar",
]

DANISH_FIRST_NAMES_FEMALE = [
    "Anna",
    "Bente",
    "Camilla",
    "Dorthe",
    "Emma",
    "Freja",
    "Gitte",
    "Hanne",
    "Ida",
    "Julie",
    "Karen",
    "Lene",
    "Marie",
    "Nanna",
    "Olivia",
    "Pia",
    "Rita",
    "Sara",
    "Trine",
    "Ulla",
    "Victoria",
    "Sofie",
    "Laura",
    "Maja",
    "Clara",
    "Emilie",
    "Isabella",
    "Mathilde",
    "Amalie",
    "Alberte",
]

DANISH_SURNAMES = [
    "Jensen",
    "Nielsen",
    "Hansen",
    "Pedersen",
    "Andersen",
    "Christensen",
    "Larsen",
    "Sørensen",
    "Rasmussen",
    "Jørgensen",
    "Petersen",
    "Madsen",
    "Kristensen",
    "Olsen",
    "Thomsen",
    "Christiansen",
    "Poulsen",
    "Johansen",
    "Knudsen",
    "Mortensen",
    "Møller",
    "Jacobsen",
    "Jakobsen",
    "Lund",
    "Schmidt",
    "Eriksen",
    "Holm",
    "Dahl",
    "Henriksen",
    "Frederiksen",
]

DANISH_STREETS = [
    "Hovedgaden",
    "Vestergade",
    "Østergade",
    "Nørregade",
    "Søndergade",
    "Stationsvej",
    "Kirkevej",
    "Skovvej",
    "Parkvej",
    "Skolevej",
    "Byvej",
    "Møllevej",
    "Engvej",
    "Havnevej",
    "Åboulevarden",
    "Gammel Kongevej",
    "Nørrebrogade",
    "Vesterbrogade",
    "Amagerbrogade",
    "Frederikssundsvej",
    "Roskildevej",
    "Lyngbyvej",
    "Helsingørvej",
]

DANISH_CITIES = [
    ("København", "1000", "101"),
    ("Aarhus", "8000", "751"),
    ("Odense", "5000", "461"),
    ("Aalborg", "9000", "851"),
    ("Esbjerg", "6700", "561"),
    ("Randers", "8900", "730"),
    ("Kolding", "6000", "621"),
    ("Horsens", "8700", "615"),
    ("Vejle", "7100", "630"),
    ("Roskilde", "4000", "265"),
    ("Herning", "7400", "657"),
    ("Silkeborg", "8600", "740"),
    ("Næstved", "4700", "370"),
    ("Fredericia", "7000", "607"),
    ("Viborg", "8800", "791"),
    ("Køge", "4600", "259"),
    ("Holstebro", "7500", "661"),
    ("Slagelse", "4200", "330"),
    ("Hillerød", "3400", "219"),
    ("Sønderborg", "6400", "540"),
]

DOCTOR_NAMES = [
    "Dr. Hansen",
    "Dr. Nielsen",
    "Dr. Jensen",
    "Dr. Pedersen",
    "Dr. Andersen",
    "Dr. Larsen",
    "Dr. Sørensen",
    "Dr. Rasmussen",
    "Dr. Christensen",
    "Dr. Petersen",
    "Dr. Olsen",
    "Dr. Madsen",
]

PERSON_STATUS = [
    ("01", "aktiv"),
    ("02", "Inaktiv"),
    ("03", "Død"),
    ("05", "Forsvundet"),
    ("07", "Udvandret"),
]

DISTRIKT_CODES = [
    ("D001", "Distrikt Nord"),
    ("D002", "Distrikt Syd"),
    ("D003", "Distrikt Øst"),
    ("D004", "Distrikt Vest"),
    ("D005", "Distrikt Centrum"),
]

SOCIAL_DISTRIKT_CODES = [
    ("S001", "Social Distrikt A"),
    ("S002", "Social Distrikt B"),
    ("S003", "Social Distrikt C"),
]


# =============================================================================
# Data Generation Functions
# =============================================================================


def generate_cpr(birth_date: datetime, gender_code: str, index: int) -> str:
    """
    Generate a valid-format Danish CPR number.

    Format: DDMMYY-XXXX where last digit is odd for males, even for females.
    Note: These are synthetic CPRs for testing only.
    """
    day = birth_date.day
    month = birth_date.month
    year = birth_date.year % 100

    # Generate last 4 digits based on index and gender
    # Last digit: odd for male (gender_code=1), even for female (gender_code=2)
    base_num = (index % 9000) + 1000
    if gender_code == "1":  # Male
        last_digit = (base_num % 5) * 2 + 1  # 1, 3, 5, 7, 9
    else:  # Female
        last_digit = (base_num % 5) * 2  # 0, 2, 4, 6, 8

    serial = (base_num // 10) % 1000

    return f"{day:02d}{month:02d}{year:02d}{serial:03d}{last_digit}"


def generate_partition_dates(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """
    Generate list of (month_start, month_end) tuples for partition distribution.
    """
    partitions = []
    current = start

    while current <= end:
        # Get last day of month
        if current.month == 12:
            next_month = datetime(current.year + 1, 1, 1)
        else:
            next_month = datetime(current.year, current.month + 1, 1)
        month_end = next_month - timedelta(days=1)

        partitions.append((current, month_end))
        current = next_month

    return partitions


def generate_patient_batch(
    start_id: int,
    batch_size: int,
    partitions: list[tuple[datetime, datetime]],
    rows_per_partition: int,
) -> list[tuple]:
    """
    Generate a batch of patient records.

    Returns list of tuples matching PATIENT_OPLYSNINGER_COLUMNA schema.
    """
    batch = []

    for i in range(batch_size):
        patient_id = start_id + i

        # Determine which partition this patient belongs to
        partition_idx = patient_id // rows_per_partition
        if partition_idx >= len(partitions):
            partition_idx = len(partitions) - 1

        partition_start, partition_end = partitions[partition_idx]

        # Random date within the partition month
        days_in_month = (partition_end - partition_start).days + 1
        random_day = random.randint(0, days_in_month - 1)
        modtidspunkt = partition_start + timedelta(
            days=random_day,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59),
        )

        # Gender
        gender_code = "1" if random.random() < 0.5 else "2"
        gender_text = "Mand" if gender_code == "1" else "Kvinde"

        # Name based on gender
        if gender_code == "1":
            first_name = random.choice(DANISH_FIRST_NAMES_MALE)
        else:
            first_name = random.choice(DANISH_FIRST_NAMES_FEMALE)
        last_name = random.choice(DANISH_SURNAMES)

        # Birth date (age 0-100)
        birth_year = random.randint(1924, 2024)
        birth_month = random.randint(1, 12)
        birth_day = random.randint(1, 28)
        birth_date = datetime(birth_year, birth_month, birth_day)
        # Format as Oracle expects: DD-MON-YYYY or as string
        birth_date_str = birth_date.strftime("%d%b%Y").upper()

        # CPR
        cpr = generate_cpr(birth_date, gender_code, patient_id)

        # Address
        city, postal, kommune = random.choice(DANISH_CITIES)
        street = f"{random.choice(DANISH_STREETS)} {random.randint(1, 200)}"

        # Doctor
        doctor_name = random.choice(DOCTOR_NAMES)
        doctor_number = f"{random.randint(100000, 999999)}"

        # Status
        status_code, status_text = random.choice(PERSON_STATUS)

        # Address protection / discretion
        has_address_protection = 1 if random.random() < 0.02 else 0  # 2%
        wants_discretion = 1 if random.random() < 0.05 else 0  # 5%

        # Distrikt
        distrikt_code, distrikt_text = random.choice(DISTRIKT_CODES)
        social_code, social_text = random.choice(SOCIAL_DISTRIKT_CODES)

        # Remarks (mostly empty)
        remarks = None
        if random.random() < 0.1:  # 10% have remarks
            remarks = f"Bemærkning for patient {patient_id}"

        # OPDATERET_TID (usually same as or slightly before MODTIDSPUNKT)
        opdateret_tid = modtidspunkt - timedelta(seconds=random.randint(0, 3600))

        # Build tuple matching column order
        record = (
            patient_id,  # PATIENT_ID
            cpr,  # CPR
            first_name,  # FORNAVNE
            last_name,  # EFTERNAVN
            street,  # VEJ
            postal,  # POSTNR
            city,  # STEDNAVN
            kommune,  # KOMMUNEKODE
            gender_code,  # KOEN_KODE
            gender_text,  # KOEN_KODETEKST
            birth_date_str,  # FOEDSELSDATO (as string for CLOB handling)
            doctor_name,  # LAEGENAVN
            doctor_number,  # LAEGENUMMER
            status_code,  # PERSON_STATUSKODE
            status_text,  # PERSON_STATUSKODETEKST
            has_address_protection,  # HAR_ADRESSEBESKYTTELSE
            wants_discretion,  # OENSKER_DISKRETION
            remarks,  # BEMAERKNINGER
            distrikt_code,  # PERSON_DISTRIKTKODE
            distrikt_text,  # PERSON_DISTRIKTKODETEKST
            social_code,  # PERSON_SOCIALDISTRIKTKODE
            social_text,  # PERSON_SOCIALDISTRIKTKODETEKST
            opdateret_tid,  # OPDATERET_TID
            modtidspunkt,  # MODTIDSPUNKT
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


def ensure_table_exists(connection) -> bool:
    """
    Check if the PATIENT_OPLYSNINGER_COLUMNA table exists.
    If not, create it with the expected schema.

    Returns True if table exists or was created successfully.
    """
    cursor = connection.cursor()

    try:
        # Check if table exists
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM all_tables
            WHERE owner = 'IFDB'
            AND table_name = 'PATIENT_OPLYSNINGER_COLUMNA'
        """
        )
        count = cursor.fetchone()[0]

        if count > 0:
            logger.info("Table IFDB.PATIENT_OPLYSNINGER_COLUMNA already exists")
            return True

        # Check if IFDB schema exists
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM all_users
            WHERE username = 'IFDB'
        """
        )
        schema_exists = cursor.fetchone()[0] > 0

        if not schema_exists:
            logger.info("Creating IFDB schema...")
            cursor.execute("CREATE USER IFDB IDENTIFIED BY ifdb_password")
            cursor.execute("GRANT CREATE TABLE TO IFDB")
            cursor.execute("GRANT UNLIMITED TABLESPACE TO IFDB")
            connection.commit()

        # Create the table
        logger.info("Creating IFDB.PATIENT_OPLYSNINGER_COLUMNA table...")
        create_sql = """
            CREATE TABLE IFDB.PATIENT_OPLYSNINGER_COLUMNA (
                PATIENT_ID NUMBER(19) PRIMARY KEY,
                CPR VARCHAR2(20),
                FORNAVNE VARCHAR2(200),
                EFTERNAVN VARCHAR2(200),
                VEJ VARCHAR2(500),
                POSTNR VARCHAR2(20),
                STEDNAVN VARCHAR2(200),
                KOMMUNEKODE VARCHAR2(10),
                KOEN_KODE VARCHAR2(5),
                KOEN_KODETEKST VARCHAR2(50),
                FOEDSELSDATO VARCHAR2(50),
                LAEGENAVN VARCHAR2(200),
                LAEGENUMMER VARCHAR2(50),
                PERSON_STATUSKODE VARCHAR2(10),
                PERSON_STATUSKODETEKST VARCHAR2(100),
                HAR_ADRESSEBESKYTTELSE NUMBER(1),
                OENSKER_DISKRETION NUMBER(1),
                BEMAERKNINGER VARCHAR2(4000),
                PERSON_DISTRIKTKODE VARCHAR2(20),
                PERSON_DISTRIKTKODETEKST VARCHAR2(200),
                PERSON_SOCIALDISTRIKTKODE VARCHAR2(20),
                PERSON_SOCIALDISTRIKTKODETEKST VARCHAR2(200),
                OPDATERET_TID TIMESTAMP,
                MODTIDSPUNKT TIMESTAMP
            )
        """
        cursor.execute(create_sql)

        # Create index on partition column
        cursor.execute(
            """
            CREATE INDEX IFDB.IDX_PATIENT_MODTIDSPUNKT
            ON IFDB.PATIENT_OPLYSNINGER_COLUMNA(MODTIDSPUNKT)
        """
        )

        connection.commit()
        logger.info("Table created successfully")
        return True

    except oracledb.DatabaseError as e:
        logger.error(f"Database error: {e}")
        return False
    finally:
        cursor.close()


def get_current_row_count(connection) -> int:
    """Get current number of rows in the table."""
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA")
        return cursor.fetchone()[0]
    except oracledb.DatabaseError:
        return 0
    finally:
        cursor.close()


def truncate_table(connection):
    """Truncate the patient table to start fresh."""
    cursor = connection.cursor()
    try:
        logger.info("Truncating IFDB.PATIENT_OPLYSNINGER_COLUMNA...")
        cursor.execute("TRUNCATE TABLE IFDB.PATIENT_OPLYSNINGER_COLUMNA")
        connection.commit()
        logger.info("Table truncated")
    except oracledb.DatabaseError as e:
        logger.error(f"Error truncating table: {e}")
        raise
    finally:
        cursor.close()


def insert_batch(connection, cursor, batch: list[tuple]):
    """Insert a batch of patient records."""
    insert_sql = """
        INSERT INTO IFDB.PATIENT_OPLYSNINGER_COLUMNA (
            PATIENT_ID, CPR, FORNAVNE, EFTERNAVN, VEJ, POSTNR, STEDNAVN,
            KOMMUNEKODE, KOEN_KODE, KOEN_KODETEKST, FOEDSELSDATO,
            LAEGENAVN, LAEGENUMMER, PERSON_STATUSKODE, PERSON_STATUSKODETEKST,
            HAR_ADRESSEBESKYTTELSE, OENSKER_DISKRETION, BEMAERKNINGER,
            PERSON_DISTRIKTKODE, PERSON_DISTRIKTKODETEKST,
            PERSON_SOCIALDISTRIKTKODE, PERSON_SOCIALDISTRIKTKODETEKST,
            OPDATERET_TID, MODTIDSPUNKT
        ) VALUES (
            :1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12,
            :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24
        )
    """

    cursor.executemany(insert_sql, batch)


def seed_performance_data(total_rows: int, force: bool = False):
    """
    Main function to seed performance test data.

    Args:
        total_rows: Total number of patient records to generate
        force: If True, truncate existing data and re-seed
    """
    logger.info("=" * 70)
    logger.info("IFDB Performance Data Seeder")
    logger.info("=" * 70)
    logger.info(f"Target rows: {total_rows:,}")

    # Generate partition list
    partitions = generate_partition_dates(START_PARTITION, END_PARTITION)
    rows_per_partition = total_rows // len(partitions)

    logger.info(
        f"Partitions: {len(partitions)} months ({START_PARTITION.strftime('%Y-%m')} to {END_PARTITION.strftime('%Y-%m')})"
    )
    logger.info(f"Rows per partition: ~{rows_per_partition:,}")
    logger.info("=" * 70)

    # Connect to Oracle
    try:
        connection = get_oracle_connection()
        logger.info("Connected to Oracle successfully")
    except Exception as e:
        logger.error(f"Failed to connect to Oracle: {e}")
        logger.error("Make sure the oracle container is running and healthy")
        sys.exit(1)

    try:
        # Ensure table exists
        if not ensure_table_exists(connection):
            logger.error("Failed to ensure table exists")
            sys.exit(1)

        # Check current row count
        current_count = get_current_row_count(connection)
        logger.info(f"Current row count: {current_count:,}")

        if current_count >= total_rows and not force:
            logger.info(f"Table already has {current_count:,} rows (>= target {total_rows:,})")
            logger.info("Use --force to re-seed data")
            return

        if current_count > 0:
            if force:
                truncate_table(connection)
                current_count = 0
            else:
                logger.info(f"Continuing from {current_count:,} rows...")

        # Seed data in batches
        cursor = connection.cursor()
        start_time = time.time()
        rows_inserted = current_count
        last_commit = rows_inserted

        logger.info("Starting data generation and insertion...")
        logger.info(f"Batch size: {BATCH_SIZE:,}, Commit interval: {COMMIT_INTERVAL:,}")

        try:
            while rows_inserted < total_rows:
                # Generate batch
                batch_start = rows_inserted
                remaining = total_rows - rows_inserted
                current_batch_size = min(BATCH_SIZE, remaining)

                batch = generate_patient_batch(
                    start_id=batch_start + 1,  # PATIENT_ID starts at 1
                    batch_size=current_batch_size,
                    partitions=partitions,
                    rows_per_partition=rows_per_partition,
                )

                # Insert batch
                insert_batch(connection, cursor, batch)
                rows_inserted += current_batch_size

                # Commit periodically
                if rows_inserted - last_commit >= COMMIT_INTERVAL:
                    connection.commit()
                    last_commit = rows_inserted

                    elapsed = time.time() - start_time
                    rate = rows_inserted / elapsed if elapsed > 0 else 0
                    eta = (total_rows - rows_inserted) / rate if rate > 0 else 0

                    logger.info(
                        f"Progress: {rows_inserted:,}/{total_rows:,} "
                        f"({100 * rows_inserted / total_rows:.1f}%) | "
                        f"Rate: {rate:.0f} rows/sec | "
                        f"ETA: {eta / 60:.1f} min"
                    )

            # Final commit
            connection.commit()

        finally:
            cursor.close()

        # Report completion
        elapsed = time.time() - start_time
        final_count = get_current_row_count(connection)

        logger.info("=" * 70)
        logger.info("SEEDING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"Total rows inserted: {final_count:,}")
        logger.info(f"Time elapsed: {elapsed / 60:.1f} minutes")
        logger.info(f"Average rate: {final_count / elapsed:.0f} rows/second")
        logger.info("=" * 70)

        # Show partition distribution sample
        cursor = connection.cursor()
        try:
            cursor.execute(
                """
                SELECT
                    TO_CHAR(MODTIDSPUNKT, 'YYYY-MM') as partition_month,
                    COUNT(*) as row_count
                FROM IFDB.PATIENT_OPLYSNINGER_COLUMNA
                GROUP BY TO_CHAR(MODTIDSPUNKT, 'YYYY-MM')
                ORDER BY partition_month
            """
            )

            logger.info("\nPartition Distribution (sample):")
            rows = cursor.fetchall()
            for i, (month, count) in enumerate(rows):
                if i < 3 or i >= len(rows) - 3:
                    logger.info(f"  {month}: {count:,} rows")
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
    parser = argparse.ArgumentParser(description="Seed IFDB Oracle with performance test data")
    parser.add_argument(
        "--rows",
        type=int,
        default=DEFAULT_TOTAL_ROWS,
        help=f"Total number of rows to generate (default: {DEFAULT_TOTAL_ROWS:,})",
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

    # Override config if provided
    if args.host:
        ORACLE_CONFIG["host"] = args.host
    if args.port:
        ORACLE_CONFIG["port"] = args.port

    seed_performance_data(args.rows, args.force)


if __name__ == "__main__":
    main()
