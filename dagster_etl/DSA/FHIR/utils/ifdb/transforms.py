# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB transform functions for converting Oracle IFDB data to FHIR resources.

These transforms use the YAML model definitions from fhir.ifdb.yml to dynamically
build SQL queries and transform rows into FHIR resources.

Keep this module to IFDB-wide source normalization and shared IFDB transforms.
Helpers that only make sense inside one asset pipeline should stay with that
asset or move to a narrowly named module such as ``xml_aktivitetsresultat.py``.
"""

from datetime import datetime
from typing import Any

import dagster as dg
import oracledb
from core.resources import IFDBOracleResource
from DSA.FHIR.utils.fhir_utils import fhir_date, fhir_datetime
from DSA.FHIR.utils.ifdb.yaml_loader import (
    get_adapter_definition,
    get_canonical_definition,
    get_column_mappings,
    is_derived_column,
    parse_column_from_path,
)
from DSA.FHIR.utils.status_mapping import map_encounter_status
from fhir.resources.R4B.address import Address
from fhir.resources.R4B.codeableconcept import CodeableConcept
from fhir.resources.R4B.coding import Coding
from fhir.resources.R4B.encounter import Encounter
from fhir.resources.R4B.extension import Extension
from fhir.resources.R4B.humanname import HumanName
from fhir.resources.R4B.identifier import Identifier
from fhir.resources.R4B.observation import Observation
from fhir.resources.R4B.patient import Patient
from fhir.resources.R4B.period import Period
from fhir.resources.R4B.reference import Reference
from utils.fhir.fhir_extensions import (
    CPR_STATUS_DECEASED,
    CPR_STATUS_INACTIVE,
    EXTENSION_ADDRESS_PROTECTION,
    EXTENSION_DISTRICT,
    EXTENSION_MODTIDSPUNKT,
    EXTENSION_SOCIAL_DISTRICT,
)
from utils.fhir.identifier_systems import (
    SYSTEM_CPR,
    SYSTEM_IFDB_EPISODE_OF_CARE_ID,
    SYSTEM_IFDB_OBJECT_ID,
    SYSTEM_IFDB_PATIENT_ID,
)
from utils.fhir.is_deleted_extension import build_is_deleted_extension


def normalize_oracle_id(value: Any) -> str | None:
    """Stringify an Oracle identifier without a stray ``.0`` suffix.

    Oracle NUMBER columns come back as Python ``float`` via ``oracledb``;
    plain ``str(1.0)`` yields ``"1.0"`` which breaks identifier-based
    FHIR reference resolution downstream.  Integer-valued floats are
    normalised to their ``int`` form, and string values with a trailing
    ``.0`` over a digit-only body are stripped likewise.
    """
    if value is None:
        return None
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    s = str(value)
    if s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        return s[:-2]
    return s


def is_true_flag(value: Any) -> bool:
    """Interpret IFDB source flag values robustly.

    Handles bool, int, float, and string representations including float-strings
    like "1.0" that arise when Oracle NUMBER(1) values are stored as NVARCHAR
    in the DWH archive.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int | float):
        return int(value) == 1
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in {"1", "true", "t", "yes", "y"}:
            return True
        try:
            return int(float(stripped)) == 1
        except (ValueError, OverflowError):
            return False
    return False


def source_deleted_to_int(value: Any) -> int:
    """Convert IFDB deleted/source removal flags to integer 0/1."""
    if value is None:
        return 0
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int | float):
        return 1 if int(value) != 0 else 0
    if isinstance(value, str):
        return 1 if value.strip().lower() in {"1", "true", "t", "yes", "y"} else 0
    return 0


# Identifier system for Danish general practitioner (ydernummer)
SYSTEM_DANISH_GP_NUMBER = "urn:oid:1.2.208.176.1.4"  # Danish GP registration number

# Column name mapping from YAML aliases to Oracle column names
# This handles the mismatch between DSI extraction (UPPERCASE Oracle) and DSA transformation (lowercase aliases)
PATIENT_COLUMN_MAPPING: dict[str, str] = {
    "patient_id": "PATIENT_ID",
    "identifier_cpr": "CPR",
    "name_given": "FORNAVNE",
    "name_family": "EFTERNAVN",
    "address_street": "VEJ",
    "address_postal": "POSTNR",
    "address_city": "STEDNAVN",
    "address_municipality": "KOMMUNEKODE",
    "gender_code": "KOEN_KODE",
    "gender_text": "KOEN_KODETEKST",
    "birth_date": "FOEDSELSDATO",
    "doctor_name": "LAEGENAVN",
    "doctor_number": "LAEGENUMMER",
    "person_status_code": "PERSON_STATUSKODE",
    "person_status_text": "PERSON_STATUSKODETEKST",
    "address_protection": "HAR_ADRESSEBESKYTTELSE",
    "discretion_flag": "OENSKER_DISKRETION",
    "remarks": "BEMAERKNINGER",
    "district_code": "PERSON_DISTRIKTKODE",
    "district_text": "PERSON_DISTRIKTKODETEKST",
    "social_district_code": "PERSON_SOCIALDISTRIKTKODE",
    "social_district_text": "PERSON_SOCIALDISTRIKTKODETEKST",
    "last_updated": "OPDATERET_TID",
    "is_deleted": "SLETTET",
    "modtidspunkt": "MODTIDSPUNKT",
}


def validate_partition_column_exists(
    ifdb_oracle: IFDBOracleResource,
    table_name: str,
    column_name: str,
) -> bool:
    """
    Validate that a partition column exists in the specified table.

    Args:
        ifdb_oracle: IFDB Oracle resource with database connection
        table_name: Fully qualified table name
            (e.g., "IFDB.PATIENT_OPLYSNINGER_COLUMNA")
        column_name: Column name to check (e.g., "MODTIDSPUNKT")

    Returns:
        True if column exists, False otherwise

    Raises:
        Exception if database query fails
    """
    # Parse schema and table from table_name
    parts = table_name.split(".")
    if len(parts) == 2:
        schema = parts[0]
        table = parts[1]
    else:
        # Default to IFDB schema if not specified
        schema = "IFDB"
        table = parts[0]

    # Query Oracle data dictionary to check if column exists
    # Use different bind variable names to avoid conflicts
    check_query = """
        SELECT COUNT(*)
        FROM ALL_TAB_COLUMNS
        WHERE OWNER = :schema_name
          AND TABLE_NAME = :table_name
          AND COLUMN_NAME = :column_name
    """

    try:
        with ifdb_oracle.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    check_query,
                    {
                        "schema_name": schema,
                        "table_name": table,
                        "column_name": column_name,
                    },
                )
                count = cursor.fetchone()[0]
                return count > 0
            finally:
                cursor.close()
    except Exception as e:
        # Re-raise with more context
        raise RuntimeError(
            f"Failed to validate partition column '{column_name}' in table '{table_name}': {e}"
        ) from e


def parse_clob_date(date_str: str) -> datetime | None:
    """
    Parse a CLOB date string to a datetime object (date-only, time set to midnight).

    Handles CLOB date formats:
    - "12DEC1955:00:00:00" (birth date format - extracts date part only)

    Args:
        date_str: Date string in CLOB format or standard format

    Returns:
        datetime object if parsing succeeds, None if not CLOB format

    Raises:
        ValueError: If CLOB format is detected but parsing fails
    """
    if not date_str:
        return None

    # Convert to string if not already
    date_str = str(date_str).strip()

    # Check if it matches CLOB format pattern (contains : and month names)
    if ":" in date_str and any(
        month in date_str.upper()
        for month in [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ]
    ):
        # CLOB format detected - must parse successfully or raise error
        try:
            # Extract date part before the colon (DDMONYYYY)
            date_part = date_str.split(":")[0]
            # Parse using format DDMONYYYY
            return datetime.strptime(date_part, "%d%b%Y")
        except (ValueError, IndexError) as e:
            # Format was detected as CLOB but parsing failed - raise error
            raise ValueError(f"Failed to parse CLOB date format '{date_str}': {e}") from e

    # Not CLOB format - return None (caller should try standard parsing)
    return None


def parse_clob_datetime(date_str: str) -> datetime | None:
    """
    Parse a CLOB datetime string to a datetime object (includes time components).

    Handles CLOB datetime formats:
    - "16MAY2025:11:39:05" (DDMONYYYY:HH24:MI:SS)
    - "20MAY2025:17:00:05.000000" (DDMONYYYY:HH24:MI:SS.FF6)

    Args:
        date_str: Datetime string in CLOB format or standard format

    Returns:
        datetime object if parsing succeeds, None if not CLOB format

    Raises:
        ValueError: If CLOB format is detected but parsing fails
    """
    if not date_str:
        return None

    # Convert to string if not already
    date_str = str(date_str).strip()

    # Check if it matches CLOB format pattern (contains : and month names)
    if ":" in date_str and any(
        month in date_str.upper()
        for month in [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ]
    ):
        # CLOB format detected - must parse successfully or raise error
        try:
            # Try format with microseconds first (DDMONYYYY:HH24:MI:SS.FF6)
            if "." in date_str:
                return datetime.strptime(date_str, "%d%b%Y:%H:%M:%S.%f")
            else:
                # Format without microseconds (DDMONYYYY:HH24:MI:SS)
                return datetime.strptime(date_str, "%d%b%Y:%H:%M:%S")
        except ValueError as e:
            # Format was detected as CLOB but parsing failed - raise error
            raise ValueError(f"Failed to parse CLOB datetime format '{date_str}': {e}") from e

    # Not CLOB format - return None (caller should try standard parsing)
    return None


def parse_clob_datetime_safe(value: Any) -> str | None:
    """Safely parse Oracle/CLOB/ISO datetime values to a FHIR datetime string."""
    if not value:
        return None
    try:
        if isinstance(value, str):
            parsed = parse_clob_datetime(value)
            if parsed:
                return fhir_datetime(parsed)
            try:
                return fhir_datetime(datetime.fromisoformat(value))
            except (ValueError, TypeError):
                return None
        return fhir_datetime(value)
    except Exception:
        return None


def build_select_query_for_canonical(
    canonical_name: str,
    partition_date: str,
    partition_column: str | None = None,
    ifdb_oracle: IFDBOracleResource | None = None,
    partition_type: str | None = None,
) -> str:
    """
    Build a SELECT query for a canonical resource based on YAML column mappings.

    Args:
        canonical_name: Name of the canonical resource (e.g., 'encounter_periode')
        partition_date: Partition date in YYYY-MM-DD format
        partition_column: Optional column name to filter by partition date
        ifdb_oracle: Optional IFDB Oracle resource. If provided and
            partition_column is specified, validates that the column exists
            in the table before building the WHERE clause.

    Returns:
        SQL SELECT query string

    Raises:
        ValueError: If partition_column is specified but does not exist
            in the table (when ifdb_oracle is provided)
    """
    canonical = get_canonical_definition(canonical_name)
    if not canonical:
        raise ValueError(f"Canonical resource '{canonical_name}' not found in YAML")

    adapter_name = canonical.get("sources", [])
    if not adapter_name or len(adapter_name) == 0:
        raise ValueError(f"No source adapter specified for canonical '{canonical_name}'")

    # Get the first source adapter
    adapter_name = adapter_name[0]
    adapter = get_adapter_definition(adapter_name)
    if not adapter:
        raise ValueError(f"Adapter '{adapter_name}' not found in YAML")

    source_tables = adapter.get("source_tables", [])
    if not source_tables:
        raise ValueError(f"No source tables specified for adapter '{adapter_name}'")

    # Use the first source table (most adapters have one)
    primary_table = source_tables[0]

    # Get column mappings
    columns = get_column_mappings(canonical_name)

    # Build SELECT clause
    select_parts = []
    for col_name, col_def in columns.items():
        if is_derived_column(col_def):
            continue  # Skip derived columns in SELECT

        col_from = col_def.get("from", "")
        if not col_from or col_from == "Derived":
            continue

        # Parse the column path (e.g., "IFDB.ENCOUNTER.OBJECT_ID")
        try:
            table_path, column_name = parse_column_from_path(col_from)
            # Check if it's from the primary table (match full path or table name)
            table_name_only = table_path.split(".")[-1] if "." in table_path else table_path
            primary_table_name = (
                primary_table.split(".")[-1] if "." in primary_table else primary_table
            )

            if table_path == primary_table or table_name_only == primary_table_name:
                # Use full table path for Oracle schema prefix
                select_parts.append(f"{table_path}.{column_name} AS {col_name}")
        except ValueError:
            # If parsing fails, use as-is (might be a literal or already correct)
            select_parts.append(f"{col_from} AS {col_name}")

    if not select_parts:
        raise ValueError(f"No selectable columns found for '{canonical_name}'")

    # Build WHERE clause for partition
    where_clause = ""
    if partition_column:
        # Validate partition column exists if connection is provided
        if ifdb_oracle is not None:
            column_exists = validate_partition_column_exists(
                ifdb_oracle=ifdb_oracle,
                table_name=primary_table,
                column_name=partition_column,
            )
            if not column_exists:
                raise ValueError(
                    f"Partition column '{partition_column}' does not exist "
                    f"in table '{primary_table}'. Cannot build partition filter. "
                    f"Please verify the column name is correct for canonical "
                    f"'{canonical_name}'."
                )

        # Fully qualify column with table name if not already qualified
        if "." not in partition_column:
            qualified_partition_col = f"{primary_table}.{partition_column}"
        else:
            qualified_partition_col = partition_column

        # Column types:
        # - MODTIDSPUNKT: DATE
        # - EPJI_MODIFICATION_DATETIME: DATE
        # - IFDB_MODIFICATION_DATETIME: TIMESTAMP(6)
        # - OPDATERET_TID: TIMESTAMP(6)

        # Check if partition_date is in YYYY-MM-DD or YYYY-MM format, or YYYY-MM-01 (monthly partition start)
        if partition_type == "monthly":
            # YYYY-MM format or YYYY-MM-01 format - get all dates in that month
            from datetime import datetime

            # YYYY-MM-01 format
            year, month = map(int, partition_date.split("-")[:2])

            start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
            if month == 12:
                end_date = datetime(year + 1, 1, 1).strftime("%Y-%m-%d")
            else:
                end_date = datetime(year, month + 1, 1).strftime("%Y-%m-%d")

            # Convert column to DATE for comparison based on its type
            if partition_column in ["MODTIDSPUNKT", "EPJI_MODIFICATION_DATETIME"]:
                # These are DATE types - use TRUNC directly
                partition_col_converted = f"TRUNC({qualified_partition_col})"
            elif partition_column in ["IFDB_MODIFICATION_DATETIME", "OPDATERET_TID"]:
                # These are TIMESTAMP(6) types - cast to DATE and truncate
                partition_col_converted = f"TRUNC(CAST({qualified_partition_col} AS DATE))"
            else:
                # Default: assume TIMESTAMP type
                partition_col_converted = f"TRUNC(CAST({qualified_partition_col} AS DATE))"

            where_clause = f"WHERE {partition_col_converted} >= DATE '{start_date}' AND {partition_col_converted} < DATE '{end_date}'"
        else:
            # Daily partition - exact date match
            partition_date_formatted = partition_date

            # Convert column to DATE for comparison based on its type
            if partition_column in ["MODTIDSPUNKT", "EPJI_MODIFICATION_DATETIME"]:
                # These are DATE types - use TRUNC directly
                partition_col_converted = f"TRUNC({qualified_partition_col})"
            elif partition_column in ["IFDB_MODIFICATION_DATETIME", "OPDATERET_TID"]:
                # These are TIMESTAMP(6) types - cast to DATE and truncate
                partition_col_converted = f"TRUNC(CAST({qualified_partition_col} AS DATE))"
            else:
                # Default: assume TIMESTAMP type
                partition_col_converted = f"TRUNC(CAST({qualified_partition_col} AS DATE))"

            where_clause = f"WHERE {partition_col_converted} = DATE '{partition_date_formatted}'"

    # Build query
    query = f"""
        SELECT {", ".join(select_parts)}
        FROM {primary_table}
        {where_clause}
    """.strip()

    return query


def transform_adapter_ifdb_encounter_periode(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_date: str,
    partition_type: str | None = None,
) -> list[dict[str, Any]]:
    """
    Query IFDB.ENCOUNTER table for encounters modified on the partition date.

    Returns:
        List of dictionaries representing encounter rows
    """
    query = build_select_query_for_canonical(
        canonical_name="encounter_periode",
        partition_date=partition_date,
        partition_column="IFDB_MODIFICATION_DATETIME",
        ifdb_oracle=ifdb_oracle,
        partition_type=partition_type,
    )

    rows = []
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)

            # Get column names (Oracle cursor.description format is different)
            columns = [desc[0] for desc in cursor.description]

            # Fetch all rows as dictionaries, converting LOB objects to strings
            # while connection is still open
            for row in cursor.fetchall():
                row_dict = {}
                for col_name, value in zip(columns, row, strict=False):
                    # Convert LOB objects (CLOB, BLOB) to strings/bytes
                    # while connection is open
                    if value is not None and isinstance(value, oracledb.LOB):
                        try:
                            # Read the LOB content as string (for CLOB) or bytes
                            # oracledb LOB objects have read() method returning str/bytes
                            row_dict[col_name] = value.read()
                        except Exception as e:
                            context.log.warning(
                                f"Failed to read LOB value for column {col_name}: {e}"
                            )
                            row_dict[col_name] = None
                    else:
                        # Not a LOB, use value as-is
                        row_dict[col_name] = value
                rows.append(row_dict)
        finally:
            cursor.close()

    context.log.info(f"Fetched {len(rows)} encounter rows for partition {partition_date}")
    return rows


def transform_fhir_encounter_periode(
    context: dg.AssetExecutionContext,
    encounter_rows: list[dict[str, Any]],
) -> list[Encounter]:
    """
    Transform IFDB encounter rows to FHIR Encounter resources using YAML mappings.

    Args:
        context: Dagster asset execution context
        encounter_rows: List of dictionaries from IFDB.ENCOUNTER table

    Returns:
        List of FHIR Encounter resources
    """
    encounters: list[Encounter] = []

    def get_column_value(row: dict[str, Any], col_name: str) -> Any:
        """Get column value from row, handling both direct and mapped names."""
        return row.get(col_name)

    def normalize_code(text: str | None) -> str:
        """Normalize code text to code value."""
        if not text:
            return "unknown"
        return "".join(ch for ch in text.lower() if ch.isalnum() or ch in ("-", "_")) or "unknown"

    for row in encounter_rows:
        # Build identifiers
        encounter_id = get_column_value(row, "encounter_id")
        identifiers = []
        if encounter_id:
            identifiers.append(Identifier(system=SYSTEM_IFDB_OBJECT_ID, value=str(encounter_id)))

        # Build subject (Patient) reference
        patient_id = get_column_value(row, "patient_id")
        subject = None
        if patient_id:
            subject = Reference(
                identifier=Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=str(patient_id))
            )

        # Build episodeOfCare reference
        episode_id = get_column_value(row, "episode_id")
        episode_ref = None
        if episode_id:
            episode_ref = Reference(
                identifier=Identifier(system=SYSTEM_IFDB_EPISODE_OF_CARE_ID, value=str(episode_id))
            )

        # Build period with CLOB date parsing
        period_start = get_column_value(row, "period_start")
        period_end = get_column_value(row, "period_end")
        period = None
        if period_start or period_end:
            # Parse period_start
            period_start_fhir = None
            if period_start:
                try:
                    # Try CLOB datetime format first
                    parsed_dt = parse_clob_datetime(period_start)
                    if parsed_dt:
                        # Successfully parsed CLOB format, convert to FHIR
                        period_start_fhir = fhir_datetime(parsed_dt)
                    else:
                        # Not CLOB format, try standard parsing
                        period_start_fhir = fhir_datetime(period_start)
                except Exception as e:
                    raise ValueError(f"Failed to parse period_start '{period_start}': {e}") from e

            # Parse period_end
            period_end_fhir = None
            if period_end:
                try:
                    # Try CLOB datetime format first
                    parsed_dt = parse_clob_datetime(period_end)
                    if parsed_dt:
                        # Successfully parsed CLOB format, convert to FHIR
                        period_end_fhir = fhir_datetime(parsed_dt)
                    else:
                        # Not CLOB format, try standard parsing
                        period_end_fhir = fhir_datetime(period_end)
                except Exception as e:
                    raise ValueError(f"Failed to parse period_end '{period_end}': {e}") from e

            period = Period(
                start=period_start_fhir,
                end=period_end_fhir,
            )

        # Build status
        status_text = get_column_value(row, "status_text")
        status = map_encounter_status(status_text)

        # Build class (FHIR Encounter requires class field)
        class_text = get_column_value(row, "class_text")
        if class_text:
            encounter_class = Coding(
                system="urn:enversion:encounter-class",
                code=normalize_code(class_text),
                display=class_text,
            )
        else:
            # Default class when missing (using "unknown" as a safe default)
            encounter_class = Coding(
                system="urn:enversion:encounter-class",
                code="unknown",
                display="Unknown",
            )

        # Create Encounter resource
        encounter = Encounter(
            identifier=identifiers if identifiers else None,
            status=status,
            subject=subject,
            episodeOfCare=[episode_ref] if episode_ref else None,
            period=period,
            **{"class": encounter_class},
        )

        encounters.append(encounter)

    context.log.info(f"Transformed {len(encounters)} encounters to FHIR resources")
    return encounters


def transform_adapter_ifdb_patient(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_date: str,
    partition_type: str,
) -> list[dict[str, Any]]:
    """
    Query IFDB.PATIENT_OPLYSNINGER_COLUMNA table for patients modified on
    the partition date.

    Returns:
        List of dictionaries representing patient rows
    """
    # Use MODTIDSPUNKT for partitioning (this column is used for filtering
    # but not in YAML columns). The YAML has last_updated from OPDATERET_TID,
    # but MODTIDSPUNKT is the actual partition column
    query = build_select_query_for_canonical(
        canonical_name="patient_ifdb",
        partition_date=partition_date,
        partition_column="MODTIDSPUNKT",
        ifdb_oracle=ifdb_oracle,
        partition_type=partition_type,
    )

    rows = []
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)

            # Get column names (Oracle cursor.description format)
            columns = [desc[0] for desc in cursor.description]

            # Fetch all rows as dictionaries, converting LOB objects to strings
            # while connection is still open
            for row in cursor.fetchall():
                row_dict = {}
                for col_name, value in zip(columns, row, strict=False):
                    # Convert LOB objects (CLOB, BLOB) to strings/bytes
                    # while connection is open
                    if value is not None and isinstance(value, oracledb.LOB):
                        try:
                            # Read the LOB content as string (for CLOB) or bytes
                            # oracledb LOB objects have read() method returning str/bytes
                            row_dict[col_name] = value.read()
                        except Exception as e:
                            context.log.warning(
                                f"Failed to read LOB value for column {col_name}: {e}"
                            )
                            row_dict[col_name] = None
                    else:
                        # Not a LOB, use value as-is
                        row_dict[col_name] = value
                rows.append(row_dict)
        finally:
            cursor.close()

    context.log.info(f"Fetched {len(rows)} patient rows for partition {partition_date}")
    return rows


def transform_fhir_patient_ifdb(
    context: dg.AssetExecutionContext,
    patient_rows: list[dict[str, Any]],
) -> list[Patient]:
    """
    Transform IFDB patient rows to FHIR Patient resources using YAML mappings.

    Args:
        context: Dagster asset execution context
        patient_rows: List of dictionaries from IFDB.PATIENT_OPLYSNINGER_COLUMNA table

    Returns:
        List of FHIR Patient resources with complete demographic information including:
        - Identifiers (CPR, patient_id)
        - Name (given, family)
        - Gender
        - Birth date
        - Address (street, postal, city, municipality/district)
        - Deceased status (based on CPR person status code)
        - General practitioner reference
        - Danish-specific extensions (district codes, address protection)
    """
    patients: list[Patient] = []

    def get_column_value(row: dict[str, Any], col_name: str) -> Any:
        """Get column value from row, handling both aliased and Oracle column names.

        The DSI extraction layer returns UPPERCASE Oracle column names,
        while test fixtures may use lowercase aliased names.
        This function tries multiple variants to ensure compatibility.

        Args:
            row: Dictionary containing patient data
            col_name: The aliased column name (e.g., "doctor_name")

        Returns:
            The column value or None if not found
        """
        # Try aliased name first (as used in test fixtures)
        value = row.get(col_name)
        if value is not None:
            return value

        # Try uppercase version of alias
        value = row.get(col_name.upper())
        if value is not None:
            return value

        # Try mapped Oracle column name (from PATIENT_COLUMN_MAPPING)
        oracle_name = PATIENT_COLUMN_MAPPING.get(col_name)
        if oracle_name:
            value = row.get(oracle_name)
            if value is not None:
                return value

        return None

    def map_gender(code: str | None, text: str | None) -> str:
        """Map gender code/text to FHIR gender value."""
        if code is not None:
            normalized = str(code).strip().lower()
            if normalized in {"1", "m", "male"}:
                return "male"
            if normalized in {"2", "k", "f", "female"}:
                return "female"
        if text:
            t = text.strip().lower()
            if "mand" in t or t == "male":
                return "male"
            if "kvinde" in t or t == "female":
                return "female"
        return "unknown"

    def is_deceased(status_code: str | None) -> bool:
        """Check if person status code indicates deceased."""
        if status_code is None:
            return False
        return str(status_code).strip() in CPR_STATUS_DECEASED

    def is_active(status_code: str | None) -> bool:
        """Check if person status code indicates active (not annulled/cancelled)."""
        if status_code is None:
            return True  # Default to active if no status
        code = str(status_code).strip()
        # Inactive if annulled (80) or deceased (90)
        return code not in CPR_STATUS_INACTIVE and code not in CPR_STATUS_DECEASED

    def build_general_practitioner_reference(
        doctor_name: str | None,
        doctor_number: str | None,
    ) -> Reference | None:
        """Build a reference to the patient's general practitioner.

        Uses Danish GP registration number (ydernummer) as identifier.
        """
        if not doctor_number and not doctor_name:
            return None

        # Create identifier reference using GP number (ydernummer)
        if doctor_number:
            return Reference(
                type="Practitioner",
                identifier=Identifier(
                    system=SYSTEM_DANISH_GP_NUMBER,
                    value=str(doctor_number),
                ),
                display=doctor_name if doctor_name else None,
            )
        # If only name is available, create display-only reference
        return Reference(
            type="Practitioner",
            display=doctor_name,
        )

    for row in patient_rows:
        # Extract core identifiers
        patient_id = get_column_value(row, "patient_id")
        cpr = get_column_value(row, "identifier_cpr")

        # Build identifiers
        identifiers = []
        if cpr:
            identifiers.append(Identifier(system=SYSTEM_CPR, value=str(cpr)))
        if patient_id:
            identifiers.append(Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=str(patient_id)))

        # Build name - only create if we have at least one name component
        given_names_str = get_column_value(row, "name_given")
        family_name = get_column_value(row, "name_family")
        given_names = (
            [n for n in (given_names_str or "").split(" ") if n] if given_names_str else None
        )

        # Only create HumanName if at least one field has a value
        name = None
        if family_name or given_names:
            name = HumanName(
                family=family_name if family_name else None,
                given=given_names if given_names else None,
            )

        # Build gender
        gender_code = get_column_value(row, "gender_code")
        gender_text = get_column_value(row, "gender_text")
        gender = map_gender(gender_code, gender_text)

        # Get address protection flag
        address_protection = get_column_value(row, "address_protection")
        # DFM archive rows store source flags as NVARCHAR ("0"/"1"), where
        # bool("0") is True. Use robust flag parsing so only true-ish values
        # suppress address details.
        has_address_protection = is_true_flag(address_protection)

        # Build address (omit details if address protection is enabled)
        address = None
        if not has_address_protection:
            street = get_column_value(row, "address_street")
            postal = get_column_value(row, "address_postal")
            city = get_column_value(row, "address_city")
            municipality = get_column_value(row, "address_municipality")

            if any([street, postal, city, municipality]):
                # New patients get a default period start of 1900-01-01
                # (will be updated during merge if patient already exists)
                address = Address(
                    line=[v for v in [street] if v] if street else None,
                    postalCode=str(postal) if postal else None,
                    city=city if city else None,
                    district=str(municipality) if municipality else None,
                    period=Period(start="1900-01-01"),
                )

        # Build birth date
        birth_date = get_column_value(row, "birth_date")

        # Convert birth_date to FHIR format, handling CLOB date formats
        fhir_birth_date = None
        if birth_date:
            try:
                # If it's still a LOB (shouldn't happen after our fix, but be safe)
                if isinstance(birth_date, oracledb.LOB):
                    birth_date = birth_date.read()

                # Try parsing as CLOB format first
                parsed_dt = parse_clob_date(birth_date)
                if parsed_dt:
                    fhir_birth_date = fhir_date(parsed_dt)
                elif isinstance(birth_date, datetime):
                    fhir_birth_date = fhir_date(birth_date)
                else:
                    # String from MSSQL archive — may be "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DD"
                    date_only = str(birth_date).strip().split(" ")[0].split("T")[0]
                    fhir_birth_date = fhir_date(date_only)
            except Exception as e:
                context.log.warning(
                    f"Failed to parse birth_date '{birth_date}': {e}. "
                    "Skipping birth_date for this patient."
                )

        # Check person status for deceased and active flags
        person_status_code = get_column_value(row, "person_status_code")
        deceased = is_deceased(person_status_code)
        active = is_active(person_status_code)

        # Build general practitioner reference
        doctor_name = get_column_value(row, "doctor_name")
        doctor_number = get_column_value(row, "doctor_number")
        gp_reference = build_general_practitioner_reference(doctor_name, doctor_number)

        # Build extensions
        extensions = []

        # MODTIDSPUNKT extension (source receipt timestamp, per-row from IFDB)
        raw_modtidspunkt = get_column_value(row, "modtidspunkt")
        modtidspunkt_fhir = parse_clob_datetime_safe(raw_modtidspunkt)
        if modtidspunkt_fhir:
            extensions.append(
                Extension(url=EXTENSION_MODTIDSPUNKT, valueDateTime=modtidspunkt_fhir)
            )

        # Address protection extension
        if has_address_protection:
            extensions.append(Extension(url=EXTENSION_ADDRESS_PROTECTION, valueBoolean=True))

        # District extension (person district)
        district_code = get_column_value(row, "district_code")
        district_text = get_column_value(row, "district_text")
        if district_code or district_text:
            district_ext = Extension(url=EXTENSION_DISTRICT)
            if district_code:
                district_ext.extension = district_ext.extension or []
                district_ext.extension.append(Extension(url="code", valueString=str(district_code)))
            if district_text:
                district_ext.extension = district_ext.extension or []
                district_ext.extension.append(Extension(url="text", valueString=str(district_text)))
            extensions.append(district_ext)

        # Social district extension
        social_district_code = get_column_value(row, "social_district_code")
        social_district_text = get_column_value(row, "social_district_text")
        if social_district_code or social_district_text:
            social_ext = Extension(url=EXTENSION_SOCIAL_DISTRICT)
            if social_district_code:
                social_ext.extension = social_ext.extension or []
                social_ext.extension.append(
                    Extension(url="code", valueString=str(social_district_code))
                )
            if social_district_text:
                social_ext.extension = social_ext.extension or []
                social_ext.extension.append(
                    Extension(url="text", valueString=str(social_district_text))
                )
            extensions.append(social_ext)

        # Source deletion extension (flag + IFDB column origin); always emit for consistent filtering
        is_deleted = get_column_value(row, "is_deleted")
        deleted_value = int(is_true_flag(is_deleted))
        extensions.append(build_is_deleted_extension(deleted_value))

        # Create Patient resource
        patient = Patient(
            identifier=identifiers if identifiers else None,
            name=[name] if name else None,
            gender=gender,
            birthDate=fhir_birth_date,
            address=[address] if address else None,
            active=active,
            deceasedBoolean=deceased if deceased else None,
            generalPractitioner=[gp_reference] if gp_reference else None,
            extension=extensions if extensions else None,
        )

        patients.append(patient)

    context.log.info(f"Transformed {len(patients)} patients to FHIR resources")
    return patients


def transform_adapter_ifdb_observation(
    context: dg.AssetExecutionContext,
    ifdb_oracle: IFDBOracleResource,
    partition_date: str,
    partition_type: str,
) -> list[dict[str, Any]]:
    """
    Query IFDB Oracle database for observation data (AKTIVITETSRESULTAT table).

    Args:
        context: Dagster asset execution context
        ifdb_oracle: IFDB Oracle resource
        partition_date: Partition date in YYYY-MM-DD format

    Returns:
        List of dictionaries containing observation data from IFDB
    """
    from DSA.FHIR.utils.ifdb.transforms import (
        build_select_query_for_canonical,
    )

    # Build query using canonical model "observation_aktivitet"
    query = build_select_query_for_canonical(
        canonical_name="observation_aktivitet",
        partition_date=partition_date,
        partition_column="OPDATERET_TID",
        ifdb_oracle=ifdb_oracle,
        partition_type=partition_type,
    )

    rows = []
    with ifdb_oracle.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)

            # Get column names
            columns = [desc[0] for desc in cursor.description]

            # Fetch all rows as dictionaries, converting LOB objects to strings
            for row in cursor.fetchall():
                row_dict = {}
                for col_name, value in zip(columns, row, strict=False):
                    # Convert LOB objects (CLOB, BLOB) to strings/bytes
                    if value is not None and isinstance(value, oracledb.LOB):
                        try:
                            row_dict[col_name] = value.read()
                        except Exception as e:
                            context.log.warning(
                                f"Failed to read LOB value for column {col_name}: {e}"
                            )
                            row_dict[col_name] = None
                    else:
                        row_dict[col_name] = value
                rows.append(row_dict)
        finally:
            cursor.close()

    context.log.info(
        f"Queried {len(rows)} observation rows from IFDB for partition {partition_date}"
    )
    return rows


def transform_fhir_observation_ifdb(
    context: dg.AssetExecutionContext,
    observation_rows: list[dict[str, Any]],
) -> list[Observation]:
    """
    Transform IFDB observation rows to FHIR Observation resources using YAML mappings.

    Args:
        context: Dagster asset execution context
        observation_rows: List of dictionaries from IFDB.AKTIVITETSRESULTAT table

    Returns:
        List of FHIR Observation resources
    """
    from DSA.FHIR.utils.ifdb.transforms import parse_clob_datetime
    from utils.fhir.fhir_extensions import EXTENSION_EPISODE_OF_CARE
    from utils.fhir.identifier_systems import (
        SYSTEM_IFDB_EPISODE_OF_CARE_ID,
        SYSTEM_IFDB_OBJECT_ID,
    )

    observations: list[Observation] = []

    def get_column_value(row: dict[str, Any], col_name: str) -> Any:
        """Get column value from row, handling both direct and mapped names.

        Oracle may return column names in uppercase, but we use lowercase aliases.
        Try both lowercase and uppercase versions.
        """
        # Try lowercase first (as specified in YAML aliases)
        value = row.get(col_name)
        if value is not None:
            return value
        # Try uppercase (Oracle default behavior)
        value = row.get(col_name.upper())
        if value is not None:
            return value
        return None

    for row in observation_rows:
        # Extract core identifiers using YAML key names (same as query aliases)
        aktivitet_id = get_column_value(row, "aktivitet_id")
        patient_id = get_column_value(row, "patient_id")
        encounter_id = get_column_value(row, "encounter_id")
        episode_id = get_column_value(row, "episode_id")

        # Extract activity and result codes using YAML key names
        activity_type_code = get_column_value(row, "activity_type_code")
        activity_type_text = get_column_value(row, "activity_type_text")
        result_type_code = get_column_value(row, "result_type_code")
        result_type_text = get_column_value(row, "result_type_text")

        # Extract other fields using YAML key names
        title = get_column_value(row, "title")
        performed_datetime = get_column_value(row, "performed_datetime")
        draft_status = get_column_value(row, "draft_status")

        # Map draft status to Observation status
        status = "preliminary" if draft_status else "final"

        # Parse datetime fields with CLOB handling
        effective_datetime = None
        if performed_datetime:
            if isinstance(performed_datetime, str):
                parsed_dt = parse_clob_datetime(performed_datetime)
                effective_datetime = fhir_datetime(parsed_dt) if parsed_dt else None
            else:
                effective_datetime = fhir_datetime(performed_datetime)

        # Build code from activity type and result type
        codings = []
        if activity_type_code:
            codings.append(Coding(code=str(activity_type_code), display=activity_type_text))
        if result_type_code:
            codings.append(Coding(code=str(result_type_code), display=result_type_text))

        # Skip observation if no code information is available
        # FHIR requires the code field to be meaningful
        if not codings and not title:
            context.log.warning(
                f"Skipping observation {aktivitet_id}: no code information available "
                f"(activity_type_code: {activity_type_code}, "
                f"result_type_code: {result_type_code}, title: {title})"
            )
            continue

        # Create extension for episode of care reference
        extensions = []
        if episode_id:
            eoc_ref = Reference(
                type="EpisodeOfCare",
                identifier=Identifier(
                    system=SYSTEM_IFDB_EPISODE_OF_CARE_ID,
                    value=str(episode_id),
                ),
            )
            eoc_extension = Extension(url=EXTENSION_EPISODE_OF_CARE, valueReference=eoc_ref)
            extensions.append(eoc_extension)

        # Create the observation
        identifiers = []
        if aktivitet_id:
            identifiers.append(Identifier(system=SYSTEM_IFDB_OBJECT_ID, value=str(aktivitet_id)))

        obs = Observation(
            identifier=identifiers if identifiers else None,
            status=status,
            code=CodeableConcept(
                coding=codings if codings else None,
                text=title if title else None,
            ),
            subject=(
                Reference(
                    type="Patient",
                    identifier=Identifier(system=SYSTEM_IFDB_PATIENT_ID, value=str(patient_id)),
                )
                if patient_id
                else None
            ),
            encounter=(
                Reference(
                    type="Encounter",
                    identifier=Identifier(system=SYSTEM_IFDB_OBJECT_ID, value=str(encounter_id)),
                )
                if encounter_id
                else None
            ),
            issued=effective_datetime,
            extension=extensions if extensions else None,
        )
        observations.append(obs)

    return observations


def to_fhir_instant(value: datetime | str | None) -> str | None:
    """Coerce a datetime or ISO-8601 string to FHIR ``instant`` format.

    FHIR ``instant`` requires full date+time with timezone (``YYYY-MM-DDThh:mm:ss+zz:zz``).
    This helper ensures timezone is present and that the output always includes time
    components -- unlike ``fhir_datetime`` which allows date-only values.
    """
    if value is None:
        return None
    from datetime import UTC

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value.isoformat()
    s = str(value).strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.isoformat()
    except ValueError:
        if "T" not in s:
            s = f"{s}T00:00:00"
        if "+" not in s and "Z" not in s:
            s = f"{s}+00:00"
        return s


def build_performer_dict(row_dict: dict[str, Any]) -> list[dict]:
    """Build Observation/Procedure performer list with Practitioner and/or Organization refs.

    Shared across Observation and Procedure Aktivitetsresultat builders.
    """
    from utils.fhir.identifier_systems import SYSTEM_IFDB_STAFF, SYSTEM_SOR

    performers: list[dict] = []

    sor_code = row_dict.get("UDFOERT_ADMENHED_SORCODE")
    if sor_code and str(sor_code).strip():
        org_ref: dict[str, Any] = {"type": "Organization"}
        org_name = row_dict.get("UDFOERT_ADMENHED_NAVN")
        if org_name:
            org_ref["display"] = str(org_name)
        org_ref["identifier"] = {
            "system": SYSTEM_SOR,
            "value": str(sor_code).strip(),
        }
        performers.append(org_ref)

    pers_id = row_dict.get("UDFOERT_PERSID")
    pers_id_norm = normalize_oracle_id(pers_id)
    pers_navn = row_dict.get("UDFOERT_PERSNAVN")
    if pers_id_norm or pers_navn:
        pract_ref: dict[str, Any] = {"type": "Practitioner"}
        if pers_navn:
            pract_ref["display"] = str(pers_navn)
        if pers_id_norm:
            pract_ref["identifier"] = {
                "system": SYSTEM_IFDB_STAFF,
                "value": pers_id_norm,
            }
        performers.append(pract_ref)

    return performers


__all__ = [
    "build_performer_dict",
    "build_select_query_for_canonical",
    "is_true_flag",
    "normalize_oracle_id",
    "parse_clob_datetime",
    "parse_clob_datetime_safe",
    "source_deleted_to_int",
    "to_fhir_instant",
    "transform_adapter_ifdb_encounter_periode",
    "transform_fhir_encounter_periode",
    "transform_adapter_ifdb_patient",
    "transform_fhir_patient_ifdb",
    "transform_adapter_ifdb_observation",
    "transform_fhir_observation_ifdb",
]
