# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
IFDB transform functions for converting Oracle IFDB data to FHIR resources.

Keep this module to IFDB-wide source normalization and shared IFDB transforms.
Helpers that only make sense inside one asset pipeline should stay with that
asset or move to a narrowly named module such as ``xml_aktivitetsresultat.py``.
"""

from datetime import datetime
from typing import Any

import dagster as dg
from DSA.FHIR.utils.fhir_utils import fhir_datetime


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


def parse_xml_compact_datetime_safe(value: Any) -> str | None:
    """Parse a RIS_YDELSE.XML compact datetime to a FHIR datetime string.

    The XML carries wall-clock timestamps as digit strings ``YYYYMMDDHHMM``
    (e.g. ``202605111423``), optionally with seconds (``YYYYMMDDHHMMSS``) or
    date-only (``YYYYMMDD``) — a format ``parse_clob_datetime`` does not handle.
    Falls back to the CLOB/ISO parser for any non-compact form.
    """
    if not value:
        return None
    s = str(value).strip()
    if s.isdigit() and len(s) in (8, 12, 14):
        for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
            try:
                return fhir_datetime(datetime.strptime(s, fmt))
            except ValueError:
                continue
        dg.get_dagster_logger().warning(
            f"Compact XML datetime {s!r} matched no %Y%m%d[%H%M[%S]] format; "
            "falling back to CLOB parser"
        )
    return parse_clob_datetime_safe(value)


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
    "is_true_flag",
    "normalize_oracle_id",
    "parse_clob_datetime",
    "parse_clob_datetime_safe",
    "source_deleted_to_int",
    "to_fhir_instant",
]
