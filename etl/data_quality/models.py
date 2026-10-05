# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared data models for data quality checks.

All check-type-specific configuration dataclasses live here so that
new checks only need to define a config and a query helper.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class GrainCheckConfig:
    """Configuration for one FHIR resource grain check across all four tiers.

    The same business identifier VALUE flows through each tier under a
    different column name.  This dataclass captures that mapping so the
    check logic stays generic.

    Tier path:
        IFDB Oracle  ->  FHIR Server (Aidbox PG)  ->  DSA MSSQL  ->  DSO MSSQL
    """

    resource_name: str
    """FHIR resource type, e.g. ``"Patient"``."""

    ifdb_table: str
    """Fully-qualified Oracle table, e.g. ``"IFDB.PATIENT_OPLYSNINGER_COLUMNA"``."""

    ifdb_grain_column: str
    """Column holding the grain key in Oracle, e.g. ``"PATIENT_ID"``."""

    fhir_pg_table: str
    """Aidbox PostgreSQL table (lowercase resource type), e.g. ``"patient"``."""

    identifier_system: str
    """FHIR identifier system URI used to store the grain value,
    e.g. ``"urn:source:ifdb:patient_id"``."""

    mssql_table: str
    """Target table in ``[fhir]`` schema on DSA / DSO MSSQL, e.g. ``"Patient"``."""

    mssql_grain_column: str
    """Column in the MSSQL flat table that holds the IFDB grain value,
    e.g. ``"ifdb_patient_id"``."""


@dataclass
class TierCounts:
    """Row / grain counts for a single tier."""

    total_rows: int
    distinct_grain_keys: int

    @property
    def duplicate_count(self) -> int:
        return self.total_rows - self.distinct_grain_keys


@dataclass(frozen=True)
class DeletedResourceCheckConfig:
    """Configuration for one resource type in the deleted-resources check."""

    ifdb_config: Any
    """``TableExtractionConfig`` from DSI (has ``.table``)."""

    resource_type: str
    identifier_system: str
    id_column: str
    asset_dep: str
    result_key: str
    enabled: bool = True
