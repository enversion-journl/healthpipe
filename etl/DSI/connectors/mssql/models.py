# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

TABLE_TYPE_MASTER_DATA = "master_data"
TABLE_TYPE_TRANSACTIONAL = "transactional"
TABLE_TYPE_BRIDGE = "bridge"
TABLE_TYPE_DEPENDENT = "dependent"


@dataclass(frozen=True)
class TableConfig:
    """Configuration describing how a single source table is staged."""

    output_name: str
    source_schema: str
    source_table: str
    target_table: str
    description: str
    exclude_columns: Sequence[str] = ()
    custom_where_clause: str | None = None
    partition_column: str | None = None
    partition_columns: Sequence[str] = ()
    is_full_load: bool = False
    consistency_columns: Sequence[str] = ()
    table_type: str = TABLE_TYPE_TRANSACTIONAL
    parent_table: str | None = None
    is_string_date: bool = False
    # Config-driven FK mapping for dependent/bridge tables
    fk_column: str | None = None
    parent_partition_col: str | None = None
    parent_fk_column: str | None = None
    fk_cast: str | None = None
    fk_path: tuple[tuple[str, str, str], ...] = ()
    # Opt-in: >1 enables process-based parallel windowed extraction on full-load
    # runs for mssql-python sources (clamped to MAX_SOURCE_CONNECTIONS). 1 = serial.
    parallel_workers: int = 1
    # Per-table cap on the bulk-copy fetch chunk size (overrides the 10k global
    # ceiling) for tables that hit the mssql-python cursor.bulkcopy() wedge.
    bcp_max_chunk_size: int | None = None


@dataclass(frozen=True)
class TableLoadResult:
    """Result metadata returned after staging a table."""

    source_table: str
    target_table: str
    row_count: int

    def as_metadata(self) -> dict[str, Any]:
        return {
            "table_name": self.target_table,
            "source_table": self.source_table,
            "row_count": self.row_count,
        }
