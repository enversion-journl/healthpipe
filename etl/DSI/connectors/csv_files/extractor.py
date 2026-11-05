# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""CsvExtractor for CSV file extraction in the DSI layer.

Returns ExtractResult with list[dict[str, Any]] data.
Handles CSV-specific concerns: encoding, delimiters, column selection,
and optional partition filtering on a date column.
"""

from __future__ import annotations

import dagster as dg
from DSI.connectors.base import ExtractResult
from DSI.connectors.csv_files.models import CsvExtractionConfig, read_csv_as_dicts


class CsvExtractor:
    """Extracts data from CSV files.

    Returns ExtractResult with list[dict[str, Any]] data.
    Unlike SqlExtractor subclasses this is a standalone class since
    CSV files do not use database connections.
    """

    def __init__(self, *, context: dg.AssetExecutionContext, source_system: str = "csv"):
        self.context = context
        self.source_system = source_system

    def extract(self, config: CsvExtractionConfig) -> ExtractResult:
        """Read a CSV file and return an ExtractResult.

        Args:
            config: CSV extraction configuration.

        Returns:
            ExtractResult with row data, column info and row count.
        """
        rows = read_csv_as_dicts(config, self.context)

        columns = list(rows[0].keys()) if rows else (config.columns or [])
        columns_info = [(c, "str", None, None, None, None) for c in columns]

        self.context.log.info(
            f"[CSV EXTRACTION] Read {len(rows)} records from {config.file_path} "
            f"(source_system: {self.source_system})"
        )

        return ExtractResult(
            data=rows,
            columns_info=columns_info,
            column_names=columns,
            row_count=len(rows),
        )
