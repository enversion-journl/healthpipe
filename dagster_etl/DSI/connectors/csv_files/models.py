# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
CSV extraction models for DSI layer.

Provides CsvExtractionConfig — a dataclass satisfying the ExtractionConfig
protocol — and helper utilities for reading CSV files into row dictionaries.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import dagster as dg


@dataclass(frozen=True)
class CsvExtractionConfig:
    """Configuration for extracting data from a CSV file.

    Satisfies the ExtractionConfig protocol (description + partition_column).

    Attributes:
        file_path: Absolute or relative path to the CSV file.
        description: Human-readable description of the extraction.
        delimiter: Column delimiter character.
        encoding: File encoding.
        columns: Optional list of columns to select. If None, all columns are returned.
        partition_column: Column to use for partition filtering.
            Typically None for CSV files (full load).
    """

    file_path: str
    description: str = ""
    delimiter: str = ","
    encoding: str = "utf-8"
    columns: list[str] | None = None
    partition_column: str | None = None
    has_header: bool = True
    column_names: list[str] = field(default_factory=list)


def read_csv_as_dicts(
    config: CsvExtractionConfig,
    context: dg.AssetExecutionContext | None = None,
) -> list[dict[str, Any]]:
    """Read a CSV file and return rows as a list of dictionaries.

    Args:
        config: CSV extraction configuration.
        context: Optional Dagster context for logging.

    Returns:
        List of dictionaries where each dict represents a row with column names as keys.

    Raises:
        FileNotFoundError: If the CSV file does not exist.
        ValueError: If column_names is required but not provided.
    """
    path = Path(config.file_path)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")

    if context:
        context.log.info(f"Reading CSV file: {path}")

    rows: list[dict[str, Any]] = []

    with open(path, encoding=config.encoding, newline="") as f:
        if config.has_header:
            reader = csv.DictReader(f, delimiter=config.delimiter)
        else:
            if not config.column_names:
                raise ValueError("column_names must be provided when has_header=False")
            reader = csv.DictReader(f, fieldnames=config.column_names, delimiter=config.delimiter)

        for row in reader:
            if config.columns:
                row = {k: v for k, v in row.items() if k in config.columns}
            rows.append(row)

    if context:
        context.log.info(f"Read {len(rows)} rows from {path.name}")

    return rows
