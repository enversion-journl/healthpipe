# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SqlExtractor base class and ExtractResult for the DSI extraction layer.

Provides:
- ExtractResult: Uniform result container for all extraction methods
- SqlExtractor: Abstract base class defining the extraction interface
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from core.extract import ExtractBatchingPolicy, RowBatchSource
from dagster import AssetExecutionContext


@dataclass
class ExtractResult:
    """Result of an extraction containing data and schema metadata."""

    data: list
    columns_info: list[tuple]
    column_names: list[str]
    row_count: int

    @property
    def is_empty(self) -> bool:
        return self.row_count == 0


class SqlExtractor(ABC):
    """Abstract base class for SQL-based data extractions.

    Provides a standardized interface for extracting data from source systems.
    Concrete implementations handle database-specific query syntax, data types,
    and connection semantics.
    """

    def __init__(
        self,
        source_conn,
        target_conn=None,
        *,
        context: AssetExecutionContext,
        source_system: str,
    ):
        self.source_conn = source_conn
        self.target_conn = target_conn
        self.context = context
        self.source_system = source_system

    @abstractmethod
    def extract(self, config, partition_key: str | None, partition_type: str) -> ExtractResult:
        """Extract rows from a source table with partition filtering."""

    def make_row_batch_source(
        self,
        config: Any,
        partition_key: str | None,
        partition_type: str,
        policy: ExtractBatchingPolicy,
    ) -> RowBatchSource:
        """Build a streaming row-batch source for asset-value materialization.

        Concrete extractors override this when they support source-side
        batching. The collect-all ``extract()`` API remains the legacy default.
        """
        raise NotImplementedError(
            f"{type(self).__name__}.make_row_batch_source() is not implemented. "
            "Override in subclass if batched asset-value extraction is supported."
        )

    def insert(self, config, extract_result: ExtractResult, partition_key: str | None) -> int:
        """Insert extracted data into the target table.

        Default implementation raises NotImplementedError.
        Override in subclasses that support insertion (MssqlExtractor).
        """
        raise NotImplementedError(
            f"{type(self).__name__}.insert() is not implemented. "
            "Override in subclass if insertion is supported."
        )

    def relation_extract(
        self, config, partition_key: str | None, partition_type: str
    ) -> ExtractResult:
        """Extract from a dependent table filtered via FK relationship to parent.

        Default implementation raises NotImplementedError.
        Override in subclasses that support relation extraction (MssqlExtractor).
        """
        raise NotImplementedError(
            f"{type(self).__name__}.relation_extract() is not implemented. "
            "Override in subclass if relation extraction is supported."
        )

    @abstractmethod
    def custom_extract(self, query: str) -> ExtractResult:
        """Execute an arbitrary SQL query and return results."""

    def check_consistency(self, config) -> bool:
        """Check if target data matches source (for full-load skip optimization).

        Default implementation returns False (always reload).
        """
        return False
