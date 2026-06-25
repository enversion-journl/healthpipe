# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""OracleExtractor for IFDB Oracle extraction in the DSI layer.

Returns ExtractResult with list[dict[str, Any]] data from all extraction methods.
Handles Oracle-specific concerns: LOB objects, TRUNC/CAST date filtering,
TO_DATE for catchup windows.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

from core.extract import ExtractBatchingPolicy
from core.partitions import is_first_partition
from DSI.connectors.base import ExtractResult, SqlExtractor
from DSI.connectors.oracle.models import (
    TableExtractionConfig,
    build_partition_query,
    fetch_rows_as_dicts,
    iter_rows_as_dict_batches,
)


@dataclass
class OracleRowBatchSource:
    """Row-batch source backed by one executed Oracle cursor."""

    cursor: Any
    batch_size: int
    context: Any
    column_names: tuple[str, ...]
    _closed: bool = False

    def iter_batches(self) -> Iterator[list[dict[str, Any]]]:
        yield from iter_rows_as_dict_batches(self.cursor, self.batch_size, self.context)

    def close(self) -> None:
        if self._closed:
            return
        self.cursor.close()
        self._closed = True


class OracleExtractor(SqlExtractor):
    """Extracts data from Oracle databases (IFDB).

    Returns ExtractResult with list[dict[str, Any]] data from all extraction methods.
    Handles Oracle-specific concerns: LOB objects, TRUNC/CAST date filtering,
    TO_DATE for catchup windows.
    """

    def extract(
        self,
        config: TableExtractionConfig,
        partition_key: str | None,
        partition_type: str,
    ) -> ExtractResult:
        policy = ExtractBatchingPolicy(
            enabled=True,
            fetch_size=2000,
            env_prefix="ORACLE_EXTRACT_LEGACY",
        )
        batch_source = self.make_row_batch_source(config, partition_key, partition_type, policy)
        try:
            rows = [row for batch in batch_source.iter_batches() for row in batch]
            columns = (
                list(batch_source.column_names)
                if batch_source.column_names
                else list(config.columns)
            )
        finally:
            batch_source.close()

        table_name = config.table.split(".")[-1].lower()
        entity_name = table_name.split("_")[0] if "_" in table_name else table_name

        columns_info = [(c, None, None, None, None, None) for c in columns]

        self.context.log.info(
            f"[IFDB EXTRACTION] Retrieved {len(rows)} {entity_name} records from {config.table} "
            f"(partition: {partition_type}, key: {partition_key})"
        )

        return ExtractResult(
            data=rows,
            columns_info=columns_info,
            column_names=columns,
            row_count=len(rows),
        )

    def make_row_batch_source(
        self,
        config: TableExtractionConfig,
        partition_key: str | None,
        partition_type: str,
        policy: ExtractBatchingPolicy,
    ) -> OracleRowBatchSource:
        query = self._build_extract_query(config, partition_key, partition_type)
        self._log_query(config, query)

        cursor = self.source_conn.cursor()
        try:
            cursor.execute(query)
            column_names = tuple(desc[0] for desc in (cursor.description or []))
            if not column_names:
                column_names = tuple(config.columns)
        except BaseException:
            cursor.close()
            raise

        return OracleRowBatchSource(
            cursor=cursor,
            batch_size=policy.fetch_size,
            context=self.context,
            column_names=column_names,
        )

    def _build_extract_query(
        self,
        config: TableExtractionConfig,
        partition_key: str | None,
        partition_type: str,
    ) -> str:
        run_tags = getattr(getattr(self.context, "run", None), "tags", {}) or {}
        is_scheduled_catchup = run_tags.get("ifdb_extraction_mode") == "scheduled_catchup"
        if is_scheduled_catchup:
            self.context.log.info(
                "Running in SCHEDULED CATCHUP mode: "
                "Extracting window from Yesterday 05:00 to Today 06:00."
            )

        first_partition = bool(partition_key) and is_first_partition(partition_key)
        if first_partition:
            self.context.log.info(
                f"First partition detected ({partition_key}): using upper-bound-only filtering "
                "to include all historical data."
            )

        query = build_partition_query(
            config,
            partition_key or "",
            partition_type,
            is_scheduled_catchup,
            first_partition,
        )
        return query

    def _log_query(self, config: TableExtractionConfig, query: str) -> None:
        self.context.log.info(f"Executing extraction query for {config.table}")
        self.context.log.debug(f"Query: {query}")

    def custom_extract(self, query: str) -> ExtractResult:
        self.context.log.info("Executing custom Oracle query")
        self.context.log.debug(f"Query: {query}")

        cursor = self.source_conn.cursor()
        try:
            cursor.execute(query)
            rows = fetch_rows_as_dicts(cursor, self.context)
        finally:
            cursor.close()

        columns = list(rows[0].keys()) if rows else []
        columns_info = [(c, None, None, None, None, None) for c in columns]

        self.context.log.info(f"Custom query returned {len(rows)} rows")

        return ExtractResult(
            data=rows,
            columns_info=columns_info,
            column_names=columns,
            row_count=len(rows),
        )
