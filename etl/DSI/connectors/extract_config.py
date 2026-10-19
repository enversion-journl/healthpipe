# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Universal DFM extract configuration.

DfmExtractConfig is a dialect-agnostic dataclass that describes WHAT to extract
and how the checkpoint behaves.  SQL formatting (HOW) is handled by dialect-
specific builders (build_mssql_where, build_oracle_where, etc.).

Used by:
- DSI/connectors/mssql/dfm_sql_builders.py  (MSSQL WHERE generation)
- DSI/oracle/ifdb_extract.py               (Oracle WHERE generation)
- DSI/connectors/factory.py                (DFM asset factory)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DfmExtractConfig:
    """Universal design-time config for DFM-based extraction.

    Dialect-agnostic: describes WHAT to extract and how the checkpoint
    behaves.  SQL formatting (HOW) is handled by dialect-specific WHERE
    clause builders.

    Attributes:
        source_schema: Schema in the source database (e.g. "dbo", "fact", "IFDB").
        source_table: Table name in the source database (e.g. "SpendAnalysisLines").
        target_table: Target ext table name in the staging database
            (e.g. "kmd_uddata_spend_analysis_lines").
        delta_column: Simple column name for delta filtering.
            Examples: "sysdato", "IFDB_MODIFICATION_DATETIME", "dw_timestamp_update".
            Mutually exclusive with delta_expression.
        delta_expression: Complex SQL expression for delta filtering.
            Examples: "GREATEST([dw_audit_insert], [dw_audit_update], [dw_audit])".
            Mutually exclusive with delta_column.
        checkpoint_type: How the checkpoint value is formatted in the WHERE clause.
            "datetime" — compared via CAST/TO_DATE (default).
            "int" — compared as a raw integer (no quotes).
        lookback_days: Design-time default number of extra days to subtract from the
            checkpoint in delta mode, protecting against late-arriving data.
            Can be overridden at run-time via RunModeConfig.lookback_days.
        is_full_load: When True, no checkpoint filter is ever applied — always
            extracts all rows from the source table.
        checkpoint_from: Where to READ the checkpoint from.  Tuple of
            (schema, table_name) mapping directly to
            DfmManager.start_dfm_run(checkpoint_from=...).
            Examples: ("arc", "spend_analysis_lines"), ("dim", "Kreditor").
            None = no checkpoint source (full-load tables).
        columns: Explicit column list for the SELECT clause.  Required for Oracle
            (cross-platform) extracts; optional for MSSQL (defaults to SELECT *).
        select_from_expr: Full SELECT+FROM override.  When set, the extractor uses
            this verbatim instead of generating
            "SELECT {columns or *} FROM [source_schema].[source_table]".
            The WHERE clause from checkpoint/RunModeConfig is still appended:
            if the expression already contains a top-level WHERE keyword the
            framework appends "AND (<delta>)"; otherwise it appends
            "WHERE <delta>".  CTEs or subqueries with an internal WHERE must
            still expose a top-level WHERE if they need delta filtering.
        exclude_columns: Columns to drop from SELECT * (MSSQL post-fetch filter).
        column_types: Mapping of column name to MSSQL type for ext table DDL.
            Used for cross-platform extracts (e.g. Oracle→MSSQL) where the target
            ext table needs properly sized columns instead of NVARCHAR(MAX).
        upper_bound_column: Column to use for the upper-bound (flow-start ceiling).
            When set, the WHERE clause becomes:
              delta_column > checkpoint AND upper_bound_column <= upper_bound
            The archive checkpoint becomes the ceiling, which the next run compares
            with the delta column, so both columns must follow the clock the
            ceiling was read from.
        flow_start_from: Where to READ the upper-bound value from at runtime.
            Tuple of (schema, table) in meta.DFMState — the factory reads this
            checkpoint and passes it as upper_bound to the WHERE builder (delta mode
            only), and records the value it read under ``used_ceiling_key`` for the
            archive merge.  Example: IFDB_TRANSFER_CEILING for IFDB tables.
            When None (default), no upper bound is applied by the factory.
        index_keys: Columns to create a clustered index on after loading the ext table.
        description: Human-readable description for asset metadata and documentation.
    """

    # Source identity
    source_schema: str
    source_table: str
    target_table: str

    # Delta definition (mutually exclusive: delta_column OR delta_expression)
    delta_column: str | None = None
    delta_expression: str | None = None
    checkpoint_type: str = "datetime"
    lookback_days: int = 0

    # Full-load (no checkpoint filter, always pull all)
    is_full_load: bool = False

    # Checkpoint source — where to READ the checkpoint from.
    checkpoint_from: tuple[str, str] | None = None

    # Column selection
    columns: tuple[str, ...] | None = None
    select_from_expr: str | None = None
    exclude_columns: tuple[str, ...] = ()

    # Target table typing (for cross-platform extracts like Oracle→MSSQL)
    column_types: dict[str, str] | None = None

    # Column the flow-start ceiling filters on (defaults to the delta column)
    upper_bound_column: str | None = None

    # Flow-start source — where to READ the upper-bound value from at runtime.
    # When set, the factory reads dfm.get_checkpoint(flow_start_from[0], flow_start_from[1])
    # and passes it as upper_bound to the WHERE builder (delta mode only).
    # The value read is recorded under used_ceiling_key for the archive merge.
    # Example: IFDB_TRANSFER_CEILING for IFDB extracts.
    flow_start_from: tuple[str, str] | None = None

    # Post-load optimization
    index_keys: tuple[str, ...] | None = None

    # Metadata
    description: str = ""

    @property
    def effective_delta_col(self) -> str | None:
        """The effective column/expression for WHERE clause generation.

        Returns delta_expression if set, otherwise delta_column.
        Both may be None for full-load tables.
        """
        return self.delta_expression or self.delta_column

    @property
    def used_ceiling_key(self) -> tuple[str, str]:
        """meta.DFMState row with the flow_start_from ceiling this table's last extract read."""
        return ("dfm", f"{self.target_table}_flow_start")

    _VALID_CHECKPOINT_TYPES = frozenset({"datetime", "int"})

    def __post_init__(self) -> None:
        if self.checkpoint_type not in self._VALID_CHECKPOINT_TYPES:
            raise ValueError(
                f"DfmExtractConfig for {self.source_schema}.{self.source_table}: "
                f"checkpoint_type must be one of {sorted(self._VALID_CHECKPOINT_TYPES)}, "
                f"got {self.checkpoint_type!r}."
            )
        if self.delta_column and self.delta_expression:
            raise ValueError(
                f"DfmExtractConfig for {self.source_schema}.{self.source_table}: "
                "delta_column and delta_expression are mutually exclusive. "
                "Use delta_column for simple columns, delta_expression for complex expressions."
            )
        if not self.is_full_load and not self.effective_delta_col:
            raise ValueError(
                f"DfmExtractConfig for {self.source_schema}.{self.source_table}: "
                "delta extracts require either delta_column or delta_expression. "
                "Set is_full_load=True for tables that always extract all rows."
            )
