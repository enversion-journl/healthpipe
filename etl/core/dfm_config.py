# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DFM configuration classes.

RunModeConfig      — run-time config applied to every DFM-enabled asset.
FlowStartConfig    — run-time config for the dfm_ifdb_flow_start asset.
IFDBCycleWaitConfig — run-time config for the dfm_ifdb_cycle_wait asset.
ArchiveMergeConfig — design-time config driving the MERGE SQL generator.
DfmResetConfig     — config for the ad-hoc checkpoint reset asset.
"""

from dataclasses import dataclass

import dagster as dg
from pydantic import Field, model_validator

VALID_MODES = ("delta", "full_load", "period", "ids")


class RunModeConfig(dg.Config):
    """Run-time config controlling extraction mode and checkpoint behaviour.

    Default values produce delta behaviour (the most common scheduled mode).

    ``advance_checkpoint`` controls whether the asset advances its DFM
    checkpoint on success:
      - ``None`` (default): advances for delta/full_load, blocks for period/ids.
      - ``True``: explicitly advance (opt-in for period/ids, e.g. period orchestrator).
      - ``False``: explicitly block advancement (dry-run / test).
    """

    mode: str = Field(
        default="delta",
        description='Extraction mode. Must be one of: "delta" (incremental from checkpoint), '
        '"full_load" (extract all rows), "period" (date range via date_from/date_to), '
        '"ids" (filter by specific IDs via id_values or id_filter_table).',
    )
    advance_checkpoint: bool | None = Field(
        default=None,
        description="Advance the DFM checkpoint after a successful run. "
        "None (default) = mode-appropriate (True for delta/full_load, False for period/ids). "
        "Set True to explicitly advance for period/ids. Set False for dry-run.",
    )
    delta_column: str | None = Field(
        default=None,
        description="Override the design-time delta column used for checkpoint filtering. "
        'Example: "IFDB_MODIFICATION_DATETIME".',
    )
    lookback_days: int | None = Field(
        default=None,
        description="In delta mode, subtract this many days from the checkpoint to re-extract "
        "recently modified rows. Example: 3.",
    )
    date_from: str | None = Field(
        default=None,
        description="Start date for period mode (inclusive — rows on or after this date). "
        'Format: "YYYY-MM-DD". Example: "2025-01-01" to include from Jan 1st.',
    )
    date_to: str | None = Field(
        default=None,
        description="End date for period mode (optional upper bound — rows up to and "
        "including this full day). When omitted, no upper boundary is applied. "
        'Format: "YYYY-MM-DD". Example: "2025-01-31" to include all of Jan 31st.',
    )
    id_filter_table: str | None = Field(
        default=None,
        description="In ids mode, a table containing IDs to filter on via a subquery "
        '(SELECT id_filter_column FROM table). Example: "tmp.my_ids".',
    )
    id_filter_column: str | None = Field(
        default=None,
        description="Column name to filter on in ids mode. Required when mode='ids'. "
        "Used both with id_filter_values (inline) and id_filter_table (subquery). "
        'Example: "PATIENT_ID", "OBJECT_ID", "DW_EK_SpendAnalysisLine".',
    )
    id_filter_values: list[str] | None = Field(
        default=None,
        description="In ids mode, an explicit list of ID values to filter on "
        "(alternative to id_filter_table). "
        'Example: ["ABC123", "DEF456"].',
    )
    soft_delete_threshold: int | None = Field(
        default=None,
        description="Raise the soft-delete guard for THIS RUN only. A deliberate rebuild "
        "retires far more rows than a daily delta, but raising the standing threshold "
        "would blind the guard on every scheduled run afterwards. Confirm the count is "
        "explained before setting it. Example: 10000.",
    )

    @model_validator(mode="after")
    def _validate_mode_constraints(self) -> "RunModeConfig":
        if self.mode not in VALID_MODES:
            raise ValueError(f"Invalid mode: {self.mode!r}. Must be one of: {VALID_MODES}")
        if self.mode == "ids":
            if not self.id_filter_values and not self.id_filter_table:
                raise ValueError(
                    "mode='ids' requires either id_filter_values or id_filter_table " "to be set."
                )
            if not self.id_filter_column:
                raise ValueError("mode='ids' requires id_filter_column to be set.")
        if self.mode == "period":
            if not self.date_from:
                raise ValueError("mode='period' requires date_from to be set.")
        return self

    @property
    def should_advance_checkpoint(self) -> bool:
        """Resolve advance_checkpoint: None → mode-appropriate default."""
        if self.advance_checkpoint is not None:
            return self.advance_checkpoint
        return self.mode in ("delta", "full_load")


class FlowStartConfig(dg.Config):
    """Run-time config for flow-start assets that capture an upper-bound timestamp.

    Currently only used by dfm_ifdb_flow_start but designed for reuse by any
    source system that provides an external "last-produced-data" ceiling
    (e.g. a JOB_LOG table or equivalent mechanism).
    """

    flow_start_override: str | None = Field(
        default=None,
        description="Manually set the flow-start upper-bound timestamp. "
        "When set, the source system query is skipped entirely; for IFDB the value "
        "sets both the transfer ceiling and the EPJ ceiling. "
        "When left empty (default), the timestamp is read from the source "
        "(IFDB: Oracle SYSDATE at a complete cycle and MIN(MAX(SLUTTID)) in "
        "JOB_LOG/JOB_STATUS); if JOB_LOG returns no rows (e.g. local dev), it falls "
        "back to MSSQL SYSDATETIME(). "
        'Format: "YYYY-MM-DD HH:MM:SS". Example: "2025-06-01 12:00:00".',
    )


class IFDBCycleWaitConfig(dg.Config):
    """Run-time config for dfm_ifdb_cycle_wait, which holds the IFDB flow start."""

    cycle_wait_timeout_minutes: int = Field(
        default=180,
        ge=0,
        description="How long to wait for every active IFDB job type to finish the same "
        "cycle before the asset fails. 0 turns the wait off: the asset then returns at once "
        "without reading IFDB.",
    )


@dataclass(frozen=True)
class ArchiveMergeConfig:
    """Declarative config that drives build_archive_merge_sql().

    One instance per archive table, defined at design time.

    ``merge_keys`` vs ``non_key_columns``:
      - ``upsert``: merge_keys are the business-key columns used in the MERGE
        ``ON`` clause (must be non-empty), and together with non_key_columns
        they form the full INSERT/UPDATE column set.
      - ``insert_only``: there is NO merge / ON clause — every run just appends
        the source rows. merge_keys is therefore irrelevant and should be left
        empty (``()``); list all copied columns in non_key_columns.

    ``enable_bulk_insert_on_first_load`` (upsert only): when True and the target
    arc table is empty, the initial load bypasses the MERGE in favour of a
    single ``INSERT ... WITH (TABLOCK) SELECT`` from the ext heap (see
    core.archive_merge); ext keeps its rows, as with MERGE. This is much faster
    for large cold loads (minimally logged under SIMPLE/BULK_LOGGED recovery)
    but inserts source rows directly, so it must only be enabled for sources
    whose ext rows are guaranteed unique on ``merge_keys`` — otherwise the arc
    primary key is violated. Off by default; the MERGE path is always safe.

    ``dedup_source_on_keys`` (upsert only): when True the source is wrapped in a
    ``ROW_NUMBER() OVER (PARTITION BY merge_keys ...) = 1`` derived table before
    it reaches the MERGE (and the empty-target fast load). Set it for every
    source table that has NO primary key upstream, where ``merge_keys`` is a
    grain we *assert* rather than one the source enforces: a single duplicate
    pair then aborts the whole MERGE on the arc primary key. Deduplicating in
    arc rather than in the extract keeps ext a faithful raw landing and costs
    only a sort over the delta slice. The winner sorts first on
    ``non_key_columns`` DESC — deterministic, and populated rows beat NULL-heavy
    ones; irrelevant when the duplicates are exact copies. Where a duplicate
    group genuinely disagrees this DROPS the losing row, so
    ``execute_archive_merge`` counts the dropped rows, logs a warning when the
    count is non-zero, and reports it as ``rows_dedup_dropped`` in the asset
    metadata — treat a non-zero count as a signal that the asserted grain is
    wrong, not as normal operation.
    """

    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    merge_keys: tuple[str, ...]
    non_key_columns: tuple[str, ...]
    # Columns that decide whether a matched row actually changed, when
    # update_only_on_change is set. Defaults to non_key_columns. Narrow it when
    # a column is rewritten every run by construction — a delivery or batch
    # stamp — which would otherwise make the change guard always true and
    # update_only_on_change a no-op. The columns stay in non_key_columns and are
    # still written; they just do not count as a change on their own.
    change_detect_columns: tuple[str, ...] | None = None
    checkpoint_query: str | None = None
    flow_start_key: tuple[str, str] | None = None
    strategy: str = "upsert"
    update_only_on_change: bool = False
    detect_deletes: bool = False
    soft_delete_column: str = "dw_is_deleted"
    source_delete_column: str | None = None
    upstream_is_full_load: bool = False
    enable_bulk_insert_on_first_load: bool = False
    dedup_source_on_keys: bool = False
    # Extra predicate on WHEN NOT MATCHED BY SOURCE (T-SQL: target columns
    # only, as tgt.[column]). Limits delete detection to a slice of the
    # archive — e.g. a delivery that covers only some activity years.
    # Computed per run from ext; ignored unless detect_deletes is effective.
    delete_scope_predicate: str | None = None
    # Extra predicate narrowing which SOURCE rows reach the MERGE (T-SQL:
    # source columns, UNQUALIFIED — it is applied inside a derived table over
    # the source). Use when the run must merge only part of what ext holds,
    # e.g. only the activity-year slices whose delivery is actually newer than
    # what the archive reflects.
    #
    # MUST be kept consistent with delete_scope_predicate: both describe the
    # same slice set. Narrowing the source while delete detection still covers
    # a slice makes WHEN NOT MATCHED BY SOURCE soft-delete that whole slice
    # (the [internal ticket] failure class).
    source_scope_predicate: str | None = None

    def __post_init__(self) -> None:
        if isinstance(self.merge_keys, str):
            raise TypeError(
                f"ArchiveMergeConfig for {self.target_schema}.{self.target_table}: "
                f"merge_keys must be a tuple, not a string. "
                f"Did you forget the trailing comma? Use ('{self.merge_keys}',)"
            )
        if isinstance(self.non_key_columns, str):
            raise TypeError(
                f"ArchiveMergeConfig for {self.target_schema}.{self.target_table}: "
                f"non_key_columns must be a tuple, not a string. "
                f"Did you forget the trailing comma? Use ('{self.non_key_columns}',)"
            )
        # upsert needs keys for the MERGE ON clause; an empty tuple would emit a
        # broken/empty ON clause. insert_only ignores merge_keys (pure append),
        # so it should be left empty.
        if self.strategy != "insert_only" and not self.merge_keys:
            raise ValueError(
                f"ArchiveMergeConfig for {self.target_schema}.{self.target_table}: "
                f"strategy={self.strategy!r} requires non-empty merge_keys for the "
                f"MERGE ON clause."
            )
        # insert_only never emits an ON clause, so there is no grain to dedup on.
        if self.dedup_source_on_keys and self.strategy == "insert_only":
            raise ValueError(
                f"ArchiveMergeConfig for {self.target_schema}.{self.target_table}: "
                "dedup_source_on_keys is meaningless with strategy='insert_only' "
                "(no merge keys, no ON clause)."
            )


class DfmResetConfig(dg.Config):
    """Config for the ad-hoc checkpoint reset asset."""

    schema_name: str = Field(
        description='Schema owning the checkpoint to reset. Example: "arc", "ext", "dfm".',
    )
    table_name: str = Field(
        description='Table owning the checkpoint to reset. Example: "ifdb_encounter".',
    )
    new_value: str = Field(
        description="New checkpoint value to set. " 'Example: "2025-01-01 00:00:00".',
    )
    reason: str = Field(
        default="",
        description="Free-text reason for the reset (logged for auditing). "
        'Example: "Re-extract after source bugfix".',
    )
    scope: str = Field(
        default="staging",
        description='Checkpoint scope. Options: "staging", "dso".',
    )
