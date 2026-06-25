# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
DFM configuration classes.

RunModeConfig      — run-time config applied to every DFM-enabled asset.
FlowStartConfig    — run-time config for the dfm_ifdb_flow_start asset.
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
        "When set, the source system query is skipped entirely. "
        "When left empty (default), the timestamp is read from the source "
        "(IFDB: MAX(JOBSLUT) in JOB_LOG/JOB_STATUS); if that returns NULL "
        "(e.g. local dev), it falls back to MSSQL SYSDATETIME(). "
        'Format: "YYYY-MM-DD HH:MM:SS". Example: "2025-06-01 12:00:00".',
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
    """

    source_schema: str
    source_table: str
    target_schema: str
    target_table: str
    merge_keys: tuple[str, ...]
    non_key_columns: tuple[str, ...]
    checkpoint_query: str | None = None
    flow_start_key: tuple[str, str] | None = None
    strategy: str = "upsert"
    update_only_on_change: bool = False
    detect_deletes: bool = False
    soft_delete_column: str = "DW_IsDeleted"
    source_delete_column: str | None = None
    upstream_is_full_load: bool = False

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


class DfmResetConfig(dg.Config):
    """Config for the ad-hoc checkpoint reset asset."""

    schema_name: str = Field(
        description='Schema owning the checkpoint to reset. Example: "arc", "ext", "dfm".',
    )
    table_name: str = Field(
        description='Table owning the checkpoint to reset. Example: "encounter_ifdb".',
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
