# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SilkeborgData E26PERS archive — UPSERT ext.silkeborgdata_e26_pers into arc.

The archive accumulates monthly snapshots at (Inst, Tjenestenummer,
Udtraeksdato) grain and NEVER deletes: ext only ever holds the deliveries just
read, so delete detection would soft-delete every earlier month.

This asset owns the DFM checkpoint. execute_archive_merge runs checkpoint_query
AFTER the MERGE has committed, so the query sees the rows just written, and
writes the result to meta.DFMState for arc.silkeborgdata_e26_pers. The extract
reads that value back as its lower bound on the next run.

Two config fields are deliberately NOT set, and both matter:

  change_detect_columns — left unspecified, so it defaults to non_key_columns,
    which means dw_leverance_dato COUNTS as a change. The field's own docstring
    invites narrowing it for "a delivery or batch stamp", and haiba_archive.py
    does exactly that. Doing it here would break the checkpoint permanently and
    silently: a row whose only change is the delivery stamp would not be
    updated, dw_leverance_dato would not be written, MAX(dw_leverance_dato)
    would not move, and the same delivery folder would be re-selected every day,
    green, forever. The docstring's condition does not apply either — the stamp
    is not rewritten every run by construction, only when the delivery really is
    a different one. The price of letting it count as a change is that a pure
    re-issue of an identical period touches dw_timestamp_update. That is the
    right trade: a slightly noisy change signal beats a stuck checkpoint.

  dedup_source_on_keys — left False. E26PERS has no upstream primary key, which
    is precisely what that field is for, but it cannot do this job: its winner is
    decided by ROW_NUMBER() ... ORDER BY non_key_columns DESC, whereas we need
    the NEWEST DELIVERY FOLDER to win. Deduplication therefore lives in the
    extract, where the order is controlled. Do not add it on top — it would sort
    a different winner than the extract just chose — and do not remove the
    Python dedup, which enable_bulk_insert_on_first_load depends on.

Why MAX(dw_leverance_dato) is safe here at all, when HAIBA rejected it: HAIBA's
grain excludes the delivery period, so a week without value changes writes
nothing and the column would leave the archive looking older than it is. Here
Udtraeksdato IS part of the key, so every new delivery brings new keys and
therefore real INSERTs. The guard against the remaining nul-case — a selected
delivery that yields zero rows — is a hard failure in the extract.
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from DSI.csv_files.silkeborgdata_e26_pers import ARC_TABLE, EXT_COLUMNS, KEY_COLUMNS

_MERGE_KEYS = KEY_COLUMNS

E26_PERS_ARCHIVE_CONFIG = ArchiveMergeConfig(
    source_schema="ext",
    source_table=ARC_TABLE,
    target_schema="arc",
    target_table=ARC_TABLE,
    merge_keys=_MERGE_KEYS,
    # Derived from EXT_COLUMNS, so dw_leverance_dato and dw_leverance_mappe come
    # along automatically. If they were missing, they would never be copied to
    # arc, checkpoint_query would return NULL forever, and every run would
    # reload the whole history.
    non_key_columns=tuple(c for c in EXT_COLUMNS if c not in _MERGE_KEYS),
    # upsert, not insert_only: makes a reload of the same delivery idempotent.
    # insert_only would fail on the primary key on any rerun.
    strategy="upsert",
    # Keeps dw_timestamp_update meaningful as a change signal — a rerun of an
    # unchanged delivery touches no rows.
    update_only_on_change=True,
    # change_detect_columns is deliberately NOT set — see module docstring.
    # A month archive never deletes; ext is only ever a partial slice.
    detect_deletes=False,
    upstream_is_full_load=False,
    # dedup_source_on_keys is deliberately NOT set — see module docstring.
    # First load into an empty arc becomes one minimally-logged
    # INSERT ... SELECT WITH (TABLOCK) instead of a MERGE. Safe because the
    # extract guarantees ext is unique on merge_keys.
    enable_bulk_insert_on_first_load=True,
    # The DFM contract's pivot. The date must come from the DELIVERY FOLDER, not
    # from Udtraeksdato: MAX(Udtraeksdato) would describe which period the data
    # is about instead of which delivery we have fetched, so a late or re-issued
    # delivery could be skipped. CONVERT(..., 23) yields YYYY-MM-DD, which sorts
    # correctly both as a string and after parsing, in line with the
    # 20260623_normalize_dfm_state_iso_checkpoints migration. An empty archive
    # returns NULL, the checkpoint is not moved, and the next run behaves as a
    # first run — the desired behaviour.
    checkpoint_query=(
        "SELECT CONVERT(CHAR(10), MAX([dw_leverance_dato]), 23) "
        "FROM [arc].[silkeborgdata_e26_pers]"
    ),
)


@dg.asset(
    name="arc_silkeborgdata_e26_pers",
    group_name="silkeborgdata_archive",
    compute_kind="mssql",
    deps=["extract_silkeborgdata_e26_pers"],
    tags={
        "domain": "silkeborgdata",
        "layer": "archive",
        "source": "silkeborgdata",
        "entity": "e26pers",
    },
    description=(
        "UPSERT ext.silkeborgdata_e26_pers -> arc.silkeborgdata_e26_pers "
        "(persistent archive, one row per Inst+Tjenestenummer+Udtraeksdato). "
        "Sets the DFM checkpoint to the latest delivery folder's date. Never deletes."
    ),
)
def arc_silkeborgdata_e26_pers(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    result = execute_archive_merge(context, mssql_target_staging, E26_PERS_ARCHIVE_CONFIG, config)

    context.log.info(
        f"MERGEd into arc.{ARC_TABLE} — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}; mode={config.mode}, "
        f"checkpoint={result.new_checkpoint!r}"
    )

    return dg.MaterializeResult(
        metadata=build_archive_metadata(
            result,
            config,
            source_table=f"ext.{ARC_TABLE}",
            target_table=f"arc.{ARC_TABLE}",
        )
    )


__all__ = ["E26_PERS_ARCHIVE_CONFIG", "arc_silkeborgdata_e26_pers"]
