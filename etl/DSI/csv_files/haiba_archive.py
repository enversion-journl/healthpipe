# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
HAIBA archive layer — MERGE ext.haiba_* into the persistent arc.haiba_* tables.

Every delivery is a full snapshot and the archive must mirror the newest one,
so the merge runs with ``detect_deletes=True``: rows SDS no longer publishes get
``dw_is_deleted = 1``. That makes the extract's two invariants load-bearing —
see DSI/csv_files/haiba.py. ``WHEN NOT MATCHED BY SOURCE`` in
core/archive_merge.py is unscoped, so anything missing from ext is soft-deleted
across the whole table, not just the HAI type being loaded.

Mirroring the newest snapshot means the archive alone cannot answer "what did
SDS tell us last month?". SDS revises history continuously — roughly 16,500
BACT rows changed value on an unchanged key over a single week — so
``capture_agg_revisions`` writes those before/after pairs to
arc.haiba_agg_revision before each merge. That log is the whole reason for
routing HAIBA through an archive instead of reading the files straight into a
mart.

Expect one very large delete batch each January: the delivery window is
anchored to a calendar year and rolls a whole year at a time (343,177 BACT rows
disappeared between the February 2025 and April 2026 deliveries, ~22% of the
table). In normal weeks deletions are in the hundreds.

Case data is near append-only (0 changed rows against 3,268 unchanged over a
week), so only the agg tables get a revision log.

Which delivery the archive stands on is registered in meta.HAIBALeverance, not
inferred from MAX(arc.leverance_dato). That column is only rewritten when a
row's values change, and the case tables can go weeks without one — so the
column would leave the archive looking older than it is, and
``guard_delivery_direction`` would let a stale pinned extract through.

The extract's manifest (ext.<table>_leverance_manifest) is the merge contract:
empty means the run loaded nothing — a declared no-op, not an invitation to
re-merge whatever ext still holds from an earlier run. After a MERGE the
delivery checkpoints in meta.DFMState are advanced from the manifest, one per
(HAI type, kind) — only then, so a failed merge leaves them unmoved and the
next extract run re-reads the delivery.
"""

from dataclasses import dataclass
from datetime import date, datetime

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    delivery_identity,
)
from DSI.csv_files._sds_tar import upsert_register
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    DELIVERY_COLUMN,
    HaibaSchema,
    haiba_delivery_unit,
    manifest_table,
)
from pydantic import Field
from utils.sql_safety import safe_bracket_quote


class HaibaArchiveConfig(RunModeConfig):
    """Archive run config, extended with the historical backfill switch."""

    backfill_historical: bool = Field(
        default=False,
        description=(
            "Load a delivery older than what the archive already holds, inserting "
            "only rows whose key is missing and flagging them dw_is_deleted = 1. "
            "Existing rows are left untouched and nothing is soft-deleted. Pair it "
            "with delivery_on_or_before on the extract to recover a window SDS has "
            "dropped — e.g. 2020, last published in the 2025-12-27 delivery. "
            "Never use for a current delivery: it would flag live rows as deleted."
        ),
    )


REVISION_TABLE = "haiba_agg_revision"

# Control table in meta, not arc: it mirrors no delivery file and takes no part
# in the MERGE. It carries the delivery the archive stands on, which is a
# property of the table rather than of any row — see tables/haiba/02_meta.sql.
LEVERANCE_TABLE = "HAIBALeverance"

# Measure columns per agg schema — the ones SDS revises, and the only ones the
# revision log tracks. The dimensions are part of the merge key or descriptive.
_AGG_MEASURES = {
    AGG_INFEKTION.name: ("antal", "risikodoegn"),
    AGG_ORTOPAEDI.name: ("antal90", "antal365", "operationer"),
}


def build_config(schema: HaibaSchema) -> ArchiveMergeConfig:
    """One archive config per ext/arc table pair.

    Derived from the extract schema rather than re-typed, so a column added to
    ext cannot silently go missing from the MERGE column list.
    """
    keys = schema.key_columns
    non_key = tuple(c for c in schema.all_columns if c not in keys)
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=schema.ext_table,
        target_schema="arc",
        target_table=schema.ext_table,
        merge_keys=keys,
        non_key_columns=non_key,
        # leverance_dato differs on every delivery by construction, so leaving it
        # in the change check would make update_only_on_change below a no-op and
        # rewrite all 6.7M agg rows weekly. It is still written on a real change;
        # the delivery the archive stands on lives in meta.HAIBALeverance.
        change_detect_columns=tuple(c for c in non_key if c != DELIVERY_COLUMN),
        strategy="upsert",
        # Makes dw_timestamp_update a real change signal instead of a
        # touch-everything timestamp.
        update_only_on_change=True,
        # The archive mirrors the newest delivery: rows SDS drops are
        # soft-deleted rather than left behind as if still current.
        detect_deletes=True,
        # The extract always writes every known HAI type at its own newest
        # delivery, so ext is a complete snapshot regardless of run mode.
        # Without this the engine disables delete detection.
        upstream_is_full_load=True,
        # ext rows are unique on the merge keys — verified against all 14
        # delivery files — so the cold-start bulk path is safe.
        enable_bulk_insert_on_first_load=True,
    )


AGG_INFEKTION_CONFIG = build_config(AGG_INFEKTION)
AGG_ORTOPAEDI_CONFIG = build_config(AGG_ORTOPAEDI)
CASES_INFEKTION_CONFIG = build_config(CASES_INFEKTION)
CASES_SSI_CONFIG = build_config(CASES_SSI)


def build_revision_sql(schema: HaibaSchema) -> str:
    """INSERT ... SELECT that logs every measure change ext is about to apply.

    Runs BEFORE the MERGE — afterwards the old values are gone. One row per
    changed measure, so a row where both antal and risikodoegn moved produces
    two entries.
    """
    measures = _AGG_MEASURES[schema.name]
    keys = schema.key_columns
    on_clause = " AND ".join(f"tgt.[{k}] = src.[{k}]" for k in keys)
    key_select = ", ".join(f"src.[{k}]" for k in keys)
    key_columns = ", ".join(f"[{k}]" for k in keys)

    # One UNION ALL branch per measure. NULL-safe comparison: a measure going
    # from a value to NULL (or back) is a revision too, and <> would miss it.
    branches = "\n    UNION ALL\n".join(
        f"""    SELECT
        '{schema.name}', {key_select},
        '{measure}', tgt.[{measure}], src.[{measure}],
        tgt.[leverance_dato], src.[leverance_dato]
    FROM [ext].[{schema.ext_table}] AS src
    INNER JOIN [arc].[{schema.ext_table}] AS tgt ON {on_clause}
    WHERE EXISTS (
        SELECT tgt.[{measure}] EXCEPT SELECT src.[{measure}]
    )"""
        for measure in measures
    )

    return f"""
INSERT INTO [arc].[{REVISION_TABLE}] (
    [kildetabel], {key_columns},
    [maalekolonne], [vaerdi_foer], [vaerdi_efter],
    [leverance_dato_foer], [leverance_dato]
)
{branches};
"""


def build_backfill_sql(schema: HaibaSchema) -> str:
    """INSERT of rows the archive has never seen, flagged as no longer published.

    Deliberately not a MERGE: a pinned older delivery run through the normal
    path would overwrite current values with stale ones and soft-delete every
    row newer than the pin. This only adds keys that are absent, so a rerun is
    a no-op and current data cannot be touched.
    """
    keys = schema.key_columns
    columns = schema.all_columns
    col_list = ", ".join(f"[{c}]" for c in columns)
    src_list = ", ".join(f"src.[{c}]" for c in columns)
    on_clause = " AND ".join(f"tgt.[{k}] = src.[{k}]" for k in keys)
    table = schema.ext_table
    return f"""
INSERT INTO [arc].[{table}] (
    {col_list},
    [dw_is_deleted], [dw_timestamp_insert], [dw_timestamp_update]
)
SELECT
    {src_list},
    1, CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
FROM [ext].[{table}] AS src
WHERE NOT EXISTS (
    SELECT 1 FROM [arc].[{table}] AS tgt WHERE {on_clause}
);
"""


@dataclass(frozen=True)
class HaibaDeliveryManifest:
    """One HAI type's delivery as the extract recorded it this run."""

    hai_type: str
    kind: str
    filnavn: str
    leverance_dato: date
    fil_mtime: datetime


def read_manifest(mssql: MSSQLResource, schema: HaibaSchema) -> dict[str, HaibaDeliveryManifest]:
    """What the extract loaded this run, per HAI type.

    A missing table (an ext from before delivery detection deployed) reads the
    same as an empty one: this run loaded nothing.
    """
    table = safe_bracket_quote(manifest_table(schema))
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT OBJECT_ID(N'[ext].{table}', N'U')")
            if cursor.fetchone()[0] is None:
                return {}
            cursor.execute(
                f"SELECT [hai_type], [kind], [filnavn], [{DELIVERY_COLUMN}], [fil_mtime] "
                f"FROM [ext].{table}"
            )
            rows = cursor.fetchall()
        finally:
            cursor.close()
    return {row[0]: HaibaDeliveryManifest(*row) for row in rows}


def advance_delivery_checkpoints(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    manifest: dict[str, HaibaDeliveryManifest],
    mode: str,
) -> list[str]:
    """Advance the delivery checkpoint for every HAI type this run merged.

    Only called after the MERGE has committed: the checkpoint promises "this
    delivery is in arc", so a failed merge leaves it unmoved and the next run
    re-reads the delivery — the safe direction.
    """
    dfm = DfmManager(mssql, context)
    stamped: list[str] = []
    with dfm.connection():
        for hai_type in sorted(manifest):
            entry = manifest[hai_type]
            unit = haiba_delivery_unit(hai_type, entry.kind)
            identity = delivery_identity(entry.leverance_dato, entry.fil_mtime)
            dfm.set_checkpoint(
                DELIVERY_SCHEMA, unit, value=identity, mode=mode, target_type=DELIVERY_TARGET_TYPE
            )
            stamped.append(f"{unit} = {identity} ({entry.filnavn})")
            context.log.info(f"checkpoint {unit} -> {identity}")
    return stamped


def _max_delivery(mssql: MSSQLResource, schema_name: str, table: str) -> str | None:
    """Newest leverance_dato in a table, or None when it is empty/absent."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            # mssql-python (what MSSQLResource.get_connection yields) is an
            # ODBC driver and takes ? placeholders — not the %s that pymssql
            # uses. Same convention as sor_register.py.
            cursor.execute(
                "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s "
                "ON s.schema_id = t.schema_id WHERE s.name = ? AND t.name = ?",
                (schema_name, table),
            )
            if not cursor.fetchone()[0]:
                return None
            cursor.execute(f"SELECT MAX([leverance_dato]) FROM [{schema_name}].[{table}]")
            value = cursor.fetchone()[0]
            return str(value) if value else None
        finally:
            cursor.close()


def _recorded_delivery(mssql: MSSQLResource, source_table: str) -> str | None:
    """Newest delivery registered for a table, or None before the first run."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s "
                "ON s.schema_id = t.schema_id WHERE s.name = ? AND t.name = ?",
                ("meta", LEVERANCE_TABLE),
            )
            if not cursor.fetchone()[0]:
                return None
            cursor.execute(
                f"SELECT MAX([leverance_dato]) FROM [meta].[{LEVERANCE_TABLE}] "
                f"WHERE [kildetabel] = ?",
                (source_table,),
            )
            value = cursor.fetchone()[0]
            return str(value) if value else None
        finally:
            cursor.close()


# The register is per (kildetabel, hai_type) — its PK. leverance_dato is part
# of the key too so the history accumulates one row per delivery of each type.
_LEVERANCE_KEYS = ("kildetabel", "hai_type", "leverance_dato")


def record_delivery(
    mssql: MSSQLResource,
    source_table: str,
    hai_type: str,
    leverance_dato: str,
    indlaesningstype: str,
    inserted: int | None = None,
    updated: int | None = None,
    soft_deleted: int | None = None,
) -> None:
    """Register one HAI type's delivery that the archive now stands on.

    Called once per type (see _record_deliveries). Runs after the load has
    committed. A failure here leaves the register behind the data, which makes
    the direction guard permissive rather than wrongly blocking — the safe
    direction for a bookkeeping write. The merge engine reports only table-level
    counts, so the per-type count columns are left NULL; the run totals live in
    the archive asset's metadata.
    """
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            upsert_register(
                cursor,
                LEVERANCE_TABLE,
                _LEVERANCE_KEYS,
                {
                    "kildetabel": source_table,
                    "hai_type": hai_type,
                    "leverance_dato": leverance_dato,
                    "indlaesningstype": indlaesningstype,
                    "raekker_indsat": inserted,
                    "raekker_opdateret": updated,
                    "raekker_soft_slettet": soft_deleted,
                },
            )
            conn.commit()
        finally:
            cursor.close()


def _ext_delivery_dates(mssql: MSSQLResource, schema: HaibaSchema) -> dict[str, str]:
    """Newest leverance_dato per HAI type in the current ext snapshot.

    Keyed by the arc-column type value (agg: 'COHA', not the filename's
    'CDI_COHA') — the vocabulary the register and the per-type checks share.
    One selected file per type means one date per type; MAX is defensive.
    """
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT [{schema.type_column}], MAX([leverance_dato]) "
                f"FROM [ext].[{schema.ext_table}] GROUP BY [{schema.type_column}]"
            )
            return {str(t): str(d) for t, d in cursor.fetchall() if d is not None}
        finally:
            cursor.close()


def _record_deliveries(mssql: MSSQLResource, schema: HaibaSchema, indlaesningstype: str) -> None:
    """Register the current ext delivery of every HAI type it carries."""
    for hai_type, leverance_dato in sorted(_ext_delivery_dates(mssql, schema).items()):
        record_delivery(mssql, schema.ext_table, hai_type, leverance_dato, indlaesningstype)


def guard_delivery_direction(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    schema: HaibaSchema,
    backfill: bool,
) -> tuple[str | None, str | None]:
    """Refuse to merge an older delivery over a newer archive.

    A pin left on the extract is otherwise silently destructive: the normal
    MERGE would regress every changed measure to the older delivery's values
    and soft-delete every row newer than the pin.
    """
    ext_max = _max_delivery(mssql, "ext", schema.ext_table)
    # From the register, not MAX(arc.leverance_dato): that column only advances
    # when a row's values change, so a delivery that revised nothing would leave
    # the archive looking older than it is — and the case tables go weeks without
    # a single change. Falls back to the column on the first run after the
    # register was introduced, when it has no row yet but arc does.
    arc_max = _recorded_delivery(mssql, schema.ext_table) or _max_delivery(
        mssql, "arc", schema.ext_table
    )
    if backfill:
        if ext_max and arc_max and ext_max >= arc_max:
            raise ValueError(
                f"backfill_historical is set, but ext.{schema.ext_table} holds "
                f"delivery {ext_max}, which is not older than the archive's {arc_max}. "
                f"Backfill is only for recovering a window SDS has dropped — using it "
                f"on a current delivery would flag live rows as deleted."
            )
        return ext_max, arc_max
    if ext_max and arc_max and ext_max < arc_max:
        raise ValueError(
            f"ext.{schema.ext_table} holds delivery {ext_max}, older than the "
            f"archive's {arc_max}. Merging it would overwrite current values and "
            f"soft-delete everything newer. If the extract was pinned on purpose, "
            f"run the archive with backfill_historical=true instead."
        )
    return ext_max, arc_max


def capture_agg_revisions(
    context: dg.AssetExecutionContext, mssql: MSSQLResource, schema: HaibaSchema
) -> int:
    """Log SDS' revisions to the current snapshot, returning the row count."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_revision_sql(schema))
            captured = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            conn.commit()
        finally:
            cursor.close()
    if captured:
        context.log.info(
            f"Logged {captured} measure revision(s) from SDS to "
            f"arc.{REVISION_TABLE} for {schema.ext_table}"
        )
    return captured


def _run_archive(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    config: RunModeConfig,
    schema: HaibaSchema,
    merge_config: ArchiveMergeConfig,
) -> dg.MaterializeResult:
    backfill = getattr(config, "backfill_historical", False)

    manifest = read_manifest(mssql, schema)
    if not manifest:
        context.log.info(
            f"arc.{schema.ext_table}: empty manifest — the extract loaded "
            f"nothing this run, so there is nothing to merge"
        )
        # The run log still records the skipped run; the delivery checkpoints
        # are deliberately not advanced — nothing was merged.
        dfm = DfmManager(mssql, context)
        with dfm.connection():
            run_ctx = dfm.start_dfm_run(
                "arc", schema.ext_table, dagster_run_id=context.run_id, mode=config.mode
            )
            dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
        return dg.MaterializeResult(
            metadata={
                "besluttet": dg.MetadataValue.text("sprunget over"),
                "aarsag": dg.MetadataValue.text("tomt manifest — ingen ny leverance"),
                "row_count": dg.MetadataValue.int(0),
                "mode": dg.MetadataValue.text(config.mode),
                "source_table": dg.MetadataValue.text(f"ext.{schema.ext_table}"),
                "target_table": dg.MetadataValue.text(f"arc.{schema.ext_table}"),
            }
        )

    ext_max, arc_max = guard_delivery_direction(context, mssql, schema, backfill)

    if backfill:
        return _run_backfill(context, mssql, schema, ext_max, arc_max)

    revisions = capture_agg_revisions(context, mssql, schema) if schema.name in _AGG_MEASURES else 0

    result = execute_archive_merge(context, mssql, merge_config, config)

    context.log.info(
        f"MERGEd into arc.{schema.ext_table} — inserted={result.rows_inserted}, "
        f"updated={result.rows_updated}; mode={config.mode}"
    )

    if ext_max:
        # Per HAI type, from ext — a per-table watermark cannot see one type
        # running on an old delivery, and the run's table-level counts are on
        # the metadata below.
        _record_deliveries(mssql, schema, "merge")

    # Honours the dry-run contract: advance_checkpoint=False must leave the
    # delivery checkpoints unmoved, or the next extract skips this delivery.
    stamped: list[str] = []
    if config.should_advance_checkpoint:
        stamped = advance_delivery_checkpoints(context, mssql, manifest, config.mode)
    else:
        context.log.info("advance_checkpoint=False — delivery checkpoints left unmoved")

    metadata = build_archive_metadata(
        result,
        config,
        source_table=f"ext.{schema.ext_table}",
        target_table=f"arc.{schema.ext_table}",
    )
    metadata["besluttet"] = dg.MetadataValue.text("merget")
    metadata["sds_revisions_logged"] = dg.MetadataValue.int(revisions)
    metadata["leverance_dato"] = dg.MetadataValue.text(ext_max or "-")
    metadata["forrige_leverance"] = dg.MetadataValue.text(arc_max or "(tom)")
    metadata["stemplede_enheder"] = dg.MetadataValue.json(stamped or ["ingen"])
    return dg.MaterializeResult(metadata=metadata)


def _run_backfill(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    schema: HaibaSchema,
    ext_max: str | None,
    arc_max: str | None,
) -> dg.MaterializeResult:
    """Insert historical rows SDS no longer publishes, flagged dw_is_deleted."""
    context.log.warning(
        f"BACKFILL of arc.{schema.ext_table} from delivery {ext_max} (archive is at "
        f"{arc_max}). Rows are inserted with dw_is_deleted = 1; nothing existing is "
        f"updated or deleted."
    )
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_backfill_sql(schema))
            inserted = cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
            conn.commit()
        finally:
            cursor.close()

    # Recorded for the history, not as a watermark: the guard reads MAX, and a
    # backfill date is older than what is already there, so it cannot move it.
    # The delivery checkpoints are not touched either — regressing one to the
    # pinned delivery would make the next run re-ingest the current one.
    if ext_max:
        _record_deliveries(mssql, schema, "backfill")

    context.log.info(f"Backfilled {inserted} historical row(s) into arc.{schema.ext_table}")
    return dg.MaterializeResult(
        metadata={
            "mode": dg.MetadataValue.text("backfill_historical"),
            "rows_backfilled": dg.MetadataValue.int(inserted),
            "source_delivery": dg.MetadataValue.text(ext_max or "-"),
            "archive_delivery": dg.MetadataValue.text(arc_max or "-"),
            "target_table": dg.MetadataValue.text(f"arc.{schema.ext_table}"),
        }
    )


def _build_asset(schema: HaibaSchema, merge_config: ArchiveMergeConfig):
    logs_revisions = schema.name in _AGG_MEASURES

    @dg.asset(
        name=f"arc_{schema.ext_table}",
        group_name="haiba_archive",
        compute_kind="mssql",
        deps=[f"ext_{schema.ext_table}"],
        tags={"domain": "haiba", "layer": "archive", "source": "haiba"},
        description=(
            f"MERGE ext.{schema.ext_table} into arc.{schema.ext_table}, or declare "
            f"a no-op when the extract's manifest is empty. Mirrors "
            f"the newest delivery: rows SDS no longer publishes are soft-deleted."
            + (
                " Measure changes are logged to arc.haiba_agg_revision first."
                if logs_revisions
                else ""
            )
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: HaibaArchiveConfig,
    ) -> dg.MaterializeResult:
        return _run_archive(context, mssql_target_staging, config, schema, merge_config)

    return _asset


arc_haiba_agg_infektion = _build_asset(AGG_INFEKTION, AGG_INFEKTION_CONFIG)
arc_haiba_agg_ortopaedi = _build_asset(AGG_ORTOPAEDI, AGG_ORTOPAEDI_CONFIG)
arc_haiba_cases_infektion = _build_asset(CASES_INFEKTION, CASES_INFEKTION_CONFIG)
arc_haiba_cases_ssi = _build_asset(CASES_SSI, CASES_SSI_CONFIG)

haiba_archive_assets = [
    arc_haiba_agg_infektion,
    arc_haiba_agg_ortopaedi,
    arc_haiba_cases_infektion,
    arc_haiba_cases_ssi,
]

__all__ = [
    "AGG_INFEKTION_CONFIG",
    "HaibaArchiveConfig",
    "HaibaDeliveryManifest",
    "AGG_ORTOPAEDI_CONFIG",
    "CASES_INFEKTION_CONFIG",
    "CASES_SSI_CONFIG",
    "advance_delivery_checkpoints",
    "read_manifest",
    "arc_haiba_agg_infektion",
    "arc_haiba_agg_ortopaedi",
    "arc_haiba_cases_infektion",
    "arc_haiba_cases_ssi",
    "LEVERANCE_TABLE",
    "build_backfill_sql",
    "build_config",
    "guard_delivery_direction",
    "build_revision_sql",
    "capture_agg_revisions",
    "record_delivery",
    "haiba_archive_assets",
]
