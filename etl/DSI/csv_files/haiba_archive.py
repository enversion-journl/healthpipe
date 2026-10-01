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
"""

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.resources import MSSQLResource
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    DELIVERY_COLUMN,
    HaibaSchema,
)
from pydantic import Field


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


LEVERANCE_COLUMNS = (
    "kildetabel",
    "leverance_dato",
    "indlaesningstype",
    "raekker_indsat",
    "raekker_opdateret",
    "raekker_soft_slettet",
)


def build_leverance_sql() -> str:
    """Upsert of one register row, keyed on (kildetabel, leverance_dato).

    An upsert rather than an insert: re-running the same delivery must refresh
    its counts, not add a second row for it.
    """
    placeholders = ", ".join("?" * len(LEVERANCE_COLUMNS))
    columns = ", ".join(f"[{c}]" for c in LEVERANCE_COLUMNS)
    src_values = ", ".join(f"src.[{c}]" for c in LEVERANCE_COLUMNS)
    updates = "\n    ".join(
        f"tgt.[{c}] = src.[{c}],"
        for c in LEVERANCE_COLUMNS
        if c not in ("kildetabel", "leverance_dato")
    )
    return f"""
MERGE [meta].[{LEVERANCE_TABLE}] AS tgt
USING (VALUES ({placeholders})) AS src ({columns})
    ON tgt.[kildetabel] = src.[kildetabel]
   AND tgt.[leverance_dato] = src.[leverance_dato]
WHEN MATCHED THEN UPDATE SET
    {updates}
    tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED THEN INSERT ({columns})
    VALUES ({src_values});
"""


def record_delivery(
    mssql: MSSQLResource,
    source_table: str,
    leverance_dato: str,
    indlaesningstype: str,
    inserted: int | None = None,
    updated: int | None = None,
    soft_deleted: int | None = None,
) -> None:
    """Register the delivery the archive now stands on.

    Runs after the load has committed. A failure here leaves the register behind
    the data, which makes the direction guard permissive rather than wrongly
    blocking — the safe direction for a bookkeeping write.
    """
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                build_leverance_sql(),
                (
                    source_table,
                    leverance_dato,
                    indlaesningstype,
                    inserted,
                    updated,
                    soft_deleted,
                ),
            )
            conn.commit()
        finally:
            cursor.close()


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
        record_delivery(
            mssql,
            schema.ext_table,
            ext_max,
            "merge",
            inserted=result.rows_inserted,
            updated=result.rows_updated,
            soft_deleted=result.rows_soft_deleted,
        )

    metadata = build_archive_metadata(
        result,
        config,
        source_table=f"ext.{schema.ext_table}",
        target_table=f"arc.{schema.ext_table}",
    )
    metadata["sds_revisions_logged"] = dg.MetadataValue.int(revisions)
    metadata["leverance_dato"] = dg.MetadataValue.text(ext_max or "-")
    metadata["forrige_leverance"] = dg.MetadataValue.text(arc_max or "(tom)")
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
    if ext_max:
        record_delivery(mssql, schema.ext_table, ext_max, "backfill", inserted=inserted)

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
            f"MERGE ext.{schema.ext_table} into arc.{schema.ext_table}. Mirrors "
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
    "AGG_ORTOPAEDI_CONFIG",
    "CASES_INFEKTION_CONFIG",
    "CASES_SSI_CONFIG",
    "arc_haiba_agg_infektion",
    "arc_haiba_agg_ortopaedi",
    "arc_haiba_cases_infektion",
    "arc_haiba_cases_ssi",
    "LEVERANCE_COLUMNS",
    "LEVERANCE_TABLE",
    "build_backfill_sql",
    "build_config",
    "build_leverance_sql",
    "guard_delivery_direction",
    "build_revision_sql",
    "capture_agg_revisions",
    "record_delivery",
    "haiba_archive_assets",
]
