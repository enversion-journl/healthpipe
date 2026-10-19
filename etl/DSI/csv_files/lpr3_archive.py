# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""LPR3 archive lifecycle and pre-merge guards.

Every merge is bound to the current run's manifest and landing lease. Guards
run before either the shared bulk-first-load path or the shared MERGE path.
Source preparation supplies typed, resolved rows; no technical source SK is
allowed in the archive contract. The delivery checkpoint belongs to the final
gate, never to an individual table merge.
"""

import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime

import dagster as dg
from core.archive_merge import execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.dfm_manager import DfmManager
from core.etl_thresholds import check_soft_delete_threshold
from core.resources import MSSQLResource
from DSI.connectors.delivery_detection import (
    DELIVERY_SCHEMA,
    DELIVERY_TARGET_TYPE,
    delivery_identity,
)
from DSI.csv_files._lpr3_canonical import canonical_select, conflicting_versions_sql
from DSI.csv_files._lpr3_keys import (
    dimension_source,
    group_identity_sql,
    identity_table,
    relational_identity_sql,
    resolved_view_sql,
    result_identity_sql,
    simple_identity_select,
)
from DSI.csv_files._lpr3_run import owned_landing
from DSI.csv_files.lpr3 import DELIVERY_UNIT, MANIFEST_TABLE
from DSI.csv_files.lpr3_datasets import (
    ARCHIVE_DATASETS,
    ARCHIVE_SOURCES,
    DELIVERY_COLUMNS,
    SOURCES_BY_PREFIX,
    key_source_dependencies,
)
from pydantic import Field


@dataclass(frozen=True)
class SourceDelivery:
    source_prefix: str
    run_id: str
    filename: str
    cadence: str
    timestamp: datetime
    cutoff: date
    rows: int
    source_rows: int
    source_bytes: int
    audit_min: int | None
    audit_distinct: int
    checkpoint: str
    advance_checkpoint: bool


def read_manifest(conn, run_id):
    """Empty means no-op; missing table, mixed identities or stale run fail closed."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT OBJECT_ID(N'ext.{MANIFEST_TABLE}',N'U')")
        if cursor.fetchone()[0] is None:
            raise ValueError("LPR3 archive requires an extract manifest")
        cursor.execute(
            f"SELECT kildetabel,run_id,filnavn,kadence,leverance_tidsstempel,leverance_dato,"
            f"raekker_ext,raekker_kilde,bytes_kilde,audit_insert_min,audit_insert_distinct,"
            f"checkpoint_identitet,ryk_checkpoint,checkpoint_enhed,ext_tabel FROM [ext].[{MANIFEST_TABLE}]"
        )
        records = cursor.fetchall()
    finally:
        cursor.close()
    result = {}
    identities = set()
    for row in records:
        item = SourceDelivery(*row[:13])
        source = SOURCES_BY_PREFIX.get(item.source_prefix)
        if (
            source is None
            or item.source_prefix in result
            or item.run_id != run_id
            or row[13] != DELIVERY_UNIT
            or row[14] != source.ext_table
            or item.rows != item.source_rows
            or min(item.rows, item.source_bytes, item.audit_distinct) < 0
            or item.checkpoint != delivery_identity(item.timestamp)
        ):
            raise ValueError("Invalid or stale LPR3 manifest; archive remains untouched")
        identities.add(
            (
                item.filename,
                item.cadence,
                item.timestamp,
                item.cutoff,
                item.checkpoint,
                item.advance_checkpoint,
            )
        )
        result[item.source_prefix] = item
    if len(identities) > 1:
        raise ValueError("LPR3 manifest contains multiple delivery identities")
    return result


def missing_sources(dataset, manifest):
    """Include lookup dependencies: a missing or empty dimension must not null old arc FKs."""
    return tuple(
        sorted(
            prefix
            for prefix in dataset.required_sources
            if prefix not in manifest or (prefix.startswith("Dim") and manifest[prefix].rows == 0)
        )
    )


def build_merge_config(dataset):
    non_key = (
        tuple(c for c in dataset.arc_columns if c not in dataset.merge_keys) + DELIVERY_COLUMNS
    )
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=dataset.source_table,
        target_schema="arc",
        target_table=dataset.arc_table,
        merge_keys=dataset.merge_keys,
        non_key_columns=non_key,
        change_detect_columns=dataset.change_detect_columns,
        update_only_on_change=True,
        detect_deletes=True,
        upstream_is_full_load=True,
        enable_bulk_insert_on_first_load=True,
        dedup_source_on_keys=False,
    )


def scalar(conn, sql, parameters=()):
    cursor = conn.cursor()
    try:
        cursor.execute(sql, parameters)
        while cursor.description is None:
            if not cursor.nextset():
                raise ValueError("LPR3 scalar query returned no result set")
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def _own_sk(source):
    entity = source.arc_table.removeprefix("lpr3_")
    return next(c.name for c in source.columns if c.name.casefold() == f"dw_sk_{entity}")


def guard_source_counts(conn, dataset, manifest):
    """Exact file totals, unique pair IDs and two-way pair parity before joins."""
    for prefix in dataset.sources:
        source = SOURCES_BY_PREFIX[prefix]
        actual = scalar(conn, f"SELECT COUNT_BIG(*) FROM [ext].[{source.ext_table}]")
        if actual != manifest[prefix].rows:
            raise ValueError(f"LPR3 source row count changed after extract: {prefix}")
    if len(dataset.sources) != 2:
        return
    dim = dimension_source(dataset)
    fact = next(SOURCES_BY_PREFIX[p] for p in dataset.sources if not p.startswith("Dim"))
    for source in (dim, fact):
        key = _own_sk(source)
        count = scalar(
            conn,
            f"SELECT COUNT_BIG(*) FROM (SELECT [{key}] FROM [ext].[{source.ext_table}] "
            f"GROUP BY [{key}] HAVING COUNT_BIG(*)<>1 OR NULLIF([{key}],'') IS NULL) d",
        )
        if count:
            raise ValueError(
                f"LPR3 pair has {count} invalid/duplicate source keys: {source.source_prefix}"
            )
    # Measured exception: HelbredsForloeb has exactly one unknown dimension
    # row, SK=-1, without a fact. No other unmatched entity is permitted.
    omit_unknown = dataset.entity == "helbredsforloeb"
    if omit_unknown and scalar(
        conn, f"SELECT COUNT_BIG(*) FROM ext.[{fact.ext_table}] WHERE [{_own_sk(fact)}]=N'-1'"
    ):
        raise ValueError("LPR3 HelbredsForloeb unexpectedly supplies an unknown fact row")
    selections = [
        f"SELECT [{_own_sk(s)}] FROM [ext].[{s.ext_table}]"
        + (f" WHERE [{_own_sk(s)}]<>'-1'" if omit_unknown else "")
        for s in (dim, fact)
    ]
    for left, right in (selections, selections[::-1]):
        missing = scalar(conn, f"SELECT COUNT_BIG(*) FROM ({left} EXCEPT {right}) d")
        if missing:
            raise ValueError(
                f"LPR3 Dim/fct pair parity failed for {dataset.arc_table}: {missing} unmatched keys"
            )


def guard_resolved_rows(conn, dataset, manifest):
    primary = next((p for p in dataset.sources if not p.startswith("Dim")), dataset.sources[0])
    if dataset.entity == "helbredsforloeb":
        primary = next(p for p in dataset.sources if p.startswith("Dim"))
    expected = manifest[primary].rows
    expression = (
        "SUM([antal_kilderaekker])"
        if "antal_kilderaekker" in dataset.arc_columns
        else "COUNT_BIG(*)"
    )
    represented = scalar(
        conn, f"SELECT COALESCE({expression},0) FROM [ext].[{dataset.source_table}]"
    )
    if represented != expected:
        raise ValueError(
            f"LPR3 resolved row parity failed for {dataset.arc_table}: expected {expected}, represented {represented}"
        )
    keys = ",".join(f"[{key}]" for key in dataset.merge_keys)
    nulls = " OR ".join(f"[{key}] IS NULL" for key in dataset.merge_keys)
    invalid = scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM (SELECT {keys} FROM [ext].[{dataset.source_table}] "
        f"GROUP BY {keys} HAVING COUNT_BIG(*)>1 OR {nulls}) d",
    )
    if invalid:
        raise ValueError(
            f"LPR3 resolved keys are not unique/non-null for {dataset.arc_table}: {invalid} groups"
        )
    return expected


def source_index_sql(dataset):
    keys = ",".join(f"[{key}]" for key in dataset.merge_keys)
    return f"CREATE UNIQUE CLUSTERED INDEX [UX_ext_{dataset.source_table}] ON [ext].[{dataset.source_table}] ({keys})"


def guard_source_index(conn, dataset):
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT c.name FROM sys.indexes i JOIN sys.index_columns ic "
            "ON ic.object_id=i.object_id AND ic.index_id=i.index_id "
            "JOIN sys.columns c ON c.object_id=ic.object_id AND c.column_id=ic.column_id "
            "WHERE i.object_id=OBJECT_ID(?) AND i.name=? AND i.is_unique=1 "
            "AND i.is_disabled=0 AND i.has_filter=0 AND ic.key_ordinal>0 ORDER BY ic.key_ordinal",
            (f"ext.{dataset.source_table}", f"UX_ext_{dataset.source_table}"),
        )
        columns = tuple(row[0] for row in cursor.fetchall())
    finally:
        cursor.close()
    if columns != dataset.merge_keys:
        raise ValueError("LPR3 bulk-first-load requires the verified unique source index")


def guard_delivery_direction(conn, dataset, delivery):
    previous = scalar(
        conn,
        "SELECT MAX(leverance_dato) FROM meta.LPR3Leverance WHERE arc_tabel=? AND status='archived'",
        (dataset.arc_table,),
    )
    if previous is not None and delivery.cutoff < previous:
        raise ValueError("LPR3 cannot merge an older data cutoff over an archived delivery")


class LPR3ArchiveConfig(dg.Config):
    soft_delete_threshold: int | None = Field(
        default=None,
        description="Allow up to this many soft-deletes in this archive table for this run "
        "only, instead of the ratio cap. Also read by lpr3_archive_noegler's pre-merge check.",
    )


def check_soft_deletes(dataset, expected, live, override=None, context=None):
    """Ratio cap with a 1,000-row floor; an explicit run override replaces it."""
    asset = f"arc_{dataset.arc_table}"
    if override is not None:
        check_soft_delete_threshold(asset, expected, context, override=override)
        return
    cap = (
        int(live * dataset.soft_delete_ratio)
        if live >= dataset.ratio_min_live_rows
        else dataset.ratio_min_live_rows
    )
    if context:
        context.log.info(
            f"{dataset.arc_table}: expected soft-deletes={expected}, live={live}, allowed={cap}, ratio={dataset.soft_delete_ratio}"
        )
    if expected > cap:
        raise dg.Failure(
            description=(
                f"{asset}: {expected} expected soft-deletes of {live} live rows exceeds "
                f"threshold of {cap} (ratio {dataset.soft_delete_ratio}). If this is explained, "
                f"re-run with ops.{asset}.config.soft_delete_threshold set above it."
            ),
            metadata={"expected_soft_deletes": expected, "live": live, "threshold": cap},
        )


def guard_soft_deletes(conn, dataset, context=None, override=None, source_table=None):
    source = f"[ext].[{source_table or dataset.source_table}]"
    target = f"[arc].[{dataset.arc_table}]"
    on = " AND ".join(f"src.[{key}]=tgt.[{key}]" for key in dataset.merge_keys)
    live = scalar(conn, f"SELECT COUNT_BIG(*) FROM {target} WHERE dw_is_deleted=0")
    expected = scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM {target} tgt WHERE tgt.dw_is_deleted=0 AND NOT EXISTS (SELECT 1 FROM {source} src WHERE {on})",
    )
    check_soft_deletes(dataset, expected, live, override, context)
    return live, expected


def soft_delete_overrides(context):
    ops = (context.run.run_config or {}).get("ops", {})
    return {
        dataset.arc_table: ops.get(f"arc_{dataset.arc_table}", {})
        .get("config", {})
        .get("soft_delete_threshold")
        for dataset in ARCHIVE_DATASETS
    }


def precheck_soft_deletes(conn, context, ready, overrides):
    """Check every identity-keyed table before the first merge, so no table merges alone."""
    failures = []
    for dataset in ARCHIVE_DATASETS:
        if dataset.entity not in ready:
            continue
        try:
            guard_soft_deletes(
                conn,
                dataset,
                context,
                overrides.get(dataset.arc_table),
                source_table=identity_table(dataset.entity),
            )
        except dg.Failure as failure:
            failures.append(failure.description)
    if failures:
        raise dg.Failure(
            description="LPR3 soft-delete guard failed before any archive merge:\n"
            + "\n".join(failures)
        )


def refresh_surrogates_sql(dataset):
    """Refresh source audit/DW_EK labels without falsely changing content timestamps."""
    columns = dataset.surrogate_columns
    if not columns:
        return None
    on = " AND ".join(f"src.[{key}]=tgt.[{key}]" for key in dataset.merge_keys)
    changes = " OR ".join(
        f"ISNULL(NULLIF(tgt.[{c}],src.[{c}]),NULLIF(src.[{c}],tgt.[{c}])) IS NOT NULL"
        for c in columns
    )
    assignments = ",".join(f"tgt.[{c}]=src.[{c}]" for c in columns)
    return f"UPDATE tgt SET {assignments} FROM [arc].[{dataset.arc_table}] tgt JOIN [ext].[{dataset.source_table}] src ON {on} WHERE tgt.dw_is_deleted=0 AND ({changes})"


def assert_gate_complete(conn, manifest, run_id):
    """No checkpoint if a table is missing or any merge/audit belongs to an old run."""
    if not manifest:
        return
    if set(manifest) != set(SOURCES_BY_PREFIX):
        raise dg.Failure("LPR3 checkpoint not advanced: delivery is missing source tables")
    delivery = next(iter(manifest.values()))
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT kildetabel,arc_tabel FROM meta.LPR3Leverance "
            "WHERE run_id=? AND leverance_tidsstempel=? AND status='archived'",
            (run_id, delivery.timestamp),
        )
        completed = dict(cursor.fetchall())
    finally:
        cursor.close()
    expected = {prefix: table for table, sources in ARCHIVE_SOURCES.items() for prefix in sources}
    if completed != expected:
        raise dg.Failure(
            "LPR3 checkpoint not advanced: not all 38 archive merges completed in this run"
        )


def reload_alarm(conn, delivery):
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT TOP (1) audit_insert_min,audit_insert_distinct FROM meta.LPR3Leverance "
            "WHERE kildetabel=? AND leverance_tidsstempel<? AND status='archived' "
            "ORDER BY leverance_tidsstempel DESC",
            (delivery.source_prefix, delivery.timestamp),
        )
        previous = cursor.fetchone()
    finally:
        cursor.close()
    return bool(
        previous
        and (
            (
                previous[0] is not None
                and delivery.audit_min is not None
                and delivery.audit_min < previous[0]
            )
            or delivery.audit_distinct < previous[1]
        )
    )


def record_delivery(
    conn,
    dataset,
    manifest,
    *,
    result=None,
    full_load=False,
    refreshed=0,
    seconds=None,
    fallback_rows=None,
    unresolved_references=None,
):
    """Each source's state is durable; only a successful merge records archived."""
    # The shared merge's row_count is actions, not the active archive size.
    # A replay has zero actions but must retain its actual archive row total.
    archive_rows = (
        scalar(conn, f"SELECT COUNT_BIG(*) FROM [arc].[{dataset.arc_table}] WHERE dw_is_deleted=0")
        if result is not None
        else None
    )
    alarms = []
    for prefix in dataset.sources:
        if prefix not in manifest:
            continue
        delivery = manifest[prefix]
        alarm = reload_alarm(conn, delivery)
        if alarm:
            alarms.append(prefix)
        values = {
            "kildetabel": prefix,
            "leverance_tidsstempel": delivery.timestamp,
            "run_id": delivery.run_id,
            "kadence": delivery.cadence,
            "leverance_dato": delivery.cutoff,
            "arc_tabel": dataset.arc_table,
            "indlaesningstype": "full" if full_load else "delta",
            "merge_metode": ("bulk" if full_load else "merge") if result else None,
            "status": "archived" if result else "extracted",
            "raekker_ext": delivery.rows,
            "raekker_kilde": delivery.source_rows,
            "bytes_kilde": delivery.source_bytes,
            "raekker_arc": archive_rows,
            "raekker_indsat": result.rows_inserted if result else None,
            "raekker_opdateret": result.rows_updated if result else None,
            "raekker_soft_slettet": result.rows_soft_deleted if result else None,
            "surrogat_id_opdateret": refreshed if result else None,
            "noegle_fallback_raekker": fallback_rows,
            "referencer_uoploest": unresolved_references,
            "audit_insert_min": delivery.audit_min,
            "audit_insert_distinct": delivery.audit_distinct,
            "genindlaesning_alarm": int(alarm),
            "sekunder": seconds,
        }
        columns = list(values)
        cursor = conn.cursor()
        try:
            cursor.execute(
                "MERGE meta.LPR3Leverance AS tgt USING (SELECT "
                + ",".join(f"? AS [{c}]" for c in columns)
                + ") AS src "
                "ON tgt.kildetabel=src.kildetabel AND tgt.leverance_tidsstempel=src.leverance_tidsstempel "
                # A failed replay keeps the committed delivery's run_id, cutoff and audit.
                + (
                    "WHEN MATCHED THEN UPDATE SET "
                    if result
                    else "WHEN MATCHED AND tgt.status<>'archived' THEN UPDATE SET "
                )
                + ",".join(f"tgt.[{c}]=src.[{c}]" for c in columns[2:])
                + ",tgt.dw_timestamp_update=CAST(SYSDATETIME() AS DATETIME2(0)) "
                "WHEN NOT MATCHED THEN INSERT (" + ",".join(f"[{c}]" for c in columns) + ") "
                "VALUES (" + ",".join(f"src.[{c}]" for c in columns) + ");",
                tuple(values.values()),
            )
            conn.commit()
        finally:
            cursor.close()
    return tuple(alarms)


class _SafeDatabase:
    """MSSQLResource stand-in for the shared merge that replaces raw driver errors with value-free ones."""

    def __init__(self, resource):
        self.resource = resource

    @contextmanager
    def get_connection(self, **kwargs):
        try:
            with self.resource.get_connection(**kwargs) as conn:
                yield conn
        except Exception:
            raise ValueError("LPR3 database operation failed; source values suppressed") from None


def execute_safe(conn, sql):
    """One complete SQL batch, with a driver-error boundary before Dagster logging."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        # SQL Server can defer an error in a later statement until nextset().
        # Never commit a partially executed identity-building batch as success.
        while cursor.nextset():
            pass
        conn.commit()
    except Exception:
        conn.rollback()
        raise ValueError("LPR3 SQL batch failed; source values suppressed") from None
    finally:
        cursor.close()


def build_identity_map(conn, dataset):
    table = identity_table(dataset.entity)
    execute_safe(conn, f"DROP TABLE IF EXISTS [ext].[{table}]")
    method = dataset.key_policy.method
    if method in ("forloeb_roots", "first_contact"):
        batches = relational_identity_sql(dataset)
    elif method == "code_set":
        batches = group_identity_sql(dataset)
    else:
        select = (
            result_identity_sql(dataset)
            if method == "report_number"
            else simple_identity_select(dataset)
        )
        if dataset.canonical_versions:
            select = canonical_select(dataset, select)
        batches = [f"SELECT * INTO [ext].[{table}] FROM ({select}) identity_source"]
    for sql in batches:
        execute_safe(conn, sql)
    invalid = scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM (SELECT source_sk FROM [ext].[{table}] GROUP BY source_sk HAVING COUNT_BIG(*)>1 OR source_sk IS NULL) d",
    )
    if invalid:
        raise ValueError(
            f"LPR3 identity map {dataset.entity} has {invalid} duplicate/null source keys"
        )
    execute_safe(conn, f"CREATE UNIQUE CLUSTERED INDEX [UX_{table}] ON [ext].[{table}](source_sk)")
    keys = ",".join(f"[{c}]" for c in dataset.merge_keys)
    nulls = " OR ".join(f"[{c}] IS NULL" for c in dataset.merge_keys)
    duplicate_condition = "" if dataset.canonical_versions else "COUNT_BIG(*)>1 OR "
    invalid = scalar(
        conn,
        f"SELECT COUNT_BIG(*) FROM (SELECT {keys} FROM [ext].[{table}] GROUP BY {keys} HAVING {duplicate_condition}{nulls}) d",
    )
    if invalid:
        raise ValueError(
            f"LPR3 identity map {dataset.entity} has {invalid} duplicate/null business keys; archive not merged"
        )
    if dataset.canonical_versions:
        conflicts = scalar(conn, conflicting_versions_sql(dataset, table))
        if conflicts:
            raise ValueError(
                f"LPR3 {dataset.entity} has {conflicts} nonidentical canonical versions; archive not merged"
            )
        execute_safe(
            conn,
            f"CREATE INDEX [IX_{table}_canonical] ON ext.[{table}] (canonical_source_sk)",
        )
    rows = scalar(conn, f"SELECT COUNT_BIG(*) FROM [ext].[{table}]")
    if dataset.entity in ("helbredsforloeb", "sygehusophold"):
        denominator = scalar(conn, f"SELECT COUNT_BIG(*) FROM [ext].[{table}] WHERE source_sk<>-1")
        direct = scalar(
            conn,
            f"SELECT COUNT_BIG([{dataset.entity}_uuid]) FROM [ext].[{table}] WHERE source_sk<>-1",
        )
        if direct * 1000 < denominator * 999:
            raise ValueError(
                f"LPR3 {dataset.entity} UUID coverage below 99.9%: {direct} of {denominator}; archive not merged"
            )
    return rows


def prepare_identities(conn, context, manifest):
    for dataset in ARCHIVE_DATASETS:
        for prefix in dataset.sources:
            if prefix in manifest:
                guard_delivery_direction(conn, dataset, manifest[prefix])
        record_delivery(conn, dataset, manifest)
        if all(p in manifest for p in dataset.sources):
            guard_source_counts(conn, dataset, manifest)
    # All A identities precede their derived B identities. FK projection comes
    # later, so cycles such as ProcedureRegistrering <-> Betalingsoplysning do
    # not require joining current/archive versions or reordering deliveries.
    priority = {"code_set": 1, "forloeb_roots": 1, "first_contact": 1, "report_number": 1}
    ready = {}
    for dataset in sorted(
        ARCHIVE_DATASETS, key=lambda d: (priority.get(d.key_policy.method, 0), d.arc_table)
    ):
        if dataset.key_policy.key_class == "bridge":
            continue
        missing = set(key_source_dependencies(dataset.entity)) - set(manifest)
        if missing:
            context.log.warning(
                f"LPR3 skips identity {dataset.entity}: missing sources {sorted(missing)}"
            )
            continue
        start = time.monotonic()
        ready[dataset.entity] = build_identity_map(conn, dataset)
        context.log.info(
            f"LPR3 identity {dataset.entity}: {ready[dataset.entity]} rows in {time.monotonic()-start:.3f}s"
        )
        if dataset.entity == "helbredsforloeb":
            mismatches = scalar(
                conn,
                "SELECT COUNT_BIG(*) FROM ext.lpr3_fctbridgeforloebsrelation b "
                "JOIN ext.lpr3_fcthelbredsforloeb f ON b.DW_SK_Helbredsforloeb=f.DW_SK_Helbredsforloeb "
                "JOIN ext.lpr3_forloeb_identitet r ON r.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(b.DW_SK_Forloeb,N'')),-1) "
                "JOIN ext.lpr3_forloeb_identitet p ON p.source_sk=NULLIF(CONVERT(BIGINT,NULLIF(f.DW_SK_Forloeb_Foerste,N'')),-1) "
                "WHERE b.DW_SK_Forloeb_Forrige=N'-1' AND r.forloeb_uuid<>p.forloeb_uuid",
            )
            context.add_output_metadata({"helbredsforloeb_root_first_disagreements": mismatches})
            if mismatches:
                context.log.warning(
                    f"LPR3 helbredsforloeb: {mismatches} root/first UUID disagreements; resolved root is authoritative"
                )
    return ready


@dg.asset(
    group_name="lpr3_archive",
    compute_kind="mssql",
    deps=["ext_lpr3"],
    tags={"domain": "lpr3", "layer": "archive", "source": "lpr3"},
    description=(
        "Build the run's private ext identity tables that map SDS source keys to stable "
        "LPR3 archive keys, then check every identity-keyed table's expected soft-deletes "
        "before any archive merge."
    ),
)
def lpr3_archive_noegler(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dict:
    with mssql_target_staging.get_connection() as conn:
        with owned_landing(conn, context.run_id):
            manifest = read_manifest(conn, context.run_id)
            if not manifest:
                return {}
            ready = prepare_identities(conn, context, manifest)
            precheck_soft_deletes(conn, context, ready, soft_delete_overrides(context))
            return ready


def _run_archive(context, resource, dataset, soft_delete_threshold=None):
    started = time.monotonic()
    with resource.get_connection() as conn:
        with owned_landing(conn, context.run_id):
            manifest = read_manifest(conn, context.run_id)
            if not manifest:
                return dg.MaterializeResult(
                    metadata={
                        "no_op": True,
                        "rows_inserted": 0,
                        "rows_updated": 0,
                        "rows_soft_deleted": 0,
                    }
                )
            missing = missing_sources(dataset, manifest)
            if missing:
                context.log.warning(
                    f"LPR3 merge skipped for {dataset.arc_table}: missing {missing}"
                )
                return dg.MaterializeResult(metadata={"skipped_missing_sources": list(missing)})
            guard_delivery_direction(conn, dataset, manifest[dataset.sources[0]])
            guard_source_counts(conn, dataset, manifest)
            execute_safe(conn, resolved_view_sql(dataset))
            execute_safe(
                conn,
                f"DROP TABLE IF EXISTS [ext].[{dataset.source_table}]; SELECT * INTO [ext].[{dataset.source_table}] FROM [ext].[v_{dataset.arc_table}]",
            )
            represented = guard_resolved_rows(conn, dataset, manifest)
            execute_safe(conn, source_index_sql(dataset))
            guard_source_index(conn, dataset)
            guard_soft_deletes(conn, dataset, context, soft_delete_threshold)
            reserved_bytes = scalar(
                conn,
                "SELECT COALESCE(SUM(p.reserved_page_count),0)*CONVERT(BIGINT,8192) "
                "FROM sys.dm_db_partition_stats p JOIN sys.tables t ON t.object_id=p.object_id "
                "JOIN sys.schemas s ON s.schema_id=t.schema_id "
                "WHERE s.name IN ('ext','arc') AND t.name LIKE 'lpr3[_]%'",
            )
            full_load = (
                scalar(
                    conn,
                    f"SELECT CASE WHEN EXISTS (SELECT 1 FROM [arc].[{dataset.arc_table}]) THEN 0 ELSE 1 END",
                )
                == 1
            )
            record_delivery(conn, dataset, manifest, full_load=full_load)
            result = execute_archive_merge(
                context,
                _SafeDatabase(resource),
                build_merge_config(dataset),
                RunModeConfig(advance_checkpoint=False),
            )
            refresh = refresh_surrogates_sql(dataset)
            refreshed = 0
            if refresh:
                # @@ROWCOUNT is read on the same statement/session.
                refreshed = scalar(conn, refresh + "; SELECT CONVERT(BIGINT,@@ROWCOUNT)")
                conn.commit()
            key_methods = {}
            if "noeglemetode" in dataset.arc_columns:
                cursor = conn.cursor()
                try:
                    cursor.execute(
                        f"SELECT noeglemetode,COUNT_BIG(*) FROM [ext].[{dataset.source_table}] GROUP BY noeglemetode"
                    )
                    key_methods = {str(method): int(count) for method, count in cursor.fetchall()}
                finally:
                    cursor.close()
            fallback = sum(
                key_methods.get(method, 0)
                for method in (
                    "technical_unresolved",
                    "technical_collision",
                    "technical_reference",
                    "boundary_set",
                    "contact_set",
                )
            )
            source_weight = (
                "antal_kilderaekker" if "antal_kilderaekker" in dataset.arc_columns else "1"
            )
            unresolved = scalar(
                conn,
                f"SELECT COALESCE(SUM(CONVERT(BIGINT,_lpr3_uoploeste_referencer)*{source_weight}),0) FROM [ext].[{dataset.source_table}]",
            )
            alarms = record_delivery(
                conn,
                dataset,
                manifest,
                result=result,
                full_load=full_load,
                refreshed=refreshed,
                seconds=time.monotonic() - started,
                fallback_rows=fallback,
                unresolved_references=unresolved,
            )
            if alarms:
                context.log.warning(f"LPR3 audit-insert reload alarm: {alarms}")
            execute_safe(
                conn,
                f"DROP TABLE [ext].[{dataset.source_table}]; DROP VIEW [ext].[v_{dataset.arc_table}]",
            )
    return dg.MaterializeResult(
        metadata={
            "source_rows_represented": represented,
            "rows_inserted": result.rows_inserted,
            "rows_updated": result.rows_updated,
            "rows_soft_deleted": result.rows_soft_deleted,
            "source_labels_refreshed": refreshed,
            "fallback_rows": fallback,
            "key_methods": dg.MetadataValue.json(key_methods),
            "unresolved_references": unresolved,
            "merge_method": "bulk" if full_load else "merge",
            "lpr3_reserved_bytes_before_merge": reserved_bytes,
            "seconds": time.monotonic() - started,
        }
    )


def _archive_asset(dataset, predecessor):
    @dg.asset(
        name=f"arc_{dataset.arc_table}",
        group_name="lpr3_archive",
        compute_kind="mssql",
        deps=[predecessor],
        tags={"domain": "lpr3", "layer": "archive", "source": "lpr3"},
        description=(
            f"Guard and merge the run's LPR3 source rows into arc.{dataset.arc_table}; "
            "absence from a full snapshot soft-deletes within the configured limit."
        ),
    )
    def archive(
        context: dg.AssetExecutionContext,
        config: LPR3ArchiveConfig,
        mssql_target_staging: MSSQLResource,
    ):
        return _run_archive(context, mssql_target_staging, dataset, config.soft_delete_threshold)

    return archive


# Sequential: each typed source copy is dropped before the next merge builds its own.
_archive_assets = []
_previous_asset = "lpr3_archive_noegler"
for _dataset in ARCHIVE_DATASETS:
    _archive_assets.append(_archive_asset(_dataset, _previous_asset))
    _previous_asset = f"arc_{_dataset.arc_table}"


@dg.asset(
    group_name="lpr3_archive",
    compute_kind="mssql",
    deps=[_previous_asset],
    tags={"domain": "lpr3", "layer": "archive", "source": "lpr3"},
    description=(
        "Advance the LPR3 DFM delivery checkpoint once every archive table has passed the "
        "gate for this run's manifest, then drop the run's identity tables. Never advanced "
        "by a run pinned with delivery_on_or_before."
    ),
)
def lpr3_leverance_checkpoint(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
):
    with mssql_target_staging.get_connection() as conn:
        with owned_landing(conn, context.run_id):
            manifest = read_manifest(conn, context.run_id)
            if not manifest:
                return dg.MaterializeResult(metadata={"no_op": True})
            assert_gate_complete(conn, manifest, context.run_id)
            delivery = next(iter(manifest.values()))
            if delivery.advance_checkpoint:
                DfmManager(mssql_target_staging, context).set_checkpoint(
                    DELIVERY_SCHEMA,
                    DELIVERY_UNIT,
                    value=delivery.checkpoint,
                    target_type=DELIVERY_TARGET_TYPE,
                )
            for dataset in ARCHIVE_DATASETS:
                if dataset.key_policy.key_class != "bridge":
                    execute_safe(
                        conn, f"DROP TABLE IF EXISTS [ext].[{identity_table(dataset.entity)}]"
                    )
    return dg.MaterializeResult(
        metadata={
            "checkpoint_advanced": delivery.advance_checkpoint,
            "delivery": delivery.checkpoint,
        }
    )


lpr3_archive_assets = [lpr3_archive_noegler, *_archive_assets, lpr3_leverance_checkpoint]
