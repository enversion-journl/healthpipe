# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Udredningsret archive layer — MERGE ext.udredningsret_mont into arc on
business keys ([internal ticket], [internal ticket], decisions B3/B6).

The source's own ids identify nothing across deliveries: F_ID is renumbered for
whole helbredsforloeb at a time (31 % of the markers in 27 days), PK
(= F_ID_loebenr) follows, and OBJEKTID_FORLOBSMARKOR is reissued for 3.8 % of
the markers per month — 98.5 % of the "removed" UUIDs reappear with the same
(helbredsforloeb, code, date). SDS' DW_EK_HelbredsForloeb is a surrogate that
was renumbered in December 2021. The merge therefore runs on keys computed
HERE, in a source table derived from ext:

  helbredsforloeb_noegle  SHA2_256(CPR | first marker's code | date |
                          responsible unit | forloebselement UUID) — the first
                          marker is the helbredsforloeb's earliest (date, code,
                          unit, element) within the delivery. Measured 100 %
                          unique; changes for 0.01 % per month.
  markoer_noegle          SHA2_256(helbredsforloeb key | code | date)
  loebenummer             ROW_NUMBER over markoer_noegle, ordered by a hash of
                          the marker's STABLE content (12,236 duplicates of
                          8.17 M) — see LOEBENUMMER_HASH_EXCLUDED
  antal_kilderaekker      fully identical rows (same content, only another
                          PK/OBJEKTID — 1,705 rows) collapsed into one

The source table ext.udredningsret_mont_noegler is a physical table (SELECT
INTO), not a view like DRG's ext.v_*: the key computation is three window
functions over 8.2 M rows, and the merge engine reads the source several times
(guards, expected soft-deletes, MERGE). Hence the _noegler suffix and not a v_
prefix — v_ is reserved for views in ext. It is DROPPED again after a
successful merge (review F18): between runs it would be a third ~5 GB copy of
the dataset with no reader; on failure it is left in place for post mortem.
The keys are named *_noegle like DRG's derived arc columns
(drg_archive.py: *_uuid / *_noegle); clean_code_* is formed in pre_stg (US-3).

The unique clustered index on the merge keys is created AFTER the key-
uniqueness guard (so a duplicate fails with the guard's message, not a raw
index error) and verified BEFORE the merge, because
enable_bulk_insert_on_first_load presupposes it.

DELIVERY MANIFEST ([internal ticket] pattern): the extract writes one row to
ext.udredningsret_leverance_manifest when it loaded a delivery and none when
the run was a no-op. An empty manifest short-circuits this asset BEFORE any
key table, guard or merge — re-merging ext's old content would be pure cost.
The gate asset udredningsret_leverance_checkpoint advances the DFM delivery
checkpoint from the manifest once the merge has succeeded, unless the manifest
says the run was pinned (ryk_checkpoint = 0).

Guards, in order:
  1. delivery direction  — an older LPROPDATERINGSDATO than the archive's must
                           never be merged on top (meta.UdredningsretLeverance)
  2. row parity          — SUM(antal_kilderaekker) in the source table = rows
                           in ext (no row lost or doubled in the computation)
  3. key uniqueness      — (markoer_noegle, loebenummer) is unique by
                           construction; a duplicate is a programming error and
                           fails before MERGE
  4. recovery            — >= 99.9 % of the archive's live helbredsforloeb keys
                           must be found in the delivery (B3 guard against a
                           large-scale renumbering/first-marker change)
  5. soft-delete cap     — check_soft_delete_threshold() on the EXPECTED count
                           before the merge (absolute cap in etl_thresholds +
                           1 % of the live rows); the source is one full
                           snapshot, so the delete scope is the whole table.
                           Measured: 0.06 %/month.
"""

import dataclasses
from dataclasses import dataclass

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.dfm_manager import DfmManager
from core.etl_thresholds import check_soft_delete_threshold
from core.resources import MSSQLResource
from DSI.csv_files.udredningsret import (
    DELIVERY_COLUMN,
    DELIVERY_UNIT,
    KADENCE_COLUMN,
    MANIFEST_TABLE,
    TIMESTAMP_COLUMN,
)
from DSI.csv_files.udredningsret_datasets import (
    ARCHIVED_DATASETS,
    CONTENT_EXCLUDED,
    DEDUP_ORDER_COLUMNS,
    HELBREDSFORLOEB_GROUP_COLUMN,
    HELBREDSFORLOEB_ORDER_COLUMNS,
    LOEBENUMMER_HASH_EXCLUDED,
    MARKOER_KEY_COLUMNS,
    NOEGLE_WIDTH,
    SURROGATE_ID_COLUMNS,
    UDREDNINGSRET_MONT,
    UdredningsretDataset,
)
from pydantic import Field

LEVERANCE_TABLE = "UdredningsretLeverance"

# Soft-delete cap as a share of the live rows: the source deletes 0.06 % per
# month (4,532 of 8.08 M); 1 % is a quarter's real deletions times ten. The
# absolute cap lives in core/etl_thresholds.py.
SOFT_DELETE_RATIO = 0.01
RATIO_MIN_LIVE_ROWS = 1000

# Share of the archive's live helbredsforloeb keys that must be found in the
# delivery. Measured week over week: 99.99 % (218 changes of 2.2 M in 27 days).
# A lower share means the source changed the first marker at scale, and the
# merge would soft-delete the helbredsforloeb and recreate them under new keys.
MIN_HELBREDSFORLOEB_GENFINDELSE = 0.999


def source_table_name(dataset: UdredningsretDataset) -> str:
    """ext.<table>_noegler: physical source table with the business keys (not a view)."""
    return f"{dataset.ext_table}_noegler"


def source_index_name(dataset: UdredningsretDataset) -> str:
    return f"UX_ext_{source_table_name(dataset)}"


def _concat(columns: list[str], alias: str = "") -> str:
    """CONCAT with '|' as separator; CONCAT treats NULL as ''.

    The first argument is cast to NVARCHAR(MAX): without a LOB argument CONCAT
    silently truncates at 4000 characters, and 26 columns of up to 500 each can
    exceed that.
    """
    prefix = f"{alias}." if alias else ""
    parts: list[str] = []
    for i, column in enumerate(columns):
        if i:
            parts.append("'|'")
        expression = f"{prefix}[{column}]"
        parts.append(f"CAST({expression} AS NVARCHAR(MAX))" if i == 0 else expression)
    return "CONCAT(" + ", ".join(parts) + ")"


def _sha(expression: str) -> str:
    return f"CONVERT(CHAR({NOEGLE_WIDTH}), HASHBYTES('SHA2_256', {expression}), 2)"


def build_source_table_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> list[str]:
    """T-SQL that (re)builds ext.<table>_noegler: the arc column set with computed keys.

    Returns [DROP, SELECT INTO], executed in order. Column order in the source
    table = dataset.arc_columns + leverance_dato, so the merge can use it 1:1.
    The unique index is added separately (build_source_index_sql) after the
    key-uniqueness guard.

    Two different orderings, deliberately (review F13):
      - the dedup partition (indhold_rn) orders by OBJEKTID/PK — the rows in it
        are fully identical, so the choice is irrelevant;
      - loebenummer orders by loebenummer_hash, a hash of the content WITHOUT
        every id the source renumbers, so the rank survives a reissued
        OBJEKTID or a renumbered F_ID/PK/DW_EK. OBJEKTID/PK only break the
        residual tie between rows that differ in nothing but those ids.
    """
    ext = f"[ext].[{dataset.ext_table}]"
    target = f"[ext].[{source_table_name(dataset)}]"
    content_columns = [c for c in dataset.source_columns if c not in CONTENT_EXCLUDED]
    stable_columns = [c for c in dataset.source_columns if c not in LOEBENUMMER_HASH_EXCLUDED]
    hf_key_columns = ["CPR", *HELBREDSFORLOEB_ORDER_COLUMNS]
    order_by_first = ", ".join(f"[{c}]" for c in HELBREDSFORLOEB_ORDER_COLUMNS)
    order_by_dedup = ", ".join(f"[{c}]" for c in DEDUP_ORDER_COLUMNS)
    order_by_loebenummer = ", ".join(
        ["n.[loebenummer_hash]", *(f"n.[{c}]" for c in DEDUP_ORDER_COLUMNS)]
    )
    source_select = ",\n        ".join(f"n.[{c}]" for c in dataset.source_columns)
    hf_key_expr = _sha(
        "CONCAT("
        + ", '|', ".join(
            f"LOWER([{c}])" if c == "OBJEKTID_FORLOBELEMENT" else f"[{c}]" for c in hf_key_columns
        )
        + ")"
    )
    markoer_key_expr = _sha(
        "CONCAT(hf.[helbredsforloeb_noegle], "
        + ", ".join(f"'|', d.[{c}]" for c in MARKOER_KEY_COLUMNS)
        + ")"
    )
    build = f"""WITH indhold AS (
    SELECT e.*,
           {_sha(_concat(content_columns, "e"))} AS indhold_hash,
           {_sha(_concat(stable_columns, "e"))} AS loebenummer_hash
    FROM {ext} AS e
),
dedup AS (
    SELECT i.*,
           ROW_NUMBER() OVER (PARTITION BY i.[indhold_hash] ORDER BY {order_by_dedup}) AS indhold_rn,
           COUNT(*)     OVER (PARTITION BY i.[indhold_hash]) AS antal_kilderaekker
    FROM indhold AS i
),
foerste AS (
    SELECT [{HELBREDSFORLOEB_GROUP_COLUMN}], {", ".join(f"[{c}]" for c in hf_key_columns)},
           ROW_NUMBER() OVER (PARTITION BY [{HELBREDSFORLOEB_GROUP_COLUMN}]
                              ORDER BY {order_by_first}) AS rn
    FROM {ext}
),
hf AS (
    SELECT [{HELBREDSFORLOEB_GROUP_COLUMN}],
           {hf_key_expr} AS helbredsforloeb_noegle
    FROM foerste
    WHERE rn = 1
),
noegler AS (
    SELECT hf.[helbredsforloeb_noegle],
           {markoer_key_expr} AS markoer_noegle,
           d.*
    FROM dedup AS d
    JOIN hf ON hf.[{HELBREDSFORLOEB_GROUP_COLUMN}] = d.[{HELBREDSFORLOEB_GROUP_COLUMN}]
    WHERE d.[indhold_rn] = 1
)
SELECT n.[helbredsforloeb_noegle],
       n.[markoer_noegle],
       CAST(ROW_NUMBER() OVER (PARTITION BY n.[markoer_noegle]
                               ORDER BY {order_by_loebenummer}) AS INT) AS loebenummer,
       CAST(n.[antal_kilderaekker] AS INT) AS antal_kilderaekker,
        {source_select},
       n.[{DELIVERY_COLUMN}]
INTO {target}
FROM noegler AS n;"""
    return [f"DROP TABLE IF EXISTS {target};", build]


def build_source_index_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> str:
    """Unique clustered index on the merge keys — the precondition for fast load."""
    keys = ", ".join(f"[{k}]" for k in dataset.merge_keys)
    return (
        f"CREATE UNIQUE CLUSTERED INDEX [{source_index_name(dataset)}] "
        f"ON [ext].[{source_table_name(dataset)}] ({keys});"
    )


def drop_source_table_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> str:
    return f"DROP TABLE IF EXISTS [ext].[{source_table_name(dataset)}];"


# ---------------------------------------------------------------------------
# Merge configuration
# ---------------------------------------------------------------------------


def build_merge_config(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> ArchiveMergeConfig:
    non_key = tuple(c for c in dataset.arc_columns if c not in dataset.merge_keys) + (
        DELIVERY_COLUMN,
    )
    # leverance_dato is rewritten by every delivery by construction and must not
    # count as a change in itself (HAIBA/DRG pattern) — which also means it is
    # only WRITTEN when something else changed: on arc it reads "the delivery
    # in which this row last changed", not "the delivery it came from" (review
    # F16; "which delivery does the archive reflect" is answered by
    # meta.UdredningsretLeverance). PK/F_ID are SDS' delivery-internal sequence
    # numbers (see SURROGATE_ID_COLUMNS) and do not count either; every non_key
    # column IS written whenever a real change is found.
    ignored = {DELIVERY_COLUMN, *SURROGATE_ID_COLUMNS}
    change_detect = tuple(c for c in non_key if c not in ignored)
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=source_table_name(dataset),
        target_schema="arc",
        target_table=dataset.ext_table,
        merge_keys=dataset.merge_keys,
        non_key_columns=non_key,
        change_detect_columns=change_detect,
        strategy="upsert",
        update_only_on_change=True,
        # The source is one full snapshot from 2018: the delete scope is the whole table.
        detect_deletes=True,
        upstream_is_full_load=True,
        # First load: 8.2 M rows through MERGE run at ~1,500 rows/s (DRG
        # experience); INSERT...SELECT WITH (TABLOCK) is safe here because the
        # source table has a UNIQUE clustered index on the merge keys — which
        # guard_source_unique_index verifies before the merge.
        enable_bulk_insert_on_first_load=True,
    )


MERGE_CONFIGS: dict[str, ArchiveMergeConfig] = {
    d.ext_table: build_merge_config(d) for d in ARCHIVED_DATASETS
}


# ---------------------------------------------------------------------------
# Manifest and guards
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ExtDelivery:
    """The delivery the manifest says ext holds."""

    filnavn: str
    filnavn_stempel: str  # 'YYYY-MM-DD HH:MM:SS' from the filename
    kadence: str
    leverance_dato: str
    lpropdateringsdato: str
    checkpoint_enhed: str
    checkpoint_identitet: str
    ryk_checkpoint: bool


def read_manifest(conn) -> ExtDelivery | None:
    """The extract's manifest row, or None for a declared no-op run.

    A missing manifest TABLE is an error (the extract never ran), an empty one
    is the no-op contract.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID(?) AND type = 'U'",
            f"[ext].[{MANIFEST_TABLE}]",
        )
        if not cursor.fetchone()[0]:
            raise ValueError(
                f"ext.{MANIFEST_TABLE} does not exist — the extract has not run; the "
                f"archive must never merge without a manifest."
            )
        cursor.execute(
            f"SELECT [filnavn], [{TIMESTAMP_COLUMN}], [{KADENCE_COLUMN}], [{DELIVERY_COLUMN}], "
            f"[lpropdateringsdato], [checkpoint_enhed], [checkpoint_identitet], [ryk_checkpoint] "
            f"FROM [ext].[{MANIFEST_TABLE}]"
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    if not rows:
        return None
    if len(rows) != 1:
        raise ValueError(
            f"ext.{MANIFEST_TABLE} has {len(rows)} rows — the contract is at most one "
            f"delivery per run."
        )
    filnavn, stamp, kadence, delivery, lpr, unit, identity, advance = rows[0]
    return ExtDelivery(
        filnavn=str(filnavn),
        filnavn_stempel=str(stamp)[:19],
        kadence=str(kadence),
        leverance_dato=str(delivery),
        lpropdateringsdato=str(lpr),
        checkpoint_enhed=str(unit),
        checkpoint_identitet=str(identity),
        ryk_checkpoint=bool(advance),
    )


def guard_ext_not_empty(conn, dataset: UdredningsretDataset) -> None:
    """A manifest that promises a delivery but an empty ext is a broken contract."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [ext].[{dataset.ext_table}]")
        count = cursor.fetchone()[0]
    finally:
        cursor.close()
    if not count:
        raise ValueError(
            f"ext.{dataset.ext_table} is empty although the manifest names a delivery — "
            f"an empty source must never be merged (it would soft-delete the whole archive)."
        )


def guard_delivery_direction(
    conn, dataset: UdredningsretDataset, delivery: ExtDelivery, backfill: bool
) -> str | None:
    """An older delivery must never be merged on top of a newer one.

    Direction is judged on LPROPDATERINGSDATO (the source's own date for the
    content); the filename's date is the fallback when LEVERANCEINFO is missing
    from the archive's history. Both are compared as strings, which is
    chronological ONLY because the extract enforces ISO YYYY-MM-DD (review
    F14). Returns the archive's previous maximum.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT MAX([lpropdateringsdato]), MAX([leverance_dato]) "
            f"FROM [meta].[{LEVERANCE_TABLE}] WHERE [kildetabel] = ?",
            dataset.ext_table,
        )
        arc_lpr, arc_delivery = cursor.fetchone()
    finally:
        cursor.close()
    if arc_lpr is None and arc_delivery is None:
        return None
    ext_value, arc_value, label = (
        (delivery.lpropdateringsdato, arc_lpr, "LPROPDATERINGSDATO")
        if delivery.lpropdateringsdato and arc_lpr
        else (delivery.leverance_dato, str(arc_delivery), "leverance_dato")
    )
    if backfill:
        if str(ext_value) >= str(arc_value):
            raise ValueError(
                f"arc.{dataset.ext_table}: backfill requires an OLDER delivery than the "
                f"archive's ({label} {ext_value} >= {arc_value})"
            )
    elif str(ext_value) < str(arc_value):
        raise ValueError(
            f"arc.{dataset.ext_table}: ext holds the delivery with {label} {ext_value}, "
            f"but the archive already reflects {arc_value}. An older delivery must not be "
            f"merged on top of a newer one — use backfill_historical if that is intended."
        )
    return str(arc_value)


def guard_row_parity(conn, dataset: UdredningsretDataset) -> dict[str, int]:
    """Every ext row must be represented exactly once in the source table (as its
    own row, or counted in antal_kilderaekker on its identical twin)."""
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT COUNT(*) FROM [ext].[{dataset.ext_table}]")
        ext_rows = cursor.fetchone()[0]
        cursor.execute(
            f"SELECT COUNT(*), ISNULL(SUM([antal_kilderaekker]), 0) "
            f"FROM [ext].[{source_table_name(dataset)}]"
        )
        source_rows, represented = cursor.fetchone()
    finally:
        cursor.close()
    if represented != ext_rows:
        raise ValueError(
            f"ext.{source_table_name(dataset)}: {represented:,} ext rows represented, "
            f"but ext has {ext_rows:,} — the key computation lost or doubled rows "
            f"(typically a blank DW_EK_HelbredsForloeb). Investigate before merging."
        )
    return {
        "ext_raekker": int(ext_rows),
        "kilderaekker_efter_dubletreduktion": int(source_rows),
        "fuldt_identiske_dubletter": int(ext_rows - source_rows),
    }


def guard_key_uniqueness(conn, dataset: UdredningsretDataset) -> None:
    """(markoer_noegle, loebenummer) is unique by construction — a duplicate is a programming error."""
    keys = ", ".join(f"[{k}]" for k in dataset.merge_keys)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM (SELECT {keys} FROM [ext].[{source_table_name(dataset)}] "
            f"GROUP BY {keys} HAVING COUNT(*) > 1) AS d"
        )
        duplicates = cursor.fetchone()[0]
    finally:
        cursor.close()
    if duplicates:
        raise ValueError(
            f"ext.{source_table_name(dataset)}: {duplicates:,} key groups have duplicates on "
            f"({keys}) — loebenummer should have made the key unique. Programming error in "
            f"build_source_table_sql, not a delivery defect."
        )


def guard_source_unique_index(conn, dataset: UdredningsretDataset) -> None:
    """The source table MUST have a unique clustered index on the merge keys before the merge.

    enable_bulk_insert_on_first_load does INSERT...SELECT without duplicate
    protection and is only safe because the source is unique on the keys. This
    check is the guarantee, not a comment.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT COUNT(*) FROM sys.indexes WHERE object_id = OBJECT_ID(?) "
            "AND name = ? AND is_unique = 1 AND type = 1",
            f"[ext].[{source_table_name(dataset)}]",
            source_index_name(dataset),
        )
        found = cursor.fetchone()[0]
    finally:
        cursor.close()
    if not found:
        raise ValueError(
            f"ext.{source_table_name(dataset)}: the unique clustered index "
            f"{source_index_name(dataset)} on {dataset.merge_keys} is missing — the merge "
            f"(fast load) presupposes it. Programming error in build_source_index_sql."
        )


def guard_helbredsforloeb_recovery(
    conn, dataset: UdredningsretDataset, minimum: float
) -> dict[str, float | int]:
    """The archive's live helbredsforloeb keys must be found in the delivery.

    Below the limit the source has changed the first marker for many
    helbredsforloeb (or CPR/unit has changed): the merge would soft-delete them
    and recreate them under new keys, and downstream dw_ek would change.
    Skipped when the archive is empty (first load).
    """
    source = source_table_name(dataset)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(DISTINCT [helbredsforloeb_noegle]) FROM [arc].[{dataset.ext_table}] "
            f"WHERE [dw_is_deleted] = 0"
        )
        live = cursor.fetchone()[0]
        if not live:
            return {"levende_helbredsforloeb": 0, "genfundet": 0, "andel": 1.0}
        cursor.execute(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT a.[helbredsforloeb_noegle] "
            f"FROM [arc].[{dataset.ext_table}] AS a WHERE a.[dw_is_deleted] = 0 "
            f"AND EXISTS (SELECT 1 FROM [ext].[{source}] AS s "
            f"WHERE s.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle])) AS f"
        )
        found = cursor.fetchone()[0]
    finally:
        cursor.close()
    share = found / live
    if share < minimum:
        raise ValueError(
            f"arc.{dataset.ext_table}: only {share:.4%} of {live:,} live helbredsforloeb keys "
            f"are recovered in the delivery (limit {minimum:.2%}). The source changed the first "
            f"marker/CPR/unit for many helbredsforloeb, or the delivery is incomplete. "
            f"Investigate before merging; override with min_helbredsforloeb_genfindelse if explained."
        )
    return {"levende_helbredsforloeb": int(live), "genfundet": int(found), "andel": round(share, 6)}


def count_live(conn, dataset: UdredningsretDataset) -> int:
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM [arc].[{dataset.ext_table}] WHERE [dw_is_deleted] = 0"
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def count_expected_soft_deletes(conn, dataset: UdredningsretDataset) -> int:
    """Live arc rows without a counterpart in the delivery — counted BEFORE the
    merge, so the cap can fire before anything is committed."""
    on_clause = " AND ".join(f"src.[{k}] = tgt.[{k}]" for k in dataset.merge_keys)
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM [arc].[{dataset.ext_table}] AS tgt "
            f"WHERE tgt.[dw_is_deleted] = 0 AND NOT EXISTS "
            f"(SELECT 1 FROM [ext].[{source_table_name(dataset)}] AS src WHERE {on_clause})"
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def record_delivery(
    conn,
    dataset: UdredningsretDataset,
    delivery: ExtDelivery,
    result,
    parity: dict[str, int],
    recovery: dict[str, float | int],
    backfill: bool,
) -> None:
    """Upsert meta.UdredningsretLeverance per (kildetabel, leverance_tidsstempel).

    Runs AFTER the merge has been committed: if the registration fails, the
    direction guard becomes permissive (not blocking), and the next run repairs it.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            MERGE [meta].[{LEVERANCE_TABLE}] AS tgt
            USING (SELECT ? AS kildetabel, CAST(? AS DATETIME2(0)) AS leverance_tidsstempel) AS src
            ON tgt.[kildetabel] = src.kildetabel
               AND tgt.[leverance_tidsstempel] = src.leverance_tidsstempel
            WHEN MATCHED THEN UPDATE SET
                tgt.[kadence] = ?, tgt.[leverance_dato] = ?, tgt.[lpropdateringsdato] = ?,
                tgt.[indlaesningstype] = ?, tgt.[raekker_ext] = ?, tgt.[raekker_kilde] = ?,
                tgt.[raekker_indsat] = ?, tgt.[raekker_opdateret] = ?, tgt.[raekker_soft_slettet] = ?,
                tgt.[helbredsforloeb_genfindelse] = ?,
                tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS DATETIME2(0))
            WHEN NOT MATCHED THEN INSERT
                ([kildetabel], [leverance_tidsstempel], [kadence], [leverance_dato],
                 [lpropdateringsdato], [indlaesningstype], [raekker_ext], [raekker_kilde],
                 [raekker_indsat], [raekker_opdateret], [raekker_soft_slettet],
                 [helbredsforloeb_genfindelse])
                VALUES (src.kildetabel, src.leverance_tidsstempel, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            dataset.ext_table,
            delivery.filnavn_stempel,
            delivery.kadence,
            delivery.leverance_dato,
            delivery.lpropdateringsdato,
            "backfill" if backfill else "merge",
            parity["ext_raekker"],
            parity["kilderaekker_efter_dubletreduktion"],
            result.rows_inserted,
            result.rows_updated,
            result.rows_soft_deleted,
            recovery["andel"],
            delivery.kadence,
            delivery.leverance_dato,
            delivery.lpropdateringsdato,
            "backfill" if backfill else "merge",
            parity["ext_raekker"],
            parity["kilderaekker_efter_dubletreduktion"],
            result.rows_inserted,
            result.rows_updated,
            result.rows_soft_deleted,
            recovery["andel"],
        )
        conn.commit()
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


class UdredningsretArchiveConfig(RunModeConfig):
    backfill_historical: bool = Field(
        default=False,
        description=(
            "Load a delivery that is older than the archive's. Turns delete detection and "
            "revival off, so newer rows are not soft-deleted; only updates/inserts values. "
            "Always run the newest delivery afterwards. Never for a current delivery."
        ),
    )
    min_helbredsforloeb_genfindelse: float | None = Field(
        default=None,
        description=(
            f"Override the recovery guard for THIS run (share of the archive's live "
            f"helbredsforloeb keys that must be found in the delivery; default "
            f"{MIN_HELBREDSFORLOEB_GENFINDELSE}). Only once the cause is understood."
        ),
    )


def _noop_result(source: str, target: str) -> dg.MaterializeResult:
    return dg.MaterializeResult(
        metadata={
            "besluttet": "no-op — tomt manifest (ingen ny leverance indlaest)",
            "source_table": source,
            "target_table": target,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_soft_deleted": 0,
        }
    )


def _run_archive(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    dataset: UdredningsretDataset,
    config: UdredningsretArchiveConfig,
) -> dg.MaterializeResult:
    merge_config = MERGE_CONFIGS[dataset.ext_table]
    if config.backfill_historical:
        merge_config = dataclasses.replace(merge_config, detect_deletes=False)
        context.log.warning(
            "backfill_historical: delete detection and revival are off for this run — "
            "run the newest delivery afterwards."
        )
    minimum = (
        config.min_helbredsforloeb_genfindelse
        if config.min_helbredsforloeb_genfindelse is not None
        else MIN_HELBREDSFORLOEB_GENFINDELSE
    )
    source = f"ext.{source_table_name(dataset)}"
    target = f"arc.{dataset.ext_table}"

    with mssql.get_connection() as conn:
        delivery = read_manifest(conn)
        if delivery is None:
            # Declared no-op from the extract: nothing new was loaded, ext still
            # holds the previous delivery. Short-circuit BEFORE key table, guards
            # and merge — re-merging it would cost ~10 min and change nothing.
            context.log.info("empty delivery manifest — no new delivery; archive left untouched")
            return _noop_result(source, target)
        guard_ext_not_empty(conn, dataset)
        previous = guard_delivery_direction(conn, dataset, delivery, config.backfill_historical)

        cursor = conn.cursor()
        try:
            context.log.info(f"building {source} with business keys")
            for statement in build_source_table_sql(dataset):
                cursor.execute(statement)
            conn.commit()
        finally:
            cursor.close()

        parity = guard_row_parity(conn, dataset)
        guard_key_uniqueness(conn, dataset)

        # The index only AFTER the uniqueness guard: a duplicate must fail with
        # the guard's message, not with a raw "duplicate key" error from CREATE
        # UNIQUE INDEX.
        cursor = conn.cursor()
        try:
            cursor.execute(build_source_index_sql(dataset))
            conn.commit()
        finally:
            cursor.close()
        guard_source_unique_index(conn, dataset)

        recovery = guard_helbredsforloeb_recovery(conn, dataset, minimum)
        context.log.info(
            f"source: {parity['kilderaekker_efter_dubletreduktion']:,} rows "
            f"({parity['fuldt_identiske_dubletter']:,} fully identical duplicates collapsed); "
            f"helbredsforloeb recovered: {recovery['andel']:.4%}"
        )

        live = count_live(conn, dataset)
        expected_soft_deletes = (
            count_expected_soft_deletes(conn, dataset) if merge_config.detect_deletes else 0
        )

    # The cap is enforced BEFORE the merge on the expected count: the merge is
    # not transactional with the guard, and a rerun after a failure would see 0 new.
    ratio_live = live if live >= RATIO_MIN_LIVE_ROWS else None
    check_soft_delete_threshold(
        f"arc_{dataset.ext_table}",
        expected_soft_deletes,
        context,
        override=config.soft_delete_threshold,
        live_row_count=ratio_live,
        ratio=SOFT_DELETE_RATIO,
    )

    result = execute_archive_merge(context, mssql, merge_config, config)

    check_soft_delete_threshold(
        f"arc_{dataset.ext_table}",
        result.rows_soft_deleted,
        context,
        override=config.soft_delete_threshold,
        live_row_count=ratio_live,
        ratio=SOFT_DELETE_RATIO,
    )

    with mssql.get_connection() as conn:
        record_delivery(
            conn, dataset, delivery, result, parity, recovery, config.backfill_historical
        )
        # Only after success: on failure the key table stays for post mortem.
        # Between runs it would be a third ~5 GB copy of the dataset (review F18).
        cursor = conn.cursor()
        try:
            cursor.execute(drop_source_table_sql(dataset))
            conn.commit()
        finally:
            cursor.close()

    metadata = build_archive_metadata(result, config, source_table=source, target_table=target)
    metadata["leverance"] = dataclasses.asdict(delivery)
    metadata["arkivets_hidtidige_leverance"] = previous or "ingen (foerste indlaesning)"
    metadata["raekkeparitet"] = parity
    metadata["helbredsforloeb_genfindelse"] = recovery
    metadata["levende_raekker_foer"] = live
    metadata["noegletabel_droppet"] = source
    return dg.MaterializeResult(metadata=metadata)


def _build_asset(dataset: UdredningsretDataset):
    @dg.asset(
        name=f"arc_{dataset.ext_table}",
        group_name="udredningsret_archive",
        compute_kind="mssql",
        deps=["ext_udredningsret"],
        tags={"domain": "udredningsret", "layer": "archive", "source": "udredningsret"},
        description=(
            f"Build ext.{source_table_name(dataset)} with business keys (helbredsforloeb, "
            f"markoer, loebenummer) and MERGE it into arc.{dataset.ext_table} with soft-delete "
            f"over the whole snapshot. No-op when the extract's manifest is empty."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: UdredningsretArchiveConfig,
    ) -> dg.MaterializeResult:
        return _run_archive(context, mssql_target_staging, dataset, config)

    return _asset


def _run_checkpoint_gate(
    context: dg.AssetExecutionContext, mssql: MSSQLResource
) -> dg.MaterializeResult:
    """Advance the DFM delivery checkpoint to the delivery the archive now reflects.

    Reached only when every arc asset of the run succeeded (Dagster deps). The
    manifest is the source of truth for WHICH delivery was loaded — the share
    is not re-scanned, because a new file may have landed during the load.
    """
    with mssql.get_connection() as conn:
        delivery = read_manifest(conn)
    if delivery is None:
        return dg.MaterializeResult(
            metadata={"besluttet": "no-op — tomt manifest, checkpoint uaendret"}
        )
    if delivery.checkpoint_enhed != DELIVERY_UNIT:
        raise ValueError(
            f"manifest names checkpoint unit {delivery.checkpoint_enhed!r}, this gate owns "
            f"{DELIVERY_UNIT!r} — refusing to stamp a foreign unit."
        )
    if not delivery.ryk_checkpoint:
        context.log.warning(
            f"pinned run ({delivery.filnavn}) — the delivery checkpoint is NOT advanced"
        )
        return dg.MaterializeResult(
            metadata={
                "besluttet": "pin — checkpoint ikke rykket",
                "leverance": delivery.filnavn,
                "identitet": delivery.checkpoint_identitet,
            }
        )
    dfm = DfmManager(mssql, context)
    previous = dfm.get_delivery_checkpoint(DELIVERY_UNIT)
    dfm.set_delivery_checkpoint(DELIVERY_UNIT, delivery.checkpoint_identitet)
    return dg.MaterializeResult(
        metadata={
            "besluttet": "checkpoint rykket",
            "checkpoint_enhed": DELIVERY_UNIT,
            "checkpoint_foer": previous or "ingen (foerste indlaesning)",
            "checkpoint_efter": delivery.checkpoint_identitet,
            "leverance": delivery.filnavn,
        }
    )


@dg.asset(
    name="udredningsret_leverance_checkpoint",
    group_name="udredningsret_archive",
    compute_kind="mssql",
    deps=[f"arc_{d.ext_table}" for d in ARCHIVED_DATASETS],
    tags={"domain": "udredningsret", "layer": "archive", "source": "udredningsret"},
    description=(
        "Gate: stamp the DFM delivery checkpoint (meta.DFMState, "
        f"('dfm', '{DELIVERY_UNIT}')) with the delivery named in the extract's manifest, "
        "once every archive merge of the run has succeeded. Never advanced by a pinned run."
    ),
)
def udredningsret_leverance_checkpoint(
    context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
) -> dg.MaterializeResult:
    return _run_checkpoint_gate(context, mssql_target_staging)


udredningsret_archive_assets = [
    *(_build_asset(d) for d in ARCHIVED_DATASETS),
    udredningsret_leverance_checkpoint,
]

__all__ = [
    "LEVERANCE_TABLE",
    "MERGE_CONFIGS",
    "MIN_HELBREDSFORLOEB_GENFINDELSE",
    "SOFT_DELETE_RATIO",
    "ExtDelivery",
    "UdredningsretArchiveConfig",
    "build_merge_config",
    "build_source_index_sql",
    "build_source_table_sql",
    "count_expected_soft_deletes",
    "count_live",
    "drop_source_table_sql",
    "guard_delivery_direction",
    "guard_ext_not_empty",
    "guard_helbredsforloeb_recovery",
    "guard_key_uniqueness",
    "guard_row_parity",
    "guard_source_unique_index",
    "read_manifest",
    "record_delivery",
    "source_index_name",
    "source_table_name",
    "udredningsret_archive_assets",
    "udredningsret_leverance_checkpoint",
]
