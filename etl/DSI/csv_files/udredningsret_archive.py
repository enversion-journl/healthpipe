# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Udredningsret archive layer — MERGE ext.udredningsret_mont into arc on
business keys (SDS LPR_Udredningsret, decisions B3/B6).

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
(guards, MERGE). Hence the _noegler suffix and not a v_
prefix — v_ is reserved for views in ext. It is DROPPED again after a
successful merge (review F18): between runs it would be a third ~5 GB copy of
the dataset with no reader; on failure it is left in place for post mortem.
The keys are named *_noegle like DRG's derived arc columns
(drg_archive.py: *_uuid / *_noegle); clean_code_* is formed in pre_stg (US-3).

The unique clustered index on the merge keys is created AFTER the key-
uniqueness guard (so a duplicate fails with the guard's message, not a raw
index error) and verified BEFORE the merge, because
enable_bulk_insert_on_first_load presupposes it.

SURROGATE IDS (decision U1 in udredningsret_model.md): PK and F_ID are outside
change-detect — a pure renumbering is not a change and must not bump
dw_timestamp_update or leverance_dato on 1.5 M rows a week. But staging groups
markers into udredningsforloeb on F_ID WITHIN the delivery (B3/B4), so a stale
F_ID on unchanged rows would split a renumbered helbredsforloeb into two
groups. Hence, AFTER a successful merge, refresh_surrogate_ids runs one narrow
UPDATE that copies PK/F_ID from the key table onto the live arc rows where they
differ — nothing else is written on that step (no timestamp, no
leverance_dato). It reads the key table, so it runs before the table is
dropped, and it is SKIPPED on backfill_historical: an older delivery must not
push older ids over newer ones. A REGROUPING of markers between F_ID groups is
not a relabel and must re-stage: the same function therefore detects it BEFORE
the id UPDATE (the only moment both labellings exist) and bumps
dw_timestamp_update on those helbredsforloeb afterwards — decision U15, see
build_regroup_detection_sql.

DELIVERY MANIFEST (delivery-detection pattern): the extract writes one row to
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
"""

import dataclasses
from dataclasses import dataclass

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.dfm_manager import DfmManager
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

# Session-scoped work table with the helbredsforloeb that need a bump. Written
# by build_regroup_detection_sql, read and dropped by build_regroup_bump_sql —
# on the SAME connection, like the staging layer's #scope: temp tables live for
# the session, not for the batch.
REGROUP_TABLE = "#omgruppering"

# The surrogate id staging groups markers into udredningsforloeb on, within the
# delivery (B3/B4). Its RENUMBERING is a non-event (decision U1); its
# REGROUPING is what the detector below exists for.
REGROUP_COLUMN = "F_ID"


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


def build_refresh_surrogate_ids_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> str:
    """T-SQL that copies SURROGATE_ID_COLUMNS from the key table onto live arc rows.

    Runs AFTER the merge (decision U1): the merge did not touch the rows whose
    only difference is a renumbered PK/F_ID, so this UPDATE brings those ids
    up to the delivery's values. Deliberately narrow — only the surrogate
    columns are in the SET list, never dw_timestamp_update or leverance_dato,
    because a renumbering is still not a change. Restricted to rows that
    actually differ, so a rerun of the same delivery updates nothing. Pure
    function: the SQL is unit-tested without a database.

    Known assumption: a changed F_ID is a 1:1 relabel of the whole
    helbredsforloeb (measured: always). A REGROUPING with no other column
    changed (one F_ID group split in two, two merged, a marker swapping groups,
    or an F_ID going blank) would bump no timestamp here and hence never
    re-enter staging's delta scope on its own. That case is NOT left to DQA —
    subject 6 only counts fact rows that were already re-staged and subject 9
    covers a changed first marker, so neither sees it. It is caught in this
    module instead (decision U15): build_regroup_detection_sql runs BEFORE this
    UPDATE, while both labellings still exist, and build_regroup_bump_sql bumps
    dw_timestamp_update on the affected helbredsforloeb after it. The fact's
    control total (U13) is the backstop, and a net one — a split and a merge in
    the same window cancel out. Documented in udredningsret_kilde.md
    (merge/guard semantics) and udredningsret_model.md section 10.
    """
    set_clause = ", ".join(f"tgt.[{c}] = src.[{c}]" for c in SURROGATE_ID_COLUMNS)
    on_clause = " AND ".join(f"src.[{k}] = tgt.[{k}]" for k in dataset.merge_keys)
    differs = " OR ".join(
        f"ISNULL(tgt.[{c}], N'') <> ISNULL(src.[{c}], N'')" for c in SURROGATE_ID_COLUMNS
    )
    return (
        f"UPDATE tgt SET {set_clause}\n"
        f"FROM [arc].[{dataset.ext_table}] AS tgt\n"
        f"JOIN [ext].[{source_table_name(dataset)}] AS src ON {on_clause}\n"
        f"WHERE tgt.[dw_is_deleted] = 0 AND ({differs});"
    )


def build_regroup_detection_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> list[str]:
    """T-SQL that fills REGROUP_TABLE with the helbredsforloeb that need a timestamp bump.

    Returns [scaffold, populate], executed in that order on the same connection;
    only the LAST statement takes a parameter (the prune threshold from
    read_merge_start). The split is deliberate: the driver (mssql_python) runs a
    parameterised statement as a PREPARED batch, and a temp table created inside
    one dies with that batch — measured: the bump then fails with "Invalid
    object name '#omgruppering'". The table is created by the unparameterised
    scaffold (session scope) and only FILLED by the parameterised INSERT — the
    pattern DSA/CodeSystem/utils.py uses, and the reason the staging layer keeps
    its #scope batches free of parameters.

    Decision U1 keeps PK/F_ID out of change detection because SDS renumbers
    F_ID for whole helbredsforloeb at a time: a pure RELABEL is not a change,
    and treating it as one would re-stage ~1.5 M rows a week. A REGROUP is a
    change. If markers move between F_ID groups inside one helbredsforloeb (a
    split, a merge, a marker swapping groups, an F_ID going blank) and every
    change-detect column is byte-identical, the merge leaves the rows alone,
    refresh_surrogate_ids writes the new F_ID onto them, and the helbredsforloeb
    never re-enters staging's delta scope — until the fact's control total
    (U13) fails the whole nightly chain, run after run, on that one forloeb.

    The two inequalities are exact for the LABELLING, not a heuristic. Let R be
    the set of (old group, new group) pairs realised by the matched markers of
    one helbredsforloeb. Every old and every new group occurs at least once, so
    |R| >= max(|old|, |new|). |R| = |old| iff R is a function old -> new; given
    that, |R| = |new| iff every new group has exactly one preimage. So
    |R| = |old| = |new| IS "the relabel is a bijection on groups", and
    everything else is a regroup — including the membership swap that every
    count-based check misses. par is already DISTINCT on (hf, old, new), so
    COUNT(*) per helbredsforloeb is |R| and the two DISTINCTs run over roughly
    one row per group, not over 8.2 M.

    Blank is where that argument stops, and hence the third HAVING term.
    '#BLANK' is a label on both sides, but it is NOT a group downstream:
    stg.pre_Udredningsforloeb forms one forloeb per (helbredsforloeb, NON-BLANK
    F_ID) and the fact's control total counts exactly those groups (U13). A
    marker crossing the blank boundary therefore adds or removes a whole group
    while the labelling stays a bijection — measured on SQL Server with these
    very expressions: one whole group of two going blank gives |R| = |old| =
    |new| = 2, and a blank marker given an F_ID gives the same, so both
    inequalities stay silent on exactly the drift that changes arc_antal. With
    1.17 M markers (14 %) belonging to no forloeb at all, that drift is ordinary
    here. The third term flags any helbredsforloeb with a pair that is blank on
    exactly ONE side; it adds no false positive, because such a pair can only
    occur on a row the merge left alone and it always changes the group
    population. A pure relabel is still not flagged (verified the same way).

    The prune drops the rows THIS RUN's merge changed: their helbredsforloeb
    already got a timestamp and is already in scope, and their untouched
    siblings would otherwise make a half-updated helbredsforloeb look
    non-bijective. The threshold is the server's own clock read BEFORE the merge
    (read_merge_start), not the delivery date — nothing forbids two deliveries
    packed on the same DATE (the direction guard rejects only a strictly older
    one, and the DFM identity is the pack timestamp to the second), and a date
    prune would then also drop rows an EARLIER run changed, whose helbredsforloeb
    may have been staged since: a regrouping whose evidence lies in exactly those
    rows would be pruned away and never bumped — the same silent, chain-stopping
    outcome this detector exists to prevent. The merge stamps
    CAST(SYSDATETIME() AS datetime2(0)) like the bump does, so every row this run
    wrote is >= the threshold and none of them survives the prune. Hence the
    metric means "helbredsforloeb needing a bump", not "helbredsforloeb
    regrouped".

    Pure function: the SQL is unit-tested without a database.
    """
    index_name = f"IX_{REGROUP_TABLE.lstrip('#')}"
    on_clause = "\n     AND ".join(f"src.[{k}] = tgt.[{k}]" for k in dataset.merge_keys)
    scaffold = (
        f"DROP TABLE IF EXISTS {REGROUP_TABLE};\n"
        f"CREATE TABLE {REGROUP_TABLE} ([hf] CHAR({NOEGLE_WIDTH}) NOT NULL);\n"
        f"CREATE UNIQUE CLUSTERED INDEX [{index_name}] ON {REGROUP_TABLE}([hf]);"
    )
    populate = f"""WITH par AS (
    SELECT DISTINCT
        tgt.[helbredsforloeb_noegle] AS [hf],
        ISNULL(NULLIF(LTRIM(RTRIM(tgt.[{REGROUP_COLUMN}])), N''), N'#BLANK') AS [gammel],
        ISNULL(NULLIF(LTRIM(RTRIM(src.[{REGROUP_COLUMN}])), N''), N'#BLANK') AS [ny]
    FROM [arc].[{dataset.ext_table}] AS tgt
    JOIN [ext].[{source_table_name(dataset)}] AS src
      ON {on_clause}
    WHERE tgt.[dw_is_deleted] = 0
      AND ISNULL(tgt.[dw_timestamp_update], '1900-01-01') < CAST(? AS datetime2(0))
)
INSERT INTO {REGROUP_TABLE} ([hf])
SELECT p.[hf]
FROM par AS p
GROUP BY p.[hf]
HAVING COUNT(*) <> COUNT(DISTINCT p.[gammel])
    OR COUNT(*) <> COUNT(DISTINCT p.[ny])
    OR MAX(CASE WHEN (CASE WHEN p.[gammel] = N'#BLANK' THEN 1 ELSE 0 END)
                  <> (CASE WHEN p.[ny] = N'#BLANK' THEN 1 ELSE 0 END)
                THEN 1 ELSE 0 END) = 1;"""
    return [scaffold, populate]


def build_regroup_bump_sql(dataset: UdredningsretDataset = UDREDNINGSRET_MONT) -> str:
    """T-SQL that bumps dw_timestamp_update on the live rows of the detected helbredsforloeb.

    ONLY dw_timestamp_update, never leverance_dato: that column means "the
    delivery in which this row last changed" (review F16), no value changed
    here, and writing it would blind the detector's own prune on the next
    delivery. The bump is what puts the helbredsforloeb back into staging's
    delta scope (_scope_sql branch 1), so the new grouping is re-staged and the
    fact's control total stays green. It seeks the existing nonclustered index
    (helbredsforloeb_noegle, dw_is_deleted). Takes no parameters — the work
    table must stay visible to it (see build_regroup_detection_sql) — and drops
    the work table, so the connection is left as it was found.
    """
    return (
        f"UPDATE tgt SET tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))\n"
        f"FROM [arc].[{dataset.ext_table}] AS tgt\n"
        f"JOIN {REGROUP_TABLE} AS o ON o.[hf] = tgt.[helbredsforloeb_noegle]\n"
        f"WHERE tgt.[dw_is_deleted] = 0;\n"
        f"DROP TABLE IF EXISTS {REGROUP_TABLE};"
    )


def read_merge_start(conn) -> str:
    """The server's own clock, truncated to the second, read BEFORE the merge.

    It is the regrouping detector's prune threshold: every row the merge writes
    carries CAST(SYSDATETIME() AS datetime2(0)) from a moment at or after this
    one (core/archive_merge.py), so "dw_timestamp_update < threshold" is exactly
    "this run did not change the row" — while a row an older run changed stays
    in the comparison, whatever date it was delivered on (see
    build_regroup_detection_sql). Read on the SERVER, never in Python: the
    stamps it is compared against are the server's, and ISO 8601 (style 126)
    parses back to datetime2 independently of language settings.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT CONVERT(NVARCHAR(19), CAST(SYSDATETIME() AS datetime2(0)), 126)")
        return str(cursor.fetchone()[0])
    finally:
        cursor.close()


def refresh_surrogate_ids(
    conn,
    dataset: UdredningsretDataset,
    merge_start: str,
) -> tuple[int, int]:
    """Detect regroupings, refresh PK/F_ID, bump the regrouped helbredsforloeb.

    Returns (rows refreshed, helbredsforloeb bumped). The three steps run on ONE
    cursor and are committed ONCE, in this order — and the order is the whole
    point. Detection must read arc BEFORE the id UPDATE: afterwards arc.F_ID
    already equals the delivery's F_ID and the old grouping is gone, and the
    key table it compares against is dropped a moment later, so this is the only
    moment where both labellings coexist. Splitting the steps across commits
    would be worse than not detecting at all: a crash between the UPDATE and the
    bump destroys the evidence, and the rerun — whose refresh only touches rows
    that still differ — then finds nothing and leaves the stale grouping in
    place in silence.

    The refresh runs on its own connection AFTER execute_archive_merge has
    completed the arc DFM run, so a failure here leaves the merge committed
    with stale ids and the DFM run marked completed — the state U1 exists to
    prevent. That window is repaired by the rerun: the manifest is still
    present, the merge finds 0/0/0 and the refresh runs again (idempotent —
    only rows that still differ are written).
    """
    cursor = conn.cursor()
    try:
        scaffold, populate = build_regroup_detection_sql(dataset)
        cursor.execute(scaffold)
        cursor.execute(populate, merge_start)
        cursor.execute(f"SELECT COUNT(*) FROM {REGROUP_TABLE}")
        row = cursor.fetchone()
        regrouped = int(row[0]) if row and row[0] is not None else 0

        cursor.execute(build_refresh_surrogate_ids_sql(dataset))
        # the driver reports -1 when the server sends no count (SET NOCOUNT ON,
        # a trigger on arc); never surface that as "-1 rows refreshed"
        refreshed = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else 0

        cursor.execute(build_regroup_bump_sql(dataset))
        conn.commit()
    finally:
        cursor.close()
    return int(refreshed), regrouped


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
    # column IS written whenever a real change is found. Unlike leverance_dato,
    # PK/F_ID are ALSO refreshed on the rows the merge left alone —
    # refresh_surrogate_ids runs after the merge without a timestamp bump
    # (decision U1: staging groups markers into udredningsforloeb on F_ID
    # within the delivery, so the archive must carry the delivery's F_ID).
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


def measure_helbredsforloeb_recovery(conn, dataset: UdredningsretDataset) -> dict[str, float | int]:
    """Share of the archive's live helbredsforloeb keys found in the delivery (1.0 on first load)."""
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

        recovery = measure_helbredsforloeb_recovery(conn, dataset)
        context.log.info(
            f"source: {parity['kilderaekker_efter_dubletreduktion']:,} rows "
            f"({parity['fuldt_identiske_dubletter']:,} fully identical duplicates collapsed); "
            f"helbredsforloeb recovered: {recovery['andel']:.4%}"
        )

        live = count_live(conn, dataset)
        # The regrouping detector's prune threshold, read BEFORE the merge: the
        # rows the merge is about to write get a stamp from after this moment,
        # and only those must be pruned (build_regroup_detection_sql).
        merge_start = read_merge_start(conn)

    result = execute_archive_merge(context, mssql, merge_config, config)

    with mssql.get_connection() as conn:
        # Decision U1: bring PK/F_ID on the rows the merge left alone up to the
        # delivery's values, without a timestamp bump — but bump the ones whose
        # F_ID GROUPING changed, which is a real change the merge cannot see
        # (build_regroup_detection_sql). Both read the key table and the arc
        # rows as the merge left them, so this must run before the DROP below.
        # Skipped on backfill: an older delivery's ids must never overwrite the
        # newer ones already in arc, and its grouping is not the archive's.
        if config.backfill_historical:
            refreshed, regrouped = 0, 0
            context.log.warning(
                "backfill_historical: PK/F_ID refresh and regrouping detection skipped — an "
                "older delivery must not push older surrogate ids over the archive's newer "
                "ones, and its grouping is not the one the archive should follow."
            )
        else:
            refreshed, regrouped = refresh_surrogate_ids(conn, dataset, merge_start)
            context.log.info(
                f"{refreshed:,} live rows got PK/F_ID refreshed without a content change; "
                f"{regrouped:,} helbredsforloeb were regrouped and got dw_timestamp_update "
                f"bumped so staging re-reads them"
            )
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
    metadata["surrogat_id_opdateret"] = refreshed
    metadata["omgrupperinger"] = regrouped
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
    if not delivery.checkpoint_identitet or len(delivery.checkpoint_identitet) > 50:
        raise ValueError("manifest checkpoint identity must contain 1–50 characters")
    dfm = DfmManager(mssql, context)
    previous = dfm.get_checkpoint("dfm", DELIVERY_UNIT)
    dfm.set_checkpoint("dfm", DELIVERY_UNIT, value=delivery.checkpoint_identitet)
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
    "ExtDelivery",
    "UdredningsretArchiveConfig",
    "build_merge_config",
    "build_refresh_surrogate_ids_sql",
    "build_regroup_bump_sql",
    "build_regroup_detection_sql",
    "build_source_index_sql",
    "build_source_table_sql",
    "count_live",
    "drop_source_table_sql",
    "guard_delivery_direction",
    "guard_ext_not_empty",
    "guard_key_uniqueness",
    "guard_row_parity",
    "guard_source_unique_index",
    "measure_helbredsforloeb_recovery",
    "read_manifest",
    "record_delivery",
    "read_merge_start",
    "refresh_surrogate_ids",
    "source_index_name",
    "source_table_name",
    "udredningsret_archive_assets",
    "udredningsret_leverance_checkpoint",
]
