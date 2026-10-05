# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DRG archive layer — MERGE ext.drg* into the persistent arc tables.

Every merge sources from a resolution VIEW over ext rather than the raw ext
table: the views translate the per-delivery integer references into the
stable LPR3 UUIDs (measured 100 % stable across deliveries AND takstsystemer,
see drg_noegler_beslutningsnotat.md) and drop the volatile IDs entirely —
after a merge the archive holds rows from different deliveries, and the
integer IDs would be out of sync across tables. All intra-delivery joins
are scoped on takstsystem: the integer IDs are only unique within one
delivery.

Delete detection is YEAR-SLICE SCOPED via ArchiveMergeConfig's
delete_scope_predicate: an M delivery carries three activity years, a U
delivery only two, so NOT MATCHED BY SOURCE is limited to the
(takstsystem, aktivitetsaar) slices actually present in ext — otherwise every
U run would soft-delete the oldest year and the next M run would resurrect
it. Retired takstsystemer (somatik T24) keep their archive rows for the same
reason. Psyk tables have no activity year column; their deliveries are full
snapshots per takstsystem, so the scope is the takstsystem alone.

Guards, in order per table:
  1. delivery direction  — an older delivery must never merge over a newer
     archive state (per takstsystem, meta.DRGLeverance)
  2. resolved references — row PARITY between ext and its resolution view:
     the resolved keys come from INNER JOINs, so an unresolvable reference
     silently DROPS the row from the view (it never becomes NULL) and the
     merge would soft-delete its archive counterpart
  3. key uniqueness      — the composite keys are measured properties, not
     SDS guarantees. CONFLICTING duplicates (same key, different values)
     fail the run rather than letting a dedup decide what the archive
     believes; BYTE-IDENTICAL duplicates enable the engine's deterministic
     dedup for that run only, since keeping one of two identical copies
     loses nothing (measured: SDS shipped one such pair in T24 psyk
     diagnoses, 2026-08-27). See drg_noegler_beslutningsnotat.md.
  4. price revision log  — changed price columns are captured to
     arc.drg_pris_revision BEFORE the merge overwrites them (idempotent via
     NOT EXISTS; skipped on backfill runs)
  5. soft-delete ceiling — check_soft_delete_threshold() on the EXPECTED
     count BEFORE the merge (the merge is not transactional with the guard,
     and a retry after a post-merge failure would see 0 new soft-deletes),
     plus a defense-in-depth check on the actual count after
"""

import dataclasses
import re
from dataclasses import dataclass

import dagster as dg
from core.archive_merge import build_archive_metadata, execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.etl_thresholds import check_soft_delete_threshold
from core.resources import MSSQLResource
from DSI.csv_files.drg_datasets import DATASETS, DrgDataset
from pydantic import Field
from utils.sql_safety import safe_bracket_quote

LEVERANCE_TABLE = "DRGLeverance"
REVISION_TABLE = "drg_pris_revision"

# Soft-delete ceiling: a merge may delete at most this share of the live rows
# INSIDE the delete scope (the delivery's own (takstsystem x year-slice)
# slices). The absolute cap (etl_thresholds, default 1000) is the tighter
# first guard on large tables; the ratio catches small slices where 1000 rows
# would be the whole population. Below the floor the absolute cap acts alone
# — otherwise test data and small year-slices would trip it.
SOFT_DELETE_RATIO = 0.20
RATIO_MIN_LIVE_ROWS = 1000

_DATASETS_BY_TABLE = {d.ext_table: d for d in DATASETS}

# ---------------------------------------------------------------------------
# Resolution views: per-table FROM/JOIN clause + SQL expression per added
# column. `e` is the dataset's own ext table; joins resolve integer refs to
# UUIDs within the same takstsystem (delivery scope).
#
# Suffix rule for the added key columns (all hold a KONTAKT_VAERDI):
#   *_uuid   — the entity's own LPR3 UUID (kontakt, procedure, …)
#   *_noegle — a first-contact UUID standing in for a forloeb that has no
#              UUID of its own (sygehusforloeb_noegle, forloeb_noegle).
# Source-copied denormalised columns keep the source's casing (somatik
# DIAGNOSE vs psyk Diagnose); that is the rule, not an accident.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _ViewJoin:
    from_clause: str
    added_expressions: dict[str, str]


_VIEW_JOINS: dict[str, _ViewJoin] = {
    "drg_kontakt": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_kontakt] AS e\n"
            "LEFT JOIN [ext].[drg_sghforlob] AS f\n"
            "    ON f.[takstsystem] = e.[takstsystem] AND f.[ID] = e.[DRGSGHFORLOB_ID]\n"
            "LEFT JOIN [ext].[drg_kontakt] AS indk\n"
            "    ON indk.[takstsystem] = f.[takstsystem] AND indk.[ID] = f.[DRGKONTAKT_ID_IND]"
        ),
        added_expressions={"sygehusforloeb_noegle": "indk.[KONTAKT_VAERDI]"},
    ),
    "drg_sghforlob": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_sghforlob] AS e\n"
            "JOIN [ext].[drg_kontakt] AS indk\n"
            "    ON indk.[takstsystem] = e.[takstsystem] AND indk.[ID] = e.[DRGKONTAKT_ID_IND]\n"
            "LEFT JOIN [ext].[drg_kontakt] AS udk\n"
            "    ON udk.[takstsystem] = e.[takstsystem] AND udk.[ID] = e.[DRGKONTAKT_ID_UD]\n"
            "LEFT JOIN [ext].[drg_kontakt] AS drgk\n"
            "    ON drgk.[takstsystem] = e.[takstsystem] AND drgk.[ID] = e.[DRG_KONTAKT_ID_DRGSGHF]"
        ),
        added_expressions={
            "ind_kontakt_uuid": "indk.[KONTAKT_VAERDI]",
            "ud_kontakt_uuid": "udk.[KONTAKT_VAERDI]",
            "drg_kontakt_uuid": "drgk.[KONTAKT_VAERDI]",
            # No derived sygdomsforloeb key: MIN(ind-kontakt-UUID) OVER the
            # forloeb is window-dependent (M covers three activity years, U
            # only two), so year-crossing forloeb would split in arc.
            # See drg_datasets.py.
        },
    ),
    "drg_diagnose": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_diagnose] AS e\n"
            "JOIN [ext].[drg_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID] = e.[DRGKONTAKT_ID]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
    "drg_diagnose_tillaeg": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_diagnose_tillaeg] AS e\n"
            "JOIN [ext].[drg_diagnose] AS d\n"
            "    ON d.[takstsystem] = e.[takstsystem] AND d.[ID] = e.[DIAGNOSE_ID]\n"
            "JOIN [ext].[drg_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID] = e.[DRGKONTAKT_ID]"
        ),
        added_expressions={
            "kontakt_uuid": "k.[KONTAKT_VAERDI]",
            # Source casing kept 1:1 (somatik DIAGNOSE vs psyk Diagnose).
            "DIAGNOSE": "ISNULL(d.[DIAGNOSE], '')",
            "ART": "ISNULL(d.[ART], '')",
            "SIDEANGIVELSE": "ISNULL(d.[SIDEANGIVELSE], '')",
        },
    ),
    "drg_procedure": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_procedure] AS e\n"
            "JOIN [ext].[drg_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID] = e.[DRGKONTAKT_ID]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
    "drg_procedure_tillaeg": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_procedure_tillaeg] AS e\n"
            "JOIN [ext].[drg_procedure] AS p\n"
            "    ON p.[takstsystem] = e.[takstsystem] AND p.[ID] = e.[PROCEDURE_ID]\n"
            "JOIN [ext].[drg_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID] = e.[DRGKONTAKT_ID]"
        ),
        added_expressions={
            "procedure_uuid": "p.[PROCEDURE_VAERDI]",
            "kontakt_uuid": "k.[KONTAKT_VAERDI]",
        },
    ),
    "drg_interne_koder": _ViewJoin(
        from_clause=(
            "FROM [ext].[drg_interne_koder] AS e\n"
            "JOIN [ext].[drg_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID] = e.[DRGKONTAKT_ID]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
    "drg_takstbeskrivelse": _ViewJoin(
        from_clause="FROM [ext].[drg_takstbeskrivelse] AS e", added_expressions={}
    ),
    "drg_leveranceinfo": _ViewJoin(
        from_clause="FROM [ext].[drg_leveranceinfo] AS e", added_expressions={}
    ),
    "drgpsyk_kontakt": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_kontakt] AS e\n"
            "LEFT JOIN [ext].[drgpsyk_forloeb] AS f\n"
            "    ON f.[takstsystem] = e.[takstsystem]\n"
            "    AND f.[PsykiatriForloeb_ID] = e.[PsykiatriForloeb_ID]\n"
            "LEFT JOIN [ext].[drgpsyk_kontakt] AS fk\n"
            "    ON fk.[takstsystem] = f.[takstsystem]\n"
            "    AND fk.[DW_EK_Kontakt] = f.[DW_EK_Kontakt_foerste]"
        ),
        added_expressions={"forloeb_noegle": "fk.[KONTAKT_VAERDI]"},
    ),
    "drgpsyk_forloeb": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_forloeb] AS e\n"
            "JOIN [ext].[drgpsyk_kontakt] AS fk\n"
            "    ON fk.[takstsystem] = e.[takstsystem]\n"
            "    AND fk.[DW_EK_Kontakt] = e.[DW_EK_Kontakt_foerste]\n"
            "LEFT JOIN [ext].[drgpsyk_kontakt] AS sk\n"
            "    ON sk.[takstsystem] = e.[takstsystem]\n"
            "    AND sk.[DW_EK_Kontakt] = e.[DW_EK_Kontakt_sidste]"
        ),
        added_expressions={
            "foerste_kontakt_uuid": "fk.[KONTAKT_VAERDI]",
            "sidste_kontakt_uuid": "sk.[KONTAKT_VAERDI]",
        },
    ),
    "drgpsyk_diagnose": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_diagnose] AS e\n"
            "JOIN [ext].[drgpsyk_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID_INT] = e.[Kontakt_ID_INT]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
    "drgpsyk_diagnose_tillaeg": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_diagnose_tillaeg] AS e\n"
            "JOIN [ext].[drgpsyk_diagnose] AS d\n"
            "    ON d.[takstsystem] = e.[takstsystem] AND d.[ID] = e.[DIAGNOSE_ID]\n"
            "JOIN [ext].[drgpsyk_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID_INT] = e.[Kontakt_ID_INT]"
        ),
        added_expressions={
            "kontakt_uuid": "k.[KONTAKT_VAERDI]",
            "Diagnose": "ISNULL(d.[Diagnose], '')",
            "ART": "ISNULL(d.[ART], '')",
            "SIDEANGIVELSE": "ISNULL(d.[SIDEANGIVELSE], '')",
        },
    ),
    "drgpsyk_procedure": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_procedure] AS e\n"
            "JOIN [ext].[drgpsyk_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID_INT] = e.[Kontakt_ID_INT]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
    "drgpsyk_procedure_tillaeg": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_procedure_tillaeg] AS e\n"
            "JOIN [ext].[drgpsyk_procedure] AS p\n"
            "    ON p.[takstsystem] = e.[takstsystem] AND p.[ID] = e.[PROCEDURE_ID]\n"
            "JOIN [ext].[drgpsyk_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID_INT] = e.[Kontakt_ID_INT]"
        ),
        added_expressions={
            "procedure_uuid": "p.[PROCEDURE_VAERDI]",
            "kontakt_uuid": "k.[KONTAKT_VAERDI]",
        },
    ),
    "drgpsyk_betalingsoplysninger": _ViewJoin(
        from_clause=(
            "FROM [ext].[drgpsyk_betalingsoplysninger] AS e\n"
            "JOIN [ext].[drgpsyk_kontakt] AS k\n"
            "    ON k.[takstsystem] = e.[takstsystem] AND k.[ID_INT] = e.[Kontakt_ID_INT]"
        ),
        added_expressions={"kontakt_uuid": "k.[KONTAKT_VAERDI]"},
    ),
}


def view_name(dataset: DrgDataset) -> str:
    return f"v_{dataset.ext_table}"


def build_view_sql(dataset: DrgDataset) -> str:
    """CREATE OR ALTER VIEW ext.v_<table>: the arc column set, UUID-resolved.

    Merge-key columns are wrapped in ISNULL(..., '') — NULL never matches
    itself in a MERGE ON clause, and the arc keys are NOT NULL.
    """
    join = _VIEW_JOINS[dataset.ext_table]
    added = {a.name for a in dataset.arc_added}
    select: list[str] = ["    e.[takstsystem]"]
    for name in dataset.arc_columns:
        if name in added:
            expression = join.added_expressions[name]
        elif name in dataset.merge_keys:
            expression = f"ISNULL(e.[{name}], '')"
        else:
            expression = f"e.[{name}]"
        select.append(f"    {expression} AS [{name}]")
    select.append("    e.[leverance_dato]")
    return (
        f"CREATE OR ALTER VIEW [ext].[{view_name(dataset)}] AS\n"
        f"SELECT\n" + ",\n".join(select) + f"\n{join.from_clause};"
    )


# ---------------------------------------------------------------------------
# Merge configuration
# ---------------------------------------------------------------------------


def build_merge_config(dataset: DrgDataset) -> ArchiveMergeConfig:
    merge_keys = ("takstsystem", *dataset.merge_keys)
    non_key = tuple(c for c in dataset.arc_columns if c not in dataset.merge_keys) + (
        "leverance_dato",
    )
    # leverance_dato is rewritten on every delivery by construction and
    # must not count as a change on its own (HAIBA pattern).
    change_detect = tuple(c for c in non_key if c != "leverance_dato")
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=view_name(dataset),
        target_schema="arc",
        target_table=dataset.ext_table,
        merge_keys=merge_keys,
        non_key_columns=non_key,
        change_detect_columns=change_detect,
        strategy="upsert",
        update_only_on_change=True,
        # Deliveries are full snapshots per (takstsystem, year-slice);
        # leveranceinfo is a log and must never soft-delete old rows.
        detect_deletes=not dataset.insert_only,
        upstream_is_full_load=True,
    )


MERGE_CONFIGS: dict[str, ArchiveMergeConfig] = {
    d.ext_table: build_merge_config(d) for d in DATASETS
}


def _sql_string_literal(value: str) -> str:
    """Quote a SQL string literal, doubling embedded apostrophes."""
    return "'" + str(value).replace("'", "''") + "'"


def _qualify(alias: str, identifier: str) -> str:
    col = safe_bracket_quote(identifier)
    return f"{alias}.{col}" if alias else col


def _rebind_scope_alias(predicate: str, *, from_alias: str, to_alias: str) -> str:
    """Replace ``from_alias.`` column qualifiers. ``to_alias`` may be empty."""
    old = re.compile(rf"(?<![A-Za-z0-9_]){re.escape(from_alias)}\.")
    new = f"{to_alias}." if to_alias else ""
    return old.sub(new, predicate)


def build_delete_scope_predicate(conn, dataset: DrgDataset, *, alias: str = "tgt") -> str | None:
    """Limit delete detection to the slices this delivery actually contains.

    Somatik: (takstsystem × activity-year) slices from ext. Psyk and the
    tariff catalogue: takstsystem slices. Returns None if ext is empty
    (the merge engine then turns delete detection off). ``alias`` is how
    the MERGE (and the pre-merge counts) qualify columns.
    """
    cursor = conn.cursor()
    try:
        table_ident = safe_bracket_quote(dataset.ext_table)
        if dataset.aktivitetsaar_column:
            year_ident = safe_bracket_quote(dataset.aktivitetsaar_column)
            cursor.execute(
                f"SELECT [takstsystem], {year_ident} "
                f"FROM [ext].{table_ident} "
                f"GROUP BY [takstsystem], {year_ident}"
            )
            slices: dict[str, list[str]] = {}
            for takstsystem, aar in cursor.fetchall():
                slices.setdefault(takstsystem, []).append(aar or "")
            if not slices:
                return None
            takst_col = _qualify(alias, "takstsystem")
            year_col = _qualify(alias, dataset.aktivitetsaar_column)
            parts = []
            for takstsystem in sorted(slices):
                years = ", ".join(_sql_string_literal(y) for y in sorted(slices[takstsystem]))
                parts.append(
                    f"({takst_col} = {_sql_string_literal(takstsystem)} "
                    f"AND ISNULL({year_col}, '') IN ({years}))"
                )
            return " OR ".join(parts)
        cursor.execute(f"SELECT DISTINCT [takstsystem] FROM [ext].{table_ident}")
        systems = sorted(row[0] for row in cursor.fetchall())
        if not systems:
            return None
        values = ", ".join(_sql_string_literal(s) for s in systems)
        return f"{_qualify(alias, 'takstsystem')} IN ({values})"
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def guard_delivery_direction(conn, dataset: DrgDataset, backfill: bool) -> dict[str, str]:
    """An older delivery must never merge over a newer one — per takstsystem."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT [takstsystem], MAX([leverance_dato]) "
            f"FROM [ext].[{dataset.ext_table}] GROUP BY [takstsystem]"
        )
        ext_dates = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.execute(
            f"SELECT [takstsystem], MAX([leverance_dato]) FROM [meta].[{LEVERANCE_TABLE}] "
            f"WHERE [kildetabel] = ? GROUP BY [takstsystem]",
            dataset.ext_table,
        )
        arc_dates = {row[0]: row[1] for row in cursor.fetchall()}
    finally:
        cursor.close()

    for takstsystem, ext_date in ext_dates.items():
        arc_date = arc_dates.get(takstsystem)
        if arc_date is None:
            continue
        if backfill:
            if str(ext_date) >= str(arc_date):
                raise ValueError(
                    f"arc.{dataset.ext_table}/{takstsystem}: backfill kraever en AELDRE "
                    f"leverance end arkivets ({ext_date} >= {arc_date})"
                )
        elif str(ext_date) < str(arc_date):
            raise ValueError(
                f"arc.{dataset.ext_table}/{takstsystem}: ext indeholder leverancen "
                f"{ext_date}, men arkivet afspejler allerede {arc_date}. En aeldre "
                f"leverance maa ikke merges oven i en nyere — brug backfill_historical "
                f"hvis det er tilsigtet."
            )
    return {t: str(d) for t, d in ext_dates.items()}


def guard_resolved_references(conn, dataset: DrgDataset) -> None:
    """Every ext row must survive the resolution view 1:1.

    Resolved keys come from INNER JOINs, so an unresolvable reference never
    becomes NULL — it silently drops the row from the view, and the merge
    would then soft-delete the matching archive row as "gone". The guard is
    therefore ROW PARITY between the ext table and the view: fewer rows =
    unresolvable references, more = a join fanned out (both are delivery
    defects). Datasets with no joins are skipped.
    """
    if "JOIN" not in _VIEW_JOINS[dataset.ext_table].from_clause:
        return
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT (SELECT COUNT(*) FROM [ext].[{dataset.ext_table}]) - "
            f"(SELECT COUNT(*) FROM [ext].[{view_name(dataset)}])"
        )
        difference = cursor.fetchone()[0]
    finally:
        cursor.close()
    if difference:
        problem = (
            f"{difference:,} raekker tabt (uoploeselig parent-reference)"
            if difference > 0
            else f"{-difference:,} raekker duplikeret (join faner ud)"
        )
        raise ValueError(
            f"ext.{view_name(dataset)}: {problem} i oploesnings-viewet — "
            f"leverancedefekt, undersoeg foer merge. Uden dette vaern ville "
            f"mergen soft-slette de tabte raekkers arkivmodstykker."
        )


def guard_key_uniqueness(conn, dataset: DrgDataset) -> int:
    """Duplicates on the merge key: conflicting ones fail, identical ones don't.

    Uniqueness is a measured property, not an SDS guarantee. Two kinds of
    duplicate can break it, and they deserve opposite answers:

    * CONFLICTING — same key, different values. Dropping one loses
      information, and which one survives would decide what the archive
      believes. Fails the run; the fallback in the key memo (rebuild per
      contact) must then be activated deliberately.
    * IDENTICAL — the same row delivered twice, byte for byte. Nothing is
      lost by keeping one copy, and failing the whole table over it means
      archiving nothing at all. Measured in the dev environment 2026-08-27:
      SDS shipped one such pair in T24 psyk diagnoses — 2 rows out of
      2,254,178, byte-identical.

    Returns the number of key groups holding identical duplicates, so the
    caller can enable the engine's deterministic source dedup for THIS run
    only — dedup is never on by default, and only ever after this guard has
    proven there is no conflict to hide.
    """
    if not dataset.merge_keys:
        return 0
    key_columns = ("takstsystem", *dataset.merge_keys)
    keys = ", ".join(f"[{k}]" for k in key_columns)
    all_columns = ", ".join(
        f"[{c}]" for c in ("takstsystem", *dataset.arc_columns, "leverance_dato")
    )
    view = view_name(dataset)
    cursor = conn.cursor()
    try:
        # Groups sharing a key, split by whether the FULL rows differ.
        cursor.execute(
            f"""
            WITH dubletter AS (
                SELECT {keys}
                FROM [ext].[{view}]
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            ),
            distinkte AS (
                SELECT {keys}, COUNT(*) AS fulde_varianter
                FROM (SELECT DISTINCT {all_columns} FROM [ext].[{view}]) AS unikke
                GROUP BY {keys}
            )
            SELECT
                SUM(CASE WHEN d.fulde_varianter > 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN d.fulde_varianter = 1 THEN 1 ELSE 0 END)
            FROM dubletter AS b
            JOIN distinkte AS d ON {" AND ".join(f"d.[{k}] = b.[{k}]" for k in key_columns)}
            """
        )
        row = cursor.fetchone()
        conflicting = int(row[0] or 0)
        identical = int(row[1] or 0)
    finally:
        cursor.close()
    if conflicting:
        raise ValueError(
            f"ext.{view}: {conflicting:,} noeglegrupper har dubletter med "
            f"FORSKELLIGE vaerdier paa ({keys}) — unikhedsantagelsen holder "
            f"ikke for denne leverance, og en dedup ville afgoere hvilken "
            f"vaerdi arkivet tror paa. Se fallback-strategien i "
            f"drg_noegler_beslutningsnotat.md."
        )
    return identical


def count_live_in_scope(conn, dataset: DrgDataset, scope_predicate: str) -> int:
    """Live archive rows inside the delete scope — the ratio guard's denominator.

    Counted BEFORE the merge ("live population before this run's deletes").
    The predicate is built with tgt. qualifiers; here the table is queried
    directly, so the alias is rebound.
    """
    predicate = _rebind_scope_alias(scope_predicate, from_alias="tgt", to_alias="")
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM [arc].[{dataset.ext_table}] "
            f"WHERE [dw_is_deleted] = 0 AND ({predicate})"
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def count_expected_soft_deletes(conn, dataset: DrgDataset, scope_predicate: str) -> int:
    """Live archive rows in scope with no counterpart in the delivery.

    Counted BEFORE the merge so the soft-delete ceiling can fire before
    anything is committed: the merge is not transactional with the guard,
    so a post-merge check would leave the mass-delete in the archive — and
    a retry would see 0 new soft-deletes and go green.
    """
    predicate = _rebind_scope_alias(scope_predicate, from_alias="tgt", to_alias="arc_tgt")
    on_clause = " AND ".join(
        f"src.[{k}] = arc_tgt.[{k}]" for k in ("takstsystem", *dataset.merge_keys)
    )
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT COUNT(*) FROM [arc].[{dataset.ext_table}] AS arc_tgt "
            f"WHERE arc_tgt.[dw_is_deleted] = 0 AND ({predicate}) "
            f"AND NOT EXISTS (SELECT 1 FROM [ext].[{view_name(dataset)}] AS src "
            f"WHERE {on_clause})"
        )
        return cursor.fetchone()[0]
    finally:
        cursor.close()


def capture_price_revisions(conn, dataset: DrgDataset) -> int:
    """Log changed price columns to arc.drg_pris_revision BEFORE the merge.

    The log commits in its own transaction ahead of the merge (the merge
    runs on the engine's own connection). If the merge then fails, a retry
    would see the same changes again — the NOT EXISTS guard makes the
    insert idempotent so revisions are never double-counted.
    """
    if not dataset.revision_columns:
        return 0
    key_column = dataset.merge_keys[0]
    on_clause = " AND ".join(f"src.[{k}] = tgt.[{k}]" for k in ("takstsystem", *dataset.merge_keys))
    total = 0
    cursor = conn.cursor()
    try:
        for column in dataset.revision_columns:
            cursor.execute(
                f"""
                INSERT INTO [arc].[{REVISION_TABLE}]
                    ([kildetabel], [takstsystem], [noegle], [kolonne],
                     [vaerdi_foer], [vaerdi_efter], [leverance_dato])
                SELECT ?, tgt.[takstsystem], tgt.[{key_column}], ?,
                       tgt.[{column}], src.[{column}], src.[leverance_dato]
                FROM [arc].[{dataset.ext_table}] AS tgt
                JOIN [ext].[{view_name(dataset)}] AS src ON {on_clause}
                WHERE tgt.[dw_is_deleted] = 0
                  AND EXISTS (SELECT tgt.[{column}] EXCEPT SELECT src.[{column}])
                  AND NOT EXISTS (
                      SELECT 1 FROM [arc].[{REVISION_TABLE}] AS r
                      WHERE r.[kildetabel] = ?
                        AND r.[takstsystem] = tgt.[takstsystem]
                        AND r.[noegle] = tgt.[{key_column}]
                        AND r.[kolonne] = ?
                        AND r.[leverance_dato] = src.[leverance_dato]
                        AND EXISTS (SELECT r.[vaerdi_foer] INTERSECT SELECT tgt.[{column}])
                        AND EXISTS (SELECT r.[vaerdi_efter] INTERSECT SELECT src.[{column}])
                  )
                """,
                dataset.ext_table,
                column,
                dataset.ext_table,
                column,
            )
            total += cursor.rowcount if cursor.rowcount and cursor.rowcount > 0 else 0
        conn.commit()
    finally:
        cursor.close()
    return total


def record_delivery(conn, dataset: DrgDataset, ext_dates: dict[str, str], result) -> None:
    """Upsert meta.DRGLeverance per (source table, takstsystem, delivery date).

    Runs AFTER the merge commits: if registration fails the guard becomes
    permissive (not blocking), and the next run repairs it.
    """
    single = len(ext_dates) == 1
    cursor = conn.cursor()
    try:
        for takstsystem, leverance_dato in ext_dates.items():
            cursor.execute(
                f"""
                MERGE [meta].[{LEVERANCE_TABLE}] AS tgt
                USING (SELECT ? AS kildetabel, ? AS takstsystem, ? AS leverance_dato) AS src
                ON tgt.[kildetabel] = src.kildetabel
                   AND tgt.[takstsystem] = src.takstsystem
                   AND tgt.[leverance_dato] = src.leverance_dato
                WHEN MATCHED THEN UPDATE SET
                    tgt.[indlaesningstype] = ?,
                    tgt.[raekker_indsat] = ?,
                    tgt.[raekker_opdateret] = ?,
                    tgt.[raekker_soft_slettet] = ?,
                    tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS DATETIME2(0))
                WHEN NOT MATCHED THEN INSERT
                    ([kildetabel], [takstsystem], [leverance_dato], [indlaesningstype],
                     [raekker_indsat], [raekker_opdateret], [raekker_soft_slettet])
                    VALUES (src.kildetabel, src.takstsystem, src.leverance_dato,
                            ?, ?, ?, ?);
                """,
                dataset.ext_table,
                takstsystem,
                leverance_dato,
                "merge",
                result.rows_inserted if single else None,
                result.rows_updated if single else None,
                result.rows_soft_deleted if single else None,
                "merge",
                result.rows_inserted if single else None,
                result.rows_updated if single else None,
                result.rows_soft_deleted if single else None,
            )
        conn.commit()
    finally:
        cursor.close()


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


class DrgArchiveConfig(RunModeConfig):
    backfill_historical: bool = Field(
        default=False,
        description=(
            "Load a delivery older than what the archive already holds. "
            "Turns off delete-detection, resurrection and the price-revision "
            "log so newer rows are not soft-deleted and reverse revisions are "
            "not written. Updates/inserts values only — never touches "
            "dw_is_deleted. Always re-run the newest delivery afterwards. "
            "Never use for a current delivery."
        ),
    )


def _run_archive(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    dataset: DrgDataset,
    config: DrgArchiveConfig,
) -> dg.MaterializeResult:
    merge_config = MERGE_CONFIGS[dataset.ext_table]

    if config.backfill_historical:
        # An older delivery must neither soft-delete rows that arrived later
        # nor write reverse price revisions (arc's newer value -> the old
        # source's) into the log. detect_deletes=False also turns off the
        # engine's resurrection, so backfill never touches dw_is_deleted —
        # it only updates/inserts values. Always re-run the newest delivery
        # after a backfill.
        merge_config = dataclasses.replace(merge_config, detect_deletes=False)
        context.log.warning(
            "backfill_historical: delete-detection, genoplivning og "
            "prisrevisionslog er slaaet fra for denne koersel — koer nyeste "
            "leverance bagefter."
        )

    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(build_view_sql(dataset))
            conn.commit()
        finally:
            cursor.close()

        ext_dates = guard_delivery_direction(conn, dataset, config.backfill_historical)
        guard_resolved_references(conn, dataset)
        identiske_dubletter = guard_key_uniqueness(conn, dataset)
        if identiske_dubletter:
            # The guard has proven the duplicates are byte-identical, so the
            # engine's deterministic ROW_NUMBER dedup cannot pick a "wrong"
            # row — there is only one row to pick. Enabled for THIS run only;
            # a conflicting duplicate would have failed the guard above.
            merge_config = dataclasses.replace(merge_config, dedup_source_on_keys=True)
            context.log.warning(
                f"ext.{view_name(dataset)}: {identiske_dubletter:,} noeglegrupper "
                f"har BYTE-IDENTISKE dubletter — kilden har leveret samme raekke "
                f"flere gange. Dedup slaaet til for denne koersel (ingen "
                f"information tabes); konfliktende dubletter fejler fortsat."
            )
        revisions = 0
        if not config.backfill_historical:
            revisions = capture_price_revisions(conn, dataset)
        if revisions:
            context.log.info(
                f"arc.{REVISION_TABLE}: {revisions:,} prisaendringer logget for "
                f"{dataset.ext_table}"
            )

        live_in_scope: int | None = None
        expected_soft_deletes = 0
        if merge_config.detect_deletes:
            scope = build_delete_scope_predicate(conn, dataset)
            if scope:
                merge_config = dataclasses.replace(merge_config, delete_scope_predicate=scope)
                context.log.info(f"delete-scope: {scope}")
                live_in_scope = count_live_in_scope(conn, dataset, scope)
                expected_soft_deletes = count_expected_soft_deletes(conn, dataset, scope)

    # Ceiling is enforced BEFORE the merge on the expected count: the merge
    # is not transactional with the guard, so a post-check alone would leave
    # the mass-delete committed — and the retry would see 0 new soft-deletes
    # and go green.
    ratio_live = (
        live_in_scope
        if live_in_scope is not None and live_in_scope >= RATIO_MIN_LIVE_ROWS
        else None
    )
    check_soft_delete_threshold(
        f"arc_{dataset.ext_table}",
        expected_soft_deletes,
        context,
        override=config.soft_delete_threshold,
        live_row_count=ratio_live,
        ratio=SOFT_DELETE_RATIO,
    )

    result = execute_archive_merge(context, mssql, merge_config, config)

    # Defense-in-depth: the actual count must match the expected.
    check_soft_delete_threshold(
        f"arc_{dataset.ext_table}",
        result.rows_soft_deleted,
        context,
        override=config.soft_delete_threshold,
        live_row_count=ratio_live,
        ratio=SOFT_DELETE_RATIO,
    )

    with mssql.get_connection() as conn:
        record_delivery(conn, dataset, ext_dates, result)

    metadata = build_archive_metadata(
        result,
        config,
        source_table=f"ext.{view_name(dataset)}",
        target_table=f"arc.{dataset.ext_table}",
    )
    metadata["leverancer"] = ext_dates
    metadata["pris_revisioner"] = revisions
    metadata["identiske_kildedubletter"] = identiske_dubletter
    if live_in_scope is not None:
        metadata["levende_raekker_i_scope"] = live_in_scope
    return dg.MaterializeResult(metadata=metadata)


def _build_asset(dataset: DrgDataset):
    extract_dep = "ext_drg" if dataset.leverancetype == "DRG" else "ext_drgpsyk"

    @dg.asset(
        name=f"arc_{dataset.ext_table}",
        group_name="drg_archive",
        compute_kind="mssql",
        deps=[extract_dep],
        tags={"domain": "drg", "layer": "archive", "source": "drg"},
        description=(
            f"MERGE ext.{view_name(dataset)} (UUID-resolved view) into "
            f"arc.{dataset.ext_table} with year-slice scoped delete detection."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
        config: DrgArchiveConfig,
    ) -> dg.MaterializeResult:
        return _run_archive(context, mssql_target_staging, dataset, config)

    return _asset


drg_archive_assets = [_build_asset(d) for d in DATASETS]

__all__ = [
    "DrgArchiveConfig",
    "MERGE_CONFIGS",
    "build_delete_scope_predicate",
    "build_merge_config",
    "build_view_sql",
    "count_expected_soft_deletes",
    "count_live_in_scope",
    "drg_archive_assets",
    "guard_delivery_direction",
    "guard_key_uniqueness",
    "guard_resolved_references",
    "view_name",
]
