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
     believes; CONTENT-IDENTICAL duplicates (equal apart from declared
     registration-id columns) enable the engine's deterministic dedup for
     that run only, since keeping one of two identical copies loses
     nothing. See drg_noegler_beslutningsnotat.md.
  4. price revision log  — changed price columns are captured to
     arc.drg_pris_revision BEFORE the merge overwrites them (idempotent via
     NOT EXISTS)
"""

import dataclasses
import re
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
from DSI.csv_files.drg import CAP_DERIVED, CAP_REAL, drg_delivery_unit, manifest_table
from DSI.csv_files.drg_datasets import DATASETS, DrgDataset
from utils.sql_safety import safe_bracket_quote

LEVERANCE_TABLE = "DRGLeverance"
# meta.DRGLeverance.aktivitetsaar is NVARCHAR(4) — a key column, deliberately
# narrow. ext's year column is NVARCHAR(500) and validated by nothing, so the
# width is checked before anything is merged (the [internal ticket] lesson).
MAX_AKTIVITETSAAR_WIDTH = 4
REVISION_TABLE = "drg_pris_revision"

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


def read_ext_slices(conn, dataset: DrgDataset) -> dict[str, list[str]]:
    """The slices ext actually holds, as ``takstsystem -> activity years``.

    Datasets with no activity-year column (psyk, the tariff catalogue, the
    delivery log) report ``[""]`` per takstsystem: their deliveries are full
    snapshots per takstsystem, so the takstsystem IS the slice.
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
                year = aar or ""
                if len(year) > MAX_AKTIVITETSAAR_WIDTH:
                    # meta.DRGLeverance.aktivitetsaar is a narrow key column.
                    # Catching this here fails the run BEFORE the merge; left
                    # to record_delivery it would fail AFTER the merge
                    # committed, leaving arc updated and its cap unrecorded.
                    raise ValueError(
                        f"ext.{dataset.ext_table}: aktivitetsaar er "
                        f"{len(year)} tegn (max {MAX_AKTIVITETSAAR_WIDTH}) for "
                        f"takstsystem {takstsystem}. Leverancen er defekt eller "
                        f"skemaet har aendret sig — undersoeg foer indlaesning."
                    )
                slices.setdefault(takstsystem, []).append(year)
            return {t: sorted(set(years)) for t, years in slices.items()}
        cursor.execute(f"SELECT DISTINCT [takstsystem] FROM [ext].{table_ident}")
        return {row[0]: [""] for row in cursor.fetchall()}
    finally:
        cursor.close()


def build_slice_predicate(
    dataset: DrgDataset, slices: dict[str, list[str]], *, alias: str = "tgt"
) -> str | None:
    """A T-SQL predicate selecting exactly ``slices``.

    ONE builder for both scopes on purpose. The source scope and the delete
    scope must describe the same slice set — narrowing the source while delete
    detection still covers a slice makes WHEN NOT MATCHED BY SOURCE soft-delete
    that whole slice. Deriving both from one function makes them
    impossible to drift apart.

    ``alias`` is how the consumer qualifies columns: "tgt" for the MERGE's
    delete clause, "src" for joins against the source view, "" for the
    derived table the source scope is applied inside.
    """
    if not slices:
        return None
    if dataset.aktivitetsaar_column:
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
    values = ", ".join(_sql_string_literal(t) for t in sorted(slices))
    return f"{_qualify(alias, 'takstsystem')} IN ({values})"


# ---------------------------------------------------------------------------
# Delivery manifest and the slice-level cap decision ([internal ticket] tier 2)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DeliveryManifest:
    """One delivery this run loaded, as recorded by the extract."""

    leverancetype: str
    takstsystem: str
    kadence: str | None
    leverance_ts: datetime
    filnavn: str
    leverance_dato: date
    cap: str | None
    cap_kilde: str | None


def capless_takstsystemer(manifest: dict[str, DeliveryManifest]) -> list[str]:
    """Takstsystemer whose delivery carries no TRUSTWORTHY cap.

    No cap at all (psyk ships no LEVERANCEINFO), or a derived one (the
    packaging date overstates an M file's real cap): for these the
    packaging-date guard stays the protection — a derived cap must not switch
    both guards off at once.
    """
    return sorted(t for t, m in manifest.items() if not m.cap or m.cap_kilde == CAP_DERIVED)


def read_manifest(conn, dataset: DrgDataset) -> dict[str, DeliveryManifest]:
    """See :func:`read_manifest_by_type` — this is the per-dataset spelling."""
    return read_manifest_by_type(conn, dataset.leverancetype)


def read_manifest_by_type(conn, leverancetype: str) -> dict[str, DeliveryManifest]:
    """What the extract loaded this run, per takstsystem.

    An empty manifest means the run loaded nothing — a declared no-op, not an
    invitation to re-merge whatever ext still holds from an earlier run.
    """
    table = safe_bracket_quote(manifest_table(leverancetype))
    cursor = conn.cursor()
    try:
        # The manifest is created by the extract. Re-materialising an arc asset
        # on its own against an ext from before this feature deployed is an
        # ordinary operator action, and it must not die on "Invalid object
        # name" — no manifest means this run loaded nothing, which is exactly
        # the declared no-op.
        cursor.execute(f"SELECT OBJECT_ID(N'[ext].{table}')")
        if cursor.fetchone()[0] is None:
            return {}
        cursor.execute(
            f"SELECT [leverancetype], [takstsystem], [kadence], [leverance_ts], [filnavn], "
            f"[leverance_dato], [lpropdateringsdato], [cap_kilde] FROM [ext].{table}"
        )
        rows = cursor.fetchall()
    finally:
        cursor.close()
    return {
        row[1]: DeliveryManifest(
            leverancetype=row[0],
            takstsystem=row[1],
            kadence=row[2],
            leverance_ts=row[3],
            filnavn=row[4],
            leverance_dato=row[5],
            cap=row[6],
            cap_kilde=row[7],
        )
        for row in rows
    }


def recorded_caps(conn, dataset: DrgDataset) -> dict[tuple[str, str], str]:
    """The data cap the archive reflects, per (takstsystem, aktivitetsaar).

    MAX over the accepted deliveries for the slice: only slices that were
    actually merged get a row in meta.DRGLeverance, so the maximum is the
    newest cap the archive really stands on. Derived caps are excluded — they
    overstate the real cap and would reject the next real delivery.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT [takstsystem], [aktivitetsaar], MAX([lpropdateringsdato]) "
            f"FROM [meta].[{LEVERANCE_TABLE}] "
            f"WHERE [kildetabel] = ? AND [lpropdateringsdato] IS NOT NULL "
            f"AND [cap_kilde] = ? "
            f"GROUP BY [takstsystem], [aktivitetsaar]",
            dataset.ext_table,
            CAP_REAL,
        )
        return {(row[0], row[1] or ""): row[2] for row in cursor.fetchall() if row[2]}
    finally:
        cursor.close()


def plan_merge(
    conn, dataset: DrgDataset
) -> tuple[dict[str, DeliveryManifest], dict[str, list[str]], list[str]]:
    """What this run may merge: the manifest, the accepted slices, and the notes.

    The manifest is read FIRST and nothing touches ext until it is non-empty.
    A run where the extract found no new delivery leaves ext alone — it does not
    even recreate the tables — so reading ext here would fail on a missing
    object instead of declaring the no-op the design calls for.
    """
    manifest = read_manifest(conn, dataset)
    if not manifest:
        return manifest, {}, []
    ext_slices = read_ext_slices(conn, dataset)
    caps = recorded_caps(conn, dataset)
    accepted, notes = decide_slices(ext_slices, manifest, caps)
    return manifest, accepted, notes


def decide_slices(
    ext_slices: dict[str, list[str]],
    manifest: dict[str, DeliveryManifest],
    caps: dict[tuple[str, str], str],
) -> tuple[dict[str, list[str]], list[str]]:
    """Which slices this delivery may merge, and why the others were skipped.

    A slice is merged when the delivery's cap is STRICTLY newer than the cap
    the archive already reflects, or when the archive reflects none (first load
    of that slice). A delivery with no cap at all — psyk, which ships no
    LEVERANCEINFO — is not subject to this comparison; those slices always pass
    and the packaging-date direction guard remains their protection.

    """
    accepted: dict[str, list[str]] = {}
    notes: list[str] = []
    for takstsystem in sorted(ext_slices):
        entry = manifest.get(takstsystem)
        if entry is None:
            # ext still holds this takstsystem from an earlier run, but this
            # run did not load it. The manifest is the contract — merging it
            # would re-present stale rows as a fresh delivery.
            notes.append(f"{takstsystem}: ikke i manifestet — ext er fra en tidligere koersel")
            continue
        delivery_cap = entry.cap
        for aar in sorted(ext_slices[takstsystem]):
            recorded = caps.get((takstsystem, aar))
            if delivery_cap is None or recorded is None:
                accepted.setdefault(takstsystem, []).append(aar)
            elif delivery_cap > recorded:
                accepted.setdefault(takstsystem, []).append(aar)
            else:
                notes.append(
                    f"{takstsystem}/{aar or '-'}: springer over — leverancens cap "
                    f"{delivery_cap} er ikke nyere end arkivets {recorded}"
                )
    return accepted, notes


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def guard_delivery_direction(
    conn,
    dataset: DrgDataset,
    takstsystemer: list[str] | None = None,
) -> dict[str, str]:
    """An older delivery must never merge over a newer one — per takstsystem.

    ``takstsystemer`` limits the check to those takstsystemer whose delivery
    carries no data cap. Where a cap exists the slice-level comparison is
    strictly better: packaging dates order M against U wrongly, because an M
    file is packed days after the data it carries.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT [takstsystem], MAX([leverance_dato]) "
            f"FROM [ext].[{dataset.ext_table}] GROUP BY [takstsystem]"
        )
        ext_dates = {row[0]: row[1] for row in cursor.fetchall()}
        if takstsystemer is not None:
            ext_dates = {t: d for t, d in ext_dates.items() if t in set(takstsystemer)}
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
        if str(ext_date) < str(arc_date):
            raise ValueError(
                f"arc.{dataset.ext_table}/{takstsystem}: ext indeholder leverancen "
                f"{ext_date}, men arkivet afspejler allerede {arc_date}. En aeldre "
                f"leverance maa ikke merges oven i en nyere. Skal en tidligere "
                f"tilstand genskabes, toemmes DRG-tabellerne og genindlaeses med "
                f"delivery_on_or_before sat."
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


def guard_key_uniqueness(conn, dataset: DrgDataset, source_scope: str | None = None) -> int:
    """Duplicates on the merge key: conflicting ones fail, identical ones don't.

    Uniqueness is a measured property, not an SDS guarantee. Two kinds of
    duplicate can break it, and they deserve opposite answers:

    * CONFLICTING — same key, different values. Dropping one loses
      information, and which one survives would decide what the archive
      believes. Fails the run; the fallback in the key memo (rebuild per
      contact) must then be activated deliberately.
    * IDENTICAL — the same delivery content twice. Nothing is lost by
      keeping one copy, and failing the whole table over it means archiving
      nothing at all.

    Identity is judged on the delivery CONTENT: columns declared as
    ``registreringsid_columns`` on the dataset (per-row registration ids,
    no content of their own) are left out of the comparison.

    Returns the number of key groups holding identical duplicates, so the
    caller can enable the engine's deterministic source dedup for THIS run
    only — dedup is never on by default, and only ever after this guard has
    proven there is no conflict to hide.
    """
    if not dataset.merge_keys:
        return 0
    # Only the slices this run will actually merge can break the arc key. A
    # conflicting duplicate in a REJECTED slice must not fail the table.
    where = f"WHERE {source_scope}" if source_scope else ""
    key_columns = ("takstsystem", *dataset.merge_keys)
    keys = ", ".join(f"[{k}]" for k in key_columns)
    all_columns = ", ".join(
        f"[{c}]"
        for c in ("takstsystem", *dataset.arc_columns, "leverance_dato")
        if c not in dataset.registreringsid_columns
    )
    view = view_name(dataset)
    cursor = conn.cursor()
    try:
        # Groups sharing a key, split by whether the content columns differ.
        cursor.execute(
            f"""
            WITH dubletter AS (
                SELECT {keys}
                FROM [ext].[{view}]
                {where}
                GROUP BY {keys}
                HAVING COUNT(*) > 1
            ),
            distinkte AS (
                SELECT {keys}, COUNT(*) AS fulde_varianter
                FROM (SELECT DISTINCT {all_columns} FROM [ext].[{view}] {where}) AS unikke
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
    """Live archive rows inside the delete scope, reported as run metadata.

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


def capture_price_revisions(conn, dataset: DrgDataset, source_scope: str | None = None) -> int:
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
    # Only slices that will actually be merged may produce revisions — a
    # rejected slice never overwrites anything, so logging a "revision" for it
    # would record a change that never happened.
    if source_scope:
        on_clause += f" AND ({source_scope})"
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


def record_delivery(
    conn,
    dataset: DrgDataset,
    accepted: dict[str, list[str]],
    manifest: dict[str, DeliveryManifest],
    result,
) -> None:
    """Upsert meta.DRGLeverance per ACCEPTED (takstsystem, aktivitetsaar) slice.

    Keyed on ``leverance_ts``, not ``leverance_dato``: two deliveries can be
    packed on the same date — which is exactly why the delivery identity is a
    full timestamp — and the date in the key would let the second silently
    overwrite the first's registration.

    Only accepted slices are recorded. The register is what
    :func:`recorded_caps` reads back as "the cap this slice reflects", so a
    slice that was rejected — or merged by a run that then failed — must leave
    no trace, or the next delivery would be compared against a cap the archive
    never actually received.

    Runs AFTER the merge commits: if registration fails the cap stays behind
    and the next run re-merges the slice, which is idempotent.
    """
    rows = [(t, aar) for t in sorted(accepted) for aar in sorted(accepted[t])]
    # Row counts are per TABLE, not per slice — attributing them to one slice
    # is only honest when the run wrote exactly one.
    single = len(rows) == 1
    cursor = conn.cursor()
    try:
        for takstsystem, aktivitetsaar in rows:
            entry = manifest[takstsystem]
            cursor.execute(
                f"""
                MERGE [meta].[{LEVERANCE_TABLE}] AS tgt
                USING (SELECT ? AS kildetabel, ? AS takstsystem, ? AS aktivitetsaar,
                              ? AS leverance_ts) AS src
                ON tgt.[kildetabel] = src.kildetabel
                   AND tgt.[takstsystem] = src.takstsystem
                   AND tgt.[aktivitetsaar] = src.aktivitetsaar
                   AND tgt.[leverance_ts] = src.leverance_ts
                WHEN MATCHED THEN UPDATE SET
                    tgt.[kadence] = ?,
                    tgt.[leverance_dato] = ?,
                    tgt.[lpropdateringsdato] = ?,
                    tgt.[cap_kilde] = ?,
                    tgt.[indlaesningstype] = ?,
                    tgt.[raekker_indsat] = ?,
                    tgt.[raekker_opdateret] = ?,
                    tgt.[raekker_soft_slettet] = ?,
                    tgt.[dw_timestamp_update] = CAST(SYSDATETIME() AS DATETIME2(0))
                WHEN NOT MATCHED THEN INSERT
                    ([kildetabel], [takstsystem], [aktivitetsaar], [leverance_ts],
                     [kadence], [leverance_dato], [lpropdateringsdato], [cap_kilde],
                     [indlaesningstype], [raekker_indsat], [raekker_opdateret],
                     [raekker_soft_slettet])
                    VALUES (src.kildetabel, src.takstsystem, src.aktivitetsaar,
                            src.leverance_ts, ?, ?, ?, ?, ?, ?, ?, ?);
                """,
                dataset.ext_table,
                takstsystem,
                aktivitetsaar,
                entry.leverance_ts,
                entry.kadence,
                entry.leverance_dato,
                entry.cap,
                entry.cap_kilde,
                "merge",
                result.rows_inserted if single else None,
                result.rows_updated if single else None,
                result.rows_soft_deleted if single else None,
                entry.kadence,
                entry.leverance_dato,
                entry.cap,
                entry.cap_kilde,
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
    """Run config for the DRG archive assets.

    No backfill switch: DRG is a stand-alone solution whose archive can be
    emptied and rebuilt from the deliveries on the share, so reproducing an
    earlier state is "wipe, then load with ``delivery_on_or_before`` pinned" —
    which leaves the delivery register consistent with what the archive
    actually holds. A backfill that merged an older delivery over a newer one
    could not do that: the register would still record the newer cap, and the
    slice comparison would then refuse to re-apply the newest delivery.
    """


def _run_archive(
    context: dg.AssetExecutionContext,
    mssql: MSSQLResource,
    dataset: DrgDataset,
    config: DrgArchiveConfig,
) -> dg.MaterializeResult:
    merge_config = MERGE_CONFIGS[dataset.ext_table]

    with mssql.get_connection() as conn:
        # The manifest first: an empty one is a DECLARED no-op, and deciding
        # that before creating views or running guards is what makes a run
        # with nothing new genuinely cheap.
        manifest, accepted, skip_notes = plan_merge(conn, dataset)
        for note in skip_notes:
            context.log.info(note)
        if not accepted:
            # "superseded" and "stuck" look identical from outside — both show a
            # checkpoint that moved with nothing merged. Naming the case makes
            # the difference legible: the delivery WAS read and understood, and
            # deliberately not applied because a newer one already covers every
            # slice it carries.
            aarsag = "tomt manifest" if not manifest else "superseded — ingen nyere aarsskiver"
            context.log.info(f"arc.{dataset.ext_table}: intet at merge — {aarsag}")
            # The run log is the operational record of "it ran and decided
            # nothing", and with detection in place that is the COMMON case —
            # so a skipped run must be as visible as a merging one. The
            # checkpoint is deliberately not advanced: nothing was merged.
            dfm = DfmManager(mssql, context)
            with dfm.connection():
                run_ctx = dfm.start_dfm_run(
                    "arc",
                    dataset.ext_table,
                    dagster_run_id=context.run_id,
                    mode=config.mode,
                )
                dfm.complete_dfm_run(run_ctx.log_id, rows_affected=0)
            return dg.MaterializeResult(
                metadata={
                    "besluttet": "sprunget over",
                    "aarsag": aarsag,
                    "sprunget_over": skip_notes or ["ingen"],
                    "leverancer": {t: m.filnavn for t, m in sorted(manifest.items())},
                }
            )

        # Source scope and delete scope come from the SAME accepted set — see
        # build_slice_predicate for why they must never be derived separately.
        source_scope = build_slice_predicate(dataset, accepted, alias="")
        source_scope_src = build_slice_predicate(dataset, accepted, alias="src")
        merge_config = dataclasses.replace(merge_config, source_scope_predicate=source_scope)

        cursor = conn.cursor()
        try:
            cursor.execute(build_view_sql(dataset))
            conn.commit()
        finally:
            cursor.close()

        # Psyk ships no LEVERANCEINFO, so tier 2 cannot order its deliveries;
        # the packaging-date guard stays its protection. Where a cap exists the
        # slice comparison above IS the guard, and comparing packaging dates
        # would reject a correct M-then-U sequence (M is packed later than the
        # data it carries).
        # Per takstsystem, not per run: where a trustworthy cap exists the
        # slice comparison IS the guard, and comparing packaging dates there
        # would reject a correct M-then-U sequence. For the rest (see
        # capless_takstsystemer) the packaging-date guard remains the
        # protection.
        capless = capless_takstsystemer(manifest)
        if capless:
            guard_delivery_direction(conn, dataset, takstsystemer=capless)
        guard_resolved_references(conn, dataset)
        identiske_dubletter = guard_key_uniqueness(conn, dataset, source_scope)
        if identiske_dubletter:
            # Duplicates carry identical content and may differ only on
            # registration-id columns; dedup picks a deterministic winner.
            merge_config = dataclasses.replace(merge_config, dedup_source_on_keys=True)
            context.log.warning(
                f"ext.{view_name(dataset)}: {identiske_dubletter:,} noeglegrupper "
                f"har INDHOLDS-IDENTISKE dubletter — samme leveranceindhold "
                f"flere gange (evt. under forskellige registrerings-id'er). "
                f"Dedup slaaet til for denne koersel; konfliktende dubletter "
                f"fejler fortsat."
            )
        revisions = capture_price_revisions(conn, dataset, source_scope_src)
        if revisions:
            context.log.info(
                f"arc.{REVISION_TABLE}: {revisions:,} prisaendringer logget for "
                f"{dataset.ext_table}"
            )

        live_in_scope: int | None = None
        if merge_config.detect_deletes:
            scope = build_slice_predicate(dataset, accepted, alias="tgt")
            if scope:
                merge_config = dataclasses.replace(merge_config, delete_scope_predicate=scope)
                context.log.info(f"delete-scope: {scope}")
                live_in_scope = count_live_in_scope(conn, dataset, scope)

    result = execute_archive_merge(context, mssql, merge_config, config)

    with mssql.get_connection() as conn:
        record_delivery(conn, dataset, accepted, manifest, result)

    metadata = build_archive_metadata(
        result,
        config,
        source_table=f"ext.{view_name(dataset)}",
        target_table=f"arc.{dataset.ext_table}",
    )
    metadata["leverancer"] = {t: m.filnavn for t, m in sorted(manifest.items())}
    metadata["besluttet"] = "merget"
    metadata["accepterede_skiver"] = {
        t: ", ".join(a or "-" for a in v) for t, v in sorted(accepted.items())
    }
    metadata["sprunget_over"] = skip_notes or ["ingen"]
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


def _build_gate_asset(leverancetype: str):
    """The asset that advances the delivery checkpoints for one leverancetype.

    It depends on EVERY archive table of that leverancetype, because the
    Dagster graph is the only place where "all of them merged in this run" can
    be read: each arc asset owns only its own ("arc", <table>) checkpoint, and
    an archive-table checkpoint cannot carry unit granularity. If any merge
    fails, this asset does not run, no checkpoint moves, and the next run
    re-reads the delivery — the safe direction.

    A delivery whose slices were all superseded still advances its checkpoint:
    the file WAS evaluated and loaded, so leaving the checkpoint behind would
    re-read it on every run until a newer one arrived.
    """
    arc_assets = [f"arc_{d.ext_table}" for d in DATASETS if d.leverancetype == leverancetype]

    @dg.asset(
        name=f"drg_leverance_checkpoint_{leverancetype.lower()}",
        group_name="drg_archive",
        compute_kind="mssql",
        deps=arc_assets,
        tags={"domain": "drg", "layer": "archive", "source": "drg"},
        description=(
            f"Advance the delivery checkpoint for each {leverancetype} unit this "
            f"run ingested, once all {len(arc_assets)} archive tables have merged."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext,
        mssql_target_staging: MSSQLResource,
    ) -> dg.MaterializeResult:
        with mssql_target_staging.get_connection() as conn:
            manifest = read_manifest_by_type(conn, leverancetype)
        if not manifest:
            context.log.info(f"{leverancetype}: intet manifest — intet checkpoint at rykke")
            return dg.MaterializeResult(metadata={"stemplede_enheder": ["ingen"]})

        dfm = DfmManager(mssql_target_staging, context)
        stamped: list[str] = []
        with dfm.connection():
            for takstsystem in sorted(manifest):
                entry = manifest[takstsystem]
                # The file's cadence letter maps back to the job's bucket: only
                # the monthly job accepts a file without one, so None is M.
                unit = drg_delivery_unit(leverancetype, takstsystem, entry.kadence or "M")
                identity = delivery_identity(entry.leverance_ts)
                dfm.set_checkpoint(
                    DELIVERY_SCHEMA, unit, value=identity, target_type=DELIVERY_TARGET_TYPE
                )
                stamped.append(f"{unit} = {identity} ({entry.filnavn})")
                context.log.info(f"checkpoint {unit} -> {identity}")
        return dg.MaterializeResult(metadata={"stemplede_enheder": stamped})

    return _asset


drg_archive_assets = [_build_asset(d) for d in DATASETS] + [
    _build_gate_asset(leverancetype) for leverancetype in ("DRG", "DRGPSYK")
]

__all__ = [
    "DeliveryManifest",
    "DrgArchiveConfig",
    "MERGE_CONFIGS",
    "build_slice_predicate",
    "capless_takstsystemer",
    "decide_slices",
    "plan_merge",
    "read_ext_slices",
    "read_manifest",
    "recorded_caps",
    "build_merge_config",
    "build_view_sql",
    "count_live_in_scope",
    "drg_archive_assets",
    "guard_delivery_direction",
    "guard_key_uniqueness",
    "guard_resolved_references",
    "view_name",
]
