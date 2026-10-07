# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
dm.HAIBA_* — the dimensional HAIBA layer: map -> fact + transaction dimension.

arc.* is 1:1 with the delivery files. This layer splits each arc table three
ways:

* ``map.HAIBA_*`` mints dw_ek per clean_code — a '|'-joined concatenation of
  the arc primary key. Append-only: a clean_code keeps its dw_ek forever, also
  when the row is soft-deleted in arc, and the mint never UPDATEs or DELETEs.
* ``dm.HAIBA_*`` (fact) carries ONLY DW keys and typed measures.
* ``dm.HAIBA_*Egenskaber`` (transaction dimension) carries the source's
  descriptive columns 1:1 with the fact on dw_ek.

Deliberately absent from dm: the organisational text columns (they live in
dim.Organisation behind dw_ek_Organisation) and raw CPR — the CPR exists only
in the map clean_code and in arc, both staging-internal. dw_is_deleted is
carried onto both fact and egenskaber (decided 2026-08-24), so SDS
retractions stay visible; consumers filter dw_is_deleted = 0.

Fact and egenskaber are rebuilt in full from arc on each run; arc is the
source of truth and a full rebuild is always consistent with it.

SOR resolution: dim.Organisation.kode holds the SOR code — the
column is ``kode``, not ``code``. A code that cannot be resolved gets
dw_ek_Organisation = -1, the UNKNOWN sentinel seeded by the organisation DDL.

An empty SOR code is not a failed lookup. SORIdentifier is empty exactly when
the row is a national aggregate (RegionKode 8888/9999) — 0.4% of the infection
rows but 85% of Acute_HIP, because the orthopedic files are dominated by
national totals. Those rows get -1 too, and the run metadata counts them
separately so a report cannot mistake structure for a data problem.

Date keys come from the shared ``dato_key`` helper
(DSA/SQL/common/dato_dimension.py) and carry all four dim.Dato sentinels:
-1 unparseable, -2 before range, 99999998 blank, 99999999 after range. The
BLANK/UNKNOWN split matters here: reo_date and HAI_date are structurally
blank on real SSI rows, and "SDS has no date here" must never read as "SDS
sent something we could not parse" — the same distinction build_stats_sql
draws between tom_sor and uoploest on the SOR side. Measures are typed with
TRY_CONVERT; the conversion counter tracks losses (non-blank source that
converted to NULL) into the run metadata, so a format change from SDS
surfaces instead of silently zeroing a KPI. The NULLIF inside every measure
conversion is load-bearing: TRY_CONVERT(INT, '') returns 0 — a "successful"
conversion of a blank.

dw_ek_Borger resolves through ref.PatientToBorger. The encounter
relation is stamped afterwards by DSA/SQL/dm/haiba_kontakt.py —
the relation keys (encounter_id_*, location_id_*) onto the fact so the join
out to the contact is direct, the match quality onto the egenskaber table.
"""

from dataclasses import dataclass

import dagster as dg
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from DSA.SQL.common.dato_dimension import dato_key
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    DELIVERY_COLUMN,
    HaibaSchema,
)

UNKNOWN_ORGANISATION = -1
# dim.Borger's UNKNOWN sentinel (-2 means N/A, which does not apply here).
UNKNOWN_BORGER = -1

# A steady-state run mints near zero new map rows outside SDS's January
# window. Minting more than this fraction of the source in one run means the
# clean_code components changed shape (SDS format change, or an edit to
# schema.key_columns) and the whole history is being silently re-keyed —
# every consumer's stored dw_ek then points at a different row while row
# counts, konverteringstab and the parity guard all stay green. The first
# run ever legitimately mints 100%, so the check only arms on a non-empty map.
REKEY_WARN_FRACTION = 0.5

# Redundant with dim.Organisation behind dw_ek_Organisation — never copied to
# dm. HAI_RegionKode is deliberately NOT in the cases tuple: like RegionKode
# on the agg side it carries the national-aggregate distinction (8888/9999)
# and is the only recoverable region when the SOR lookup lands on -1, so it
# stays on the case egenskaber tables.
_ORG_TEXT_AGG = (
    "ejerforhold",
    "region",
    "Sundhedsinstitution",
    "Matrikel",
    "OverliggendeEnhed",
    "AfdelingAfsnit",
)
_ORG_TEXT_CASES = (
    "HAI_Region",
    "HAI_ejerforhold",
    "HAI_Matrikel",
    "HAI_Sundhedsinstitution",
    "HAI_OverliggendeEnhed",
    "HAI_AfdelingAfsnit",
)


@dataclass(frozen=True)
class DmTable:
    """One arc table and the map/fact/egenskaber trio it rebuilds into.

    ``sor_roles`` maps the source column carrying a SOR code to the fact key
    column that receives the lookup — seven such columns across the four
    tables. ``date_roles`` does the same for date strings to YYYYMMDD keys,
    and ``measures`` for measure strings to typed fact columns
    (source, target, SQL type).
    """

    schema: HaibaSchema
    dm_table: str
    sor_roles: tuple[tuple[str, str], ...]
    date_roles: tuple[tuple[str, str], ...]
    measures: tuple[tuple[str, str, str], ...]
    # (qualified dm table, index name) — disabled during the rebuild and
    # rebuilt after, as maintaining them row by row during an 8M-row insert
    # costs more than rebuilding them once.
    nonclustered_indexes: tuple[tuple[str, str], ...] = ()
    # Case tables carry a CPR and therefore resolve a dw_ek_Borger; the agg
    # tables have no person data at all. The CPR itself never reaches dm.
    cpr_column: str | None = None

    @property
    def arc_table(self) -> str:
        return self.schema.ext_table

    @property
    def dw_ek_column(self) -> str:
        return f"dw_ek_{self.dm_table}"

    @property
    def egenskaber_table(self) -> str:
        # PascalCase, no separating underscore: HAIBA_CasesSSIEgenskaber.
        return f"{self.dm_table}Egenskaber"

    @property
    def clean_code_expr(self) -> str:
        """'|'-joined arc primary key, computed on alias ``a``.

        The key columns are NOT NULL and carry '' rather than NULL (see the
        arc DDL), so plain CONCAT is deterministic and collision-free — '|'
        cannot appear inside any of them (SOR ids, ISO dates, CPR, enums).
        """
        parts = ", N'|', ".join(f"a.[{c}]" for c in self.schema.key_columns)
        return f"CONCAT({parts})"

    @property
    def egenskaber_columns(self) -> tuple[str, ...]:
        """Source columns copied 1:1 to the egenskaber table.

        Everything from arc except the organisational text columns, the CPR,
        the measure sources and leverance_dato (inserted explicitly). Derived
        from the extract schema so a new source column cannot silently fall
        between fact and egenskaber — the DDL-parity test catches it instead.
        """
        dropped = set(_ORG_TEXT_AGG if self.schema.kind == "agg" else _ORG_TEXT_CASES)
        dropped.update(src for src, _, _ in self.measures)
        dropped.add(DELIVERY_COLUMN)
        if self.cpr_column:
            dropped.add(self.cpr_column)
        return tuple(c for c in self.schema.all_columns if c not in dropped)


DM_TABLES = (
    DmTable(
        AGG_INFEKTION,
        "HAIBA_AggInfektion",
        sor_roles=(("SORIdentifier", "dw_ek_Organisation"),),
        date_roles=(("HAIBA_dato", "dw_ek_Dato"),),
        measures=(
            ("antal", "antal", "INT"),
            ("risikodoegn", "risikodoegn", "DECIMAL(18,2)"),
        ),
        nonclustered_indexes=(
            ("HAIBA_AggInfektion", "IX_dm_HAIBA_Agg_Infektion_Org"),
            ("HAIBA_AggInfektion", "IX_dm_HAIBA_Agg_Infektion_Dato"),
        ),
    ),
    DmTable(
        AGG_ORTOPAEDI,
        "HAIBA_AggOrtopaedi",
        sor_roles=(("SORIdentifier", "dw_ek_Organisation"),),
        date_roles=(("HAIBA_dato", "dw_ek_Dato"),),
        measures=(
            ("antal90", "antal90", "INT"),
            ("antal365", "antal365", "INT"),
            ("operationer", "operationer", "INT"),
        ),
        nonclustered_indexes=(
            ("HAIBA_AggOrtopaedi", "IX_dm_HAIBA_Agg_Ortopaedi_Org"),
            ("HAIBA_AggOrtopaedi", "IX_dm_HAIBA_Agg_Ortopaedi_Dato"),
        ),
    ),
    DmTable(
        CASES_INFEKTION,
        "HAIBA_CasesInfektion",
        sor_roles=(
            ("HAI_Opholdsadresse", "dw_ek_Organisation_opholdsadresse"),
            ("HAI_Kontaktansvar", "dw_ek_Organisation_kontaktansvar"),
        ),
        date_roles=(("HAI_date", "dw_ek_Dato_hai"),),
        measures=(),
        nonclustered_indexes=(
            ("HAIBA_CasesInfektion", "IX_dm_HAIBA_Cases_Infektion_Borger"),
            ("HAIBA_CasesInfektion", "IX_dm_HAIBA_Cases_Infektion_Org"),
            ("HAIBA_CasesInfektion", "IX_dm_HAIBA_Cases_Infektion_Dato"),
            (
                "HAIBA_CasesInfektionEgenskaber",
                "IX_dm_HAIBA_Cases_Infektion_Egenskaber_Kontakt",
            ),
        ),
        cpr_column="cprnr",
    ),
    DmTable(
        CASES_SSI,
        "HAIBA_CasesSSI",
        sor_roles=(
            ("HAI_producent", "dw_ek_Organisation_producent"),
            ("HAI_kontaktansvarlig", "dw_ek_Organisation_kontaktansvarlig"),
            ("reo_producent", "dw_ek_Organisation_reo_producent"),
            ("reo_kontaktansvarlig", "dw_ek_Organisation_reo_kontaktansvarlig"),
        ),
        date_roles=(
            ("operation_date", "dw_ek_Dato_operation"),
            ("HAI_date", "dw_ek_Dato_hai"),
            ("reo_date", "dw_ek_Dato_reo"),
        ),
        # Measure names are the source names verbatim, same rule as the agg
        # tables (antal, risikodoegn). SSI_acute_* is TINYINT, not BIT, until
        # the 0/1 encoding is confirmed against a delivery: TRY_CONVERT(BIT,
        # '2') returns 1 — any non-zero numeric string converts "successfully",
        # so the conversion counter provably cannot see a deviating encoding.
        # TINYINT keeps the deviation visible in the data itself.
        measures=(
            ("HAI_kamme_positive_biopsies", "HAI_kamme_positive_biopsies", "INT"),
            ("HAI_kamme_total_biopsies", "HAI_kamme_total_biopsies", "INT"),
            ("SSI_acute_90", "SSI_acute_90", "TINYINT"),
            ("SSI_acute_365", "SSI_acute_365", "TINYINT"),
        ),
        nonclustered_indexes=(
            ("HAIBA_CasesSSI", "IX_dm_HAIBA_Cases_SSI_Borger"),
            ("HAIBA_CasesSSI", "IX_dm_HAIBA_Cases_SSI_Dato_operation"),
        ),
        cpr_column="cprnr",
    ),
)


def _measure_expr(source: str, sql_type: str) -> str:
    """Typed measure, NULL on blank or unparseable.

    The NULLIF is load-bearing here too: TRY_CONVERT(INT, '') returns 0, not
    NULL — a blank count must land as NULL, never as a zero that sums cleanly.
    """
    return f"TRY_CONVERT({sql_type}, NULLIF(a.[{source}], ''))"


def build_rebuild_sql(table: DmTable) -> str:
    """Mint + TRUNCATE/INSERT batch that rebuilds one map/fact/egenskaber trio.

    The mint runs first and is append-only: NOT EXISTS against the map, never
    an UPDATE or DELETE, so a clean_code keeps its dw_ek across runs and
    soft-deletes. Both dm INSERTs then INNER JOIN the map on the same
    clean_code expression — every arc row was just minted, so the joins are
    lossless and the row-count guard in ``_rebuild`` verifies exactly that.

    The SOR lookup goes through a materialised #org rather than joining
    dim.Organisation directly, and that is not premature optimisation:

    dim.Organisation holds 165,477 rows. Comparing its varchar ``kode`` to the
    source's nvarchar SOR column forces a conversion — nvarchar wins type
    precedence — which makes the predicate unseekable. On the 8M-row infection
    table the optimiser answered that with Nested Loops over a Table Spool,
    i.e. a scan of 165k rows per source row. That plan burned 600+ seconds of
    CPU and 19.6M logical reads without finishing.

    Copying dim into a temp table with the key already cast to nvarchar and a
    unique clustered index on it removes the conversion from the predicate, so
    the join is on matching types against an indexed key. The schema only
    guarantees ``kode`` uniqueness among LIVE rows (UQ_dim_Organisation_kode_Live
    is filtered on dw_is_deleted = 0; a retired organisation keeps its row, and
    its kode can be reissued under a new key) — so soft-deleted rows are
    filtered both to keep the unique index build valid and so facts never
    resolve to a retired organisation. Unique kode means the join cannot
    multiply rows — which would otherwise break the dm primary key.

    ``WITH (TABLOCK)`` keeps the inserts minimally logged, and nonclustered
    indexes are disabled during the load and rebuilt after, mirroring the fast
    path in core/archive_merge.py. Note that TABLOCK holds an exclusive table
    lock for the duration: readers of these tables wait while they rebuild.
    """
    cc = table.clean_code_expr
    dw_ek = table.dw_ek_column

    org_joins = "\n".join(
        f"LEFT JOIN #org AS o{i} ON o{i}.[kode] = a.[{raw}] AND a.[{raw}] <> ''"
        for i, (raw, _) in enumerate(table.sor_roles)
    )
    org_select = ", ".join(
        f"COALESCE(o{i}.[dw_ek_Organisation], {UNKNOWN_ORGANISATION})"
        for i, _ in enumerate(table.sor_roles)
    )
    org_cols = [key for _, key in table.sor_roles]

    # COALESCE folds NULL into the blank branch: the extract stores a blank
    # non-key CSV field as NULL (iter_rows: `value or None`), and dato_key
    # sends NULL to -1 UNKNOWN — but a blank delivery field means "SDS has no
    # date here" (no reoperation, no infection) and must land as 99999998
    # BLANK, never read as a data problem. Key-column dates (cases_infektion
    # HAI_date, agg HAIBA_dato) are stored as '' and already hit the blank
    # branch; the COALESCE is a no-op for those.
    dato_select = ", ".join(dato_key(f"COALESCE(a.[{raw}], N'')") for raw, _ in table.date_roles)
    dato_cols = [key for _, key in table.date_roles]

    measure_select = ", ".join(_measure_expr(src, typ) for src, _, typ in table.measures)
    measure_cols = [target for _, target, _ in table.measures]

    # ref.PatientToBorger mints dw_ek_Borger per patient_id (IDENTITY, merged
    # on patient_id), and dim.Borger has no CPR-level survivorship on top — so
    # a CPR with two patient_ids carries two genuinely DISTINCT citizen keys,
    # not duplicate rows. The MIN() picks one deterministically because the dm
    # primary key needs exactly one; it does not deduplicate anything. The
    # cost is real and therefore surfaced, not hidden: the case attaches to
    # the lower key, per-citizen aggregates split across the keys, and the
    # kontakt stamp only searches contacts reachable from the key on the fact
    # row. _rebuild counts the affected CPRs into the run metadata
    # (cpr_med_flere_borgere) so one today cannot silently become fifty.
    borger_build = borger_join = borger_drop = ""
    borger_cols: list[str] = []
    borger_select = ""
    if table.cpr_column:
        borger_build = """
DROP TABLE IF EXISTS #borger;

SELECT CAST([cpr] AS NVARCHAR(10)) AS [cpr], MIN([dw_ek_Borger]) AS [dw_ek_Borger]
INTO #borger
FROM [ref].[PatientToBorger]
WHERE [dw_is_deleted] = 0 AND [cpr] IS NOT NULL
GROUP BY [cpr];

CREATE UNIQUE CLUSTERED INDEX [IX_borger_cpr] ON #borger([cpr]);
"""
        borger_join = f"LEFT JOIN #borger AS b ON b.[cpr] = a.[{table.cpr_column}]"
        borger_drop = "DROP TABLE IF EXISTS #borger;"
        borger_cols = ["dw_ek_Borger"]
        borger_select = f"COALESCE(b.[dw_ek_Borger], {UNKNOWN_BORGER}), "

    fakta_cols = [dw_ek, *borger_cols, *org_cols, *dato_cols, *measure_cols]
    fakta_insert = ", ".join(f"[{c}]" for c in [*fakta_cols, "leverance_dato", "dw_is_deleted"])
    fakta_select_parts = [org_select, dato_select]
    if measure_select:
        fakta_select_parts.append(measure_select)
    fakta_select = ", ".join(fakta_select_parts)

    egen_insert = ", ".join(
        f"[{c}]" for c in [dw_ek, *table.egenskaber_columns, "leverance_dato", "dw_is_deleted"]
    )
    egen_select = ", ".join(f"a.[{c}]" for c in table.egenskaber_columns)

    disable = "\n".join(
        f"ALTER INDEX [{ix}] ON [dm].[{tbl}] DISABLE;" for tbl, ix in table.nonclustered_indexes
    )
    rebuild = "\n".join(
        f"ALTER INDEX [{ix}] ON [dm].[{tbl}] REBUILD;" for tbl, ix in table.nonclustered_indexes
    )

    return f"""
DROP TABLE IF EXISTS #org;

SELECT CAST([kode] AS NVARCHAR(50)) AS [kode], [dw_ek_Organisation]
INTO #org
FROM [dim].[Organisation]
WHERE [dw_is_deleted] = 0;

CREATE UNIQUE CLUSTERED INDEX [IX_org_kode] ON #org([kode]);
{borger_build}
INSERT INTO [map].[{table.dm_table}] ([clean_code])
SELECT {cc}
FROM [arc].[{table.arc_table}] AS a
WHERE NOT EXISTS (
    SELECT 1 FROM [map].[{table.dm_table}] AS m WHERE m.[clean_code] = {cc}
);

{disable}

TRUNCATE TABLE [dm].[{table.dm_table}];

INSERT INTO [dm].[{table.dm_table}] WITH (TABLOCK) ({fakta_insert})
SELECT m.[{dw_ek}], {borger_select}{fakta_select}, a.[leverance_dato], a.[dw_is_deleted]
FROM [arc].[{table.arc_table}] AS a
INNER JOIN [map].[{table.dm_table}] AS m ON m.[clean_code] = {cc}
{org_joins}
{borger_join};

TRUNCATE TABLE [dm].[{table.egenskaber_table}];

INSERT INTO [dm].[{table.egenskaber_table}] WITH (TABLOCK) ({egen_insert})
SELECT m.[{dw_ek}], {egen_select}, a.[leverance_dato], a.[dw_is_deleted]
FROM [arc].[{table.arc_table}] AS a
INNER JOIN [map].[{table.dm_table}] AS m ON m.[clean_code] = {cc};

{rebuild}

DROP TABLE IF EXISTS #org;
{borger_drop}
"""


def build_stats_sql(table: DmTable) -> str:
    """Counts that separate a structural blank from a genuinely unknown code.

    Reporting them as one number would show 85% "missing" for Acute_HIP every
    week, which is the national-aggregate rows behaving exactly as designed.
    The raw SOR value lives on egenskaber and the resolved key on the fact,
    so the two are joined on dw_ek.
    """
    parts = []
    for raw, key in table.sor_roles:
        parts.append(
            f"SELECT '{raw}' AS rolle, COUNT(*) AS raekker, "
            f"SUM(CASE WHEN e.[{raw}] IS NULL OR e.[{raw}] = '' THEN 1 ELSE 0 END) AS tom_sor, "
            f"SUM(CASE WHEN e.[{raw}] <> '' AND f.[{key}] = {UNKNOWN_ORGANISATION} "
            f"THEN 1 ELSE 0 END) AS uoploest "
            f"FROM [dm].[{table.egenskaber_table}] AS e "
            f"INNER JOIN [dm].[{table.dm_table}] AS f "
            f"ON f.[{table.dw_ek_column}] = e.[{table.dw_ek_column}]"
        )
    return "\nUNION ALL\n".join(parts) + ";"


def build_konvertering_sql(table: DmTable) -> str:
    """Rows whose non-blank source value did not survive its typed conversion.

    Measured against arc, per measure and date column, in ONE scan: a
    conditional SUM per column instead of a UNION ALL of full scans with
    non-sargable predicates — same result, one pass over the 6.7M rows.
    Date columns are checked with DATETIME2 to match the dato_key helper the
    rebuild uses, so the counter and the key derivation cannot disagree on
    what parses. A format change from SDS (decimal comma, a new date shape)
    lands here instead of silently becoming NULL/-1 in the fact — blanks are
    excluded, they are structure.
    """
    checks = [(raw, "DATETIME2") for raw, _ in table.date_roles]
    checks += [(src, typ) for src, _, typ in table.measures]
    sums = ", ".join(
        f"COALESCE(SUM(CASE WHEN NULLIF([{col}], '') IS NOT NULL "
        f"AND TRY_CONVERT({typ}, NULLIF([{col}], '')) IS NULL "
        f"THEN 1 ELSE 0 END), 0) AS [{col}]"
        for col, typ in checks
    )
    return f"SELECT {sums} FROM [arc].[{table.arc_table}];"


def _rebuild(
    context: dg.AssetExecutionContext, mssql: MSSQLResource, table: DmTable
) -> dg.MaterializeResult:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT COUNT(*), COALESCE(SUM(CAST([dw_is_deleted] AS INT)), 0) "
                f"FROM [arc].[{table.arc_table}]"
            )
            source_rows, source_deleted = cursor.fetchone()

            cursor.execute(f"SELECT COUNT(*) FROM [map].[{table.dm_table}]")
            map_rows_before = cursor.fetchone()[0]

            cursor.execute(build_rebuild_sql(table))
            # The rebuild is a multi-statement batch. An ODBC driver only
            # surfaces a later statement's error once the caller advances past
            # the earlier result sets — so without this loop a failing INSERT
            # is swallowed and the asset reports success on a table TRUNCATE
            # just emptied. That happened: a missing column left
            # dm.HAIBA_CasesInfektion at 0 rows behind a green run.
            if hasattr(cursor, "nextset"):
                while cursor.nextset():
                    pass
            conn.commit()

            cursor.execute(f"SELECT COUNT(*) FROM [dm].[{table.dm_table}]")
            rows = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM [dm].[{table.egenskaber_table}]")
            egen_rows = cursor.fetchone()[0]
            cursor.execute(f"SELECT COUNT(*) FROM [map].[{table.dm_table}]")
            map_rows = cursor.fetchone()[0]

            if source_rows and not rows:
                # Belt and braces: whatever the driver reports, a rebuild that
                # empties a table whose source has rows is a failure.
                raise ValueError(
                    f"dm.{table.dm_table} is empty after rebuild while "
                    f"arc.{table.arc_table} holds {source_rows} rows — the "
                    f"INSERT did not run."
                )
            # dw_is_deleted is carried, not filtered, so fact and egenskaber
            # must both match arc row for row. A shortfall means the mint
            # missed a clean_code or the map join dropped rows.
            if not (rows == egen_rows == source_rows):
                raise ValueError(
                    f"row-count mismatch after rebuild: arc.{table.arc_table}="
                    f"{source_rows}, dm.{table.dm_table}={rows}, "
                    f"dm.{table.egenskaber_table}={egen_rows}."
                )

            cursor.execute(build_stats_sql(table))
            stats = {
                rolle: {"raekker": n, "tom_sor": tom, "uoploest": uop}
                for rolle, n, tom, uop in cursor.fetchall()
            }

            cursor.execute(build_konvertering_sql(table))
            konvertering = dict(
                zip([d[0] for d in cursor.description], cursor.fetchone(), strict=True)
            )

            # One CPR carrying several live dw_ek_Borger keys means the case
            # attaches to MIN() of genuinely distinct citizen keys — per-
            # citizen aggregates split, and the kontakt stamp cannot reach
            # contacts under the other patient record. Not fixable here (a
            # ref/dim.Borger survivorship question) but it must stay visible.
            cpr_flere_borgere = None
            cases_uden_borger = None
            if table.cpr_column:
                cursor.execute(
                    "SELECT COUNT(*) FROM ("
                    "SELECT [cpr] FROM [ref].[PatientToBorger] "
                    "WHERE [dw_is_deleted] = 0 AND [cpr] IS NOT NULL "
                    "GROUP BY [cpr] "
                    "HAVING COUNT(DISTINCT [dw_ek_Borger]) > 1"
                    ") AS flere"
                )
                cpr_flere_borgere = cursor.fetchone()[0]

                # Cases whose CPR did not resolve to a citizen. Normally
                # transient: haiba_ingest_job posts a stub Patient for an
                # unknown CPR, the nightly DFM chain projects it into
                # ref.PatientToBorger, and the next rebuild resolves it.
                # haiba_publish_sensor reads this same count off dm and keeps
                # re-publishing while it falls, so recovery is automatic. A
                # count that stops falling is the residue that never resolves
                # — most often a CPR the FHIR server knows only as a
                # previous_ssn, which ref.PatientToBorger never carries.
                cursor.execute(
                    f"SELECT COUNT(*) FROM [dm].[{table.dm_table}] "
                    f"WHERE [dw_ek_Borger] = {UNKNOWN_BORGER}"
                )
                cases_uden_borger = cursor.fetchone()[0]
        finally:
            cursor.close()

    for rolle, s in stats.items():
        context.log.info(
            f"{table.dm_table}.{rolle}: {s['raekker']} rows, "
            f"{s['tom_sor']} blank SOR (national aggregates), "
            f"{s['uoploest']} unresolved"
        )
    for kolonne, tabt in konvertering.items():
        if tabt:
            context.log.warning(
                f"{table.dm_table}.{kolonne}: {tabt} non-blank values did not "
                f"survive their typed conversion — check the source format."
            )

    # The map is append-only, so a format change in any clean_code component
    # (SDS reshaping a date, SOR zero-padding, an edited key_columns) mints a
    # full second generation of keys while every other guard stays green.
    # Steady state mints near zero rows; the first run ever legitimately
    # mints everything, hence the map_rows_before guard.
    map_rows_minted = map_rows - map_rows_before
    if map_rows_before and source_rows and map_rows_minted > REKEY_WARN_FRACTION * source_rows:
        context.log.warning(
            f"map.{table.dm_table}: {map_rows_minted} new dw_ek minted this "
            f"run against {source_rows} source rows — the clean_code "
            f"components may have changed shape, silently re-keying the "
            f"history. Check the source format and schema.key_columns."
        )
    if cpr_flere_borgere:
        context.log.warning(
            f"{table.dm_table}: {cpr_flere_borgere} CPR(s) map to more than "
            f"one live dw_ek_Borger in ref.PatientToBorger — cases attach to "
            f"MIN() of the keys, and the kontakt stamp cannot reach contacts "
            f"under the other patient record."
        )
    if cases_uden_borger:
        context.log.warning(
            f"{table.dm_table}: {cases_uden_borger} case(s) carry "
            f"dw_ek_Borger = {UNKNOWN_BORGER} and therefore no FHIR contact "
            f"either (match_kvalitet_* = 'ingen_borger'). Expected right after "
            f"a delivery introduced new CPRs — haiba_publish_sensor re-runs "
            f"this rebuild on the following nights while the count falls."
        )

    metadata = {
        "row_count": dg.MetadataValue.int(rows),
        "source_row_count": dg.MetadataValue.int(source_rows),
        "soft_deleted_rows": dg.MetadataValue.int(source_deleted),
        "active_rows": dg.MetadataValue.int(source_rows - source_deleted),
        "map_row_count": dg.MetadataValue.int(map_rows),
        "map_rows_minted": dg.MetadataValue.int(map_rows_minted),
        "sor_roles": dg.MetadataValue.json(stats),
        "konverteringstab": dg.MetadataValue.json(konvertering),
        "source_table": dg.MetadataValue.text(f"arc.{table.arc_table}"),
        "target_table": dg.MetadataValue.text(f"dm.{table.dm_table} + dm.{table.egenskaber_table}"),
    }
    if cpr_flere_borgere is not None:
        metadata["cpr_med_flere_borgere"] = dg.MetadataValue.int(cpr_flere_borgere)
    if cases_uden_borger is not None:
        metadata["cases_uden_borger"] = dg.MetadataValue.int(cases_uden_borger)
    return dg.MaterializeResult(metadata=metadata)


def _build_asset(table: DmTable):
    @dg.asset(
        name=f"dm_{table.arc_table}",
        group_name="haiba_dm",
        compute_kind="mssql",
        # The stub-patient asset is an ordering dep, not a data dep: it posts
        # to Aidbox, and the projection into ref.PatientToBorger belongs to the
        # nightly DFM chain. It lives in haiba_ingest_job while the rebuild
        # lives in haiba_publish_job, so within a single job this dep never
        # forces execution — it stops the two from being siblings when both are
        # selected, which would let the rebuild run first and read the citizen
        # keys before the stubs were even posted.
        deps=[f"arc_{table.arc_table}"] + (["fhir_patient_haiba"] if table.cpr_column else []),
        tags={"domain": "haiba", "layer": "dm", "source": "haiba"},
        description=(
            f"Mint dw_ek in map.{table.dm_table} and rebuild the fact "
            f"dm.{table.dm_table} plus dm.{table.egenskaber_table} from "
            f"arc.{table.arc_table}, resolving {len(table.sor_roles)} SOR "
            f"column(s) and {len(table.date_roles)} date key(s) "
            f"(-1 when unknown or blank)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
    ) -> dg.MaterializeResult:
        # Log-only DFM run: dm is not a checkpoint owner — the rebuild is
        # TRUNCATE + INSERT from arc, so there is no delta position to hold.
        # The run still lands in meta.DFMRunLog (timing, rows, errors), locked
        # to full_load because that is the only mode the rebuild has.
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "dm", table.dm_table, dagster_run_id=context.run_id, mode="full_load"
        )
        try:
            result = _rebuild(context, mssql_target_staging, table)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise
        dfm.complete_dfm_run(run_ctx.log_id, rows_affected=result.metadata["row_count"].value)
        return result

    return _asset


dm_haiba_agg_infektion = _build_asset(DM_TABLES[0])
dm_haiba_agg_ortopaedi = _build_asset(DM_TABLES[1])
dm_haiba_cases_infektion = _build_asset(DM_TABLES[2])
dm_haiba_cases_ssi = _build_asset(DM_TABLES[3])

haiba_dm_assets = [
    dm_haiba_agg_infektion,
    dm_haiba_agg_ortopaedi,
    dm_haiba_cases_infektion,
    dm_haiba_cases_ssi,
]

__all__ = [
    "DM_TABLES",
    "DmTable",
    "build_konvertering_sql",
    "build_rebuild_sql",
    "build_stats_sql",
    "haiba_dm_assets",
]
