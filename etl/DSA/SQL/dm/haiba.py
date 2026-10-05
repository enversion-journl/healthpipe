# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
dm.HAIBA_* — the resolved HAIBA layer.

arc.* is 1:1 with the delivery files. This layer keeps every source value and
adds the DW keys beside it, so a consumer can see both what SDS delivered and
what it resolved to. Rebuilt in full from arc on each run: arc is the source of
truth, and a full rebuild is always consistent with it.

SOR resolution (AB#27832): dim.Organisation.kode holds the SOR code — the
column is ``kode``, not ``code``. A code that cannot be resolved gets
dw_ek_Organisation = -1, the UNKNOWN sentinel seeded by the organisation DDL.

An empty SOR code is not a failed lookup. SORIdentifier is empty exactly when
the row is a national aggregate (RegionKode 8888/9999) — 0.4% of the infection
rows but 85% of Acute_HIP, because the orthopedic files are dominated by
national totals. Those rows get -1 too, and the run metadata counts them
separately so a report cannot mistake structure for a data problem.

dw_ek_Borger (AB#27833) and the encounter relation (AB#27834) extend this
layer; the SELECT below is where their joins belong.
"""

from dataclasses import dataclass

import dagster as dg
from core.resources import MSSQLResource
from DSI.csv_files.haiba import (
    AGG_INFEKTION,
    AGG_ORTOPAEDI,
    CASES_INFEKTION,
    CASES_SSI,
    HaibaSchema,
)

UNKNOWN_ORGANISATION = -1
# dim.Borger's UNKNOWN sentinel (-2 means N/A, which does not apply here).
UNKNOWN_BORGER = -1


@dataclass(frozen=True)
class DmTable:
    """One dm table and the SOR columns it resolves.

    ``sor_roles`` maps the source column carrying a SOR code to the key column
    that receives the lookup. HAIBA carries seven such columns across the four
    tables — two on the agg side, two on infection cases and four on SSI, where
    the index operation and the reoperation each have a producer and a
    responsible unit.
    """

    schema: HaibaSchema
    dm_table: str
    sor_roles: tuple[tuple[str, str], ...]
    # Disabled during the rebuild and rebuilt after — maintaining them row by
    # row during an 8M-row insert costs more than rebuilding them once.
    nonclustered_indexes: tuple[str, ...] = ()
    # Case tables carry a CPR and therefore resolve a dw_ek_Borger; the agg
    # tables have no person data at all.
    cpr_column: str | None = None

    @property
    def arc_table(self) -> str:
        return self.schema.ext_table


DM_TABLES = (
    DmTable(
        AGG_INFEKTION,
        "HAIBA_Agg_Infektion",
        (("SORIdentifier", "dw_ek_Organisation"),),
        ("IX_dm_HAIBA_Agg_Infektion_Org",),
    ),
    DmTable(
        AGG_ORTOPAEDI,
        "HAIBA_Agg_Ortopaedi",
        (("SORIdentifier", "dw_ek_Organisation"),),
        ("IX_dm_HAIBA_Agg_Ortopaedi_Org",),
    ),
    DmTable(
        CASES_INFEKTION,
        "HAIBA_Cases_Infektion",
        (
            ("HAI_Opholdsadresse", "dw_ek_Organisation_opholdsadresse"),
            ("HAI_Kontaktansvar", "dw_ek_Organisation_kontaktansvar"),
        ),
        ("IX_dm_HAIBA_Cases_Infektion_Org",),
        cpr_column="cprnr",
    ),
    DmTable(
        CASES_SSI,
        "HAIBA_Cases_SSI",
        (
            ("HAI_producent", "dw_ek_Organisation_producent"),
            ("HAI_kontaktansvarlig", "dw_ek_Organisation_kontaktansvarlig"),
            ("reo_producent", "dw_ek_Organisation_reo_producent"),
            ("reo_kontaktansvarlig", "dw_ek_Organisation_reo_kontaktansvarlig"),
        ),
        cpr_column="cprnr",
    ),
)


def build_rebuild_sql(table: DmTable) -> str:
    """TRUNCATE + INSERT ... SELECT that rebuilds one dm table from arc.

    The lookup goes through a materialised #org rather than joining
    dim.Organisation directly, and that is not premature optimisation:

    dim.Organisation holds 165,477 rows. Comparing its varchar ``kode`` to the
    source's nvarchar SOR column forces a conversion — nvarchar wins type
    precedence — which makes the predicate unseekable. On the 8M-row infection
    table the optimiser answered that with Nested Loops over a Table Spool,
    i.e. a scan of 165k rows per source row. That plan burned 600+ seconds of
    CPU and 19.6M logical reads without finishing.

    Copying dim into a temp table with the key already cast to nvarchar and a
    unique clustered index on it removes the conversion from the predicate, so
    the join is on matching types against an indexed key. ``kode`` is verified
    unique in dim.Organisation (165,477 rows, 165,477 distinct), so the join
    cannot multiply rows — which would otherwise break the dm primary key.

    ``WITH (TABLOCK)`` keeps the insert minimally logged, and nonclustered
    indexes are disabled during the load and rebuilt after, mirroring the fast
    path in core/archive_merge.py. Note that TABLOCK holds an exclusive table
    lock for the duration: readers of this table wait while it rebuilds.
    """
    source_columns = [*table.schema.all_columns, "dw_is_deleted"]
    key_columns = [key for _, key in table.sor_roles]

    if table.cpr_column:
        key_columns.append("dw_ek_Borger")
    insert_cols = ", ".join(f"[{c}]" for c in [*source_columns, *key_columns])
    select_cols = ", ".join(f"a.[{c}]" for c in source_columns)
    joins = "\n".join(
        f"LEFT JOIN #org AS o{i} ON o{i}.[kode] = a.[{raw}] AND a.[{raw}] <> ''"
        for i, (raw, _) in enumerate(table.sor_roles)
    )
    key_select = ", ".join(
        f"COALESCE(o{i}.[dw_ek_Organisation], {UNKNOWN_ORGANISATION})"
        for i, _ in enumerate(table.sor_roles)
    )

    # ref.PatientToBorger is where dw_ek_Borger is minted. cpr is NOT unique
    # there — one CPR currently has two patient_ids — so it is collapsed with
    # MIN before the join. Without that a duplicated person would multiply the
    # case row and break the dm primary key.
    borger_build = borger_join = borger_drop = ""
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
        key_select += f", COALESCE(b.[dw_ek_Borger], {UNKNOWN_BORGER})"

    disable = "\n".join(
        f"ALTER INDEX [{ix}] ON [dm].[{table.dm_table}] DISABLE;"
        for ix in table.nonclustered_indexes
    )
    rebuild = "\n".join(
        f"ALTER INDEX [{ix}] ON [dm].[{table.dm_table}] REBUILD;"
        for ix in table.nonclustered_indexes
    )

    return f"""
DROP TABLE IF EXISTS #org;

SELECT CAST([kode] AS NVARCHAR(50)) AS [kode], [dw_ek_Organisation]
INTO #org
FROM [dim].[Organisation];

CREATE UNIQUE CLUSTERED INDEX [IX_org_kode] ON #org([kode]);
{borger_build}
{disable}

TRUNCATE TABLE [dm].[{table.dm_table}];

INSERT INTO [dm].[{table.dm_table}] WITH (TABLOCK) ({insert_cols})
SELECT {select_cols}, {key_select}
FROM [arc].[{table.arc_table}] AS a
{joins}
{borger_join};

{rebuild}

DROP TABLE IF EXISTS #org;
{borger_drop}
"""


def build_stats_sql(table: DmTable) -> str:
    """Counts that separate a structural blank from a genuinely unknown code.

    Reporting them as one number would show 85% "missing" for Acute_HIP every
    week, which is the national-aggregate rows behaving exactly as designed.
    """
    parts = []
    for raw, key in table.sor_roles:
        parts.append(
            f"SELECT '{raw}' AS rolle, COUNT(*) AS raekker, "
            f"SUM(CASE WHEN [{raw}] IS NULL OR [{raw}] = '' THEN 1 ELSE 0 END) AS tom_sor, "
            f"SUM(CASE WHEN [{raw}] <> '' AND [{key}] = {UNKNOWN_ORGANISATION} "
            f"THEN 1 ELSE 0 END) AS uoploest "
            f"FROM [dm].[{table.dm_table}]"
        )
    return "\nUNION ALL\n".join(parts) + ";"


def _rebuild(
    context: dg.AssetExecutionContext, mssql: MSSQLResource, table: DmTable
) -> dg.MaterializeResult:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"SELECT COUNT(*) FROM [arc].[{table.arc_table}]")
            source_rows = cursor.fetchone()[0]

            cursor.execute(build_rebuild_sql(table))
            # The rebuild is a multi-statement batch. An ODBC driver only
            # surfaces a later statement's error once the caller advances past
            # the earlier result sets — so without this loop a failing INSERT
            # is swallowed and the asset reports success on a table TRUNCATE
            # just emptied. That happened: a missing column left
            # dm.HAIBA_Cases_Infektion at 0 rows behind a green run.
            if hasattr(cursor, "nextset"):
                while cursor.nextset():
                    pass
            conn.commit()

            cursor.execute(f"SELECT COUNT(*) FROM [dm].[{table.dm_table}]")
            rows = cursor.fetchone()[0]
            if source_rows and not rows:
                # Belt and braces: whatever the driver reports, a rebuild that
                # empties a table whose source has rows is a failure.
                raise ValueError(
                    f"dm.{table.dm_table} is empty after rebuild while "
                    f"arc.{table.arc_table} holds {source_rows} rows — the "
                    f"INSERT did not run."
                )

            cursor.execute(build_stats_sql(table))
            stats = {
                rolle: {"raekker": n, "tom_sor": tom, "uoploest": uop}
                for rolle, n, tom, uop in cursor.fetchall()
            }
        finally:
            cursor.close()

    for rolle, s in stats.items():
        context.log.info(
            f"{table.dm_table}.{rolle}: {s['raekker']} rows, "
            f"{s['tom_sor']} blank SOR (national aggregates), "
            f"{s['uoploest']} unresolved"
        )

    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(rows),
            "source_row_count": dg.MetadataValue.int(source_rows),
            "sor_roles": dg.MetadataValue.json(stats),
            "source_table": dg.MetadataValue.text(f"arc.{table.arc_table}"),
            "target_table": dg.MetadataValue.text(f"dm.{table.dm_table}"),
        }
    )


def _build_asset(table: DmTable):
    @dg.asset(
        name=f"dm_{table.arc_table}",
        group_name="haiba_dm",
        compute_kind="mssql",
        deps=[f"arc_{table.arc_table}"],
        tags={"domain": "haiba", "layer": "dm", "source": "haiba"},
        description=(
            f"Rebuild dm.{table.dm_table} from arc.{table.arc_table}, resolving "
            f"{len(table.sor_roles)} SOR column(s) to dw_ek_Organisation "
            f"(-1 when unknown or blank)."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
    ) -> dg.MaterializeResult:
        return _rebuild(context, mssql_target_staging, table)

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
    "build_rebuild_sql",
    "build_stats_sql",
    "haiba_dm_assets",
]
