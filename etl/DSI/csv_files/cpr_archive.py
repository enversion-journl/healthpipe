# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Archiving of the CPR delivery from ext.cpr_* to arc.cpr_*.

A change extract is person snapshots, so the archive replaces per person: delete detection
is scoped to the personal numbers in the delivery's person records (001). A base extract
covers all persons and acts as a full delete detection. All tables are archived in
one asset because the delivery is the unit.
"""

# No "from __future__ import annotations": dg.Config cannot be resolved with deferred annotations.


from dataclasses import replace

import dagster as dg
from core.archive_merge import execute_archive_merge
from core.dfm_config import ArchiveMergeConfig, RunModeConfig
from core.mssql_application_lock import application_lock
from core.resources import MSSQLResource
from DSI.csv_files.cpr import (
    ARKIVTABELLER,
    TABEL_PR_NAVN,
    kolonner_for_tabel,
    marker_arkiveret,
)
from DSI.csv_files.cpr_publication import CPR_PUBLICATION_LOCK, begin_archive_publication

# The person record delimits which persons the delivery speaks about.
PERSON_TABEL = "cpr_001_person"
LEVERANCE_TABEL = "CPRLeverance"


def byg_konfiguration(tabel: str) -> ArchiveMergeConfig:
    """The archive configuration for one ext and arc table pair; columns derive from the layout."""
    arkivtabel = TABEL_PR_NAVN[tabel]
    kolonner = kolonner_for_tabel(tabel)
    noegler = arkivtabel.noegle
    ikke_noegle = tuple(k for k in kolonner if k not in noegler) + ("leverance_dato",)
    return ArchiveMergeConfig(
        source_schema="ext",
        source_table=tabel,
        target_schema="arc",
        target_table=tabel,
        merge_keys=noegler,
        non_key_columns=ikke_noegle,
        # The delivery date changes in every delivery and is in the change check only where it must move.
        change_detect_columns=tuple(
            k for k in ikke_noegle if arkivtabel.sidst_set or k != "leverance_dato"
        ),
        strategy="upsert",
        update_only_on_change=True,
        detect_deletes=True,
        # Ext holds the whole delivery for the affected persons; the scope is set per run.
        upstream_is_full_load=True,
        enable_bulk_insert_on_first_load=True,
    )


KONFIGURATIONER = {
    t.tabel: byg_konfiguration(t.tabel)
    for t in ARKIVTABELLER
    if not t.erstattes and not t.akkumulerer
}


SCOPE_TABEL = "CPRArkivScope"


def laas_afgraensning(mssql: MSSQLResource) -> int:
    """Freeze the delivery's set of persons in meta.CPRArkivScope and return the count.

    Each table is written on its own connection and reads the snapshot, so a concurrent
    ext_cpr cannot move the set of persons during the run.
    """
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(f"DROP TABLE IF EXISTS [meta].[{SCOPE_TABEL}]")
            cursor.execute(
                f"SELECT DISTINCT [pnr] INTO [meta].[{SCOPE_TABEL}] "
                f"FROM [ext].[{PERSON_TABEL}] WHERE [pnr] IS NOT NULL"
            )
            cursor.execute(
                f"CREATE CLUSTERED INDEX [IX_{SCOPE_TABEL}_pnr] ON [meta].[{SCOPE_TABEL}] ([pnr])"
            )
            cursor.execute(f"SELECT COUNT(*) FROM [meta].[{SCOPE_TABEL}]")
            antal = cursor.fetchone()[0]
            conn.commit()
        finally:
            cursor.close()
    return antal


def byg_afgraensning(alias: str = "tgt", personkolonne: str = "pnr") -> str:
    """The scope to the frozen set of persons, on the column that carries the person."""
    return f"{alias}.[{personkolonne}] IN (SELECT [pnr] FROM [meta].[{SCOPE_TABEL}])"


def med_afgraensning(konfiguration: ArchiveMergeConfig, afgraensning: str) -> ArchiveMergeConfig:
    """The configuration with delete detection scoped to the delivery's persons."""
    return replace(konfiguration, delete_scope_predicate=afgraensning)


def _ext_er_tom(mssql: MSSQLResource, tabel: str) -> bool:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                "SELECT COUNT(*) FROM sys.tables t JOIN sys.schemas s "
                "ON s.schema_id = t.schema_id WHERE s.name = ? AND t.name = ?",
                ("ext", tabel),
            )
            if not cursor.fetchone()[0]:
                return True
            cursor.execute(f"SELECT TOP 1 1 FROM [ext].[{tabel}]")
            return cursor.fetchone() is None
        finally:
            cursor.close()


def _leveranceoplysninger(mssql: MSSQLResource) -> dict:
    """What the ext layer says about the delivery that is ready."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"SELECT CONVERT(CHAR(8), MAX([leverance_dato]), 112), COUNT(*) "
                f"FROM [ext].[{PERSON_TABEL}]"
            )
            raekke = cursor.fetchone()
        finally:
            cursor.close()
    return {"leverance_dato": raekke[0] if raekke else None, "personer": raekke[1] if raekke else 0}


def _opdater_tael(
    mssql: MSSQLResource,
    leverance_dato: str,
    tabel: str,
    indsat: int,
    opdateret: int,
    soft_slettet: int,
) -> None:
    """Write the archiving counters to the control table's row for this delivery and table."""
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                UPDATE [meta].[{LEVERANCE_TABEL}]
                   SET [raekker_indsat] = ?,
                       [raekker_opdateret] = ?,
                       [raekker_soft_slettet] = ?,
                       [dw_timestamp_update] = CAST(SYSDATETIME() AS datetime2(0))
                 WHERE [kildetabel] = ? AND [leverance_dato] = CONVERT(date, ?, 112)
                """,
                (indsat, opdateret, soft_slettet, tabel, leverance_dato),
            )
            conn.commit()
        finally:
            cursor.close()


def erstat_pr_person(
    mssql: MSSQLResource, tabel: str, kolonner: tuple, personkolonne: str = "pnr"
) -> dict:
    """Replace the archive's rows for the delivery's persons with the ext layer's; a hard delete.

    Fails before commit if a row in ext belongs to a person without a person record.
    """
    kolonneliste = ", ".join(f"[{k}]" for k in kolonner) + ", [leverance_dato]"
    i_personkredsen = f"[{personkolonne}] IN (SELECT [pnr] FROM [meta].[{SCOPE_TABEL}])"
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                DELETE FROM [arc].[{tabel}]
                 WHERE {i_personkredsen}
                """
            )
            slettet = cursor.rowcount
            cursor.execute(
                f"""
                INSERT INTO [arc].[{tabel}] ({kolonneliste},
                        [dw_is_deleted], [dw_timestamp_insert], [dw_timestamp_update])
                SELECT {kolonneliste}, 0,
                       CAST(SYSDATETIME() AS datetime2(0)),
                       CAST(SYSDATETIME() AS datetime2(0))
                  FROM [ext].[{tabel}]
                 WHERE {i_personkredsen}
                """
            )
            indsat = cursor.rowcount
            cursor.execute(f"SELECT COUNT(*) FROM [ext].[{tabel}]")
            i_ext = cursor.fetchone()[0]
            if i_ext != indsat:
                # Not committed: DELETE and INSERT are rolled back when the connection closes.
                raise ValueError(
                    f"{tabel}: {i_ext - indsat} of {i_ext} rows in ext belong to persons without a "
                    f"person record in the delivery and would have been lost. The delivery is "
                    f"incomplete; nothing has been written for the table."
                )
            conn.commit()
        finally:
            cursor.close()
    return {"indsat": indsat, "opdateret": 0, "soft_slettet": 0, "hard_slettet": slettet}


def tilfoej_haendelser(mssql: MSSQLResource, tabel: str, kolonner: tuple) -> dict:
    """Add the delivery's events that the archive lacks; nothing is updated or deleted.

    DISTINCT, because the source does not promise that an event appears only once in a file.
    """
    kolonneliste = ", ".join(f"[{k}]" for k in kolonner)
    noegle = TABEL_PR_NAVN[tabel].noegle
    samme = " AND ".join(f"ISNULL(a.[{k}], '') = ISNULL(e.[{k}], '')" for k in noegle)
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"""
                INSERT INTO [arc].[{tabel}] ({kolonneliste}, [leverance_dato],
                        [dw_is_deleted], [dw_timestamp_insert], [dw_timestamp_update])
                SELECT {kolonneliste}, MIN([leverance_dato]), 0,
                       CAST(SYSDATETIME() AS datetime2(0)), CAST(SYSDATETIME() AS datetime2(0))
                  FROM [ext].[{tabel}] e
                 WHERE NOT EXISTS (SELECT 1 FROM [arc].[{tabel}] a WHERE {samme})
                 GROUP BY {kolonneliste}
                """
            )
            indsat = cursor.rowcount
            conn.commit()
        finally:
            cursor.close()
    return {"indsat": indsat, "opdateret": 0, "soft_slettet": 0}


@dg.asset(
    name="arc_cpr",
    group_name="cpr",
    deps=["ext_cpr"],
    compute_kind="mssql",
    # See ext_cpr: same key, so the two never run at the same time.
    tags={"domain": "cpr", "layer": "archive", "source": "cpr", "dagster/concurrency_key": "cpr"},
    description=(
        "Merge the CPR delivery from ext.cpr_* to arc.cpr_*. Replaces per person: "
        "delete detection is scoped to the personal numbers the delivery "
        "contains, so persons it does not mention are left untouched."
    ),
)
def arc_cpr(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    with application_lock(mssql_target_staging, CPR_PUBLICATION_LOCK):
        if _ext_er_tom(mssql_target_staging, PERSON_TABEL):
            context.log.info("No delivery in ext; there is nothing to archive.")
            return dg.MaterializeResult(metadata={"status": "ingen leverance i ext"})

        oplysninger = _leveranceoplysninger(mssql_target_staging)
        leverance_dato = oplysninger["leverance_dato"]
        if not leverance_dato:
            raise ValueError(
                "ext.cpr_001_person has no delivery date. The load is "
                "incomplete, and the archive must not merge on an unknown basis."
            )

        begin_archive_publication(mssql_target_staging, leverance_dato, len(ARKIVTABELLER))
        personer_i_scope = laas_afgraensning(mssql_target_staging)
        context.log.info("The set of persons is frozen: %d personal numbers", personer_i_scope)
        resultater = {}
        for arkivtabel in ARKIVTABELLER:
            afgraensning = byg_afgraensning(personkolonne=arkivtabel.personkolonne)
            if arkivtabel.akkumulerer:
                tal = tilfoej_haendelser(
                    mssql_target_staging, arkivtabel.tabel, kolonner_for_tabel(arkivtabel.tabel)
                )
                resultater[arkivtabel.tabel] = tal
                _opdater_tael(
                    mssql_target_staging,
                    leverance_dato,
                    arkivtabel.tabel,
                    tal["indsat"],
                    tal["opdateret"],
                    tal["soft_slettet"],
                )
                context.log.info("%s: %d events added", arkivtabel.tabel, tal["indsat"])
                continue
            if arkivtabel.erstattes:
                tal = erstat_pr_person(
                    mssql_target_staging,
                    arkivtabel.tabel,
                    kolonner_for_tabel(arkivtabel.tabel),
                    personkolonne=arkivtabel.personkolonne,
                )
                resultater[arkivtabel.tabel] = tal
                _opdater_tael(
                    mssql_target_staging,
                    leverance_dato,
                    arkivtabel.tabel,
                    tal["indsat"],
                    tal["opdateret"],
                    tal["soft_slettet"],
                )
                context.log.info(
                    "%s: replaced per person — %d inserted, %d removed",
                    arkivtabel.tabel,
                    tal["indsat"],
                    tal["hard_slettet"],
                )
                continue

            konfiguration = med_afgraensning(KONFIGURATIONER[arkivtabel.tabel], afgraensning)
            resultat = execute_archive_merge(
                context,
                mssql_target_staging,
                konfiguration,
                RunModeConfig(mode="full_load"),
            )
            resultater[arkivtabel.tabel] = {
                "indsat": resultat.rows_inserted,
                "opdateret": resultat.rows_updated,
                "soft_slettet": resultat.rows_soft_deleted,
            }
            _opdater_tael(
                mssql_target_staging,
                leverance_dato,
                arkivtabel.tabel,
                resultat.rows_inserted,
                resultat.rows_updated,
                resultat.rows_soft_deleted,
            )
            context.log.info(
                "%s: %d inserted, %d updated, %d soft-deleted",
                arkivtabel.tabel,
                resultat.rows_inserted,
                resultat.rows_updated,
                resultat.rows_soft_deleted,
            )

        marker_arkiveret(mssql_target_staging, leverance_dato)

        return dg.MaterializeResult(
            metadata={
                "leverance_dato": leverance_dato,
                "personer_i_leverancen": oplysninger["personer"],
                "personer_i_sletningsscope": personer_i_scope,
                "tabeller": len(resultater),
                "indsat_i_alt": sum(r["indsat"] for r in resultater.values()),
                "opdateret_i_alt": sum(r["opdateret"] for r in resultater.values()),
                "soft_slettet_i_alt": sum(r["soft_slettet"] for r in resultater.values()),
                "pr_tabel": dg.MetadataValue.json(resultater),
            }
        )


cpr_archive_assets = [arc_cpr]
