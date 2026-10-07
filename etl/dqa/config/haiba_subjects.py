# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA subject configs for the HAIBA dm domain (DSA / mssql_target_staging).

Every check runs at Cadence.DAILY on purpose: dm.HAIBA_* only changes when
haiba_publish_job runs (delivery-driven — roughly weekly, plus the recovery
re-publishes the sensor requests while unresolved-citizen counts fall), and
``dqa_dwh_haiba_daily_job`` is sensor-chained onto exactly that job. "Daily"
here therefore means "after every publish", which is the natural cadence for
a full-rebuild domain — a separate weekly tier would just re-check unchanged
tables. The DSO<->DSA reconciliation for the same eight tables lives in
``dqa/dso.py`` (``dqa_dso_haiba_job``).

The check surface mirrors the rebuild's own in-run guards, but at rest:

- **row_parity** fact==arc and fact==egenskaber — the rebuild asserts this
  in-run; at rest it catches a manual TRUNCATE or a half-finished pair.
- **konverteringstab** re-runs the rebuild's own TRY_CONVERT-loss counter
  (single source of truth: ``DSA.SQL.dm.haiba.build_konvertering_sql``) so a
  format change from SDS lands in dqa.Results where it can be trended.
- **match_kvalitet** 'ikke_koert' must not survive a publish (the stamp
  rewrites every row), and the quality distribution is kept as observability.
- **referential_integrity** from the lineage (dw_ek_ -> dim.*) plus the
  hand-listed encounter/location relation keys -> fhir.Encounter/Location
  (the lineage's DWH edge loader resolves by dw_ek naming, so *_id columns
  are listed here instead).
- **freshness** on meta.HAIBALeverance — the "SDS stopped delivering (or the
  ingest stopped reading)" signal no row-level check can give.

The arc layer gets its own subjects (``dwh_haiba_arc_*``) for the
delivery-level checks that no dm check can express:

- **kontroltotal** cases <-> agg: the case count per HAI type equals the agg
  table's SUM(antal) filtered to Rolle_ansvar='Kontaktansvar' and
  RegionKode='1083' — but ONLY when both sides' newest delivery of that type
  matches. Those dates come from meta.HAIBALeverance, not MAX(arc.leverance_dato)
  (which only moves on a value change, so aligned deliveries could look out of
  step and the gate would compare nothing while standing green). HAIBAcpr and
  HAIBAagg are routinely delivered on different dates (verified on dev
  2026-08-30: cases=2026-08-08 vs agg=2026-08-15 gave 0.1-0.3 % drift on every
  type), so out-of-step types are reported in extra, never failed; a standing
  ``ingen_typer_i_takt`` flag shows if the gate has gone permanently inert.
- **freshness per HAI type** — reads meta.HAIBALeverance, which records the
  delivery of every type each run (per kildetabel + hai_type). arc.leverance_dato
  only advances on a value change, and the case feed is near append-only, so a
  per-type check against arc would alarm on a live feed; and a single type
  silently running on an old delivery (UTI was gone 2026-01-10 to 2026-02-21)
  is invisible at table grain. A retired type stops being registered and leaves
  the check; deliberately no expected_groups — SDS adds and retires types.
- **encoding sanity**: no U+FFFD replacement characters in any text column
  after load (a wrong-encoding read corrupts silently instead of raising).

The extract/archive guards live in the ETL itself, not here: header-only and
row-drift protection plus arc-width rejection in DSI/csv_files/haiba.py
(invariant 1), and unknown-column rejection in iter_rows.

GDPR: nothing here may put clean_code or cprnr values in examples/extra —
identity columns are dw_ek surrogates or HAI types throughout, and the arc
checks return counts only (the cases PK carries CPR, so no example keys).
"""

from __future__ import annotations

from dqa.checks.helpers import get_conn
from dqa.model import Cadence, CheckOutcome, CheckSpecification, DqaSubject, Severity, Track
from dqa.references import dw_ek_columns, referential_specs
from DSA.SQL.dm.haiba import DM_TABLES, DmTable, build_konvertering_sql
from DSA.SQL.dm.haiba_kontakt import KONTAKT_TABELLER

_RES = "mssql_target_staging"

_DM_BY_TABLE: dict[str, DmTable] = {t.dm_table: t for t in DM_TABLES}

# Per-column caps on the -1 sentinel rate (active rows). Organisation roles are
# deliberately uncapped: a blank SOR is a national aggregate (up to 85 % of
# Acute_HIP) and lookup coverage is observed, not gated, until baselines are
# read off dqa.Results. Date keys use -1 only for a NON-blank value that failed
# to parse (blank is the 99999998 sentinel), so any real rate is a format
# change. dw_ek_Borger = -1 is transient after a delivery introduces new CPRs
# (stubs resolve on the following nights), so the cap has slack for that.
_SENTINEL_CAPS: dict[str, float] = {
    "dw_ek_Borger": 0.05,
    "dw_ek_Dato": 0.01,
    "dw_ek_Dato_hai": 0.01,
    "dw_ek_Dato_operation": 0.01,
    "dw_ek_Dato_reo": 0.01,
}


def _konverteringstab(context, subject, spec, resources) -> CheckOutcome:
    """The rebuild's own conversion-loss counter, persisted as a DQA result."""
    table = _DM_BY_TABLE[subject.table.partition(".")[2]]
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        try:
            cur.execute(build_konvertering_sql(table))
            columns = [d[0] for d in cur.description]
            row = cur.fetchone()
        finally:
            cur.close()
    tab = {c: int(v or 0) for c, v in zip(columns, row, strict=True)}
    total = sum(tab.values())
    return CheckOutcome(
        passed=total == 0,
        violations=total,
        extra={"tab_pr_kolonne": tab},
    )


def _match_fordeling(context, subject, spec, resources) -> CheckOutcome:
    """Observability: match-quality counts per role on the egenskaber table.

    Always passes (Severity.INFO); the point is the trend in dqa.Results —
    the exact-match share follows the EPJ rollout (≈0 % in 2020, 66-70 %
    from 2022), so a cap makes no sense while pre-rollout years dominate.
    """
    egenskaber = spec.params["egenskaber_table"]
    fordeling: dict[str, dict[str, int]] = {}
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        try:
            for column in spec.params["kvalitet_columns"]:
                cur.execute(
                    f"SELECT [{column}], COUNT(*) FROM [dm].[{egenskaber}] "
                    f"WHERE [dw_is_deleted] = 0 GROUP BY [{column}]"
                )
                fordeling[column] = {str(k): int(n) for k, n in cur.fetchall()}
        finally:
            cur.close()
    return CheckOutcome(passed=True, extra={"fordeling": fordeling})


def _sentinel_spec(table: str, identity: str) -> CheckSpecification | None:
    fk_cols = [c for c in dw_ek_columns(table) if c != identity]
    if not fk_cols:
        return None
    caps = {c: _SENTINEL_CAPS[c] for c in fk_cols if c in _SENTINEL_CAPS}
    return CheckSpecification(
        check_type="sentinel_rate",
        severity=Severity.WARN if caps else Severity.INFO,
        cadence=Cadence.DAILY,
        params={"columns": fk_cols, "max_rate": caps, "where": "[dw_is_deleted] = 0"},
        name_suffix="dw_ek",
    )


def _parity_specs(dm: DmTable) -> list[CheckSpecification]:
    return [
        CheckSpecification(
            check_type="row_parity",
            severity=Severity.ERROR,
            cadence=Cadence.DAILY,
            params={"other_table": f"arc.{dm.arc_table}"},
            name_suffix="vs_arc",
        ),
        CheckSpecification(
            check_type="row_parity",
            severity=Severity.ERROR,
            cadence=Cadence.DAILY,
            params={"other_table": f"dm.{dm.egenskaber_table}"},
            name_suffix="vs_egenskaber",
        ),
    ]


def _kontakt_specs(dm: DmTable) -> list[CheckSpecification]:
    """Relation-key RI plus the two match-quality checks, for the case tables."""
    kontakt = next((k for k in KONTAKT_TABELLER if k.dm_table == dm.dm_table), None)
    if kontakt is None:
        return []
    specs: list[CheckSpecification] = []
    for rolle in kontakt.roller:
        targets: list[tuple[str, str, str]] = [
            (rolle.encounter_column, "fhir.Encounter", "encounter_id")
        ]
        if rolle.location_column:
            targets.append((rolle.location_column, "fhir.Location", "location_id"))
        for column, parent_table, parent_key in targets:
            specs.append(
                CheckSpecification(
                    check_type="referential_integrity",
                    severity=Severity.ERROR,
                    cadence=Cadence.DAILY,
                    params={
                        "column": column,
                        "parent_table": parent_table,
                        "parent_key": parent_key,
                        "required": False,
                        "ignore_values": [""],
                    },
                    name_suffix=column,
                )
            )

    kvalitet_columns = [rolle.kvalitet_column for rolle in kontakt.roller]
    ikke_koert = " OR ".join(f"[{c}] = 'ikke_koert'" for c in kvalitet_columns)
    specs.append(
        CheckSpecification(
            # The stamp rewrites EVERY row (soft-deleted included), so any
            # 'ikke_koert' after a publish means the stamp did not run after
            # the rebuild — exactly what the DSO sync's dependency on the
            # kontakt asset exists to prevent.
            check_type="custom_sql",
            severity=Severity.ERROR,
            cadence=Cadence.DAILY,
            params={
                "sql": (
                    f"SELECT COUNT(*) FROM [dm].[{kontakt.egenskaber_table}] " f"WHERE {ikke_koert}"
                ),
                "checked_sql": f"SELECT COUNT(*) FROM [dm].[{kontakt.egenskaber_table}]",
                "examples_sql": (
                    f"SELECT TOP 20 [{kontakt.dw_ek_column}] "
                    f"FROM [dm].[{kontakt.egenskaber_table}] WHERE {ikke_koert}"
                ),
            },
            name_suffix="ikke_koert",
        )
    )
    specs.append(
        CheckSpecification(
            check_type="match_fordeling",
            severity=Severity.INFO,
            cadence=Cadence.DAILY,
            params={
                "egenskaber_table": kontakt.egenskaber_table,
                "kvalitet_columns": kvalitet_columns,
            },
            name_suffix="fordeling",
            fn=_match_fordeling,
        )
    )
    return specs


def _dm_subject(dm: DmTable) -> DqaSubject:
    table = f"dm.{dm.dm_table}"
    identity = dm.dw_ek_column
    checks: list[CheckSpecification] = []
    sentinel = _sentinel_spec(table, identity)
    if sentinel is not None:
        checks.append(sentinel)
    checks += _parity_specs(dm)
    checks.append(
        CheckSpecification(
            check_type="konverteringstab",
            severity=Severity.WARN,
            cadence=Cadence.DAILY,
            name_suffix="konverteringstab",
            fn=_konverteringstab,
        )
    )
    checks += referential_specs(table, cadence=Cadence.DAILY)
    checks += _kontakt_specs(dm)
    return DqaSubject(
        name=f"dwh_{dm.arc_table}",
        track=Track.DWH,
        table=table,
        resource_key=_RES,
        identity_column=identity,
        checks=tuple(checks),
        deleted_column="dw_is_deleted",
        source="haiba",
    )


def _leverance_subject() -> DqaSubject:
    return DqaSubject(
        name="dwh_haiba_leverance",
        track=Track.DWH,
        table="meta.HAIBALeverance",
        resource_key=_RES,
        identity_column="kildetabel",
        checks=(
            CheckSpecification(
                check_type="freshness",
                severity=Severity.WARN,
                cadence=Cadence.DAILY,
                params={
                    "watermark_column": "leverance_dato",
                    # SDS delivers weekly; 14 days absorbs a skipped delivery
                    # and holidays before flagging.
                    "max_age_days": 14,
                    "group_column": "kildetabel",
                    "expected_groups": [t.arc_table for t in DM_TABLES],
                },
                name_suffix="leverance",
            ),
        ),
        source="haiba",
    )


# cases HAI_type -> agg HAI: the CDI types are spelled differently on the two
# sides (CDI_COHA -> COHA, CDI_HOHA -> HOHA); every other type maps 1:1. The
# mapping lives in the CASE below.
#
# The delivery dates that gate the comparison come from meta.HAIBALeverance,
# NOT MAX(arc.leverance_dato): arc.leverance_dato only advances when a row's
# values change, so on the near append-only case feed two sides that were
# delivered on the same date could still look out of step — the gate would
# then never compare anything and stand green while inert. The register records
# the actual delivery of every type, so the gate reflects what was ingested.
# The register keys cases types as 'CDI_COHA' (its arc-column value), so the
# lev CTEs map them to the agg spelling before joining.
_KONTROLTOTAL_SQL = """
WITH cases AS (
    SELECT CASE [HAI_type]
               WHEN N'CDI_COHA' THEN N'COHA'
               WHEN N'CDI_HOHA' THEN N'HOHA'
               ELSE [HAI_type]
           END AS [HAI],
           COUNT(*) AS [n]
    FROM [arc].[haiba_cases_infektion] WHERE [dw_is_deleted] = 0 GROUP BY [HAI_type]
), agg AS (
    SELECT [HAI], SUM(TRY_CONVERT(BIGINT, NULLIF([antal], ''))) AS [s]
    FROM [arc].[haiba_agg_infektion]
    WHERE [dw_is_deleted] = 0
      AND [Rolle_ansvar] = N'Kontaktansvar' AND [RegionKode] = N'1083'
    GROUP BY [HAI]
), cases_lev AS (
    SELECT CASE [hai_type]
               WHEN N'CDI_COHA' THEN N'COHA'
               WHEN N'CDI_HOHA' THEN N'HOHA'
               ELSE [hai_type]
           END AS [HAI],
           MAX([leverance_dato]) AS [lev]
    FROM [meta].[HAIBALeverance]
    WHERE [kildetabel] = N'haiba_cases_infektion'
    GROUP BY [hai_type]
), agg_lev AS (
    SELECT [hai_type] AS [HAI], MAX([leverance_dato]) AS [lev]
    FROM [meta].[HAIBALeverance]
    WHERE [kildetabel] = N'haiba_agg_infektion'
    GROUP BY [hai_type]
)
SELECT COALESCE(c.[HAI], a.[HAI]), c.[n], a.[s], cl.[lev], al.[lev]
FROM cases AS c
FULL OUTER JOIN agg AS a ON a.[HAI] = c.[HAI]
LEFT JOIN cases_lev AS cl ON cl.[HAI] = c.[HAI]
LEFT JOIN agg_lev  AS al ON al.[HAI] = a.[HAI]
"""


def _kontroltotal(context, subject, spec, resources) -> CheckOutcome:
    """Case count == agg SUM(antal) per HAI type — when the deliveries align.

    A violation only exists when both sides' newest leverance_dato MATCH and
    the totals still differ (verified exact in the delivery profiling). Types
    whose deliveries are out of step land in extra as 'ude_af_takt' — routine
    between weekly drops, so failing on them would be a standing false alarm.
    """
    with get_conn(resources, subject.resource_key) as conn:
        cur = conn.cursor()
        try:
            cur.execute(_KONTROLTOTAL_SQL)
            rows = cur.fetchall()
        finally:
            cur.close()

    afvigelser: dict[str, dict] = {}
    ude_af_takt: dict[str, dict] = {}
    for hai, cases_n, agg_sum, cases_lev, agg_lev in rows:
        detail = {
            "cases": int(cases_n) if cases_n is not None else None,
            "agg": int(agg_sum) if agg_sum is not None else None,
            "leverance_cases": str(cases_lev),
            "leverance_agg": str(agg_lev),
        }
        if cases_lev is not None and agg_lev is not None and cases_lev == agg_lev:
            if cases_n != agg_sum:
                afvigelser[str(hai)] = detail
        else:
            ude_af_takt[str(hai)] = detail
    # Surfaced so a permanently inert gate cannot hide: if EVERY type is out of
    # step (or unregistered), the check compares nothing and still reports
    # green. That is legitimate transiently between weekly drops, so it is a
    # visible flag rather than a violation — but a standing True is the signal
    # that the register or the delivery cadence needs a look.
    ingen_typer_i_takt = bool(rows) and len(ude_af_takt) == len(rows)
    return CheckOutcome(
        passed=not afvigelser,
        checked=len(rows),
        violations=len(afvigelser),
        examples=sorted(afvigelser),
        extra={
            "afvigelser": afvigelser,
            "ude_af_takt": ude_af_takt,
            "ingen_typer_i_takt": ingen_typer_i_takt,
        },
    )


def _arc_subject(dm: DmTable) -> DqaSubject:
    """Delivery-level checks on one arc table: per-type freshness + encoding."""
    schema = dm.schema
    text_columns = [c for c in schema.all_columns if c != "leverance_dato"]
    # BIN2 collation is load-bearing: U+FFFD is collation-ignorable in the
    # database's default collation, so a plain LIKE N'%' + NCHAR(65533) + N'%'
    # matches EVERY non-empty string (measured on dev: 6,791,256 of 6,791,256
    # agg rows "matched", 0 under BIN2). A binary collation makes the match
    # literal, so the check counts real replacement characters, not every cell.
    ffd = " + ".join(
        f"SUM(CASE WHEN [{c}] COLLATE Latin1_General_100_BIN2 "
        f"LIKE N'%' + NCHAR(65533) + N'%' THEN 1 ELSE 0 END)"
        for c in text_columns
    )
    checks: list[CheckSpecification] = [
        CheckSpecification(
            check_type="freshness",
            severity=Severity.WARN,
            cadence=Cadence.DAILY,
            params={
                # Read the per-type delivery register, NOT arc.leverance_dato:
                # that column only advances when a row's values change, and the
                # near append-only case feed can go weeks per type without one,
                # so a per-type check against arc would alarm on a live feed.
                # meta.HAIBALeverance records every delivery of every type.
                "table": "meta.HAIBALeverance",
                "watermark_column": "leverance_dato",
                # SDS delivers weekly; UTI has been gone for six weeks before,
                # and three counts as "someone should look".
                "max_age_days": 21,
                # hai_type carries the arc-column vocabulary (agg: 'COHA').
                "group_column": "hai_type",
                # Deliberately no expected_groups: SDS adds and retires HAI
                # types without notice; a retired type stops being registered
                # and leaves the check instead of alarming forever.
                "where": f"[kildetabel] = N'{dm.arc_table}'",
            },
            name_suffix="pr_hai_type",
        ),
        CheckSpecification(
            # No examples on purpose: the cases PKs carry CPR. Counts only.
            # Active rows only: a historical U+FFFD row SDS has since retracted
            # is soft-deleted, so it must not keep the check red forever (it can
            # no longer be corrected or cleaned up) — the same argument the
            # freshness check makes one spec up.
            check_type="custom_sql",
            severity=Severity.WARN,
            cadence=Cadence.DAILY,
            params={
                "sql": f"SELECT {ffd} FROM [arc].[{dm.arc_table}] WHERE [dw_is_deleted] = 0",
                "checked_sql": (
                    f"SELECT COUNT(*) FROM [arc].[{dm.arc_table}] WHERE [dw_is_deleted] = 0"
                ),
            },
            name_suffix="encoding_ufffd",
        ),
    ]
    if dm.arc_table == "haiba_cases_infektion":
        checks.append(
            CheckSpecification(
                check_type="kontroltotal",
                severity=Severity.ERROR,
                cadence=Cadence.DAILY,
                name_suffix="cases_vs_agg",
                fn=_kontroltotal,
            )
        )
    return DqaSubject(
        name=f"dwh_haiba_arc_{dm.schema.name}",
        track=Track.DWH,
        table=f"arc.{dm.arc_table}",
        resource_key=_RES,
        identity_column=schema.type_column,
        checks=tuple(checks),
        # Harmonised with _dm_subject: the three checks here are all raw SQL
        # today, but not_null/uniqueness/referential_integrity read
        # subject.deleted_column, so leaving it unset is a trap for the next
        # check added to this subject.
        deleted_column="dw_is_deleted",
        source="haiba",
    )


HAIBA_SUBJECTS: list[DqaSubject] = (
    [_dm_subject(t) for t in DM_TABLES]
    + [_arc_subject(t) for t in DM_TABLES]
    + [_leverance_subject()]
)

__all__ = ["HAIBA_SUBJECTS"]
