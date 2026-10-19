# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""DQA subjects for the udredningsret datamart (US-6, spec section 9).

Ten subjects, all prefixed ``udredningsret_``, all ``custom_sql`` and inline, all
**counts only** — no check here returns a row value, an example key or anything
derived from CPR. That is not just GDPR hygiene: the domain's own business key
(``clean_code_Helbredsforloeb`` = ``UDREDHF_`` + a SHA2_256 over the citizen's CPR
and the first marker) is a stable pseudonym, so surfacing it in ``dqa.Results``
would put a person-referable value in a table read across environments. Every
check therefore reports a violation count and a denominator (``checked_sql``) and
nothing else; an operator who needs the offending keys re-runs the documented SQL
inside the warehouse. Spec: docs/source_systems/udredningsret/udredningsret_model.md
section 9 (definitions and measured numbers) and section 8 (orchestration).

**Why these are not derived from the lineage.** The fact-level check families that
CAN be derived — ``sentinel_rate`` per FK role and weekly ``referential_integrity``
— already exist: ``dwh_udredningsforloeb`` (US-4) and ``dwh_udredningsretmarkoer``
(US-5) in ``dwh_subjects.py``. The subjects here are the ones no lineage edge can
express: source-population properties (duplicate markers, several end markers in
one forloeb), model invariants across arc and fct, and the per-helbredsforloeb
reconciliation. They complement the ``_FACTS`` entries, they do not repeat them.

**Track and routing.** ``Track.DWH`` with ``source=None``, so the anchors land in
``dqa_dwh_daily_job`` / ``dqa_dwh_weekly_job`` exactly as spec section 8 requires
and as the two ``_FACTS`` subjects for the same tables already do. HAIBA splits its
own source (``dqa_dwh_haiba_daily_job``) because its whole dm layer is a separate
quality surface; splitting here would scatter the checks on ``fct.Udredningsforloeb``
across two jobs for no gain.

**Cadence.** Every check runs at ``Cadence.DAILY`` on purpose, the same argument
``haiba_subjects.py`` makes: the udredningsret tables only change when
``udredningsret_publish_job`` runs, which is delivery-driven (weekly-ish, sensor
requested — see ``sensors/udredningsret_publish_sensor.py``), and
``dqa_udredningsret_after_publish`` is chained onto exactly that job. "Daily" here
means "after every publish", which is the real cadence of the data. A WEEKLY tier
would be worse than useless for these: ``dqa_dwh_weekly_job`` has no cron, so a
weekly check would only ever run when a publish happened to land on a Saturday.
The heavier subjects (3, 4, 9, 10 scan the 8.2 M-row marker/archive population)
therefore pay their cost once per delivery, not once per day.

That cost is **conditional**, though: ``dqa_dwh_daily_job`` is shared, and
``dqa_erp_daily_after_dfm`` requests the same job after the **nightly**
``erp_daily_dfm_job``. "Once per delivery" holds for as long as that ERP sensor is
STOPPED — which it is today, as are ``dqa_drg_after_publish`` and
``dqa_udredningsret_after_publish``. Turn it on and subjects 3, 4, 9 and 10 scan
8.2 M rows every night without anyone touching udredningsret. If that ever becomes
a real cost the answer is a source split of its own, as HAIBA has
(``dqa_dwh_haiba_daily_job``) — not a WEEKLY tier, which would still have no cron.

**Severity.** Deliberately three tiers, because these counts do not all mean the
same thing:

- ``Severity.INFO`` (subjects 1-7 and 9) — properties of SDS' own data that are
  expected to be non-zero and have no agreed baseline yet. INFO always passes, so
  the number is recorded in ``dqa.Results`` and trended instead of alarming every
  publish. Promote one to WARN with a cap once its baseline has been read off
  ``dqa.Results``, exactly as ``_SENTINEL_MAX_RATES`` does for the facts.
- ``Severity.WARN`` (subject 8) — a SOR code that is present but does not resolve
  is a lookup regression, not a source property. Measured 0 of 738 forloeb units
  and 0 of 1,360 kontakt units (spec section 5), so any count is news.
- ``Severity.ERROR`` (subject 10) — the per-helbredsforloeb reconciliation. A
  deviation means the fact does not reproduce the archive, which is a model defect.
  Not BLOCKING: DQA surfaces, it never gates the ETL (the fact's own control total
  already fails the run on the global total).

**The measured numbers in spec section 9 are the expectation.** The ADO acceptance
criteria quote 12,236 "key duplicates" and 31,110 "several end markers"; those come
from the profiling script, which counted rows and used wider code groups than
``MARKOER_GRUPPER``. The definitions below are the ones expressible on the model's
own tables and the numbers beside them are what was measured on the delivery of
2026-09-07 (10,531 and 31,057). The profiling numbers are deliberately not chased.
"""

from __future__ import annotations

from dqa.model import Cadence, CheckSpecification, DqaSubject, Severity, Track
from DSA.SQL.staging.udredningsret_kolonner import (
    DIM_EGENSKABER_DWH_TABLE,
    DIM_FORLOEBSMARKOER_DWH_TABLE,
    FCT_FORLOEB_DWH_TABLE,
    FCT_MARKOER_DWH_TABLE,
    MAP_HF_TABLE,
    MARKOER_GRUPPE_TEKSTER,
    UNKNOWN_ORGANISATION,
    clean_code_helbredsforloeb_sql,
)
from DSI.csv_files.udredningsret_datasets import UDREDNINGSRET_MONT

_RES = "mssql_target_staging"  # arc/map/dim/fct all live on the Staging server (DSA)

# arc and the delivery register, single-sourced from the dataset config so a
# renamed ext/arc table can never leave a check pointing at a table that is gone.
_ARC_TABLE = f"[arc].[{UDREDNINGSRET_MONT.ext_table}]"
_LEVERANCE_TABLE = "[meta].[UdredningsretLeverance]"
_KILDETABEL = UDREDNINGSRET_MONT.ext_table

# The -1 UNKNOWN member every map/dim in the domain reserves (05_seeds.sql). On
# fct.UdredningsretMarkoer.dw_ek_Udredningsforloeb it is not a failed lookup but
# design: a marker without an F_ID belongs to no forloeb (decision U5), so the
# per-forloeb subjects below must exclude it or they would aggregate 1.17 M
# unrelated markers into one phantom group.
_UKENDT = -1

# Two years. Long enough that no real assessment path reaches it, short enough
# that a date-parsing regression on the start marker shows up (spec section 9).
_MAX_UDREDNINGSTID_DAGE = 730


def _gruppe(navn: str) -> str:
    """Return a marker-group name, checked against the staging tuple it must match.

    ``dim.Forloebsmarkoer.gruppe`` is written by ``markoer_gruppe_sql`` from
    ``MARKOER_GRUPPER``. Renaming a group there would otherwise leave the checks
    below silently counting zero rows forever; this raises at import (i.e. when
    Dagster loads the definitions) instead.
    """
    if navn not in MARKOER_GRUPPE_TEKSTER:
        raise ValueError(
            f"Unknown marker group {navn!r} — it must be one of "
            f"{sorted(MARKOER_GRUPPE_TEKSTER)} (udredningsret_kolonner.MARKOER_GRUPPER)."
        )
    return navn


_ENDELIGT_UDREDT = _gruppe("endeligt_udredt")
_AFBRUDT = _gruppe("afbrudt")

# Live rows of each table, the denominator most of the checks report as "checked".
_LEVENDE_FORLOEB = f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0"
_LEVENDE_MARKOERER = f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} WHERE [dw_is_deleted] = 0"
_LEVENDE_ARC = f"SELECT COUNT(*) FROM {_ARC_TABLE} WHERE [dw_is_deleted] = 0"


def _subject(
    navn: str,
    tabel: str,
    identitet: str,
    checks: tuple[CheckSpecification, ...],
    deleted_column: str | None = None,
) -> DqaSubject:
    """One udredningsret subject.

    Every check here carries its own ``dw_is_deleted`` predicate, so the subject's
    ``deleted_column`` changes nothing today — it is set anyway (as
    ``haiba_subjects._arc_subject`` argues) so the next check added to a subject,
    if it is a row-level registered check, scopes to active rows by itself.
    """
    return DqaSubject(
        name=navn,
        track=Track.DWH,
        table=tabel,
        resource_key=_RES,
        identity_column=identitet,
        checks=checks,
        deleted_column=deleted_column,
    )


def _tael(sql: str, checked: str, suffix: str, severity: Severity) -> CheckSpecification:
    """A counts-only custom_sql check. No ``examples_sql`` anywhere — see module docstring."""
    return CheckSpecification(
        check_type="custom_sql",
        severity=severity,
        cadence=Cadence.DAILY,
        params={"sql": sql, "checked_sql": checked},
        name_suffix=suffix,
    )


# ---------------------------------------------------------------------------
# 1. Double-registered markers — the rows the archive collapsed (measured: 1,705
#    source rows reduced on the delivery of 2026-09-07).
#
#    Two counts because the spec's definition and its measured number are two
#    different numbers: how many fact rows stand for more than one source row,
#    and how many source rows that removed. antal_kilderaekker is NOT NULL
#    DEFAULT 1, so the sum needs no ISNULL; it is cast to BIGINT because SUM over
#    INT overflows at 2^31 and the population is 8.2 M rows.
# ---------------------------------------------------------------------------

_DOBBELTREGISTREREDE = _subject(
    "udredningsret_dobbeltregistrerede_markoerer",
    "fct.UdredningsretMarkoer",
    "dw_ek_UdredningsretMarkoer",
    (
        _tael(
            f"SELECT COUNT(*) FROM {FCT_MARKOER_DWH_TABLE} "
            f"WHERE [dw_is_deleted] = 0 AND [antal_kilderaekker] > 1",
            _LEVENDE_MARKOERER,
            "raekker",
            Severity.INFO,
        ),
        _tael(
            f"SELECT ISNULL(SUM(CAST([antal_kilderaekker] AS BIGINT)) - COUNT_BIG(*), 0) "
            f"FROM {FCT_MARKOER_DWH_TABLE} "
            f"WHERE [dw_is_deleted] = 0 AND [antal_kilderaekker] > 1",
            _LEVENDE_MARKOERER,
            "reducerede_kilderaekker",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 2. Duplicates on the marker key — archive rows that share (markoer_noegle) and
#    are separated by loebenummer (measured 10,531). Counted on arc, not on the
#    fact: the fact is 1:1 with the archive row by construction, so the archive
#    is where the property lives. The profiling script's 12,236 counted a wider
#    population (see module docstring).
# ---------------------------------------------------------------------------

_MARKOERNOEGLE_DUBLETTER = _subject(
    "udredningsret_markoernoegle_dubletter",
    f"arc.{UDREDNINGSRET_MONT.ext_table}",
    "markoer_noegle",
    (
        _tael(
            f"SELECT COUNT(*) FROM {_ARC_TABLE} "
            f"WHERE [dw_is_deleted] = 0 AND [loebenummer] > 1",
            _LEVENDE_ARC,
            "loebenummer",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 3. Forloeb with more than one "endeligt udredt" marker (measured 31,057).
#    A forloeb should end once; SDS' data says otherwise often enough that this
#    is observability, not a gate. The group comes from dim.Forloebsmarkoer, so
#    the check reads the same classification staging wrote.
#
#    The subject is the forloeb fact even though the evidence is read from the
#    marker fact: both the violation count and the denominator are forloeb, and
#    naming the marker table would make the reported rate read as a share of 8.2 M
#    markers instead of a share of 1.9 M forloeb.
# ---------------------------------------------------------------------------

_FLERE_AFSLUTNINGER_SQL = f"""
SELECT COUNT(*) FROM (
    SELECT f.[dw_ek_Udredningsforloeb]
    FROM {FCT_MARKOER_DWH_TABLE} AS f
    INNER JOIN {DIM_FORLOEBSMARKOER_DWH_TABLE} AS d
        ON d.[dw_ek_Forloebsmarkoer] = f.[dw_ek_Forloebsmarkoer]
    WHERE f.[dw_is_deleted] = 0
      AND f.[dw_ek_Udredningsforloeb] <> {_UKENDT}
      AND d.[gruppe] = N'{_ENDELIGT_UDREDT}'
    GROUP BY f.[dw_ek_Udredningsforloeb]
    HAVING COUNT(*) > 1
) AS g
"""

_FLERE_ENDELIGT_UDREDT = _subject(
    "udredningsret_flere_endeligt_udredt",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (_tael(_FLERE_AFSLUTNINGER_SQL, _LEVENDE_FORLOEB, "pr_forloeb", Severity.INFO),),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 4. Forloeb carrying both an end marker and an interruption (measured 43,730).
#    Contradictory by the datamart's own description — a forloeb is either
#    completed or broken off — so the count is the size of that contradiction.
# ---------------------------------------------------------------------------

_AFSLUTNING_OG_AFBRYDELSE_SQL = f"""
SELECT COUNT(*) FROM (
    SELECT f.[dw_ek_Udredningsforloeb]
    FROM {FCT_MARKOER_DWH_TABLE} AS f
    INNER JOIN {DIM_FORLOEBSMARKOER_DWH_TABLE} AS d
        ON d.[dw_ek_Forloebsmarkoer] = f.[dw_ek_Forloebsmarkoer]
    WHERE f.[dw_is_deleted] = 0
      AND f.[dw_ek_Udredningsforloeb] <> {_UKENDT}
      AND d.[gruppe] IN (N'{_ENDELIGT_UDREDT}', N'{_AFBRUDT}')
    GROUP BY f.[dw_ek_Udredningsforloeb]
    HAVING MAX(CASE WHEN d.[gruppe] = N'{_ENDELIGT_UDREDT}' THEN 1 ELSE 0 END) = 1
       AND MAX(CASE WHEN d.[gruppe] = N'{_AFBRUDT}' THEN 1 ELSE 0 END) = 1
) AS g
"""

_AFSLUTNING_OG_AFBRYDELSE = _subject(
    "udredningsret_afslutning_og_afbrydelse",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (_tael(_AFSLUTNING_OG_AFBRYDELSE_SQL, _LEVENDE_FORLOEB, "pr_forloeb", Severity.INFO),),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 5. Assessment time beyond two years (measured 4,788). udredningstid_dage is
#    only carried for rows in the indicator (decision U2), so the denominator is
#    those rows — not every forloeb — or the rate would drift with the share of
#    open forloeb rather than with the outliers.
# ---------------------------------------------------------------------------

_LANG_UDREDNINGSTID = _subject(
    "udredningsret_lang_udredningstid",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (
        _tael(
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0 "
            f"AND [udredningstid_dage] > {_MAX_UDREDNINGSTID_DAGE}",
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 0 "
            f"AND [udredningstid_dage] IS NOT NULL",
            "over_730_dage",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 6. Indicator value changed since the last delivery (~5,000/month expected).
#
#    Expressible form, as the spec defines it: live fact rows that were UPDATED
#    rather than inserted (dw_timestamp_update > dw_timestamp_insert) inside the
#    current delivery period. The fact is Type 1, so the previous value is gone
#    and "the classification or udredningstid_dage differs" cannot be asked of
#    the table directly — but the load's MERGE only touches a row when its EXCEPT
#    change-detection finds a real difference, so an updated row IS a row whose
#    values changed in this delivery. The count is therefore a superset of "the
#    indicator changed": it also catches a changed organisation or diagnosis.
#
#    The period is bounded by the newest registered delivery, the same signature
#    the publish sensor reads. If the register is empty the bound becomes
#    9999-12-31 and the window is empty — a deliberately fail-safe direction:
#    without it a missing register would turn the check into "every row ever
#    updated" and report a meaningless five-figure number as news.
# ---------------------------------------------------------------------------

_SENESTE_LEVERANCE_SQL = (
    f"SELECT MAX([leverance_tidsstempel]) FROM {_LEVERANCE_TABLE} "
    f"WHERE [kildetabel] = N'{_KILDETABEL}'"
)

_AENDRET_INDIKATORVAERDI_SQL = f"""
SELECT COUNT(*)
FROM {FCT_FORLOEB_DWH_TABLE}
WHERE [dw_is_deleted] = 0
  AND [dw_timestamp_update] > [dw_timestamp_insert]
  AND [dw_timestamp_update] >= ISNULL(
      ({_SENESTE_LEVERANCE_SQL}), CAST('9999-12-31' AS DATETIME2))
"""

_AENDRET_INDIKATORVAERDI = _subject(
    "udredningsret_aendret_indikatorvaerdi",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (
        _tael(
            _AENDRET_INDIKATORVAERDI_SQL,
            _LEVENDE_FORLOEB,
            "leveranceperiode",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 7. Retired forloeb (0 on the first delivery). Retirement is legitimate — a
#    forloeb whose markers were all soft-deleted, or were regrouped under another
#    start, keeps its row with dw_is_deleted = 1 (spec section 6) — so this is a
#    trend, not a defect. The denominator is EVERY row (live and retired), which
#    is what makes the number a share of the fact rather than of what survived.
# ---------------------------------------------------------------------------

_SOFT_SLETTEDE_FORLOEB = _subject(
    "udredningsret_soft_slettede_forloeb",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (
        _tael(
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE} WHERE [dw_is_deleted] = 1",
            f"SELECT COUNT(*) FROM {FCT_FORLOEB_DWH_TABLE}",
            "retirerede",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 8. SOR without a lookup, per role (measured 0 and 0).
#
#    A blank SOR code legitimately resolves to -1, so the check only counts rows
#    where the raw code is PRESENT and the key is still the sentinel — i.e. a
#    code dim.Organisation does not know. The raw codes live on the transaction
#    dimension, which shares the fact's PK, so the join is PK-to-PK on the same
#    grain. One check per role, because the two roles fail for different reasons
#    (a forloeb unit is the reporting department, a kontakt unit the treating
#    one) and an aggregate would hide which one regressed.
# ---------------------------------------------------------------------------

_SOR_ROLLER: tuple[tuple[str, str], ...] = (
    ("dw_ek_Organisation_forloeb", "forloeb_enhed_sor"),
    ("dw_ek_Organisation_kontakt", "kontakt_enhed_sor"),
)


def _sor_check(noegle_kolonne: str, raa_kolonne: str) -> CheckSpecification:
    krop = (
        f"FROM {FCT_FORLOEB_DWH_TABLE} AS f "
        f"INNER JOIN {DIM_EGENSKABER_DWH_TABLE} AS e "
        f"ON e.[dw_ek_Udredningsforloeb] = f.[dw_ek_Udredningsforloeb] "
        f"WHERE f.[dw_is_deleted] = 0 "
        f"AND NULLIF(LTRIM(RTRIM(e.[{raa_kolonne}])), '') IS NOT NULL"
    )
    return _tael(
        f"SELECT COUNT(*) {krop} AND f.[{noegle_kolonne}] = {UNKNOWN_ORGANISATION}",
        f"SELECT COUNT(*) {krop}",
        raa_kolonne,
        Severity.WARN,
    )


_SOR_UDEN_OPSLAG = _subject(
    "udredningsret_sor_uden_opslag",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    tuple(_sor_check(noegle, raa) for noegle, raa in _SOR_ROLLER),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 9. Helbredsforloeb whose first marker changed (0 on the first delivery).
#
#    When SDS changes a helbredsforloeb's first marker (~0.01 %/month), the
#    archive mints a new helbredsforloeb_noegle: the old markers are soft-deleted
#    and the forloeb under the old key are retired, while a new key appears with
#    the same citizen and the same forloebselement. The check counts the retired
#    keys that have such a successor — the citizen key and the first
#    forloebselement UUID are the pair that survives the renumbering.
#
#    dw_ek_Borger must be a real member: -1 (CPR not yet in dim.Borger) and -2
#    (structurally absent CPR) are shared by thousands of unrelated rows, so
#    matching on them would manufacture successors. A helbredsforloeb counts if
#    ANY of its retired forloeb has a successor, which is wider than "all of its
#    forloeb were retired" — deliberately, since a partial regroup is the same
#    signal.
# ---------------------------------------------------------------------------

_SKIFTET_FOERSTE_MARKOER_SQL = f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT gammel.[dw_ek_Helbredsforloeb]
    FROM {FCT_FORLOEB_DWH_TABLE} AS gammel
    INNER JOIN {DIM_EGENSKABER_DWH_TABLE} AS gammel_e
        ON gammel_e.[dw_ek_Udredningsforloeb] = gammel.[dw_ek_Udredningsforloeb]
    WHERE gammel.[dw_is_deleted] = 1
      AND gammel.[dw_ek_Borger] > 0
      AND NULLIF(LTRIM(RTRIM(gammel_e.[foerste_forloebselement_uuid])), '') IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM {FCT_FORLOEB_DWH_TABLE} AS ny
          INNER JOIN {DIM_EGENSKABER_DWH_TABLE} AS ny_e
              ON ny_e.[dw_ek_Udredningsforloeb] = ny.[dw_ek_Udredningsforloeb]
          WHERE ny.[dw_is_deleted] = 0
            AND ny.[dw_ek_Borger] = gammel.[dw_ek_Borger]
            AND ny.[dw_ek_Helbredsforloeb] <> gammel.[dw_ek_Helbredsforloeb]
            AND ny_e.[foerste_forloebselement_uuid]
                = gammel_e.[foerste_forloebselement_uuid]
      )
) AS hf
"""

_SKIFTET_FOERSTE_MARKOER_CHECKED_SQL = f"""
SELECT COUNT(*) FROM (
    SELECT DISTINCT [dw_ek_Helbredsforloeb]
    FROM {FCT_FORLOEB_DWH_TABLE}
    WHERE [dw_is_deleted] = 1
) AS hf
"""

_SKIFTET_FOERSTE_MARKOER = _subject(
    "udredningsret_skiftet_foerste_markoer",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (
        _tael(
            _SKIFTET_FOERSTE_MARKOER_SQL,
            _SKIFTET_FOERSTE_MARKOER_CHECKED_SQL,
            "ny_helbredsforloeb_noegle",
            Severity.INFO,
        ),
    ),
    deleted_column="dw_is_deleted",
)

# ---------------------------------------------------------------------------
# 10. Forloeb count per helbredsforloeb against the archive (expected 0).
#
#     The backstop the fact's own control total cannot be. That total compares
#     two scalars over the whole population (decision U13), so a split and a
#     merge of forloeb inside the same window cancel out and the run goes green
#     with both keys wrong. This compares PER helbredsforloeb — a FULL OUTER JOIN
#     between the archive's (helbredsforloeb_noegle, F_ID) groups and the live
#     fact rows resolved through map.Helbredsforloeb — so cancellation ACROSS keys
#     is impossible and the count is the number of keys whose forloeb COUNT
#     disagrees. It is the cause-agnostic complement to U15's regroup detector in
#     the archive: U15 keeps the assumption true, this is the independent check
#     afterwards.
#
#     Its reach stops at the count. A regroup that preserves the number of groups
#     inside one helbredsforloeb — a marker moving from F_ID A to F_ID B while both
#     groups survive — is still U15's territory alone. Verified on SQL Server with
#     this check's own expressions: a split plus a merge in the same window leaves
#     U13's control total green and makes this report 2, while a pure membership
#     swap leaves both green. So "0 here" means "no key has the wrong NUMBER of
#     forloeb", not "the fact reproduces the archive group by group".
#
#     The key on both sides is the clean_code, built with the staging chain's own
#     builder, so the prefix is never re-spelled here. A fact row whose
#     helbredsforloeb key is the -1 sentinel joins the map's 'UNKNOWN' member,
#     which the archive side never produces — it is reported as a difference,
#     which is exactly right (the load's UNMAPPED guard means it should not
#     happen at all). Blank F_ID is excluded on the archive side: those markers
#     belong to no forloeb (decision U5) and the fact has no row for them.
#
#     The denominator is every live helbredsforloeb in the archive (2,233,064
#     measured), read off the (helbredsforloeb_noegle, dw_is_deleted) index. It
#     is wider than the compared population by the keys whose markers all lack an
#     F_ID, which is what makes it a stable reference rather than a number that
#     moves with the violation set.
# ---------------------------------------------------------------------------

_ARKIV_CLEAN_CODE = clean_code_helbredsforloeb_sql("a.[helbredsforloeb_noegle]")

_FORLOEB_PR_HELBREDSFORLOEB_SQL = f"""
WITH arkiv AS (
    SELECT
        {_ARKIV_CLEAN_CODE} AS [clean_code_Helbredsforloeb],
        COUNT(DISTINCT LTRIM(RTRIM(a.[F_ID]))) AS [antal]
    FROM {_ARC_TABLE} AS a
    WHERE a.[dw_is_deleted] = 0
      AND a.[F_ID] IS NOT NULL AND LTRIM(RTRIM(a.[F_ID])) <> ''
    GROUP BY a.[helbredsforloeb_noegle]
),
fakta AS (
    SELECT m.[clean_code_Helbredsforloeb], COUNT(*) AS [antal]
    FROM {FCT_FORLOEB_DWH_TABLE} AS f
    INNER JOIN {MAP_HF_TABLE} AS m
        ON m.[dw_ek_Helbredsforloeb] = f.[dw_ek_Helbredsforloeb]
    WHERE f.[dw_is_deleted] = 0
    GROUP BY m.[clean_code_Helbredsforloeb]
)
SELECT COUNT(*)
FROM arkiv AS a
FULL OUTER JOIN fakta AS f
    ON f.[clean_code_Helbredsforloeb] = a.[clean_code_Helbredsforloeb]
WHERE ISNULL(a.[antal], 0) <> ISNULL(f.[antal], 0)
"""

_FORLOEB_PR_HELBREDSFORLOEB = _subject(
    "udredningsret_forloeb_pr_helbredsforloeb",
    "fct.Udredningsforloeb",
    "dw_ek_Udredningsforloeb",
    (
        _tael(
            _FORLOEB_PR_HELBREDSFORLOEB_SQL,
            f"SELECT COUNT(DISTINCT [helbredsforloeb_noegle]) FROM {_ARC_TABLE} "
            f"WHERE [dw_is_deleted] = 0",
            "arkiv_vs_fakta",
            Severity.ERROR,
        ),
    ),
    deleted_column="dw_is_deleted",
)


UDREDNINGSRET_SUBJECTS: list[DqaSubject] = [
    _DOBBELTREGISTREREDE,
    _MARKOERNOEGLE_DUBLETTER,
    _FLERE_ENDELIGT_UDREDT,
    _AFSLUTNING_OG_AFBRYDELSE,
    _LANG_UDREDNINGSTID,
    _AENDRET_INDIKATORVAERDI,
    _SOFT_SLETTEDE_FORLOEB,
    _SOR_UDEN_OPSLAG,
    _SKIFTET_FOERSTE_MARKOER,
    _FORLOEB_PR_HELBREDSFORLOEB,
]

__all__ = ["UDREDNINGSRET_SUBJECTS"]
