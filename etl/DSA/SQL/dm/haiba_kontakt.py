# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Stamp the FHIR contact relation onto the dm HAIBA case rows.

A HAIBA case says "this citizen had this infection, on this date, attributed to
this unit". It does not say which hospital contact that was. This module finds
the contact and writes it beside the case, so a consumer can go from an
infection to the admission it belongs to without re-deriving the join.

Since the dimensional split, a case is a fact row
(dw_ek_Borger and the resolved keys) plus an egenskaber row (the raw SOR
codes and dates), joined 1:1 on dw_ek. The relation is stamped in two
places by design: the relation KEYS (encounter_id_*, location_id_*) land on
the FACT so the join out to the contact is direct, while the match QUALITY
(match_kvalitet_*, match_dage_*) lands on the egenskaber table. The rebuild
resets both sides ('ikke_koert' / NULL), so these assets run after it and
re-stamp every row.

Two kinds of role, and they resolve against different FHIR resources:

* A unit that is *responsible* or *performing* — HAI_Kontaktansvar on the
  infection cases, all four SSI roles — is an organisation. It resolves through
  fhir.Organization.identifier_value_sor to Encounter.service_provider_id, and
  is matched against the encounter's own period.
* A unit describing *where the patient physically was* — HAI_Opholdsadresse —
  is a location. It resolves through fhir.Location.identifier_value_sor to
  Encounter_Location, and is matched against the location period, which is
  narrower than the encounter period and therefore the better answer.

Nothing here is a clean join, so every row carries the quality of its own
match rather than a silent best guess. Measured on 26,978 infection cases:

    2020        0% exact      Cosmic was not live
    2021      1.9% exact      rollout under way
    2022+   66-70% exact      steady state

That gradient is the single most important fact about this data. An aggregate
spanning 2020-2026 without filtering on match quality will show infections
"disappearing" from contacts the further back you look, and that is the EPJ
rollout, not infection control. See ``MATCH_KVALITET``.

Of the ~30% that are not exact in the steady-state years:

* ``kun_periode`` (~18%): the patient was admitted at that moment but the
  responsible unit differs. 99% of these have exactly one covering contact, so
  the link to the admission is solid — only the department attribution differs.
  This is expected: HAIBA names the unit deemed responsible for the infection,
  which need not be the one the patient lay in. Checking whether the encounter's
  parent organisation was the HAIBA unit explained 0 of 3,226 cases, so this is
  not a one-level hierarchy mismatch.
One role is weak by nature and it is worth naming so nobody "fixes" it by
switching the join: SSI's ``reo_producent`` reaches only 43/287 exact against
Encounter, because the unit performing a reoperation is neither the admitting
service provider nor the ward. Routing it through Encounter_Location instead
was measured and gives 73/287 — better, still poor, and not enough to justify
treating one role differently from the three beside it, which all score
decisively better through Organization (196, 217 and 220 of 287 respectively).
The admission itself is still identified; only the unit attribution is not.

* ``kun_sor`` (~12%): the right unit, but the date falls outside the contact.
  The gap distribution spikes hard at exactly two days after discharge (719 of
  2,442 cases), which is post-discharge attribution — the infection is dated
  after the patient went home. ``match_dage_*`` carries the signed day gap so a
  consumer can apply their own window; deliberately no threshold is hardcoded
  here, because the case definitions belong to infection control, not to ETL.

``match_dage_*`` counts whole calendar days to the nearest edge of the contact,
so read it together with the quality column, never alone:

    quality in (eksakt, eksakt_flere, kun_periode)  ->  0, the case is inside
    quality == kun_sor and match_dage == 0          ->  outside, by under a day
    quality == kun_sor and match_dage <> 0          ->  outside by that many days
    quality in (intet_match, ingen_kontakter, ...)  ->  NULL, nothing was chosen

The second line is a real group, not an edge case: 269 of 4,229 kun_sor rows
sit within hours of the contact — a sample taken before the admission was
formally opened, or on the evening of the day of discharge. Treating
``match_dage = 0`` on its own as "inside the contact" would silently absorb
them; treating them as a whole day away would overstate the gap.

Full distribution for kontaktansvar over 26,978 cases: 15,442 inside, 888 one
to two days after, 701 three to thirty days after, 343 up to a month before,
585 one to twelve months before, 1,443 more than a year before, 7,576 with no
contact at all. The long negative tail is the same pre-2022 story — a 2020 case
whose only same-unit contact is in 2026.
"""

from dataclasses import dataclass

import dagster as dg
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource

# Sentinel written by the dm rebuild when no citizen could be resolved.
UNKNOWN_BORGER = -1

MATCH_KVALITET = {
    "eksakt": "SOR-kode og tidspunkt matcher, én kandidat",
    "eksakt_flere": "SOR-kode og tidspunkt matcher, flere kandidater — nyeste valgt",
    "kun_periode": "Kontakten dækker tidspunktet, men en anden SOR-kode",
    "kun_sor": "SOR-koden matcher, men tidspunktet ligger uden for kontakten",
    "intet_match": "Borgeren har kontakter, men ingen der matcher",
    "ingen_kontakter": "Borgeren findes i FHIR, men har ingen kontakter",
    "ingen_borger": "Ingen dw_ek_Borger — casen kunne ikke knyttes til en borger",
    "ikke_koert": "Relationen er endnu ikke beregnet for denne række",
}


def _live(alias: str) -> str:
    """Soft-delete predicate for a FHIR flat table, tolerating a legacy NULL.

    dw_is_deleted is now INT NOT NULL DEFAULT 0 on every fhir.* table, so a bare
    `= 0` is the eventual form of this predicate. The ISNULL stays until the
    migration that tightens the column is confirmed applied in EVERY environment:
    against a still-nullable column `= 0` is UNKNOWN for NULL and silently
    returns nothing at all (Encounter_Location held 19,891,446 NULL against 0
    zeros), so switching early would empty the HAIBA contact join rather than
    fail loudly. ISNULL is correct both before and after; `= 0` only after.

    Drop the wrapper (and the non-sargable scan it forces) once every
    environment is migrated -- tracked separately from the migration itself.
    """
    return f"ISNULL({alias}.[dw_is_deleted], 0) = 0"


# period_start/period_end are strings and come in two shapes: 19 chars local
# ("2023-08-09T10:08:00") and 25 chars with an offset ("...+00:00", 128,271
# rows). Truncating the second to 19 would read a UTC stamp as local time and
# shift the day at midnight, so the offset form is converted properly.
#
# The NULLIF is load-bearing, not defensive noise. TRY_CONVERT(datetime2, '')
# does not return NULL — it returns 1900-01-01. fhir.Encounter_Location holds
# 3,558,776 rows (17.8%) with a blank location_period_start and a NULL end,
# which without this became an interval from 1900 to forever and matched every
# case that was tested against it. It inflated kun_periode on the location role
# in every year, and looked entirely plausible in the output.
def _ts(column: str) -> str:
    clean = f"NULLIF(LTRIM(RTRIM({column})), '')"
    return (
        f"CASE WHEN LEN({clean}) = 25 THEN CAST(TRY_CONVERT(datetimeoffset, {clean}) "
        f"AT TIME ZONE 'Romance Standard Time' AS datetime2(0)) "
        f"ELSE TRY_CONVERT(datetime2(0), LEFT({clean}, 19)) END"
    )


@dataclass(frozen=True)
class KontaktRolle:
    """One SOR column on a dm case table, and the contact it resolves to.

    ``via`` picks the FHIR resource: ``organisation`` matches the encounter's
    service provider against its own period; ``lokation`` matches the location
    the patient occupied against the narrower location period.
    """

    navn: str
    sor_column: str
    date_column: str
    via: str  # "organisation" | "lokation"

    @property
    def encounter_column(self) -> str:
        return f"encounter_id_{self.navn}"

    @property
    def location_column(self) -> str | None:
        return f"location_id_{self.navn}" if self.via == "lokation" else None

    @property
    def kvalitet_column(self) -> str:
        return f"match_kvalitet_{self.navn}"

    @property
    def dage_column(self) -> str:
        return f"match_dage_{self.navn}"

    @property
    def fact_columns(self) -> tuple[str, ...]:
        """Relation keys — live on the fact so the join out is direct."""
        cols = [self.encounter_column]
        if self.location_column:
            cols.append(self.location_column)
        return tuple(cols)

    @property
    def egenskaber_columns(self) -> tuple[str, ...]:
        """Match quality — lives on the egenskaber transaction dimension."""
        return (self.kvalitet_column, self.dage_column)

    @property
    def columns(self) -> tuple[str, ...]:
        return self.fact_columns + self.egenskaber_columns


@dataclass(frozen=True)
class KontaktTabel:
    """A dm case fact table and the roles stamped onto its egenskaber table.

    ``dm_table`` is the fact; the raw SOR/date columns and the relation
    columns live on ``egenskaber_table``, 1:1 on ``dw_ek_column``.
    """

    dm_table: str
    roller: tuple[KontaktRolle, ...]

    @property
    def dw_ek_column(self) -> str:
        return f"dw_ek_{self.dm_table}"

    @property
    def egenskaber_table(self) -> str:
        # PascalCase, no separating underscore: HAIBA_CasesSSIEgenskaber.
        return f"{self.dm_table}Egenskaber"


KONTAKT_TABELLER = (
    KontaktTabel(
        "HAIBA_CasesInfektion",
        (
            KontaktRolle("kontaktansvar", "HAI_Kontaktansvar", "HAI_date", "organisation"),
            KontaktRolle("opholdsadresse", "HAI_Opholdsadresse", "HAI_date", "lokation"),
        ),
    ),
    KontaktTabel(
        "HAIBA_CasesSSI",
        (
            # The index operation and the reoperation are separate events with
            # their own dates; each has a performing and a responsible unit.
            # All 287 SSI cases carry a reo_date, so no role is ever inapplicable.
            KontaktRolle("producent", "HAI_producent", "operation_date", "organisation"),
            KontaktRolle(
                "kontaktansvarlig", "HAI_kontaktansvarlig", "operation_date", "organisation"
            ),
            KontaktRolle("reo_producent", "reo_producent", "reo_date", "organisation"),
            KontaktRolle(
                "reo_kontaktansvarlig", "reo_kontaktansvarlig", "reo_date", "organisation"
            ),
        ),
    ),
)


def build_match_sql(tabel: KontaktTabel, rolle: KontaktRolle) -> str:
    """Stage the candidates for one role and write the winner onto the table.

    Staged through temp tables rather than joined straight against
    fhir.Encounter for the same reason the dm rebuild is: Encounter holds 18.4M
    rows, and letting the optimiser choose its own path across a type mismatch
    produced a plan that did not finish. Narrowing to the ~21,800 HAIBA patients
    first turns it into a one-million-row join that runs in half a minute.

    The join to ref.PatientToBorger is deliberately not collapsed to one
    patient_id per citizen. It is 1:1 today, but if a citizen ever carried two
    patient records the contacts under both should be searched — the ranking
    below picks a single winner per case either way, so fan-out is safe here in
    a way it is not in the dm rebuild's INSERT.

    The case key is the fact's dw_ek — dw_ek_Borger sits on the fact and the
    raw SOR and date on egenskaber, so both sides of the 1:1 pair are read.
    The write is split the same way the columns are: the relation keys onto
    the fact, the match quality onto egenskaber — two UPDATEs from the same
    #match, so they cannot diverge.
    """
    dw_ek = tabel.dw_ek_column

    if rolle.via == "lokation":
        # The location period is when the patient occupied that ward — narrower
        # and more accurate than the whole admission.
        enc_source = f"""
SELECT e.[patient_id], el.[encounter_id], el.[location_id],
       {_ts("el.[location_period_start]")} AS p_start,
       {_ts("el.[location_period_end]")} AS p_end,
       CAST(l.[identifier_value_sor] AS NVARCHAR(50)) AS sor
INTO #enc
FROM [fhir].[Encounter] AS e
INNER JOIN #pat AS pp ON pp.[patient_id] = e.[patient_id]
INNER JOIN [fhir].[Encounter_Location] AS el
        ON el.[encounter_id] = e.[encounter_id]
       AND {_live("el")}
       -- A row with a blank location_id is not a location assignment; 3,558,776
       -- of them carry a blank period too and would otherwise match everything.
       AND NULLIF(LTRIM(RTRIM(el.[location_id])), '') IS NOT NULL
-- Filtered in the ON clause, not the WHERE: a retracted Location must leave the
-- occupancy row in place with a NULL sor, so the case still reaches kun_periode
-- rather than losing the admission entirely.
LEFT JOIN [fhir].[Location] AS l
       ON l.[location_id] = el.[location_id] AND {_live("l")}
WHERE {_live("e")} AND e.[status] <> 'entered-in-error'
"""
    else:
        enc_source = f"""
SELECT e.[patient_id], e.[encounter_id], CAST(NULL AS NVARCHAR(255)) AS [location_id],
       {_ts("e.[period_start]")} AS p_start,
       {_ts("e.[period_end]")} AS p_end,
       CAST(o.[identifier_value_sor] AS NVARCHAR(50)) AS sor
INTO #enc
FROM [fhir].[Encounter] AS e
INNER JOIN #pat AS pp ON pp.[patient_id] = e.[patient_id]
-- Organization.dw_is_deleted is NOT NULL DEFAULT 0, so a bare `= 0` is both
-- correct here and matchable against the filtered index on identifier_value_sor.
LEFT JOIN [fhir].[Organization] AS o
       ON o.[organization_id] = e.[service_provider_id] AND o.[dw_is_deleted] = 0
WHERE {_live("e")} AND e.[status] <> 'entered-in-error'
"""

    location_set = ""
    if rolle.location_column:
        location_set = f",\n    f.[{rolle.location_column}] = m.[location_id]"

    return f"""
DROP TABLE IF EXISTS #pat;
DROP TABLE IF EXISTS #cs;
DROP TABLE IF EXISTS #enc;
DROP TABLE IF EXISTS #match;

-- Every patient record behind a HAIBA case, so the 18.4M-row Encounter scan
-- is narrowed before anything else touches it.
SELECT DISTINCT CAST(p.[patient_id] AS NVARCHAR(255)) AS [patient_id]
INTO #pat
FROM [dm].[{tabel.dm_table}] AS f
INNER JOIN [ref].[PatientToBorger] AS p
        ON p.[dw_ek_Borger] = f.[dw_ek_Borger] AND p.[dw_is_deleted] = 0
WHERE f.[dw_ek_Borger] <> {UNKNOWN_BORGER};
CREATE UNIQUE CLUSTERED INDEX [IX_pat] ON #pat([patient_id]);

SELECT f.[{dw_ek}] AS [dw_ek], CAST(p.[patient_id] AS NVARCHAR(255)) AS [patient_id],
       CAST(eg.[{rolle.sor_column}] AS NVARCHAR(50)) AS sor,
       TRY_CONVERT(datetime2(0), eg.[{rolle.date_column}]) AS hai_ts
INTO #cs
FROM [dm].[{tabel.dm_table}] AS f
INNER JOIN [dm].[{tabel.egenskaber_table}] AS eg
        ON eg.[{dw_ek}] = f.[{dw_ek}]
INNER JOIN [ref].[PatientToBorger] AS p
        ON p.[dw_ek_Borger] = f.[dw_ek_Borger] AND p.[dw_is_deleted] = 0
WHERE f.[dw_ek_Borger] <> {UNKNOWN_BORGER};
CREATE CLUSTERED INDEX [IX_cs] ON #cs([patient_id]);
{enc_source};
CREATE CLUSTERED INDEX [IX_enc] ON #enc([patient_id]);

WITH kandidat AS (
    SELECT c.[dw_ek], e.[encounter_id], e.[location_id],
        CASE
            WHEN e.sor = c.sor AND c.hai_ts >= e.p_start
                 AND (e.p_end IS NULL OR c.hai_ts <= e.p_end) THEN 1
            WHEN c.hai_ts >= e.p_start
                 AND (e.p_end IS NULL OR c.hai_ts <= e.p_end) THEN 2
            WHEN e.sor = c.sor THEN 3
            ELSE 9
        END AS tier,
        -- Signed whole calendar days to the nearest edge: negative when the
        -- case predates the contact, positive when it follows. 0 means inside
        -- OR outside by less than a day (269 rows) — the quality column is
        -- what separates those two. Lets a consumer choose their own window.
        CASE
            WHEN c.hai_ts < e.p_start THEN DATEDIFF(DAY, e.p_start, c.hai_ts)
            WHEN e.p_end IS NOT NULL AND c.hai_ts > e.p_end
                 THEN DATEDIFF(DAY, e.p_end, c.hai_ts)
            ELSE 0
        END AS dage,
        e.p_start
    FROM #cs AS c
    INNER JOIN #enc AS e ON e.[patient_id] = c.[patient_id]
    WHERE c.hai_ts IS NOT NULL AND e.p_start IS NOT NULL
),
rangeret AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY [dw_ek]
            -- Best tier first, then nearest in time, then the most recent
            -- admission, then the id: fully deterministic across runs.
            ORDER BY tier, ABS(dage), p_start DESC, [encounter_id]
        ) AS rn,
        COUNT(*) OVER (PARTITION BY [dw_ek], tier) AS n_i_tier
    FROM kandidat
    WHERE tier < 9
)
SELECT [dw_ek], [encounter_id], [location_id], tier, dage, n_i_tier
INTO #match
FROM rangeret
WHERE rn = 1;
CREATE CLUSTERED INDEX [IX_match] ON #match([dw_ek]);

-- Every row is rewritten, including the ones that found nothing, so a rerun
-- cannot leave a stale relation behind from an earlier delivery. The relation
-- KEYS land on the fact (direct join out to the contact); the match QUALITY
-- lands on egenskaber. Same #match feeds both, so they cannot diverge.
UPDATE f SET
    f.[{rolle.encounter_column}] = m.[encounter_id]{location_set}
FROM [dm].[{tabel.dm_table}] AS f
LEFT JOIN #match AS m ON m.[dw_ek] = f.[{dw_ek}];

UPDATE eg SET
    eg.[{rolle.dage_column}] = m.dage,
    eg.[{rolle.kvalitet_column}] =
        CASE
            WHEN f.[dw_ek_Borger] = {UNKNOWN_BORGER} THEN 'ingen_borger'
            WHEN m.tier = 1 AND m.n_i_tier = 1 THEN 'eksakt'
            WHEN m.tier = 1 THEN 'eksakt_flere'
            WHEN m.tier = 2 THEN 'kun_periode'
            WHEN m.tier = 3 THEN 'kun_sor'
            WHEN pe.[patient_id] IS NULL THEN 'ingen_kontakter'
            ELSE 'intet_match'
        END
FROM [dm].[{tabel.egenskaber_table}] AS eg
INNER JOIN [dm].[{tabel.dm_table}] AS f
        ON f.[{dw_ek}] = eg.[{dw_ek}]
LEFT JOIN #match AS m ON m.[dw_ek] = eg.[{dw_ek}]
LEFT JOIN (SELECT DISTINCT [patient_id] FROM #enc) AS pe
       ON pe.[patient_id] = (
            SELECT MIN(p2.[patient_id]) FROM [ref].[PatientToBorger] AS p2
            WHERE p2.[dw_ek_Borger] = f.[dw_ek_Borger] AND p2.[dw_is_deleted] = 0
       );

DROP TABLE IF EXISTS #pat;
DROP TABLE IF EXISTS #cs;
DROP TABLE IF EXISTS #enc;
DROP TABLE IF EXISTS #match;
"""


def build_stats_sql(tabel: KontaktTabel, rolle: KontaktRolle) -> str:
    """Match quality broken down by year.

    Deliberately by year rather than as a single rate: the EPJ rollout means one
    overall percentage is meaningless, and a report that shows only the total
    will read the 2020 gap as a data-quality regression.
    """
    return f"""
SELECT YEAR(TRY_CONVERT(datetime2(0), [{rolle.date_column}])) AS aar,
       [{rolle.kvalitet_column}] AS kvalitet, COUNT(*) AS antal
FROM [dm].[{tabel.egenskaber_table}]
GROUP BY YEAR(TRY_CONVERT(datetime2(0), [{rolle.date_column}])),
         [{rolle.kvalitet_column}]
ORDER BY aar, kvalitet;
"""


def _stamp(
    context: dg.AssetExecutionContext, mssql: MSSQLResource, tabel: KontaktTabel
) -> dg.MaterializeResult:
    stats: dict[str, dict] = {}
    with mssql.get_connection() as conn:
        for rolle in tabel.roller:
            cursor = conn.cursor()
            try:
                cursor.execute(build_match_sql(tabel, rolle))
                # A multi-statement batch only surfaces a later statement's
                # error once the caller advances past the earlier result sets.
                # Without this the UPDATE can fail behind a green asset — it
                # already happened once on the dm rebuild.
                if hasattr(cursor, "nextset"):
                    while cursor.nextset():
                        pass
                conn.commit()

                cursor.execute(build_stats_sql(tabel, rolle))
                rows = cursor.fetchall()
            finally:
                cursor.close()

            per_aar: dict[str, dict[str, int]] = {}
            for aar, kvalitet, antal in rows:
                per_aar.setdefault(str(aar), {})[kvalitet] = antal
            stats[rolle.navn] = per_aar

            total = sum(sum(v.values()) for v in per_aar.values())
            eksakt = sum(v.get("eksakt", 0) + v.get("eksakt_flere", 0) for v in per_aar.values())
            context.log.info(
                f"{tabel.dm_table}.{rolle.navn}: {eksakt}/{total} exact "
                f"({eksakt * 100 // total if total else 0}%) — see per-year "
                f"metadata; pre-2022 rows predate the EPJ rollout"
            )

    return dg.MaterializeResult(
        metadata={
            "target_table": dg.MetadataValue.text(f"dm.{tabel.egenskaber_table}"),
            "roller": dg.MetadataValue.json(stats),
            "match_kvalitet": dg.MetadataValue.json(MATCH_KVALITET),
        }
    )


def _build_asset(tabel: KontaktTabel):
    arc_name = "haiba_cases_infektion" if "Infektion" in tabel.dm_table else "haiba_cases_ssi"

    @dg.asset(
        name=f"dm_{arc_name}_kontakt",
        group_name="haiba_dm",
        compute_kind="mssql",
        deps=[f"dm_{arc_name}"],
        tags={"domain": "haiba", "layer": "dm", "source": "haiba"},
        description=(
            f"Stamp the FHIR contact relation onto dm.{tabel.egenskaber_table} "
            f"for {len(tabel.roller)} role(s), each with its own match quality."
        ),
    )
    def _asset(
        context: dg.AssetExecutionContext, mssql_target_staging: MSSQLResource
    ) -> dg.MaterializeResult:
        # Log-only DFM run against the same dm table as the rebuild — a
        # publish therefore leaves two DFMRunLog rows per case table (rebuild,
        # then stamp), tied together by dagster_run_id. The stamp re-derives
        # every row, hence full_load; rows_affected is left unset because the
        # per-role UPDATEs overlap on the same rows.
        dfm = DfmManager(mssql_target_staging, context)
        run_ctx = dfm.start_dfm_run(
            "dm", tabel.dm_table, dagster_run_id=context.run_id, mode="full_load"
        )
        try:
            result = _stamp(context, mssql_target_staging, tabel)
        except Exception as exc:
            dfm.fail_dfm_run(run_ctx.log_id, error_message=str(exc))
            raise
        dfm.complete_dfm_run(run_ctx.log_id)
        return result

    return _asset


dm_haiba_cases_infektion_kontakt = _build_asset(KONTAKT_TABELLER[0])
dm_haiba_cases_ssi_kontakt = _build_asset(KONTAKT_TABELLER[1])

haiba_kontakt_assets = [
    dm_haiba_cases_infektion_kontakt,
    dm_haiba_cases_ssi_kontakt,
]

__all__ = [
    "KONTAKT_TABELLER",
    "KontaktRolle",
    "KontaktTabel",
    "MATCH_KVALITET",
    "build_match_sql",
    "build_stats_sql",
    "haiba_kontakt_assets",
]
