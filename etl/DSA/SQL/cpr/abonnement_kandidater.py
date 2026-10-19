# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Candidates for CPR subscription: personal numbers from fhir.Patient or arc.ifdb_patient that the CPR archive lacks.

Writes only meta.CPRAbonnementKandidat, not to the share. Only counts in metadata and log.
"""

import dagster as dg
from core.resources import MSSQLResource

KANDIDAT_TABEL = "[meta].[CPRAbonnementKandidat]"

# Only ten-digit personal numbers; replacement CPR is not in the register. A candidate none of
# our sources carries any longer is deleted; subscribed and delivered numbers stay.
FLET_SQL = f"""
WITH vores AS (
    SELECT LTRIM(RTRIM(patient_ssn)) AS pnr, 'fhir' AS kilde
    FROM [fhir].[Patient]
    WHERE dw_is_deleted = 0
      AND patient_ssn IS NOT NULL
      AND LEN(LTRIM(RTRIM(patient_ssn))) = 10
      AND LTRIM(RTRIM(patient_ssn)) NOT LIKE '%[^0-9]%'
    UNION
    SELECT LTRIM(RTRIM(CPR)) AS pnr, 'arkiv' AS kilde
    FROM [arc].[ifdb_patient]
    WHERE dw_is_deleted = 0
      AND CPR IS NOT NULL
      AND LEN(LTRIM(RTRIM(CPR))) = 10
      AND LTRIM(RTRIM(CPR)) NOT LIKE '%[^0-9]%'
),
samlet AS (
    SELECT pnr,
           CASE WHEN COUNT(DISTINCT kilde) > 1 THEN 'begge' ELSE MIN(kilde) END AS kilde
    FROM vores
    GROUP BY pnr
),
mangler AS (
    SELECT s.pnr, s.kilde
    FROM samlet s
    LEFT JOIN [arc].[cpr_001_person] c
           ON c.pnr = s.pnr AND c.dw_is_deleted = 0
    WHERE c.pnr IS NULL
)
MERGE {KANDIDAT_TABEL} AS maal
USING mangler AS kilde
   ON maal.pnr = kilde.pnr
WHEN MATCHED THEN
    UPDATE SET maal.kilde = kilde.kilde,
               maal.sidste_gang_set = SYSDATETIME(),
               maal.dw_timestamp_update = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN
    INSERT (pnr, kilde, foerste_gang_set, sidste_gang_set)
    VALUES (kilde.pnr, kilde.kilde, SYSDATETIME(), SYSDATETIME())
WHEN NOT MATCHED BY SOURCE
     AND maal.tilmeldt_dato IS NULL
     AND maal.set_i_udtraek_dato IS NULL
     AND NOT EXISTS (
         SELECT 1 FROM [arc].[cpr_001_person] c
         WHERE c.pnr = maal.pnr AND c.dw_is_deleted = 0
     ) THEN
    DELETE;
"""

# A candidate that has since been delivered stays, with the date it was seen.
LUKKET_SQL = f"""
UPDATE k
   SET k.set_i_udtraek_dato = CAST(SYSDATETIME() AS DATE),
       k.dw_timestamp_update = SYSDATETIME()
FROM {KANDIDAT_TABEL} k
JOIN [arc].[cpr_001_person] c ON c.pnr = k.pnr AND c.dw_is_deleted = 0
WHERE k.set_i_udtraek_dato IS NULL
"""

TAL_SQL = f"""
SELECT
    COUNT(*)                                                           AS kandidater_i_alt,
    SUM(CASE WHEN kilde = 'fhir'  THEN 1 ELSE 0 END)                   AS kun_ressource,
    SUM(CASE WHEN kilde = 'arkiv' THEN 1 ELSE 0 END)                   AS kun_epj_raekke,
    SUM(CASE WHEN kilde = 'begge' THEN 1 ELSE 0 END)                   AS begge_kilder,
    SUM(CASE WHEN tilmeldt_dato IS NOT NULL THEN 1 ELSE 0 END)         AS allerede_tilmeldt,
    SUM(CASE WHEN set_i_udtraek_dato IS NOT NULL THEN 1 ELSE 0 END)    AS siden_set_i_udtraek,
    SUM(CASE WHEN tilmeldt_dato IS NULL AND set_i_udtraek_dato IS NULL
             THEN 1 ELSE 0 END)                                        AS afventer_tilmelding
FROM {KANDIDAT_TABEL}
"""

GRUNDLAG_SQL = """
SELECT
    (SELECT COUNT(*) FROM [arc].[cpr_001_person] WHERE dw_is_deleted = 0) AS personer_i_cpr_arkivet,
    (SELECT COUNT(*) FROM [fhir].[Patient] WHERE dw_is_deleted = 0)       AS patientressourcer
"""


def _en_raekke(mssql: MSSQLResource, sql: str) -> dict:
    with mssql.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            navne = [k[0] for k in cursor.description]
            return dict(zip(navne, cursor.fetchone(), strict=False))
        finally:
            cursor.close()


@dg.asset(
    name="cpr_abonnement_kandidater",
    group_name="cpr",
    compute_kind="mssql",
    deps=["sof_patient_dfm"],
    tags={"layer": "meta", "module": "cpr", "domain": "cpr"},
    description=(
        "meta.CPRAbonnementKandidat — the personal numbers known from our own sources that "
        "the CPR extract does not deliver. Candidate list for subscription; does not write "
        "to the share. Only counts in metadata, never numbers."
    ),
)
def cpr_abonnement_kandidater(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
) -> dg.MaterializeResult:
    """Build the candidate list and count how large it is."""
    grundlag = _en_raekke(mssql_target_staging, GRUNDLAG_SQL)
    if not int(grundlag["personer_i_cpr_arkivet"] or 0):
        # Without a filled archive ALL our persons would look like candidates.
        raise dg.Failure(
            description=(
                "The CPR archive is empty. The candidate list can only be built once the "
                "base extract is loaded — otherwise the whole set of persons is a candidate."
            )
        )

    with mssql_target_staging.get_connection() as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(FLET_SQL)
            flettet = cursor.rowcount
            cursor.execute(LUKKET_SQL)
            lukket = cursor.rowcount
            conn.commit()
        finally:
            cursor.close()

    tal = _en_raekke(mssql_target_staging, TAL_SQL)
    i_alt = int(tal["kandidater_i_alt"] or 0)
    personer = int(grundlag["personer_i_cpr_arkivet"] or 0)
    andel = round(100 * i_alt / max(personer, 1), 2)

    context.log.info(
        "Subscription candidates: %d in total, %d awaiting enrolment "
        "(%.2f %% of the CPR archive's persons)",
        i_alt,
        int(tal["afventer_tilmelding"] or 0),
        andel,
    )
    return dg.MaterializeResult(
        metadata={
            **{k: int(v or 0) for k, v in tal.items()},
            "flettet_denne_koersel": int(flettet or 0),
            "markeret_set_i_udtraek": int(lukket or 0),
            "personer_i_cpr_arkivet": personer,
            "patientressourcer": int(grundlag["patientressourcer"] or 0),
            "andel_af_personkredsen_pct": andel,
            "verdict": (
                f"{i_alt} candidates ({andel} % of the set of persons) — clarify with DOL "
                "before enrolling"
                if andel >= 5
                else f"{i_alt} candidates ({andel} % of the set of persons)"
            ),
        }
    )


all_assets = [cpr_abonnement_kandidater]

__all__ = ["all_assets", "cpr_abonnement_kandidater"]
