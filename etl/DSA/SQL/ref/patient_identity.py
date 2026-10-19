# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""ref.PatientIdentity — which identities are the same citizen, built in one place.

Two identities can belong to the same citizen, and that is not something we compute:
the sources declare it. Record type 022 of the CPR register says that an old and a
current personal number are the same person. The EPJ archive row says that a patient id
belongs to a personal number. Both change only when a delivery arrives.

The union is built once here, and the write path looks it up.

Chains are followed to the end: if A has changed to B, and B to C, all three belong to C.
A historical number with two different current ones is a contradiction in the source: it
is not linked, but counted. If a pair stands in both directions, the newest applies. A
cycle or a chain longer than the depth limit is otherwise a hard failure.
"""

import dagster as dg
from core.dfm_config import RunModeConfig
from core.dfm_manager import DfmManager
from core.resources import MSSQLResource
from utils.fhir.identifier_systems import SYSTEM_CPR, SYSTEM_IFDB_PATIENT_ID

TABEL = "[ref].[PatientIdentity]"

SYSTEM_EPJ = SYSTEM_IFDB_PATIENT_ID

# The chain in 022 is followed to the end with a recursive CTE. The limit stops a cycle in
# the source; KAEDE_KONTROL_SQL fails on it.
MAKS_DYBDE = 10

_KAEDE_CTE = f"""
WITH raa AS (
    SELECT LTRIM(RTRIM([pnr])) AS pnr, LTRIM(RTRIM([pnrgaeld])) AS pnrgaeld,
           MAX([leverance_dato]) AS dato
    FROM [arc].[cpr_022_historisk_personnummer]
    WHERE [dw_is_deleted] = 0
      AND NULLIF(LTRIM(RTRIM([pnr])), '') IS NOT NULL
      AND NULLIF(LTRIM(RTRIM([pnrgaeld])), '') IS NOT NULL
      AND LTRIM(RTRIM([pnr])) <> LTRIM(RTRIM([pnrgaeld]))
    GROUP BY LTRIM(RTRIM([pnr])), LTRIM(RTRIM([pnrgaeld]))
),
-- A number that has been reverted stands in both directions. The archive only replaces
-- the persons a delivery mentions, so the older direction is obsolete.
historik AS (
    SELECT r.pnr, r.pnrgaeld FROM raa r
    WHERE NOT EXISTS (
        SELECT 1 FROM raa t
        WHERE t.pnr = r.pnrgaeld AND t.pnrgaeld = r.pnr AND t.dato > r.dato
    )
),
-- Only numbers with one current successor are followed. A number with two is a
-- contradiction in the source; it is not linked and is counted as ambiguous.
entydig AS (
    SELECT pnr, MIN(pnrgaeld) AS pnrgaeld FROM historik
    GROUP BY pnr HAVING COUNT(DISTINCT pnrgaeld) = 1
),
tvetydig AS (
    SELECT pnr FROM historik GROUP BY pnr HAVING COUNT(DISTINCT pnrgaeld) > 1
),
kaede AS (
    SELECT pnr AS start_pnr, pnrgaeld AS naaet, 1 AS dybde FROM entydig
    UNION ALL
    SELECT k.start_pnr, e.pnrgaeld, k.dybde + 1
    FROM kaede k JOIN entydig e ON e.pnr = k.naaet
    WHERE k.dybde < {MAKS_DYBDE}
)"""

# A number that reaches itself is a cycle; one that still changes onwards at the
# limit is a chain that would be cut off.
KAEDE_KONTROL_SQL = f"""{_KAEDE_CTE}
SELECT COUNT(DISTINCT k.start_pnr)
FROM kaede k
WHERE k.naaet = k.start_pnr
   OR (k.dybde = {MAKS_DYBDE} AND EXISTS (SELECT 1 FROM entydig e WHERE e.pnr = k.naaet))
"""

TVETYDIG_SQL = f"""{_KAEDE_CTE}
SELECT COUNT(*) FROM tvetydig
"""

BYG_SQL = f"""{_KAEDE_CTE},
-- The end of the chain: the number that has not itself changed onwards.
gaeldende AS (
    SELECT start_pnr AS pnr, naaet AS person_key
    FROM (SELECT start_pnr, naaet, ROW_NUMBER() OVER (PARTITION BY start_pnr ORDER BY dybde DESC) AS rn
          FROM kaede) t
    WHERE rn = 1
),
arkiv AS (
    SELECT LTRIM(RTRIM([PATIENT_ID])) AS patient_id,
           NULLIF(LTRIM(RTRIM([CPR])), '') AS cpr,
           CASE WHEN ISNULL([SLETTET], '0') IN ('0', '0.0') THEN 1 ELSE 0 END AS aktiv,
           [OPDATERET_TID] AS opdateret
    FROM [arc].[ifdb_patient]
    WHERE NULLIF(LTRIM(RTRIM([PATIENT_ID])), '') IS NOT NULL
),
-- Key of the person: the current number, else the number itself, else the EPJ id.
epj AS (
    SELECT a.patient_id, a.cpr, a.aktiv, a.opdateret,
           COALESCE(g.person_key, a.cpr, 'epj:' + a.patient_id) AS person_key
    FROM arkiv a LEFT JOIN gaeldende g ON g.pnr = a.cpr
),
-- One primary patient id per person: active before deleted, newest before older.
epj_rang AS (
    SELECT patient_id, person_key,
           ROW_NUMBER() OVER (PARTITION BY person_key
                              ORDER BY aktiv DESC, opdateret DESC, patient_id ASC) AS rn
    FROM epj
),
numre AS (
    -- Current numbers: those that have not themselves changed onwards.
    SELECT DISTINCT cpr AS identity_value, COALESCE(g.person_key, cpr) AS person_key,
           CAST(CASE WHEN g.person_key IS NULL THEN 1 ELSE 0 END AS BIT) AS is_current,
           'epj' AS source
    FROM (SELECT DISTINCT cpr FROM epj WHERE cpr IS NOT NULL) e
    LEFT JOIN gaeldende g ON g.pnr = e.cpr
    UNION
    -- Numbers the register knows, whether or not EPJ has them. An ambiguous number
    -- is not linked to any of its current ones.
    SELECT h.pnr, COALESCE(g.person_key, h.pnrgaeld), CAST(0 AS BIT), 'cpr_022'
    FROM historik h LEFT JOIN gaeldende g ON g.pnr = h.pnr
    WHERE h.pnr NOT IN (SELECT pnr FROM tvetydig)
    UNION
    SELECT h.pnrgaeld, COALESCE(g2.person_key, h.pnrgaeld),
           CAST(CASE WHEN g2.person_key IS NULL THEN 1 ELSE 0 END AS BIT), 'cpr_022'
    FROM historik h LEFT JOIN gaeldende g2 ON g2.pnr = h.pnrgaeld
)
SELECT identity_system, identity_value, person_key, is_current, source
INTO #PatientIdentity
FROM (
    SELECT '{SYSTEM_CPR}' AS identity_system, identity_value, person_key,
           MAX(CAST(is_current AS INT)) AS is_current, MIN(source) AS source
    FROM numre GROUP BY identity_value, person_key
    UNION ALL
    SELECT '{SYSTEM_EPJ}', r.patient_id, r.person_key,
           CASE WHEN r.rn = 1 THEN 1 ELSE 0 END, 'epj'
    FROM epj_rang r
) samlet;
"""

# One identity may only belong to one person. If it happens anyway, the sources
# contradict each other, and the table must not be built on guesswork.
KONTROL_SQL = """
SELECT COUNT(*) FROM (
    SELECT identity_system, identity_value FROM #PatientIdentity
    GROUP BY identity_system, identity_value HAVING COUNT(DISTINCT person_key) > 1
) t
"""

FLET_SQL = f"""
MERGE {TABEL} AS maal
USING #PatientIdentity AS kilde
   ON maal.identity_system = kilde.identity_system
  AND maal.identity_value = kilde.identity_value
WHEN MATCHED AND (maal.person_key <> kilde.person_key
              OR maal.is_current <> kilde.is_current
              OR maal.source <> kilde.source)
    THEN UPDATE SET person_key = kilde.person_key, is_current = kilde.is_current,
                    source = kilde.source,
                    dw_timestamp_update = CAST(SYSDATETIME() AS datetime2(0))
WHEN NOT MATCHED BY TARGET
    THEN INSERT (identity_system, identity_value, person_key, is_current, source)
         VALUES (kilde.identity_system, kilde.identity_value, kilde.person_key,
                 kilde.is_current, kilde.source)
WHEN NOT MATCHED BY SOURCE
    THEN DELETE;
"""


@dg.asset(
    name="ref_patient_identity",
    group_name="ref",
    compute_kind="mssql",
    deps=["arc_patient_ifdb", "arc_cpr"],
    tags={"layer": "ref", "module": "shared_dimensions", "domain": "ref"},
    description=(
        "ref.PatientIdentity — which identities belong to the same citizen. Built from the EPJ "
        "archive rows and 022 of the CPR register, with chains followed to the end. The write "
        "path looks the union up here."
    ),
)
def ref_patient_identity(
    context: dg.AssetExecutionContext,
    mssql_target_staging: MSSQLResource,
    config: RunModeConfig,
) -> dg.MaterializeResult:
    """Build the union from scratch. The table is small, and a full rebuild is the
    only form that cannot leave an obsolete link behind."""
    dfm = DfmManager(mssql_target_staging, context)
    run_ctx = dfm.start_dfm_run(
        "ref",
        "PatientIdentity",
        dagster_run_id=context.run_id,
        mode=config.mode,
    )
    try:
        with mssql_target_staging.get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "IF OBJECT_ID('tempdb..#PatientIdentity') IS NOT NULL DROP TABLE #PatientIdentity"
                )
                cursor.execute(KAEDE_KONTROL_SQL)
                kaedefejl = int(cursor.fetchone()[0])
                if kaedefejl:
                    raise dg.Failure(
                        description=(
                            f"{kaedefejl} personal number(s) in 022 are in a cycle or a chain of "
                            f"over {MAKS_DYBDE} changes that the delivery dates do not settle."
                        ),
                        metadata={"chain_errors": kaedefejl},
                    )
                cursor.execute(TVETYDIG_SQL)
                tvetydige = int(cursor.fetchone()[0])
                if tvetydige:
                    context.log.warning(
                        "%d historical personal numbers have several current numbers in 022 "
                        "and are not linked",
                        tvetydige,
                    )
                cursor.execute(BYG_SQL)
                cursor.execute(
                    "SELECT COUNT(*), SUM(CASE WHEN source = 'epj' THEN 1 ELSE 0 END) "
                    "FROM #PatientIdentity"
                )
                bygget, fra_epj = (int(v or 0) for v in cursor.fetchone())
                if not bygget or not fra_epj:
                    raise dg.Failure(
                        description=(
                            "The union came out empty or without EPJ identities. If "
                            "arc.ifdb_patient is empty, the MERGE would delete them all."
                        )
                    )
                cursor.execute(KONTROL_SQL)
                modstrid = int(cursor.fetchone()[0])
                if modstrid:
                    raise dg.Failure(
                        description=(
                            f"{modstrid} identity/identities point to several persons. The sources "
                            "contradict each other; the union must not be built on a guess."
                        ),
                        metadata={"conflicting_identities": modstrid, "rows_built": bygget},
                    )
                cursor.execute(FLET_SQL)
                flettet = cursor.rowcount
                cursor.execute(
                    f"SELECT COUNT(*), COUNT(DISTINCT person_key), "
                    f"SUM(CASE WHEN is_current = 0 THEN 1 ELSE 0 END) FROM {TABEL}"
                )
                i_alt, personer, tidligere = (int(v or 0) for v in cursor.fetchone())
                conn.commit()
            finally:
                cursor.close()
    except Exception as exc:
        dfm.fail_dfm_run(run_ctx.log_id, error_message=type(exc).__name__)
        raise
    dfm.complete_dfm_run(run_ctx.log_id, rows_affected=max(flettet, 0))
    context.log.info(
        "ref.PatientIdentity: %d identities for %d persons (%d previous numbers or duplicates)",
        i_alt,
        personer,
        tidligere,
    )
    return dg.MaterializeResult(
        metadata={
            "identities": i_alt,
            "persons": personer,
            "non_current_identities": tidligere,
            "rows_merged": max(flettet, 0),
            "identities_per_person": round(i_alt / max(personer, 1), 3),
            "ambiguous_historical_numbers": tvetydige,
        }
    )


__all__ = ["ref_patient_identity"]
