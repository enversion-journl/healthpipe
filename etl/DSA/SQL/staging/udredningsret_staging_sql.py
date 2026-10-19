# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL builders for the udredningsret staging chain (US-3).

Pure functions returning T-SQL for stg.pre_Helbredsforloeb,
stg.pre_Udredningsforloeb, stg.dim_Forloebsmarkoer, stg.fct_UdredningsretMarkoer,
stg.fct_Udredningsforloeb and stg.dim_UdredningsretEgenskaber — kept out of the
asset modules so the SQL is unit-testable without a database (the DRG/HAIBA
pattern, drg_staging_sql.py). Each builder returns ONE batch: DROP TABLE IF
EXISTS + SELECT INTO + indexes, statements separated by ';\\n'. Column names,
types, order and rules follow docs/source_systems/udredningsret/
udredningsret_model.md (the spec); section and decision numbers below refer
to it.

Design, written down once:

* stg.pre_Helbredsforloeb IS the scope table (spec 3.1). The delta scope is
  every helbredsforloeb with at least one arc row — live OR soft-deleted —
  touched after the checkpoint, plus (decision U9, "borger recovery in
  delta") every helbredsforloeb whose live fct.Udredningsforloeb rows still
  carry dw_ek_Borger = -1, resolved back to the arc key through
  map.Helbredsforloeb's clean_code. That second branch is guarded with
  OBJECT_ID checks so US-3 runs before US-4's tables exist. A full load
  (checkpoint None) takes every helbredsforloeb. Every other staging table
  restricts itself to the helbredsforloeb present in stg.pre_Helbredsforloeb.
* A helbredsforloeb whose rows are ALL soft-deleted stays in scope and is
  staged with dw_is_deleted = 1 and zero forloeb — that is how the forloeb
  fact learns that its rows are gone (spec 3.1/6).
* CPR is resolved to dw_ek_Borger while the scoped arc rows are copied into
  a session temp table (#hf_raekker) via #borger_current_cpr/#borger_previous_cpr from
  DSA/SQL/common/dimension_lookup_temp_tables.py (decision U10). The temp table stores the
  RESOLVED key and the lookup outcome, never the CPR, and no stg table ever
  carries CPR. Blank CPR -> -2, unknown -> -1 (created in FHIR by
  fhir_patient_udredningsret, spec 5). One helbredsforloeb has one CPR by
  construction (the CPR is hashed into helbredsforloeb_noegle), so MIN over
  the live rows' resolved keys is a deterministic collapse; for an
  all-deleted helbredsforloeb the key is taken over its deleted rows so the
  retirement row still points at the right borger.
* stg.pre_Udredningsforloeb groups the LIVE markers of a scoped
  helbredsforloeb by F_ID within the delivery (decisions B3/B4/U1). Start,
  end and attribute markers are chosen with ROW_NUMBER windows over ONE
  materialised temp table, using the marker groups of
  udredningsret_kolonner.markoer_gruppe_sql (SDS appendix 1):
    - start   = earliest henvist_udredning marker, else the forloeb's
                earliest marker (U4); ties: lowest code, lowest marker UUID.
    - end     = the MRK_H='1' marker outside henvist_udredning (SDS'
                indicator end), else the latest endeligt_udredt marker, else
                NONE — slutdato NULL, which dato_key turns into the BLANK
                sentinel on the fact (U2). Ties: latest date, lowest code,
                lowest UUID.
    - attribute marker = the end marker, else the latest marker (U3). Both
                priorities are expressed in one ORDER BY, so the row ranked
                first by that order is the attribute marker, and it is the
                end marker exactly when its priority class is 0 or 1.
    Every window ends on arc's loebenummer as the final tie-breaker: two
    arc rows of the same marker (same code, date and marker UUID, ranked 1
    and 2 by the archive on the stable content hash) tie on everything else,
    and the attribute marker's pk/attributes are NOT part of the ORDER BY,
    so without it the row chosen would be engine-dependent and could flip
    kontakt_enhed_sor/speciale/udredningstid and thereby the classification
    key between runs.
  indgaar_i_indikator = the group has a henvist marker AND a non-henvist
  marker both with MRK_H='1'. The counters antal_markoerer/
  antal_udredningsplaner/antal_afbrydelser are formed here from arc (U6), so
  the forloeb fact never reads the marker fact. Indicator values are staged
  as delivered (B5) — no own computation of H, compliance or quarter.
* loebenummer (spec 2) is reused from the persisted map/dimension identity,
  including retired courses. Only new courses receive a number above the
  highest reserved slot for (helbredsforloeb_noegle, startdato). ROW_NUMBER
  orders new courses only; adding/removing peers cannot shift existing keys.
  The first element and start marker identify a course, with singleton
  element fallback for reissued marker UUIDs. Ambiguous matches fail closed.
* Blank source values become NULL (NULLIF(LTRIM(RTRIM(x)), '')) before the
  typed CAST, so a blank never masquerades as a value and dato_key's BLANK
  branch is only reached where the spec wants it (slutdato via ISNULL).
* Robustness: CHAR(64) keys are compared to CHAR(64) columns; NVARCHAR(500)
  source columns are trimmed once when copied into the temp tables; joins
  against #borger_current_cpr/#borger_previous_cpr/#organisation_sor and the scope hit unique clustered
  indexes on temp tables (the HAIBA anti-unseekable-join trick); the grain
  of every staged table is enforced physically by a unique clustered index
  on its clean_code, and the assets add an explicit COUNT/COUNT DISTINCT
  check (build_grain_check_sql).

Execution order expected from the assets (same connection, temp tables are
session-scoped): create_borger_cpr_lookup_tables_sql -> build_pre_helbredsforloeb_sql ->
build_pre_udredningsforloeb_sql; create_organisation_sor_lookup_table_sql before
build_stg_fct_markoer_sql and build_stg_fct_udredningsforloeb_sql.
"""

from DSA.SQL.common.dato_dimension import dato_key
from DSA.SQL.common.dimension_lookup_temp_tables import (
    create_borger_cpr_lookup_tables_sql,
    create_organisation_sor_lookup_table_sql,
)
from DSA.SQL.staging.udredningsret_kolonner import (
    DIM_EGENSKABER_TABLE,
    DIM_FORLOEBSMARKOER_TABLE,
    EGENSKABER_ATTRIBUTTER,
    FCT_FORLOEB_TABLE,
    FCT_MARKOER_TABLE,
    FORLOEB_ATTRIBUTTER,
    FORLOEB_PREFIX,
    HF_PREFIX,
    MARKOER_SOR_ROLLER,
    NA_BORGER,
    PRE_FORLOEB_TABLE,
    PRE_HF_TABLE,
    SOR_ROLLER,
    UNKNOWN_BORGER,
    UNKNOWN_ORGANISATION,
    clean_code_forloebsmarkoer_sql,
    clean_code_helbredsforloeb_sql,
    clean_code_markoer_sql,
    clean_code_udredningsforloeb_sql,
    klassifikation_clean_code_sql,
    markoer_gruppe_sql,
    markoer_gruppe_tekst_sql,
    opgoerelse_dato_sql,
)

__all__ = [
    "build_borger_coverage_sql",
    "create_borger_cpr_lookup_tables_sql",
    "build_grain_check_sql",
    "create_organisation_sor_lookup_table_sql",
    "build_pre_helbredsforloeb_sql",
    "build_pre_udredningsforloeb_sql",
    "build_sor_coverage_sql",
    "build_stg_dim_egenskaber_sql",
    "build_stg_dim_forloebsmarkoer_sql",
    "build_stg_fct_markoer_sql",
    "build_stg_fct_udredningsforloeb_sql",
]

ARC_TABLE = "[arc].[udredningsret_mont]"

# Group names used in the window/aggregate logic — must match the first
# elements of udredningsret_kolonner.MARKOER_GRUPPER.
_HENVIST = "henvist_udredning"
_ENDELIGT_UDREDT = "endeligt_udredt"
_UDREDNINGSPLAN = "udredningsplan"
_AFBRUDT = "afbrudt"


def _cp(checkpoint: str) -> str:
    """Quoted checkpoint literal. The value comes from meta.DFMState
    (system-generated ISO datetime) and is escaped defensively anyway."""
    return "'" + str(checkpoint).replace("'", "''") + "'"


def _trim(col_sql: str) -> str:
    """Trimmed source value, blank -> NULL."""
    return f"NULLIF(LTRIM(RTRIM({col_sql})), '')"


def _trim_lower(col_sql: str) -> str:
    """Trimmed, lower-cased UUID, blank -> NULL (UUIDs are compared case-insensitively)."""
    return f"NULLIF(LOWER(LTRIM(RTRIM({col_sql}))), '')"


# ---------------------------------------------------------------------------
# stg.pre_Helbredsforloeb (spec 3.1 + 3.2)
# ---------------------------------------------------------------------------


def _scope_sql(checkpoint: str | None) -> str:
    """#scope = the helbredsforloeb keys to stage this run (spec 3.1)."""
    if checkpoint is None:
        return f"""
DROP TABLE IF EXISTS #scope;
SELECT DISTINCT a.[helbredsforloeb_noegle]
INTO #scope
FROM {ARC_TABLE} AS a;
CREATE UNIQUE CLUSTERED INDEX [IX_scope_hf] ON #scope([helbredsforloeb_noegle]);
"""
    cp = _cp(checkpoint)
    prefix_len = len(HF_PREFIX)
    return f"""
DROP TABLE IF EXISTS #scope_kandidater;
CREATE TABLE #scope_kandidater ([helbredsforloeb_noegle] CHAR(64) NOT NULL);

-- (1) helbredsforloeb with at least one arc row (live or soft-deleted) touched
--     after the checkpoint
INSERT INTO #scope_kandidater ([helbredsforloeb_noegle])
SELECT DISTINCT a.[helbredsforloeb_noegle]
FROM {ARC_TABLE} AS a
WHERE a.[dw_timestamp_update] > {cp};

-- (2) borger recovery (decision U9): helbredsforloeb whose live fact rows still
--     point at the unknown borger are re-staged until the nightly chain has
--     minted the borger. Guarded so US-3 runs before US-4's tables exist.
IF OBJECT_ID(N'[fct].[Udredningsforloeb]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[map].[Helbredsforloeb]', 'U') IS NOT NULL
BEGIN
    INSERT INTO #scope_kandidater ([helbredsforloeb_noegle])
    SELECT DISTINCT CAST(SUBSTRING(m.[clean_code_Helbredsforloeb], {prefix_len + 1}, 64) AS CHAR(64))
    FROM [fct].[Udredningsforloeb] AS f
    JOIN [map].[Helbredsforloeb] AS m
      ON m.[dw_ek_Helbredsforloeb] = f.[dw_ek_Helbredsforloeb]
    WHERE f.[dw_is_deleted] = 0
      AND f.[dw_ek_Borger] = {UNKNOWN_BORGER}
      AND LEFT(m.[clean_code_Helbredsforloeb], {prefix_len}) = '{HF_PREFIX}';
END;

DROP TABLE IF EXISTS #scope;
SELECT DISTINCT [helbredsforloeb_noegle]
INTO #scope
FROM #scope_kandidater;
CREATE UNIQUE CLUSTERED INDEX [IX_scope_hf] ON #scope([helbredsforloeb_noegle]);
DROP TABLE IF EXISTS #scope_kandidater;
"""


def build_pre_helbredsforloeb_sql(checkpoint: str | None) -> str:
    """#scope + DROP + SELECT INTO stg.pre_Helbredsforloeb (one row per key in scope).

    Requires create_borger_cpr_lookup_tables_sql() to have run on the connection. The
    scoped arc rows are first copied into #hf_raekker with the borger already
    resolved (no CPR stored), then aggregated per helbredsforloeb_noegle:
    first LIVE marker via ROW_NUMBER ordered exactly like the archive's key
    computation (FORLOEBSMARKOER_DATO, FORLOEBSMARKOER, ANSVARLIG_ENHED_FORLOB,
    LOWER(OBJEKTID_FORLOBELEMENT)), counters over live rows, dw_is_deleted = 1
    when no live row remains, dw_timestamp_update = MAX over ALL rows.
    """
    cpr = "a.[CPR]"
    live = "n.[dw_is_deleted] = 0"
    return f"""{_scope_sql(checkpoint)}
-- Scoped arc rows with the borger resolved. No length cast on the delivery
-- side of the CPR join: CAST(... AS NVARCHAR(10)) would silently TRUNCATE a
-- malformed 11+-character value and could mis-match another person's CPR;
-- an overlong value simply finds no match (-1). CPR itself is not copied.
DROP TABLE IF EXISTS #hf_raekker;
SELECT
    a.[helbredsforloeb_noegle],
    a.[dw_is_deleted],
    a.[dw_timestamp_update],
    CASE
        WHEN {cpr} IS NULL OR LTRIM(RTRIM({cpr})) = '' THEN {NA_BORGER}
        ELSE COALESCE(b.[dw_ek_Borger], bt.[dw_ek_Borger], {UNKNOWN_BORGER})
    END AS [dw_ek_Borger],
    CASE
        WHEN {cpr} IS NULL OR LTRIM(RTRIM({cpr})) = '' THEN 'tom'
        WHEN b.[dw_ek_Borger] IS NOT NULL THEN 'cpr'
        WHEN bt.[dw_ek_Borger] IS NOT NULL THEN 'tidligere_cpr'
        ELSE 'ukendt'
    END AS [borger_opslag],
    {_trim("a.[DW_EK_HelbredsForloeb]")} AS [helbredsforloeb_id],
    {_trim("a.[F_ID]")} AS [f_id],
    {_trim("a.[FORLOEBSMARKOER]")} AS [kode],
    {_trim("a.[FORLOEBSMARKOER_DATO]")} AS [dato],
    {_trim("a.[ANSVARLIG_ENHED_FORLOB]")} AS [enhed_sor],
    {_trim_lower("a.[OBJEKTID_FORLOBELEMENT]")} AS [forloebselement_uuid]
INTO #hf_raekker
FROM {ARC_TABLE} AS a
JOIN #scope AS s ON s.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]
LEFT JOIN #borger_current_cpr AS b ON b.[cpr] = LTRIM(RTRIM({cpr}))
LEFT JOIN #borger_previous_cpr AS bt ON bt.[cpr] = LTRIM(RTRIM({cpr}));
CREATE CLUSTERED INDEX [IX_hf_raekker]
    ON #hf_raekker([helbredsforloeb_noegle], [dw_is_deleted]);

DROP TABLE IF EXISTS {PRE_HF_TABLE};

WITH nummereret AS (
    SELECT r.*,
           CASE WHEN r.[dw_is_deleted] = 0 THEN ROW_NUMBER() OVER (
               PARTITION BY r.[helbredsforloeb_noegle], r.[dw_is_deleted]
               ORDER BY r.[dato], r.[kode], r.[enhed_sor], r.[forloebselement_uuid]
           ) END AS [rn_foerste]
    FROM #hf_raekker AS r
)
SELECT
    {clean_code_helbredsforloeb_sql("n.[helbredsforloeb_noegle]")} AS [clean_code_Helbredsforloeb],
    n.[helbredsforloeb_noegle],
    CAST(MAX(CASE WHEN {live} THEN n.[helbredsforloeb_id] END) AS NVARCHAR(50)) AS [helbredsforloeb_id],
    COALESCE(MIN(CASE WHEN {live} THEN n.[dw_ek_Borger] END), MIN(n.[dw_ek_Borger])) AS [dw_ek_Borger],
    CAST(COALESCE(MIN(CASE WHEN {live} THEN n.[borger_opslag] END), MIN(n.[borger_opslag]))
         AS NVARCHAR(20)) AS [borger_opslag],
    CAST(MAX(CASE WHEN n.[rn_foerste] = 1 THEN n.[kode] END) AS NVARCHAR(20)) AS [foerste_markoer_kode],
    CAST(MAX(CASE WHEN n.[rn_foerste] = 1 THEN n.[dato] END) AS NVARCHAR(10)) AS [foerste_markoer_dato],
    CAST(MAX(CASE WHEN n.[rn_foerste] = 1 THEN n.[enhed_sor] END) AS NVARCHAR(25)) AS [foerste_enhed_sor],
    CAST(MAX(CASE WHEN n.[rn_foerste] = 1 THEN n.[forloebselement_uuid] END) AS NVARCHAR(36))
        AS [foerste_forloebselement_uuid],
    SUM(CASE WHEN {live} THEN 1 ELSE 0 END) AS [antal_markoerer],
    COUNT(DISTINCT CASE WHEN {live} THEN n.[forloebselement_uuid] END) AS [antal_forloebselementer],
    COUNT(DISTINCT CASE WHEN {live} THEN n.[f_id] END) AS [antal_udredningsforloeb],
    CAST(CASE WHEN SUM(CASE WHEN {live} THEN 1 ELSE 0 END) = 0 THEN 1 ELSE 0 END AS BIT) AS [dw_is_deleted],
    MAX(n.[dw_timestamp_update]) AS [dw_timestamp_update]
INTO {PRE_HF_TABLE}
FROM nummereret AS n
GROUP BY n.[helbredsforloeb_noegle];

CREATE UNIQUE CLUSTERED INDEX [IX_pre_Helbredsforloeb_PK]
    ON {PRE_HF_TABLE}([clean_code_Helbredsforloeb]);
CREATE NONCLUSTERED INDEX [IX_pre_Helbredsforloeb_noegle]
    ON {PRE_HF_TABLE}([helbredsforloeb_noegle]);
DROP TABLE IF EXISTS #hf_raekker;
"""


# ---------------------------------------------------------------------------
# stg.pre_Udredningsforloeb (spec 3.3)
# ---------------------------------------------------------------------------

# Priority class of a marker as END marker (lower wins): 0 = SDS' indicator
# end (MRK_H='1' outside henvist), 1 = endeligt udredt, 2 = not an end marker.
_SLUT_PRIORITET = (
    f"CASE WHEN m.[mrk_h_1] = 1 AND m.[gruppe] <> '{_HENVIST}' THEN 0 "
    f"WHEN m.[gruppe] = '{_ENDELIGT_UDREDT}' THEN 1 ELSE 2 END"
)


def _forloeb_identity_sql() -> str:
    """Reuse published ordinals; reserve every map slot, including retired rows.

    The dimension is the persisted identity evidence. Match the stable first
    element and start marker; a reissued marker UUID is accepted only when the
    first element identifies a single old AND current course. Ambiguity fails
    before dimensions/facts can be overwritten. No F_ID or end-date matching.
    """
    return f"""
DROP TABLE IF EXISTS #forloeb_historik;
CREATE TABLE #forloeb_historik (
    [prefix] VARCHAR(100) NOT NULL,
    [loebenummer] INT NOT NULL,
    [foerste_forloebselement_uuid] NVARCHAR(36) NULL,
    [start_markoer_uuid] NVARCHAR(36) NULL,
    [has_dimension] BIT NOT NULL
);
IF OBJECT_ID(N'[map].[Udredningsforloeb]', 'U') IS NOT NULL
   AND OBJECT_ID(N'[dim].[UdredningsretEgenskaber]', 'U') IS NOT NULL
BEGIN
    INSERT INTO #forloeb_historik
    SELECT
        LEFT(m.[clean_code_Udredningsforloeb],
             LEN(m.[clean_code_Udredningsforloeb]) - p.[suffix_length]),
        TRY_CONVERT(INT, RIGHT(m.[clean_code_Udredningsforloeb], p.[suffix_length])),
        d.[foerste_forloebselement_uuid], d.[start_markoer_uuid],
        CASE WHEN d.[dw_ek_Udredningsforloeb] IS NULL THEN 0 ELSE 1 END
    FROM [map].[Udredningsforloeb] AS m
    LEFT JOIN [dim].[UdredningsretEgenskaber] AS d
      ON d.[dw_ek_Udredningsforloeb] = m.[dw_ek_Udredningsforloeb]
    CROSS APPLY (SELECT NULLIF(CHARINDEX('_', REVERSE(m.[clean_code_Udredningsforloeb])), 0) - 1
                 AS [suffix_length]) AS p
    WHERE m.[clean_code_Udredningsforloeb] LIKE '{FORLOEB_PREFIX}%'
      AND EXISTS (SELECT 1 FROM {PRE_HF_TABLE} AS h
                  WHERE h.[helbredsforloeb_noegle] =
                        SUBSTRING(m.[clean_code_Udredningsforloeb], {len(FORLOEB_PREFIX) + 1}, 64));
END;
CREATE CLUSTERED INDEX [IX_forloeb_historik]
    ON #forloeb_historik([prefix], [foerste_forloebselement_uuid]);

DROP TABLE IF EXISTS #forloeb_identitet;
WITH nu AS (
    SELECT f.*,
           CAST('{FORLOEB_PREFIX}' + f.[helbredsforloeb_noegle] + '_'
                + REPLACE(ISNULL(f.[startdato], ''), '-', '') + '_' AS VARCHAR(100)) AS [prefix],
           COUNT(*) OVER (PARTITION BY f.[helbredsforloeb_noegle], f.[startdato],
                                      f.[foerste_forloebselement_uuid]) AS [antal_nu]
    FROM #forloeb AS f
), historik AS (
    SELECT h.*,
           COUNT(*) OVER (PARTITION BY h.[prefix], h.[foerste_forloebselement_uuid]) AS [antal_foer]
    FROM #forloeb_historik AS h
    -- Map-only reservations have no identity evidence yet. They still count
    -- towards MAX(loebenummer), but must not match a new course.
    WHERE h.[has_dimension] = 1
)
SELECT f.*, h.[loebenummer] AS [gemt_loebenummer]
INTO #forloeb_identitet
FROM nu AS f
LEFT JOIN historik AS h
  ON h.[prefix] = f.[prefix]
 AND (h.[foerste_forloebselement_uuid] = f.[foerste_forloebselement_uuid]
      OR (h.[foerste_forloebselement_uuid] IS NULL AND f.[foerste_forloebselement_uuid] IS NULL))
 AND (h.[start_markoer_uuid] = f.[start_markoer_uuid]
      OR (h.[start_markoer_uuid] IS NULL AND f.[start_markoer_uuid] IS NULL)
      OR (h.[antal_foer] = 1 AND f.[antal_nu] = 1));

-- Do not resolve an ambiguous identity by mutable end date or delivery F_ID.
IF EXISTS (SELECT 1 FROM #forloeb_identitet
           GROUP BY [helbredsforloeb_noegle], [f_id] HAVING COUNT(*) > 1)
   OR EXISTS (SELECT 1 FROM #forloeb_identitet WHERE [gemt_loebenummer] IS NOT NULL
              GROUP BY [prefix], [gemt_loebenummer] HAVING COUNT(*) > 1)
    THROW 51000, 'Ambiguous udredningsforloeb identity; existing keys were not reassigned.', 1;

WITH maksimum AS (
    SELECT [prefix], MAX([loebenummer]) AS [sidste_loebenummer]
    FROM #forloeb_historik GROUP BY [prefix]
), nummereret AS (
    SELECT f.*,
           COALESCE(f.[gemt_loebenummer], ISNULL(h.[sidste_loebenummer], 0) +
               CAST(ROW_NUMBER() OVER (
                   PARTITION BY f.[prefix], CASE WHEN f.[gemt_loebenummer] IS NULL THEN 0 ELSE 1 END
                   ORDER BY f.[foerste_forloebselement_uuid], f.[slutdato], f.[f_id]
               ) AS INT)) AS [loebenummer]
    FROM #forloeb_identitet AS f
    LEFT JOIN maksimum AS h ON h.[prefix] = f.[prefix]
)
"""


def build_pre_udredningsforloeb_sql() -> str:
    """DROP + SELECT INTO stg.pre_Udredningsforloeb, one row per (helbredsforloeb_noegle, F_ID).

    Reads only LIVE arc rows with a non-blank F_ID whose helbredsforloeb is in
    stg.pre_Helbredsforloeb (dw_is_deleted is therefore always 0 here —
    vanished forloeb are handled through the scope, spec 6). See the module
    docstring for the start/end/attribute rules and the loebenummer.
    """
    attr_temp = ",\n    ".join(
        f"{_trim(f'a.[{src}]')} AS [{name}]" for name, src, _ in FORLOEB_ATTRIBUTTER
    )
    attr_agg = ",\n        ".join(
        f"MAX(CASE WHEN r.[rn_attribut] = 1 THEN r.[{name}] END) AS [{name}]"
        for name, _, _ in FORLOEB_ATTRIBUTTER
    )
    attr_final = ",\n    ".join(
        f"CAST(n.[{name}] AS {sql_type}) AS [{name}]" for name, _, sql_type in FORLOEB_ATTRIBUTTER
    )
    start = "r.[rn_start] = 1"
    slut = "r.[rn_attribut] = 1 AND r.[slut_prioritet] < 2"
    attribut = "r.[rn_attribut] = 1"
    return f"""
-- Live markers of the scoped helbredsforloeb, grouped later on F_ID. Values
-- are trimmed once here; blank -> NULL. The marker group is computed once per
-- row so the windows and the counters agree.
DROP TABLE IF EXISTS #forloeb_markoerer;
SELECT
    a.[helbredsforloeb_noegle],
    -- Keep the full archive width: F_ID participates in grouping and joins.
    -- The compound lookup index below is nonclustered (1,700-byte limit).
    CAST(LTRIM(RTRIM(a.[F_ID])) AS NVARCHAR(500)) AS [f_id],
    a.[loebenummer],
    {_trim("a.[FORLOEBSMARKOER]")} AS [kode],
    {_trim("a.[FORLOEBSMARKOER_DATO]")} AS [dato],
    {_trim_lower("a.[OBJEKTID_FORLOBSMARKOR]")} AS [markoer_uuid],
    {_trim_lower("a.[OBJEKTID_FORLOBELEMENT]")} AS [forloebselement_uuid],
    {_trim("a.[DW_EK_HelbredsForloeb]")} AS [helbredsforloeb_id],
    {_trim("a.[PK]")} AS [pk],
    CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END AS [mrk_h_1],
    {markoer_gruppe_sql("LTRIM(RTRIM(a.[FORLOEBSMARKOER]))")} AS [gruppe],
    {attr_temp},
    a.[dw_timestamp_update]
INTO #forloeb_markoerer
FROM {ARC_TABLE} AS a
WHERE a.[dw_is_deleted] = 0
  AND a.[F_ID] IS NOT NULL AND LTRIM(RTRIM(a.[F_ID])) <> ''
  AND EXISTS (SELECT 1 FROM {PRE_HF_TABLE} AS h
              WHERE h.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]);
CREATE CLUSTERED INDEX [IX_forloeb_markoerer_hf]
    ON #forloeb_markoerer([helbredsforloeb_noegle]);
CREATE NONCLUSTERED INDEX [IX_forloeb_markoerer]
    ON #forloeb_markoerer([helbredsforloeb_noegle], [f_id]);

DROP TABLE IF EXISTS {PRE_FORLOEB_TABLE};
DROP TABLE IF EXISTS #forloeb;

WITH rangeret AS (
    SELECT m.*,
           CASE WHEN m.[gruppe] = '{_HENVIST}' THEN 1 ELSE 0 END AS [er_henvist],
           {_SLUT_PRIORITET} AS [slut_prioritet],
           -- start marker: earliest henvist, else earliest of all (U4)
           ROW_NUMBER() OVER (
               PARTITION BY m.[helbredsforloeb_noegle], m.[f_id]
               ORDER BY CASE WHEN m.[gruppe] = '{_HENVIST}' THEN 0 ELSE 1 END,
                        m.[dato], m.[kode], m.[markoer_uuid], m.[loebenummer]
           ) AS [rn_start],
           -- attribute marker: the end marker (priority 0, then 1), else the
           -- latest marker (U2/U3)
           ROW_NUMBER() OVER (
               PARTITION BY m.[helbredsforloeb_noegle], m.[f_id]
               ORDER BY {_SLUT_PRIORITET},
                        m.[dato] DESC, m.[kode], m.[markoer_uuid], m.[loebenummer]
           ) AS [rn_attribut]
    FROM #forloeb_markoerer AS m
),
forloeb AS (
    SELECT
        r.[helbredsforloeb_noegle],
        r.[f_id],
        MAX(CASE WHEN {start} THEN r.[dato] END) AS [startdato],
        MAX(CASE WHEN {start} THEN r.[kode] END) AS [start_markoer_kode],
        MAX(CASE WHEN {start} THEN r.[markoer_uuid] END) AS [start_markoer_uuid],
        MAX(CASE WHEN {slut} THEN r.[dato] END) AS [slutdato],
        MAX(CASE WHEN {slut} THEN r.[kode] END) AS [slut_markoer_kode],
        MAX(CASE WHEN {slut} THEN r.[markoer_uuid] END) AS [slut_markoer_uuid],
        MAX(r.[dato]) AS [seneste_markoer_dato],
        MIN(r.[forloebselement_uuid]) AS [foerste_forloebselement_uuid],
        MAX(r.[helbredsforloeb_id]) AS [helbredsforloeb_id],
        MAX(CASE WHEN {attribut} THEN r.[pk] END) AS [pk],
        {attr_agg},
        CASE WHEN SUM(CASE WHEN r.[mrk_h_1] = 1 AND r.[er_henvist] = 1 THEN 1 ELSE 0 END) > 0
              AND SUM(CASE WHEN r.[mrk_h_1] = 1 AND r.[er_henvist] = 0 THEN 1 ELSE 0 END) > 0
             THEN 1 ELSE 0 END AS [indgaar_i_indikator],
        COUNT(*) AS [antal_markoerer],
        SUM(CASE WHEN r.[gruppe] = '{_UDREDNINGSPLAN}' THEN 1 ELSE 0 END) AS [antal_udredningsplaner],
        SUM(CASE WHEN r.[gruppe] = '{_AFBRUDT}' THEN 1 ELSE 0 END) AS [antal_afbrydelser],
        MAX(r.[dw_timestamp_update]) AS [dw_timestamp_update]
    FROM rangeret AS r
    GROUP BY r.[helbredsforloeb_noegle], r.[f_id]
)
SELECT * INTO #forloeb FROM forloeb;

{_forloeb_identity_sql()}
SELECT
    {clean_code_udredningsforloeb_sql("n.[helbredsforloeb_noegle]", "n.[startdato]", "n.[loebenummer]")}
        AS [clean_code_Udredningsforloeb],
    {clean_code_helbredsforloeb_sql("n.[helbredsforloeb_noegle]")} AS [clean_code_Helbredsforloeb],
    n.[helbredsforloeb_noegle],
    CAST(n.[f_id] AS NVARCHAR(500)) AS [f_id],
    CAST(n.[pk] AS NVARCHAR(20)) AS [pk],
    n.[loebenummer],
    CAST(n.[startdato] AS NVARCHAR(10)) AS [startdato],
    CAST(n.[start_markoer_kode] AS NVARCHAR(20)) AS [start_markoer_kode],
    CAST(n.[start_markoer_uuid] AS NVARCHAR(36)) AS [start_markoer_uuid],
    CAST(n.[slutdato] AS NVARCHAR(10)) AS [slutdato],
    CAST(n.[slut_markoer_kode] AS NVARCHAR(20)) AS [slut_markoer_kode],
    CAST(n.[slut_markoer_uuid] AS NVARCHAR(36)) AS [slut_markoer_uuid],
    CAST(n.[seneste_markoer_dato] AS NVARCHAR(10)) AS [seneste_markoer_dato],
    CAST(n.[foerste_forloebselement_uuid] AS NVARCHAR(36)) AS [foerste_forloebselement_uuid],
    CAST(n.[helbredsforloeb_id] AS NVARCHAR(50)) AS [helbredsforloeb_id],
    {attr_final},
    CAST(n.[indgaar_i_indikator] AS BIT) AS [indgaar_i_indikator],
    n.[antal_markoerer],
    n.[antal_udredningsplaner],
    n.[antal_afbrydelser],
    CAST(0 AS BIT) AS [dw_is_deleted],
    n.[dw_timestamp_update]
INTO {PRE_FORLOEB_TABLE}
FROM nummereret AS n;

CREATE UNIQUE CLUSTERED INDEX [IX_pre_Udredningsforloeb_PK]
    ON {PRE_FORLOEB_TABLE}([clean_code_Udredningsforloeb]);
CREATE NONCLUSTERED INDEX [IX_pre_Udredningsforloeb_hf_fid]
    ON {PRE_FORLOEB_TABLE}([helbredsforloeb_noegle], [f_id]);
DROP TABLE IF EXISTS #forloeb_markoerer;
DROP TABLE IF EXISTS #forloeb;
DROP TABLE IF EXISTS #forloeb_identitet;
DROP TABLE IF EXISTS #forloeb_historik;
"""


# ---------------------------------------------------------------------------
# stg.dim_Forloebsmarkoer (spec 3.4, decision U7)
# ---------------------------------------------------------------------------


def build_stg_dim_forloebsmarkoer_sql() -> str:
    """DROP + SELECT INTO stg.dim_Forloebsmarkoer: one row per distinct live FORLOEBSMARKOER.

    Not delta-scoped (433 codes). The grouping expression is the SAME
    normalisation the clean_code uses (UPPER(LTRIM(RTRIM()))), so two case
    variants of one code can never mint two rows with one clean_code and trip
    the unique index — the key's normalisation decides, not the collation's.
    Text from codesystem.SKSConcept.display (LEFT JOIN on code); an unknown
    code is still minted with tekst 'Ukendt' and tekst_kilde 'ukendt' —
    never a hard fail. Blank codes are excluded:
    the marker fact stages a NULL clean_code_Forloebsmarkoer for them, which
    resolves to -1 downstream. dw_timestamp_update = MAX over the code's arc
    rows so the dim merge can change-detect like every other stg table.
    """
    return f"""
DROP TABLE IF EXISTS {DIM_FORLOEBSMARKOER_TABLE};

WITH koder AS (
    SELECT UPPER(LTRIM(RTRIM(a.[FORLOEBSMARKOER]))) AS [kode],
           MAX(a.[dw_timestamp_update]) AS [dw_timestamp_update]
    FROM {ARC_TABLE} AS a
    WHERE a.[dw_is_deleted] = 0
      AND a.[FORLOEBSMARKOER] IS NOT NULL
      AND LTRIM(RTRIM(a.[FORLOEBSMARKOER])) <> ''
    GROUP BY UPPER(LTRIM(RTRIM(a.[FORLOEBSMARKOER])))
)
SELECT
    {clean_code_forloebsmarkoer_sql("k.[kode]")} AS [clean_code_Forloebsmarkoer],
    CAST(k.[kode] AS NVARCHAR(20)) AS [kode],
    CAST({markoer_gruppe_sql("k.[kode]")} AS NVARCHAR(40)) AS [gruppe],
    CAST({markoer_gruppe_tekst_sql("k.[kode]")} AS NVARCHAR(100)) AS [gruppe_tekst],
    CAST(ISNULL(s.[display], N'Ukendt') AS NVARCHAR(500)) AS [tekst],
    CAST(CASE WHEN s.[code] IS NULL THEN 'ukendt' ELSE 'SKS' END AS NVARCHAR(30)) AS [tekst_kilde],
    CAST(0 AS BIT) AS [dw_is_deleted],
    k.[dw_timestamp_update]
INTO {DIM_FORLOEBSMARKOER_TABLE}
FROM koder AS k
LEFT JOIN [codesystem].[SKSConcept] AS s ON s.[code] = k.[kode];

CREATE UNIQUE CLUSTERED INDEX [IX_dim_Forloebsmarkoer_PK]
    ON {DIM_FORLOEBSMARKOER_TABLE}([clean_code_Forloebsmarkoer]);
"""


# ---------------------------------------------------------------------------
# stg.fct_UdredningsretMarkoer (spec 3.5)
# ---------------------------------------------------------------------------


def build_stg_fct_markoer_sql() -> str:
    """DROP + SELECT INTO stg.fct_UdredningsretMarkoer: one row per arc row (live AND soft-deleted) in scope.

    Requires create_organisation_sor_lookup_table_sql() on the connection. clean_code_Udredningsforloeb
    comes from stg.pre_Udredningsforloeb on (helbredsforloeb_noegle, F_ID) and
    is NULL when F_ID is blank or the group does not exist (a soft-deleted
    marker with a stale F_ID) — the fact loader maps NULL to -1 (decision U5).
    Borger key and lookup outcome come from stg.pre_Helbredsforloeb (same
    borger for the whole helbredsforloeb). The forloeb unit is resolved via
    #organisation_sor; blank and unmatched -> -1 with the raw code kept beside the key.
    """
    (raw_src, key), *_ = MARKOER_SOR_ROLLER  # ANSVARLIG_ENHED_FORLOB -> dw_ek_Organisation_forloeb
    sor = "LTRIM(RTRIM(a.[ANSVARLIG_ENHED_FORLOB]))"
    kode = "a.[FORLOEBSMARKOER]"
    return f"""
DROP TABLE IF EXISTS {FCT_MARKOER_TABLE};

SELECT
    {clean_code_markoer_sql("a.[markoer_noegle]", "a.[loebenummer]")} AS [clean_code_UdredningsretMarkoer],
    u.[clean_code_Udredningsforloeb],
    h.[clean_code_Helbredsforloeb],
    CASE WHEN {_trim(kode)} IS NULL THEN NULL
         ELSE {clean_code_forloebsmarkoer_sql(kode)} END AS [clean_code_Forloebsmarkoer],
    h.[dw_ek_Borger],
    h.[borger_opslag],
    CAST({_trim("a.[ANSVARLIG_ENHED_FORLOB]")} AS NVARCHAR(25)) AS [{raw_src}],
    COALESCE(o0.[dw_ek_Organisation], {UNKNOWN_ORGANISATION}) AS [{key}],
    CAST({_trim(kode)} AS NVARCHAR(20)) AS [markoer_kode],
    CAST({_trim("a.[FORLOEBSMARKOER_DATO]")} AS NVARCHAR(10)) AS [markoer_dato],
    {dato_key("a.[FORLOEBSMARKOER_DATO]")} AS [dw_ek_Dato_markoer],
    CAST({_trim("a.[DW_EK_HelbredsForloeb]")} AS NVARCHAR(50)) AS [helbredsforloeb_id],
    CAST({_trim_lower("a.[OBJEKTID_FORLOBELEMENT]")} AS NVARCHAR(36)) AS [forloebselement_uuid],
    CAST({_trim_lower("a.[OBJEKTID_FORLOBSMARKOR]")} AS NVARCHAR(36)) AS [markoer_uuid],
    CAST({_trim("a.[F_ID]")} AS NVARCHAR(500)) AS [f_id],
    CAST({_trim("a.[PK]")} AS NVARCHAR(20)) AS [pk],
    CAST(CASE WHEN LTRIM(RTRIM(a.[MRK_H])) = '1' THEN 1 ELSE 0 END AS BIT) AS [indgaar_i_indikator_H],
    CAST(CASE WHEN LTRIM(RTRIM(a.[MRK_L_N])) = '1' THEN 1 ELSE 0 END AS BIT) AS [indgaar_i_indikator_L_N],
    CAST({_trim("a.[C_SGHREGION]")} AS NVARCHAR(10)) AS [sygehusregion],
    CAST({_trim("a.[C_BOREGION]")} AS NVARCHAR(10)) AS [bopaelsregion],
    a.[antal_kilderaekker],
    TRY_CONVERT(DATETIME2, a.[FORLOEBSMARKOER_DATO]) AS [dw_haendelsestidspunkt],
    a.[dw_is_deleted],
    a.[dw_timestamp_update]
INTO {FCT_MARKOER_TABLE}
FROM {ARC_TABLE} AS a
JOIN {PRE_HF_TABLE} AS h
  ON h.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]
LEFT JOIN {PRE_FORLOEB_TABLE} AS u
  ON u.[helbredsforloeb_noegle] = a.[helbredsforloeb_noegle]
 AND u.[f_id] = LTRIM(RTRIM(a.[F_ID]))
LEFT JOIN #organisation_sor AS o0
  ON o0.[kode] = {sor} AND {sor} <> '';

CREATE UNIQUE CLUSTERED INDEX [IX_fct_UdredningsretMarkoer_PK]
    ON {FCT_MARKOER_TABLE}([clean_code_UdredningsretMarkoer]);
"""


# ---------------------------------------------------------------------------
# stg.fct_Udredningsforloeb + stg.dim_UdredningsretEgenskaber (spec 3.6)
# ---------------------------------------------------------------------------


def _sor_joins(alias: str, roles: tuple[tuple[str, str], ...]) -> str:
    return "\n".join(
        f"LEFT JOIN #organisation_sor AS o{i} ON o{i}.[kode] = {alias}.[{raw}] AND {alias}.[{raw}] IS NOT NULL"
        for i, (raw, _) in enumerate(roles)
    )


def _bit_from_flag(col_sql: str) -> str:
    """'1' -> 1, '0' -> 0, anything else (NULL/blank) -> NULL, typed BIT."""
    return f"CAST(CASE {col_sql} WHEN '1' THEN 1 WHEN '0' THEN 0 END AS BIT)"


def build_stg_fct_udredningsforloeb_sql() -> str:
    """DROP + SELECT INTO stg.fct_Udredningsforloeb from stg.pre_Udredningsforloeb.

    Requires create_organisation_sor_lookup_table_sql() on the connection. Borger from
    stg.pre_Helbredsforloeb; both SOR roles via #organisation_sor; dato keys via the shared
    dato_key(): dw_ek_Dato_slut = dato_key(ISNULL(slutdato, '')) so a forloeb
    without an end marker lands on BLANK (99999998, decision U2) rather than
    UNKNOWN, and dw_ek_Dato_opgoerelse on the reporting quarter's first day
    (opgoerelse_dato_sql; other formats -> BLANK). Typed measures use
    TRY_CONVERT (a non-numeric delivery value becomes NULL); udredningstid_dage
    is only carried when indgaar_i_indikator = 1 (U2's downstream rule);
    overholdt/inden_30_dage are BIT with NULL for blank.
    """
    sor_selects = ",\n    ".join(
        f"p.[{raw}],\n    COALESCE(o{i}.[dw_ek_Organisation], {UNKNOWN_ORGANISATION}) AS [{key}]"
        for i, (raw, key) in enumerate(SOR_ROLLER)
    )
    return f"""
DROP TABLE IF EXISTS {FCT_FORLOEB_TABLE};

SELECT
    p.[clean_code_Udredningsforloeb],
    p.[clean_code_Helbredsforloeb],
    {klassifikation_clean_code_sql("p")} AS [clean_code_UdredningsretKlassifikation],
    h.[dw_ek_Borger],
    h.[borger_opslag],

    -- SOR per role: raw code kept beside the resolved key
    {sor_selects},

    -- Dato keys (sentinels handled by dato_key; BLANK for missing end/quarter)
    {dato_key("p.[startdato]")} AS [dw_ek_Dato_start],
    {dato_key("ISNULL(p.[slutdato], '')")} AS [dw_ek_Dato_slut],
    {dato_key(opgoerelse_dato_sql("p.[opgoerelseskvartal]"))} AS [dw_ek_Dato_opgoerelse],

    -- Indicator measures, as delivered (B5), typed
    CASE WHEN p.[indgaar_i_indikator] = 1
         THEN TRY_CONVERT(INT, p.[udredningstid_raa]) END AS [udredningstid_dage],
    p.[indgaar_i_indikator],
    {_bit_from_flag("p.[overholdt]")} AS [overholdt],
    {_bit_from_flag("p.[inden_30_dage]")} AS [inden_30_dage],
    TRY_CONVERT(SMALLINT, p.[alder_raa]) AS [alder_ved_henvisning],
    p.[aktionsdiagnose_kode],
    p.[diagnosekontakt_uuid],
    p.[helbredsforloeb_id],
    p.[antal_markoerer],
    p.[antal_udredningsplaner],
    p.[antal_afbrydelser],
    TRY_CONVERT(DATETIME2, p.[startdato]) AS [dw_haendelsestidspunkt],
    p.[dw_is_deleted],
    p.[dw_timestamp_update]
INTO {FCT_FORLOEB_TABLE}
FROM {PRE_FORLOEB_TABLE} AS p
JOIN {PRE_HF_TABLE} AS h
  ON h.[helbredsforloeb_noegle] = p.[helbredsforloeb_noegle]
{_sor_joins("p", SOR_ROLLER)};

CREATE UNIQUE CLUSTERED INDEX [IX_fct_Udredningsforloeb_PK]
    ON {FCT_FORLOEB_TABLE}([clean_code_Udredningsforloeb]);
"""


def build_stg_dim_egenskaber_sql() -> str:
    """DROP + SELECT INTO stg.dim_UdredningsretEgenskaber from stg.pre_Udredningsforloeb.

    Shares grain and clean_code with stg.fct_Udredningsforloeb (the fact and
    its transaction dimension describe the same rows, shared PK downstream)
    and carries the classification clean_code plus EGENSKABER_ATTRIBUTTER —
    the ONE tuple the DDL test derives the dim columns from. No CPR, no
    borger key (that is the fact's side).
    """
    cols = ",\n    ".join(f"p.[{c}]" for c in EGENSKABER_ATTRIBUTTER)
    return f"""
DROP TABLE IF EXISTS {DIM_EGENSKABER_TABLE};

SELECT
    p.[clean_code_Udredningsforloeb],
    {klassifikation_clean_code_sql("p")} AS [clean_code_UdredningsretKlassifikation],
    {cols},
    p.[dw_is_deleted],
    p.[dw_timestamp_update]
INTO {DIM_EGENSKABER_TABLE}
FROM {PRE_FORLOEB_TABLE} AS p;

CREATE UNIQUE CLUSTERED INDEX [IX_dim_UdredningsretEgenskaber_PK]
    ON {DIM_EGENSKABER_TABLE}([clean_code_Udredningsforloeb]);
"""


# ---------------------------------------------------------------------------
# Diagnostics for asset metadata (counts only — never a row value)
# ---------------------------------------------------------------------------


def build_borger_coverage_sql() -> str:
    """Borger lookup outcome per helbredsforloeb in scope: cpr / tidligere_cpr / ukendt / tom."""
    return f"""
SELECT [borger_opslag], COUNT(*) AS [antal]
FROM {PRE_HF_TABLE}
GROUP BY [borger_opslag];
"""


def build_sor_coverage_sql(table: str, roles: tuple[tuple[str, str], ...]) -> str:
    """Per-role SOR lookup rates on a staged table: blank (expected) vs filled-but-unmatched vs matched.

    `roles` is a tuple of (raw code column, resolved key column), e.g.
    SOR_ROLLER for stg.fct_Udredningsforloeb or MARKOER_SOR_ROLLER for
    stg.fct_UdredningsretMarkoer. One row per role.
    """
    parts = []
    for raw, key in roles:
        parts.append(
            f"""
SELECT '{key}' AS [rolle],
    SUM(CASE WHEN [{raw}] IS NULL OR [{raw}] = '' THEN 1 ELSE 0 END) AS [tom_kode],
    SUM(CASE WHEN [{raw}] <> '' AND [{key}] = {UNKNOWN_ORGANISATION}
        THEN 1 ELSE 0 END) AS [udfyldt_uden_match],
    SUM(CASE WHEN [{key}] <> {UNKNOWN_ORGANISATION} THEN 1 ELSE 0 END) AS [matchet],
    COUNT(*) AS [ialt]
FROM {table}"""
        )
    return "\nUNION ALL\n".join(parts) + ";"


def build_grain_check_sql(table: str, column: str) -> str:
    """COUNT(*) vs COUNT(DISTINCT key): the explicit grain check beside the unique index."""
    return f"SELECT COUNT(*), COUNT(DISTINCT [{column}]) FROM {table}"
