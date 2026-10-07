# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL builders for the DRG staging chain.

Pure functions building the SQL for pre_stg_DRGKontakt, stg_fct_DRGKontakt
and stg_dim_DRGKontaktEgenskaber — kept out of the asset modules so the SQL
is unit-testable without a database (the HAIBA dm pattern).

Key decisions, written down once:

* ONE pre_stg asset for both delivery types (UNION ALL with a leverancetype
  discriminator), not one per type: they land in the same stg tables and the
  same fact, and two assets writing one table would race.
* CPR is resolved to dw_ek_Borger INSIDE pre_stg via #borger/#borger_tidl
  temp tables against dim.Borger, and the raw CPR never enters any stg table.
  dim.Borger is Type 2 on ADDRESS periods only —
  dw_ek_Borger is constant across a person's T2 rows — so the key lookup
  needs no version selection (version choice only matters when denormalising
  address attributes, which staging does not do). Unknown CPRs get -1 and are
  created in FHIR by a separate asset (the two-pass pattern from
  Patient_HAIBA.py); blank CPR gets -2 (N/A).
* SOR codes are resolved per role in stg_fct via a #org temp table (the
  HAIBA anti-unseekable-join trick); the raw code is kept next to the key
  for troubleshooting. Both blank and unmatched codes key to -1 like HAIBA —
  the blank/unmatched distinction lives in the reported rates, not the key,
  because dim.Organisation only seeds a -1 sentinel row.
* Delta scope propagates across tables: a contact is in
  the delta when the contact ITSELF, any of its diagnoses/procedures/
  supplements/internal codes OR its forloeb changed since the checkpoint.
  Implemented as #delta_kontakt/#delta_forloeb temp sets built from the
  children's dw_timestamp_update, so the probe scans the small changed set
  instead of probing every child table per contact row.
"""

from DSA.SQL.staging.drg_kontakt_kolonner import (
    CPR_SQL,
    DIM_ATTRIBUTE_NAMES,
    FORLOEB_FLAGS_SOMATIK,
    FORLOEB_MEASURES_PSYK,
    FORLOEB_MEASURES_SOMATIK,
    PRE_COLUMN_NAMES,
    PRE_COLUMNS,
    SOR_ROLES,
    clean_code_sql,
    klassifikation_clean_code_sql,
)

from etl.DSA.SQL.common.dato_dimension import dato_key

PRE_TABLE = "[stg].[pre_DRGKontakt]"
FCT_TABLE = "[stg].[fct_DRGKontakt]"
DIM_TABLE = "[stg].[dim_DRGKontaktEgenskaber]"

UNKNOWN_BORGER = -1
NA_BORGER = -2
UNKNOWN_ORGANISATION = -1

# Child tables whose changes must propagate their delta to the contact.
# All carry the resolved contact UUID under the same column name.
_CHILD_TABLES = (
    "drg_diagnose",
    "drg_diagnose_tillaeg",
    "drg_procedure",
    "drg_procedure_tillaeg",
    "drg_interne_koder",
    "drgpsyk_diagnose",
    "drgpsyk_diagnose_tillaeg",
    "drgpsyk_procedure",
    "drgpsyk_procedure_tillaeg",
    "drgpsyk_betalingsoplysninger",
)


def build_borger_lookup_sql() -> str:
    """#borger (cpr_nummer) + #borger_tidl (tidligere_cpr fallback).

    dw_ek_Borger is constant across dim.Borger's T2 address rows; MIN() is a
    deterministic collapse, mirroring HAIBA's guard against the one measured
    CPR with two patient ids. tidligere_cpr matters because DRG reaches years
    back in time; coverage with/without fallback is reported.
    """
    return """
DROP TABLE IF EXISTS #borger;
SELECT CAST([cpr_nummer] AS NVARCHAR(10)) AS [cpr], MIN([dw_ek_Borger]) AS [dw_ek_Borger]
INTO #borger
FROM [dim].[Borger]
WHERE [cpr_nummer] IS NOT NULL AND [cpr_nummer] <> ''
GROUP BY CAST([cpr_nummer] AS NVARCHAR(10));
CREATE UNIQUE CLUSTERED INDEX [IX_borger_cpr] ON #borger([cpr]);

DROP TABLE IF EXISTS #borger_tidl;
SELECT [cpr], MIN([dw_ek_Borger]) AS [dw_ek_Borger]
INTO #borger_tidl
FROM (
    SELECT CAST([tidligere_cpr] AS NVARCHAR(10)) AS [cpr], [dw_ek_Borger]
    FROM [dim].[Borger]
    WHERE [tidligere_cpr] IS NOT NULL AND [tidligere_cpr] <> ''
) AS t
GROUP BY [cpr];
CREATE UNIQUE CLUSTERED INDEX [IX_borger_tidl_cpr] ON #borger_tidl([cpr]);
"""


def build_delta_scope_sql(checkpoint: str | None) -> str:
    """Temp sets of contacts/forloeb touched since the checkpoint.

    Empty string when there is no checkpoint (full load: no filter at all).
    The checkpoint value comes from meta.DFMState (system-generated ISO
    datetime), escaped defensively anyway.
    """
    if checkpoint is None:
        return ""
    cp = "'" + str(checkpoint).replace("'", "''") + "'"

    child_selects = [
        f"SELECT [takstsystem], [kontakt_uuid] FROM [arc].[{table}] "
        f"WHERE [dw_timestamp_update] > {cp}"
        for table in _CHILD_TABLES
    ]
    union = "\n    UNION\n    ".join(child_selects)
    return f"""
DROP TABLE IF EXISTS #delta_kontakt;
SELECT DISTINCT [takstsystem], [kontakt_uuid]
INTO #delta_kontakt
FROM (
    {union}
) AS aendrede_boern;
CREATE UNIQUE CLUSTERED INDEX [IX_delta_kontakt]
    ON #delta_kontakt([takstsystem], [kontakt_uuid]);

DROP TABLE IF EXISTS #delta_forloeb;
SELECT [takstsystem], [forloeb_noegle]
INTO #delta_forloeb
FROM (
    SELECT [takstsystem], [ind_kontakt_uuid] AS [forloeb_noegle]
    FROM [arc].[drg_sghforlob] WHERE [dw_timestamp_update] > {cp}
    UNION
    SELECT [takstsystem], [foerste_kontakt_uuid]
    FROM [arc].[drgpsyk_forloeb] WHERE [dw_timestamp_update] > {cp}
) AS aendrede_forloeb;
CREATE UNIQUE CLUSTERED INDEX [IX_delta_forloeb]
    ON #delta_forloeb([takstsystem], [forloeb_noegle]);
"""


def _delta_where(checkpoint: str | None, forloeb_key_sql: str) -> str:
    """Per-branch delta predicate: contact itself, a child, or its forloeb."""
    if checkpoint is None:
        return ""
    cp = "'" + str(checkpoint).replace("'", "''") + "'"
    return f"""
  AND (
    k.[dw_timestamp_update] > {cp}
    OR EXISTS (SELECT 1 FROM #delta_kontakt dk
               WHERE dk.[takstsystem] = k.[takstsystem]
                 AND dk.[kontakt_uuid] = k.[KONTAKT_VAERDI])
    OR EXISTS (SELECT 1 FROM #delta_forloeb df
               WHERE df.[takstsystem] = k.[takstsystem]
                 AND df.[forloeb_noegle] = {forloeb_key_sql})
  )"""


def _pre_branch(leverancetype: str, checkpoint: str | None) -> str:
    """One UNION ALL branch of the pre_stg SELECT (somatik or psyk)."""
    arc_table = "[arc].[drg_kontakt]" if leverancetype == "DRG" else "[arc].[drgpsyk_kontakt]"
    forloeb_key = "k.[sygehusforloeb_noegle]" if leverancetype == "DRG" else "k.[forloeb_noegle]"
    cpr = CPR_SQL[leverancetype]
    staged = ",\n        ".join(c.select_for(leverancetype) for c in PRE_COLUMNS)
    return f"""
    SELECT
        '{leverancetype}' AS [leverancetype],
        k.[takstsystem],
        {clean_code_sql(leverancetype)} AS [clean_code_DRGKontakt],
        {staged},
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
        k.[dw_is_deleted],
        k.[dw_timestamp_update]
    FROM {arc_table} AS k
    -- No length cast on the delivery side: CAST(... AS NVARCHAR(10)) would
    -- silently TRUNCATE a malformed 11+-character value and could mis-match
    -- another person's CPR. An overlong value simply finds no match (-1).
    LEFT JOIN #borger AS b ON b.[cpr] = LTRIM(RTRIM({cpr}))
    LEFT JOIN #borger_tidl AS bt ON bt.[cpr] = LTRIM(RTRIM({cpr}))
    WHERE 1 = 1{_delta_where(checkpoint, forloeb_key)}"""


def build_pre_stg_sql(checkpoint: str | None) -> str:
    """DROP + SELECT INTO stg.pre_DRGKontakt (both delivery types, deduped).

    Soft-deleted contacts ARE included (dw_is_deleted carried through) so
    the downstream fact merge can retire them; the fact layer filters. Dedup is
    defensive — arc's PK already guarantees the grain — with a deterministic
    ORDER BY, mirroring pre_stg_Vareudlevering_ils.
    """
    staged_columns = ", ".join(
        f"[{c}]"
        for c in (
            "leverancetype",
            "takstsystem",
            "clean_code_DRGKontakt",
            *PRE_COLUMN_NAMES,
            "dw_ek_Borger",
            "borger_opslag",
            "dw_is_deleted",
            "dw_timestamp_update",
        )
    )
    return f"""
DROP TABLE IF EXISTS {PRE_TABLE};

SELECT {staged_columns}
INTO {PRE_TABLE}
FROM (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY [clean_code_DRGKontakt]
               ORDER BY [dw_timestamp_update] DESC, [kontakt_uuid] ASC
           ) AS [rn]
    FROM (
{_pre_branch("DRG", checkpoint)}
    UNION ALL
{_pre_branch("DRGPSYK", checkpoint)}
    ) AS samlet
) AS ranked
WHERE [rn] = 1;

CREATE UNIQUE CLUSTERED INDEX [IX_pre_DRGKontakt_PK]
    ON {PRE_TABLE}([clean_code_DRGKontakt]);
CREATE NONCLUSTERED INDEX [IX_pre_DRGKontakt_forloeb]
    ON {PRE_TABLE}([leverancetype], [takstsystem], [forloeb_noegle]);
"""


def build_org_lookup_sql() -> str:
    """#org: live dim.Organisation rows with kode cast to NVARCHAR, uniquely indexed.

    Same rationale as HAIBA (dm/haiba.py): joining dim.Organisation's varchar
    kode against an nvarchar source column is unseekable; the temp copy makes
    the join type-matched and indexed. The schema only guarantees kode
    uniqueness among LIVE rows (a retired organisation keeps its row, and its
    kode can be reissued under a new key) — so soft-deleted rows are filtered
    both to keep the unique index valid and so new facts never resolve to a
    retired organisation.
    """
    return """
DROP TABLE IF EXISTS #org;
SELECT CAST([kode] AS NVARCHAR(50)) AS [kode], [dw_ek_Organisation]
INTO #org
FROM [dim].[Organisation]
WHERE [dw_is_deleted] = 0;
CREATE UNIQUE CLUSTERED INDEX [IX_org_kode] ON #org([kode]);
"""


def build_stg_fct_sql() -> str:
    """DROP + SELECT INTO stg.fct_DRGKontakt from stg.pre_DRGKontakt.

    Typed measures (TRY_CONVERT — a non-numeric delivery value becomes NULL,
    counted in the asset's stats), dato keys via dato_key(), SOR keys per
    role via #org, and forloeb measures on the CARRYING contact only:
    somatik = tungeste_drgkontakt='1', psyk = the forloeb's first contact.
    """
    sor_joins = "\n".join(
        f"LEFT JOIN #org AS o{i} ON o{i}.[kode] = p.[{raw}] AND p.[{raw}] <> ''"
        for i, (raw, _) in enumerate(SOR_ROLES)
    )
    sor_selects = ",\n        ".join(
        f"COALESCE(o{i}.[dw_ek_Organisation], {UNKNOWN_ORGANISATION}) AS [{key}],\n"
        f"        p.[{raw}]"
        for i, (raw, key) in enumerate(SOR_ROLES)
    )
    somatik_measures = ",\n        ".join(
        f"CASE WHEN p.[leverancetype] = 'DRG' AND p.[tungeste_drgkontakt] = '1' "
        f"THEN TRY_CONVERT(DECIMAL(19,4), {expr}) END AS [{name}]"
        for name, expr in FORLOEB_MEASURES_SOMATIK
    )
    somatik_flags = ",\n        ".join(
        f"CASE WHEN p.[leverancetype] = 'DRG' AND p.[tungeste_drgkontakt] = '1' "
        f"THEN {expr} END AS [{name}]"
        for name, expr in FORLOEB_FLAGS_SOMATIK
    )
    psyk_measures = ",\n        ".join(
        f"CASE WHEN p.[leverancetype] = 'DRGPSYK' "
        f"THEN TRY_CONVERT(DECIMAL(19,4), {expr}) END AS [{name}]"
        for name, expr in FORLOEB_MEASURES_PSYK
    )
    return f"""
DROP TABLE IF EXISTS {FCT_TABLE};

SELECT
        p.[clean_code_DRGKontakt],
        {klassifikation_clean_code_sql('p')} AS [clean_code_DRGKontaktKlassifikation],
        p.[leverancetype],
        p.[takstsystem],
        p.[aktivitetsaar],
        p.[kontakt_uuid],
        p.[forloeb_noegle],
        p.[dw_ek_Borger],
        p.[borger_opslag],

        -- SOR pr. rolle: resolved key + the raw code kept beside it
        {sor_selects},

        -- Dato keys (sentinels handled by dato_key)
        {dato_key('p.[ind_tidspunkt]')} AS [dw_ek_Dato_ind],
        TRY_CONVERT(DATETIME2, p.[ind_tidspunkt]) AS [dato_tid_ind],
        {dato_key('p.[ud_tidspunkt]')} AS [dw_ek_Dato_ud],
        TRY_CONVERT(DATETIME2, p.[ud_tidspunkt]) AS [dato_tid_ud],
        {dato_key('p.[henvisnings_tidspunkt]')} AS [dw_ek_Dato_henvisning],

        -- Contact measures (typed; somatik only, psyk stays NULL)
        TRY_CONVERT(DECIMAL(19,4), p.[pris_drg_raa]) AS [pris_drg],
        TRY_CONVERT(DECIMAL(19,4), p.[langliggerpris_raa]) AS [langliggerpris],
        TRY_CONVERT(DECIMAL(19,4), p.[totalpris_drg_raa]) AS [totalpris_drg],
        TRY_CONVERT(INT, p.[langliggerdage_raa]) AS [langliggerdage],
        TRY_CONVERT(INT, p.[liggedage_raa]) AS [liggedage],
        TRY_CONVERT(INT, p.[kontaktdage_raa]) AS [kontaktdage],
        TRY_CONVERT(DECIMAL(19,4), p.[varighed_time_raa]) AS [varighed_time],

        -- Flags
        p.[tungeste_drgkontakt],
        p.[udfaldsgivende_kontakt],

        -- Forloeb measures on the carrying contact only
        {somatik_measures},
        {somatik_flags},
        {psyk_measures},

        TRY_CONVERT(DATETIME2, p.[ind_tidspunkt]) AS [dw_haendelsestidspunkt],
        p.[dw_is_deleted],
        p.[dw_timestamp_update]

INTO {FCT_TABLE}
FROM {PRE_TABLE} AS p
LEFT JOIN [arc].[drg_sghforlob] AS f
    ON p.[leverancetype] = 'DRG'
   AND p.[tungeste_drgkontakt] = '1'
   AND f.[takstsystem] = p.[takstsystem]
   AND f.[ind_kontakt_uuid] = p.[forloeb_noegle]
   AND f.[dw_is_deleted] = 0
LEFT JOIN [arc].[drgpsyk_forloeb] AS fp
    ON p.[leverancetype] = 'DRGPSYK'
   AND fp.[takstsystem] = p.[takstsystem]
   AND fp.[foerste_kontakt_uuid] = p.[kontakt_uuid]
   AND fp.[dw_is_deleted] = 0
{sor_joins};

CREATE UNIQUE CLUSTERED INDEX [IX_fct_DRGKontakt_PK]
    ON {FCT_TABLE}([clean_code_DRGKontakt]);
"""


def build_forloeb_kontroltotal_sql() -> str:
    """Control totals: carried forloeb measures vs the arc forloeb tables.

    Scoped to the forloeb actually represented by a carrying contact in stg
    (the staging is delta-scoped, so an archive-wide sum only matches on a
    full load). Two checks per delivery type:
      1. exactly one carrying contact per (takstsystem, forloeb) — the
         carrier is identified by the carrying PROPERTY (somatik: the
         tungeste flag; psyk: the forloeb join), never by a measure being
         non-NULL, so a forloeb whose measures are all blank still counts.
         Somatik counts carriers in LIVE arc rows (scoped to the staged
         forloeb): a delta batch only sees its own rows, but a second
         carrier introduced by an earlier batch is visible in arc
      2. the carried sum equals the arc sum for those forloeb — retired
         (soft-deleted) forloeb rows carry nothing and compare against
         nothing, matching the fact join
    Returns one row per delivery type with deviation counts.
    """

    # Compare EVERY carried forloeb measure against its arc source, not just the
    # headline price — built from the same FORLOEB_* constants build_stg_fct_sql
    # writes from, so the check can never silently cover fewer columns than the
    # writer stages. Measures are numeric (TRY_CONVERT DECIMAL, matching the
    # writer); the naerhed/kirurgi flags are compared as text. EXCEPT treats two
    # NULLs as equal, so a forloeb whose measures are all blank still matches.
    def _cols(pairs, *, numeric, arc_alias="f"):
        stg = ", ".join(f"s.[{name}]" for name, _ in pairs)
        arc = ", ".join(
            (
                f"TRY_CONVERT(DECIMAL(19,4), {expr.replace('fp.', arc_alias + '.')})"
                if numeric
                else expr.replace("fp.", arc_alias + ".")
            )
            for _, expr in pairs
        )
        return stg, arc

    som_m_stg, som_m_arc = _cols(FORLOEB_MEASURES_SOMATIK, numeric=True)
    som_f_stg, som_f_arc = _cols(FORLOEB_FLAGS_SOMATIK, numeric=False)
    som_stg, som_arc = f"{som_m_stg}, {som_f_stg}", f"{som_m_arc}, {som_f_arc}"
    psyk_stg, psyk_arc = _cols(FORLOEB_MEASURES_PSYK, numeric=True)
    return f"""
SELECT
    'DRG' AS [leverancetype],
    (SELECT COUNT(*) FROM (
        SELECT k.[takstsystem], k.[sygehusforloeb_noegle]
        FROM [arc].[drg_kontakt] AS k
        WHERE k.[dw_is_deleted] = 0 AND k.[TUNGESTE_DRGKONTAKT] = '1'
          AND EXISTS (SELECT 1 FROM {FCT_TABLE} AS s
                      WHERE s.[leverancetype] = 'DRG'
                        AND s.[takstsystem] = k.[takstsystem]
                        AND s.[forloeb_noegle] = k.[sygehusforloeb_noegle])
        GROUP BY k.[takstsystem], k.[sygehusforloeb_noegle]
        HAVING COUNT(*) > 1) AS flere_baerende) AS [forloeb_med_flere_baerende],
    (SELECT COUNT(*)
     FROM {FCT_TABLE} AS s
     JOIN [arc].[drg_sghforlob] AS f
       ON f.[takstsystem] = s.[takstsystem]
      AND f.[ind_kontakt_uuid] = s.[forloeb_noegle]
      AND f.[dw_is_deleted] = 0
     WHERE s.[leverancetype] = 'DRG'
       AND s.[tungeste_drgkontakt] = '1'
       AND EXISTS (SELECT {som_stg}
                   EXCEPT
                   SELECT {som_arc})
    ) AS [afvigende_maal]
UNION ALL
SELECT
    'DRGPSYK',
    (SELECT COUNT(*) FROM (
        SELECT s.[takstsystem], f.[foerste_kontakt_uuid]
        FROM {FCT_TABLE} AS s
        JOIN [arc].[drgpsyk_forloeb] AS f
          ON f.[takstsystem] = s.[takstsystem]
         AND f.[foerste_kontakt_uuid] = s.[kontakt_uuid]
         AND f.[dw_is_deleted] = 0
        WHERE s.[leverancetype] = 'DRGPSYK'
        GROUP BY s.[takstsystem], f.[foerste_kontakt_uuid]
        HAVING COUNT(*) > 1) AS flere_baerende),
    (SELECT COUNT(*)
     FROM {FCT_TABLE} AS s
     JOIN [arc].[drgpsyk_forloeb] AS f
       ON f.[takstsystem] = s.[takstsystem]
      AND f.[foerste_kontakt_uuid] = s.[kontakt_uuid]
      AND f.[dw_is_deleted] = 0
     WHERE s.[leverancetype] = 'DRGPSYK'
       AND EXISTS (SELECT {psyk_stg}
                   EXCEPT
                   SELECT {psyk_arc})
    );
"""


def build_stg_dim_sql() -> str:
    """DROP + SELECT INTO stg.dim_DRGKontaktEgenskaber from pre.

    Shares grain and clean_code with stg_fct — the fact and its transaction
    dimension must describe the same rows — and reads pre only, never arc. Kommune/regions-NAMES are deliberately absent:
    no classification source exists in the repo;
    the delivery's own codes are staged as-is.
    """
    attribute_names = ("leverancetype", "takstsystem", *DIM_ATTRIBUTE_NAMES)
    cols = ",\n        ".join(f"p.[{c}]" for c in attribute_names)
    return f"""
DROP TABLE IF EXISTS {DIM_TABLE};

SELECT
        p.[clean_code_DRGKontakt],
        {klassifikation_clean_code_sql("p")} AS [clean_code_DRGKontaktKlassifikation],
        {cols},
        p.[dw_is_deleted],
        p.[dw_timestamp_update]
INTO {DIM_TABLE}
FROM {PRE_TABLE} AS p;

CREATE UNIQUE CLUSTERED INDEX [IX_dim_DRGKontaktEgenskaber_PK]
    ON {DIM_TABLE}([clean_code_DRGKontakt]);
"""


def build_borger_coverage_sql() -> str:
    """Lookup rates for asset metadata, expected vs unexpected split."""
    return f"""
SELECT [leverancetype], [borger_opslag], COUNT(*) AS [antal]
FROM {PRE_TABLE}
GROUP BY [leverancetype], [borger_opslag];
"""


def build_sor_coverage_sql() -> str:
    """Per-role lookup rates: blank (expected) vs filled-but-unmatched."""
    parts = []
    for raw, key in SOR_ROLES:
        parts.append(
            f"""
SELECT '{key}' AS [rolle], [leverancetype],
    SUM(CASE WHEN [{raw}] IS NULL OR [{raw}] = '' THEN 1 ELSE 0 END) AS [tom_kode],
    SUM(CASE WHEN [{raw}] <> '' AND [{key}] = {UNKNOWN_ORGANISATION}
        THEN 1 ELSE 0 END) AS [udfyldt_uden_match],
    SUM(CASE WHEN [{key}] <> {UNKNOWN_ORGANISATION} THEN 1 ELSE 0 END) AS [matchet],
    COUNT(*) AS [ialt]
FROM {FCT_TABLE}
GROUP BY [leverancetype]"""
        )
    return "\nUNION ALL\n".join(parts) + ";"
