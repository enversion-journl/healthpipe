# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Pure SQL builders for the DRG code + group chain (staging -> map -> dim,
plus group interning -> group->code bridge).

A contact's diagnoses and procedures are exposed on fct.DRGKontakt via a
GROUP KEY (group-key / multi-valued dimension), so analyses can filter on SKS
codes without the flattened DIA01-20/PROC01-40 columns, and the whole set is
stored once per DISTINCT set rather than once per contact.

Design:
  * dim.DRGDiagnose  — one member per DISTINCT (diagnose_kode, art,
    sideangivelse, tillaegskoder), independent of contact.
  * dim.DRGProcedure — one member per DISTINCT (procedure_kode,
    sideangivelse, handlingsspec, kontrast, tillaegskoder). No timestamps.
  * map.DRG{Diagnose,Procedure}Gruppe — intern a contact's DISTINCT set of
    codes as one group key (procedures include the producing unit).
  * bridge.DRG{Diagnose,Procedure}Gruppe — append-only group->code membership.
  * fct.DRGKontakt.dw_ek_DRG{Diagnose,Procedure}Gruppe — one FK per contact;
    the empty set resolves to the -1 group.

The code staging is built from the ARC CHILD tables (the flattened columns are
only a consistency check), unioning somatik (DRG) and psyk (DRGPSYK).
Tillaegskoder are denormalised onto the combination as a sorted, delimited set;
the group key is a hash over the sorted, distinct member set. Both are
order-independent. The child tables carry no CPR.
"""

from DSA.SQL.staging.drg_kontakt_kolonner import clean_code_sql
from DSA.SQL.staging.drg_staging_sql import build_org_lookup_sql

DIAGNOSE_STG = "[stg].[bridge_DRGKontaktDiagnose]"
PROCEDURE_STG = "[stg].[bridge_DRGKontaktProcedure]"

# Persistent delta-contact scope (read on a separate connection by fct's
# group-FK refresh). One row per contact whose diagnose/procedure registrations
# changed since the checkpoint — the set of contacts whose group FK is re-pointed.
DIAGNOSE_SCOPE = "[stg].[bridge_DRGKontaktDiagnose_scope]"
PROCEDURE_SCOPE = "[stg].[bridge_DRGKontaktProcedure_scope]"

# A missing consumer checkpoint (and full_load) counts as the full archive.
BEGINNING_OF_TIME = "0001-01-01"

# Canonical dimension attribute sets (staged column names), tillaeg last.
DIAGNOSE_ATTRIBUTES = ("diagnose_kode", "art", "sideangivelse", "tillaegskoder")
PROCEDURE_ATTRIBUTES = (
    "procedure_kode",
    "sideangivelse",
    "handlingsspec",
    "kontrast",
    "tillaegskoder",
)

_DIAGNOSE_SOURCES = {
    "DRG": {
        "table": "[arc].[drg_diagnose]",
        "tillaeg": "[arc].[drg_diagnose_tillaeg]",
        "kode": "DIAGNOSE",
        "aar": "[Aktivitetsaar]",
    },
    "DRGPSYK": {
        "table": "[arc].[drgpsyk_diagnose]",
        "tillaeg": "[arc].[drgpsyk_diagnose_tillaeg]",
        "kode": "Diagnose",
        "aar": "CAST(NULL AS NVARCHAR(6))",
    },
}

_PROCEDURE_SOURCES = {
    "DRG": {
        "table": "[arc].[drg_procedure]",
        "tillaeg": "[arc].[drg_procedure_tillaeg]",
        "kode": "PROCEDURE",
        "aar": "[Aktivitetsaar]",
    },
    "DRGPSYK": {
        "table": "[arc].[drgpsyk_procedure]",
        "tillaeg": "[arc].[drgpsyk_procedure_tillaeg]",
        "kode": "PROCEDUREKODE",
        "aar": "CAST(NULL AS NVARCHAR(6))",
    },
}


def _combination_clean_code(prefix: str, alias: str, columns: tuple[str, ...]) -> str:
    """A hashed clean_code over a code combination, injective and stable.

    Each value is UPPER+trim-normalised and length-prefixed before hashing,
    so ('AB','C') and ('A','BC') never collide and a NULL collapses with an
    empty value by design. The hash keeps the key inside the 900-byte
    clustered-key limit even when tillaegskoder holds a long set.
    """
    parts = []
    for col in columns:
        norm = f"ISNULL(UPPER(LTRIM(RTRIM(CONVERT(NVARCHAR(MAX), {alias}.[{col}])))), N'')"
        parts.append(f"CONCAT(LEN({norm}), N':', {norm}, N'|')")
    combined = f"CONCAT({', '.join(parts)})"
    return f"'{prefix}_' + CONVERT(CHAR(64), HASHBYTES('SHA2_256', {combined}), 2)"


def diagnose_clean_code_sql(alias: str) -> str:
    return _combination_clean_code("DRGDIAG", alias, DIAGNOSE_ATTRIBUTES)


def procedure_clean_code_sql(alias: str) -> str:
    return _combination_clean_code("DRGPROC", alias, PROCEDURE_ATTRIBUTES)


def _bridge_clean_code(leverancetype: str, alias: str) -> str:
    """clean_code_DRGKontakt from a child table's kontakt_uuid — the same
    string map.DRGKontakt minted from the contact's KONTAKT_VAERDI."""
    return (
        clean_code_sql(leverancetype)
        .replace("k.[KONTAKT_VAERDI]", f"{alias}.[kontakt_uuid]")
        .replace("k.[takstsystem]", f"{alias}.[takstsystem]")
    )


def _delta_predicate(checkpoint: str, alias: str) -> str:
    escaped = "'" + str(checkpoint).replace("'", "''") + "'"
    return f"{alias}.[dw_timestamp_update] > {escaped}"


def _scope_sql(scope_table: str, index: str, sources: dict, checkpoint: str) -> str:
    """Persist the delta contacts (clean_code_DRGKontakt) whose child or tillaeg
    rows changed since the checkpoint.

    For BOTH delivery types and WITHOUT a dw_is_deleted filter: a contact whose
    registrations were all retracted still lands in the scope, so fct's group-FK
    refresh re-points it to the -1 empty group. In full load the checkpoint is
    BEGINNING_OF_TIME, so the scope is every contact. Persistent (stg) rather
    than a temp, because fct reads it on a separate connection.
    """
    parts = []
    for lev, src in sources.items():
        cc = _bridge_clean_code(lev, "d")
        for tbl in (src["table"], src["tillaeg"]):
            parts.append(
                f"SELECT {cc} AS [clean_code_DRGKontakt] FROM {tbl} AS d"
                f" WHERE {_delta_predicate(checkpoint, 'd')}"
            )
    union = "\n    UNION\n    ".join(parts)
    return f"""
DROP TABLE IF EXISTS {scope_table};
SELECT DISTINCT [clean_code_DRGKontakt]
INTO {scope_table}
FROM (
    {union}
) AS d;
CREATE UNIQUE CLUSTERED INDEX [{index}] ON {scope_table}([clean_code_DRGKontakt]);
"""


def build_diagnose_scope_sql(checkpoint: str) -> str:
    return _scope_sql(DIAGNOSE_SCOPE, "IX_scope_diagnose", _DIAGNOSE_SOURCES, checkpoint)


def build_procedure_scope_sql(checkpoint: str) -> str:
    return _scope_sql(PROCEDURE_SCOPE, "IX_scope_procedure", _PROCEDURE_SOURCES, checkpoint)


def _diagnose_tillaeg_preagg() -> str:
    """Pre-aggregate diagnosis tillaegskoder once into #tillaeg_diagnose.

    One sorted, delimited set per (takstsystem, kontakt_uuid, code, art, side),
    so the main staging pass LEFT JOINs it instead of running a correlated
    STRING_AGG per registration. kontakt_uuid is unique per contact, so both
    delivery types share the temp without a leverancetype column. Scoped to the
    delta contacts (all contacts in full load, where the scope table is every
    contact).
    """
    parts = []
    for lev, src in _DIAGNOSE_SOURCES.items():
        parts.append(
            f"""
        SELECT t.[takstsystem], t.[kontakt_uuid],
               t.[{src['kode']}] AS [kode], t.[ART] AS [art],
               t.[SIDEANGIVELSE] AS [side],
               t.[TILLAEGSKODE_DIAGNOSE] AS [tillaeg]
        FROM {src['tillaeg']} AS t
        INNER JOIN {DIAGNOSE_SCOPE} sc ON sc.[clean_code_DRGKontakt] = {_bridge_clean_code(lev, 't')}
        WHERE t.[dw_is_deleted] = 0"""
        )
    union = "\n        UNION ALL".join(parts)
    return f"""
DROP TABLE IF EXISTS #tillaeg_diagnose;
SELECT [takstsystem], [kontakt_uuid], [kode], [art], [side],
       STRING_AGG(CAST([tillaeg] AS NVARCHAR(MAX)), ';')
           WITHIN GROUP (ORDER BY [tillaeg]) AS [tillaegskoder]
INTO #tillaeg_diagnose
FROM ({union}
) AS s
GROUP BY [takstsystem], [kontakt_uuid], [kode], [art], [side];
CREATE UNIQUE CLUSTERED INDEX [IX_tillaeg_diagnose]
    ON #tillaeg_diagnose([takstsystem], [kontakt_uuid], [kode], [art], [side]);
"""


def _diagnose_attr_branch(leverancetype: str) -> str:
    src = _DIAGNOSE_SOURCES[leverancetype]
    cc = _bridge_clean_code(leverancetype, "d")
    kode = src["kode"]
    return f"""
        SELECT
            {cc} AS [clean_code_DRGKontakt],
            '{leverancetype}' AS [leverancetype],
            d.[takstsystem],
            {src['aar']} AS [aktivitetsaar],
            UPPER(LTRIM(RTRIM(d.[{kode}]))) AS [diagnose_kode],
            d.[ART] AS [art],
            d.[SIDEANGIVELSE] AS [sideangivelse],
            tl.[tillaegskoder] AS [tillaegskoder]
        FROM {src['table']} AS d
        INNER JOIN {DIAGNOSE_SCOPE} sc ON sc.[clean_code_DRGKontakt] = {cc}
        LEFT JOIN #tillaeg_diagnose tl
            ON tl.[takstsystem] = d.[takstsystem]
           AND tl.[kontakt_uuid] = d.[kontakt_uuid]
           AND tl.[kode] = d.[{kode}]
           AND tl.[art] = d.[ART]
           AND tl.[side] = d.[SIDEANGIVELSE]
        WHERE d.[dw_is_deleted] = 0"""


def build_diagnose_stg_sql() -> str:
    """DROP + SELECT INTO stg.bridge_DRGKontaktDiagnose: one row per LIVE
    diagnosis registration for the delta contacts (the scope table is built
    first). Tillaegskoder are pre-aggregated once and joined; only live rows
    are staged.
    """
    preagg = _diagnose_tillaeg_preagg()
    branches = "\n        UNION ALL".join(_diagnose_attr_branch(lev) for lev in _DIAGNOSE_SOURCES)
    return f"""{preagg}
DROP TABLE IF EXISTS {DIAGNOSE_STG};
SELECT
    a.[clean_code_DRGKontakt],
    {diagnose_clean_code_sql('a')} AS [clean_code_DRGDiagnose],
    a.[leverancetype],
    a.[takstsystem],
    a.[aktivitetsaar],
    a.[diagnose_kode],
    a.[art],
    a.[sideangivelse],
    a.[tillaegskoder]
INTO {DIAGNOSE_STG}
FROM ({branches}
) AS a;
CREATE UNIQUE CLUSTERED INDEX [IX_stg_bridge_diagnose]
    ON {DIAGNOSE_STG}([clean_code_DRGKontakt], [clean_code_DRGDiagnose]);
"""


def _procedure_tillaeg_preagg() -> str:
    """Pre-aggregate procedure tillaegskoder once into #tillaeg_procedure.

    One sorted, delimited set per (takstsystem, procedure_uuid); PROCEDURE_VAERDI
    is globally unique, so both delivery types share the temp. Scoped to the
    delta contacts (all contacts in full load).
    """
    parts = []
    for lev, src in _PROCEDURE_SOURCES.items():
        parts.append(
            f"""
        SELECT t.[takstsystem], t.[procedure_uuid],
               t.[TILLAEGSKODE_PROCEDURE] AS [tillaeg]
        FROM {src['tillaeg']} AS t
        INNER JOIN {PROCEDURE_SCOPE} sc ON sc.[clean_code_DRGKontakt] = {_bridge_clean_code(lev, 't')}
        WHERE t.[dw_is_deleted] = 0"""
        )
    union = "\n        UNION ALL".join(parts)
    return f"""
DROP TABLE IF EXISTS #tillaeg_procedure;
SELECT [takstsystem], [procedure_uuid],
       STRING_AGG(CAST([tillaeg] AS NVARCHAR(MAX)), ';')
           WITHIN GROUP (ORDER BY [tillaeg]) AS [tillaegskoder]
INTO #tillaeg_procedure
FROM ({union}
) AS s
GROUP BY [takstsystem], [procedure_uuid];
CREATE UNIQUE CLUSTERED INDEX [IX_tillaeg_procedure]
    ON #tillaeg_procedure([takstsystem], [procedure_uuid]);
"""


def _procedure_attr_branch(leverancetype: str) -> str:
    src = _PROCEDURE_SOURCES[leverancetype]
    cc = _bridge_clean_code(leverancetype, "d")
    kode = src["kode"]
    return f"""
        SELECT
            {cc} AS [clean_code_DRGKontakt],
            '{leverancetype}' AS [leverancetype],
            d.[takstsystem],
            {src['aar']} AS [aktivitetsaar],
            d.[PROCEDURE_VAERDI] AS [procedure_vaerdi],
            UPPER(LTRIM(RTRIM(d.[{kode}]))) AS [procedure_kode],
            d.[SIDEANGIVELSE] AS [sideangivelse],
            d.[HANDLINGSSPEC] AS [handlingsspec],
            d.[KONTRAST] AS [kontrast],
            d.[PRODUCERENDE_ENHED] AS [producerende_sor],
            tl.[tillaegskoder] AS [tillaegskoder]
        FROM {src['table']} AS d
        INNER JOIN {PROCEDURE_SCOPE} sc ON sc.[clean_code_DRGKontakt] = {cc}
        LEFT JOIN #tillaeg_procedure tl
            ON tl.[takstsystem] = d.[takstsystem]
           AND tl.[procedure_uuid] = d.[PROCEDURE_VAERDI]
        WHERE d.[dw_is_deleted] = 0"""


def build_procedure_stg_sql() -> str:
    """DROP + SELECT INTO stg.bridge_DRGKontaktProcedure: one row per LIVE
    procedure registration for the delta contacts, with the raw producing-unit
    SOR (resolved to dw_ek_Organisation in the asset). Tillaegskoder are
    pre-aggregated once and joined.
    """
    preagg = _procedure_tillaeg_preagg()
    branches = "\n        UNION ALL".join(_procedure_attr_branch(lev) for lev in _PROCEDURE_SOURCES)
    return f"""{preagg}
DROP TABLE IF EXISTS {PROCEDURE_STG};
SELECT
    a.[clean_code_DRGKontakt],
    {procedure_clean_code_sql('a')} AS [clean_code_DRGProcedure],
    a.[leverancetype],
    a.[takstsystem],
    a.[aktivitetsaar],
    a.[procedure_vaerdi],
    a.[procedure_kode],
    a.[sideangivelse],
    a.[handlingsspec],
    a.[kontrast],
    a.[producerende_sor],
    a.[tillaegskoder]
INTO {PROCEDURE_STG}
FROM ({branches}
) AS a;
CREATE UNIQUE CLUSTERED INDEX [IX_stg_bridge_procedure]
    ON {PROCEDURE_STG}([clean_code_DRGKontakt], [procedure_vaerdi]);
"""


# ---------------------------------------------------------------------------
# Group layer: intern a contact's DISTINCT set of codes as one group key.
# ---------------------------------------------------------------------------

# Per-contact group hash + group->member staging tables.
DIAGNOSE_GRUPPE_STG = "[stg].[DRGKontaktDiagnoseGruppe]"
DIAGNOSE_GRUPPE_MEDLEM_STG = "[stg].[DRGDiagnoseGruppeMedlem]"
PROCEDURE_GRUPPE_STG = "[stg].[DRGKontaktProcedureGruppe]"
PROCEDURE_GRUPPE_MEDLEM_STG = "[stg].[DRGProcedureGruppeMedlem]"


def _gruppe_hash(prefix: str, element_expr: str, order_by: str) -> str:
    """Hash a contact's sorted, distinct member set into one group clean_code.

    Order-independent (sorted) and injective: every member clean_code is a
    fixed-length string ('DRG...' + 64 hex), the elements are '|'-delimited and
    the delimiter never occurs inside a member, so distinct sets never collide.
    """
    agg = (
        f"STRING_AGG(CONVERT(VARCHAR(MAX), {element_expr}), '|') "
        f"WITHIN GROUP (ORDER BY {order_by})"
    )
    return f"'{prefix}_' + CONVERT(CHAR(64), HASHBYTES('SHA2_256', {agg}), 2)"


def build_diagnose_gruppe_stg_sql() -> str:
    """DROP + SELECT INTO the diagnose group staging: one group clean_code per
    contact (over its distinct diagnose members) and the distinct group->member
    rows. Derives from the already-scoped per-registration staging."""
    hash_expr = _gruppe_hash("DRGDIAGGRP", "clean_code_DRGDiagnose", "clean_code_DRGDiagnose")
    return f"""
DROP TABLE IF EXISTS #ccd;
SELECT DISTINCT [clean_code_DRGKontakt], [clean_code_DRGDiagnose]
INTO #ccd
FROM {DIAGNOSE_STG};

DROP TABLE IF EXISTS {DIAGNOSE_GRUPPE_STG};
SELECT
    [clean_code_DRGKontakt],
    {hash_expr} AS [clean_code_DRGDiagnoseGruppe]
INTO {DIAGNOSE_GRUPPE_STG}
FROM #ccd
GROUP BY [clean_code_DRGKontakt];
CREATE UNIQUE CLUSTERED INDEX [IX_stg_diag_gruppe]
    ON {DIAGNOSE_GRUPPE_STG}([clean_code_DRGKontakt]);

DROP TABLE IF EXISTS {DIAGNOSE_GRUPPE_MEDLEM_STG};
SELECT DISTINCT g.[clean_code_DRGDiagnoseGruppe], d.[clean_code_DRGDiagnose]
INTO {DIAGNOSE_GRUPPE_MEDLEM_STG}
FROM #ccd d
INNER JOIN {DIAGNOSE_GRUPPE_STG} g ON g.[clean_code_DRGKontakt] = d.[clean_code_DRGKontakt];
CREATE UNIQUE CLUSTERED INDEX [IX_stg_diag_gruppe_medlem]
    ON {DIAGNOSE_GRUPPE_MEDLEM_STG}([clean_code_DRGDiagnoseGruppe], [clean_code_DRGDiagnose]);
"""


def build_procedure_gruppe_stg_sql() -> str:
    """DROP + SELECT INTO the procedure group staging. The producing unit is
    resolved to dw_ek_Organisation and is part of the group's defining set, so
    the group hash and the membership both carry it."""
    element = "CONCAT([clean_code_DRGProcedure], ':', [dw_ek_Organisation_producerende])"
    hash_expr = _gruppe_hash(
        "DRGPROCGRP", element, "[clean_code_DRGProcedure], [dw_ek_Organisation_producerende]"
    )
    return (
        build_org_lookup_sql()
        + f"""
DROP TABLE IF EXISTS #ccp;
SELECT DISTINCT
    s.[clean_code_DRGKontakt],
    s.[clean_code_DRGProcedure],
    COALESCE(o.[dw_ek_Organisation], -1) AS [dw_ek_Organisation_producerende]
INTO #ccp
FROM {PROCEDURE_STG} s
LEFT JOIN #org o
    ON o.[kode] = s.[producerende_sor]
   AND s.[producerende_sor] IS NOT NULL
   AND s.[producerende_sor] <> '';

DROP TABLE IF EXISTS {PROCEDURE_GRUPPE_STG};
SELECT
    [clean_code_DRGKontakt],
    {hash_expr} AS [clean_code_DRGProcedureGruppe]
INTO {PROCEDURE_GRUPPE_STG}
FROM #ccp
GROUP BY [clean_code_DRGKontakt];
CREATE UNIQUE CLUSTERED INDEX [IX_stg_proc_gruppe]
    ON {PROCEDURE_GRUPPE_STG}([clean_code_DRGKontakt]);

DROP TABLE IF EXISTS {PROCEDURE_GRUPPE_MEDLEM_STG};
SELECT DISTINCT
    g.[clean_code_DRGProcedureGruppe],
    p.[clean_code_DRGProcedure],
    p.[dw_ek_Organisation_producerende]
INTO {PROCEDURE_GRUPPE_MEDLEM_STG}
FROM #ccp p
INNER JOIN {PROCEDURE_GRUPPE_STG} g ON g.[clean_code_DRGKontakt] = p.[clean_code_DRGKontakt];
CREATE UNIQUE CLUSTERED INDEX [IX_stg_proc_gruppe_medlem]
    ON {PROCEDURE_GRUPPE_MEDLEM_STG}(
        [clean_code_DRGProcedureGruppe], [clean_code_DRGProcedure], [dw_ek_Organisation_producerende]);
"""
    )
