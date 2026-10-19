# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Declarative single source of truth for the udredningsret staging chain.

Table names, clean_code builders, the marker groups of SDS' appendix 1, the
attribute lists that the transaction/classification dimensions are derived
from, SOR roles, the classification texts and the downstream checkpoint
owners — everything the SQL builders (udredningsret_staging_sql.py), the
assets, the map/dim/fct loaders and the DDL tests need to agree on lives
here ONCE. Authoritative spec: docs/source_systems/udredningsret/
udredningsret_model.md (sections 2, 3, 4.2 and 7; decisions U2-U10).

Conventions (as drg_kontakt_kolonner.py):

* All clean_codes are ASCII and typed VARCHAR (spec section 2) — readable
  concatenation of the arc business keys wherever it stays well under 255
  characters; only the classification is hashed (SHA2_256, length-prefixed
  like DRG's klassifikation_clean_code_sql). CPR NEVER enters a clean_code:
  the helbredsforloeb key carries it hashed only.
* Identifiers are Danish with ae/oe/aa (never non-ASCII); prose is English.
* Every builder is a pure function of SQL expression strings, so the
  rendered SQL is unit-testable without a database.
"""

from DSA.SQL.common.hash_clean_code import build_attribute_hash_sql
from DSI.csv_files.udredningsret_datasets import HENVIST_MARKOERER

# ---------------------------------------------------------------------------
# Staging tables (spec section 1)
# ---------------------------------------------------------------------------

PRE_HF_TABLE = "[stg].[pre_Helbredsforloeb]"
PRE_FORLOEB_TABLE = "[stg].[pre_Udredningsforloeb]"
FCT_FORLOEB_TABLE = "[stg].[fct_Udredningsforloeb]"
DIM_EGENSKABER_TABLE = "[stg].[dim_UdredningsretEgenskaber]"
FCT_MARKOER_TABLE = "[stg].[fct_UdredningsretMarkoer]"
DIM_FORLOEBSMARKOER_TABLE = "[stg].[dim_Forloebsmarkoer]"

# ---------------------------------------------------------------------------
# Warehouse tables (spec section 4.2). The map/dim/fct loaders read ONLY
# stg.*, map.*, codesystem.* and arc.* (the latter for the fact's control
# total) — never ext.*. The _DWH_ infix keeps them apart from the staging
# tables above, which carry the same entity names with a stg_ prefix.
# ---------------------------------------------------------------------------

MAP_HF_TABLE = "[map].[Helbredsforloeb]"
MAP_FORLOEB_TABLE = "[map].[Udredningsforloeb]"
MAP_KLASSIFIKATION_TABLE = "[map].[UdredningsretKlassifikation]"
MAP_FORLOEBSMARKOER_TABLE = "[map].[Forloebsmarkoer]"
MAP_MARKOER_TABLE = "[map].[UdredningsretMarkoer]"

DIM_KLASSIFIKATION_DWH_TABLE = "[dim].[UdredningsretKlassifikation]"
DIM_EGENSKABER_DWH_TABLE = "[dim].[UdredningsretEgenskaber]"
DIM_FORLOEBSMARKOER_DWH_TABLE = "[dim].[Forloebsmarkoer]"
FCT_FORLOEB_DWH_TABLE = "[fct].[Udredningsforloeb]"
FCT_MARKOER_DWH_TABLE = "[fct].[UdredningsretMarkoer]"

# Sentinels shared with the rest of the warehouse
UNKNOWN_BORGER = -1  # CPR present but not in dim.Borger (yet)
NA_BORGER = -2  # CPR blank
UNKNOWN_ORGANISATION = -1  # SOR code blank or without dim.Organisation match

# ---------------------------------------------------------------------------
# clean_code prefixes and builders (spec section 2)
# ---------------------------------------------------------------------------

HF_PREFIX = "UDREDHF_"
FORLOEB_PREFIX = "UDREDFORL_"
MARKOER_PREFIX = "UDREDMRK_"
KLASSIFIKATION_PREFIX = "UDREDKLASS_"
FORLOEBSMARKOER_PREFIX = "FORLOEBSMARKOER_"

HF_CLEAN_CODE_TYPE = "VARCHAR(80)"  # 8 + 64 = 72 characters
FORLOEB_CLEAN_CODE_TYPE = "VARCHAR(100)"  # 10 + 64 + 1 + 8 + 1 + loebenummer <= 90
MARKOER_CLEAN_CODE_TYPE = "VARCHAR(100)"  # 9 + 64 + 1 + loebenummer <= 80
KLASSIFIKATION_CLEAN_CODE_TYPE = "NVARCHAR(100)"  # as DRG's map/dim DDL
FORLOEBSMARKOER_CLEAN_CODE_TYPE = "VARCHAR(50)"  # 16 + code


def clean_code_helbredsforloeb_sql(noegle_col: str) -> str:
    """clean_code_Helbredsforloeb = 'UDREDHF_' + helbredsforloeb_noegle.

    The arc key is a CHAR(64) SHA2_256 hex digest (CPR hashed inside it), so
    the concatenation is always exactly 72 ASCII characters.
    """
    return f"CAST('{HF_PREFIX}' + {noegle_col} AS {HF_CLEAN_CODE_TYPE})"


def clean_code_udredningsforloeb_sql(
    noegle_col: str, startdato_col: str, loebenummer_col: str
) -> str:
    """clean_code_Udredningsforloeb = 'UDREDFORL_' + noegle + '_' + yyyymmdd + '_' + loebenummer.

    startdato is the ISO date of the start marker (spec 3.3); the hyphens are
    removed so the code stays ASCII-only and readable. ISNULL keeps the code
    non-NULL for a forloeb whose start marker has a blank date (measured 0),
    so the unique clustered index on the staging table still holds the grain.
    loebenummer is the persisted per-(helbredsforloeb, startdato) sequence of spec
    section 2 — never SDS' F_ID/PK, which are renumbered per delivery.
    """
    return (
        f"CAST('{FORLOEB_PREFIX}' + {noegle_col} + '_' "
        f"+ REPLACE(ISNULL({startdato_col}, ''), '-', '') + '_' "
        f"+ CAST({loebenummer_col} AS VARCHAR(10)) AS {FORLOEB_CLEAN_CODE_TYPE})"
    )


def clean_code_markoer_sql(noegle_col: str, loebenummer_col: str) -> str:
    """clean_code_UdredningsretMarkoer = 'UDREDMRK_' + markoer_noegle + '_' + arc.loebenummer.

    Exactly arc's merge key (markoer_noegle, loebenummer), so the fact row and
    the archive row are 1:1 and a soft-delete in arc retires the same fact row.
    """
    return (
        f"CAST('{MARKOER_PREFIX}' + {noegle_col} + '_' "
        f"+ CAST({loebenummer_col} AS VARCHAR(10)) AS {MARKOER_CLEAN_CODE_TYPE})"
    )


def clean_code_forloebsmarkoer_sql(kode_col: str) -> str:
    """clean_code_Forloebsmarkoer = 'FORLOEBSMARKOER_' + UPPER(TRIM(FORLOEBSMARKOER))."""
    return (
        f"CAST('{FORLOEBSMARKOER_PREFIX}' + UPPER(LTRIM(RTRIM({kode_col}))) "
        f"AS {FORLOEBSMARKOER_CLEAN_CODE_TYPE})"
    )


# ---------------------------------------------------------------------------
# Marker groups (SDS appendix 1; spec section 3.3, decisions U2-U4)
# ---------------------------------------------------------------------------

# "Referred to assessment": the four codes of the datamart description plus the
# cancer-package referral pattern AFB??A. Reused from the dataset module so the
# extract, the archive and staging never disagree on the domain.
HENVIST_KODER: tuple[str, ...] = HENVIST_MARKOERER
HENVIST_LIKE = "AFB[0-9][0-9]A"

OEVRIGE_GRUPPE = "oevrige"
OEVRIGE_GRUPPE_TEKST = "Øvrige"


def _in_list(koder: tuple[str, ...]) -> str:
    return "(" + ", ".join(f"'{k}'" for k in koder) + ")"


# Ordered: the FIRST matching predicate wins in markoer_gruppe_sql, anything
# else is OEVRIGE_GRUPPE. Each predicate template is rendered with {col} = the
# (trimmed) marker-code expression. Same rules as the profiling script's
# MARKER_GROUPS, so the staged counts are comparable with the profiling report.
MARKOER_GRUPPER: tuple[tuple[str, str, str], ...] = (
    (
        "henvist_udredning",
        "Henvist til udredning",
        f"{{col}} IN {_in_list(HENVIST_KODER)} OR {{col}} LIKE '{HENVIST_LIKE}'",
    ),
    ("udredning_start", "Udredning start", "{col} = 'AFV01B'"),
    ("endeligt_udredt", "Endeligt udredt", "{col} LIKE 'AFX01A%' OR {col} = 'AFX01C'"),
    (
        "udredningsplan",
        "Udredningsplan",
        "{col} LIKE 'AFX01D1%' OR {col} IN ('AFX01D2', 'AFX01D3', 'AFX01D4', 'AFX01D9')",
    ),
    ("afbrudt", "Afbrudt", "{col} IN ('AFV01X1', 'AFV01X2', 'AFV01X4')"),
    (
        "kraeftpakke_slut_beslutning",
        "Kræftpakke slut/beslutning",
        "{col} IN ('AFA01X2', 'AFA01X9', 'AFD01X2', 'AFD01X9', 'AFD01D') "
        "OR {col} LIKE 'AFB[0-9][0-9]C%' OR {col} LIKE 'AFB[0-9][0-9]X[29]'",
    ),
    ("behandling_start", "Behandling start", "{col} = 'AFV01F'"),
)

MARKOER_GRUPPE_TEKSTER: dict[str, str] = {g: t for g, t, _ in MARKOER_GRUPPER} | {
    OEVRIGE_GRUPPE: OEVRIGE_GRUPPE_TEKST
}


def henvist_predicate_sql(col_sql: str) -> str:
    """Parenthesised predicate: the marker code is a referral (henvist_udredning)."""
    _, _, template = MARKOER_GRUPPER[0]
    return "(" + template.format(col=col_sql) + ")"


def markoer_gruppe_sql(col_sql: str) -> str:
    """ONE CASE expression mapping a marker code to its group (first match wins)."""
    whens = "\n".join(
        f"    WHEN {template.format(col=col_sql)} THEN '{gruppe}'"
        for gruppe, _, template in MARKOER_GRUPPER
    )
    return f"CASE\n{whens}\n    ELSE '{OEVRIGE_GRUPPE}' END"


def markoer_gruppe_tekst_sql(col_sql: str) -> str:
    """Companion CASE expression: marker code -> Danish group text (N'' literals)."""
    whens = "\n".join(
        f"    WHEN {template.format(col=col_sql)} THEN N'{tekst}'"
        for _, tekst, template in MARKOER_GRUPPER
    )
    return f"CASE\n{whens}\n    ELSE N'{OEVRIGE_GRUPPE_TEKST}' END"


# ---------------------------------------------------------------------------
# Attribute marker columns (spec section 3.3, decision U3): read from the end
# marker, else the forloeb's latest marker. (pre_stg column, arc source
# column, staged SQL type) — the SQL builder derives its SELECT list, the
# NULLIF-trim rule and the CASTs from this tuple.
# ---------------------------------------------------------------------------

FORLOEB_ATTRIBUTTER: tuple[tuple[str, str, str], ...] = (
    ("aktionsdiagnose_kode", "C_ADIAG", "NVARCHAR(20)"),
    ("diagnosekontakt_uuid", "C_ADIAG_KONTAKT", "NVARCHAR(36)"),
    ("kontakt_enhed_sor", "ANSVARLIG_ENHED_KONTAKT", "NVARCHAR(25)"),
    ("forloeb_enhed_sor", "ANSVARLIG_ENHED_FORLOB", "NVARCHAR(25)"),
    ("speciale", "Speciale_ansvarlig_enhed_kontakt", "NVARCHAR(100)"),
    ("alder_raa", "ALDER", "NVARCHAR(10)"),
    ("bopaelsregion_forloeb", "C_BOREGION_F", "NVARCHAR(10)"),
    ("bopaelsregion", "C_BOREGION", "NVARCHAR(10)"),
    ("sygehusregion", "C_SGHREGION", "NVARCHAR(10)"),
    ("psykiatri", "PSYKIATRI", "NVARCHAR(5)"),
    ("hjerteomraade", "MRK_C", "NVARCHAR(5)"),
    ("udredningstid_raa", "H", "NVARCHAR(10)"),
    ("inden_30_dage", "MRK_ST_H", "NVARCHAR(5)"),
    ("overholdt", "MRK_ST_L", "NVARCHAR(5)"),
    ("aarsag", "MRK_ST_N", "NVARCHAR(5)"),
    ("mrk_l_n", "MRK_L_N", "NVARCHAR(5)"),
    ("opgoerelseskvartal", "OPGPER", "NVARCHAR(10)"),
    ("opgoerelsesmaaned", "OPGMAANED", "NVARCHAR(10)"),
    ("forloebslabel", "FORLOEBSLABEL", "NVARCHAR(20)"),
)
FORLOEB_ATTRIBUT_NAVNE: tuple[str, ...] = tuple(n for n, _, _ in FORLOEB_ATTRIBUTTER)

# ---------------------------------------------------------------------------
# Transaction dimension stg.dim_UdredningsretEgenskaber (spec section 3.6):
# the dim SQL and the DDL test derive their column list from this ONE tuple.
# ---------------------------------------------------------------------------

EGENSKABER_ATTRIBUTTER: tuple[str, ...] = (
    "f_id",
    "pk",
    "start_markoer_kode",
    "start_markoer_uuid",
    "slut_markoer_kode",
    "slut_markoer_uuid",
    "seneste_markoer_dato",
    "foerste_forloebselement_uuid",
    "helbredsforloeb_id",
    "forloeb_enhed_sor",
    "kontakt_enhed_sor",
    "aktionsdiagnose_kode",
    "diagnosekontakt_uuid",
    "opgoerelseskvartal",
    "opgoerelsesmaaned",
    "sygehusregion",
    "bopaelsregion",
    "speciale",
    "forloebslabel",
    "mrk_l_n",
)

# ---------------------------------------------------------------------------
# Classification dimension (spec section 3.6): the eight indicator attributes,
# 2,948 measured combinations. Key = length-prefixed SHA2_256 as DRG.
# ---------------------------------------------------------------------------

KLASSIFIKATION_ATTRIBUTTER: tuple[str, ...] = (
    "overholdt",
    "aarsag",
    "inden_30_dage",
    "psykiatri",
    "hjerteomraade",
    "forloebslabel",
    "bopaelsregion_forloeb",
    "speciale",
)

# Every classification attribute EXCEPT speciale carries a rendered Danish
# text column beside its raw value in dim.UdredningsretKlassifikation
# (speciale is free text from the delivery and explains itself). The
# dimension's column tuple and the DDL test derive the <attribut> /
# <attribut>_tekst pairs from this ONE tuple.
KLASSIFIKATION_TEKST_KOLONNER: tuple[str, ...] = tuple(
    attribut for attribut in KLASSIFIKATION_ATTRIBUTTER if attribut != "speciale"
)


def klassifikation_clean_code_sql(alias: str) -> str:
    """clean_code_UdredningsretKlassifikation over the eight attributes.

    Same builder as drg_kontakt_kolonner.klassifikation_clean_code_sql: the
    values are normalised (UPPER + trim, NULL and blank collapse) and
    length-prefixed, so the concatenation is injective. The dimension row
    carries the attributes in readable form; the hash only keys it. Cast to
    NVARCHAR(100) so the staged column matches the map/dim DDL.
    """
    return (
        f"CAST('{KLASSIFIKATION_PREFIX}' "
        f"+ {build_attribute_hash_sql(alias, KLASSIFIKATION_ATTRIBUTTER)} "
        f"AS {KLASSIFIKATION_CLEAN_CODE_TYPE})"
    )


# ---------------------------------------------------------------------------
# SOR roles on the forloeb fact (spec section 5, decision B8): staged raw-code
# column -> resolved key column. Both resolved through #organisation_sor; blank and
# unmatched key to -1, distinguished only in the reported coverage.
# ---------------------------------------------------------------------------

SOR_ROLLER: tuple[tuple[str, str], ...] = (
    ("forloeb_enhed_sor", "dw_ek_Organisation_forloeb"),
    ("kontakt_enhed_sor", "dw_ek_Organisation_kontakt"),
)

# The markoer fact resolves only the forloeb unit (spec section 3.5).
MARKOER_SOR_ROLLER: tuple[tuple[str, str], ...] = (SOR_ROLLER[0],)

# ---------------------------------------------------------------------------
# Classification texts (spec sections 4.2 and 7). Keys are the RAW indicator
# values as delivered; the empty-string key is the blank/NULL case (pre_stg
# stages blank as NULL, so consumers look up ISNULL(value, '')). Any value
# without a key is a data surprise, not an error — consumers fall back to the
# raw value itself (regions) or 'Ukendt'.
# ---------------------------------------------------------------------------

# The fallback text for a value the dicts below have no key for, and the text
# on the -1 sentinel members. REGION_UKENDT_TEKST is the same word under the
# region rule, where it covers the blank code rather than an unknown one.
UKENDT_TEKST = "Ukendt"

# MRK_ST_N (spec section 7). PROVENANCE (decision U8, provisional): these are
# OUR wording, derived from SDS' SKS texts for the AFX01D* plan codes and the
# measured co-occurrence with MRK_ST_L on the end markers (2026-09-12). They
# are to be replaced 1:1 by SDS' own wording from Datamart_udredningsret.pdf
# (SDS specification appendix) once that text is verified.
AARSAG_TEKSTER: dict[str, str] = {
    "1": "Udredt inden for 30 dage",
    "2": "Udredningsplan inden for 30 dage pga. faglige årsager",
    "3": "Udredningsplan inden for 30 dage pga. patientens afslag om udredning i anden enhed",
    "4": "Udredningsplan inden for 30 dage pga. patientens ønske om senere udredning",
    "5": "Ikke overholdt: udredningsplan pga. manglende kapacitet",
    "6": "Ikke overholdt: udredningsplan pga. andre årsager",
    "7": "Ikke overholdt: udredningsplan først efter 30 dage",
    "8": "Ikke overholdt: ingen udredningsplan",
    "0": "Uoplyst",
    "": "Ikke i opgørelsen",
}

# MRK_ST_L
OVERHOLDT_TEKSTER: dict[str, str] = {
    "1": "Overholdt",
    "0": "Ikke overholdt",
    "": "Ikke i opgørelsen",
}

# MRK_ST_H
INDEN_30_TEKSTER: dict[str, str] = {
    "1": "Inden for 30 dage",
    "0": "Over 30 dage",
    "": "Ikke i opgørelsen",
}

# PSYKIATRI
PSYKIATRI_TEKSTER: dict[str, str] = {
    "1": "Psykiatri",
    "0": "Somatik",
    "": "Ukendt",
}

# MRK_C
HJERTE_TEKSTER: dict[str, str] = {
    "1": "Hjerteområdet",
    "0": "Uden for hjerteområdet",
    "": "Ukendt",
}

# C_BOREGION_F — Statistics Denmark's region codes. Other codes -> the code
# itself, blank -> 'Ukendt' (consumer rule, spec section 4.2).
REGION_TEKSTER: dict[str, str] = {
    "1081": "Region Nordjylland",
    "1082": "Region Midtjylland",
    "1083": "Example Region",
    "1084": "Region Hovedstaden",
    "1085": "Region Sjælland",
}
REGION_UKENDT_TEKST = "Ukendt"


def _sql_tekst_literal(value: str) -> str:
    """T-SQL string body: a single quote inside the text is doubled."""
    return value.replace("'", "''")


def tekst_case_sql(col_sql: str, tekster: dict[str, str], default_sql: str) -> str:
    """ONE CASE expression mapping a raw indicator value to its Danish text.

    The lookup key is ISNULL(LTRIM(RTRIM(<col>)), ''), so NULL and blank
    collapse onto the dicts' empty-string key exactly as pre_stg stages them,
    and a padded value still finds its text. `default_sql` is the ELSE branch
    as SQL — N'Ukendt', or the column expression itself for the region rule —
    never a Python string that this function would quote.

    The texts are rendered as N'' literals (they carry Danish letters), with
    single quotes doubled, so no dict entry can break out of its literal.
    """
    whens = "\n".join(
        f"    WHEN '{_sql_tekst_literal(key)}' THEN N'{_sql_tekst_literal(tekst)}'"
        for key, tekst in tekster.items()
    )
    return f"CASE ISNULL(LTRIM(RTRIM({col_sql})), '')\n{whens}\n    ELSE {default_sql} END"


# ---------------------------------------------------------------------------
# Reporting quarter -> date (spec section 3.6)
# ---------------------------------------------------------------------------

OPGOERELSE_KVARTAL_LIKE = "[1-4]K[0-9][0-9]"


def opgoerelse_dato_sql(col_sql: str) -> str:
    """ISO 'YYYY-MM-01' of the quarter's first day for an OPGPER value 'nKyy'.

    '2K26' -> '2026-04-01' (quarter n -> month 01/04/07/10, century 20).
    Anything else — NULL, blank or an unexpected format — yields '' so the
    shared dato_key() lands on the BLANK sentinel (99999998), never on
    UNKNOWN: a missing reporting period is a state of the forloeb, not a
    parse failure. The pattern is exactly four characters, so a longer or
    shorter value never matches.
    """
    return (
        f"CASE WHEN {col_sql} LIKE '{OPGOERELSE_KVARTAL_LIKE}' "
        f"THEN '20' + SUBSTRING({col_sql}, 3, 2) + '-' "
        f"+ CASE SUBSTRING({col_sql}, 1, 1) "
        f"WHEN '1' THEN '01' WHEN '2' THEN '04' WHEN '3' THEN '07' ELSE '10' END "
        f"+ '-01' ELSE '' END"
    )


# ---------------------------------------------------------------------------
# Delta window (spec section 3.1): the pre_stg checkpoint is the MIN over these
# downstream owners, so an owner that fell behind gets its rows re-staged.
# ---------------------------------------------------------------------------

DOWNSTREAM_CHECKPOINT_EJERE: tuple[tuple[str, str], ...] = (
    ("dim", "UdredningsretEgenskaber"),
    ("dim", "UdredningsretKlassifikation"),
    ("dim", "Forloebsmarkoer"),
    ("fct", "Udredningsforloeb"),
    ("fct", "UdredningsretMarkoer"),
)
