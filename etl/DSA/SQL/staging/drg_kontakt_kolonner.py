# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Declarative column mapping for the DRG staging chain.

Somatik and psyk deliveries share ONE staging column set because the fact
is a single table for both delivery types, mapped here as common_name -> (somatik SQL expression,
psyk SQL expression, SQL type). NULL where a delivery type has no source.
Expressions reference `k` (the arc contact table) and are consumed by the
pre_stg/stg_fct/stg_dim builders in drg_staging_sql.py.

GDPR: CPR is resolved to dw_ek_Borger inside pre_stg and NEVER lands in any
stg table. The raw CPR expression therefore lives
in a separate constant consumed only by the #borger join — it is not part of
the staged column set.

Naming: clean_code_DRGKontakt is built from (leverancetype, takstsystem,
KONTAKT_VAERDI) — never from CPR (privacy) or the delivery-internal
integer IDs (renumbered on every delivery).
"""

from dataclasses import dataclass

# Sentinels shared with the rest of the warehouse
UNKNOWN = -1  # value present but not resolvable
NOT_APPLICABLE = -2  # structurally absent (e.g. no referral on psyk)


@dataclass(frozen=True)
class StgColumn:
    """One staged column: SQL expressions per delivery type + target type."""

    name: str
    somatik: str | None  # SQL expression against alias k, or None -> NULL
    psyk: str | None
    sql_type: str
    # True for descriptive attributes that belong in stg_dim; the dim column
    # list is DERIVED from this flag so mapping and dim SQL cannot drift.
    i_dim: bool = False

    def select_for(self, leverancetype: str) -> str:
        expression = self.somatik if leverancetype == "DRG" else self.psyk
        if expression is None:
            return f"CAST(NULL AS {self.sql_type}) AS [{self.name}]"
        return f"CAST({expression} AS {self.sql_type}) AS [{self.name}]"


def clean_code_sql(leverancetype: str) -> str:
    """clean_code_DRGKontakt for one UNION branch, leverancetype as a literal.

    The delivery type MUST be part of the code: the two delivery types draw
    KONTAKT_VAERDI from the same national key space, and cross-type
    disjointness is neither measured nor enforced — without the literal, a
    somatik and a psyk contact sharing a uuid would share a dedup partition
    and one of them would be dropped silently.

    Typed VARCHAR (not NVARCHAR): every part is ASCII — the literal prefix, the
    takstsystem code and the LPR3 KONTAKT_VAERDI (a uuid) — so a single-byte
    type carries the value losslessly at half the on-disk width. The clean_code
    keys the fact/dim/map unique indexes on ~35.6M rows, so halving the key
    width halves those indexes. This CAST is the single source of the column's
    type across the whole staging chain: pre/fct/dim and the bridge scopes are
    all SELECT INTO and inherit their clean_code type from here.
    """
    return (
        f"CAST('DRGKONTAKT_{leverancetype}_' + k.[takstsystem] + '_' "
        "+ UPPER(LTRIM(RTRIM(k.[KONTAKT_VAERDI]))) AS VARCHAR(255))"
    )


# Raw CPR expression per delivery type — consumed ONLY by the #borger lookup
# join inside pre_stg; never selected into a staging table.
CPR_SQL = {"DRG": "k.[CPR]", "DRGPSYK": "k.[CPRnummer]"}

# ---------------------------------------------------------------------------
# pre_stg column set: identity, lookup inputs and raw timestamps. Everything
# downstream (stg_fct/stg_dim) reads from stg.pre_DRGKontakt only.
# ---------------------------------------------------------------------------

PRE_COLUMNS: tuple[StgColumn, ...] = (
    # Identity
    StgColumn("kontakt_uuid", "k.[KONTAKT_VAERDI]", "k.[KONTAKT_VAERDI]", "NVARCHAR(36)"),
    StgColumn("aktivitetsaar", "k.[AKTIVITETSAAR]", None, "NVARCHAR(6)", i_dim=True),
    # Forloeb key: somatik = the sygehusforloeb's ind-contact UUID resolved in
    # arc; psyk = the forloeb's first-contact UUID resolved in arc.
    StgColumn(
        "forloeb_noegle",
        "k.[sygehusforloeb_noegle]",
        "k.[forloeb_noegle]",
        "NVARCHAR(36)",
        i_dim=True,
    ),
    StgColumn("helbredsforloeb_uuid", "k.[FORLOB_VAERDI]", None, "NVARCHAR(500)"),
    # SOR lookup inputs (resolved to dw_ek_Organisation per role in stg_fct;
    # the raw code is KEPT next to the key for troubleshooting)
    StgColumn("sygehus_sor", "k.[SYGEHUS]", "k.[SYGEHUS]", "NVARCHAR(25)"),
    StgColumn("kontaktansvar_sor", "k.[DRG_KONTAKT_ENHED]", "k.[KONTAKT_ENHED]", "NVARCHAR(25)"),
    StgColumn("forloebselement_sor", "k.[DRG_FORLOBSELEMENT_ENHED]", None, "NVARCHAR(25)"),
    StgColumn("henvisende_sor", "k.[HENVISENDEINSTANS]", None, "NVARCHAR(25)"),
    # Raw timestamps (dato keys are computed in stg_fct via dato_key())
    StgColumn(
        "ind_tidspunkt", "k.[INDTIDSPUNKT_DRGKONTAKT]", "k.[INDTIDSPUNKT_KONTAKT]", "NVARCHAR(30)"
    ),
    StgColumn(
        "ud_tidspunkt", "k.[UDTIDSPUNKT_DRGKONTAKT]", "k.[UDTIDSPUNKT_KONTAKT]", "NVARCHAR(30)"
    ),
    StgColumn("henvisnings_tidspunkt", "k.[HENVISNINGSTIDSPUNKT]", None, "NVARCHAR(30)"),
    # Measures (typed in stg_fct; carried raw here)
    StgColumn("pris_drg_raa", "k.[PRIS_DRG]", None, "NVARCHAR(20)"),
    StgColumn("langliggerdage_raa", "k.[LANGLIGGERDAGE]", None, "NVARCHAR(20)"),
    StgColumn("langliggerpris_raa", "k.[LANGLIGGERPRIS]", None, "NVARCHAR(20)"),
    StgColumn("totalpris_drg_raa", "k.[TOTALPRIS_DRG]", None, "NVARCHAR(20)"),
    StgColumn("liggedage_raa", "k.[LIGGEDAGE]", None, "NVARCHAR(20)"),
    StgColumn("kontaktdage_raa", "k.[KONTAKTDAGE]", None, "NVARCHAR(20)"),
    StgColumn("varighed_time_raa", "k.[VARIGHED_TIME]", None, "NVARCHAR(20)"),
    # Flags
    StgColumn("tungeste_drgkontakt", "k.[TUNGESTE_DRGKONTAKT]", None, "NVARCHAR(6)", i_dim=True),
    StgColumn(
        "udfaldsgivende_kontakt", "k.[UDFALDSGIVENDE_KONTAKT]", None, "NVARCHAR(6)", i_dim=True
    ),
    StgColumn("kontakt_afsluttet", None, "k.[KONTAKT_AFSLUTTET]", "NVARCHAR(6)", i_dim=True),
    # Descriptive attributes for stg_dim (carried through pre so dim never
    # rejoins arc)
    StgColumn("drg_kode", "k.[DRG]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("drg_grouper", "k.[DRG_GROUPER]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("mdc", "k.[MDC]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("regel", "k.[REGEL]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("rtc", "k.[RTC]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("d90ua05", "k.[D90UA05]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("trimpunkt", "k.[TRIMPUNKT]", None, "NVARCHAR(10)", i_dim=True),
    StgColumn("kontakttype", "k.[KONTAKTTYPE]", "k.[KONTAKTTYPE]", "NVARCHAR(100)", i_dim=True),
    StgColumn("drgkontakttype", "k.[DRGKONTAKTTYPE]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("prioritet", "k.[PRIORITET]", "k.[PRIORITET]", "NVARCHAR(100)", i_dim=True),
    StgColumn("kontaktaarsag", "k.[KONTAKTAARSAG]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("afslutningsmaade", "k.[AFSLUTNINGSMAADE]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("henvisningsmaade", "k.[HENVISNINGSMAADE]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("henvisningsaarsag", "k.[HENVISNINGSAARSAG]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("speciale", "k.[SPECIALE]", "k.[SPECIALE]", "NVARCHAR(480)", i_dim=True),
    StgColumn("sygehusnavn", "k.[SYGEHUSNAVN]", "k.[SYGEHUSNAVN]", "NVARCHAR(510)", i_dim=True),
    StgColumn("sygehustype", "k.[SYGEHUSTYPE]", "k.[SYGEHUSTYPE]", "NVARCHAR(10)", i_dim=True),
    StgColumn(
        "sygehus_region", "k.[SYGEHUS_REGION]", "k.[SYGEHUS_REGION]", "NVARCHAR(20)", i_dim=True
    ),
    StgColumn(
        "kontaktansvar_enhedsnavn",
        "k.[DRG_KONTAKT_ENHEDSNAVN]",
        "k.[KONTAKT_ENHEDSNAVN]",
        "NVARCHAR(510)",
        i_dim=True,
    ),
    StgColumn("betalingsaftale", "k.[BETALINGSAFTALE]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn("betaler", "k.[BETALER]", None, "NVARCHAR(100)", i_dim=True),
    StgColumn(
        "specialiseringsniveau", "k.[SPECIALISERINGSNIVEAU]", None, "NVARCHAR(100)", i_dim=True
    ),
    # Demographics — the delivery's own values are authoritative for the
    # settlement; dim.Borger's address history is NOT a substitute
    StgColumn("bo_kom", "k.[BO_KOM]", "k.[BO_KOM]", "NVARCHAR(20)", i_dim=True),
    StgColumn("bo_region", "k.[BO_REGION]", "k.[BO_REGION]", "NVARCHAR(20)", i_dim=True),
    StgColumn("bo_kom_kilde", "k.[BO_KOM_SOURCE]", "k.[BO_KOM_SOURCE]", "NVARCHAR(10)", i_dim=True),
    StgColumn("betaler_kom", "k.[BETALER_KOM]", None, "NVARCHAR(20)", i_dim=True),
    StgColumn("betaler_region", "k.[BETALER_REGION]", None, "NVARCHAR(20)", i_dim=True),
    StgColumn("alder_aar", "k.[ALDER_AAR]", "k.[ALDER_AAR_IND]", "NVARCHAR(11)", i_dim=True),
    StgColumn("koen", "k.[KOEN]", "k.[KOEN]", "NVARCHAR(1)", i_dim=True),
)

PRE_COLUMN_NAMES: tuple[str, ...] = tuple(c.name for c in PRE_COLUMNS)
DIM_ATTRIBUTE_NAMES: tuple[str, ...] = tuple(c.name for c in PRE_COLUMNS if c.i_dim)

# ---------------------------------------------------------------------------
# Classification dimension: the primary analysis dimension. Key = the full
# attribute combination (leverancetype + takstsystem + the attributes below);
# low-frequency and free-text attributes stay on the transaction dimension.
# ---------------------------------------------------------------------------

KLASSIFIKATION_ATTRIBUTES: tuple[str, ...] = (
    "drg_kode",
    "mdc",
    "drg_grouper",
    "regel",
    "rtc",
    "d90ua05",
    "drgkontakttype",
    "kontakttype",
    "prioritet",
    "kontaktaarsag",
    "afslutningsmaade",
    "henvisningsmaade",
    "henvisningsaarsag",
    "betalingsaftale",
    "betaler",
    "specialiseringsniveau",
    "sygehustype",
)


def klassifikation_clean_code_sql(alias: str) -> str:
    """clean_code_DRGKontaktKlassifikation over the full attribute combination.

    The code is a SHA2_256 over (leverancetype, takstsystem, attributes): a
    readable concatenation of 19 values can exceed SQL Server's 900-byte
    clustered-key limit, and the dimension row carries every attribute in
    readable form anyway. Each value is length-prefixed before hashing, so
    the concatenation is injective — without the prefix, ('AB','C') and
    ('A','BC') would hash identically. Values are normalised like other
    clean_codes (UPPER + trim); NULL and empty collapse to the same
    combination by design.
    """
    parts = []
    for col in ("leverancetype", "takstsystem", *KLASSIFIKATION_ATTRIBUTES):
        norm = f"ISNULL(UPPER(LTRIM(RTRIM({alias}.[{col}]))), N'')"
        parts.append(f"CONCAT(LEN({norm}), N':', {norm}, N'|')")
    combined = f"CONCAT({', '.join(parts)})"
    return f"'DRGKLASS_' + CONVERT(CHAR(64), HASHBYTES('SHA2_256', {combined}), 2)"


# SOR roles: staged raw-code column -> resolved key column.
SOR_ROLES: tuple[tuple[str, str], ...] = (
    ("sygehus_sor", "dw_ek_Organisation_sygehus"),
    ("kontaktansvar_sor", "dw_ek_Organisation_kontaktansvar"),
    ("forloebselement_sor", "dw_ek_Organisation_forloebselement"),
    ("henvisende_sor", "dw_ek_Organisation_henvisende"),
)

# Forloeb measures laid on the CARRYING contact so a forloeb is counted
# exactly once. Somatik: the contact flagged TUNGESTE_DRGKONTAKT='1'.
# Psyk: the forloeb's first contact (its UUID equals foerste_kontakt_uuid) —
# the psyk schema has no tungeste flag; see drg_noegler_beslutningsnotat.md.
FORLOEB_MEASURES_SOMATIK: tuple[tuple[str, str], ...] = (
    ("forloeb_totalpris", "f.[TOTALPRIS_DRGSGHF]"),
    ("forloeb_pris_drg", "f.[PRIS_DRG_DRGSGHF]"),
    ("forloeb_langliggerpris", "f.[LANGLIGGERPRIS_DRGSGHF]"),
    ("forloeb_loensumafgift_pris", "f.[LONSUMAFGIFT_PRIS_DRGSGHF]"),
    ("forloeb_loensumafgift_langliggerpris", "f.[LONSUMAFGIFT_LANGLIGGERPRIS]"),
    ("forloeb_antal_kontakter", "f.[ANTAL_I_FORLOB]"),
)
FORLOEB_FLAGS_SOMATIK: tuple[tuple[str, str], ...] = (
    ("forloeb_mrk_naerhed_1", "f.[MRK_NAERHED_1]"),
    ("forloeb_mrk_naerhed_2", "f.[MRK_NAERHED_2]"),
    ("forloeb_mrk_naerhed_4", "f.[MRK_NAERHED_4]"),
    ("forloeb_mrk_kirurgi", "f.[MRK_Kirurgi]"),
)
# Psyk expressions reference alias fp (the psyk forloeb join in stg_fct).
# The _psyk suffix is used only where a somatik measure carries the same
# name — the two stay separate columns in the shared column set.
FORLOEB_MEASURES_PSYK: tuple[tuple[str, str], ...] = (
    ("forloeb_pris_psykiatriforloeb", "fp.[PRIS_PsykiatriForloeb]"),
    ("forloeb_dtd_justeret_takst", "fp.[DtdJusteretTakst]"),
    ("forloeb_loensumafgift_pris_psyk", "fp.[LONSUMAFGIFT_PRIS_Forloeb]"),
    ("forloeb_liggedage", "fp.[LIGGEDAGE_Forloeb]"),
    ("forloeb_antal_kontakter_psyk", "fp.[ANTAL_I_FORLOEB]"),
)
