# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""
Shared SQL fragments for supplier tax-id normalisation (MySupply ↔ KMD matching).

MySupply header.faktura_udsteder_cvr_nummer_type is observational metadata only — values
seen in production include: (null), CPR, cvr, DK:CPR, DK:cvr, DK:VAT, ean, n/a, SE,
SEN, Telefon, VAT, ZZZ. Historically, DK:VAT on the type field often indicated that
the header number was a momsnummer (VAT id); newer data tends to store moms in
metadata.leverandoer_cvr with type cvr/DK:cvr on the header instead. We do not branch
on type labels — both value columns are normalised on pre_stg / apply-pass
so legacy and current layouts both match KMD without reclassifying rows by era.

KMD SupplierCompanyTaxID and MySupply tax-id columns may hold cvr, Danish momsnummer
(VAT), or foreign ids. Observed country prefixes on the value itself include DK, SE,
and DE (two letters + digits). We strip any [A-Z]{2} prefix when the third character
is a digit so DK30049179, SE1234567890 and DE123456789 all normalise consistently.
"""

# SQL expression (trimmed input) → digits/id without a 2-letter country prefix.
STRIP_TAX_ID_COUNTRY_PREFIX = """CASE
        WHEN {trimmed} LIKE '[A-Z][A-Z][0-9]%'
        THEN SUBSTRING({trimmed}, 3, LEN({trimmed}))
        ELSE {trimmed}
    END"""


def sql_trim_column(column: str) -> str:
    return f"LTRIM(RTRIM({column}))"


def sql_strip_tax_id_country_prefix(column: str) -> str:
    """Return SQL expression: strip DK/SE/DE/… prefix from a raw tax-id column."""
    trimmed = sql_trim_column(column)
    return STRIP_TAX_ID_COUNTRY_PREFIX.format(trimmed=trimmed)


def sql_cvr_cleancode_from_column(column: str) -> str:
    """Return SQL CASE: NULL when empty, else CVR_{normalised tax id}."""
    trimmed = sql_trim_column(column)
    stripped = STRIP_TAX_ID_COUNTRY_PREFIX.format(trimmed=trimmed)
    return f"""CASE
            WHEN {column} IS NOT NULL AND {trimmed} <> ''
            THEN 'CVR_' + {stripped}
            ELSE NULL
        END"""


# The two legacy Kreditor CleanCode formulas below deliberately do NOT reuse
# STRIP_TAX_ID_COUNTRY_PREFIX: they strip ONLY a 'DK' prefix (not any
# two-letter country code), preserving the exact historical map.Kreditor key
# semantics per source. Shared by stg_dim_Kreditor and dim_Kreditor.


def sql_cvr_cleancode_from_juridisk_enhed(column: str) -> str:
    """SydAX Kreditor CleanCode: trim + DK-prefix strip, NO space removal."""
    trimmed = sql_trim_column(column)
    return f"""CASE
                WHEN {column} IS NOT NULL AND {trimmed} <> ''
                THEN 'CVR_' + CASE WHEN {trimmed} LIKE 'DK[0-9]%'
                                   THEN SUBSTRING({trimmed}, 3, LEN({trimmed}))
                                   ELSE {trimmed} END
                ELSE NULL
            END"""


def sql_cvr_cleancode_from_se_nr(column: str) -> str:
    """ILS lev CleanCode: CAST NVARCHAR(100) + trim + space removal + DK-prefix strip."""
    trimmed = f"LTRIM(RTRIM(CAST({column} AS NVARCHAR(100))))"
    packed = f"REPLACE({trimmed}, ' ', '')"
    return f"""CASE
                WHEN {column} IS NOT NULL AND {trimmed} <> ''
                THEN 'CVR_' + CASE WHEN {packed} LIKE 'DK[0-9]%' THEN SUBSTRING({packed}, 3, LEN({packed})) ELSE {packed} END
                ELSE NULL
            END"""


# ILS giro = 'Person' suppliers (home patients, employees with expenses) carry a
# birth date or CPR number in se_nr/cvr_nr, not a CVR. Every ILS chain that
# derives a clean_code_Kreditor must go through this guard so a person never
# becomes, or links to, a CVR-keyed map.Kreditor member. giro is matched
# verbatim, like the datamarts' Person masking.


def sql_unless_ils_person(candidate: str, giro: str = "lev.giro") -> str:
    """NULL for ILS giro = 'Person' rows, else the candidate expression."""
    return f"CASE WHEN {giro} = 'Person' THEN NULL ELSE {candidate} END"


def sql_ils_kreditor_cleancode(se_nr: str = "lev.se_nr", giro: str = "lev.giro") -> str:
    """ILS clean_code_Kreditor from lev.se_nr; NULL for giro = 'Person' rows."""
    return sql_unless_ils_person(sql_cvr_cleancode_from_se_nr(se_nr), giro)
