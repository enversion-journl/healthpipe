# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SQL CleanCode expressions for the ILS supplier (``dbo.lev``).

``clean_code_ILSLeverandoer = 'ILSLEV_' + UPPER(TRIM(lnr))`` is minted by
``stg_dim_ILSLeverandoer`` and seeds ``map.ILSLeverandoer``. A lookup against
that map must build its key with ``sql_clean_code_ils_leverandoer``. A blank
``lnr`` yields NULL, never the bare prefix.
"""

from utils.sql.tax_id_sql import sql_cvr_cleancode_from_se_nr

ILS_LEVERANDOER_PREFIX = "ILSLEV_"


def sql_clean_code_ils_leverandoer(lnr: str = "lnr") -> str:
    """``clean_code_ILSLeverandoer`` as ``map.ILSLeverandoer`` holds it."""
    norm = f"UPPER(LTRIM(RTRIM(CAST({lnr} AS NVARCHAR(100)))))"
    return (
        "CASE\n"
        f"    WHEN NULLIF({norm}, '') IS NOT NULL\n"
        f"    THEN '{ILS_LEVERANDOER_PREFIX}' + {norm}\n"
        "    ELSE NULL\n"
        "END"
    )


def sql_kreditor_clean_code_from_lnr(lnr: str = "lnr", land: str = "land") -> str:
    """Candidate ``clean_code_Kreditor`` from the supplier number of a DK supplier.

    For Danish suppliers the ILS supplier number is as a rule the CVR number:
    exactly eight digits without a leading zero, with ``land = 'DK'``. The
    result is a candidate only; the caller accepts it when ``map.Kreditor``
    holds that CVR. The key is spelled by ``sql_cvr_cleancode_from_se_nr``, the
    formula ``map.Kreditor`` is minted with for ILS.
    """
    trimmed = f"LTRIM(RTRIM(CAST({lnr} AS NVARCHAR(100))))"
    return (
        "CASE\n"
        f"    WHEN {trimmed} LIKE '[1-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]'\n"
        f"     AND LTRIM(RTRIM({land})) = 'DK'\n"
        f"    THEN {sql_cvr_cleancode_from_se_nr(lnr)}\n"
        "    ELSE NULL\n"
        "END"
    )
