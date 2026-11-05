# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""The complete demographic tuple and the hash stored directly on Borger."""

from DSA.SQL.common.hash_clean_code import build_attribute_hash_sql

ATTRIBUTES = (
    "region_id",
    "regionnavn",
    "kommune_id",
    "kommunenavn",
    "postnr",
    "postdistrikt",
    "sogn_id",
    "sognenavn",
    "koen_navn",
    "foedselsaar",
    "foedselsmaaned",
)


def key_sql() -> str:
    """Hash known birth cohorts and retain established keys for an unknown cohort."""
    legacy = build_attribute_hash_sql("demografi", ATTRIBUTES[:-2]).replace("demografi.", "")
    complete = build_attribute_hash_sql("demografi", ATTRIBUTES).replace("demografi.", "")
    return (
        "CASE WHEN [foedselsaar] IS NULL AND [foedselsmaaned] IS NULL "
        f"THEN {legacy} ELSE {complete} END"
    )


def borger_key_sql() -> str:
    """Hash the demographic tuple using only the year and month of birth."""
    return (
        key_sql()
        .replace("[foedselsaar]", "CONVERT(NVARCHAR(4), YEAR([foedselsdato]))")
        .replace("[foedselsmaaned]", "CONVERT(NVARCHAR(2), MONTH([foedselsdato]))")
    )


def borger_attribute_sql(column: str, alias: str = "b") -> str:
    """Select a demographic attribute without exposing the full birth date."""
    if column == "foedselsaar":
        return f"CAST(YEAR({alias}.[foedselsdato]) AS SMALLINT)"
    if column == "foedselsmaaned":
        return f"CAST(MONTH({alias}.[foedselsdato]) AS TINYINT)"
    return f"NULLIF(UPPER(LTRIM(RTRIM({alias}.[{column}]))), N'')"
