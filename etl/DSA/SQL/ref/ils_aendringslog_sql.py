# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL for ref.ILSAendringslog — ILS dbo.changelog as-is.

One row per change event (prrowid), all tabelnavne in a single table. noegle is
the raw primaernoegle: no FK resolution, because its meaning depends on
tabelnavn (and for some, on feltnavn/feltlabel too).

The log is append-only, so the MERGE never deletes; a matched row is only
rewritten when a value actually changed.
"""

REF_TABLE = "[ref].[ILSAendringslog]"

# klok is seconds after midnight. Values outside one day would wrap silently in
# DATEADD on a TIME, so they become NULL; INVALID_KLOK counts them per run.
KLOK_IN_DAY = "a.[klok] BETWEEN 0 AND 86399"
INVALID_KLOK = f"a.[klok] IS NOT NULL AND NOT ({KLOK_IN_DAY})"

# hh:mm:ss, so two changes of one field within the same minute stay ordered.
TIDSPUNKT_EXPR = f"""
CASE
    WHEN {KLOK_IN_DAY}
        THEN CONVERT(CHAR(8), DATEADD(SECOND, a.[klok], CAST('00:00' AS TIME(0))), 108)
END
""".strip()

SOURCE_COLUMNS: dict[str, str] = {
    "prrowid": "a.[prrowid]",
    "tabelnavn": "LOWER(LTRIM(RTRIM(a.[tabelnavn])))",
    "noegle": "UPPER(LTRIM(RTRIM(a.[primaernoegle])))",
    "feltnavn": "a.[feltnavn]",
    "feltlabel": "a.[feltlabel]",
    "fra": "a.[fravaerdi]",
    "til": "a.[tilvaerdi]",
    "opret": "a.[opret]",
    "ajourfoert_af": "UPPER(LTRIM(RTRIM(a.[bd_init])))",
    "dato": "CAST(a.[Dato] AS DATE)",
    "tidspunkt": TIDSPUNKT_EXPR,
}

_NON_KEY = [c for c in SOURCE_COLUMNS if c != "prrowid"]


def _scope_filter(full_scope: bool) -> str:
    """Arc rows in this run's scope; delta binds one checkpoint parameter."""
    return "" if full_scope else "a.[dw_timestamp_update] > CAST(? AS DATETIME2(0))"


def build_invalid_klok_count_sql(*, full_scope: bool) -> str:
    """Count in-scope arc rows whose klok lands as a NULL tidspunkt."""
    scope = _scope_filter(full_scope)
    return f"SELECT COUNT_BIG(*) FROM [arc].[ils_changelog] a WHERE {INVALID_KLOK}" + (
        f" AND {scope}" if scope else ""
    )


def build_merge_sql(*, full_scope: bool) -> str:
    """MERGE arc.ils_changelog into ref.ILSAendringslog.

    Delta scope takes one bound parameter: the arc dw_timestamp_update
    checkpoint. Full scope (full_load / first run) reads the whole archive.
    """
    scope = _scope_filter(full_scope)
    where = f"WHERE {scope}" if scope else ""
    select_list = ",\n        ".join(f"{expr} AS [{col}]" for col, expr in SOURCE_COLUMNS.items())
    src_cols = ", ".join(f"src.[{c}]" for c in _NON_KEY)
    tgt_cols = ", ".join(f"tgt.[{c}]" for c in _NON_KEY)
    update_set = ",\n        ".join(f"tgt.[{c}] = src.[{c}]" for c in _NON_KEY)
    insert_cols = ", ".join(f"[{c}]" for c in SOURCE_COLUMNS)
    insert_vals = ", ".join(f"src.[{c}]" for c in SOURCE_COLUMNS)
    now = "CAST(SYSDATETIME() AS datetime2(0))"

    return f"""
MERGE {REF_TABLE} WITH (HOLDLOCK) AS tgt
USING (
    SELECT
        {select_list}
    FROM [arc].[ils_changelog] a
    {where}
) AS src
    ON tgt.[prrowid] = src.[prrowid]
WHEN MATCHED AND EXISTS (
    SELECT {src_cols}
    EXCEPT
    SELECT {tgt_cols}
) THEN UPDATE SET
        {update_set},
        tgt.[dw_timestamp_update] = {now}
WHEN NOT MATCHED BY TARGET THEN
    INSERT ({insert_cols}, [dw_timestamp_insert], [dw_timestamp_update])
    VALUES ({insert_vals}, {now}, {now});
""".strip()
