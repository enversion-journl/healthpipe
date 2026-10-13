# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SQL CleanCode expressions for the ILS sgh/lgr/rnr keys.

Both Lager and Rekvirent are hierarchies of the same shape, and the maps hold
members at two levels:

* ``SGH_{sgh}`` — the hospital. ``map.Rekvirent`` holds eight of these, and
  ``map.Lager`` holds the matching sygehus concepts under ``REGION_SYD``.
* ``SGH_{sgh}_LGR_{lgr}`` / ``SGH_{sgh}_RNR_{rnr}`` — the warehouse or the
  requisitioner below it.

Keys are built **raw**: the codesystems mint them strip-only from the archive
values, and ``stg_dim_Lager``/``stg_dim_Rekvirent`` copy the concept code
through unchanged.

Hence the three rules encoded below:

1. Normalisation is trim-only, because that is all the minting side does
   (``_safe_strip`` in ``CodeSystem/lager/lager.py``, ``str(...).strip()`` in
   ``CodeSystem/rekvirent/rekvirent.py``). See ``sql_norm_code``.
2. A code that is empty or whitespace-only is **not stated**, so it collapses
   to NULL rather than becoming part of a key.
3. When the child code is not stated but the hospital is, the key resolves to
   the hospital-level member — more informative than the ``-1`` sentinel, and
   still distinguishable downstream via the dimension's level/entityType.
"""


def sql_norm_code(expr: str) -> str:
    """Trim only — exactly as the codesystems mint the segment.

    **Do not add case-folding or space-removal here.** A key segment is only
    reachable if it is spelled the way the map minted it, and the minting side
    does nothing but ``str(value).strip()``. Anything stricter fabricates a key
    that ``map.Lager``/``map.Rekvirent`` cannot hold:

    * ``REPLACE(v, ' ', '')`` made the genuine ``map.Rekvirent`` member
      ``SGH_6000_RNR_SLB TELE`` unreachable — the fact side looked up
      ``SGH_6000_RNR_SLBTELE``, which no map holds.
    * ``UPPER(v)`` is merely redundant: the databases collate
      ``Danish_Norwegian_CI_AS``, so ``=`` already ignores case. Redundant, but
      it hid the point that this expression must not reshape the value at all.

    Empty and whitespace-only values collapse to NULL — "not stated" must not
    become a key segment.

    Note that T-SQL ``LTRIM``/``RTRIM`` strip only U+0020, whereas Python's
    ``.strip()`` also strips tabs and newlines. A segment padded with a tab
    would therefore still diverge — measured against the dev archive, no
    sgh/lgr/rnr value in ``arc.ils_udlev``/``ils_rekv``/``ils_lgr`` contains a
    tab, CR or LF, so the gap is currently theoretical.
    """
    stripped = f"LTRIM(RTRIM(CAST({expr} AS NVARCHAR(50))))"
    return f"NULLIF({stripped}, '')"


def _sql_sgh_child_clean_code(sgh: str, child: str, child_prefix: str) -> str:
    """``SGH_{sgh}[_{prefix}_{child}]`` — raw, hospital-level fallback, else NULL."""
    sgh_n = sql_norm_code(sgh)
    child_n = sql_norm_code(child)
    return (
        "CASE\n"
        f"    WHEN {sgh_n} IS NOT NULL AND {child_n} IS NOT NULL\n"
        f"    THEN 'SGH_' + {sgh_n} + '_{child_prefix}_' + {child_n}\n"
        f"    WHEN {sgh_n} IS NOT NULL\n"
        f"    THEN 'SGH_' + {sgh_n}\n"
        "    ELSE NULL\n"
        "END"
    )


def sql_clean_code_lager(sgh: str = "sgh", lgr: str = "lgr") -> str:
    """``clean_code_Lager`` as ``map.Lager`` holds it."""
    return _sql_sgh_child_clean_code(sgh, lgr, "LGR")


def sql_clean_code_rekvirent(sgh: str = "sgh", rnr: str = "rnr") -> str:
    """``clean_code_Rekvirent`` as ``map.Rekvirent`` holds it."""
    return _sql_sgh_child_clean_code(sgh, rnr, "RNR")
