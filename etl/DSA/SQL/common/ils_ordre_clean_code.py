# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared SQL CleanCode expression for the ILS order header (``dbo.ordh``).

``clean_code_Ordre = 'ORDRE_' + sgh + '_' + lgr + '_' + ordre`` is minted by
``pre_stg_Ordre_ils`` and seeds ``map.Ordre``. The order lines (``dbo.ordvare``)
carry the same three columns, so a lookup from them must build its key with
``sql_clean_code_ordre``.

The segments are trimmed and upper-cased. A missing sgh or lgr leaves its segment
empty rather than dropping the order; a missing order number yields NULL, never
the bare prefix.
"""

from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

ORDRE_PREFIX = "ORDRE_"


def sql_clean_code_ordre(sgh: str = "sgh", lgr: str = "lgr", ordre: str = "ordre") -> str:
    """``clean_code_Ordre`` as ``map.Ordre`` holds it."""
    ordre_n = sql_norm_code(ordre)
    return (
        "CASE\n"
        f"    WHEN {ordre_n} IS NOT NULL\n"
        f"    THEN '{ORDRE_PREFIX}' + UPPER(ISNULL({sql_norm_code(sgh)}, ''))"
        f" + '_' + UPPER(ISNULL({sql_norm_code(lgr)}, '')) + '_' + {ordre_n}\n"
        "    ELSE NULL\n"
        "END"
    )
