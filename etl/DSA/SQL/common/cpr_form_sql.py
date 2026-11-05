# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL expression that masks values shaped like a Danish CPR number.

Some free-text code columns hold a CPR number where a user id belongs (ILS
``ordh.bd_oprid`` did in 2008-2013). A value is masked when it has the CPR form:
``DDMMYYNNNN`` or ``DDMMYY-NNNN`` with a day of 01-31 and a month of 01-12. The
modulus 11 check is not required, because CPR numbers issued since 2007 need not
pass it. Any other value is returned unchanged.
"""

CPR_FORM_MARKER = "[CPR-FORM]"

_DIGITS6 = "[0-3][0-9][0-1][0-9][0-9][0-9]"


def sql_mask_cpr_form(expr: str, marker: str = CPR_FORM_MARKER) -> str:
    """``expr``, or ``marker`` when ``expr`` has the CPR form (single-line SQL)."""
    is_cpr_form = (
        f"(({expr} LIKE '{_DIGITS6}[0-9][0-9][0-9][0-9]' AND LEN({expr}) = 10)"
        f" OR ({expr} LIKE '{_DIGITS6}-[0-9][0-9][0-9][0-9]' AND LEN({expr}) = 11))"
        f" AND SUBSTRING({expr}, 1, 2) BETWEEN '01' AND '31'"
        f" AND SUBSTRING({expr}, 3, 2) BETWEEN '01' AND '12'"
    )
    return f"CASE WHEN {is_cpr_form} THEN '{marker}' ELSE {expr} END"
