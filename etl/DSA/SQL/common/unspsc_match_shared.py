# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

# DKK invoice amounts are 2 dp; tier match keys must align KMD micro-line sums with MySupply.
MATCH_CURRENCY_SCALE = 2


def sql_match_currency(expr: str) -> str:
    """Normalise a currency amount for tier equi-join (LineAmount, unit_price, invoice_total).

    Ref rows are rounded at build time; apply-pass must use the same expression on
    the MySupply side so both sides of the join share DKK grain (2 dp).
    """
    return f"ROUND(TRY_CONVERT(DECIMAL(18, 4), {expr}), {MATCH_CURRENCY_SCALE})"
