# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""SQL expression for ``identifier_staff``: a staff code in its short form.

Staff codes come in a short form and a long form, which is the short code with a
two-letter marker in front and an optional instance digit behind (``aa-aaa9aa``,
``aa-aaa9aa-9``, ``aa-aaa9aa--9``). The long form reduces to the upper-cased short
code, so the same person gets the same identifier whichever form a source stores.

Only a value that matches the long form exactly (marker, a 5- or 6-character
alphanumeric code, optional digit tail) is reduced. Anything else, such as free
text with a hyphen (``ANNA-LISE``), is carried upper-cased and trimmed but
otherwise unchanged, so two different values cannot collapse onto one identifier.
"""

from DSA.SQL.common.ils_clean_code_shared import sql_norm_code

_MARKER = "[A-Z][A-Z]-"
_CODE_LENGTHS = (5, 6)
_TAILS = ("", "-[0-9]", "--[0-9]")

# Binary collation keeps [A-Z] to the 26 ASCII capitals whatever the database collation.
_PATTERN_COLLATION = "Latin1_General_100_BIN2"


def _long_form_patterns() -> list[str]:
    """LIKE patterns for every long-form shape, one per code length and tail."""
    return [_MARKER + "[A-Z0-9]" * n + tail for n in _CODE_LENGTHS for tail in _TAILS]


def sql_staff_identifier(expr: str) -> str:
    """``identifier_staff`` from a raw staff code, or NULL when it is empty."""
    v = f"UPPER({sql_norm_code(expr)})"
    is_long_form = "\n        OR ".join(
        f"{v} COLLATE {_PATTERN_COLLATION} LIKE '{p}'" for p in _long_form_patterns()
    )
    # The code starts after the marker (position 4) and ends at the next hyphen.
    short_code = f"SUBSTRING({v}, 4, CHARINDEX('-', {v} + '-', 4) - 4)"
    return f"CASE\n    WHEN {is_long_form}\n    THEN {short_code}\n    ELSE {v}\nEND"
