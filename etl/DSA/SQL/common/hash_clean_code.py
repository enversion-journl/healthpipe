# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Shared hash expression for clean_codes over an attribute combination.

A readable concatenation of many attributes can exceed SQL Server's 900-byte
clustered-key limit, so those clean_codes are hashed instead. The dimension
row carries every attribute in readable form anyway; the hash only keys it.

Used by the DRG and udredningsret classification dimensions, which must build
the SAME hash: the map is minted from the dim staging and looked up from the
fact, so two spellings of the expression would key nothing.
"""

from collections.abc import Sequence


def build_attribute_hash_sql(alias: str, columns: Sequence[str]) -> str:
    """SHA2_256 (hex, 64) over the columns, length-prefixed and injective.

    Each value is normalised (UPPER + trim) and length-prefixed before
    hashing, so the concatenation is injective -- without the prefix
    ('AB','C') and ('A','BC') would hash identically. NULL and empty collapse
    to the same combination by design. The caller adds its own clean_code
    prefix and cast.
    """
    parts = []
    for col in columns:
        norm = f"ISNULL(UPPER(LTRIM(RTRIM({alias}.[{col}]))), N'')"
        parts.append(f"CONCAT(LEN({norm}), N':', {norm}, N'|')")
    combined = f"CONCAT({', '.join(parts)})"
    return f"CONVERT(CHAR(64), HASHBYTES('SHA2_256', {combined}), 2)"
