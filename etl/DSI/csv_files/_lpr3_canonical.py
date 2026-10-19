# SPDX-FileCopyrightText: 2026 Enversion Journl ApS
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This file is part of Healthpipe. See the LICENSE file in the project
# root for the full AGPL-3.0 license text.

"""Collapse proven identical dimension versions without losing source references.

The source-SK lookup remains one row per source. Only its archive projection is
many-to-one. Compare exact source bytes (including validity, flags and audits),
apart from DW_SK/DW_EK identities, before accepting repeated business keys.
"""

from DSI.csv_files._lpr3_keys import dimension_source, source_identity_column


def canonical_select(dataset, select):
    keys = ",".join(f"[{c}]" for c in dataset.merge_keys)
    return (
        f"SELECT identities.*,MIN(source_sk) OVER (PARTITION BY {keys}) canonical_source_sk,"
        f"COUNT_BIG(*) OVER (PARTITION BY {keys}) antal_kilderaekker "
        f"FROM ({select}) identities"
    )


def conflicting_versions_sql(dataset, table):
    source = dimension_source(dataset)
    keys = ",".join(f"i.[{c}]" for c in dataset.merge_keys)
    payload = ",".join(
        f"CONVERT(VARBINARY(MAX),s.[{c.name}])"
        for c in source.columns
        if not c.name.startswith(("DW_SK_", "DW_EK_"))
    )
    return f"""SELECT COUNT_BIG(*) FROM (
SELECT {','.join(f'[{c}]' for c in dataset.merge_keys)} FROM (
SELECT {keys} FROM ext.[{source.ext_table}] s JOIN ext.[{table}] i
ON i.source_sk=CONVERT(BIGINT,s.[{source_identity_column(dataset)}])
WHERE i.antal_kilderaekker>1
GROUP BY {keys},{payload}
) variants GROUP BY {','.join(f'[{c}]' for c in dataset.merge_keys)}
HAVING COUNT_BIG(*)>1) conflicts"""
